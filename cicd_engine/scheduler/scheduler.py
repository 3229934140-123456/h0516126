from __future__ import annotations

import concurrent.futures
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from ..artifacts.artifact_manager import ArtifactManager
from ..executor.executor import (
    EnvironmentManager,
    ExecutionEnvironment,
    ExecutionResult,
)
from ..logging.log_stream import LogLevel, LogStream
from ..models.pipeline import (
    DependencyGraph,
    FailureStrategy,
    Pipeline,
    PipelineResult,
    PipelineStatus,
    Stage,
    Step,
    StepStatus,
    Variable,
)
from ..state.state_manager import StateManager, StatusAggregator


class SchedulerEventType(Enum):
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"
    STAGE_FAILED = "stage_failed"
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"
    STEP_SKIPPED = "step_skipped"
    STEP_RETRY = "step_retry"
    ARTIFACT_PUBLISHED = "artifact_published"
    ARTIFACT_RESTORED = "artifact_restored"
    CONDITION_FALSE = "condition_false"
    ERROR = "error"


@dataclass
class SchedulerEvent:
    type: SchedulerEventType
    pipeline_id: str
    stage_name: Optional[str] = None
    step_name: Optional[str] = None
    message: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class ExecutionContext:
    pipeline: Pipeline
    pipeline_id: str
    log_stream: LogStream
    state_manager: StateManager
    artifact_manager: ArtifactManager
    env_manager: EnvironmentManager
    variables: Dict[str, Any] = field(default_factory=dict)
    output_variables: Dict[str, Any] = field(default_factory=dict)
    completed_nodes: Set[str] = field(default_factory=set)
    skipped_nodes: Set[str] = field(default_factory=set)
    failed_nodes: Set[str] = field(default_factory=set)
    running_nodes: Set[str] = field(default_factory=set)
    abort_flag: threading.Event = field(default_factory=threading.Event)
    pause_flag: threading.Event = field(default_factory=threading.Event)
    max_workers: int = 4
    _node_key_step_map: Dict[str, tuple] = field(default_factory=dict)
    _step_key_node_map: Dict[str, str] = field(default_factory=dict)
    _stage_key_node_map: Dict[str, str] = field(default_factory=dict)

    def get_context_variables(self) -> Dict[str, Any]:
        ctx = {}
        ctx["pipeline"] = {
            "id": self.pipeline_id,
            "name": self.pipeline.name,
            "version": self.pipeline.version,
            "status": self.state_manager.get_state(self.pipeline_id).status.value
            if self.state_manager.get_state(self.pipeline_id)
            else "unknown",
        }
        ctx.update(self.variables)
        ctx.update(self.output_variables)

        step_statuses = {}
        state = self.state_manager.get_state(self.pipeline_id)
        if state:
            for key, s in state.step_states.items():
                step_statuses[key.replace(":", "_").replace("-", "_")] = s.get("status", "unknown")
                step_statuses[f"{key.replace(':', '_').replace('-', '_')}_success"] = s.get("status") == "success"
                step_statuses[f"{key.replace(':', '_').replace('-', '_')}_failed"] = s.get("status") in (
                    "failed",
                    "timeout",
                    "cancelled",
                )
        ctx["steps"] = step_statuses

        pipeline_state = self.state_manager.get_state(self.pipeline_id)
        if pipeline_state:
            ctx["success"] = pipeline_state.status in (
                PipelineStatus.SUCCESS,
                PipelineStatus.PARTIAL_SUCCESS,
            )
            ctx["failed"] = pipeline_state.status in (
                PipelineStatus.FAILED,
                PipelineStatus.CANCELLED,
            )
            ctx["progress"] = pipeline_state.progress
            ctx["duration"] = pipeline_state.duration or 0

        return ctx

    def is_aborted(self) -> bool:
        return self.abort_flag.is_set()

    def is_paused(self) -> bool:
        return self.pause_flag.is_set()

    def wait_if_paused(self) -> None:
        while self.pause_flag.is_set() and not self.abort_flag.is_set():
            time.sleep(0.2)

    def abort(self) -> None:
        self.abort_flag.set()

    def pause(self) -> None:
        self.pause_flag.set()

    def resume(self) -> None:
        self.pause_flag.clear()


class PipelineScheduler:
    def __init__(
        self,
        *,
        log_stream: LogStream,
        state_manager: StateManager,
        artifact_manager: ArtifactManager,
        env_manager: EnvironmentManager,
        max_workers: int = 4,
    ):
        self.log_stream = log_stream
        self.state_manager = state_manager
        self.artifact_manager = artifact_manager
        self.env_manager = env_manager
        self.max_workers = max_workers
        self._event_handlers: List[Callable[[SchedulerEvent], None]] = []
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._contexts: Dict[str, ExecutionContext] = {}

    def on_event(self, handler: Callable[[SchedulerEvent], None]) -> None:
        self._event_handlers.append(handler)

    def off_event(self, handler: Callable[[SchedulerEvent], None]) -> None:
        if handler in self._event_handlers:
            self._event_handlers.remove(handler)

    def _emit_event(self, event: SchedulerEvent) -> None:
        for handler in list(self._event_handlers):
            try:
                handler(event)
            except Exception as e:
                print(f"Event handler error: {e}")

    def _log(self, ctx: ExecutionContext, message: str, level: LogLevel = LogLevel.INFO,
             stage_name: Optional[str] = None, step_name: Optional[str] = None) -> None:
        self.log_stream.log(
            message,
            level,
            pipeline_id=ctx.pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
        )

    def execute(self, pipeline: Pipeline, *, timeout: Optional[int] = None) -> PipelineResult:
        ctx = self._create_context(pipeline)
        self._contexts[pipeline.pipeline_id] = ctx

        try:
            return self._execute_pipeline(ctx, timeout=timeout)
        finally:
            self._contexts.pop(pipeline.pipeline_id, None)
            self.env_manager.cleanup_all()

    def _create_context(self, pipeline: Pipeline) -> ExecutionContext:
        ctx = ExecutionContext(
            pipeline=pipeline,
            pipeline_id=pipeline.pipeline_id,
            log_stream=self.log_stream,
            state_manager=self.state_manager,
            artifact_manager=self.artifact_manager,
            env_manager=self.env_manager,
            max_workers=self.max_workers,
        )

        for v in pipeline.variables:
            ctx.variables[v.key] = v.value

        self.state_manager.create_state(pipeline)

        for stage in pipeline.stages:
            stage_node_key = f"stage:{stage.name}"
            ctx._stage_key_node_map[stage.name] = stage_node_key
            ctx._node_key_step_map[stage_node_key] = (stage, None)
            for step in stage.steps:
                step_node_key = f"step:{stage.name}:{step.name}"
                ctx._step_key_node_map[f"{stage.name}:{step.name}"] = step_node_key
                ctx._node_key_step_map[step_node_key] = (stage, step)

        return ctx

    def _execute_pipeline(
        self, ctx: ExecutionContext, *, timeout: Optional[int] = None
    ) -> PipelineResult:
        pipeline_start = time.time()
        self._emit_event(
            SchedulerEvent(
                type=SchedulerEventType.PIPELINE_STARTED,
                pipeline_id=ctx.pipeline_id,
                message=f"Pipeline '{ctx.pipeline.name}' started",
            )
        )
        self._log(ctx, f"===== Pipeline '{ctx.pipeline.name}' started =====", LogLevel.INFO)
        self.state_manager.update_pipeline(
            ctx.pipeline_id,
            PipelineStatus.RUNNING,
            start_time=pipeline_start,
        )

        graph = DependencyGraph(ctx.pipeline)

        try:
            cycles = graph.detect_cycles()
            if cycles:
                error_msg = f"Circular dependencies detected: {cycles}"
                self._log(ctx, error_msg, LogLevel.CRITICAL)
                self._emit_event(
                    SchedulerEvent(
                        type=SchedulerEventType.ERROR,
                        pipeline_id=ctx.pipeline_id,
                        message=error_msg,
                    )
                )
                raise ValueError(error_msg)
        except ValueError as e:
            error_msg = str(e)
            self._log(ctx, error_msg, LogLevel.CRITICAL)
            self.state_manager.update_pipeline(
                ctx.pipeline_id,
                PipelineStatus.FAILED,
                end_time=time.time(),
                error_message=error_msg,
            )
            return self.state_manager.get_result(ctx.pipeline_id) or PipelineResult(
                pipeline_id=ctx.pipeline_id,
                pipeline_name=ctx.pipeline.name,
                status=PipelineStatus.FAILED,
                error_message=error_msg,
            )

        total_timeout = timeout or ctx.pipeline.timeout
        deadline = pipeline_start + total_timeout

        try:
            self._run_schedule_loop(ctx, graph, deadline)
        except Exception as e:
            error_msg = f"Pipeline execution error: {str(e)}"
            self._log(ctx, error_msg, LogLevel.ERROR)
            self.state_manager.update_pipeline(
                ctx.pipeline_id,
                PipelineStatus.FAILED,
                end_time=time.time(),
                error_message=error_msg,
            )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.PIPELINE_FAILED,
                    pipeline_id=ctx.pipeline_id,
                    message=error_msg,
                )
            )

        pipeline_end = time.time()
        result = self.state_manager.get_result(ctx.pipeline_id)
        state = self.state_manager.get_state(ctx.pipeline_id)
        if result and state:
            final_status = result.status
            if not state.end_time:
                state.end_time = pipeline_end
                self.state_manager.update_pipeline(
                    ctx.pipeline_id,
                    final_status,
                    end_time=pipeline_end,
                )
        elif result:
            final_status = result.status
            state = self.state_manager.get_state(ctx.pipeline_id)
            if state and not state.end_time:
                state.end_time = pipeline_end
                self.state_manager.update_pipeline(
                    ctx.pipeline_id,
                    final_status,
                    end_time=pipeline_end,
                )
        else:
            final_state = self.state_manager.get_state(ctx.pipeline_id)
            if final_state:
                final_status = final_state.status
                if not final_state.end_time:
                    final_state.end_time = pipeline_end
                    self.state_manager.update_pipeline(
                        ctx.pipeline_id,
                        final_status,
                        end_time=pipeline_end,
                    )
            else:
                final_status = PipelineStatus.SUCCESS
                self.state_manager.update_pipeline(
                    ctx.pipeline_id,
                    final_status,
                    start_time=pipeline_start,
                    end_time=pipeline_end,
                )

        if final_status in (PipelineStatus.RUNNING, PipelineStatus.PENDING, PipelineStatus.PAUSED):
            state = self.state_manager.get_state(ctx.pipeline_id)
            if state:
                state.aggregate_from_steps()
                final_status = state.status
                self.state_manager.update_pipeline(
                    ctx.pipeline_id,
                    final_status,
                    end_time=pipeline_end,
                )

        if final_status == PipelineStatus.SUCCESS:
            self._log(
                ctx,
                f"===== Pipeline '{ctx.pipeline.name}' completed successfully in {pipeline_end - pipeline_start:.2f}s =====",
                LogLevel.INFO,
            )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.PIPELINE_COMPLETED,
                    pipeline_id=ctx.pipeline_id,
                    message=f"Pipeline completed in {pipeline_end - pipeline_start:.2f}s",
                    data={"duration": pipeline_end - pipeline_start},
                )
            )
        elif final_status == PipelineStatus.PARTIAL_SUCCESS:
            self._log(
                ctx,
                f"===== Pipeline '{ctx.pipeline.name}' partially completed =====",
                LogLevel.WARN,
            )
        else:
            self._log(
                ctx,
                f"===== Pipeline '{ctx.pipeline.name}' failed ({final_status.value}) =====",
                LogLevel.ERROR,
            )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.PIPELINE_FAILED,
                    pipeline_id=ctx.pipeline_id,
                    message=f"Pipeline failed with status: {final_status.value}",
                )
            )

        return self.state_manager.get_result(ctx.pipeline_id) or PipelineResult(
            pipeline_id=ctx.pipeline_id,
            pipeline_name=ctx.pipeline.name,
            status=final_status,
            start_time=pipeline_start,
            end_time=pipeline_end,
        )

    def _run_schedule_loop(
        self, ctx: ExecutionContext, graph: DependencyGraph, deadline: float
    ) -> None:
        futures: Dict[concurrent.futures.Future, str] = {}
        step_node_keys: Set[str] = {
            k for k in graph.all_node_keys() if k.startswith("step:")
        }
        stage_node_keys: Set[str] = {
            k for k in graph.all_node_keys() if k.startswith("stage:")
        }
        processed_steps: Set[str] = set()
        processed_stages: Set[str] = set()
        started_stages: Set[str] = set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=ctx.max_workers) as executor:
            self._executor = executor

            while processed_steps < step_node_keys or processed_stages < stage_node_keys:
                if ctx.is_aborted():
                    self._handle_abort(ctx, step_node_keys | stage_node_keys,
                                       processed_steps | processed_stages)
                    break

                if time.time() > deadline:
                    self._handle_timeout(ctx, step_node_keys | stage_node_keys,
                                         processed_steps | processed_stages)
                    break

                ctx.wait_if_paused()

                completed_futures = [f for f in futures if f.done()]
                for f in completed_futures:
                    node_key = futures.pop(f)
                    try:
                        result = f.result()
                        processed_steps.add(node_key)
                        ctx.completed_nodes.add(node_key)
                        if not result:
                            ctx.failed_nodes.add(node_key)
                    except Exception as e:
                        self._log(
                            ctx,
                            f"Internal error in node {node_key}: {e}",
                            LogLevel.ERROR,
                        )
                        ctx.failed_nodes.add(node_key)
                        processed_steps.add(node_key)

                completed_stages: Set[str] = set()
                for stage_node in started_stages:
                    stage_name = stage_node[len("stage:"):]
                    stage = ctx.pipeline.get_stage(stage_name)
                    if not stage:
                        completed_stages.add(stage_node)
                        continue
                    all_done = True
                    for s in stage.steps:
                        sk = f"step:{stage_name}:{s.name}"
                        if sk not in processed_steps and sk not in ctx.skipped_nodes:
                            all_done = False
                            break
                    if all_done:
                        completed_stages.add(stage_node)
                        if stage_node not in processed_stages:
                            end_time = time.time()
                            state = self.state_manager.get_state(ctx.pipeline_id)
                            if state:
                                agg = StatusAggregator.aggregate_step_statuses(
                                    [state.get_step_status(stage_name, s.name) or StepStatus.PENDING
                                     for s in stage.steps]
                                )
                                self.state_manager.update_stage(
                                    ctx.pipeline_id, stage_name, agg, end_time=end_time
                                )
                            else:
                                agg = StepStatus.SUCCESS
                            self._log(
                                ctx,
                                f"----- Stage '{stage_name}' completed ({agg.value}) -----",
                                LogLevel.INFO,
                                stage_name=stage_name,
                            )
                            if agg in (StepStatus.SUCCESS, StepStatus.SKIPPED):
                                self._emit_event(
                                    SchedulerEvent(
                                        type=SchedulerEventType.STAGE_COMPLETED,
                                        pipeline_id=ctx.pipeline_id,
                                        stage_name=stage_name,
                                        data={"status": agg.value},
                                    )
                                )
                            else:
                                self._emit_event(
                                    SchedulerEvent(
                                        type=SchedulerEventType.STAGE_FAILED,
                                        pipeline_id=ctx.pipeline_id,
                                        stage_name=stage_name,
                                        data={"status": agg.value},
                                    )
                                )
                processed_stages = completed_stages

                for stage_node in stage_node_keys:
                    if stage_node in processed_stages or stage_node in started_stages:
                        continue
                    deps = graph.get_dependencies(stage_node)
                    deps_ready = all(
                        dep in processed_stages or dep in ctx.completed_nodes
                        or dep in ctx.skipped_nodes
                        for dep in deps
                    )
                    if not deps or deps_ready:
                        stage_name = stage_node[len("stage:"):]
                        stage = ctx.pipeline.get_stage(stage_name)
                        if stage:
                            self._execute_stage(ctx, stage, stage_node)
                            started_stages.add(stage_node)
                            if stage.condition and not self._evaluate_condition(ctx, stage.condition):
                                processed_stages.add(stage_node)
                                for s in stage.steps:
                                    sk = f"step:{stage_name}:{s.name}"
                                    ctx.skipped_nodes.add(sk)
                                    processed_steps.add(sk)

                if ctx.failed_nodes and self._should_abort_on_failure(ctx):
                    self._handle_failure_abort(ctx, step_node_keys | stage_node_keys,
                                               processed_steps | processed_stages)
                    break

                running_step_keys = {futures[f] for f in futures}
                ready_steps = []
                skipped_due_to_upstream: List[str] = []

                for step_key in step_node_keys:
                    if step_key in processed_steps or step_key in running_step_keys:
                        continue
                    if step_key in ctx.skipped_nodes:
                        processed_steps.add(step_key)
                        continue
                    stage_, step_ = ctx._node_key_step_map.get(step_key, (None, None))
                    if not stage_ or not step_:
                        processed_steps.add(step_key)
                        continue
                    stage_node_of_step = f"stage:{stage_.name}"
                    if stage_node_of_step not in started_stages:
                        continue

                    deps = graph.get_dependencies(step_key)
                    all_ready = True
                    has_failed_dep = False
                    failed_dep_name = None

                    if deps:
                        for dep in deps:
                            if dep.startswith("stage:"):
                                dep_stage_name = dep[len("stage:"):]
                                dep_stage = ctx.pipeline.get_stage(dep_stage_name)
                                stage_failed = False
                                if dep_stage:
                                    dep_state = self.state_manager.get_state(ctx.pipeline_id)
                                    if dep_state:
                                        dep_status = dep_state.get_stage_status(dep_stage_name)
                                        if dep_status and dep_status in (StepStatus.FAILED, StepStatus.TIMEOUT):
                                            stage_failed = True
                                if stage_failed:
                                    has_failed_dep = True
                                    failed_dep_name = dep_stage_name
                                    break

                                if dep not in processed_stages:
                                    all_ready = False
                                    break
                                continue

                            if dep in ctx.failed_nodes:
                                _, dep_step = ctx._node_key_step_map.get(dep, (None, None))
                                if dep_step and dep_step.allow_failure:
                                    continue
                                has_failed_dep = True
                                failed_dep_name = dep
                                break

                            if dep in processed_steps or dep in ctx.skipped_nodes or dep in ctx.completed_nodes:
                                continue

                            all_ready = False
                            break

                    if has_failed_dep:
                        skipped_due_to_upstream.append((step_key, failed_dep_name))
                        continue

                    if all_ready:
                        ready_steps.append(step_key)

                for step_key, failed_dep in skipped_due_to_upstream:
                    ctx.skipped_nodes.add(step_key)
                    processed_steps.add(step_key)
                    _, step = ctx._node_key_step_map.get(step_key, (None, None))
                    stage_name = step_key.split(":", 2)[1]
                    step_name = step_key.split(":", 2)[2]
                    error_msg = f"Skipped due to upstream failure: {failed_dep}"
                    self._log(
                        ctx,
                        f"Step '{step_name}' skipped: {error_msg}",
                        LogLevel.WARN,
                        stage_name=stage_name,
                        step_name=step_name,
                    )
                    self.state_manager.update_step(
                        ctx.pipeline_id,
                        stage_name,
                        step_name,
                        StepStatus.SKIPPED,
                        error_message=error_msg,
                        start_time=time.time(),
                        end_time=time.time(),
                    )
                    self._emit_event(
                        SchedulerEvent(
                            type=SchedulerEventType.STEP_SKIPPED,
                            pipeline_id=ctx.pipeline_id,
                            stage_name=stage_name,
                            step_name=step_name,
                            message=error_msg,
                        )
                    )

                for step_key in ready_steps:
                    if step_key in processed_steps or step_key in running_step_keys:
                        continue
                    if len(futures) >= ctx.max_workers:
                        break
                    node = ctx._node_key_step_map.get(step_key)
                    if not node:
                        processed_steps.add(step_key)
                        continue
                    stage, step = node
                    future = executor.submit(
                        self._execute_step, ctx, stage, step, step_key
                    )
                    futures[future] = step_key
                    ctx.running_nodes.add(step_key)

                no_progress = False
                if not futures and processed_steps < step_node_keys:
                    remaining = step_node_keys - processed_steps - ctx.skipped_nodes
                    if remaining:
                        any_ready = False
                        upstream_skip: List[tuple] = []
                        pending_stage_steps = False
                        for step_key in remaining:
                            stage_, step_ = ctx._node_key_step_map.get(step_key, (None, None))
                            if not stage_ or not step_:
                                processed_steps.add(step_key)
                                continue
                            stage_node_of_step = f"stage:{stage_.name}"
                            if stage_node_of_step not in started_stages:
                                pending_stage_steps = True
                                continue
                            deps = graph.get_dependencies(step_key)
                            all_ready = True
                            has_failed_dep = False
                            failed_dep_name = None
                            if deps:
                                for dep in deps:
                                    if dep.startswith("stage:"):
                                        dep_stage_name = dep[len("stage:"):]
                                        dep_stage = ctx.pipeline.get_stage(dep_stage_name)
                                        stage_failed = False
                                        if dep_stage:
                                            dep_state = self.state_manager.get_state(ctx.pipeline_id)
                                            if dep_state:
                                                dep_status = dep_state.get_stage_status(dep_stage_name)
                                                if dep_status and dep_status in (StepStatus.FAILED, StepStatus.TIMEOUT):
                                                    stage_failed = True
                                        if stage_failed:
                                            has_failed_dep = True
                                            failed_dep_name = dep_stage_name
                                            break

                                        if dep not in processed_stages:
                                            all_ready = False
                                            break
                                        continue
                                    if dep in ctx.failed_nodes:
                                        _, dep_step = ctx._node_key_step_map.get(dep, (None, None))
                                        if dep_step and dep_step.allow_failure:
                                            continue
                                        has_failed_dep = True
                                        failed_dep_name = dep
                                        break
                                    if dep in processed_steps or dep in ctx.skipped_nodes or dep in ctx.completed_nodes:
                                        continue
                                    all_ready = False
                                    break
                            if has_failed_dep:
                                upstream_skip.append((step_key, failed_dep_name))
                                continue
                            if all_ready:
                                any_ready = True
                                break
                        if upstream_skip:
                            for step_key, failed_dep in upstream_skip:
                                ctx.skipped_nodes.add(step_key)
                                processed_steps.add(step_key)
                                _, step = ctx._node_key_step_map.get(step_key, (None, None))
                                stage_name = step_key.split(":", 2)[1]
                                step_name = step_key.split(":", 2)[2]
                                error_msg = f"Skipped due to upstream failure: {failed_dep}"
                                self._log(
                                    ctx,
                                    f"Step '{step_name}' skipped: {error_msg}",
                                    LogLevel.WARN,
                                    stage_name=stage_name,
                                    step_name=step_name,
                                )
                                self.state_manager.update_step(
                                    ctx.pipeline_id,
                                    stage_name,
                                    step_name,
                                    StepStatus.SKIPPED,
                                    error_message=error_msg,
                                    start_time=time.time(),
                                    end_time=time.time(),
                                )
                        elif not any_ready and not pending_stage_steps:
                            self._log(
                                ctx,
                                f"Cannot proceed with remaining steps: {remaining}",
                                LogLevel.WARN,
                            )
                            for step_key in remaining:
                                if step_key in ctx.skipped_nodes:
                                    continue
                                ctx.skipped_nodes.add(step_key)
                                processed_steps.add(step_key)
                                _, step = ctx._node_key_step_map.get(step_key, (None, None))
                                stage_name = step_key.split(":", 2)[1]
                                step_name = step_key.split(":", 2)[2]
                                error_msg = "Cannot resolve dependencies"
                                self.state_manager.update_step(
                                    ctx.pipeline_id, stage_name, step_name, StepStatus.SKIPPED,
                                    error_message=error_msg,
                                    start_time=time.time(),
                                    end_time=time.time(),
                                )

                if futures:
                    time.sleep(0.05)
                elif processed_steps < step_node_keys:
                    time.sleep(0.05)
                else:
                    break

            if futures:
                for f in futures:
                    f.cancel()

        self._executor = None

    def _should_abort_on_failure(self, ctx: ExecutionContext) -> bool:
        return ctx.pipeline.failure_strategy in (
            FailureStrategy.ABORT,
            FailureStrategy.ROLLBACK,
        )

    def _handle_abort(
        self, ctx: ExecutionContext, all_nodes: Set[str], processed: Set[str]
    ) -> None:
        self._log(ctx, "Pipeline aborted by user", LogLevel.WARN)
        now = time.time()
        state = self.state_manager.get_state(ctx.pipeline_id)
        for node_key in all_nodes - processed:
            node = ctx._node_key_step_map.get(node_key)
            if node:
                stage, step = node
                if step is None:
                    stage_start = None
                    if state:
                        stage_state = state.stage_states.get(stage.name, {})
                        stage_start = stage_state.get("start_time")
                    self.state_manager.update_stage(
                        ctx.pipeline_id,
                        stage.name,
                        StepStatus.CANCELLED,
                        start_time=stage_start or now,
                        end_time=now,
                    )
                else:
                    step_start = None
                    if state:
                        step_state = state.step_states.get(
                            f"{stage.name}:{step.name}", {}
                        )
                        step_start = step_state.get("start_time")
                    self.state_manager.update_step(
                        ctx.pipeline_id,
                        stage.name,
                        step.name,
                        StepStatus.CANCELLED,
                        start_time=step_start or now,
                        end_time=now,
                    )
                    ctx.skipped_nodes.add(node_key)
            processed.add(node_key)

    def _handle_timeout(
        self, ctx: ExecutionContext, all_nodes: Set[str], processed: Set[str]
    ) -> None:
        self._log(ctx, "Pipeline timed out", LogLevel.ERROR)
        now = time.time()
        state = self.state_manager.get_state(ctx.pipeline_id)
        for node_key in all_nodes - processed:
            node = ctx._node_key_step_map.get(node_key)
            if node:
                stage, step = node
                if step is None:
                    stage_start = None
                    if state:
                        stage_state = state.stage_states.get(stage.name, {})
                        stage_start = stage_state.get("start_time")
                    self.state_manager.update_stage(
                        ctx.pipeline_id,
                        stage.name,
                        StepStatus.CANCELLED,
                        start_time=stage_start or now,
                        end_time=now,
                    )
                else:
                    step_start = None
                    if state:
                        step_state = state.step_states.get(
                            f"{stage.name}:{step.name}", {}
                        )
                        step_start = step_state.get("start_time")
                    self.state_manager.update_step(
                        ctx.pipeline_id,
                        stage.name,
                        step.name,
                        StepStatus.TIMEOUT,
                        error_message="Pipeline timeout",
                        start_time=step_start or now,
                        end_time=now,
                    )
                    ctx.failed_nodes.add(node_key)
            processed.add(node_key)

    def _handle_failure_abort(
        self, ctx: ExecutionContext, all_nodes: Set[str], processed: Set[str]
    ) -> None:
        self._log(ctx, "Aborting pipeline due to failure (strategy: ABORT)", LogLevel.WARN)
        now = time.time()
        state = self.state_manager.get_state(ctx.pipeline_id)
        for node_key in all_nodes - processed:
            node = ctx._node_key_step_map.get(node_key)
            if node:
                stage, step = node
                if step is None:
                    stage_start = None
                    if state:
                        stage_state = state.stage_states.get(stage.name, {})
                        stage_start = stage_state.get("start_time")
                    self.state_manager.update_stage(
                        ctx.pipeline_id,
                        stage.name,
                        StepStatus.CANCELLED,
                        start_time=stage_start or now,
                        end_time=now,
                    )
                else:
                    step_start = None
                    if state:
                        step_state = state.step_states.get(
                            f"{stage.name}:{step.name}", {}
                        )
                        step_start = step_state.get("start_time")
                    self.state_manager.update_step(
                        ctx.pipeline_id,
                        stage.name,
                        step.name,
                        StepStatus.SKIPPED,
                        start_time=step_start or now,
                        end_time=now,
                    )
                    ctx.skipped_nodes.add(node_key)
            processed.add(node_key)

    def _get_ready_nodes(
        self, ctx: ExecutionContext, graph: DependencyGraph, processed: Set[str]
    ) -> List[str]:
        ready = []
        running = set(ctx.running_nodes)

        for node_key in graph.all_node_keys():
            if node_key in processed or node_key in running:
                continue

            deps = graph.get_dependencies(node_key)
            if not deps:
                ready.append(node_key)
            else:
                all_deps_done = True
                for dep in deps:
                    if dep in processed or dep in ctx.completed_nodes or dep in ctx.skipped_nodes:
                        continue
                    all_deps_done = False
                    break
                if all_deps_done:
                    ready.append(node_key)

        return ready

    def _execute_stage(
        self, ctx: ExecutionContext, stage: Stage, node_key: str
    ) -> bool:
        if ctx.is_aborted():
            ctx.running_nodes.discard(node_key)
            now = time.time()
            self.state_manager.update_stage(
                ctx.pipeline_id,
                stage.name,
                StepStatus.CANCELLED,
                start_time=now,
                end_time=now,
            )
            for step in stage.steps:
                self.state_manager.update_step(
                    ctx.pipeline_id,
                    stage.name,
                    step.name,
                    StepStatus.CANCELLED,
                    start_time=now,
                    end_time=now,
                )
            return True

        stage_start = time.time()

        for v in stage.variables:
            ctx.variables[v.key] = v.value
            self.state_manager.set_variable(ctx.pipeline_id, v.key, v.value)

        if stage.condition and not self._evaluate_condition(ctx, stage.condition):
            self._log(
                ctx,
                f"Stage '{stage.name}' skipped: condition not met",
                LogLevel.INFO,
                stage_name=stage.name,
            )
            ctx.skipped_nodes.add(node_key)
            ctx.running_nodes.discard(node_key)
            self.state_manager.update_stage(
                ctx.pipeline_id,
                stage.name,
                StepStatus.SKIPPED,
                start_time=stage_start,
                end_time=stage_start,
            )
            for step in stage.steps:
                step_key = f"step:{stage.name}:{step.name}"
                ctx.skipped_nodes.add(step_key)
                self.state_manager.update_step(
                    ctx.pipeline_id,
                    stage.name,
                    step.name,
                    StepStatus.SKIPPED,
                    start_time=stage_start,
                    end_time=stage_start,
                )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.STEP_SKIPPED,
                    pipeline_id=ctx.pipeline_id,
                    stage_name=stage.name,
                    message=f"Stage '{stage.name}' skipped by condition",
                )
            )
            return True

        self._log(ctx, f"----- Stage '{stage.name}' started -----", LogLevel.INFO, stage_name=stage.name)
        self._emit_event(
            SchedulerEvent(
                type=SchedulerEventType.STAGE_STARTED,
                pipeline_id=ctx.pipeline_id,
                stage_name=stage.name,
                message=f"Stage '{stage.name}' started",
            )
        )
        self.state_manager.update_stage(
            ctx.pipeline_id, stage.name, StepStatus.RUNNING, start_time=stage_start
        )
        ctx.running_nodes.discard(node_key)

        self.state_manager.update_stage(
            ctx.pipeline_id,
            stage.name,
            StepStatus.RUNNING,
            start_time=stage_start,
        )
        return True

    def _execute_step(
        self, ctx: ExecutionContext, stage: Stage, step: Step, node_key: str
    ) -> bool:
        ctx.running_nodes.discard(node_key)

        if ctx.is_aborted():
            self.state_manager.update_step(
                ctx.pipeline_id, stage.name, step.name, StepStatus.CANCELLED
            )
            ctx.skipped_nodes.add(node_key)
            return True

        step_start = time.time()

        if step.condition and not self._evaluate_condition(ctx, step.condition):
            self._log(
                ctx,
                f"Step '{step.name}' skipped: condition not met",
                LogLevel.INFO,
                stage_name=stage.name,
                step_name=step.name,
            )
            ctx.skipped_nodes.add(node_key)
            self.state_manager.update_step(
                ctx.pipeline_id,
                stage.name,
                step.name,
                StepStatus.SKIPPED,
                start_time=step_start,
                end_time=step_start,
            )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.STEP_SKIPPED,
                    pipeline_id=ctx.pipeline_id,
                    stage_name=stage.name,
                    step_name=step.name,
                    message=f"Step '{step.name}' skipped by condition",
                )
            )
            return True

        self._emit_event(
            SchedulerEvent(
                type=SchedulerEventType.STEP_STARTED,
                pipeline_id=ctx.pipeline_id,
                stage_name=stage.name,
                step_name=step.name,
                message=f"Step '{step.name}' started",
            )
        )
        self.state_manager.update_step(
            ctx.pipeline_id,
            stage.name,
            step.name,
            StepStatus.RUNNING,
            start_time=step_start,
        )

        retry_count = 0
        max_retries = step.retries
        success = False
        last_error: Optional[str] = None
        last_exit_code = -1
        timeout = False

        workdir = None

        try:
            while retry_count <= max_retries and not success and not ctx.is_aborted():
                if retry_count > 0:
                    self._log(
                        ctx,
                        f"Retrying step '{step.name}' (attempt {retry_count + 1}/{max_retries + 1})",
                        LogLevel.WARN,
                        stage_name=stage.name,
                        step_name=step.name,
                    )
                    self._emit_event(
                        SchedulerEvent(
                            type=SchedulerEventType.STEP_RETRY,
                            pipeline_id=ctx.pipeline_id,
                            stage_name=stage.name,
                            step_name=step.name,
                            data={"retry_count": retry_count},
                        )
                    )
                    time.sleep(step.retry_delay)

                result, workdir = self._run_step_commands(ctx, stage, step)
                success = result.success
                last_exit_code = result.exit_code
                timeout = result.timeout
                if not success:
                    last_error = result.error_message or f"Exit code {result.exit_code}"
                    if result.output_variables:
                        ctx.output_variables.update(result.output_variables)

                if result.output_variables:
                    for k, v in result.output_variables.items():
                        if k != "__env_export__":
                            self.state_manager.set_variable(
                                ctx.pipeline_id, k, v, output=True
                            )
                            ctx.output_variables[k] = v

                retry_count += 1

        except Exception as e:
            last_error = str(e)
            self._log(
                ctx,
                f"Exception in step '{step.name}': {e}",
                LogLevel.ERROR,
                stage_name=stage.name,
                step_name=step.name,
            )

        step_end = time.time()

        if success and step.artifacts_out:
            try:
                stored_arts = self.artifact_manager.publish_batch(
                    step.artifacts_out,
                    pipeline_id=ctx.pipeline_id,
                    stage_name=stage.name,
                    step_name=step.name,
                    working_dir=workdir,
                )
                for art in stored_arts:
                    self._log(
                        ctx,
                        f"Published artifact '{art.name}' ({art.size_bytes} bytes)",
                        LogLevel.INFO,
                        stage_name=stage.name,
                        step_name=step.name,
                    )
                    self._emit_event(
                        SchedulerEvent(
                            type=SchedulerEventType.ARTIFACT_PUBLISHED,
                            pipeline_id=ctx.pipeline_id,
                            stage_name=stage.name,
                            step_name=step.name,
                            data={
                                "artifact_name": art.name,
                                "artifact_id": art.artifact_id,
                                "size_bytes": art.size_bytes,
                            },
                        )
                    )
            except Exception as e:
                self._log(
                    ctx,
                    f"Error publishing artifacts: {e}",
                    LogLevel.WARN,
                    stage_name=stage.name,
                    step_name=step.name,
                )

        if success:
            final_status = StepStatus.SUCCESS
            self._log(
                ctx,
                f"Step '{step.name}' completed successfully ({step_end - step_start:.2f}s)",
                LogLevel.INFO,
                stage_name=stage.name,
                step_name=step.name,
            )
            self._emit_event(
                SchedulerEvent(
                    type=SchedulerEventType.STEP_COMPLETED,
                    pipeline_id=ctx.pipeline_id,
                    stage_name=stage.name,
                    step_name=step.name,
                    data={"duration": step_end - step_start},
                )
            )
        else:
            if step.allow_failure:
                if timeout:
                    final_status = StepStatus.TIMEOUT
                else:
                    final_status = StepStatus.FAILED
                final_status = StepStatus.SUCCESS
                success = True
                self._log(
                    ctx,
                    f"Step '{step.name}' failed but marked as allow_failure",
                    LogLevel.WARN,
                    stage_name=stage.name,
                    step_name=step.name,
                )
            else:
                if timeout:
                    final_status = StepStatus.TIMEOUT
                else:
                    final_status = StepStatus.FAILED
                ctx.failed_nodes.add(node_key)

                self._log(
                    ctx,
                    f"Step '{step.name}' failed: {last_error}",
                    LogLevel.ERROR,
                    stage_name=stage.name,
                    step_name=step.name,
                )
                self._emit_event(
                    SchedulerEvent(
                        type=SchedulerEventType.STEP_FAILED,
                        pipeline_id=ctx.pipeline_id,
                        stage_name=stage.name,
                        step_name=step.name,
                        message=last_error,
                        data={"exit_code": last_exit_code, "retry_count": retry_count - 1},
                    )
                )

        self.state_manager.update_step(
            ctx.pipeline_id,
            stage.name,
            step.name,
            final_status,
            end_time=step_end,
            exit_code=last_exit_code,
            error_message=last_error if not success else None,
            retry_count=max(0, retry_count - 1),
        )

        return success or step.allow_failure

    def _run_step_commands(
        self, ctx: ExecutionContext, stage: Stage, step: Step
    ) -> tuple[ExecutionResult, str]:
        env = self.env_manager.create_environment(
            pipeline_id=ctx.pipeline_id,
            stage_name=stage.name,
            step_name=step.name,
            image=step.image,
            env_vars=self._build_env_vars(ctx, stage, step),
            working_dir=step.working_dir,
        )

        try:
            env.setup()
        except Exception as e:
            self._log(
                ctx,
                f"Failed to setup environment: {e}",
                LogLevel.WARN,
                stage_name=stage.name,
                step_name=step.name,
            )

        if step.artifacts_in:
            self._restore_artifacts(ctx, stage, step, env.working_dir)

        self._log(
            ctx,
            f"Working directory: {env.working_dir}",
            LogLevel.DEBUG,
            stage_name=stage.name,
            step_name=step.name,
        )

        step_context = {
            "pipeline_id": ctx.pipeline_id,
            "stage_name": stage.name,
            "step_name": step.name,
        }

        stop_on_failure = step.failure_strategy in (
            FailureStrategy.ABORT,
            FailureStrategy.ROLLBACK,
        )

        result = env.execute_commands(
            step.script,
            timeout=step.timeout,
            step_context=step_context,
            stop_on_failure=stop_on_failure,
        )

        return result, env.working_dir

    def _restore_artifacts(
        self, ctx: ExecutionContext, stage: Stage, step: Step, target_dir: str
    ) -> None:
        for art_name in step.artifacts_in:
            restored = self.artifact_manager.restore(
                art_name,
                target_dir,
                pipeline_id=ctx.pipeline_id,
                stage_name=stage.name,
                step_name=step.name,
                target_subdir=art_name,
            )
            if restored:
                self._log(
                    ctx,
                    f"Restored artifact '{art_name}'",
                    LogLevel.INFO,
                    stage_name=stage.name,
                    step_name=step.name,
                )
                self._emit_event(
                    SchedulerEvent(
                        type=SchedulerEventType.ARTIFACT_RESTORED,
                        pipeline_id=ctx.pipeline_id,
                        stage_name=stage.name,
                        step_name=step.name,
                        data={"artifact_name": art_name},
                    )
                )
            else:
                self._log(
                    ctx,
                    f"Warning: Could not restore artifact '{art_name}'",
                    LogLevel.WARN,
                    stage_name=stage.name,
                    step_name=step.name,
                )

    def _build_env_vars(
        self, ctx: ExecutionContext, stage: Stage, step: Step
    ) -> Dict[str, str]:
        env_vars: Dict[str, str] = {}

        for key, value in ctx.variables.items():
            env_vars[key] = str(value)
        for key, value in ctx.output_variables.items():
            if key != "__env_export__":
                env_vars[key] = str(value)

        env_vars["PIPELINE_ID"] = ctx.pipeline_id
        env_vars["PIPELINE_NAME"] = ctx.pipeline.name
        env_vars["PIPELINE_VERSION"] = ctx.pipeline.version
        env_vars["STAGE_NAME"] = stage.name
        env_vars["STEP_NAME"] = step.name
        env_vars["CI"] = "true"
        env_vars["CICD"] = "true"

        for v in stage.variables:
            env_vars[v.key] = str(v.value)
        for v in step.variables:
            env_vars[v.key] = str(v.value)

        env_vars.update({k: str(v) for k, v in step.environment.items()})

        return env_vars

    def _evaluate_condition(self, ctx: ExecutionContext, condition) -> bool:
        context = ctx.get_context_variables()
        try:
            result = condition.evaluate(context)
            if not result:
                self._emit_event(
                    SchedulerEvent(
                        type=SchedulerEventType.CONDITION_FALSE,
                        pipeline_id=ctx.pipeline_id,
                        message="Condition evaluated to false",
                        data={"expression": condition.expression or str(condition)},
                    )
                )
            return result
        except Exception as e:
            self._log(
                ctx,
                f"Error evaluating condition: {e}",
                LogLevel.WARN,
            )
            return False

    def abort(self, pipeline_id: str) -> None:
        ctx = self._contexts.get(pipeline_id)
        if ctx:
            ctx.abort()

    def pause(self, pipeline_id: str) -> None:
        ctx = self._contexts.get(pipeline_id)
        if ctx:
            ctx.pause()

    def resume(self, pipeline_id: str) -> None:
        ctx = self._contexts.get(pipeline_id)
        if ctx:
            ctx.resume()
