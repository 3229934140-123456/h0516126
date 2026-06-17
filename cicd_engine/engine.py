from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Union

from .artifacts.artifact_manager import (
    ArtifactManager,
    ArtifactStore,
    FileSystemStore,
    MemoryStore,
)
from .executor.executor import EnvironmentManager
from .logging.log_stream import (
    FileLogStore,
    InMemoryLogStore,
    LogEntry,
    LogLevel,
    LogStream,
    LogStore,
    LogSubscriber,
)
from .models.pipeline import (
    DependencyGraph,
    FailureStrategy,
    Pipeline,
    PipelineResult,
    PipelineStatus,
    Stage,
    Step,
    StepStatus,
)
from .parser.pipeline_parser import PipelineParser, ValidationError
from .scheduler.scheduler import (
    ExecutionContext,
    PipelineScheduler,
    SchedulerEvent,
    SchedulerEventType,
)
from .state.state_manager import (
    FileStateStore,
    MemoryStateStore,
    StateManager,
    StateStore,
)


class PipelineEngine:
    def __init__(
        self,
        *,
        base_dir: Optional[str] = None,
        artifact_store: Optional[ArtifactStore] = None,
        log_stores: Optional[List[LogStore]] = None,
        state_store: Optional[StateStore] = None,
        max_workers: int = 4,
        console_output: bool = True,
        min_console_level: LogLevel = LogLevel.INFO,
        auto_persist: bool = True,
    ):
        self.base_dir = os.path.abspath(base_dir or os.path.join(os.getcwd(), ".cicd_engine"))
        os.makedirs(self.base_dir, exist_ok=True)

        self.workspace_dir = os.path.join(self.base_dir, "workspace")
        self.artifacts_dir = os.path.join(self.base_dir, "artifacts")
        self.logs_dir = os.path.join(self.base_dir, "logs")
        self.state_dir = os.path.join(self.base_dir, "state")
        os.makedirs(self.workspace_dir, exist_ok=True)
        os.makedirs(self.artifacts_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(self.state_dir, exist_ok=True)

        self.log_stream = LogStream(
            stores=log_stores
            or [
                InMemoryLogStore(),
                FileLogStore(base_dir=self.logs_dir, split_by_pipeline=True),
            ],
            console_output=console_output,
            min_console_level=min_console_level,
        )

        self.artifact_manager = ArtifactManager(
            store=artifact_store or FileSystemStore(base_dir=self.artifacts_dir)
        )

        self.state_manager = StateManager(
            store=state_store or FileStateStore(base_dir=self.state_dir),
            auto_persist=auto_persist,
        )

        self.env_manager = EnvironmentManager(
            base_dir=self.workspace_dir,
            log_stream=self.log_stream,
        )

        self.scheduler = PipelineScheduler(
            log_stream=self.log_stream,
            state_manager=self.state_manager,
            artifact_manager=self.artifact_manager,
            env_manager=self.env_manager,
            max_workers=max_workers,
        )

        self._event_handlers: List[Callable[[SchedulerEvent], None]] = []
        self._log_handlers: List[Callable[[LogEntry], None]] = []

        def _forward_scheduler_event(event: SchedulerEvent) -> None:
            for handler in list(self._event_handlers):
                try:
                    handler(event)
                except Exception:
                    pass

        self.scheduler.on_event(_forward_scheduler_event)

    @classmethod
    def in_memory(cls, max_workers: int = 4, console_output: bool = True,
                  min_console_level: LogLevel = LogLevel.INFO) -> "PipelineEngine":
        return cls(
            artifact_store=MemoryStore(),
            log_stores=[InMemoryLogStore()],
            state_store=MemoryStateStore(),
            max_workers=max_workers,
            console_output=console_output,
            min_console_level=min_console_level,
        )

    def on_event(self, handler: Callable[[SchedulerEvent], None]) -> None:
        self._event_handlers.append(handler)

    def off_event(self, handler: Callable[[SchedulerEvent], None]) -> None:
        if handler in self._event_handlers:
            self._event_handlers.remove(handler)

    def on_log(self, handler: Callable[[LogEntry], None],
               min_level: LogLevel = LogLevel.DEBUG) -> LogSubscriber:
        from .logging.log_stream import CallbackSubscriber
        subscriber = CallbackSubscriber(handler, min_level=min_level)
        self.log_stream.subscribe(subscriber)
        return subscriber

    def parse_pipeline(
        self,
        source: Union[str, Dict[str, Any]],
        format_type: Optional[str] = None,
    ) -> Pipeline:
        if isinstance(source, dict):
            return PipelineParser.from_dict(source)
        if isinstance(source, str):
            if source.strip().startswith("{") or source.strip().startswith("["):
                format_type = format_type or "json"
                return PipelineParser.from_string(source, format_type)
            if os.path.exists(source):
                return PipelineParser.from_file(source)
            format_type = format_type or "yaml"
            return PipelineParser.from_string(source, format_type)
        raise ValueError(f"Unsupported source type: {type(source)}")

    def run(
        self,
        pipeline: Union[Pipeline, str, Dict[str, Any]],
        *,
        timeout: Optional[int] = None,
        variables: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        if not isinstance(pipeline, Pipeline):
            pipeline = self.parse_pipeline(pipeline)

        if variables:
            from .models.pipeline import Variable
            existing_keys = {v.key for v in pipeline.variables}
            for k, v in variables.items():
                if k in existing_keys:
                    for pv in pipeline.variables:
                        if pv.key == k:
                            pv.value = v
                            break
                else:
                    pipeline.variables.append(Variable(key=k, value=v))

        return self.scheduler.execute(pipeline, timeout=timeout)

    def run_file(
        self,
        file_path: str,
        *,
        timeout: Optional[int] = None,
        variables: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        pipeline = self.parse_pipeline(file_path)
        return self.run(pipeline, timeout=timeout, variables=variables)

    def validate(self, source: Union[str, Dict[str, Any]]) -> tuple[bool, List[str], List[str]]:
        parser = PipelineParser()
        try:
            if isinstance(source, dict):
                pipeline = parser.parse_dict(source)
            elif isinstance(source, str):
                if source.strip().startswith("{") or source.strip().startswith("["):
                    pipeline = parser.parse_string(source, "json")
                elif os.path.exists(source):
                    pipeline = parser.parse_file(source)
                else:
                    pipeline = parser.parse_string(source, "yaml")
            else:
                return False, [f"Unsupported source type: {type(source)}"], []

            try:
                graph = DependencyGraph(pipeline)
                graph.topological_sort()
            except ValueError as e:
                parser.errors.append(str(e))

            if parser.errors:
                return False, list(parser.errors), list(parser.warnings)
            return True, [], list(parser.warnings)

        except ValidationError as e:
            return False, e.errors, list(parser.warnings)
        except Exception as e:
            return False, [str(e)], []

    def dry_run(self, source: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        pipeline = self.parse_pipeline(source) if not isinstance(source, Pipeline) else source
        graph = DependencyGraph(pipeline)
        order = graph.topological_sort()

        plan = {
            "pipeline_id": pipeline.pipeline_id,
            "pipeline_name": pipeline.name,
            "total_stages": len(pipeline.stages),
            "total_steps": sum(len(s.steps) for s in pipeline.stages),
            "execution_order": [],
            "parallel_groups": [],
        }

        for node_key in order:
            if node_key.startswith("stage:"):
                plan["execution_order"].append({
                    "type": "stage",
                    "name": node_key[len("stage:"):],
                    "dependencies": list(graph.get_dependencies(node_key)),
                })
            elif node_key.startswith("step:"):
                parts = node_key.split(":", 2)
                plan["execution_order"].append({
                    "type": "step",
                    "stage": parts[1],
                    "name": parts[2],
                    "dependencies": list(graph.get_dependencies(node_key)),
                })

        completed: set[str] = set()
        pending = set(order)
        while pending:
            group = []
            for node_key in list(pending):
                deps = graph.get_dependencies(node_key)
                if deps.issubset(completed):
                    group.append(node_key)
            if not group:
                break
            plan["parallel_groups"].append(group)
            completed.update(group)
            pending -= set(group)

        return plan

    def list_pipelines(self, limit: Optional[int] = None, offset: int = 0):
        return self.state_manager.list_pipelines(limit=limit, offset=offset)

    def get_pipeline_state(self, pipeline_id: str):
        return self.state_manager.get_state(pipeline_id)

    def get_pipeline_result(self, pipeline_id: str):
        return self.state_manager.get_result(pipeline_id)

    def get_logs(
        self,
        pipeline_id: str,
        *,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        min_level: Optional[LogLevel] = None,
        limit: Optional[int] = None,
    ) -> List[LogEntry]:
        return self.log_stream.query_logs(
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            min_level=min_level,
            limit=limit,
        )

    def tail_logs(
        self,
        pipeline_id: str,
        *,
        lines: int = 100,
        min_level: Optional[LogLevel] = None,
    ) -> List[LogEntry]:
        return self.log_stream.tail(
            pipeline_id=pipeline_id, lines=lines, min_level=min_level
        )

    def cancel(self, pipeline_id: str) -> bool:
        state = self.state_manager.get_state(pipeline_id)
        if not state or state.status not in (
            PipelineStatus.RUNNING,
            PipelineStatus.PENDING,
            PipelineStatus.PAUSED,
        ):
            return False
        self.scheduler.abort(pipeline_id)
        return True

    def pause(self, pipeline_id: str) -> bool:
        state = self.state_manager.get_state(pipeline_id)
        if not state or state.status != PipelineStatus.RUNNING:
            return False
        self.scheduler.pause(pipeline_id)
        return True

    def resume(self, pipeline_id: str) -> bool:
        state = self.state_manager.get_state(pipeline_id)
        if not state:
            return False
        self.scheduler.resume(pipeline_id)
        return True

    def cleanup(self, pipeline_id: Optional[str] = None) -> None:
        if pipeline_id:
            self.artifact_manager.cleanup_pipeline(pipeline_id)
            self.state_manager.delete_pipeline(pipeline_id)
        else:
            self.artifact_manager.cleanup_expired()
            self.env_manager.cleanup_all()

    def shutdown(self) -> None:
        self.log_stream.close()
        self.env_manager.cleanup_all()

    def __enter__(self) -> "PipelineEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()
