from __future__ import annotations

import abc
import json
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set

from ..models.pipeline import (
    Pipeline,
    PipelineResult,
    PipelineStatus,
    Stage,
    StageResult,
    Step,
    StepResult,
    StepStatus,
)


class StatusAggregator:
    @staticmethod
    def aggregate_step_statuses(statuses: List[StepStatus]) -> StepStatus:
        if not statuses:
            return StepStatus.PENDING
        if any(s == StepStatus.RUNNING for s in statuses):
            return StepStatus.RUNNING
        if any(s == StepStatus.WAITING for s in statuses):
            return StepStatus.WAITING
        if all(s == StepStatus.SKIPPED for s in statuses):
            return StepStatus.SKIPPED
        if all(s in (StepStatus.SUCCESS, StepStatus.SKIPPED) for s in statuses):
            return StepStatus.SUCCESS
        has_failure = any(
            s in (StepStatus.FAILED, StepStatus.TIMEOUT, StepStatus.CANCELLED)
            for s in statuses
        )
        has_success = any(s == StepStatus.SUCCESS for s in statuses)
        if has_failure:
            return StepStatus.FAILED
        if has_success or all(s == StepStatus.SKIPPED for s in statuses):
            return StepStatus.SUCCESS
        if all(s == StepStatus.PENDING for s in statuses):
            return StepStatus.PENDING
        return StepStatus.PENDING

    @staticmethod
    def step_to_pipeline_status(step_status: StepStatus) -> PipelineStatus:
        mapping = {
            StepStatus.PENDING: PipelineStatus.PENDING,
            StepStatus.WAITING: PipelineStatus.RUNNING,
            StepStatus.RUNNING: PipelineStatus.RUNNING,
            StepStatus.SUCCESS: PipelineStatus.SUCCESS,
            StepStatus.FAILED: PipelineStatus.FAILED,
            StepStatus.SKIPPED: PipelineStatus.SUCCESS,
            StepStatus.CANCELLED: PipelineStatus.CANCELLED,
            StepStatus.TIMEOUT: PipelineStatus.FAILED,
        }
        return mapping.get(step_status, PipelineStatus.PENDING)

    @staticmethod
    def aggregate_pipeline_status(stage_statuses: List[StepStatus]) -> PipelineStatus:
        if not stage_statuses:
            return PipelineStatus.PENDING
        if any(s == StepStatus.RUNNING for s in stage_statuses):
            return PipelineStatus.RUNNING
        if any(s == StepStatus.WAITING for s in stage_statuses):
            return PipelineStatus.RUNNING
        all_skipped = all(s == StepStatus.SKIPPED for s in stage_statuses)
        all_success_or_skipped = all(
            s in (StepStatus.SUCCESS, StepStatus.SKIPPED) for s in stage_statuses
        )
        if all_skipped:
            return PipelineStatus.SUCCESS
        if all_success_or_skipped:
            return PipelineStatus.SUCCESS
        has_failure = any(
            s in (StepStatus.FAILED, StepStatus.TIMEOUT) for s in stage_statuses
        )
        has_success = any(s == StepStatus.SUCCESS for s in stage_statuses)
        has_cancelled = any(s == StepStatus.CANCELLED for s in stage_statuses)
        if has_cancelled:
            return PipelineStatus.CANCELLED
        if has_failure and has_success:
            return PipelineStatus.PARTIAL_SUCCESS
        if has_failure:
            return PipelineStatus.FAILED
        if all(s == StepStatus.PENDING for s in stage_statuses):
            return PipelineStatus.PENDING
        return PipelineStatus.PENDING


@dataclass
class PipelineExecutionState:
    pipeline_id: str
    pipeline_name: str
    pipeline_version: str
    status: PipelineStatus = PipelineStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    stage_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    step_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    output_variables: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        if self.start_time:
            return time.time() - self.start_time
        return None

    @property
    def progress(self) -> float:
        total = len(self.step_states)
        if total == 0:
            return 0.0
        completed = sum(
            1
            for s in self.step_states.values()
            if s.get("status")
            in (
                StepStatus.SUCCESS.value,
                StepStatus.FAILED.value,
                StepStatus.SKIPPED.value,
                StepStatus.TIMEOUT.value,
                StepStatus.CANCELLED.value,
            )
        )
        return round(completed / total * 100, 1)

    def init_from_pipeline(self, pipeline: Pipeline) -> None:
        for stage in pipeline.stages:
            self.stage_states[stage.name] = {
                "stage_id": stage.stage_id,
                "name": stage.name,
                "status": StepStatus.PENDING.value,
                "start_time": None,
                "end_time": None,
            }
            for step in stage.steps:
                step_key = f"{stage.name}:{step.name}"
                self.step_states[step_key] = {
                    "step_id": step.step_id,
                    "stage_name": stage.name,
                    "name": step.name,
                    "status": StepStatus.PENDING.value,
                    "start_time": None,
                    "end_time": None,
                    "exit_code": None,
                    "retry_count": 0,
                    "error_message": None,
                }

    def update_step_status(
        self,
        stage_name: str,
        step_name: str,
        status: StepStatus,
        **kwargs,
    ) -> None:
        key = f"{stage_name}:{step_name}"
        if key not in self.step_states:
            self.step_states[key] = {
                "stage_name": stage_name,
                "name": step_name,
            }
        self.step_states[key]["status"] = status.value
        self.step_states[key].update(kwargs)
        self.step_states[key]["updated_at"] = time.time()
        self.updated_at = time.time()

    def get_step_status(self, stage_name: str, step_name: str) -> Optional[StepStatus]:
        key = f"{stage_name}:{step_name}"
        state = self.step_states.get(key)
        if state and "status" in state:
            try:
                return StepStatus(state["status"])
            except ValueError:
                return None
        return None

    def update_stage_status(
        self,
        stage_name: str,
        status: StepStatus,
        **kwargs,
    ) -> None:
        if stage_name not in self.stage_states:
            self.stage_states[stage_name] = {"name": stage_name}
        self.stage_states[stage_name]["status"] = status.value
        self.stage_states[stage_name].update(kwargs)
        self.stage_states[stage_name]["updated_at"] = time.time()
        self.updated_at = time.time()

    def get_stage_status(self, stage_name: str) -> Optional[StepStatus]:
        state = self.stage_states.get(stage_name)
        if state and "status" in state:
            try:
                return StepStatus(state["status"])
            except ValueError:
                return None
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "stage_states": self.stage_states,
            "step_states": self.step_states,
            "variables": self.variables,
            "output_variables": self.output_variables,
            "error_message": self.error_message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "duration": self.duration,
            "progress": self.progress,
        }

    def to_json(self, pretty: bool = True) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=2 if pretty else None,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineExecutionState":
        state = cls(
            pipeline_id=data["pipeline_id"],
            pipeline_name=data["pipeline_name"],
            pipeline_version=data.get("pipeline_version", "1.0"),
        )
        try:
            state.status = PipelineStatus(data.get("status", "pending"))
        except ValueError:
            state.status = PipelineStatus.PENDING
        state.start_time = data.get("start_time")
        state.end_time = data.get("end_time")
        state.stage_states = data.get("stage_states", {})
        state.step_states = data.get("step_states", {})
        state.variables = data.get("variables", {})
        state.output_variables = data.get("output_variables", {})
        state.error_message = data.get("error_message")
        state.created_at = data.get("created_at", time.time())
        state.updated_at = data.get("updated_at", time.time())
        return state

    def aggregate_from_steps(self) -> None:
        stage_step_map: Dict[str, List[StepStatus]] = {}
        for key, step_state in self.step_states.items():
            stage_name = step_state.get("stage_name") or key.split(":")[0]
            if stage_name not in stage_step_map:
                stage_step_map[stage_name] = []
            try:
                status = StepStatus(step_state.get("status", "pending"))
                stage_step_map[stage_name].append(status)
            except ValueError:
                pass

        all_stage_statuses: List[StepStatus] = []
        for stage_name, step_statuses in stage_step_map.items():
            aggregated = StatusAggregator.aggregate_step_statuses(step_statuses)
            all_stage_statuses.append(aggregated)
            stage_state = self.stage_states.get(stage_name, {})
            if stage_state:
                stage_state["status"] = aggregated.value

        self.status = StatusAggregator.aggregate_pipeline_status(all_stage_statuses)

    def to_pipeline_result(self) -> PipelineResult:
        stage_results_map: Dict[str, StageResult] = {}
        for stage_name, stage_state in self.stage_states.items():
            stage_results_map[stage_name] = StageResult(
                stage_id=stage_state.get("stage_id", ""),
                stage_name=stage_name,
                status=self.get_stage_status(stage_name) or StepStatus.PENDING,
                start_time=stage_state.get("start_time"),
                end_time=stage_state.get("end_time"),
                step_results=[],
            )

        for key, step_state in self.step_states.items():
            stage_name = step_state.get("stage_name") or key.split(":")[0]
            step_result = StepResult(
                step_id=step_state.get("step_id", ""),
                step_name=step_state.get("name", key.split(":")[-1]),
                stage_name=stage_name,
                status=StepStatus(step_state.get("status", "pending")),
                start_time=step_state.get("start_time"),
                end_time=step_state.get("end_time"),
                exit_code=step_state.get("exit_code"),
                error_message=step_state.get("error_message"),
                retry_count=step_state.get("retry_count", 0),
            )
            if stage_name in stage_results_map:
                stage_results_map[stage_name].step_results.append(step_result)

        return PipelineResult(
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            status=self.status,
            start_time=self.start_time,
            end_time=self.end_time,
            stage_results=list(stage_results_map.values()),
            output_variables=dict(self.output_variables),
            error_message=self.error_message,
        )


class StateStore(abc.ABC):
    @abc.abstractmethod
    def save(self, state: PipelineExecutionState) -> None: ...

    @abc.abstractmethod
    def load(self, pipeline_id: str) -> Optional[PipelineExecutionState]: ...

    @abc.abstractmethod
    def delete(self, pipeline_id: str) -> bool: ...

    @abc.abstractmethod
    def list_ids(self) -> List[str]: ...

    def list(self, limit: Optional[int] = None, offset: int = 0) -> List[PipelineExecutionState]:
        results = []
        ids = self.list_ids()
        for pid in ids:
            state = self.load(pid)
            if state:
                results.append(state)
        results.sort(key=lambda s: s.created_at, reverse=True)
        if offset > 0:
            results = results[offset:]
        if limit:
            results = results[:limit]
        return results


class MemoryStateStore(StateStore):
    def __init__(self):
        self._states: Dict[str, PipelineExecutionState] = {}
        self._lock = threading.RLock()

    def save(self, state: PipelineExecutionState) -> None:
        with self._lock:
            self._states[state.pipeline_id] = state

    def load(self, pipeline_id: str) -> Optional[PipelineExecutionState]:
        with self._lock:
            return self._states.get(pipeline_id)

    def delete(self, pipeline_id: str) -> bool:
        with self._lock:
            return self._states.pop(pipeline_id, None) is not None

    def list_ids(self) -> List[str]:
        with self._lock:
            return list(self._states.keys())


class FileStateStore(StateStore):
    def __init__(self, base_dir: str = ".cicd_state"):
        self.base_dir = os.path.abspath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        self._lock = threading.RLock()

    def _get_file_path(self, pipeline_id: str) -> str:
        safe_id = pipeline_id.replace("/", "_").replace("\\", "_")
        return os.path.join(self.base_dir, f"{safe_id}.json")

    def save(self, state: PipelineExecutionState) -> None:
        file_path = self._get_file_path(state.pipeline_id)
        data = state.to_dict()
        with self._lock:
            tmp_path = file_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, file_path)

    def load(self, pipeline_id: str) -> Optional[PipelineExecutionState]:
        file_path = self._get_file_path(pipeline_id)
        if not os.path.exists(file_path):
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return PipelineExecutionState.from_dict(data)
        except (json.JSONDecodeError, OSError, KeyError):
            return None

    def delete(self, pipeline_id: str) -> bool:
        file_path = self._get_file_path(pipeline_id)
        with self._lock:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    return True
                except OSError:
                    return False
            return False

    def list_ids(self) -> List[str]:
        ids = []
        if not os.path.exists(self.base_dir):
            return ids
        for fname in os.listdir(self.base_dir):
            if fname.endswith(".json"):
                ids.append(fname[:-5])
        return ids


class StateManager:
    def __init__(self, store: Optional[StateStore] = None, auto_persist: bool = True):
        self.store = store or FileStateStore()
        self.auto_persist = auto_persist
        self._states: Dict[str, PipelineExecutionState] = {}
        self._lock = threading.RLock()
        self._dirty: Set[str] = set()

    def create_state(self, pipeline: Pipeline) -> PipelineExecutionState:
        state = PipelineExecutionState(
            pipeline_id=pipeline.pipeline_id,
            pipeline_name=pipeline.name,
            pipeline_version=pipeline.version,
        )
        state.init_from_pipeline(pipeline)
        var_map = {v.key: v.value for v in pipeline.variables}
        state.variables.update(var_map)
        with self._lock:
            self._states[pipeline.pipeline_id] = state
        if self.auto_persist:
            self._save_state(state)
        return state

    def get_state(self, pipeline_id: str) -> Optional[PipelineExecutionState]:
        with self._lock:
            if pipeline_id in self._states:
                return self._states[pipeline_id]
        state = self.store.load(pipeline_id)
        if state:
            with self._lock:
                self._states[pipeline_id] = state
        return state

    def update_step(
        self,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        status: StepStatus,
        **kwargs,
    ) -> PipelineExecutionState:
        state = self.get_or_create(pipeline_id)
        state.update_step_status(stage_name, step_name, status, **kwargs)
        state.aggregate_from_steps()
        self._mark_dirty(pipeline_id)
        if self.auto_persist:
            self._save_state(state)
        return state

    def update_stage(
        self,
        pipeline_id: str,
        stage_name: str,
        status: StepStatus,
        **kwargs,
    ) -> PipelineExecutionState:
        state = self.get_or_create(pipeline_id)
        state.update_stage_status(stage_name, status, **kwargs)
        state.aggregate_from_steps()
        self._mark_dirty(pipeline_id)
        if self.auto_persist:
            self._save_state(state)
        return state

    def update_pipeline(
        self,
        pipeline_id: str,
        status: PipelineStatus,
        **kwargs,
    ) -> PipelineExecutionState:
        state = self.get_or_create(pipeline_id)
        state.status = status
        state.updated_at = time.time()
        if "start_time" in kwargs:
            state.start_time = kwargs.pop("start_time")
        if "end_time" in kwargs:
            state.end_time = kwargs.pop("end_time")
        if "error_message" in kwargs:
            state.error_message = kwargs.pop("error_message")
        if "variables" in kwargs:
            state.variables.update(kwargs.pop("variables"))
        if "output_variables" in kwargs:
            state.output_variables.update(kwargs.pop("output_variables"))
        for k, v in kwargs.items():
            setattr(state, k, v)
        self._mark_dirty(pipeline_id)
        if self.auto_persist:
            self._save_state(state)
        return state

    def get_or_create(self, pipeline_id: str) -> PipelineExecutionState:
        state = self.get_state(pipeline_id)
        if state is None:
            state = PipelineExecutionState(
                pipeline_id=pipeline_id,
                pipeline_name="Unknown",
                pipeline_version="1.0",
            )
            with self._lock:
                self._states[pipeline_id] = state
        return state

    def set_variable(
        self, pipeline_id: str, key: str, value: Any, output: bool = False
    ) -> None:
        state = self.get_or_create(pipeline_id)
        if output:
            state.output_variables[key] = value
        else:
            state.variables[key] = value
        state.updated_at = time.time()
        self._mark_dirty(pipeline_id)
        if self.auto_persist:
            self._save_state(state)

    def get_variable(self, pipeline_id: str, key: str, default: Any = None) -> Any:
        state = self.get_state(pipeline_id)
        if state:
            if key in state.output_variables:
                return state.output_variables[key]
            if key in state.variables:
                return state.variables[key]
        return default

    def get_all_variables(self, pipeline_id: str) -> Dict[str, Any]:
        state = self.get_state(pipeline_id)
        if not state:
            return {}
        vars_map = dict(state.variables)
        vars_map.update(state.output_variables)
        return vars_map

    def persist(self, pipeline_id: Optional[str] = None) -> None:
        if pipeline_id:
            state = self.get_state(pipeline_id)
            if state:
                self._save_state(state)
                with self._lock:
                    self._dirty.discard(pipeline_id)
        else:
            with self._lock:
                dirty_ids = list(self._dirty)
            for pid in dirty_ids:
                state = self.get_state(pid)
                if state:
                    self._save_state(state)
            with self._lock:
                self._dirty.clear()

    def _save_state(self, state: PipelineExecutionState) -> None:
        try:
            self.store.save(state)
        except Exception as e:
            import sys
            print(f"Error persisting state for {state.pipeline_id}: {e}", file=sys.stderr)

    def _mark_dirty(self, pipeline_id: str) -> None:
        with self._lock:
            self._dirty.add(pipeline_id)

    def list_pipelines(
        self, limit: Optional[int] = None, offset: int = 0
    ) -> List[PipelineExecutionState]:
        results = self.store.list(limit=limit, offset=offset)
        for s in results:
            if s.pipeline_id not in self._states:
                with self._lock:
                    self._states[s.pipeline_id] = s
        return results

    def delete_pipeline(self, pipeline_id: str) -> bool:
        with self._lock:
            self._states.pop(pipeline_id, None)
            self._dirty.discard(pipeline_id)
        return self.store.delete(pipeline_id)

    def get_result(self, pipeline_id: str) -> Optional[PipelineResult]:
        state = self.get_state(pipeline_id)
        if not state:
            return None
        return state.to_pipeline_result()

    def is_completed(self, pipeline_id: str) -> bool:
        state = self.get_state(pipeline_id)
        if not state:
            return False
        return state.status in (
            PipelineStatus.SUCCESS,
            PipelineStatus.FAILED,
            PipelineStatus.CANCELLED,
            PipelineStatus.PARTIAL_SUCCESS,
        )

    def wait_for_completion(
        self,
        pipeline_id: str,
        timeout: Optional[float] = None,
        poll_interval: float = 0.5,
    ) -> Optional[PipelineResult]:
        start = time.time()
        while True:
            if self.is_completed(pipeline_id):
                return self.get_result(pipeline_id)
            if timeout and (time.time() - start) > timeout:
                return None
            time.sleep(poll_interval)
