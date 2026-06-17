from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable


class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL_SUCCESS = "partial_success"


class StepStatus(Enum):
    PENDING = "pending"
    WAITING = "waiting"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class FailureStrategy(Enum):
    ABORT = "abort"
    CONTINUE = "continue"
    RETRY = "retry"
    ROLLBACK = "rollback"


class ConditionOperator(Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    AND = "and"
    OR = "or"
    NOT = "not"
    EXISTS = "exists"


@dataclass
class Variable:
    key: str
    value: Any
    description: Optional[str] = None
    sensitive: bool = False
    scope: str = "pipeline"

    def to_env(self) -> str:
        return str(self.value)


@dataclass
class Condition:
    expression: Optional[str] = None
    operator: Optional[ConditionOperator] = None
    variable: Optional[str] = None
    expected_value: Optional[Any] = None
    conditions: Optional[List["Condition"]] = None

    def evaluate(self, context: Dict[str, Any]) -> bool:
        if self.expression:
            return self._evaluate_expression(self.expression, context)

        if self.operator and self.conditions:
            return self._evaluate_compound(context)

        if self.operator and self.variable is not None:
            return self._evaluate_simple(context)

        return True

    def _evaluate_simple(self, context: Dict[str, Any]) -> bool:
        actual = context.get(self.variable)
        expected = self.expected_value

        try:
            if self.operator == ConditionOperator.EQ:
                return actual == expected
            elif self.operator == ConditionOperator.NE:
                return actual != expected
            elif self.operator == ConditionOperator.GT:
                return float(actual) > float(expected)
            elif self.operator == ConditionOperator.LT:
                return float(actual) < float(expected)
            elif self.operator == ConditionOperator.GTE:
                return float(actual) >= float(expected)
            elif self.operator == ConditionOperator.LTE:
                return float(actual) <= float(expected)
            elif self.operator == ConditionOperator.IN:
                return actual in (expected if isinstance(expected, list) else [expected])
            elif self.operator == ConditionOperator.NOT_IN:
                return actual not in (expected if isinstance(expected, list) else [expected])
            elif self.operator == ConditionOperator.EXISTS:
                return actual is not None
        except (TypeError, ValueError):
            return False
        return True

    def _evaluate_compound(self, context: Dict[str, Any]) -> bool:
        results = [c.evaluate(context) for c in self.conditions]
        if self.operator == ConditionOperator.AND:
            return all(results)
        elif self.operator == ConditionOperator.OR:
            return any(results)
        elif self.operator == ConditionOperator.NOT:
            return not results[0] if results else True
        return True

    @staticmethod
    def _evaluate_expression(expr: str, context: Dict[str, Any]) -> bool:
        try:
            eval_globals = {"__builtins__": {}}
            eval_locals = {**context}
            result = eval(expr, eval_globals, eval_locals)
            return bool(result)
        except Exception:
            return False


@dataclass
class Artifact:
    name: str
    source_path: str
    target_path: Optional[str] = None
    description: Optional[str] = None
    retention_days: int = 7
    compressed: bool = True
    artifact_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    size_bytes: Optional[int] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class Step:
    name: str
    script: List[str] = field(default_factory=list)
    image: Optional[str] = None
    working_dir: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)
    artifacts_in: List[str] = field(default_factory=list)
    artifacts_out: List[Artifact] = field(default_factory=list)
    variables: List[Variable] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    timeout: int = 3600
    retries: int = 0
    retry_delay: int = 5
    failure_strategy: FailureStrategy = FailureStrategy.ABORT
    condition: Optional[Condition] = None
    allow_failure: bool = False
    parallel: bool = False
    step_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: StepStatus = StepStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class Stage:
    name: str
    steps: List[Step] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    variables: List[Variable] = field(default_factory=list)
    failure_strategy: FailureStrategy = FailureStrategy.ABORT
    condition: Optional[Condition] = None
    parallel: bool = False
    stage_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: StepStatus = StepStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def get_step(self, name: str) -> Optional[Step]:
        for step in self.steps:
            if step.name == name:
                return step
        return None

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class Pipeline:
    name: str
    stages: List[Stage] = field(default_factory=list)
    variables: List[Variable] = field(default_factory=list)
    triggers: List[str] = field(default_factory=lambda: ["manual"])
    failure_strategy: FailureStrategy = FailureStrategy.ABORT
    timeout: int = 86400
    version: str = "1.0"
    description: Optional[str] = None
    pipeline_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: PipelineStatus = PipelineStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    created_at: float = field(default_factory=time.time)

    def get_stage(self, name: str) -> Optional[Stage]:
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    def get_step(self, stage_name: str, step_name: str) -> Optional[Step]:
        stage = self.get_stage(stage_name)
        if stage:
            return stage.get_step(step_name)
        return None

    def all_steps(self) -> List[tuple]:
        result = []
        for stage in self.stages:
            for step in stage.steps:
                result.append((stage, step))
        return result

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class StepResult:
    step_id: str
    step_name: str
    stage_name: str
    status: StepStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    output_variables: Dict[str, Any] = field(default_factory=dict)
    produced_artifacts: List[str] = field(default_factory=list)
    log_path: Optional[str] = None
    retry_count: int = 0

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class StageResult:
    stage_id: str
    stage_name: str
    status: StepStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    step_results: List[StepResult] = field(default_factory=list)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.step_results if r.status == StepStatus.SUCCESS)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.step_results if r.status in (StepStatus.FAILED, StepStatus.TIMEOUT))

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.step_results if r.status == StepStatus.SKIPPED)


@dataclass
class PipelineResult:
    pipeline_id: str
    pipeline_name: str
    status: PipelineStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    stage_results: List[StageResult] = field(default_factory=list)
    output_variables: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def total_steps(self) -> int:
        return sum(len(s.step_results) for s in self.stage_results)

    @property
    def success_count(self) -> int:
        return sum(s.success_count for s in self.stage_results)

    @property
    def failed_count(self) -> int:
        return sum(s.failed_count for s in self.stage_results)

    @property
    def skipped_count(self) -> int:
        return sum(s.skipped_count for s in self.stage_results)


class DependencyGraph:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self._graph: Dict[str, Set[str]] = {}
        self._reverse_graph: Dict[str, Set[str]] = {}
        self._node_map: Dict[str, tuple] = {}
        self._build_graph()

    def _build_graph(self) -> None:
        for stage in self.pipeline.stages:
            stage_key = f"stage:{stage.name}"
            self._graph[stage_key] = set()
            self._reverse_graph[stage_key] = set()
            self._node_map[stage_key] = (stage, None)

            for dep in stage.depends_on:
                dep_key = f"stage:{dep}"
                self._graph[dep_key].add(stage_key)
                self._reverse_graph[stage_key].add(dep_key)

            for step in stage.steps:
                step_key = f"step:{stage.name}:{step.name}"
                self._graph[step_key] = set()
                self._reverse_graph[step_key] = set()
                self._node_map[step_key] = (stage, step)

                for dep in step.depends_on:
                    if ":" in dep:
                        dep_parts = dep.split(":")
                        dep_key = f"step:{dep_parts[0]}:{dep_parts[1]}"
                    else:
                        dep_key = f"step:{stage.name}:{dep}"
                    self._graph[dep_key].add(step_key)
                    self._reverse_graph[step_key].add(dep_key)

    def get_dependencies(self, node_key: str) -> Set[str]:
        return self._reverse_graph.get(node_key, set())

    def get_dependents(self, node_key: str) -> Set[str]:
        return self._graph.get(node_key, set())

    def get_node(self, node_key: str) -> Optional[tuple]:
        return self._node_map.get(node_key)

    def all_node_keys(self) -> List[str]:
        return list(self._node_map.keys())

    def topological_sort(self) -> List[str]:
        in_degree = {node: len(self._reverse_graph.get(node, set())) for node in self._node_map}
        queue = [node for node, degree in in_degree.items() if degree == 0]
        result = []
        temp_queue = list(queue)

        while temp_queue:
            current = temp_queue.pop(0)
            result.append(current)
            for neighbor in self._graph.get(current, set()):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    temp_queue.append(neighbor)

        if len(result) != len(self._node_map):
            raise ValueError("Pipeline has circular dependencies")

        return result

    def get_ready_nodes(self, completed_keys: Set[str]) -> List[str]:
        ready = []
        for node in self._node_map:
            if node in completed_keys:
                continue
            deps = self._reverse_graph.get(node, set())
            if deps and deps.issubset(completed_keys):
                ready.append(node)
            elif not deps and node not in completed_keys:
                has_unprocessed_dep = False
                for dep_key, deps_set in self._reverse_graph.items():
                    if node in deps_set and dep_key not in completed_keys:
                        has_unprocessed_dep = True
                        break
                if not has_unprocessed_dep:
                    ready.append(node)
        return ready

    def detect_cycles(self) -> List[List[str]]:
        cycles = []
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {node: WHITE for node in self._node_map}
        path = []

        def dfs(node: str) -> None:
            color[node] = GRAY
            path.append(node)
            for neighbor in self._graph.get(node, set()):
                if color[neighbor] == GRAY:
                    idx = path.index(neighbor)
                    cycles.append(path[idx:] + [neighbor])
                elif color[neighbor] == WHITE:
                    dfs(neighbor)
            path.pop()
            color[node] = BLACK

        for node in self._node_map:
            if color[node] == WHITE:
                dfs(node)

        return cycles
