from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from ..models.pipeline import (
    Artifact,
    Condition,
    ConditionOperator,
    FailureStrategy,
    Pipeline,
    Stage,
    Step,
    Variable,
)


class ParserError(Exception):
    def __init__(self, message: str, path: str = ""):
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)


class ValidationError(Exception):
    def __init__(self, message: str, errors: List[str] = None):
        self.errors = errors or []
        super().__init__(message)


class PipelineParser:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []

    @classmethod
    def from_file(cls, file_path: str) -> Pipeline:
        parser = cls()
        return parser.parse_file(file_path)

    @classmethod
    def from_string(cls, content: str, format_type: str = "yaml") -> Pipeline:
        parser = cls()
        return parser.parse_string(content, format_type)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Pipeline:
        parser = cls()
        return parser.parse_dict(data)

    def parse_file(self, file_path: str) -> Pipeline:
        if not os.path.exists(file_path):
            raise ParserError(f"File not found: {file_path}")

        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        if ext in (".yaml", ".yml"):
            return self.parse_string(content, "yaml")
        elif ext == ".json":
            return self.parse_string(content, "json")
        else:
            raise ParserError(f"Unsupported file format: {ext}")

    def parse_string(self, content: str, format_type: str = "yaml") -> Pipeline:
        format_type = format_type.lower()

        if format_type in ("yaml", "yml"):
            data = self._parse_yaml(content)
        elif format_type == "json":
            data = json.loads(content)
        else:
            raise ParserError(f"Unsupported format: {format_type}")

        return self.parse_dict(data)

    def _parse_yaml(self, content: str) -> Dict[str, Any]:
        try:
            import yaml
        except ImportError:
            raise ParserError("PyYAML is required for YAML parsing. Install with: pip install pyyaml")
        try:
            return yaml.safe_load(content) or {}
        except yaml.YAMLError as e:
            raise ParserError(f"YAML parsing error: {str(e)}")

    def parse_dict(self, data: Dict[str, Any]) -> Pipeline:
        self.errors.clear()
        self.warnings.clear()

        if not isinstance(data, dict):
            self.errors.append("Pipeline definition must be a dictionary/object")
            self._raise_if_errors()

        pipeline = self._parse_pipeline(data)

        self._validate_pipeline(pipeline)
        self._raise_if_errors()

        return pipeline

    def _parse_pipeline(self, data: Dict[str, Any]) -> Pipeline:
        name = self._get_required(data, "name", str, "pipeline")
        version = data.get("version", "1.0")
        description = data.get("description")
        triggers = data.get("triggers", ["manual"])
        timeout = data.get("timeout", 86400)
        failure_strategy = self._parse_failure_strategy(
            data.get("failure_strategy", "abort"), "pipeline.failure_strategy"
        )

        variables = self._parse_variables(data.get("variables", []), "pipeline.variables")
        stages = self._parse_stages(data.get("stages", []), name)

        return Pipeline(
            name=name,
            version=version,
            description=description,
            triggers=triggers if isinstance(triggers, list) else [triggers],
            timeout=timeout,
            failure_strategy=failure_strategy,
            variables=variables,
            stages=stages,
        )

    def _parse_stages(self, stages_data: List[Dict], pipeline_name: str) -> List[Stage]:
        if not isinstance(stages_data, list):
            self.errors.append("'stages' must be a list")
            return []

        stages = []
        for idx, stage_data in enumerate(stages_data):
            path = f"pipeline.stages[{idx}]"
            if not isinstance(stage_data, dict):
                self.errors.append(f"{path}: Stage definition must be a dictionary")
                continue
            try:
                stage = self._parse_stage(stage_data, path)
                stages.append(stage)
            except ParserError as e:
                self.errors.append(str(e))

        return stages

    def _parse_stage(self, data: Dict[str, Any], path: str) -> Stage:
        name = self._get_required(data, "name", str, path)
        depends_on = data.get("depends_on", [])
        parallel = data.get("parallel", False)
        failure_strategy = self._parse_failure_strategy(
            data.get("failure_strategy", "abort"), f"{path}.failure_strategy"
        )
        condition = self._parse_condition(data.get("condition"), f"{path}.condition")
        variables = self._parse_variables(data.get("variables", []), f"{path}.variables")
        steps = self._parse_steps(data.get("steps", []), f"{path}", name)

        if not isinstance(depends_on, list):
            depends_on = [str(depends_on)] if depends_on else []

        return Stage(
            name=name,
            depends_on=depends_on,
            parallel=parallel,
            failure_strategy=failure_strategy,
            condition=condition,
            variables=variables,
            steps=steps,
        )

    def _parse_steps(self, steps_data: List[Dict], parent_path: str, stage_name: str) -> List[Step]:
        if not isinstance(steps_data, list):
            self.errors.append(f"{parent_path}: 'steps' must be a list")
            return []

        steps = []
        for idx, step_data in enumerate(steps_data):
            path = f"{parent_path}.steps[{idx}]"
            if not isinstance(step_data, dict):
                self.errors.append(f"{path}: Step definition must be a dictionary")
                continue
            try:
                step = self._parse_step(step_data, path, stage_name)
                steps.append(step)
            except ParserError as e:
                self.errors.append(str(e))

        return steps

    def _parse_step(self, data: Dict[str, Any], path: str, stage_name: str) -> Step:
        name = self._get_required(data, "name", str, path)

        script = data.get("script", [])
        if isinstance(script, str):
            script = [script]
        elif not isinstance(script, list):
            self.errors.append(f"{path}.script: must be a string or list of strings")
            script = []

        image = data.get("image")
        working_dir = data.get("working_dir")
        depends_on = data.get("depends_on", [])
        if not isinstance(depends_on, list):
            depends_on = [str(depends_on)] if depends_on else []

        artifacts_in = data.get("artifacts_in", [])
        if isinstance(artifacts_in, str):
            artifacts_in = [artifacts_in]
        elif not isinstance(artifacts_in, list):
            self.errors.append(f"{path}.artifacts_in: must be a string or list")
            artifacts_in = []

        artifacts_out = self._parse_artifacts_out(
            data.get("artifacts_out", []), f"{path}.artifacts_out"
        )

        variables = self._parse_variables(data.get("variables", []), f"{path}.variables")
        environment = data.get("environment", {})
        if not isinstance(environment, dict):
            self.errors.append(f"{path}.environment: must be a dictionary")
            environment = {}

        timeout = data.get("timeout", 3600)
        retries = data.get("retries", 0)
        retry_delay = data.get("retry_delay", 5)
        failure_strategy = self._parse_failure_strategy(
            data.get("failure_strategy", "abort"), f"{path}.failure_strategy"
        )
        condition = self._parse_condition(data.get("condition"), f"{path}.condition")
        allow_failure = data.get("allow_failure", False)
        parallel = data.get("parallel", False)

        return Step(
            name=name,
            script=script,
            image=image,
            working_dir=working_dir,
            depends_on=depends_on,
            artifacts_in=artifacts_in,
            artifacts_out=artifacts_out,
            variables=variables,
            environment=environment,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            failure_strategy=failure_strategy,
            condition=condition,
            allow_failure=allow_failure,
            parallel=parallel,
        )

    def _parse_artifacts_out(
        self, artifacts_data: List[Dict], path: str
    ) -> List[Artifact]:
        if not isinstance(artifacts_data, list):
            self.errors.append(f"{path}: must be a list")
            return []

        artifacts = []
        for idx, art_data in enumerate(artifacts_data):
            art_path = f"{path}[{idx}]"
            if isinstance(art_data, str):
                artifacts.append(Artifact(name=art_data, source_path=art_data))
            elif isinstance(art_data, dict):
                name = self._get_required(art_data, "name", str, art_path)
                source_path = self._get_required(art_data, "source_path", str, art_path)
                target_path = art_data.get("target_path")
                description = art_data.get("description")
                retention_days = art_data.get("retention_days", 7)
                compressed = art_data.get("compressed", True)
                artifacts.append(
                    Artifact(
                        name=name,
                        source_path=source_path,
                        target_path=target_path,
                        description=description,
                        retention_days=retention_days,
                        compressed=compressed,
                    )
                )
            else:
                self.errors.append(f"{art_path}: must be a string or dictionary")
        return artifacts

    def _parse_variables(self, vars_data: List[Any], path: str) -> List[Variable]:
        if not isinstance(vars_data, list):
            self.errors.append(f"{path}: must be a list")
            return []

        variables = []
        for idx, var_data in enumerate(vars_data):
            var_path = f"{path}[{idx}]"
            if isinstance(var_data, dict):
                key = self._get_required(var_data, "key", str, var_path)
                value = var_data.get("value", "")
                description = var_data.get("description")
                sensitive = var_data.get("sensitive", False)
                scope = var_data.get("scope", "pipeline")
                variables.append(
                    Variable(
                        key=key,
                        value=value,
                        description=description,
                        sensitive=sensitive,
                        scope=scope,
                    )
                )
            elif isinstance(var_data, str) and "=" in var_data:
                key, value = var_data.split("=", 1)
                variables.append(Variable(key=key.strip(), value=value.strip()))
            else:
                self.errors.append(
                    f"{var_path}: invalid variable format, use dict or 'KEY=VALUE'"
                )
        return variables

    def _parse_condition(
        self, condition_data: Any, path: str
    ) -> Optional[Condition]:
        if condition_data is None:
            return None

        if isinstance(condition_data, str):
            return Condition(expression=condition_data)

        if not isinstance(condition_data, dict):
            self.errors.append(f"{path}: must be a string or dictionary")
            return None

        operator_str = condition_data.get("operator", "")
        try:
            operator = ConditionOperator(operator_str) if operator_str else None
        except ValueError:
            self.errors.append(f"{path}.operator: invalid operator '{operator_str}'")
            operator = None

        variable = condition_data.get("variable")
        expected_value = condition_data.get("value", condition_data.get("expected_value"))

        sub_conditions_data = condition_data.get("conditions", [])
        sub_conditions = []
        if sub_conditions_data:
            for idx, sub_data in enumerate(sub_conditions_data):
                sub_cond = self._parse_condition(
                    sub_data, f"{path}.conditions[{idx}]"
                )
                if sub_cond:
                    sub_conditions.append(sub_cond)

        return Condition(
            expression=condition_data.get("expression"),
            operator=operator,
            variable=variable,
            expected_value=expected_value,
            conditions=sub_conditions if sub_conditions else None,
        )

    def _parse_failure_strategy(self, value: str, path: str) -> FailureStrategy:
        try:
            return FailureStrategy(str(value).lower())
        except ValueError:
            self.errors.append(f"{path}: invalid failure strategy '{value}'")
            return FailureStrategy.ABORT

    def _get_required(
        self, data: Dict, key: str, expected_type: type, path: str
    ) -> Any:
        if key not in data:
            self.errors.append(f"{path}: missing required field '{key}'")
            return ""
        value = data[key]
        if not isinstance(value, expected_type):
            self.errors.append(
                f"{path}.{key}: expected {expected_type.__name__}, got {type(value).__name__}"
            )
            return expected_type() if expected_type != str else ""
        return value

    def _validate_pipeline(self, pipeline: Pipeline) -> None:
        if not pipeline.name:
            self.errors.append("pipeline.name cannot be empty")

        if not pipeline.stages:
            self.errors.append("pipeline must have at least one stage")

        stage_names = set()
        for stage in pipeline.stages:
            if stage.name in stage_names:
                self.errors.append(f"duplicate stage name: {stage.name}")
            stage_names.add(stage.name)

            if not stage.steps:
                self.warnings.append(f"stage '{stage.name}' has no steps")

            step_names = set()
            for step in stage.steps:
                full_step_name = f"{stage.name}:{step.name}"
                if step.name in step_names:
                    self.errors.append(f"duplicate step name in stage '{stage.name}': {step.name}")
                step_names.add(step.name)

                if not step.script and not step.image:
                    self.warnings.append(f"step '{full_step_name}' has no script defined")

        for stage in pipeline.stages:
            for dep in stage.depends_on:
                if dep not in stage_names:
                    self.errors.append(
                        f"stage '{stage.name}' depends on non-existent stage '{dep}'"
                    )

        for stage in pipeline.stages:
            for step in stage.steps:
                for dep in step.depends_on:
                    if ":" in dep:
                        dep_stage, dep_step = dep.split(":", 1)
                        if dep_stage not in stage_names:
                            self.errors.append(
                                f"step '{stage.name}:{step.name}' depends on non-existent stage '{dep_stage}'"
                            )
                        else:
                            found_stage = pipeline.get_stage(dep_stage)
                            if found_stage and not found_stage.get_step(dep_step):
                                self.errors.append(
                                    f"step '{stage.name}:{step.name}' depends on non-existent step '{dep}'"
                                )
                    else:
                        if not stage.get_step(dep):
                            self.errors.append(
                                f"step '{stage.name}:{step.name}' depends on non-existent step '{stage.name}:{dep}'"
                            )

        self._validate_no_circular_dependencies(pipeline)

    def _validate_no_circular_dependencies(self, pipeline: Pipeline) -> None:
        from ..models.pipeline import DependencyGraph

        try:
            graph = DependencyGraph(pipeline)
            cycles = graph.detect_cycles()
            if cycles:
                for cycle in cycles:
                    self.errors.append(
                        f"circular dependency detected: {' -> '.join(cycle)}"
                    )
        except ValueError as e:
            self.errors.append(str(e))

    def _raise_if_errors(self) -> None:
        if self.errors:
            raise ValidationError(
                f"Pipeline validation failed with {len(self.errors)} error(s)",
                errors=list(self.errors),
            )
