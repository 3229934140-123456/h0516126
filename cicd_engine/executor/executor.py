from __future__ import annotations

import abc
import os
import platform
import queue
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..logging.log_stream import LogEntry, LogLevel, LogStream


@dataclass
class ExecutionResult:
    success: bool
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    timeout: bool = False
    error_message: Optional[str] = None
    output_variables: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


class OutputVariableParser:
    OUTPUT_PATTERN = re.compile(
        r"^::set-output\s+name=([a-zA-Z_][a-zA-Z0-9_-]*)::(.*)$", re.MULTILINE
    )
    ENV_EXPORT_PATTERN = re.compile(
        r"^::set-env\s+name=([a-zA-Z_][a-zA-Z0-9_-]*)::(.*)$", re.MULTILINE
    )

    @classmethod
    def parse(cls, text: str) -> Tuple[Dict[str, Any], Dict[str, str], str]:
        output_vars: Dict[str, Any] = {}
        env_vars: Dict[str, str] = {}
        remaining_lines: List[str] = []

        for line in text.split("\n"):
            output_match = cls.OUTPUT_PATTERN.match(line)
            if output_match:
                name, value = output_match.groups()
                output_vars[name] = cls._parse_value(value)
                continue

            env_match = cls.ENV_EXPORT_PATTERN.match(line)
            if env_match:
                name, value = env_match.groups()
                env_vars[name] = value
                continue

            remaining_lines.append(line)

        return output_vars, env_vars, "\n".join(remaining_lines)

    @staticmethod
    def _parse_value(value: str) -> Any:
        value = value.strip()
        try:
            if value.lower() == "true":
                return True
            if value.lower() == "false":
                return False
            if value.lower() in ("null", "none", ""):
                return None
            if re.match(r"^-?\d+$", value):
                return int(value)
            if re.match(r"^-?\d+\.\d+$", value):
                return float(value)
            if (value.startswith("[") and value.endswith("]")) or \
               (value.startswith("{") and value.endswith("}")):
                import json
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    pass
        except Exception:
            pass
        return value


class ExecutionEnvironment(abc.ABC):
    def __init__(self,
                 working_dir: Optional[str] = None,
                 log_stream: Optional[LogStream] = None,
                 env_vars: Optional[Dict[str, str]] = None):
        self.working_dir = working_dir or os.getcwd()
        self.log_stream = log_stream
        self.env_vars: Dict[str, str] = dict(env_vars or {})
        self._setup_base_env()

    def _setup_base_env(self) -> None:
        import sys
        self.env_vars.setdefault("PATH", os.environ.get("PATH", ""))
        self.env_vars.setdefault("HOME", os.environ.get("HOME", os.path.expanduser("~")))
        self.env_vars.setdefault("SYSTEM", platform.system())
        self.env_vars.setdefault("PYTHON", sys.executable)
        if platform.system() == "Windows":
            self.env_vars.setdefault("SystemRoot", os.environ.get("SystemRoot", "C:\\Windows"))
            self.env_vars.setdefault("COMSPEC", os.environ.get("COMSPEC", "cmd.exe"))
            self.env_vars.setdefault("PATHEXT", os.environ.get("PATHEXT", ".COM;.EXE;.BAT;.CMD"))

    def add_env(self, key: str, value: str) -> None:
        self.env_vars[key] = str(value)

    def add_envs(self, envs: Dict[str, str]) -> None:
        for k, v in envs.items():
            self.env_vars[k] = str(v)

    @abc.abstractmethod
    def execute(
        self,
        command: str,
        *,
        timeout: int = 3600,
        capture_output: bool = True,
        step_context: Optional[Dict[str, str]] = None,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
    ) -> ExecutionResult: ...

    def execute_commands(
        self,
        commands: List[str],
        *,
        timeout: int = 3600,
        step_context: Optional[Dict[str, str]] = None,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
        stop_on_failure: bool = True,
    ) -> ExecutionResult:
        start_time = time.time()
        combined_stdout: List[str] = []
        combined_stderr: List[str] = []
        combined_output_vars: Dict[str, Any] = {}
        last_exit_code = 0
        last_error: Optional[str] = None

        per_cmd_timeout = max(1, timeout // max(1, len(commands)))

        for idx, cmd in enumerate(commands):
            ctx = dict(step_context or {})
            ctx["command_index"] = idx

            result = self.execute(
                cmd,
                timeout=per_cmd_timeout,
                capture_output=True,
                step_context=ctx,
                on_stdout=on_stdout,
                on_stderr=on_stderr,
            )

            combined_stdout.append(result.stdout)
            combined_stderr.append(result.stderr)
            combined_output_vars.update(result.output_variables)
            self.add_envs(result.output_variables.get("__env_export__", {}))

            if not result.success:
                last_exit_code = result.exit_code
                last_error = result.error_message or f"Command failed: {cmd}"
                if result.timeout:
                    last_error = f"Command timed out after {per_cmd_timeout}s: {cmd}"
                if stop_on_failure:
                    break

        end_time = time.time()

        return ExecutionResult(
            success=last_exit_code == 0 and last_error is None,
            exit_code=last_exit_code,
            stdout="\n".join(combined_stdout),
            stderr="\n".join(combined_stderr),
            start_time=start_time,
            end_time=end_time,
            timeout=False,
            error_message=last_error,
            output_variables=combined_output_vars,
        )

    def _resolve_variables(self, command: str) -> str:
        def replace_var(match):
            var_name = match.group(1) or match.group(2)
            return self.env_vars.get(var_name, match.group(0))

        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
        return pattern.sub(replace_var, command)

    def _log(self, message: str, level: LogLevel, context: Optional[Dict[str, str]] = None) -> None:
        if self.log_stream:
            kwargs = dict(message=message, level=level)
            if context:
                kwargs.update({
                    "pipeline_id": context.get("pipeline_id"),
                    "stage_name": context.get("stage_name"),
                    "step_name": context.get("step_name"),
                    "command_index": context.get("command_index") if isinstance(context.get("command_index"), int) else None,
                })
            self.log_stream.log(**kwargs)

    def setup(self) -> None:
        pass

    def cleanup(self) -> None:
        pass


class SubprocessEnvironment(ExecutionEnvironment):
    def __init__(self,
                 working_dir: Optional[str] = None,
                 log_stream: Optional[LogStream] = None,
                 env_vars: Optional[Dict[str, str]] = None,
                 shell: Optional[str] = None):
        super().__init__(working_dir, log_stream, env_vars)
        self.shell = shell or self._default_shell()
        self._process: Optional[subprocess.Popen] = None
        self._process_lock = threading.Lock()

    @staticmethod
    def _default_shell() -> str:
        system = platform.system()
        if system == "Windows":
            return os.environ.get("COMSPEC", "cmd.exe")
        else:
            return os.environ.get("SHELL", "/bin/bash")

    def _build_command_args(self, command: str) -> Tuple[List[str], bool]:
        system = platform.system()
        if system == "Windows":
            if "cmd.exe" in self.shell.lower():
                return [self.shell, "/C", command], True
            elif "powershell" in self.shell.lower():
                return [self.shell, "-Command", command], True
            else:
                return [self.shell, "-c", command], False
        else:
            return [self.shell, "-c", command], False

    def execute(
        self,
        command: str,
        *,
        timeout: int = 3600,
        capture_output: bool = True,
        step_context: Optional[Dict[str, str]] = None,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
    ) -> ExecutionResult:
        start_time = time.time()
        resolved_cmd = self._resolve_variables(command)

        self._log(f"$ {resolved_cmd}", LogLevel.INFO, step_context)

        cmd_args, use_shell = self._build_command_args(resolved_cmd)

        stdout_queue: "queue.Queue[str]" = queue.Queue()
        stderr_queue: "queue.Queue[str]" = queue.Queue()
        stdout_lines: List[str] = []
        stderr_lines: List[str] = []

        process = None
        timed_out = False

        def _stream_output(stream, q, lines_list, callback, log_level):
            try:
                for raw_line in iter(stream.readline, ""):
                    if raw_line is None:
                        break
                    line = raw_line.rstrip("\n").rstrip("\r")
                    lines_list.append(line)
                    q.put(line)
                    if callback:
                        try:
                            callback(line)
                        except Exception:
                            pass
                    self._log(line, log_level, step_context)
            except Exception:
                pass
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        try:
            process = subprocess.Popen(
                cmd_args if not use_shell else resolved_cmd,
                shell=use_shell,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                env={**os.environ, **self.env_vars},
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            with self._process_lock:
                self._process = process

            threads = []
            if process.stdout:
                t_out = threading.Thread(
                    target=_stream_output,
                    args=(process.stdout, stdout_queue, stdout_lines, on_stdout, LogLevel.STDOUT),
                    daemon=True,
                )
                threads.append(t_out)
                t_out.start()

            if process.stderr:
                t_err = threading.Thread(
                    target=_stream_output,
                    args=(process.stderr, stderr_queue, stderr_lines, on_stderr, LogLevel.STDERR),
                    daemon=True,
                )
                threads.append(t_err)
                t_err.start()

            try:
                exit_code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                self._kill_process(process)
                exit_code = -1
                timeout_msg = f"Process timed out after {timeout} seconds"
                stderr_lines.append(timeout_msg)
                self._log(timeout_msg, LogLevel.ERROR, step_context)

            for t in threads:
                t.join(timeout=2)

        except FileNotFoundError as e:
            exit_code = 127
            stderr_lines.append(f"Command not found: {e}")
            self._log(f"Command not found: {e}", LogLevel.ERROR, step_context)
        except PermissionError as e:
            exit_code = 126
            stderr_lines.append(f"Permission denied: {e}")
            self._log(f"Permission denied: {e}", LogLevel.ERROR, step_context)
        except Exception as e:
            exit_code = 1
            stderr_lines.append(f"Execution error: {str(e)}")
            self._log(f"Execution error: {str(e)}", LogLevel.ERROR, step_context)
        finally:
            with self._process_lock:
                self._process = None

        end_time = time.time()

        stdout_text = "\n".join(stdout_lines)
        stderr_text = "\n".join(stderr_lines)

        output_vars, env_exports, _ = OutputVariableParser.parse(stdout_text)
        _, _, stderr_clean = OutputVariableParser.parse(stderr_text)
        if env_exports:
            output_vars["__env_export__"] = env_exports
            self.add_envs(env_exports)

        success = exit_code == 0 and not timed_out

        return ExecutionResult(
            success=success,
            exit_code=exit_code,
            stdout=stdout_text,
            stderr=stderr_clean,
            start_time=start_time,
            end_time=end_time,
            timeout=timed_out,
            error_message=None if success else (stderr_text.strip().split("\n")[-1] if stderr_text.strip() else f"Exit code {exit_code}"),
            output_variables=output_vars,
        )

    def _kill_process(self, process: subprocess.Popen) -> None:
        try:
            if platform.system() == "Windows":
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                        capture_output=True,
                    )
            else:
                pgid = os.getpgid(process.pid) if hasattr(os, "getpgid") else process.pid
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except (ProcessLookupError, AttributeError, PermissionError):
                    process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except (ProcessLookupError, AttributeError, PermissionError):
                        process.kill()
        except Exception:
            try:
                process.kill()
            except Exception:
                pass

    def cancel(self) -> None:
        with self._process_lock:
            if self._process and self._process.poll() is None:
                self._kill_process(self._process)


class DockerEnvironment(ExecutionEnvironment):
    DOCKER_AVAILABLE: Optional[bool] = None

    def __init__(self,
                 image: str,
                 working_dir: Optional[str] = None,
                 container_name: Optional[str] = None,
                 log_stream: Optional[LogStream] = None,
                 env_vars: Optional[Dict[str, str]] = None,
                 volumes: Optional[List[str]] = None,
                 network: Optional[str] = None,
                 auto_remove: bool = True):
        super().__init__(working_dir, log_stream, env_vars)
        self.image = image
        self.container_name = container_name or f"cicd-{int(time.time())}-{os.getpid()}"
        self.volumes = list(volumes or [])
        self.network = network
        self.auto_remove = auto_remove
        self._container_id: Optional[str] = None
        self._subprocess_env = SubprocessEnvironment(
            working_dir=working_dir, log_stream=log_stream, env_vars={}
        )

    @classmethod
    def is_available(cls) -> bool:
        if cls.DOCKER_AVAILABLE is not None:
            return cls.DOCKER_AVAILABLE
        try:
            result = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            cls.DOCKER_AVAILABLE = result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            cls.DOCKER_AVAILABLE = False
        return cls.DOCKER_AVAILABLE

    def setup(self) -> None:
        if not self.is_available():
            raise RuntimeError("Docker is not available on this system")

        pull_cmd = f"docker pull {shlex.quote(self.image)}"
        result = self._subprocess_env.execute(pull_cmd, timeout=600)
        if not result.success:
            self._log(f"Warning: Failed to pull image {self.image}: {result.stderr}", LogLevel.WARN)

    def execute(
        self,
        command: str,
        *,
        timeout: int = 3600,
        capture_output: bool = True,
        step_context: Optional[Dict[str, str]] = None,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
    ) -> ExecutionResult:
        resolved_cmd = self._resolve_variables(command)

        docker_args = ["docker", "run", "--rm" if self.auto_remove else ""]
        docker_args = [a for a in docker_args if a]

        for k, v in self.env_vars.items():
            docker_args.extend(["-e", f"{k}={v}"])

        for vol in self.volumes:
            docker_args.extend(["-v", vol])

        if self.working_dir:
            docker_args.extend(["-w", "/workspace"])
            docker_args.extend(["-v", f"{os.path.abspath(self.working_dir)}:/workspace"])

        if self.network:
            docker_args.extend(["--network", self.network])

        docker_args.extend([self.image, "sh", "-c", resolved_cmd])

        docker_cmd_str = " ".join(shlex.quote(a) for a in docker_args)

        return self._subprocess_env.execute(
            docker_cmd_str,
            timeout=timeout,
            capture_output=capture_output,
            step_context=step_context,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    def cleanup(self) -> None:
        if self._container_id and not self.auto_remove:
            try:
                subprocess.run(
                    ["docker", "rm", "-f", self._container_id],
                    capture_output=True,
                    timeout=10,
                )
            except Exception:
                pass
            self._container_id = None


class EnvironmentManager:
    def __init__(self, base_dir: Optional[str] = None, log_stream: Optional[LogStream] = None):
        self.base_dir = base_dir or os.path.join(os.getcwd(), ".cicd_workspace")
        self.log_stream = log_stream
        self._environments: Dict[str, ExecutionEnvironment] = {}

    def _get_step_workdir(self, pipeline_id: str, stage_name: str, step_name: str) -> str:
        path = os.path.join(self.base_dir, pipeline_id, stage_name, step_name)
        os.makedirs(path, exist_ok=True)
        return path

    def create_environment(
        self,
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        image: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        working_dir: Optional[str] = None,
    ) -> ExecutionEnvironment:
        key = f"{pipeline_id}:{stage_name}:{step_name}"
        workdir = working_dir or self._get_step_workdir(pipeline_id, stage_name, step_name)

        if image and DockerEnvironment.is_available():
            env = DockerEnvironment(
                image=image,
                working_dir=workdir,
                log_stream=self.log_stream,
                env_vars=env_vars,
            )
        else:
            if image and self.log_stream:
                self.log_stream.warn(
                    f"Docker not available, using subprocess for image '{image}'",
                    pipeline_id=pipeline_id,
                    stage_name=stage_name,
                    step_name=step_name,
                )
            env = SubprocessEnvironment(
                working_dir=workdir,
                log_stream=self.log_stream,
                env_vars=env_vars,
            )

        self._environments[key] = env
        return env

    def get_environment(self, pipeline_id: str, stage_name: str, step_name: str) -> Optional[ExecutionEnvironment]:
        key = f"{pipeline_id}:{stage_name}:{step_name}"
        return self._environments.get(key)

    def cleanup_environment(self, pipeline_id: str, stage_name: str, step_name: str) -> None:
        key = f"{pipeline_id}:{stage_name}:{step_name}"
        env = self._environments.pop(key, None)
        if env:
            try:
                env.cleanup()
            except Exception:
                pass

    def cleanup_all(self) -> None:
        for key in list(self._environments.keys()):
            env = self._environments.pop(key)
            try:
                env.cleanup()
            except Exception:
                pass


class ExecutorFactory:
    @staticmethod
    def create(
        *,
        executor_type: str = "auto",
        image: Optional[str] = None,
        **kwargs,
    ) -> ExecutionEnvironment:
        executor_type = executor_type.lower()

        if executor_type == "docker" or (executor_type == "auto" and image):
            if executor_type == "docker" or DockerEnvironment.is_available():
                return DockerEnvironment(image=image, **kwargs)
            if executor_type == "docker":
                raise RuntimeError("Docker requested but not available")

        return SubprocessEnvironment(**kwargs)
