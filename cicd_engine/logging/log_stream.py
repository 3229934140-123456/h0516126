from __future__ import annotations

import abc
import json
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class LogLevel(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    STDOUT = "stdout"
    STDERR = "stderr"

    @property
    def severity(self) -> int:
        _severity_map = {
            "debug": 10,
            "info": 20,
            "warn": 30,
            "warning": 30,
            "error": 40,
            "critical": 50,
            "stdout": 25,
            "stderr": 35,
        }
        return _severity_map.get(self.value, 20)


@dataclass
class LogEntry:
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: float = field(default_factory=time.time)
    level: LogLevel = LogLevel.INFO
    pipeline_id: Optional[str] = None
    stage_name: Optional[str] = None
    step_name: Optional[str] = None
    command_index: Optional[int] = None
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    stream_source: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "log_id": self.log_id,
            "timestamp": self.timestamp,
            "level": self.level.value,
            "pipeline_id": self.pipeline_id,
            "stage_name": self.stage_name,
            "step_name": self.step_name,
            "command_index": self.command_index,
            "message": self.message,
            "metadata": self.metadata,
            "stream_source": self.stream_source,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(
            log_id=data.get("log_id", uuid.uuid4().hex),
            timestamp=data.get("timestamp", time.time()),
            level=LogLevel(data.get("level", "info")),
            pipeline_id=data.get("pipeline_id"),
            stage_name=data.get("stage_name"),
            step_name=data.get("step_name"),
            command_index=data.get("command_index"),
            message=data.get("message", ""),
            metadata=data.get("metadata", {}),
            stream_source=data.get("stream_source"),
        )

    def format(self, include_timestamp: bool = True, include_level: bool = True) -> str:
        parts = []
        if include_timestamp:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
            parts.append(f"[{ts}]")
        if include_level:
            parts.append(f"[{self.level.value.upper()}]")
        context_parts = []
        if self.stage_name:
            context_parts.append(self.stage_name)
        if self.step_name:
            context_parts.append(self.step_name)
        if context_parts:
            parts.append(f"[{'/'.join(context_parts)}]")
        parts.append(self.message)
        return " ".join(parts)


class LogSubscriber(abc.ABC):
    @abc.abstractmethod
    def on_log(self, entry: LogEntry) -> None: ...

    def on_close(self) -> None:
        pass


class CallbackSubscriber(LogSubscriber):
    def __init__(self, callback: Callable[[LogEntry], None],
                 min_level: LogLevel = LogLevel.DEBUG):
        self._callback = callback
        self._min_level = min_level

    def on_log(self, entry: LogEntry) -> None:
        if entry.level.severity >= self._min_level.severity:
            try:
                self._callback(entry)
            except Exception:
                pass


class LogStore(abc.ABC):
    @abc.abstractmethod
    def save(self, entry: LogEntry) -> None: ...

    @abc.abstractmethod
    def query(
        self,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        min_level: Optional[LogLevel] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[LogEntry]: ...

    def close(self) -> None:
        pass


class InMemoryLogStore(LogStore):
    def __init__(self, max_entries: int = 100000):
        self._entries: deque[LogEntry] = deque(maxlen=max_entries)
        self._lock = threading.RLock()

    def save(self, entry: LogEntry) -> None:
        with self._lock:
            self._entries.append(entry)

    def query(
        self,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        min_level: Optional[LogLevel] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[LogEntry]:
        results: List[LogEntry] = []
        min_severity = min_level.severity if min_level else 0

        with self._lock:
            entries = list(self._entries)

        for entry in entries:
            if pipeline_id and entry.pipeline_id != pipeline_id:
                continue
            if stage_name and entry.stage_name != stage_name:
                continue
            if step_name and entry.step_name != step_name:
                continue
            if min_level and entry.level.severity < min_severity:
                continue
            if start_time and entry.timestamp < start_time:
                continue
            if end_time and entry.timestamp > end_time:
                continue
            results.append(entry)

        results.sort(key=lambda e: e.timestamp)

        if offset > 0:
            results = results[offset:]
        if limit:
            results = results[:limit]

        return results


class FileLogStore(LogStore):
    def __init__(self, base_dir: str = "logs",
                 split_by_pipeline: bool = True,
                 split_by_step: bool = False):
        self.base_dir = base_dir
        self.split_by_pipeline = split_by_pipeline
        self.split_by_step = split_by_step
        self._file_handles: Dict[str, Any] = {}
        self._lock = threading.RLock()
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, entry: LogEntry, create_dir: bool = True) -> str:
        parts = [self.base_dir]
        if self.split_by_pipeline and entry.pipeline_id:
            parts.append(entry.pipeline_id)
        if create_dir:
            os.makedirs(os.path.join(*parts), exist_ok=True)
        if self.split_by_step and entry.step_name:
            filename = f"{entry.stage_name}_{entry.step_name}.log" if entry.stage_name else f"{entry.step_name}.log"
        else:
            filename = f"pipeline_{entry.pipeline_id}.log" if entry.pipeline_id else "pipeline.log"
        return os.path.join(*parts, filename)

    def save(self, entry: LogEntry) -> None:
        file_path = self._get_file_path(entry)
        line = entry.to_json() + "\n"
        with self._lock:
            try:
                if file_path not in self._file_handles:
                    self._file_handles[file_path] = open(file_path, "a", encoding="utf-8")
                self._file_handles[file_path].write(line)
                self._file_handles[file_path].flush()
            except Exception as e:
                print(f"Error saving log to {file_path}: {e}")

    def query(
        self,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        min_level: Optional[LogLevel] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[LogEntry]:
        results: List[LogEntry] = []
        min_severity = min_level.severity if min_level else 0

        files_to_scan: List[str] = []

        if self.split_by_pipeline and pipeline_id:
            pipeline_dir = os.path.join(self.base_dir, pipeline_id)
            if os.path.exists(pipeline_dir):
                if self.split_by_step and step_name:
                    for fname in os.listdir(pipeline_dir):
                        if step_name in fname:
                            files_to_scan.append(os.path.join(pipeline_dir, fname))
                else:
                    for fname in os.listdir(pipeline_dir):
                        if fname.endswith(".log"):
                            files_to_scan.append(os.path.join(pipeline_dir, fname))
        else:
            if os.path.exists(self.base_dir):
                for root, _, files in os.walk(self.base_dir):
                    for fname in files:
                        if fname.endswith(".log"):
                            if pipeline_id and pipeline_id not in fname:
                                continue
                            files_to_scan.append(os.path.join(root, fname))

        for file_path in files_to_scan:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = LogEntry.from_dict(json.loads(line))
                        except (json.JSONDecodeError, ValueError):
                            continue
                        if pipeline_id and entry.pipeline_id != pipeline_id:
                            continue
                        if stage_name and entry.stage_name != stage_name:
                            continue
                        if step_name and entry.step_name != step_name:
                            continue
                        if min_level and entry.level.severity < min_severity:
                            continue
                        if start_time and entry.timestamp < start_time:
                            continue
                        if end_time and entry.timestamp > end_time:
                            continue
                        results.append(entry)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

        results.sort(key=lambda e: e.timestamp)
        if offset > 0:
            results = results[offset:]
        if limit:
            results = results[:limit]
        return results

    def close(self) -> None:
        with self._lock:
            for path, handle in self._file_handles.items():
                try:
                    handle.close()
                except Exception:
                    pass
            self._file_handles.clear()


class LogStream:
    def __init__(self,
                 stores: Optional[List[LogStore]] = None,
                 console_output: bool = True,
                 min_console_level: LogLevel = LogLevel.INFO):
        self._stores: List[LogStore] = stores or [InMemoryLogStore()]
        self._subscribers: Set[LogSubscriber] = set()
        self._subscriber_lock = threading.RLock()
        self._console_output = console_output
        self._min_console_level = min_console_level
        self._closed = False
        self._global_context: Dict[str, Any] = {}

    def add_store(self, store: LogStore) -> None:
        self._stores.append(store)

    def remove_store(self, store: LogStore) -> None:
        if store in self._stores:
            self._stores.remove(store)
            store.close()

    def subscribe(self, subscriber: LogSubscriber) -> None:
        with self._subscriber_lock:
            self._subscribers.add(subscriber)

    def unsubscribe(self, subscriber: LogSubscriber) -> None:
        with self._subscriber_lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)
                subscriber.on_close()

    def set_global_context(self, **kwargs) -> None:
        self._global_context.update(kwargs)

    def log(self,
            message: str,
            level: LogLevel = LogLevel.INFO,
            *,
            pipeline_id: Optional[str] = None,
            stage_name: Optional[str] = None,
            step_name: Optional[str] = None,
            command_index: Optional[int] = None,
            metadata: Optional[Dict[str, Any]] = None,
            stream_source: Optional[str] = None) -> LogEntry:
        if self._closed:
            raise RuntimeError("LogStream is closed")

        entry = LogEntry(
            level=level,
            message=message,
            pipeline_id=pipeline_id or self._global_context.get("pipeline_id"),
            stage_name=stage_name or self._global_context.get("stage_name"),
            step_name=step_name or self._global_context.get("step_name"),
            command_index=command_index,
            metadata=metadata or {},
            stream_source=stream_source,
        )

        self._dispatch(entry)
        return entry

    def debug(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.DEBUG, **kwargs)

    def info(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.INFO, **kwargs)

    def warn(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.WARN, **kwargs)

    warning = warn

    def error(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.ERROR, **kwargs)

    def critical(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.CRITICAL, **kwargs)

    def stdout(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.STDOUT, stream_source="stdout", **kwargs)

    def stderr(self, message: str, **kwargs) -> LogEntry:
        return self.log(message, LogLevel.STDERR, stream_source="stderr", **kwargs)

    def _dispatch(self, entry: LogEntry) -> None:
        for store in self._stores:
            try:
                store.save(entry)
            except Exception as e:
                if self._console_output:
                    print(f"[LogStore Error] {e}")

        with self._subscriber_lock:
            subscribers = list(self._subscribers)
        for sub in subscribers:
            try:
                sub.on_log(entry)
            except Exception:
                pass

        if self._console_output and entry.level.severity >= self._min_console_level.severity:
            try:
                print(entry.format())
            except Exception:
                pass

    def query_logs(
        self,
        *,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        min_level: Optional[LogLevel] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[LogEntry]:
        all_results: List[LogEntry] = []
        for store in self._stores:
            all_results.extend(
                store.query(
                    pipeline_id=pipeline_id,
                    stage_name=stage_name,
                    step_name=step_name,
                    min_level=min_level,
                    start_time=start_time,
                    end_time=end_time,
                    limit=None,
                    offset=0,
                )
            )

        all_results.sort(key=lambda e: e.timestamp)
        if offset > 0:
            all_results = all_results[offset:]
        if limit:
            all_results = all_results[:limit]
        return all_results

    def tail(self,
             *,
             pipeline_id: Optional[str] = None,
             stage_name: Optional[str] = None,
             step_name: Optional[str] = None,
             lines: int = 100,
             min_level: Optional[LogLevel] = None) -> List[LogEntry]:
        results = self.query_logs(
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            min_level=min_level,
        )
        return results[-lines:] if lines < len(results) else results

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        with self._subscriber_lock:
            for sub in self._subscribers:
                try:
                    sub.on_close()
                except Exception:
                    pass
            self._subscribers.clear()

        for store in self._stores:
            try:
                store.close()
            except Exception:
                pass
