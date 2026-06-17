from __future__ import annotations

import abc
import fnmatch
import hashlib
import io
import json
import os
import shutil
import tarfile
import threading
import time
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from ..models.pipeline import Artifact


@dataclass
class StoredArtifact:
    artifact_id: str
    name: str
    pipeline_id: str
    stage_name: str
    step_name: str
    source_path: str
    target_path: Optional[str] = None
    storage_key: str = ""
    size_bytes: int = 0
    md5_hash: Optional[str] = None
    compressed: bool = True
    content_type: str = "application/octet-stream"
    created_at: float = field(default_factory=time.time)
    retention_days: int = 7
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > (self.retention_days * 86400)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "name": self.name,
            "pipeline_id": self.pipeline_id,
            "stage_name": self.stage_name,
            "step_name": self.step_name,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "storage_key": self.storage_key,
            "size_bytes": self.size_bytes,
            "md5_hash": self.md5_hash,
            "compressed": self.compressed,
            "content_type": self.content_type,
            "created_at": self.created_at,
            "retention_days": self.retention_days,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StoredArtifact":
        return cls(
            artifact_id=data["artifact_id"],
            name=data["name"],
            pipeline_id=data["pipeline_id"],
            stage_name=data["stage_name"],
            step_name=data["step_name"],
            source_path=data.get("source_path", ""),
            target_path=data.get("target_path"),
            storage_key=data.get("storage_key", ""),
            size_bytes=data.get("size_bytes", 0),
            md5_hash=data.get("md5_hash"),
            compressed=data.get("compressed", True),
            content_type=data.get("content_type", "application/octet-stream"),
            created_at=data.get("created_at", time.time()),
            retention_days=data.get("retention_days", 7),
            metadata=data.get("metadata", {}),
        )


class ArtifactStore(abc.ABC):
    @abc.abstractmethod
    def upload(
        self,
        artifact: Artifact,
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        source_working_dir: Optional[str] = None,
    ) -> StoredArtifact: ...

    @abc.abstractmethod
    def download(
        self,
        stored: StoredArtifact,
        target_dir: str,
        *,
        target_subdir: Optional[str] = None,
    ) -> bool: ...

    @abc.abstractmethod
    def exists(self, storage_key: str) -> bool: ...

    @abc.abstractmethod
    def delete(self, storage_key: str) -> bool: ...

    @abc.abstractmethod
    def list(
        self,
        *,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ) -> List[StoredArtifact]: ...

    @abc.abstractmethod
    def get(self, artifact_id: str) -> Optional[StoredArtifact]: ...

    def cleanup_expired(self) -> int:
        count = 0
        for art in self.list():
            if art.is_expired():
                if self.delete(art.storage_key):
                    count += 1
        return count


class MemoryStore(ArtifactStore):
    def __init__(self):
        self._artifacts: Dict[str, StoredArtifact] = {}
        self._content: Dict[str, bytes] = {}
        self._index: Dict[str, List[str]] = {}
        self._lock = threading.RLock()

    def _index_key(self, **kwargs) -> str:
        parts = []
        for k in sorted(kwargs.keys()):
            v = kwargs[k] or "*"
            parts.append(f"{k}={v}")
        return "|".join(parts)

    def upload(
        self,
        artifact: Artifact,
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        source_working_dir: Optional[str] = None,
    ) -> StoredArtifact:
        source_path = artifact.source_path
        if source_working_dir and not os.path.isabs(source_path):
            source_path = os.path.join(source_working_dir, source_path)

        content, content_size = self._read_content(source_path, artifact.compressed)
        md5 = hashlib.md5(content).hexdigest()

        storage_key = f"{pipeline_id}/{stage_name}/{step_name}/{artifact.artifact_id}"

        stored = StoredArtifact(
            artifact_id=artifact.artifact_id,
            name=artifact.name,
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            source_path=artifact.source_path,
            target_path=artifact.target_path,
            storage_key=storage_key,
            size_bytes=content_size,
            md5_hash=md5,
            compressed=artifact.compressed,
            created_at=time.time(),
            retention_days=artifact.retention_days,
        )

        with self._lock:
            self._artifacts[stored.artifact_id] = stored
            self._artifacts[storage_key] = stored
            self._content[storage_key] = content
            for ikey in [
                self._index_key(pipeline_id=pipeline_id),
                self._index_key(pipeline_id=pipeline_id, stage_name=stage_name),
                self._index_key(pipeline_id=pipeline_id, stage_name=stage_name, step_name=step_name),
            ]:
                if ikey not in self._index:
                    self._index[ikey] = []
                if stored.artifact_id not in self._index[ikey]:
                    self._index[ikey].append(stored.artifact_id)

        return stored

    def _read_content(self, path: str, compressed: bool) -> Tuple[bytes, int]:
        if os.path.isdir(path):
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz" if compressed else "w") as tar:
                tar.add(path, arcname=os.path.basename(path))
            content = buf.getvalue()
            return content, len(content)
        elif os.path.isfile(path):
            with open(path, "rb") as f:
                content = f.read()
            if compressed:
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                    info = tarfile.TarInfo(name=os.path.basename(path))
                    info.size = len(content)
                    tar.addfile(info, io.BytesIO(content))
                content = buf.getvalue()
            return content, len(content)
        else:
            raise FileNotFoundError(f"Artifact source not found: {path}")

    def download(
        self,
        stored: StoredArtifact,
        target_dir: str,
        *,
        target_subdir: Optional[str] = None,
    ) -> bool:
        with self._lock:
            content = self._content.get(stored.storage_key)
        if content is None:
            return False

        target = target_dir
        if target_subdir:
            target = os.path.join(target, target_subdir)
        os.makedirs(target, exist_ok=True)

        if stored.compressed or content.startswith(b"\x1f\x8b"):
            try:
                buf = io.BytesIO(content)
                with tarfile.open(fileobj=buf, mode="r:*") as tar:
                    tar.extractall(target)
                return True
            except tarfile.TarError:
                pass

        if stored.target_path:
            target_path = os.path.join(target, stored.target_path)
        else:
            target_path = os.path.join(target, os.path.basename(stored.source_path) or stored.name)
        os.makedirs(os.path.dirname(target_path) if os.path.dirname(target_path) else target, exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(content)
        return True

    def exists(self, storage_key: str) -> bool:
        with self._lock:
            return storage_key in self._content

    def delete(self, storage_key: str) -> bool:
        with self._lock:
            stored = self._artifacts.get(storage_key)
            if stored:
                self._artifacts.pop(stored.artifact_id, None)
                for ikey in list(self._index.keys()):
                    if stored.artifact_id in self._index.get(ikey, []):
                        self._index[ikey].remove(stored.artifact_id)
            self._artifacts.pop(storage_key, None)
            return self._content.pop(storage_key, None) is not None

    def list(
        self,
        *,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ) -> List[StoredArtifact]:
        results: List[StoredArtifact] = []
        with self._lock:
            if pipeline_id or stage_name or step_name:
                ikey = self._index_key(
                    pipeline_id=pipeline_id, stage_name=stage_name, step_name=step_name
                )
                artifact_ids = self._index.get(ikey, [])
                for aid in artifact_ids:
                    stored = self._artifacts.get(aid)
                    if stored:
                        if name_pattern and not fnmatch.fnmatch(stored.name, name_pattern):
                            continue
                        results.append(stored)
            else:
                seen_ids: Set[str] = set()
                for stored in self._artifacts.values():
                    if stored.artifact_id in seen_ids:
                        continue
                    seen_ids.add(stored.artifact_id)
                    if name_pattern and not fnmatch.fnmatch(stored.name, name_pattern):
                        continue
                    results.append(stored)
        return sorted(results, key=lambda a: a.created_at, reverse=True)

    def get(self, artifact_id: str) -> Optional[StoredArtifact]:
        with self._lock:
            return self._artifacts.get(artifact_id)


class FileSystemStore(ArtifactStore):
    def __init__(self, base_dir: str = ".cicd_artifacts"):
        self.base_dir = os.path.abspath(base_dir)
        self.meta_dir = os.path.join(self.base_dir, ".metadata")
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.meta_dir, exist_ok=True)
        self._lock = threading.RLock()

    def _get_storage_path(self, storage_key: str) -> str:
        return os.path.join(self.base_dir, storage_key)

    def _get_meta_path(self, artifact_id: str) -> str:
        return os.path.join(self.meta_dir, f"{artifact_id}.json")

    def _save_meta(self, stored: StoredArtifact) -> None:
        with self._lock:
            path = self._get_meta_path(stored.artifact_id)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(stored.to_dict(), f, ensure_ascii=False, indent=2)

    def _load_meta(self, artifact_id: str) -> Optional[StoredArtifact]:
        path = self._get_meta_path(artifact_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return StoredArtifact.from_dict(json.load(f))
        except (json.JSONDecodeError, OSError):
            return None

    def upload(
        self,
        artifact: Artifact,
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        source_working_dir: Optional[str] = None,
    ) -> StoredArtifact:
        source_path = artifact.source_path
        if source_working_dir and not os.path.isabs(source_path):
            source_path = os.path.join(source_working_dir, source_path)

        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Artifact source not found: {source_path}")

        storage_key = f"{pipeline_id}/{stage_name}/{step_name}/{artifact.artifact_id}"
        storage_path = self._get_storage_path(storage_key)
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)

        md5 = hashlib.md5()
        total_size = 0

        if os.path.isdir(source_path):
            compress = artifact.compressed
            if compress:
                final_path = storage_path + ".tar.gz"
                with tarfile.open(final_path, "w:gz") as tar:
                    tar.add(source_path, arcname=os.path.basename(source_path))
                storage_key = storage_key + ".tar.gz"
                storage_path = final_path
            else:
                final_path = storage_path + ".tar"
                with tarfile.open(final_path, "w") as tar:
                    tar.add(source_path, arcname=os.path.basename(source_path))
                storage_key = storage_key + ".tar"
                storage_path = final_path
        else:
            if artifact.compressed:
                final_path = storage_path + ".gz"
                import gzip
                with open(source_path, "rb") as f_in:
                    with gzip.open(final_path, "wb") as f_out:
                        while True:
                            chunk = f_in.read(8192)
                            if not chunk:
                                break
                            md5.update(chunk)
                            total_size += len(chunk)
                            f_out.write(chunk)
                storage_key = storage_key + ".gz"
                storage_path = final_path
            else:
                shutil.copy2(source_path, storage_path)

        if total_size == 0:
            total_size = os.path.getsize(storage_path)
            if md5.digest() == b"\xd4\x1d\x8c\xd9\x8f\x00\xb2\x04\xe9\x80\t\x98\xec\xf8B~":
                with open(storage_path, "rb") as f:
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        md5.update(chunk)

        stored = StoredArtifact(
            artifact_id=artifact.artifact_id,
            name=artifact.name,
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            source_path=artifact.source_path,
            target_path=artifact.target_path,
            storage_key=storage_key,
            size_bytes=total_size,
            md5_hash=md5.hexdigest(),
            compressed=artifact.compressed,
            created_at=time.time(),
            retention_days=artifact.retention_days,
        )
        self._save_meta(stored)
        return stored

    def download(
        self,
        stored: StoredArtifact,
        target_dir: str,
        *,
        target_subdir: Optional[str] = None,
    ) -> bool:
        storage_path = self._get_storage_path(stored.storage_key)
        if not os.path.exists(storage_path):
            return False

        target = target_dir
        if target_subdir:
            target = os.path.join(target, target_subdir)
        os.makedirs(target, exist_ok=True)

        if stored.storage_key.endswith(".tar.gz") or stored.storage_key.endswith(".tgz"):
            try:
                with tarfile.open(storage_path, "r:gz") as tar:
                    tar.extractall(target)
                return True
            except tarfile.TarError:
                return False
        elif stored.storage_key.endswith(".tar"):
            try:
                with tarfile.open(storage_path, "r:") as tar:
                    tar.extractall(target)
                return True
            except tarfile.TarError:
                return False
        elif stored.storage_key.endswith(".gz"):
            import gzip
            output_name = stored.target_path or os.path.basename(stored.source_path)
            if not output_name:
                output_name = stored.name
            if output_name.endswith(".gz"):
                output_name = output_name[:-3]
            target_path = os.path.join(target, output_name)
            with gzip.open(storage_path, "rb") as f_in:
                with open(target_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return True
        else:
            target_path = os.path.join(target, stored.target_path or os.path.basename(stored.source_path) or stored.name)
            os.makedirs(os.path.dirname(target_path) or target, exist_ok=True)
            shutil.copy2(storage_path, target_path)
            return True

    def exists(self, storage_key: str) -> bool:
        return os.path.exists(self._get_storage_path(storage_key))

    def delete(self, storage_key: str) -> bool:
        storage_path = self._get_storage_path(storage_key)
        with self._lock:
            deleted = False
            if os.path.exists(storage_path):
                try:
                    os.remove(storage_path)
                    deleted = True
                except OSError:
                    pass
            meta_id = storage_key.split("/")[-1]
            if meta_id.endswith(".tar.gz"):
                meta_id = meta_id[:-7]
            elif meta_id.endswith(".gz") or meta_id.endswith(".tar"):
                meta_id = meta_id[: meta_id.rfind(".")]
            meta_path = self._get_meta_path(meta_id)
            if os.path.exists(meta_path):
                try:
                    os.remove(meta_path)
                    deleted = True
                except OSError:
                    pass
            return deleted

    def list(
        self,
        *,
        pipeline_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ) -> List[StoredArtifact]:
        results: List[StoredArtifact] = []
        if not os.path.exists(self.meta_dir):
            return results
        for fname in os.listdir(self.meta_dir):
            if not fname.endswith(".json"):
                continue
            artifact_id = fname[:-5]
            stored = self._load_meta(artifact_id)
            if not stored:
                continue
            if pipeline_id and stored.pipeline_id != pipeline_id:
                continue
            if stage_name and stored.stage_name != stage_name:
                continue
            if step_name and stored.step_name != step_name:
                continue
            if name_pattern and not fnmatch.fnmatch(stored.name, name_pattern):
                continue
            results.append(stored)
        return sorted(results, key=lambda a: a.created_at, reverse=True)

    def get(self, artifact_id: str) -> Optional[StoredArtifact]:
        return self._load_meta(artifact_id)


class ArtifactManager:
    def __init__(self, store: Optional[ArtifactStore] = None, base_dir: Optional[str] = None):
        if store:
            self.store = store
        else:
            self.store = FileSystemStore(base_dir=base_dir or ".cicd_artifacts")
        self._registry: Dict[str, StoredArtifact] = {}
        self._name_index: Dict[str, Dict[str, List[str]]] = {}
        self._lock = threading.RLock()

    def register_store(self, store: ArtifactStore) -> None:
        self.store = store

    def publish(
        self,
        artifact: Artifact,
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        working_dir: Optional[str] = None,
    ) -> StoredArtifact:
        stored = self.store.upload(
            artifact,
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            source_working_dir=working_dir,
        )
        with self._lock:
            self._registry[stored.artifact_id] = stored
            self._registry[f"{pipeline_id}:{stored.name}"] = stored
            self._registry[f"{pipeline_id}:{stage_name}:{step_name}:{stored.name}"] = stored

            if pipeline_id not in self._name_index:
                self._name_index[pipeline_id] = {}
            if stored.name not in self._name_index[pipeline_id]:
                self._name_index[pipeline_id][stored.name] = []
            if stored.artifact_id not in self._name_index[pipeline_id][stored.name]:
                self._name_index[pipeline_id][stored.name].append(stored.artifact_id)
        return stored

    def publish_batch(
        self,
        artifacts: List[Artifact],
        *,
        pipeline_id: str,
        stage_name: str,
        step_name: str,
        working_dir: Optional[str] = None,
    ) -> List[StoredArtifact]:
        results = []
        for art in artifacts:
            try:
                stored = self.publish(
                    art,
                    pipeline_id=pipeline_id,
                    stage_name=stage_name,
                    step_name=step_name,
                    working_dir=working_dir,
                )
                results.append(stored)
            except Exception as e:
                import sys
                print(f"Warning: Failed to publish artifact '{art.name}': {e}", file=sys.stderr)
        return results

    def _resolve_name(
        self, name: str, *, pipeline_id: str, stage_name: Optional[str] = None, step_name: Optional[str] = None
    ) -> Optional[StoredArtifact]:
        with self._lock:
            candidates = []
            if stage_name and step_name:
                key = f"{pipeline_id}:{stage_name}:{step_name}:{name}"
                if key in self._registry:
                    candidates.append(self._registry[key])
            key = f"{pipeline_id}:{name}"
            if key in self._registry:
                candidates.append(self._registry[key])
            if name in self._name_index.get(pipeline_id, {}):
                for aid in self._name_index[pipeline_id][name]:
                    stored = self._registry.get(aid)
                    if stored:
                        candidates.append(stored)
            if candidates:
                return sorted(candidates, key=lambda a: a.created_at, reverse=True)[0]
        return None

    def find(
        self,
        name: str,
        *,
        pipeline_id: str,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
    ) -> Optional[StoredArtifact]:
        stored = self._resolve_name(
            name, pipeline_id=pipeline_id, stage_name=stage_name, step_name=step_name
        )
        if stored:
            return stored
        results = self.store.list(
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
            name_pattern=name,
        )
        if results:
            return results[0]
        results = self.store.list(pipeline_id=pipeline_id, name_pattern=name)
        return results[0] if results else None

    def restore(
        self,
        artifact_name: str,
        target_dir: str,
        *,
        pipeline_id: str,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        target_subdir: Optional[str] = None,
    ) -> bool:
        stored = self.find(
            artifact_name,
            pipeline_id=pipeline_id,
            stage_name=stage_name,
            step_name=step_name,
        )
        if not stored:
            return False
        return self.store.download(stored, target_dir, target_subdir=target_subdir)

    def restore_batch(
        self,
        artifact_names: List[str],
        target_dir: str,
        *,
        pipeline_id: str,
        stage_name: Optional[str] = None,
        step_name: Optional[str] = None,
        target_subdir: Optional[str] = None,
    ) -> Dict[str, bool]:
        results = {}
        for name in artifact_names:
            results[name] = self.restore(
                name,
                target_dir,
                pipeline_id=pipeline_id,
                stage_name=stage_name,
                step_name=step_name,
                target_subdir=target_subdir,
            )
        return results

    def list_pipeline_artifacts(self, pipeline_id: str) -> List[StoredArtifact]:
        from_store = self.store.list(pipeline_id=pipeline_id)
        with self._lock:
            from_registry = set()
            for aid, stored in self._registry.items():
                if aid.count(":") >= 1:
                    continue
                if stored.pipeline_id == pipeline_id:
                    from_registry.add(stored.artifact_id)
            existing_ids = {s.artifact_id for s in from_store}
            for stored in self._registry.values():
                if (
                    stored.pipeline_id == pipeline_id
                    and stored.artifact_id not in existing_ids
                    and not stored.artifact_id.startswith(pipeline_id)
                ):
                    pass
        return from_store

    def list_stage_artifacts(self, pipeline_id: str, stage_name: str) -> List[StoredArtifact]:
        return self.store.list(pipeline_id=pipeline_id, stage_name=stage_name)

    def list_step_artifacts(self, pipeline_id: str, stage_name: str, step_name: str) -> List[StoredArtifact]:
        return self.store.list(pipeline_id=pipeline_id, stage_name=stage_name, step_name=step_name)

    def cleanup_pipeline(self, pipeline_id: str) -> int:
        count = 0
        artifacts = self.store.list(pipeline_id=pipeline_id)
        for stored in artifacts:
            if self.store.delete(stored.storage_key):
                count += 1
        with self._lock:
            for key in list(self._registry.keys()):
                if key.startswith(f"{pipeline_id}:") or (
                    key.count(":") == 0
                    and self._registry.get(key)
                    and self._registry[key].pipeline_id == pipeline_id
                ):
                    self._registry.pop(key, None)
            self._name_index.pop(pipeline_id, None)
        return count

    def cleanup_expired(self) -> int:
        return self.store.cleanup_expired()
