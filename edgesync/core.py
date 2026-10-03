"""EdgeSync local multi-target replication kernel.

One master node accepts writes; objects are replicated to target nodes A
and B. File contents live in three local directories under ``root``:
``master/``, ``node_a/`` and ``node_b/``. Metadata is process-local.

Concurrency model
-----------------
A single lock guards all metadata. The lock is never held while reading
an input stream, copying file contents or verifying data; those happen
outside any exclusive region, so a stalled copy never blocks ``put``,
``delete`` or other copies. Each of ``put``, ``delete`` and the publish
step of a copy has one atomic commit point under the lock.

Resource management
-------------------
Every stored file is reference counted: open read snapshots and queued /
in-flight copy tasks hold references. A file is unlinked as soon as it is
both retired (superseded or deleted) and has no remaining references, so
no unreferenced historical or temporary files remain once queues are
drained, in-flight operations finish and read streams are closed.
"""
from __future__ import annotations

import os
import threading
from collections import deque
from dataclasses import dataclass
from typing import BinaryIO, Callable, Deque, Dict, Optional, Tuple

CHUNK_SIZE = 1024 * 1024  # 1 MiB copy buffer, well under the 8 MiB budget

MASTER = "master"
TARGETS = ("A", "B")

STATUS_SYNCED = "synced"
STATUS_PENDING = "pending"


@dataclass(frozen=True)
class TaskInfo:
    """Public description of a replication task (passed to task hooks)."""
    path: str
    version: int
    target: str


@dataclass
class ReadResult:
    """Snapshot returned by EdgeSync.read().

    ``stream`` is a stable snapshot of the replica: later overwrites or
    deletes never change its bytes. It is ``None`` when the node has no
    published replica yet, or when the path does not exist. The caller
    must close the stream; closing releases the underlying file.
    """
    exists: bool
    source_version: Optional[int]
    replica_version: Optional[int]
    stream: Optional[BinaryIO]
    status: str  # STATUS_SYNCED | STATUS_PENDING


class _ObjectRef:
    """A stored file plus the count of streams/tasks still using it."""
    __slots__ = ("file", "refs", "retired")

    def __init__(self, file: str) -> None:
        self.file = file
        self.refs = 0
        self.retired = False


class _PathMeta:
    __slots__ = ("version", "deleted", "master_obj", "published")

    def __init__(self) -> None:
        self.version: Optional[int] = None
        self.deleted = True
        self.master_obj: Optional[_ObjectRef] = None
        self.published: Dict[str, Tuple[int, _ObjectRef]] = {}


class _Task:
    __slots__ = ("info", "path_id", "src")

    def __init__(self, info: TaskInfo, path_id: int, src: _ObjectRef) -> None:
        self.info = info
        self.path_id = path_id
        self.src = src


class _SnapshotStream:
    """File wrapper that releases its object reference on close."""

    def __init__(self, raw: BinaryIO, on_close: Callable[[], None]) -> None:
        self._raw = raw
        self._on_close = on_close
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        return self._raw.read(size)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._raw.close()
            finally:
                self._on_close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self):
        return iter(self._raw)

    def __enter__(self) -> "_SnapshotStream":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _copy_stream(src: BinaryIO, dst: BinaryIO) -> None:
    while True:
        chunk = src.read(CHUNK_SIZE)
        if not chunk:
            return
        dst.write(chunk)


class EdgeSync:
    """Replicates objects from a master node to local target nodes A and B.

    ``task_hook`` (optional) is invoked with the :class:`TaskInfo` of each
    task at the start of its copy phase, outside the metadata lock. Tests
    use it as a deterministic pause point.
    """

    def __init__(self, root: str, task_hook: Optional[Callable[[TaskInfo], None]] = None) -> None:
        self._root = root
        self._dirs = {
            MASTER: os.path.join(root, "master"),
            "A": os.path.join(root, "node_a"),
            "B": os.path.join(root, "node_b"),
        }
        for d in self._dirs.values():
            os.makedirs(d, exist_ok=True)
        self._hook = task_hook
        self._lock = threading.Lock()
        self._paths: Dict[str, _PathMeta] = {}
        self._path_ids: Dict[str, int] = {}
        self._last_version: Dict[str, int] = {}
        self._next_path_id = 0
        self._queues: Dict[str, Deque[_Task]] = {t: deque() for t in TARGETS}
        self._task_seq = 0

    # ------------------------------------------------------------------ put

    def put(self, path: str, stream: BinaryIO) -> int:
        """Stream ``stream`` into the master node and schedule replication.

        Returns the new version number. Versions are never reused, even
        after the path is deleted and recreated.
        """
        with self._lock:
            path_id = self._path_id_locked(path)
            version = self._last_version.get(path, 0) + 1
            self._last_version[path] = version
            tmp = self._tmp_name(MASTER, path_id, version)
        # Stream the input to a temp file outside the lock.
        with open(tmp, "wb") as out:
            _copy_stream(stream, out)
        with self._lock:
            final = self._obj_name(MASTER, path_id, version)
            os.replace(tmp, final)  # atomic file commit
            meta = self._paths.get(path)
            if meta is None:
                meta = self._paths[path] = _PathMeta()
            old = meta.master_obj
            meta.master_obj = _ObjectRef(final)
            meta.version = version
            meta.deleted = False
            if old is not None:
                self._retire_locked(old)
            for target in TARGETS:
                self._enqueue_locked(target, path, path_id, version, meta.master_obj)
            return version

    # --------------------------------------------------------------- delete

    def delete(self, path: str) -> None:
        """Logically delete ``path``; published replicas become unreadable.

        Queued and in-flight copy tasks for the path become stale and will
        be discarded. Streams obtained before the delete stay readable.
        """
        with self._lock:
            meta = self._paths.get(path)
            if meta is None or meta.deleted:
                return
            meta.deleted = True
            meta.version = None
            if meta.master_obj is not None:
                self._retire_locked(meta.master_obj)
                meta.master_obj = None
            for _, ref in meta.published.values():
                self._retire_locked(ref)
            meta.published.clear()

    # -------------------------------------------------------------- run_one

    def run_one(self, target: str) -> bool:
        """Take and process one pending task for ``target``.

        Safe to call concurrently, including for the same target. Stale
        tasks (superseded or deleted) are skipped. Returns True if a task
        was taken, False if the queue was empty.
        """
        if target not in self._queues:
            raise ValueError(f"unknown target: {target!r}")
        with self._lock:
            queue = self._queues[target]
            if not queue:
                return False
            task = queue.popleft()
            self._task_seq += 1
            tmp = self._tmp_name(target, task.path_id, task.info.version, self._task_seq)
        if self._hook is not None:
            self._hook(task.info)
        # Cheap pre-check so expired tasks cost no I/O; the publish step
        # re-validates under the lock, which is the authoritative check.
        with self._lock:
            if not self._task_current_locked(task):
                self._release_locked(task.src)
                return True
        with open(task.src.file, "rb") as src, open(tmp, "wb") as dst:
            _copy_stream(src, dst)
        with self._lock:
            if self._task_current_locked(task):
                final = self._obj_name(target, task.path_id, task.info.version)
                os.replace(tmp, final)  # atomic publish of the file
                meta = self._paths[task.info.path]
                old = meta.published.get(target)
                meta.published[target] = (task.info.version, _ObjectRef(final))
                if old is not None:
                    self._retire_locked(old[1])
            else:
                os.unlink(tmp)
            self._release_locked(task.src)
            return True

    # ----------------------------------------------------------------- read

    def read(self, path: str, node: str) -> ReadResult:
        """Read ``path`` from ``node`` ("master", "A" or "B").

        Metadata and stream are captured at one logical instant. The
        returned stream is a stable snapshot and never blocks on the
        replication queues.
        """
        if node not in self._dirs:
            raise ValueError(f"unknown node: {node!r}")
        with self._lock:
            meta = self._paths.get(path)
            if meta is None or meta.deleted or meta.version is None:
                return ReadResult(False, None, None, None, STATUS_SYNCED)
            source_version = meta.version
            if node == MASTER:
                entry: Optional[Tuple[int, _ObjectRef]] = (source_version, meta.master_obj)
            else:
                entry = meta.published.get(node)
            synced = all(
                meta.published.get(t, (None, None))[0] == source_version for t in TARGETS
            )
            status = STATUS_SYNCED if synced else STATUS_PENDING
            if entry is None:
                return ReadResult(True, source_version, None, None, status)
            version, ref = entry
            ref.refs += 1  # keeps the file alive until the stream closes
            file_name = ref.file
        raw = open(file_name, "rb")
        stream = _SnapshotStream(raw, lambda: self._release(ref))
        return ReadResult(True, source_version, version, stream, status)

    # -------------------------------------------------------------- helpers

    def _path_id_locked(self, path: str) -> int:
        path_id = self._path_ids.get(path)
        if path_id is None:
            path_id = self._next_path_id
            self._next_path_id += 1
            self._path_ids[path] = path_id
        return path_id

    def _obj_name(self, node: str, path_id: int, version: int) -> str:
        return os.path.join(self._dirs[node], f"obj_{path_id}_v{version}")

    def _tmp_name(self, node: str, path_id: int, version: int, seq: int = 0) -> str:
        return os.path.join(self._dirs[node], f".tmp_{path_id}_{version}_{seq}")

    def _enqueue_locked(self, target: str, path: str, path_id: int,
                        version: int, src: _ObjectRef) -> None:
        src.refs += 1  # the task holds a reference until it finishes
        info = TaskInfo(path=path, version=version, target=target)
        self._queues[target].append(_Task(info, path_id, src))

    def _task_current_locked(self, task: _Task) -> bool:
        meta = self._paths.get(task.info.path)
        return (
            meta is not None
            and not meta.deleted
            and meta.version == task.info.version
            and meta.master_obj is task.src
        )

    def _retire_locked(self, ref: _ObjectRef) -> None:
        ref.retired = True
        if ref.refs == 0:
            self._unlink(ref.file)

    def _release_locked(self, ref: _ObjectRef) -> None:
        ref.refs -= 1
        if ref.retired and ref.refs == 0:
            self._unlink(ref.file)

    def _release(self, ref: _ObjectRef) -> None:
        with self._lock:
            self._release_locked(ref)

    @staticmethod
    def _unlink(file_name: str) -> None:
        try:
            os.unlink(file_name)
        except FileNotFoundError:
            pass
