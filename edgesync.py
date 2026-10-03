"""EdgeSync: local multi-target replication kernel.

One master node and two replica targets (A, B), each backed by a local
directory. Only the master accepts mutations. Metadata lives in process.

Concurrency semantics
---------------------
- put / delete / replica-publish each have a single atomic effect point,
  serialized by one internal lock. Only cheap metadata operations happen
  under the lock; input-stream reading, file copying and content checks
  always happen outside any exclusive section.
- Every committed version is stored as an immutable object file named after
  its globally unique, never-reused version id. Publishing is an atomic
  os.replace plus a metadata pointer swap, so a readable replica always
  matches its nominal version byte-for-byte.
- A replication task is validated both when claimed and when published:
  it is applied only if its version is still the current source version of
  a live path. Late/stale tasks are discarded and can never roll back a
  newer replica, resurrect a deleted path, or pollute a recreated one.

Resource release
----------------
- Object files are reference counted. References are held by: the master
  pointer of a live path, each published replica pointer, each queued or
  in-flight task, and each open read stream.
- When the last reference drops (tasks drained, in-flight operations
  finished, read streams closed), the object file is unlinked. Staging
  files of stale/failed publishes are removed immediately. No historical
  versions are kept alive beyond their references.
"""
from __future__ import annotations

import os
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import BinaryIO, Callable, Optional

MASTER = "master"
TARGETS = ("A", "B")

CHUNK_SIZE = 1 << 20  # 1 MiB per transfer buffer, well under the 8 MiB budget

STATUS_SYNCED = "synced"
STATUS_PENDING = "pending"


@dataclass(frozen=True)
class ReadResult:
    """Snapshot of a read at one logical moment."""
    exists: bool
    source_version: Optional[int]
    replica_version: Optional[int]
    stream: Optional[BinaryIO]  # None when the node has no published copy
    status: str  # "synced" | "pending"; "synced" when path does not exist


@dataclass(frozen=True)
class RunResult:
    kind: str  # "copied" | "skipped_stale" | "empty"
    path: Optional[str] = None
    version: Optional[int] = None


@dataclass
class _PathState:
    version: int
    obj: str
    published: dict = field(default_factory=dict)  # target -> (version, obj)


@dataclass
class _Task:
    path: str
    version: int
    obj: str  # master object holding this version's content


class _SnapshotStream:
    """File-like read stream over one immutable object; releases its
    reference on close so the object can be reclaimed."""

    def __init__(self, kernel: "SyncKernel", node: str, obj: str, fd: int):
        self._kernel = kernel
        self._node = node
        self._obj = obj
        self._file = os.fdopen(fd, "rb")

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
            self._kernel._release(self._node, self._obj)

    def __enter__(self) -> "_SnapshotStream":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class SyncKernel:
    def __init__(self, master_dir: str, dir_a: str, dir_b: str,
                 copy_hook: Optional[Callable[[str, str, int], None]] = None):
        # copy_hook(target, path, version) runs outside the lock after a
        # task is claimed and before its copy starts; a test seam.
        self._lock = threading.Lock()
        self._dirs = {MASTER: master_dir, "A": dir_a, "B": dir_b}
        for d in self._dirs.values():
            os.makedirs(os.path.join(d, "objects"), exist_ok=True)
        self._paths: dict[str, _PathState] = {}
        self._queues: dict[str, deque[_Task]] = {"A": deque(), "B": deque()}
        self._refs: dict[tuple[str, str], int] = {}
        self._next_version = 1
        self._tmp_counter = 0
        self.copy_hook = copy_hook

    # ---- object file helpers ----

    def _obj_path(self, node: str, obj: str) -> str:
        return os.path.join(self._dirs[node], "objects", obj)

    def _new_tmp_path(self, node: str) -> str:
        with self._lock:
            self._tmp_counter += 1
            n = self._tmp_counter
        return os.path.join(self._dirs[node], f".tmp-{os.getpid()}-{n}")

    def _acquire_locked(self, node: str, obj: str) -> None:
        key = (node, obj)
        self._refs[key] = self._refs.get(key, 0) + 1

    def _release_locked(self, node: str, obj: str) -> None:
        key = (node, obj)
        n = self._refs[key] - 1
        if n == 0:
            del self._refs[key]
            try:
                os.unlink(self._obj_path(node, obj))
            except FileNotFoundError:
                pass
        else:
            self._refs[key] = n

    def _release(self, node: str, obj: str) -> None:
        with self._lock:
            self._release_locked(node, obj)

    # ---- operations ----

    def put(self, path: str, stream: BinaryIO) -> int:
        # Stage the input outside any lock: slow I/O never blocks others.
        tmp = self._new_tmp_path(MASTER)
        with open(tmp, "wb") as out:
            while True:
                chunk = stream.read(CHUNK_SIZE)
                if not chunk:
                    break
                out.write(chunk)
        with self._lock:
            version = self._next_version
            self._next_version += 1
            obj = f"v{version}"
            os.replace(tmp, self._obj_path(MASTER, obj))  # atomic commit point
            old = self._paths.get(path)
            # Replicas keep serving their previously published copies until
            # the new version lands, so the published pointers carry over.
            published = old.published if old is not None else {}
            self._paths[path] = _PathState(version=version, obj=obj,
                                           published=published)
            self._acquire_locked(MASTER, obj)  # master pointer ref
            for target in TARGETS:
                self._queues[target].append(_Task(path, version, obj))
                self._acquire_locked(MASTER, obj)  # one ref per queued task
            if old is not None:
                self._release_locked(MASTER, old.obj)  # drop old master ref
            return version

    def delete(self, path: str) -> bool:
        with self._lock:
            st = self._paths.pop(path, None)  # atomic effect point
            if st is None:
                return False
            self._release_locked(MASTER, st.obj)
            for target, (_v, obj) in st.published.items():
                self._release_locked(target, obj)
            # Queued/in-flight tasks for older versions become stale: the
            # path is gone, so their version check fails at claim/publish.
            return True

    def _task_stale_locked(self, target: str, task: _Task) -> bool:
        st = self._paths.get(task.path)
        return st is None or st.version != task.version

    def run_one(self, target: str) -> RunResult:
        if target not in TARGETS:
            raise ValueError(f"unknown target {target!r}")
        with self._lock:
            if not self._queues[target]:
                return RunResult("empty")
            task = self._queues[target].popleft()
            if self._task_stale_locked(target, task):
                self._release_locked(MASTER, task.obj)
                return RunResult("skipped_stale", task.path, task.version)
        if self.copy_hook is not None:
            self.copy_hook(target, task.path, task.version)
        # Copy outside any lock: a paused copy never blocks put/delete/
        # other copies for the same or any other path.
        tmp = self._new_tmp_path(target)
        with open(self._obj_path(MASTER, task.obj), "rb") as src, \
                open(tmp, "wb") as dst:
            while True:
                chunk = src.read(CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
        stale_file = None
        with self._lock:
            if self._task_stale_locked(target, task):
                # Late finisher: must not overwrite a newer replica,
                # resurrect a deleted path, or pollute a recreated one.
                self._release_locked(MASTER, task.obj)
                stale_file = tmp
                result = RunResult("skipped_stale", task.path, task.version)
            else:
                st = self._paths[task.path]
                os.replace(tmp, self._obj_path(target, task.obj))
                self._acquire_locked(target, task.obj)  # new published ref
                old_pub = st.published.get(target)
                st.published[target] = (task.version, task.obj)  # publish point
                if old_pub is not None:
                    self._release_locked(target, old_pub[1])
                self._release_locked(MASTER, task.obj)  # drop task ref
                result = RunResult("copied", task.path, task.version)
        if stale_file is not None:
            os.unlink(stale_file)
        return result

    def read(self, path: str, node: str) -> ReadResult:
        if node not in self._dirs:
            raise ValueError(f"unknown node {node!r}")
        with self._lock:
            st = self._paths.get(path)
            if st is None:
                return ReadResult(False, None, None, None, STATUS_SYNCED)
            if node == MASTER:
                version, obj = st.version, st.obj
            else:
                pub = st.published.get(node)
                version, obj = pub if pub is not None else (None, None)
            stream = None
            if obj is not None:
                self._acquire_locked(node, obj)  # stream ref
                fd = os.open(self._obj_path(node, obj), os.O_RDONLY)
                stream = _SnapshotStream(self, node, obj, fd)
            status = STATUS_SYNCED if (
                st.published.get("A", (None,))[0] == st.version
                and st.published.get("B", (None,))[0] == st.version
            ) else STATUS_PENDING
            return ReadResult(True, st.version, version, stream, status)
