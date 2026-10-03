"""Tests for the EdgeSync replication kernel.

All interleavings are built with deterministic pause points (threading
Events via the task_hook), never with sleeps.
"""
import hashlib
import io
import os
import threading
import tracemalloc

import pytest

from edgesync import EdgeSync

MEM_BUDGET = 8 * 1024 * 1024  # 8 MiB


# ------------------------------------------------------------------ helpers

class PauseHook:
    """task_hook that blocks tasks matching ``pred`` until released."""

    def __init__(self, pred):
        self.pred = pred
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, info):
        if self.pred(info):
            self.entered.set()
            assert self.release.wait(timeout=30)


def run_paused(sync, target):
    t = threading.Thread(target=sync.run_one, args=(target,))
    t.start()
    return t


def drain(sync):
    while sync.run_one("A"):
        pass
    while sync.run_one("B"):
        pass


def stored_files(root):
    out = []
    for dirpath, _, names in os.walk(root):
        out.extend(os.path.join(dirpath, n) for n in names)
    return sorted(out)


def _block(seed, index):
    return hashlib.sha256(seed.to_bytes(4, "big") + index.to_bytes(8, "big")).digest()


def pattern_bytes(total, seed):
    return b"".join(_block(seed, i) for i in range(-(-total // 32)))[:total]


class LazySource:
    """Generates ``total`` deterministic bytes on demand, storing nothing."""

    def __init__(self, total, seed):
        self._total = total
        self._seed = seed
        self._off = 0
        self._buf = b""

    def read(self, n=-1):
        if self._off >= self._total:
            return b""
        if n < 0:
            n = self._total - self._off
        n = min(n, self._total - self._off)
        out = bytearray()
        while len(out) < n:
            if not self._buf:
                self._buf = _block(self._seed, (self._off + len(out)) // 32)
            take = min(n - len(out), len(self._buf))
            out += self._buf[:take]
            self._buf = self._buf[take:]
        self._off += n
        return bytes(out)


@pytest.fixture
def sync(tmp_path):
    return EdgeSync(str(tmp_path / "store"))


# ------------------------------------------------------------- basic semantics

def test_put_read_delete_roundtrip(sync):
    v1 = sync.put("f", io.BytesIO(b"hello"))
    r = sync.read("f", "A")
    assert r.exists and r.source_version == v1
    assert r.replica_version is None and r.stream is None and r.status == "pending"

    sync.run_one("A")
    r = sync.read("f", "A")
    assert r.replica_version == v1 and r.stream.read() == b"hello"
    r.stream.close()
    assert r.status == "pending"  # B still behind

    sync.run_one("B")
    for node in ("A", "B", "master"):
        r = sync.read("f", node)
        assert r.status == "synced" and r.stream.read() == b"hello"
        r.stream.close()

    sync.delete("f")
    for node in ("A", "B", "master"):
        assert not sync.read("f", node).exists


def test_read_nonexistent_path(sync):
    r = sync.read("nope", "A")
    assert not r.exists and r.stream is None


def test_versions_never_reused_after_delete(sync):
    v1 = sync.put("f", io.BytesIO(b"a"))
    sync.delete("f")
    v2 = sync.put("f", io.BytesIO(b"b"))
    sync.delete("f")
    v3 = sync.put("f", io.BytesIO(b"c"))
    assert v1 < v2 < v3


def test_stream_obtained_before_delete_stays_readable(sync):
    sync.put("f", io.BytesIO(b"persistent"))
    sync.run_one("A")
    stream = sync.read("f", "A").stream
    sync.delete("f")
    assert not sync.read("f", "A").exists
    assert stream.read() == b"persistent"
    stream.close()
    drain(sync)
    assert stored_files(sync._root) == []


def test_concurrent_run_one(sync):
    for i in range(6):
        sync.put(f"p{i}", io.BytesIO(f"data-{i}".encode()))

    def worker():
        while sync.run_one("A") or sync.run_one("B"):
            pass

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i in range(6):
        r = sync.read(f"p{i}", "B")
        assert r.status == "synced" and r.stream.read() == f"data-{i}".encode()
        r.stream.close()


# ------------------------------------------------------- required interleavings

def test_out_of_order_publish_does_not_regress(tmp_path):
    hook = PauseHook(lambda i: i.version == 1 and i.target == "A")
    sync = EdgeSync(str(tmp_path / "store"), task_hook=hook)

    sync.put("f", io.BytesIO(b"v1-data"))
    t = run_paused(sync, "A")  # stalls mid-copy of v1 -> A
    assert hook.entered.wait(timeout=5)

    v2 = sync.put("f", io.BytesIO(b"v2-data-different"))
    assert sync.run_one("A")  # newer version finishes first

    r = sync.read("f", "A")
    assert r.replica_version == v2 and r.status == "pending"
    r.stream.close()

    hook.release.set()
    t.join(timeout=5)

    r = sync.read("f", "A")
    assert r.replica_version == v2, "stale task must not regress the replica"
    assert r.stream.read() == b"v2-data-different"
    r.stream.close()
    assert r.status == "pending", "B has not caught up yet"

    assert sync.run_one("B")  # stale v1 task: skipped
    assert sync.run_one("B")  # v2 task: copied
    r = sync.read("f", "B")
    assert r.status == "synced" and r.stream.read() == b"v2-data-different"
    r.stream.close()


def test_delete_while_copy_paused_no_rebuild(tmp_path):
    hook = PauseHook(lambda i: i.version == 1 and i.target == "A")
    sync = EdgeSync(str(tmp_path / "store"), task_hook=hook)

    sync.put("f", io.BytesIO(b"doomed"))
    t = run_paused(sync, "A")
    assert hook.entered.wait(timeout=5)

    sync.delete("f")
    for node in ("A", "B", "master"):
        assert not sync.read("f", node).exists

    hook.release.set()
    t.join(timeout=5)
    drain(sync)

    assert not sync.read("f", "A").exists, "stale task must not resurrect the path"
    assert stored_files(sync._root) == [], "no files may remain after quiescing"


def test_delete_and_rebuild_while_copy_paused(tmp_path):
    hook = PauseHook(lambda i: i.version == 1 and i.target == "A")
    sync = EdgeSync(str(tmp_path / "store"), task_hook=hook)

    v1 = sync.put("f", io.BytesIO(b"old-content"))
    t = run_paused(sync, "A")
    assert hook.entered.wait(timeout=5)

    sync.delete("f")
    v2 = sync.put("f", io.BytesIO(b"rebuilt-content"))
    assert v2 > v1
    assert sync.run_one("A")  # rebuilt version publishes to A

    hook.release.set()
    t.join(timeout=5)

    r = sync.read("f", "A")
    assert r.replica_version == v2, "stale task must not clobber the rebuilt version"
    assert r.stream.read() == b"rebuilt-content"
    r.stream.close()

    drain(sync)
    remaining = stored_files(sync._root)
    assert len(remaining) == 3  # master + A + B, all at v2
    assert all(f"_v{v2}" in f for f in remaining)
    assert not any(f"_v{v1}" in f for f in remaining)


def test_large_object_snapshot_stability_and_memory_budget(tmp_path):
    size = 16 * 1024 * 1024  # larger than the 8 MiB buffer budget
    sync = EdgeSync(str(tmp_path / "store"))

    tracemalloc.start()
    sync.put("big", LazySource(size, seed=7))
    sync.run_one("A")
    sync.run_one("B")
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak <= MEM_BUDGET, f"buffering grew with object size: peak {peak} bytes"

    snap = sync.read("big", "A").stream  # held open across the overwrite
    v2 = sync.put("big", LazySource(size, seed=9))
    sync.run_one("A")
    sync.run_one("B")

    got = b"".join(iter(lambda: snap.read(1 << 20), b""))
    snap.close()
    assert got == pattern_bytes(size, seed=7), "snapshot must keep the original bytes"

    r = sync.read("big", "A")
    assert r.replica_version == v2 and r.status == "synced"
    assert r.stream.read() == pattern_bytes(size, seed=9)
    r.stream.close()

    sync.delete("big")
    drain(sync)
    assert stored_files(sync._root) == []
