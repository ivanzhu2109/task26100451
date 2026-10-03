"""EdgeSync tests. Interleavings are built with controllable pause points
(threading.Event gates in copy_hook), never with sleeps."""
import io
import os
import shutil
import tempfile
import threading
import tracemalloc
import unittest

from edgesync import SyncKernel, MASTER


def list_files(root):
    out = []
    for base, _dirs, files in os.walk(root):
        for f in files:
            out.append(os.path.relpath(os.path.join(base, f), root))
    return sorted(out)


class KernelCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="edgesync-")
        self.dirs = {n: os.path.join(self.root, n) for n in ("master", "A", "B")}

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def make_kernel(self, copy_hook=None):
        return SyncKernel(self.dirs["master"], self.dirs["A"], self.dirs["B"],
                          copy_hook=copy_hook)

    def test_basic_replication_flow(self):
        k = self.make_kernel()
        v1 = k.put("f", io.BytesIO(b"hello"))
        r = k.read("f", "A")
        self.assertTrue(r.exists)
        self.assertEqual(r.source_version, v1)
        self.assertIsNone(r.replica_version)
        self.assertIsNone(r.stream)
        self.assertEqual(r.status, "pending")
        self.assertEqual(k.read("f", MASTER).stream.read(), b"hello")

        self.assertEqual(k.run_one("A").kind, "copied")
        r = k.read("f", "A")
        self.assertEqual(r.replica_version, v1)
        self.assertEqual(r.stream.read(), b"hello")
        r.stream.close()
        self.assertEqual(k.read("f", "B").status, "pending")

        self.assertEqual(k.run_one("B").kind, "copied")
        self.assertEqual(k.read("f", "B").status, "synced")
        self.assertEqual(k.run_one("A").kind, "empty")

    def test_read_missing_path(self):
        k = self.make_kernel()
        r = k.read("nope", "A")
        self.assertFalse(r.exists)
        self.assertIsNone(r.stream)
        self.assertFalse(k.delete("nope"))


if __name__ == "__main__":
    unittest.main()


class _Gate:
    """Blocks matching copy tasks until released; fully deterministic."""

    def __init__(self, pred):
        self.pred = pred
        self.entered = threading.Event()
        self.release = threading.Event()

    def hook(self, target, path, version):
        if self.pred(target, path, version):
            self.entered.set()
            self.release.wait(10)


def run_async(fn, *args):
    box = {}
    def go():
        try:
            box["result"] = fn(*args)
        except BaseException as e:  # noqa: BLE001
            box["error"] = e
    t = threading.Thread(target=go)
    t.start()
    return t, box


class InterleavingCase(KernelCase):
    def test_paused_old_copy_cannot_roll_back_newer_replica(self):
        v1_bytes = b"version-one" * 100
        v2_bytes = b"version-two!!" * 100
        gate = _Gate(lambda target, path, ver: target == "A" and ver == 1)
        k = self.make_kernel(copy_hook=gate.hook)
        v1 = k.put("f", io.BytesIO(v1_bytes))

        t, box = run_async(k.run_one, "A")  # claims v1->A, pauses in hook
        self.assertTrue(gate.entered.wait(10))

        v2 = k.put("f", io.BytesIO(v2_bytes))
        self.assertNotEqual(v1, v2)
        r2 = k.run_one("A")  # new version overtakes the paused one
        self.assertEqual((r2.kind, r2.version), ("copied", v2))

        r = k.read("f", "A")
        self.assertEqual(r.replica_version, v2)
        self.assertEqual(r.stream.read(), v2_bytes)
        r.stream.close()
        self.assertEqual(r.status, "pending")  # B has not caught up

        gate.release.set()
        t.join(10)
        self.assertEqual(box["result"].kind, "skipped_stale")

        r = k.read("f", "A")  # no rollback, bytes intact
        self.assertEqual(r.replica_version, v2)
        self.assertEqual(r.stream.read(), v2_bytes)
        r.stream.close()

        self.assertEqual(k.run_one("B").kind, "skipped_stale")  # old v1 task
        self.assertEqual(k.run_one("B").kind, "copied")
        self.assertEqual(k.read("f", "B").status, "synced")
        r = k.read("f", "A")
        self.assertEqual(r.stream.read(), v2_bytes)
        r.stream.close()


if __name__ == "__main__":
    unittest.main()


class DeleteInterleavingCase(KernelCase):
    def test_paused_copy_then_delete_no_rebuild(self):
        gate = _Gate(lambda target, path, ver: target == "A" and ver == 1)
        k = self.make_kernel(copy_hook=gate.hook)
        k.put("f", io.BytesIO(b"doomed"))

        t, box = run_async(k.run_one, "A")  # v1->A paused mid-flight
        self.assertTrue(gate.entered.wait(10))

        self.assertTrue(k.delete("f"))
        r = k.read("f", "A")  # delete has returned: reads see "not exists"
        self.assertFalse(r.exists)
        self.assertIsNone(r.stream)

        gate.release.set()
        t.join(10)
        self.assertEqual(box["result"].kind, "skipped_stale")

        self.assertFalse(k.read("f", MASTER).exists)  # no resurrection
        self.assertEqual(k.run_one("B").kind, "skipped_stale")
        self.assertEqual(k.run_one("A").kind, "empty")
        self.assertEqual(k.run_one("B").kind, "empty")
        # quiesced: no unreferenced objects or temp files anywhere
        for d in self.dirs.values():
            self.assertEqual(list_files(d), [])

    def test_paused_copy_then_delete_and_rebuild(self):
        gate = _Gate(lambda target, path, ver: target == "A" and ver == 1)
        k = self.make_kernel(copy_hook=gate.hook)
        v1 = k.put("f", io.BytesIO(b"old-content"))

        t, box = run_async(k.run_one, "A")  # v1->A paused mid-flight
        self.assertTrue(gate.entered.wait(10))

        self.assertTrue(k.delete("f"))
        v2 = k.put("f", io.BytesIO(b"rebuilt-content"))
        self.assertGreater(v2, v1)  # version never reused after recreate

        r2 = k.run_one("A")  # rebuilt version replicates first
        self.assertEqual((r2.kind, r2.version), ("copied", v2))

        gate.release.set()
        t.join(10)
        self.assertEqual(box["result"].kind, "skipped_stale")

        r = k.read("f", "A")  # old task did not clobber the rebuilt copy
        self.assertEqual(r.replica_version, v2)
        self.assertEqual(r.stream.read(), b"rebuilt-content")
        r.stream.close()

        self.assertEqual(k.run_one("B").kind, "skipped_stale")  # stale v1
        self.assertEqual(k.run_one("B").kind, "copied")         # v2
        rb = k.read("f", "B")
        self.assertEqual(rb.status, "synced")
        rb.stream.close()

        self.assertTrue(k.delete("f"))
        for d in self.dirs.values():  # quiesced: everything reclaimed
            self.assertEqual(list_files(d), [])


if __name__ == "__main__":
    unittest.main()


import hashlib

MIB = 1 << 20
BIG_SIZE = 24 * MIB + 12345  # > 8 MiB buffer budget, not chunk-aligned


def pattern_byte(pos):
    return (pos // MIB) % 256


class LazyStream:
    """Generates `size` deterministic bytes on demand; holds nothing."""

    def __init__(self, size):
        self.size = size
        self.pos = 0

    def read(self, n=-1):
        if self.pos >= self.size:
            return b""
        if n is None or n < 0:
            n = self.size - self.pos
        n = min(n, self.size - self.pos)
        out = bytearray()
        end = self.pos + n
        p = self.pos
        while p < end:
            block_end = min(end, (p // MIB + 1) * MIB)
            out += bytes([pattern_byte(p)]) * (block_end - p)
            p = block_end
        self.pos = end
        return bytes(out)


def expected_digest(size):
    h = hashlib.sha256()
    pos = 0
    while pos < size:
        block_end = min(size, (pos // MIB + 1) * MIB)
        h.update(bytes([pattern_byte(pos)]) * (block_end - pos))
        pos = block_end
    return h.hexdigest()


def digest_stream(stream):
    h = hashlib.sha256()
    while True:
        chunk = stream.read(MIB)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


class BigObjectCase(KernelCase):
    def test_snapshot_stability_and_bounded_buffering(self):
        k = self.make_kernel()
        v1_digest = expected_digest(BIG_SIZE)

        tracemalloc.start()
        try:
            v1 = k.put("big", LazyStream(BIG_SIZE))  # lazy, > buffer budget
            self.assertEqual(k.run_one("A").kind, "copied")
            self.assertEqual(k.run_one("B").kind, "copied")
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # transfer buffering stays bounded regardless of object size
        self.assertLess(peak, 8 * MIB)

        # snapshot streams taken before the overwrite
        s_master = k.read("big", MASTER).stream
        sa = k.read("big", "A")
        self.assertEqual(sa.replica_version, v1)

        v2 = k.put("big", io.BytesIO(b"tiny-v2"))  # overwrite mid-read
        self.assertNotEqual(v1, v2)
        self.assertEqual(k.run_one("A").kind, "copied")
        self.assertEqual(k.run_one("B").kind, "copied")

        # old snapshots still yield the original version byte-for-byte
        self.assertEqual(digest_stream(s_master), v1_digest)
        self.assertEqual(digest_stream(sa.stream), v1_digest)
        s_master.close()
        sa.stream.close()

        r = k.read("big", MASTER)  # new reads see the new version
        self.assertEqual(r.source_version, v2)
        self.assertEqual(r.stream.read(), b"tiny-v2")
        r.stream.close()
        ra = k.read("big", "A")
        self.assertEqual(ra.status, "synced")
        ra.stream.close()

        self.assertTrue(k.delete("big"))
        for d in self.dirs.values():  # nothing unreferenced left behind
            self.assertEqual(list_files(d), [])


if __name__ == "__main__":
    unittest.main()


class ConcurrencyAndReadSemanticsCase(KernelCase):
    def test_overwrite_keeps_old_replica_until_new_version_lands(self):
        k = self.make_kernel()
        v1 = k.put("f", io.BytesIO(b"old"))
        k.run_one("A")
        v2 = k.put("f", io.BytesIO(b"new"))
        r = k.read("f", "A")  # A still serves v1 while v2 is in flight
        self.assertEqual(r.source_version, v2)
        self.assertEqual(r.replica_version, v1)
        self.assertEqual(r.stream.read(), b"old")
        r.stream.close()
        self.assertEqual(r.status, "pending")
        k.run_one("A")
        r = k.read("f", "A")
        self.assertEqual(r.stream.read(), b"new")
        r.stream.close()

    def test_stream_taken_before_delete_stays_readable(self):
        k = self.make_kernel()
        k.put("f", io.BytesIO(b"persistent"))
        k.run_one("A")
        s = k.read("f", "A").stream
        self.assertTrue(k.delete("f"))
        self.assertFalse(k.read("f", "A").exists)  # new reads see "gone"
        self.assertEqual(s.read(), b"persistent")  # old stream finishes fine
        s.close()
        self.assertEqual(k.run_one("B").kind, "skipped_stale")
        for d in self.dirs.values():
            self.assertEqual(list_files(d), [])

    def test_concurrent_run_one_same_target(self):
        # Forces several threads to be inside run_one for the same target at
        # the same time via a rendezvous in the copy hook (no sleeps).
        n_paths, n_threads = 12, 4
        barrier = threading.Barrier(n_threads, timeout=10)
        def hook(target, path, version):
            if target == "A":
                barrier.wait(10)
        k = self.make_kernel(copy_hook=hook)
        contents = {}
        for i in range(n_paths):
            data = f"payload-{i}".encode() * (i + 1)
            contents[f"p{i}"] = data
            k.put(f"p{i}", io.BytesIO(data))

        errors = []
        def worker():
            try:
                while True:
                    ra = k.run_one("A")
                    rb = k.run_one("B")
                    if ra.kind == "empty" and rb.kind == "empty":
                        return
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertEqual(errors, [])

        for path, data in contents.items():
            for node in ("A", "B"):
                r = k.read(path, node)
                self.assertEqual(r.status, "synced")
                self.assertEqual(r.stream.read(), data)
                r.stream.close()


if __name__ == "__main__":
    unittest.main()
