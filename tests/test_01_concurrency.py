"""Concurrency: no lost updates; CAS rejects stale writers; capacity bounded;
atomic batch all-or-nothing under contention."""

from __future__ import annotations

import threading

from kvstore.errors import CapacityExceeded, VersionMismatch

from conftest import get_logger

LOG = get_logger("test.concurrency")


def test_concurrent_conditional_updates_never_lose_an_update(make_store):
    """N threads each increment the same key via CAS retry until success.

    Final value must equal N and every version 1..N+1 must be present exactly
    once (linearizable updates, no overwrites).
    """
    n_threads = 12
    store = make_store()
    store.put("ns", "counter", 0)
    observed_versions: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(n_threads)

    def worker(idx: int):
        barrier.wait()
        for attempt in range(200):
            cur = store.get("ns", "counter")
            try:
                res = store.put(
                    "ns", "counter", cur["value"] + 1,
                    expected_version=cur["version"],
                )
                LOG.info("thread=%02d attempt=%d read_v=%d wrote_v=%d OK",
                         idx, attempt, cur["version"], res["version"])
                with lock:
                    observed_versions.append(res["version"])
                return
            except VersionMismatch as e:
                LOG.info("thread=%02d attempt=%d rejected: %s", idx, attempt, e)
        raise AssertionError(f"thread {idx} never succeeded")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = store.get("ns", "counter")
    LOG.info("input threads=%d; final value=%s version=%d; versions=%s",
             n_threads, final["value"], final["version"], sorted(observed_versions))
    assert final["value"] == n_threads, "basis: one increment per thread"
    assert final["version"] == n_threads + 1
    assert sorted(observed_versions) == list(range(2, n_threads + 2)), (
        "every new version produced exactly once -> no update lost/duplicated"
    )


def test_concurrent_create_only_one_winner(make_store):
    """if_not_exists: exactly one of N racing creators wins."""
    n_threads = 10
    store = make_store()
    barrier = threading.Barrier(n_threads)
    winners: list[int] = []
    rejected: list[int] = []
    lock = threading.Lock()

    def worker(idx: int):
        barrier.wait()
        try:
            store.put("ns", "k", {"by": idx}, if_not_exists=True)
            LOG.info("thread=%d create-if-absent -> winner", idx)
            with lock:
                winners.append(idx)
        except Exception as e:  # KeyExists / KeyDeleted never here
            LOG.info("thread=%d create-if-absent -> rejected %s", idx, e.code)
            with lock:
                rejected.append(idx)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    LOG.info("winners=%s rejected=%d (basis: exactly one create must win)",
             winners, len(rejected))
    assert len(winners) == 1
    assert len(rejected) == n_threads - 1
    assert store.get("ns", "k")["value"] == {"by": winners[0]}


def test_concurrent_capacity_is_never_silently_exceeded(make_store):
    """Capacity C: exactly C of N>C racing inserts succeed; no silent loss."""
    capacity, n_threads = 5, 20
    store = make_store(capacity=capacity)
    barrier = threading.Barrier(n_threads)
    ok, rejected = [], []
    lock = threading.Lock()

    def worker(idx: int):
        barrier.wait()
        try:
            store.put("ns", f"k{idx:02d}", idx)
            LOG.info("thread=%d insert accepted", idx)
            with lock:
                ok.append(idx)
        except CapacityExceeded as e:
            LOG.info("thread=%d insert rejected: %s", idx, e)
            with lock:
                rejected.append(idx)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    live = [f"k{i:02d}" for i in range(n_threads)]
    live_count = 0
    for name in live:
        try:
            store.get("ns", name)
            live_count += 1
        except Exception:
            pass
    LOG.info("capacity=%d accepted=%d rejected=%d readable_live=%d "
             "(basis: live keys <= capacity)",
             capacity, len(ok), len(rejected), live_count)
    assert len(ok) == capacity
    assert live_count == capacity
    assert len(rejected) == n_threads - capacity
    # every accepted key is readable with its original value
    for idx in ok:
        assert store.get("ns", f"k{idx:02d}")["value"] == idx


def test_atomic_batch_is_all_or_nothing_under_version_contention(make_store):
    """A batch touching key 'x' conditionally either fully applies or aborts
    without side effects, even raced against single-key updates."""
    store = make_store()
    store.put("ns", "x", 0)
    store.put("ns", "y", 0)
    stop = threading.Event()

    def racer():
        while not stop.is_set():
            cur = store.get("ns", "x")
            if not isinstance(cur["value"], int):
                break  # the batch won and x is no longer an integer counter
            try:
                store.put("ns", "x", cur["value"] + 1, expected_version=cur["version"])
            except VersionMismatch:
                pass

    t = threading.Thread(target=racer)
    t.start()
    attempts = 0
    for _ in range(50):
        cur = store.get("ns", "x")
        attempts += 1
        try:
            res = store.batch([
                {"op": "put", "namespace": "ns", "key": "x",
                 "value": "batch", "expected_version": cur["version"]},
                {"op": "put", "namespace": "ns", "key": "y",
                 "value": "batch-paired"},
            ])
            LOG.info("batch attempt=%d applied at x v=%s", attempts,
                     res["results"][0]["version"])
            break
        except VersionMismatch as e:
            # y must be untouched by the aborted batch
            assert store.get("ns", "y")["value"] == 0
    else:
        raise AssertionError("batch never won the race")
    stop.set()
    t.join()

    y = store.get("ns", "y")
    x = store.get("ns", "x")
    LOG.info("result x=%s y=%s (basis: rejected batches left no trace)",
             x["value"], y["value"])
    assert y["value"] == "batch-paired"
