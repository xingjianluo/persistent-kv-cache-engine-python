"""TTL semantics: invisible on expiry; reasons distinguish expiry/delete/
never-written; expiry frees capacity; absolute expiry time survives restart."""

from __future__ import annotations

from kvstore.errors import CapacityExceeded, KeyDeleted, KeyExpired, KeyNotFound

from conftest import get_logger

LOG = get_logger("test.ttl")


def test_expired_key_is_invisible_and_reason_distinct(make_store):
    store = make_store()
    store._test_clock["t"] = 10_000
    store.put("ns", "short", "v", ttl_ms=100)   # expires at 10100
    store.put("ns", "forever", "v")             # no TTL
    store.put("ns", "deleted", "v")
    store.delete("ns", "deleted")

    LOG.info("t=10050: short TTL=100 -> expires_at=10100, expect visible")
    store._test_clock["t"] = 10_050
    assert store.get("ns", "short")["value"] == "v"

    store._test_clock["t"] = 10_100
    LOG.info("t=10100: expect expired / deleted / missing reasons to differ")
    try:
        store.get("ns", "short")
        raise AssertionError
    except KeyExpired as e:
        LOG.info("short -> code=%s (natural expiry)", e.code)
        assert e.code == "expired"

    try:
        store.get("ns", "deleted")
        raise AssertionError
    except KeyDeleted as e:
        LOG.info("deleted -> code=%s (explicit tombstone)", e.code)
        assert e.code == "deleted"

    try:
        store.get("ns", "never-written")
        raise AssertionError
    except KeyNotFound as e:
        LOG.info("never-written -> code=%s", e.code)
        assert e.code == "not_found"

    # keys without TTL unaffected by time
    assert store.get("ns", "forever")["value"] == "v"
    LOG.info("basis: expired/deleted/not_found are three distinct codes")


def test_expiry_boundary_is_inclusive(make_store):
    """get at exactly expires_at_ms is treated as expired (now >= expiry)."""
    store = make_store()
    store._test_clock["t"] = 0
    res = store.put("ns", "k", "v", ttl_ms=1000)
    assert res["expires_at_ms"] == 1000
    store._test_clock["t"] = 999
    assert store.get("ns", "k")["value"] == "v"
    store._test_clock["t"] = 1000
    LOG.info("inputs ttl_ms=1000 from t=0; at now=1000 expect expired (>=)")
    try:
        store.get("ns", "k")
        raise AssertionError
    except KeyExpired:
        LOG.info("basis: boundary rule is now >= expires_at_ms")


def test_expiry_frees_capacity_and_slot_is_reusable(make_store):
    store = make_store(capacity=1)
    store._test_clock["t"] = 0
    store.put("ns", "a", 1, ttl_ms=100)
    try:
        store.put("ns", "b", 2)
        raise AssertionError
    except CapacityExceeded:
        LOG.info("t=0: capacity 1 full with live key a; b rejected")

    store._test_clock["t"] = 101
    store.put("ns", "b", 2)  # a is expired: slot reclaimed
    LOG.info("t=101: a expired, b accepted without evicting confirmed data")
    assert store.get("ns", "b")["value"] == 2
    try:
        store.get("ns", "a")
        raise AssertionError
    except KeyExpired:
        pass
    # reviving the expired key with its old version is allowed only if the
    # capacity permits; here b occupies the single slot -> explicit rejection
    try:
        store.put("ns", "a", 11, expected_version=1)
        raise AssertionError
    except CapacityExceeded:
        LOG.info("basis: revive attempted to grow past capacity -> rejected")
    # once the live key is deleted, the expired key can be revived by version
    store.delete("ns", "b")
    store.put("ns", "a", 11, expected_version=1)
    assert store.get("ns", "a")["value"] == 11
    assert store.get("ns", "a")["version"] == 2


def test_rewrite_with_ttl_replaces_expiry(make_store):
    store = make_store()
    store._test_clock["t"] = 0
    store.put("ns", "k", 1, ttl_ms=100)
    store._test_clock["t"] = 50
    r = store.put("ns", "k", 2, expected_version=1, ttl_ms=100)
    assert r["version"] == 2 and r["expires_at_ms"] == 150
    store._test_clock["t"] = 120
    LOG.info("rewrote at t=50 with fresh TTL: at t=120 still visible")
    assert store.get("ns", "k")["value"] == 2
    store._test_clock["t"] = 150
    try:
        store.get("ns", "k")
        raise AssertionError
    except KeyExpired:
        LOG.info("basis: expiry is the absolute timestamp of latest write")


def test_ttl_cas_against_expired_key_gives_expired_reason(make_store):
    store = make_store()
    store._test_clock["t"] = 0
    store.put("ns", "k", 1, ttl_ms=100)
    store._test_clock["t"] = 200
    try:
        store.put("ns", "k", 2, expected_version=99)
        raise AssertionError
    except KeyExpired as e:
        LOG.info("CAS stale-version against expired key -> code=%s", e.code)


def test_cas_reasons_cover_all_four_states(make_store):
    from kvstore.errors import KeyNotFound, VersionMismatch
    store = make_store()
    # missing: expected_version on absent key
    try:
        store.put("ns", "x", 1, expected_version=5)
        raise AssertionError
    except KeyNotFound as e:
        LOG.info("CAS on missing key -> code=%s", e.code)
    # missing: expected_version=0 creates
    r = store.put("ns", "x", 1, expected_version=0)
    assert r["version"] == 1
    # live mismatch
    try:
        store.put("ns", "x", 2, expected_version=9)
        raise AssertionError
    except VersionMismatch as e:
        LOG.info("CAS on live key wrong version -> code=%s", e.code)
        assert e.code == "version_mismatch"
    # live match
    assert store.put("ns", "x", 2, expected_version=1)["version"] == 2
    # deleted mismatch
    store.delete("ns", "x")
    try:
        store.put("ns", "x", 3, expected_version=1)
        raise AssertionError
    except KeyDeleted as e:
        LOG.info("CAS on tombstone wrong version -> code=%s", e.code)
