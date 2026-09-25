"""Shared fixtures/helpers: isolated data dir, controllable clock, op log."""

from __future__ import annotations

import logging

import pytest

from kvstore.store import KVStore


@pytest.fixture
def make_store(tmp_path):
    created: list[KVStore] = []

    def _make(capacity: int = 10_000, start_ms: int = 1_000_000, data_dir=None):
        clock = {"t": start_ms}
        store = KVStore(
            data_dir=str(data_dir) if data_dir else str(tmp_path / ("d" + str(len(created)))),
            capacity_per_namespace=capacity,
            clock_ms=lambda: clock["t"],
        )
        store._test_clock = clock
        created.append(store)
        return store

    yield _make

    for store in created:
        store.close()


def get_logger(name: str) -> logging.Logger:
    """Tests log their operation sequence, inputs and the assertion basis."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
