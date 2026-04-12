from __future__ import annotations

import copy
from collections import deque
from threading import Lock
from typing import Any


class SnapshotStore:
    def __init__(self, max_snapshots: int = 100) -> None:
        self._lock = Lock()
        self._max_snapshots = max_snapshots
        self._store: dict[str, deque[dict[str, Any]]] = {
            "NIFTY": deque(maxlen=max_snapshots),
            "SENSEX": deque(maxlen=max_snapshots),
        }

    def append(self, symbol: str, snapshot: dict[str, Any]) -> None:
        with self._lock:
            if symbol not in self._store:
                self._store[symbol] = deque(maxlen=self._max_snapshots)
            self._store[symbol].append(copy.deepcopy(snapshot))

    def latest(self, symbol: str) -> dict[str, Any]:
        with self._lock:
            bucket = self._store.get(symbol)
            if not bucket or len(bucket) == 0:
                return {}
            return copy.deepcopy(bucket[-1])

    def snapshots(self, symbol: str) -> list[dict[str, Any]]:
        with self._lock:
            bucket = self._store.get(symbol)
            if not bucket:
                return []
            return copy.deepcopy(list(bucket))

    def count(self, symbol: str) -> int:
        with self._lock:
            bucket = self._store.get(symbol)
            return len(bucket) if bucket else 0

    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())
