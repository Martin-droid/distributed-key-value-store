"""Caching policies used by storage nodes."""

from __future__ import annotations

from collections import OrderedDict
from typing import Generic, TypeVar


K = TypeVar("K")
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    """A fixed-capacity least-recently-used cache."""

    def __init__(self, capacity: int = 3) -> None:
        if capacity < 1:
            raise ValueError("cache capacity must be at least 1")
        self.capacity = capacity
        self._items: OrderedDict[K, V] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: K) -> V | None:
        if key not in self._items:
            self.misses += 1
            return None
        self._items.move_to_end(key)
        self.hits += 1
        return self._items[key]

    def put(self, key: K, value: V) -> None:
        if key in self._items:
            self._items.move_to_end(key)
        self._items[key] = value
        if len(self._items) > self.capacity:
            self._items.popitem(last=False)

    def delete(self, key: K) -> None:
        self._items.pop(key, None)

    def clear(self) -> None:
        self._items.clear()

    def keys(self) -> list[K]:
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)
