"""Storage node used by the simulated cluster."""

from dataclasses import dataclass, field
from typing import Any

from .cache import LRUCache


@dataclass
class StorageNode:
    node_id: str
    cache_capacity: int = 3
    available: bool = True
    data: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cache: LRUCache[str, Any] = LRUCache(self.cache_capacity)

    def write(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.cache.delete(key)

    def read(self, key: str) -> tuple[Any, bool]:
        cached = self.cache.get(key)
        if cached is not None:
            return cached, True
        if key not in self.data:
            raise KeyError(key)
        value = self.data[key]
        self.cache.put(key, value)
        return value, False

    def delete(self, key: str) -> None:
        self.data.pop(key, None)
        self.cache.delete(key)
