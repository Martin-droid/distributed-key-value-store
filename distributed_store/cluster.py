"""Transparent distributed key-value store facade."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .hash_ring import HashRing
from .node import StorageNode


class NodeUnavailableError(RuntimeError):
    """Raised when no available replica can serve a key."""


@dataclass(frozen=True)
class ReadResult:
    key: str
    value: Any
    served_by: str
    primary: str
    cache_hit: bool
    used_failover: bool


class DistributedKeyValueStore:
    """A replicated store that hides placement and failover from callers."""

    def __init__(
        self,
        node_ids: list[str],
        *,
        virtual_nodes: int = 64,
        replication_factor: int = 2,
        cache_capacity: int = 3,
    ) -> None:
        if not node_ids:
            raise ValueError("at least one node is required")
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        self.replication_factor = replication_factor
        self.cache_capacity = cache_capacity
        self.ring = HashRing(virtual_nodes)
        self.nodes: dict[str, StorageNode] = {}
        self._records: dict[str, Any] = {}
        for node_id in node_ids:
            self._add_empty_node(node_id)

    def _add_empty_node(self, node_id: str) -> None:
        self.ring.add_node(node_id)
        self.nodes[node_id] = StorageNode(node_id, self.cache_capacity)

    def _owners(self, key: str) -> list[str]:
        return self.ring.owners_for(key, self.replication_factor)

    def put(self, key: str, value: Any) -> list[str]:
        self._records[key] = deepcopy(value)
        owners = self._owners(key)
        for node_id in owners:
            self.nodes[node_id].write(key, deepcopy(value))
        for node_id, node in self.nodes.items():
            if node_id not in owners:
                node.delete(key)
        return owners

    def get(self, key: str, *, detailed: bool = False) -> Any | ReadResult:
        if key not in self._records:
            raise KeyError(key)
        owners = self._owners(key)
        primary = owners[0]
        for node_id in owners:
            node = self.nodes[node_id]
            if not node.available:
                continue
            value, cache_hit = node.read(key)
            result = ReadResult(
                key=key,
                value=deepcopy(value),
                served_by=node_id,
                primary=primary,
                cache_hit=cache_hit,
                used_failover=node_id != primary,
            )
            return result if detailed else result.value
        raise NodeUnavailableError(f"no available replica can serve {key!r}")

    def delete(self, key: str) -> None:
        if key not in self._records:
            raise KeyError(key)
        del self._records[key]
        for node in self.nodes.values():
            node.delete(key)

    def add_node(self, node_id: str) -> dict[str, tuple[str, str]]:
        before = {key: self.ring.primary_for(key) for key in self._records}
        self._add_empty_node(node_id)
        self._rebalance()
        return {
            key: (before[key], self.ring.primary_for(key))
            for key in self._records
            if before[key] != self.ring.primary_for(key)
        }

    def remove_node(self, node_id: str) -> dict[str, tuple[str, str]]:
        if len(self.nodes) == 1:
            raise ValueError("cannot remove the last node")
        before = {key: self.ring.primary_for(key) for key in self._records}
        self.ring.remove_node(node_id)
        del self.nodes[node_id]
        self._rebalance()
        return {
            key: (before[key], self.ring.primary_for(key))
            for key in self._records
            if before[key] != self.ring.primary_for(key)
        }

    def fail_node(self, node_id: str) -> None:
        self.nodes[node_id].available = False

    def recover_node(self, node_id: str) -> None:
        self.nodes[node_id].available = True
        self._rebalance()

    def placement(self) -> dict[str, list[str]]:
        return {key: self._owners(key) for key in sorted(self._records)}

    def _rebalance(self) -> None:
        for node in self.nodes.values():
            node.data.clear()
            node.cache.clear()
        for key, value in self._records.items():
            for owner in self._owners(key):
                self.nodes[owner].write(key, deepcopy(value))
