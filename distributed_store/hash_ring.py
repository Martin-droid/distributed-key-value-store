"""Consistent hashing ring implementation."""

from bisect import bisect_left, insort
from hashlib import sha256


class HashRing:
    """Map arbitrary keys to physical nodes through virtual-node tokens."""

    def __init__(self, virtual_nodes: int = 64) -> None:
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be at least 1")
        self.virtual_nodes = virtual_nodes
        self._tokens: list[int] = []
        self._owners: dict[int, str] = {}
        self._nodes: set[str] = set()

    @staticmethod
    def hash_value(value: str) -> int:
        return int.from_bytes(sha256(value.encode("utf-8")).digest()[:8], "big")

    @property
    def nodes(self) -> tuple[str, ...]:
        return tuple(sorted(self._nodes))

    def add_node(self, node_id: str) -> None:
        if not node_id or node_id in self._nodes:
            raise ValueError(f"node already exists or is invalid: {node_id!r}")
        self._nodes.add(node_id)
        for replica in range(self.virtual_nodes):
            token = self.hash_value(f"{node_id}:{replica}")
            while token in self._owners:
                token = (token + 1) % (2**64)
            insort(self._tokens, token)
            self._owners[token] = node_id

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            raise KeyError(node_id)
        owned = [token for token, owner in self._owners.items() if owner == node_id]
        owned_set = set(owned)
        self._tokens = [token for token in self._tokens if token not in owned_set]
        for token in owned:
            del self._owners[token]
        self._nodes.remove(node_id)

    def owners_for(self, key: str, count: int = 1) -> list[str]:
        if not self._tokens:
            raise RuntimeError("the hash ring has no nodes")
        if count < 1:
            return []

        start = bisect_left(self._tokens, self.hash_value(key))
        owners: list[str] = []
        for offset in range(len(self._tokens)):
            token = self._tokens[(start + offset) % len(self._tokens)]
            owner = self._owners[token]
            if owner not in owners:
                owners.append(owner)
                if len(owners) == min(count, len(self._nodes)):
                    break
        return owners

    def primary_for(self, key: str) -> str:
        return self.owners_for(key, 1)[0]
