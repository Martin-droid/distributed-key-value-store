import unittest

from distributed_store.cluster import DistributedKeyValueStore, NodeUnavailableError


class DistributedStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DistributedKeyValueStore(
            ["node-a", "node-b", "node-c"],
            virtual_nodes=32,
            replication_factor=2,
            cache_capacity=2,
        )
        for number, name in enumerate(
            ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"], start=101
        ):
            self.store.put(f"user:{number}", {"name": name})

    def test_values_are_available_through_transparent_api(self) -> None:
        self.assertEqual(self.store.get("user:103"), {"name": "Charlie"})

    def test_second_read_uses_node_cache(self) -> None:
        first = self.store.get("user:101", detailed=True)
        second = self.store.get("user:101", detailed=True)
        self.assertFalse(first.cache_hit)
        self.assertTrue(second.cache_hit)

    def test_primary_failure_falls_back_to_replica(self) -> None:
        key = "user:104"
        primary = self.store.placement()[key][0]
        self.store.fail_node(primary)
        result = self.store.get(key, detailed=True)
        self.assertTrue(result.used_failover)
        self.assertNotEqual(result.served_by, primary)
        self.assertEqual(result.value, {"name": "Diana"})

    def test_all_replicas_unavailable_raises_clear_error(self) -> None:
        key = "user:102"
        for owner in self.store.placement()[key]:
            self.store.fail_node(owner)
        with self.assertRaises(NodeUnavailableError):
            self.store.get(key)

    def test_adding_node_preserves_values_and_only_moves_some_primaries(self) -> None:
        before = self.store.placement()
        moved = self.store.add_node("node-d")
        self.assertLess(len(moved), len(before))
        for key in before:
            self.assertEqual(self.store.get(key)["name"], self.store._records[key]["name"])

    def test_removing_node_preserves_values(self) -> None:
        self.store.remove_node("node-b")
        for key in self.store.placement():
            self.assertEqual(self.store.get(key), self.store._records[key])

    def test_updates_invalidate_stale_cached_values(self) -> None:
        self.store.get("user:105")
        self.store.put("user:105", {"name": "Eva"})
        self.assertEqual(self.store.get("user:105"), {"name": "Eva"})

    def test_delete_removes_record_from_all_nodes(self) -> None:
        self.store.delete("user:106")
        with self.assertRaises(KeyError):
            self.store.get("user:106")
        self.assertTrue(all("user:106" not in node.data for node in self.store.nodes.values()))


if __name__ == "__main__":
    unittest.main()
