import unittest

from distributed_store.cache import LRUCache


class LRUCacheTests(unittest.TestCase):
    def test_least_recently_used_item_is_evicted(self) -> None:
        cache = LRUCache[str, int](2)
        cache.put("a", 1)
        cache.put("b", 2)
        self.assertEqual(cache.get("a"), 1)
        cache.put("c", 3)
        self.assertEqual(cache.keys(), ["a", "c"])
        self.assertIsNone(cache.get("b"))

    def test_invalid_capacity_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            LRUCache(0)


if __name__ == "__main__":
    unittest.main()
