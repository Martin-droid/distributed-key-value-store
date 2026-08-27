"""Command-line demonstration of the distributed store."""

import json

from .cluster import DistributedKeyValueStore


SAMPLE_USERS = {
    "user:101": {"name": "Alice"},
    "user:102": {"name": "Bob"},
    "user:103": {"name": "Charlie"},
    "user:104": {"name": "Diana"},
    "user:105": {"name": "Eve"},
    "user:106": {"name": "Frank"},
}


def show_placement(store: DistributedKeyValueStore) -> None:
    for key, owners in store.placement().items():
        print(f"  {key:<9} primary={owners[0]} replicas={owners[1:]}")


def main() -> None:
    store = DistributedKeyValueStore(
        ["node-a", "node-b", "node-c"],
        replication_factor=2,
        cache_capacity=2,
    )
    for key, value in SAMPLE_USERS.items():
        store.put(key, value)

    print("Initial placement")
    show_placement(store)

    print("\nTransparent reads and caching")
    for attempt in range(1, 3):
        result = store.get("user:101", detailed=True)
        print(
            f"  attempt={attempt} value={json.dumps(result.value)} "
            f"served_by={result.served_by} cache_hit={result.cache_hit}"
        )

    print("\nAdding node-d")
    moved = store.add_node("node-d")
    print(f"  remapped primary keys: {len(moved)}/{len(SAMPLE_USERS)}")
    show_placement(store)

    key = "user:104"
    primary = store.placement()[key][0]
    store.fail_node(primary)
    result = store.get(key, detailed=True)
    print(f"\nFailure simulation for {key}")
    print(
        f"  failed_primary={primary} served_by={result.served_by} "
        f"used_failover={result.used_failover} value={json.dumps(result.value)}"
    )

    print("\nRemoving node-b")
    moved = store.remove_node("node-b")
    print(f"  remapped primary keys: {len(moved)}/{len(SAMPLE_USERS)}")
    show_placement(store)


if __name__ == "__main__":
    main()
