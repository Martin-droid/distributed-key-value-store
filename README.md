# Distributed Key-Value Store

A small, dependency-free simulation of a distributed key-value store. It demonstrates:

- consistent hashing with configurable virtual nodes;
- dynamic node joins and graceful node removal;
- limited availability during node failures through replication;
- a per-node least-recently-used (LRU) cache;
- a transparent client API that hides node placement and failover details.

## Requirements

- Python 3.9 or newer

## Run the demonstration

```bash
python3 -m distributed_store.demo
```

The demonstration loads six sample users, prints their primary placement, shows cache hits and eviction, adds and removes nodes, and simulates a primary-node failure.

## Run the tests

```bash
python3 -m unittest discover -s tests -v
```

## Design

`HashRing` maps keys and virtual-node tokens into a fixed hash space. A key belongs to the first token clockwise from its hash. Adding or removing a physical node only changes ownership for the affected portion of the ring.

`DistributedKeyValueStore` stores each record on the primary node and the next distinct nodes on the ring according to the replication factor. Reads use the primary when available and transparently fall back to a replica when a node is unavailable. A recovered node is rebuilt from the authoritative in-memory record set.

Every node has an independent `LRUCache`. Cache entries are invalidated when a value changes, so clients do not receive stale data after updates.

This is an educational in-process simulation rather than a production database. A production implementation would also need persistent storage, network protocols, consensus, authentication, metrics, and stronger consistency guarantees.
