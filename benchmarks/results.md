# KeyValueDB Benchmarks

## Write Throughput
- 200 sequential SET operations against the leader
- Result: 114.0 writes/sec (single-client, unbatched — reflects consensus overhead per write)

## Failover Recovery Time
- Killed the current leader mid-operation, measured time to new leader election
- Result: 3.55s average across 6 runs (bounded by the 1.5-3.0s randomized election timeout)