# KeyValueDB

A distributed, fault-tolerant key-value store built from scratch using the Raft consensus algorithm — implements leader election, log replication, and quorum-based commit safety across a 3-node cluster.

---

## Architecture

```text
[Client] ──> Any Node ──> (421 Redirect if not leader) ──> [Leader]
                                                               │
                                                               ├─ AppendEntries (Replicate) ──> [Follower 1]
                                                               └─ AppendEntries (Replicate) ──> [Follower 2]
                                                               │
                                                               └─ Majority Ack (Quorum)
                                                                       │
                                                        [Committed & Applied to State Machine]
Key Features
Leader Election: Randomized election timeouts (1.5s–3.0s) to prevent split-vote livelocks.

Log Replication: Strict log matching consistency checks (prevLogIndex and prevLogTerm).

Quorum-Based Commit Safety: Writes are committed and applied to the state machine only after a majority of nodes acknowledge persistence.

Automatic Failover & Redirection: Automatic leader election on node crash, accompanied by HTTP 421 Misdirected Request client redirection to the active leader.

Developer Workflows: Includes an interactive CLI client and automated benchmarking / fault-injection test scripts.

Performance & Benchmarks
Tests were conducted locally on macOS against a containerized 3-node Raft cluster.

Failover Recovery Time: 3.55s average (range 2.71s–5.16s across 6 test runs, bounded by the 1.5–3.0s randomized election timeout).

Write Throughput: 114.0 writes/sec (single-client, unbatched — 200 sequential SET operations reflecting consensus RPC round-trip overhead).

Tech Stack
Language & Framework: Python 3.11+, FastAPI, Uvicorn

Containerization: Docker, Docker Compose

Networking & Data Validation: Pydantic v2, HTTPX

Testing: Pytest, Asyncio

Running Locally
1. Start the Cluster
Spin up the 3-node Raft cluster (node1:8001, node2:8002, node3:8003):

Bash
docker compose up --build
2. Interact via CLI Client
In a new terminal window, use the CLI client to interact with the database:

Bash
# Set a key-value pair
python3 client/cli.py set mykey myvalue

# Get a key value
python3 client/cli.py get mykey http://localhost:8001

# Delete a key
python3 client/cli.py delete mykey
3. Run Automated Tests & Benchmarks
Bash
# Run unit & integration test suite
docker compose run --rm node1 pytest tests/ -v

# Run throughput benchmark test
python3 scripts/throughput_test.py

# Run fault-injection failure test (kills leader node & measures recovery time)
python3 scripts/failure_test.py
Known Limitations
(By design, for a portfolio-scoped implementation)

In-Memory Storage: Log and state machine are stored in-memory (a container restart resets state; production Raft persists the log to disk before responding to RPCs).

No Log Compaction: Does not implement log snapshotting; memory usage grows linearly with the number of entries.

Static Cluster Membership: Cluster size is fixed at 3 nodes (no dynamic node addition/removal).

License
Distributed under the MIT License. See LICENSE for more information.
