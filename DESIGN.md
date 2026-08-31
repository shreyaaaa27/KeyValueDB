# Architecture & Design: Distributed Key-Value Store (Raft)

## 1. Problem Statement

In a distributed key-value store running across multiple machines, nodes can crash, experience network delays, or drop packets. Without coordination, these machines will diverge and store conflicting data.

This project uses the **Raft Consensus Algorithm** to solve this problem. Raft ensures that a cluster of nodes agrees on a single, sequentially ordered log of operations (`SET`, `DELETE`), guaranteeing strong consistency (linearizability) across the system even during node failures.

---

## 2. Cluster Size & Quorum Logic

This system is configured to run as a **3-node cluster** (`node1`, `node2`, `node3`).

* **Total Nodes ($N$):** $3$
* **Quorum / Majority Threshold:** $\lfloor N/2 \rfloor + 1 = 2$

### Fault Tolerance

Because a quorum of **2 nodes** is required to elect a leader and commit write operations, the system can tolerate the failure of **1 node** without going offline. If 2 nodes fail, the remaining node cannot achieve a majority and will safely refuse to process new writes to prevent data corruption.

---

## 3. Node States & Life Cycle

Every node in the cluster operates as an independent state machine and exists in exactly one of three states at any given moment:

```
                      [Heartbeat timeout, starts election]
       +------------+ -----------------------------------> +-----------+
       |  Follower  |                                      | Candidate |
       +------------+ <----------------------------------- +-----------+
             ^        [Discovers existing Leader            |
             |         or higher term]                      | [Gains >= 2 votes]
             |                                              v
             +---------------------------------------- +-----------+
                 [Discovers node with higher term]     |  Leader   |
                                                       +-----------+

```

1. **Follower:**
* The default initial state for all nodes.
* Passively responds to RPCs from candidates and leaders.
* If a follower receives no communications (heartbeats) before its **randomized election timeout** expires, it transitions to a Candidate.


2. **Candidate:**
* Increments its `current_term`, votes for itself, and broadcasts `RequestVote` RPCs to all peers.
* If it receives votes from a majority of nodes (2 out of 3), it becomes the Leader.
* If it receives a heartbeat from a valid Leader or sees a higher term, it steps down back to a Follower.


3. **Leader:**
* Handles all incoming client write requests (`SET key value`).
* Appends operations to its local log and replicates them to followers via `AppendEntries`.
* Sends periodic heartbeats (empty `AppendEntries` payloads) to suppress new elections.



---

## 4. Key Node State Variables

Each node tracks the following state internally:

* **`current_term`** *(Integer)*: Monotonically increasing counter serving as a logical clock to detect outdated nodes or leaders.
* **`voted_for`** *(Node ID)*: Tracks which candidate received this node's vote in the current term (prevents double-voting).
* **`log[]`** *(Array of Log Entries)*: Contains commands (`SET x=10`) paired with the term number in which the entry was created.
* **`commit_index`** *(Integer)*: Index of the highest log entry confirmed by a majority to be safe to apply to the local Key-Value store.

---

## 5. Consensus Protocol: Inter-Node RPCs

Nodes communicate using two core RPCs:

### A. `RequestVote` RPC

Invoked by Candidates during an election to gather votes from peers.

* **Arguments Sent:**
* `term`: Candidate's current term.
* `candidate_id`: ID of the candidate requesting the vote.
* `last_log_index`: Index of candidate’s last log entry.
* `last_log_term`: Term of candidate’s last log entry.


* **Response:**
* `term`: Receiver's `current_term` (so candidate can update itself if out of date).
* `vote_granted`: `true` if candidate receives the vote, `false` otherwise.


* **Voting Rules:** A node grants its vote **only if** the candidate’s term is $\ge$ its own `current_term`, the node hasn't already voted for someone else in this term, and the candidate's log is at least as up-to-date as the voter's log.

### B. `AppendEntries` RPC

Invoked by the Leader to replicate log entries and maintain authority (heartbeats).

* **Arguments Sent:**
* `term`: Leader's current term.
* `leader_id`: ID of the leader (allows followers to redirect client requests).
* `prev_log_index`: Index of log entry immediately preceding new ones.
* `prev_log_term`: Term of `prev_log_index` entry.
* `entries[]`: Array of log entries to store (empty for heartbeats).
* `leader_commit`: Leader’s `commit_index`.


* **Response:**
* `term`: Receiver's `current_term`.
* `success`: `true` if follower contained entry matching `prev_log_index` and `prev_log_term`.



---

## 6. End-to-End Write Flow (`SET key value`)

1. **Client Request:** Client sends `SET x=100` to a node.
2. **Leader Redirect:** If the node is a Follower, it rejects or forwards the request to the current Leader.
3. **Local Append:** The Leader appends `SET x=100` to its local `log[]`.
4. **Replication:** Leader sends `AppendEntries` containing the new entry to `node2` and `node3`.
5. **Follower Acknowledgment:** Once **1 follower** acknowledges success (making a total of **2 nodes**—a quorum), the Leader advances its `commit_index`.
6. **Apply & Respond:** The Leader applies `SET x=100` to its internal Key-Value map, returns success to the client, and informs the remaining followers to commit the entry on subsequent heartbeats.

---