import random
from enum import Enum
from node.models import LogEntry
import asyncio
import httpx
from node.models import RequestVoteRequest
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, node_id: str, peers: list[str], address: str = ""):        
        self.node_id = node_id
        self.peers = peers  # e.g. ["http://node2:8000", "http://node3:8000"]

        # Persistent state (would survive a restart in a real system)
        self.address = address or f"http://{node_id}:8000"        
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER

        # Leader-only state (reset on becoming leader)
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.last_heartbeat_received = time.monotonic()
        self.state_machine: dict[str, str] = {}
        self.leader_id: str | None = None
        self.leader_address: str | None = None  # e.g. "http://node2:8000"


    def random_election_timeout(self) -> float:
        # Randomized so all nodes don't call an election simultaneously
        return random.uniform(1.5, 3.0)

    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def handle_request_vote(self, req: RequestVoteRequest) -> tuple[int, bool]:
        # Step down if we see a newer term
        if req.term > self.current_term:
            self.current_term = req.term
            self.voted_for = None
            self.state = NodeState.FOLLOWER

        if req.term < self.current_term:
            return self.current_term, False

        log_ok = (
            req.last_log_term > self.last_log_term()
            or (req.last_log_term == self.last_log_term() and req.last_log_index >= self.last_log_index())
        )

        if (self.voted_for is None or self.voted_for == req.candidate_id) and log_ok:
            self.voted_for = req.candidate_id
            self.last_heartbeat_received = time.monotonic()
            return self.current_term, True

        return self.current_term, False

    async def start_election(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1  # vote for self

        request = RequestVoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index(),
            last_log_term=self.last_log_term(),
        )

        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks = [client.post(f"{peer}/request_vote", json=request.model_dump()) for peer in self.peers]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for resp in responses:
            if isinstance(resp, Exception):
                continue  # peer unreachable — just doesn't count toward votes
            data = resp.json()
            if data["term"] > self.current_term:
                self.current_term = data["term"]
                self.state = NodeState.FOLLOWER
                return
            if data["vote_granted"]:
                votes += 1

        majority = (len(self.peers) + 1) // 2 + 1
        if votes >= majority and self.state == NodeState.CANDIDATE:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            self.leader_address = self.address  # Point to self
            print(f"[{self.node_id}] Elected leader for term {self.current_term} with {votes} votes")

    def handle_append_entries(self, req) -> tuple[int, bool]:
        if req.term < self.current_term:
            return self.current_term, False

        # Valid leader contact — reset to follower and remember we heard from them
        self.current_term = req.term
        self.state = NodeState.FOLLOWER
        self.last_heartbeat_received = time.monotonic()
        self.leader_id = req.leader_id

        if hasattr(req, "leader_address"):
            self.leader_address = req.leader_address

        # Consistency check
        if req.prev_log_index > 0:
            if len(self.log) < req.prev_log_index:
                return self.current_term, False
            if self.log[req.prev_log_index - 1].term != req.prev_log_term:
                return self.current_term, False

        # Append new entries (overwrite conflicts, if any)
        if req.entries:
            self.log = self.log[:req.prev_log_index] + req.entries

        # Update commit index (capped at our own log length) — full logic Day 14
        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, len(self.log))
            self.apply_committed_entries()

        return self.current_term, True

    async def send_heartbeats(self):
        """Called on a timer (empty entries = pure heartbeat) and after client writes (real entries)."""
        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks = []
            peer_info = []

            for peer in self.peers:
                next_idx = self.next_index.get(peer, len(self.log) + 1)
                prev_log_index = next_idx - 1
                prev_log_term = self.log[prev_log_index - 1].term if 0 < prev_log_index <= len(self.log) else 0
                entries_to_send = self.log[prev_log_index:]  # everything the follower is missing

                payload = {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "leader_address": self.address,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": [e.model_dump() for e in entries_to_send],
                    "leader_commit": self.commit_index,
                }
                tasks.append(client.post(f"{peer}/append_entries", json=payload))
                peer_info.append((peer, prev_log_index, len(entries_to_send)))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for (peer, prev_log_index, num_sent), resp in zip(peer_info, responses):
            if isinstance(resp, Exception):
                continue  # peer unreachable, leave next_index/match_index unchanged, retry next round
            data = resp.json()
            if data.get("success"):
                self.match_index[peer] = prev_log_index + num_sent
                self.next_index[peer] = self.match_index[peer] + 1
            else:
                # follower rejected — back off next_index by 1 and retry earlier next round
                self.next_index[peer] = max(1, self.next_index.get(peer, len(self.log) + 1) - 1)  

    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            cmd = entry.command
            if cmd["op"] == "SET":
                self.state_machine[cmd["key"]] = cmd["value"]
            elif cmd["op"] == "DELETE":
                self.state_machine.pop(cmd["key"], None)

    def advance_commit_index_if_majority(self):
        """Leader-only: find the highest index replicated on a majority."""
        if self.state != NodeState.LEADER:
            return
        majority = (len(self.peers) + 1) // 2 + 1
        for index in range(len(self.log), self.commit_index, -1):
            count = 1  # leader itself
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= index:
                    count += 1
            if count >= majority and self.log[index - 1].term == self.current_term:
                self.commit_index = index
                break
        self.apply_committed_entries()

    async def client_write(self, op: str, key: str, value: str = None) -> bool:
        if self.state != NodeState.LEADER:
            return False
        entry = LogEntry(term=self.current_term, command={"op": op, "key": key, "value": value})
        self.log.append(entry)
        await self.send_heartbeats()          # now actually replicates the new entry
        self.advance_commit_index_if_majority()  # now has real match_index data to work with
        return True             