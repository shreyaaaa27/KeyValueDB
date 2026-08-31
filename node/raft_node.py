import random
from enum import Enum
from node.models import LogEntry
import asyncio
import httpx
from node.models import RequestVoteRequest
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, node_id: str, peers: list[str]):
        self.node_id = node_id
        self.peers = peers  # e.g. ["http://node2:8000", "http://node3:8000"]

        # Persistent state (would survive a restart in a real system)
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
            print(f"[{self.node_id}] Elected leader for term {self.current_term} with {votes} votes")