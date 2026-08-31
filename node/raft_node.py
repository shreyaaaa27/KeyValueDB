import random
from enum import Enum
from node.models import LogEntry


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