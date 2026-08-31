from node.raft_node import RaftNode, NodeState
from node.models import RequestVoteRequest


def test_grants_vote_when_eligible():
    node = RaftNode("node1", [])
    req = RequestVoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
    term, granted = node.handle_request_vote(req)
    assert granted is True
    assert node.voted_for == "node2"


def test_rejects_vote_for_stale_term():
    node = RaftNode("node1", [])
    node.current_term = 5
    req = RequestVoteRequest(term=3, candidate_id="node2", last_log_index=0, last_log_term=0)
    term, granted = node.handle_request_vote(req)
    assert granted is False


def test_does_not_vote_twice_in_same_term():
    node = RaftNode("node1", [])
    req1 = RequestVoteRequest(term=1, candidate_id="node2", last_log_index=0, last_log_term=0)
    node.handle_request_vote(req1)
    req2 = RequestVoteRequest(term=1, candidate_id="node3", last_log_index=0, last_log_term=0)
    term, granted = node.handle_request_vote(req2)
    assert granted is False