from node.raft_node import RaftNode
from node.models import AppendEntriesRequest, LogEntry


def test_heartbeat_resets_to_follower():
    node = RaftNode("node1", [])
    node.current_term = 1
    req = AppendEntriesRequest(term=2, leader_id="node2", prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
    term, success = node.handle_append_entries(req)
    assert success is True
    assert node.current_term == 2


def test_rejects_stale_leader():
    node = RaftNode("node1", [])
    node.current_term = 5
    req = AppendEntriesRequest(term=3, leader_id="node2", prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
    term, success = node.handle_append_entries(req)
    assert success is False


def test_appends_new_entries():
    node = RaftNode("node1", [])
    entry = LogEntry(term=1, command={"op": "SET", "key": "x", "value": "5"})
    req = AppendEntriesRequest(term=1, leader_id="node2", prev_log_index=0, prev_log_term=0, entries=[entry], leader_commit=0)
    node.handle_append_entries(req)
    assert len(node.log) == 1
    assert node.log[0].command["key"] == "x"