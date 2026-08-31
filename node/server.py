from fastapi import FastAPI
import os
from node.raft_node import RaftNode
from node.models import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse

NODE_ID = os.getenv("NODE_ID", "node1")
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []

app = FastAPI(title=f"KeyValueDB - {NODE_ID}")
raft = RaftNode(NODE_ID, PEERS)


@app.get("/status")
def status():
    return {
        "node_id": raft.node_id,
        "state": raft.state.value,
        "term": raft.current_term,
        "log_length": len(raft.log),
    }


@app.post("/request_vote", response_model=RequestVoteResponse)
def request_vote(req: RequestVoteRequest):
    raise NotImplementedError  # Day 12


@app.post("/append_entries", response_model=AppendEntriesResponse)
def append_entries(req: AppendEntriesRequest):
    raise NotImplementedError  # Day 13