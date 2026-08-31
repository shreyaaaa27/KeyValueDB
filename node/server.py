from fastapi import FastAPI
import os
import asyncio
from node.raft_node import RaftNode,NodeState
from node.models import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    term, granted = raft.handle_request_vote(req)
    return RequestVoteResponse(term=term, vote_granted=granted)

@app.post("/append_entries", response_model=AppendEntriesResponse)
def append_entries(req: AppendEntriesRequest):
    term, success = raft.handle_append_entries(req)
    return AppendEntriesResponse(term=term, success=success)


@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(election_timer_loop())


async def election_timer_loop():
    while True:
        try:
            await asyncio.sleep(raft.random_election_timeout())
            if raft.state != NodeState.LEADER:
                await raft.start_election()
        except Exception:
            logger.exception("election_timer_loop crashed")