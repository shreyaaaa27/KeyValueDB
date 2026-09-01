from fastapi import FastAPI
import os
import asyncio
from node.raft_node import RaftNode,NodeState
from node.models import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse
import logging
import time
from fastapi import HTTPException


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


NODE_ID = os.getenv("NODE_ID", "node1")
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []

app = FastAPI(title=f"KeyValueDB - {NODE_ID}")
NODE_ADDRESS = f"http://{NODE_ID}:8000"
raft = RaftNode(NODE_ID, PEERS, address=NODE_ADDRESS)


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


async def heartbeat_loop():
    while True:
        if raft.state == NodeState.LEADER:
            await raft.send_heartbeats()
        await asyncio.sleep(0.5)  # heartbeat every 500ms — well under the election timeout


@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(election_timer_loop())
    asyncio.create_task(heartbeat_loop())

async def election_timer_loop():
    while True:
        timeout = raft.random_election_timeout()
        await asyncio.sleep(timeout)

        if raft.state == NodeState.LEADER:
            continue  # leaders don't run elections on themselves

        elapsed = time.monotonic() - raft.last_heartbeat_received
        if elapsed >= timeout:
            await raft.start_election()
        # else: a heartbeat (or vote grant) arrived during our sleep — 
        # loop back and wait a fresh random timeout instead of electing

@app.post("/kv/{key}")
async def set_key(key: str, value: dict):
    if raft.state.value != "leader":
        raise HTTPException(
            status_code=421,
            detail={"message": "Not the leader", "leader_address": raft.leader_address},
        )
    success = await raft.client_write("SET", key, value["value"])
    return {"success": success}


@app.get("/kv/{key}")
def get_key(key: str):
    if key not in raft.state_machine:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": raft.state_machine[key]}


@app.delete("/kv/{key}")
async def delete_key(key: str):
    if raft.state.value != "leader":
        raise HTTPException(status_code=421, detail="Not the leader — retry another node")
    success = await raft.client_write("DELETE", key)
    return {"success": success}