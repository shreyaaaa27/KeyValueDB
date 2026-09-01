import sys
import httpx

NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]


def find_leader():
    for url in NODES:
        try:
            status = httpx.get(f"{url}/status", timeout=1.0).json()
            if status["state"] == "leader":
                return url
        except Exception:
            continue
    return None


def set_key(key, value):
    leader = find_leader()
    if not leader:
        print("No leader found — cluster may be electing.")
        return
    resp = httpx.post(f"{leader}/kv/{key}", json={"value": value})
    print(resp.json())


def get_key(key, node_url=None):
    url = node_url or NODES[0]
    resp = httpx.get(f"{url}/kv/{key}")
    print(resp.json())


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "set":
        set_key(sys.argv[2], sys.argv[3])
    elif cmd == "get":
        get_key(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
    else:
        print("Usage: cli.py set <key> <value>  |  cli.py get <key> [node_url]")