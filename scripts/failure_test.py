import time
import httpx
import subprocess

NODES = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003",
}


def get_status(url):
    try:
        return httpx.get(f"{url}/status", timeout=1.0).json()
    except Exception:
        return None


def find_leader():
    for name, url in NODES.items():
        status = get_status(url)
        if status and status["state"] == "leader":
            return name, status["term"]
    return None, None


def main():
    print("Finding current leader...")
    leader_name, term = find_leader()
    print(f"Leader: {leader_name} (term {term})")

    print(f"Killing {leader_name}...")
    kill_time = time.monotonic()
    subprocess.run(["docker", "compose", "stop", leader_name], check=True)

    new_leader = None
    while new_leader is None:
        time.sleep(0.2)
        for name, url in NODES.items():
            if name == leader_name:
                continue
            status = get_status(url)
            if status and status["state"] == "leader":
                new_leader = name
                new_term = status["term"]
                break

    recovery_time = time.monotonic() - kill_time
    print(f"New leader elected: {new_leader} (term {new_term})")
    print(f"Recovery time: {recovery_time:.2f} seconds")

    print(f"Restarting {leader_name}...")
    subprocess.run(["docker", "compose", "start", leader_name], check=True)


if __name__ == "__main__":
    main()