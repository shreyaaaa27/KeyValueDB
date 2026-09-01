import time
import httpx

leader_url = "http://localhost:8003"  # set to your current leader
N = 200

start = time.monotonic()
for i in range(N):
    httpx.post(f"{leader_url}/kv/key{i}", json={"value": str(i)}, timeout=5.0)
elapsed = time.monotonic() - start

print(f"{N} writes in {elapsed:.2f}s -> {N/elapsed:.1f} writes/sec")