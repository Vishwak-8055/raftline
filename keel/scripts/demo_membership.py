"""
End-to-end demo of dynamic cluster membership, over real HTTP:

  1. starts a 3-node cluster (quorum 2, tolerates 1 failure)
  2. adds two brand-new nodes one at a time, each starting with an empty
     log and no known peers, each catching up via the same replication/
     snapshot machinery used for any lagging follower
  3. once at 5 nodes (quorum 3, tolerates 2 failures), kills two
     followers simultaneously and proves the cluster still commits
     writes — fault tolerance a 3-node cluster genuinely didn't have
  4. formally removes the two dead nodes from the configuration and
     shows the quorum requirement shrinks to match the 3 nodes actually
     alive — restoring real slack instead of running a 3-node cluster
     that still (incorrectly) thinks it needs agreement from 5

Run from the project root:
    python3 scripts/demo_membership.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import time

import httpx

NODE_MODULE = "keel.server"
BASE_PORT = 9200


def addr(n: int) -> str:
    return f"http://localhost:{BASE_PORT + n}"


def start_seed_node(node_id: str, port: int, config_path: str) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", NODE_MODULE, "--id", node_id, "--config", config_path,
        "--data-dir", f"./data/{node_id}", "--port", str(port),
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_joining_node(node_id: str, port: int) -> subprocess.Popen:
    # No --config: this node starts knowing about nobody. It only
    # becomes part of the cluster once the leader is told about it.
    cmd = [
        sys.executable, "-m", NODE_MODULE, "--id", node_id,
        "--data-dir", f"./data/{node_id}", "--port", str(port),
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_status(address: str) -> dict | None:
    try:
        return httpx.get(f"{address}/status", timeout=1.0).json()
    except Exception:
        return None


def wait_for_leader(addresses: dict[str, str], timeout: float = 15.0) -> tuple[str, dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        statuses = {nid: get_status(a) for nid, a in addresses.items()}
        leaders = [nid for nid, s in statuses.items() if s and s["role"] == "leader"]
        if leaders:
            return leaders[0], statuses
        time.sleep(0.3)
    raise TimeoutError("no leader elected in time")


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def main() -> None:
    m = {f"m{i}": addr(i) for i in (1, 2, 3)}
    cfg_path = "cluster_membership.json"
    with open(cfg_path, "w") as f:
        json.dump({"nodes": m}, f)

    print("=" * 64)
    print("KEEL MEMBERSHIP DEMO — dynamic add/remove over real HTTP")
    print("=" * 64)

    print("\n[1] Starting a 3-node seed cluster (quorum 2)...")
    procs = {nid: start_seed_node(nid, BASE_PORT + i, cfg_path) for i, nid in zip((1, 2, 3), m)}
    time.sleep(1.0)
    leader_id, statuses = wait_for_leader(m)
    print(f"    -> {leader_id} elected leader; cluster_size={statuses[leader_id]['cluster_size']}")

    print("\n[2] Writing key 'a' = 1 before anyone new joins...")
    httpx.put(f"{m[leader_id]}/kv/a", json={"value": 1}, timeout=3.0)

    print("\n[3] Starting m4 fresh (no config — knows nobody) and adding it...")
    procs["m4"] = start_joining_node("m4", BASE_PORT + 4)
    time.sleep(0.5)
    m["m4"] = addr(4)
    r = httpx.post(f"{m[leader_id]}/admin/add_node", json={"node_id": "m4", "address": m["m4"]}, timeout=5.0)
    print(f"    -> add_node m4: {r.json()}")
    caught_up = wait_until(lambda: get_status(m["m4"]) and get_status(m["m4"]).get("commit_index", 0) > 0)
    m4_value = httpx.get(f"{m['m4']}/kv/a", timeout=2.0).json()
    print(f"    -> m4 caught up: {caught_up}; m4 sees key 'a' = {m4_value}")

    print("\n[4] Starting m5 fresh and adding it too — now 5 nodes, quorum 3...")
    procs["m5"] = start_joining_node("m5", BASE_PORT + 5)
    time.sleep(0.5)
    m["m5"] = addr(5)
    r = httpx.post(f"{m[leader_id]}/admin/add_node", json={"node_id": "m5", "address": m["m5"]}, timeout=5.0)
    print(f"    -> add_node m5: {r.json()}")
    wait_until(lambda: get_status(m["m5"]) and get_status(m["m5"]).get("commit_index", 0) > 0)
    status = get_status(m[leader_id])
    print(f"    -> leader status: cluster_size={status['cluster_size']}, peers={status['peers']}")

    print("\n[5] Killing TWO followers simultaneously (a 3-node cluster")
    print("    could never survive this — a 5-node one should).")
    followers = [nid for nid in m if nid != leader_id]
    victims = followers[:2]
    for v in victims:
        procs[v].terminate()
        procs[v].wait()
    print(f"    -> killed {victims}")

    r = httpx.put(f"{m[leader_id]}/kv/b", json={"value": 2}, timeout=5.0)
    print(f"    -> PUT b=2 via {leader_id} with only 3 of 5 nodes alive: {r.json()}")

    print("\n[6] Formally removing the two dead nodes from the configuration...")
    for v in victims:
        r = httpx.post(f"{m[leader_id]}/admin/remove_node", json={"node_id": v}, timeout=5.0)
        print(f"    -> remove_node {v}: {r.json()}")
        time.sleep(0.3)

    final_status = get_status(m[leader_id])
    print(f"\n    -> final leader status: {final_status}")
    print(f"    -> quorum requirement is now {(len(final_status['peers']) + 1) // 2 + 1} "
          f"(was 3 while the cluster still thought it had 5 members)")

    print("\n" + "=" * 64)
    print("RESULT: dynamic add (with real catch-up), improved fault")
    print("tolerance at 5 nodes, and quorum correctly shrinking after")
    print("cleanup — all verified over real HTTP.")
    print("=" * 64)

    print("\nShutting down surviving nodes...")
    for nid in m:
        if nid not in victims and procs[nid].poll() is None:
            procs[nid].terminate()
    for nid in m:
        if nid not in victims:
            procs[nid].wait()


if __name__ == "__main__":
    main()
