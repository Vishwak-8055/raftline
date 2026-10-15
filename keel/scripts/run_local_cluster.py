"""Launches a local N-node Keel cluster as subprocesses and leaves it
running so you can poke at it manually (curl, browser, etc). Ctrl+C to
stop everything cleanly.

    python3 scripts/run_local_cluster.py
    curl http://localhost:8001/status
    curl -X PUT http://localhost:8001/kv/foo -d '{"value": 42}'
"""
from __future__ import annotations

import json
import signal
import subprocess
import sys
import time

CONFIG_PATH = "cluster.json"


def main() -> None:
    with open(CONFIG_PATH) as f:
        nodes: dict[str, str] = json.load(f)["nodes"]

    procs = []
    for node_id, addr in nodes.items():
        port = int(addr.rsplit(":", 1)[1])
        cmd = [
            sys.executable, "-m", "keel.server",
            "--id", node_id, "--config", CONFIG_PATH,
            "--data-dir", f"./data/{node_id}", "--port", str(port),
        ]
        print(f"starting {node_id} on port {port}")
        procs.append(subprocess.Popen(cmd))
        time.sleep(0.2)

    def shutdown(*_args) -> None:
        print("\nshutting down cluster...")
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("cluster running — Ctrl+C to stop.")
    for p in procs:
        p.wait()


if __name__ == "__main__":
    main()
