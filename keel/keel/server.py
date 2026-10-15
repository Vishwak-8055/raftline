"""Entry point: run a single Keel node as an HTTP service.

Usage:
    # A node that's part of the initial, statically-configured cluster:
    python -m keel.server --id node1 --config cluster.json --port 8001

    # A brand-new node meant to be added to an already-running cluster
    later via POST /admin/add_node on the leader — it starts knowing
    about no one, and learns the cluster (and catches up on data) once
    the leader is told about it:
    python -m keel.server --id node6 --port 8006
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging

import uvicorn

from .node import RaftNode
from .state import PersistentState
from .state_machine import KVStateMachine
from .transport import HttpTransport, build_raft_app


def load_cluster_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


async def run_node(node_id: str, config_path: str | None, data_dir: str, port: int) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if config_path:
        config = load_cluster_config(config_path)
        all_nodes: dict[str, str] = config["nodes"]
        peer_ids = [n for n in all_nodes if n != node_id]
        peer_addresses = {n: addr for n, addr in all_nodes.items() if n != node_id}
    else:
        # Joining later: no known peers yet. The leader learns our
        # address (and we learn everyone else's) once someone calls
        # POST /admin/add_node on the leader with our id and address.
        peer_ids = []
        peer_addresses = {}

    state = PersistentState(node_id, data_dir)
    state_machine = KVStateMachine()
    transport = HttpTransport(peer_addresses)
    node = RaftNode(node_id, peer_ids, state, transport, state_machine)

    app = build_raft_app(node)
    server_config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(server_config)

    await asyncio.gather(server.serve(), node.run())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single Keel (Raft) node.")
    parser.add_argument("--id", required=True, help="This node's id")
    parser.add_argument(
        "--config", required=False, default=None,
        help="Path to cluster.json. Omit to start with no initial peers (a node "
             "waiting to be added to an existing cluster via /admin/add_node).",
    )
    parser.add_argument("--data-dir", default="./data", help="Directory for this node's persistent state")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    asyncio.run(run_node(args.id, args.config, args.data_dir, args.port))


if __name__ == "__main__":
    main()
