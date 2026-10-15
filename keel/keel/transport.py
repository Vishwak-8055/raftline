"""HTTP transport for Raft RPCs, plus the client-facing KV API.

Two directions of traffic share one FastAPI app per node:
  - inbound: peers POST to /raft/request_vote, /raft/append_entries,
    and /raft/install_snapshot
  - outbound: HttpTransport uses httpx to make those same calls to peers

Clients (or a proxy) talk to /kv/{key} and /status, which are plain
HTTP/JSON — no Raft knowledge required on the client side beyond "retry
against the leader_hint if you get success: false".
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    AppendEntriesArgs,
    AppendEntriesResult,
    ConfigChangeRequest,
    InstallSnapshotArgs,
    InstallSnapshotResult,
    PreVoteArgs,
    PreVoteResult,
    PutValueRequest,
    RequestVoteArgs,
    RequestVoteResult,
)
from .node import RaftNode, Transport

logger = logging.getLogger("raft.transport")


class HttpTransport(Transport):
    """Outbound RPC client: sends Raft RPCs to peers over HTTP."""

    def __init__(self, peer_addresses: dict[str, str], timeout: float = 1.0):
        # peer_addresses example: {"node2": "http://localhost:8002"}
        self.peer_addresses = peer_addresses
        self._client = httpx.AsyncClient(timeout=timeout)

    async def send_pre_vote(self, peer_id: str, args: PreVoteArgs) -> Optional[PreVoteResult]:
        url = f"{self.peer_addresses[peer_id]}/raft/pre_vote"
        try:
            resp = await self._client.post(url, json=args.model_dump())
            resp.raise_for_status()
            return PreVoteResult(**resp.json())
        except Exception as e:
            logger.debug("PreVote -> %s failed: %s", peer_id, e)
            return None

    async def send_request_vote(self, peer_id: str, args: RequestVoteArgs) -> Optional[RequestVoteResult]:
        url = f"{self.peer_addresses[peer_id]}/raft/request_vote"
        try:
            resp = await self._client.post(url, json=args.model_dump())
            resp.raise_for_status()
            return RequestVoteResult(**resp.json())
        except Exception as e:
            logger.debug("RequestVote -> %s failed: %s", peer_id, e)
            return None

    async def send_append_entries(self, peer_id: str, args: AppendEntriesArgs) -> Optional[AppendEntriesResult]:
        url = f"{self.peer_addresses[peer_id]}/raft/append_entries"
        try:
            resp = await self._client.post(url, json=args.model_dump())
            resp.raise_for_status()
            return AppendEntriesResult(**resp.json())
        except Exception as e:
            logger.debug("AppendEntries -> %s failed: %s", peer_id, e)
            return None

    async def send_install_snapshot(self, peer_id: str, args: InstallSnapshotArgs) -> Optional[InstallSnapshotResult]:
        url = f"{self.peer_addresses[peer_id]}/raft/install_snapshot"
        try:
            resp = await self._client.post(url, json=args.model_dump())
            resp.raise_for_status()
            return InstallSnapshotResult(**resp.json())
        except Exception as e:
            logger.debug("InstallSnapshot -> %s failed: %s", peer_id, e)
            return None

    async def close(self) -> None:
        await self._client.aclose()

    def add_peer(self, peer_id: str, address: str) -> None:
        self.peer_addresses[peer_id] = address

    def remove_peer(self, peer_id: str) -> None:
        self.peer_addresses.pop(peer_id, None)


def build_raft_app(node: RaftNode) -> FastAPI:
    """Builds the FastAPI app exposing Raft RPC endpoints and the KV API for `node`."""
    app = FastAPI(title=f"Keel node {node.node_id}")

    # The dashboard (dashboard/index.html) is a static file opened directly
    # in a browser or served from a different port than any given node, so
    # its fetch() calls are cross-origin. This is a monitoring surface, not
    # a security boundary — allowing any origin keeps local setup trivial.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/raft/pre_vote", response_model=PreVoteResult)
    async def pre_vote(args: PreVoteArgs) -> PreVoteResult:
        return await node.handle_pre_vote(args)

    @app.post("/raft/request_vote", response_model=RequestVoteResult)
    async def request_vote(args: RequestVoteArgs) -> RequestVoteResult:
        return await node.handle_request_vote(args)

    @app.post("/raft/append_entries", response_model=AppendEntriesResult)
    async def append_entries(args: AppendEntriesArgs) -> AppendEntriesResult:
        return await node.handle_append_entries(args)

    @app.post("/raft/install_snapshot", response_model=InstallSnapshotResult)
    async def install_snapshot(args: InstallSnapshotArgs) -> InstallSnapshotResult:
        return await node.handle_install_snapshot(args)

    @app.get("/status")
    async def status() -> dict:
        peer_addresses = getattr(node.transport, "peer_addresses", {})
        return {
            "node_id": node.node_id,
            "role": node.role.value,
            "term": node.state.current_term,
            "leader_id": node.leader_id,
            "log_length": node.state.last_log_index(),
            "commit_index": node.commit_index,
            "last_applied": node.last_applied,
            "last_included_index": node.state.last_included_index,
            "peers": sorted(node.peer_ids),
            "cluster_size": len(node.peer_ids) + 1,
            "peer_addresses": peer_addresses,
        }

    @app.post("/admin/add_node")
    async def add_node(body: ConfigChangeRequest) -> dict:
        if not body.address:
            return {"success": False, "error": "address is required to add a node"}
        success, leader_hint = await node.propose_config_change("add", body.node_id, body.address)
        return {"success": success} if success else {"success": False, "leader_hint": leader_hint}

    @app.post("/admin/remove_node")
    async def remove_node(body: ConfigChangeRequest) -> dict:
        success, leader_hint = await node.propose_config_change("remove", body.node_id, None)
        return {"success": success} if success else {"success": False, "leader_hint": leader_hint}

    @app.put("/kv/{key}")
    async def put_key(key: str, body: PutValueRequest) -> dict:
        success, leader_hint, _ = await node.submit_command({"op": "put", "key": key, "value": body.value})
        return {"success": success} if success else {"success": False, "leader_hint": leader_hint}

    @app.delete("/kv/{key}")
    async def delete_key(key: str) -> dict:
        success, leader_hint, _ = await node.submit_command({"op": "delete", "key": key})
        return {"success": success} if success else {"success": False, "leader_hint": leader_hint}

    @app.get("/kv/{key}")
    async def get_key(key: str, linearizable: bool = False) -> dict:
        if linearizable:
            success, leader_hint, value = await node.linearizable_read(key)
            if not success:
                return {"success": False, "leader_hint": leader_hint}
            return {
                "key": key, "value": value, "served_by": node.node_id,
                "role": node.role.value, "linearizable": True,
            }
        # Fast path: served from local state, so a lagging follower could
        # return a stale value. Pass ?linearizable=true for a read that's
        # guaranteed to reflect every write committed before the call.
        return {
            "key": key, "value": node.state_machine.get(key),
            "served_by": node.node_id, "role": node.role.value, "linearizable": False,
        }

    return app
