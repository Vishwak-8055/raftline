"""
Deterministic tests for the core Raft algorithm using an in-memory fake
bus instead of real networking or subprocesses. This lets us simulate
network partitions and lagging nodes precisely and assert on exact
safety properties, which is a lot harder to do reliably against real
sockets and timing.
"""
from __future__ import annotations

import asyncio
import time

from keel.models import InstallSnapshotArgs, PreVoteArgs
from keel.node import RaftNode, Transport
from keel.state import PersistentState
from keel.state_machine import KVStateMachine


class SharedBus:
    """An in-process 'network' connecting fake transports to real RaftNodes."""

    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.groups: dict[str, int] | None = None  # None == fully connected

    def register(self, node_id: str, node: RaftNode) -> None:
        self.nodes[node_id] = node

    def partition(self, groups: dict[str, int]) -> None:
        """Nodes can only reach others in the same group."""
        self.groups = groups

    def heal(self) -> None:
        self.groups = None

    def reachable(self, a: str, b: str) -> bool:
        if self.groups is None:
            return True
        return self.groups.get(a) == self.groups.get(b)


class FakeTransport(Transport):
    def __init__(self, bus: SharedBus, node_id: str):
        self.bus = bus
        self.node_id = node_id

    async def send_pre_vote(self, peer_id, args: PreVoteArgs):
        if not self.bus.reachable(self.node_id, peer_id):
            return None
        target = self.bus.nodes.get(peer_id)
        return await target.handle_pre_vote(args) if target else None

    async def send_request_vote(self, peer_id, args):
        if not self.bus.reachable(self.node_id, peer_id):
            return None
        target = self.bus.nodes.get(peer_id)
        return await target.handle_request_vote(args) if target else None

    async def send_append_entries(self, peer_id, args):
        if not self.bus.reachable(self.node_id, peer_id):
            return None
        target = self.bus.nodes.get(peer_id)
        return await target.handle_append_entries(args) if target else None

    async def send_install_snapshot(self, peer_id, args: InstallSnapshotArgs):
        if not self.bus.reachable(self.node_id, peer_id):
            return None
        target = self.bus.nodes.get(peer_id)
        return await target.handle_install_snapshot(args) if target else None

    def add_peer(self, peer_id: str, address: str) -> None:
        pass  # the shared bus already routes by node_id; no addressing needed

    def remove_peer(self, peer_id: str) -> None:
        pass


def build_cluster(tmp_path, node_ids, bus: SharedBus, **node_kwargs) -> dict[str, RaftNode]:
    nodes = {}
    defaults = dict(election_timeout_range=(0.15, 0.3), heartbeat_interval=0.05)
    defaults.update(node_kwargs)
    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        state = PersistentState(nid, str(tmp_path / nid))
        sm = KVStateMachine()
        transport = FakeTransport(bus, nid)
        node = RaftNode(nid, peers, state, transport, sm, **defaults)
        nodes[nid] = node
        bus.register(nid, node)
    return nodes


def run_all(nodes: dict[str, RaftNode]) -> list[asyncio.Task]:
    return [asyncio.create_task(n.run()) for n in nodes.values()]


async def wait_for_leader(nodes: dict[str, RaftNode], timeout: float = 3.0) -> RaftNode | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = [n for n in nodes.values() if n.role.value == "leader"]
        if leaders:
            return leaders[0]
        await asyncio.sleep(0.02)
    return None


async def test_leader_election_and_replication(tmp_path):
    bus = SharedBus()
    nodes = build_cluster(tmp_path, ["n1", "n2", "n3"], bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None

        ok, _, _ = await leader.submit_command({"op": "put", "key": "x", "value": 42})
        assert ok

        await asyncio.sleep(0.3)
        for n in nodes.values():
            assert n.state_machine.get("x") == 42
    finally:
        for t in tasks:
            t.cancel()


async def test_leader_failover_preserves_committed_writes(tmp_path):
    bus = SharedBus()
    nodes = build_cluster(tmp_path, ["n1", "n2", "n3", "n4", "n5"], bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None
        ok, _, _ = await leader.submit_command({"op": "put", "key": "x", "value": 1})
        assert ok

        old_leader_id = leader.node_id
        leader.stop()  # simulate a crash: stop driving it, leave it registered but silent

        new_leader = None
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            candidates = [n for n in nodes.values() if n.node_id != old_leader_id and n.role.value == "leader"]
            if candidates:
                new_leader = candidates[0]
                break
            await asyncio.sleep(0.02)

        assert new_leader is not None
        assert new_leader.node_id != old_leader_id

        ok, _, _ = await new_leader.submit_command({"op": "put", "key": "y", "value": 2})
        assert ok

        await asyncio.sleep(0.3)
        for n in nodes.values():
            if n.node_id == old_leader_id:
                continue
            assert n.state_machine.get("x") == 1  # write from before the crash survived
            assert n.state_machine.get("y") == 2
    finally:
        for t in tasks:
            t.cancel()


async def test_minority_partition_cannot_elect_or_commit(tmp_path):
    """A 2-node minority split from a 5-node cluster must never elect a
    leader or see a write commit — Raft trading availability for
    consistency under a partition (the "CP" side of CAP)."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3", "n4", "n5"]
    nodes = build_cluster(tmp_path, node_ids, bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None

        others = [n for n in node_ids if n != leader.node_id]
        minority = set(others[:2])
        groups = {nid: (0 if nid in minority else 1) for nid in node_ids}
        bus.partition(groups)

        await asyncio.sleep(1.5)  # give the minority plenty of chances to try and fail
        assert all(nodes[nid].role.value != "leader" for nid in minority), (
            "a 2-node minority must never elect a leader out of 5 nodes"
        )

        ok, _, _ = await leader.submit_command({"op": "put", "key": "z", "value": 99})
        assert ok, "the majority partition must still be able to commit writes"

        await asyncio.sleep(0.2)
        for nid in node_ids:
            if nid in minority:
                assert nodes[nid].state_machine.get("z") is None
            else:
                assert nodes[nid].state_machine.get("z") == 99
    finally:
        for t in tasks:
            t.cancel()


async def test_linearizable_read_rejects_stale_isolated_leader(tmp_path):
    """A leader partitioned into the minority must refuse linearizable
    reads (it can't prove a majority still recognizes it as leader),
    even though a naive local read would happily return its stale view."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3", "n4", "n5"]
    nodes = build_cluster(tmp_path, node_ids, bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None
        old_leader_id = leader.node_id

        ok, _, _ = await leader.submit_command({"op": "put", "key": "x", "value": "before-partition"})
        assert ok

        others = [n for n in node_ids if n != old_leader_id]
        minority = {old_leader_id, others[0]}
        majority_ids = [nid for nid in node_ids if nid not in minority]
        groups = {nid: (0 if nid in minority else 1) for nid in node_ids}
        bus.partition(groups)

        new_leader = None
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            candidates = [nodes[nid] for nid in majority_ids if nodes[nid].role.value == "leader"]
            if candidates:
                new_leader = candidates[0]
                break
            await asyncio.sleep(0.02)
        assert new_leader is not None

        ok, _, _ = await new_leader.submit_command({"op": "put", "key": "x", "value": "after-partition"})
        assert ok

        # Naive local read on the isolated (ex-)leader: happily returns
        # its stale, pre-partition view — no complaints.
        assert leader.state_machine.get("x") == "before-partition"

        # Linearizable read must refuse instead of lying.
        success, _, _ = await leader.linearizable_read("x")
        assert not success, "an isolated leader must not be able to confirm a linearizable read"
    finally:
        for t in tasks:
            t.cancel()


async def test_lagging_follower_catches_up_via_snapshot(tmp_path):
    """A node that's been unreachable since before the cluster's first
    write must catch up via InstallSnapshot once reconnected, since the
    entries it needs will have already been compacted off the leader's
    log — not by replaying every individual entry from scratch."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3"]
    nodes = build_cluster(tmp_path, node_ids, bus, snapshot_threshold=5)

    # n3 is unreachable from the very start — simulating a node that's
    # been down since before the cluster ever elected a leader. Its own
    # run() loop isn't started yet either, so it never wastes election
    # attempts (and never inflates its term) while cut off.
    bus.partition({"n1": 0, "n2": 0, "n3": 1})

    tasks = [asyncio.create_task(nodes["n1"].run()), asyncio.create_task(nodes["n2"].run())]
    try:
        leader = await wait_for_leader({"n1": nodes["n1"], "n2": nodes["n2"]})
        assert leader is not None

        for i in range(10):
            ok, _, _ = await leader.submit_command({"op": "put", "key": f"k{i}", "value": i})
            assert ok

        await asyncio.sleep(0.2)
        assert leader.state.last_included_index > 0, "leader should have compacted its log by now"

        # n3 "comes back online": reconnect it and only now start driving it.
        bus.heal()
        tasks.append(asyncio.create_task(nodes["n3"].run()))

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and nodes["n3"].state_machine.get("k9") != 9:
            await asyncio.sleep(0.02)

        for i in range(10):
            assert nodes["n3"].state_machine.get(f"k{i}") == i
        assert nodes["n3"].state.last_included_index > 0, (
            "n3 must have caught up via InstallSnapshot, not a full replay"
        )
    finally:
        for t in tasks:
            t.cancel()


async def test_prevote_prevents_disruption_from_reconnecting_stale_node(tmp_path):
    """Without PreVote, a node cut off since before the cluster's first
    election would keep bumping its own term every time it (uselessly)
    retries — and reconnecting with that inflated term would force the
    healthy leader to step down. PreVote's non-binding straw poll means
    a cut-off node never bumps its term in the first place, since every
    reachable peer refuses on "I already have a live leader" alone."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3"]
    nodes = build_cluster(tmp_path, node_ids, bus)

    bus.partition({"n1": 0, "n2": 0, "n3": 1})
    tasks = run_all(nodes)  # all three run, including the partitioned n3
    try:
        leader = await wait_for_leader({"n1": nodes["n1"], "n2": nodes["n2"]})
        assert leader is not None
        term_before = leader.state.current_term

        # Let n3 spend a while alone, retrying PreVote rounds that can
        # never succeed while it's cut off (this is exactly what would
        # have climbed its real term without PreVote).
        await asyncio.sleep(1.5)
        assert nodes["n3"].state.current_term == 0, (
            "PreVote must mean a permanently-isolated node never bumps its term"
        )

        bus.heal()
        await asyncio.sleep(1.0)  # give n3's next retry a chance to reach the others

        assert leader.role.value == "leader", "a reconnecting stale node must not disrupt the healthy leader"
        assert leader.state.current_term == term_before, "PreVote must prevent a spurious term bump on reconnect"
    finally:
        for t in tasks:
            t.cancel()


async def test_add_node_catches_up_and_joins_quorum(tmp_path):
    """A brand-new node with no data, added to a running cluster, must
    catch up (via the existing replication/snapshot machinery) and then
    actually count toward quorum — proving membership changes flow
    through commit + apply like any other command, not just cosmetically
    update a list somewhere."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3"]
    nodes = build_cluster(tmp_path, node_ids, bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None
        ok, _, _ = await leader.submit_command({"op": "put", "key": "x", "value": 1})
        assert ok

        # A brand-new node joins with an empty log and no known peers.
        n4_state = PersistentState("n4", str(tmp_path / "n4"))
        n4_sm = KVStateMachine()
        n4_transport = FakeTransport(bus, "n4")
        n4 = RaftNode(
            "n4", [], n4_state, n4_transport, n4_sm,
            election_timeout_range=(0.15, 0.3), heartbeat_interval=0.05,
        )
        nodes["n4"] = n4
        bus.register("n4", n4)
        tasks.append(asyncio.create_task(n4.run()))

        ok, _ = await leader.propose_config_change("add", "n4", address="fake://n4")
        assert ok

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and n4.state_machine.get("x") != 1:
            await asyncio.sleep(0.02)
        assert n4.state_machine.get("x") == 1, "the new node must catch up on data written before it joined"

        assert "n4" in leader.peer_ids
        assert leader._quorum_size() == 3, "quorum must now reflect all 4 members, not the original 3"

        # Confirm n4 is really counted (not just cosmetically listed) by
        # writing something new and checking it actually reaches n4 too.
        ok, _, _ = await leader.submit_command({"op": "put", "key": "y", "value": 2})
        assert ok
        await asyncio.sleep(0.2)
        assert n4.state_machine.get("y") == 2
    finally:
        for t in tasks:
            t.cancel()


async def test_remove_node_shrinks_quorum_and_stops_removed_node(tmp_path):
    """Removing a node must shrink the quorum requirement, and the
    removed node itself must stop trying to get re-elected rather than
    lingering as a rogue vote-seeker among its ex-peers."""
    bus = SharedBus()
    node_ids = ["n1", "n2", "n3", "n4", "n5"]
    nodes = build_cluster(tmp_path, node_ids, bus)
    tasks = run_all(nodes)
    try:
        leader = await wait_for_leader(nodes)
        assert leader is not None

        victim = next(n for n in node_ids if n != leader.node_id)
        ok, _ = await leader.propose_config_change("remove", victim)
        assert ok

        await asyncio.sleep(0.3)
        assert victim not in leader.peer_ids
        assert leader._quorum_size() == 3, "removing one of 5 should still need 3 of the remaining 4"
        assert nodes[victim]._stopped is True, "the removed node must stop itself, not just be dropped by others"

        ok, _, _ = await leader.submit_command({"op": "put", "key": "z", "value": 3})
        assert ok
        await asyncio.sleep(0.2)
        for nid in node_ids:
            if nid == victim:
                continue
            assert nodes[nid].state_machine.get("z") == 3
    finally:
        for t in tasks:
            t.cancel()
