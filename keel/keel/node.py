"""
The core Raft consensus algorithm.

Implements leader election (with a PreVote phase), log replication,
linearizable reads (the "read index" protocol), log compaction via
snapshots, and single-server cluster membership changes, per Ongaro &
Ousterhout's "In Search of an Understandable Consensus Algorithm" (the
Raft paper) — Figure 2 for the core algorithm, Section 6 for read-only
queries and membership changes, Section 7 for snapshotting — plus the
PreVote extension from Ongaro's dissertation (Section 9.6). This class
is transport-agnostic: it talks to peers only through the injected
`Transport`, which is what lets it be driven for real over HTTP
(transport.py) or exercised deterministically in tests against an
in-memory fake bus.

Membership changes here use the "single server at a time" approach
(dissertation Section 4.3) rather than full joint consensus: a
configuration change is just another log entry, applied to
`self.peer_ids` only once it's *committed* (never while merely
appended-but-uncommitted). Because a change can't be truncated once
committed, and only one change is accepted at a time (see
`_pending_config_change`), the old and new configurations are never
both "live" for majority-counting purposes at once — there's no window
where two disjoint quorums could each elect a leader. This is simpler
to implement correctly than joint consensus, at the cost of only
allowing one membership change to be in flight at a time, which is an
acceptable restriction (etcd does the same).

One simplification worth being explicit about: a newly-added node
starts counting toward quorum immediately on commit, with no separate
non-voting "learner" catch-up phase. It bootstraps via the same
InstallSnapshot/AppendEntries machinery used for any lagging follower,
but until it catches up it's a full voting member — a production system
would typically add it as a learner first specifically to avoid that
brief window.

Locking discipline (important for correctness, not just style): the
node's mutable state is protected by a single asyncio.Lock. Every method
that mutates state acquires it, but the lock is always released *before*
awaiting network I/O — asyncio.Lock is not reentrant, and a task that
awaits its own dependents while holding the lock (e.g. a leader awaiting
replication acks it needs the lock to record) will deadlock. Read the
values you need, release, do I/O, reacquire to apply results.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Optional

from .models import (
    AppendEntriesArgs,
    AppendEntriesResult,
    InstallSnapshotArgs,
    InstallSnapshotResult,
    LogEntry,
    PreVoteArgs,
    PreVoteResult,
    RequestVoteArgs,
    RequestVoteResult,
    Role,
)
from .state import PersistentState
from .state_machine import KVStateMachine

logger = logging.getLogger("raft")


class Transport:
    """Protocol every transport (HTTP, in-memory fake, ...) must implement."""

    async def send_pre_vote(self, peer_id: str, args: PreVoteArgs) -> Optional[PreVoteResult]:
        raise NotImplementedError

    async def send_request_vote(self, peer_id: str, args: RequestVoteArgs) -> Optional[RequestVoteResult]:
        raise NotImplementedError

    async def send_append_entries(self, peer_id: str, args: AppendEntriesArgs) -> Optional[AppendEntriesResult]:
        raise NotImplementedError

    async def send_install_snapshot(self, peer_id: str, args: InstallSnapshotArgs) -> Optional[InstallSnapshotResult]:
        raise NotImplementedError

    def add_peer(self, peer_id: str, address: str) -> None:
        """Register a new peer's address. No-op by default — only
        transports that need explicit addressing (HTTP) override this."""

    def remove_peer(self, peer_id: str) -> None:
        """Forget a peer's address. No-op by default."""


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        state: PersistentState,
        transport: Transport,
        state_machine: KVStateMachine,
        election_timeout_range: tuple[float, float] = (1.5, 3.0),
        heartbeat_interval: float = 0.5,
        snapshot_threshold: int = 20,
    ):
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self.state = state
        self.transport = transport
        self.state_machine = state_machine
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.snapshot_threshold = snapshot_threshold

        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index = self.state.last_included_index
        self.last_applied = self.state.last_included_index
        self._pending_config_change = False

        # Leader-only volatile state (reset on each election win).
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._election_deadline = 0.0
        self._last_contact = 0.0  # last time we had real evidence a leader exists
        self._reset_election_deadline()
        self._apply_waiters: dict[int, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._stopped = False

        # A restarted node that already has a snapshot on disk should load
        # its state machine AND its membership from it — a config change
        # committed before the snapshot was taken is otherwise invisible,
        # since the log entry that carried it may since have been
        # compacted away.
        snapshot_data = self.state.load_snapshot_data()
        if snapshot_data is not None:
            self.state_machine.restore(snapshot_data.get("kv", {}))
            members = snapshot_data.get("members")
            if members:
                self.peer_ids = [m for m in members if m != self.node_id]

    # ---------- small helpers ----------

    def _reset_election_deadline(self) -> None:
        """Called whenever we have real evidence a leader exists (a valid
        heartbeat) or we've just granted a vote — both are also reasons to
        refuse other nodes' PreVote requests for a while (see
        handle_pre_vote), so this updates both clocks together."""
        self._last_contact = time.monotonic()
        timeout = random.uniform(*self.election_timeout_range)
        self._election_deadline = self._last_contact + timeout

    def _back_off_own_timer(self) -> None:
        """Push out only our own next-attempt clock, without claiming
        fresh evidence of a leader. Used when backing off after our own
        (pre-)election attempt, whether it won or lost — attempting our
        own candidacy is not itself evidence anyone else is alive."""
        timeout = random.uniform(*self.election_timeout_range)
        self._election_deadline = time.monotonic() + timeout

    def _become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        if term > self.state.current_term:
            self.state.set_term_and_vote(term, None)
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self._reset_election_deadline()

    def _quorum_size(self) -> int:
        return (len(self.peer_ids) + 1) // 2 + 1

    def _log_is_up_to_date(self, candidate_last_log_term: int, candidate_last_log_index: int) -> bool:
        return candidate_last_log_term > self.state.last_log_term() or (
            candidate_last_log_term == self.state.last_log_term()
            and candidate_last_log_index >= self.state.last_log_index()
        )

    # ---------- main driver loop ----------

    async def run(self) -> None:
        """Background loop: drives elections, and heartbeats while leader."""
        while not self._stopped:
            if self.role == Role.LEADER:
                await self._send_heartbeats_once()
                await asyncio.sleep(self.heartbeat_interval)
            else:
                if time.monotonic() >= self._election_deadline:
                    await self._try_pre_vote_then_election()
                await asyncio.sleep(0.05)

    def stop(self) -> None:
        self._stopped = True

    # ---------- elections ----------

    async def _try_pre_vote_then_election(self) -> None:
        """Before committing to a real election (which bumps our term and
        can force a live leader to step down if it reaches them), run a
        non-binding straw poll. A node that's been offline or partitioned
        will keep failing this harmlessly for as long as it's cut off,
        instead of climbing its term and disrupting the cluster the
        moment it reconnects."""
        async with self._lock:
            if self.role == Role.LEADER:
                return
            candidate_term = self.state.current_term + 1
            last_log_index = self.state.last_log_index()
            last_log_term = self.state.last_log_term()
            self._back_off_own_timer()

        args = PreVoteArgs(
            term=candidate_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        results = await asyncio.gather(
            *[self.transport.send_pre_vote(p, args) for p in self.peer_ids],
            return_exceptions=True,
        )

        pre_votes = 1  # ourselves
        highest_term_seen = 0
        for r in results:
            if isinstance(r, PreVoteResult):
                highest_term_seen = max(highest_term_seen, r.term)
                if r.vote_granted:
                    pre_votes += 1

        async with self._lock:
            if highest_term_seen > self.state.current_term:
                self._become_follower(highest_term_seen)
                return
            if self.role == Role.LEADER:
                return  # something else already resolved this
            if pre_votes < self._quorum_size():
                return  # lost the straw poll — don't disrupt anyone; retry later

        await self._start_election()

    async def _start_election(self) -> None:
        async with self._lock:
            if self.role == Role.LEADER:
                return
            self.role = Role.CANDIDATE
            term_at_start = self.state.current_term + 1
            self.state.set_term_and_vote(term_at_start, self.node_id)
            self._back_off_own_timer()
            last_log_index = self.state.last_log_index()
            last_log_term = self.state.last_log_term()

        logger.info("[%s] starting election for term %d", self.node_id, term_at_start)
        args = RequestVoteArgs(
            term=term_at_start,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        results = await asyncio.gather(
            *[self.transport.send_request_vote(p, args) for p in self.peer_ids],
            return_exceptions=True,
        )

        became_leader = False
        async with self._lock:
            if self.state.current_term != term_at_start or self.role != Role.CANDIDATE:
                return  # stale: term moved on, or another RPC already resolved this
            votes = 1  # vote for self
            for r in results:
                if isinstance(r, RequestVoteResult):
                    if r.term > self.state.current_term:
                        self._become_follower(r.term)
                        return
                    if r.vote_granted:
                        votes += 1
            if votes >= self._quorum_size():
                self.role = Role.LEADER
                self.leader_id = self.node_id
                self._pending_config_change = False
                last_index = self.state.last_log_index()
                self.next_index = {p: last_index + 1 for p in self.peer_ids}
                self.match_index = {p: 0 for p in self.peer_ids}
                # Append a no-op entry in the new term. Raft can only advance
                # commitIndex by counting replicas of entries from the
                # *current* term (Figure 8 in the paper) — without this, a
                # freshly-elected leader can't make older, uncommitted
                # entries visible until a client happens to write something.
                noop_index = last_index + 1
                self.state.append_entries([LogEntry(term=term_at_start, index=noop_index, command=None)])
                became_leader = True

        if became_leader:
            logger.info("[%s] became LEADER for term %d", self.node_id, term_at_start)
            await self._send_heartbeats_once()

    # ---------- RPC handlers (invoked by the transport layer) ----------

    async def handle_pre_vote(self, args: PreVoteArgs) -> PreVoteResult:
        async with self._lock:
            # We only entertain a pre-vote if WE currently have no reason
            # to believe a leader is alive — i.e. it's been at least the
            # minimum election timeout since we last heard from one. A
            # node with a live leader always says no here, regardless of
            # its own randomized deadline, which is what stops a
            # reconnecting stale node from ever disrupting a healthy
            # cluster: every healthy voter refuses on this check alone.
            min_timeout = self.election_timeout_range[0]
            no_current_leader = (time.monotonic() - self._last_contact) >= min_timeout
            term_ok = args.term >= self.state.current_term
            log_ok = self._log_is_up_to_date(args.last_log_term, args.last_log_index)
            grant = no_current_leader and term_ok and log_ok
            return PreVoteResult(term=self.state.current_term, vote_granted=grant, voter_id=self.node_id)

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteResult:
        async with self._lock:
            if args.term > self.state.current_term:
                self._become_follower(args.term)
            if args.term < self.state.current_term:
                return RequestVoteResult(term=self.state.current_term, vote_granted=False, voter_id=self.node_id)

            log_ok = self._log_is_up_to_date(args.last_log_term, args.last_log_index)
            can_vote = self.state.voted_for in (None, args.candidate_id)

            if can_vote and log_ok:
                self.state.set_term_and_vote(args.term, args.candidate_id)
                self._reset_election_deadline()
                return RequestVoteResult(term=self.state.current_term, vote_granted=True, voter_id=self.node_id)
            return RequestVoteResult(term=self.state.current_term, vote_granted=False, voter_id=self.node_id)

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesResult:
        async with self._lock:
            if args.term < self.state.current_term:
                return AppendEntriesResult(term=self.state.current_term, success=False, follower_id=self.node_id)

            self._become_follower(args.term, leader_id=args.leader_id)

            if args.prev_log_index > 0:
                if args.prev_log_index == self.state.last_included_index:
                    missing = False
                    log_matches = args.prev_log_term == self.state.last_included_term
                    conflicting_term = self.state.last_included_term
                else:
                    prev_entry = self.state.get_entry(args.prev_log_index)
                    missing = prev_entry is None
                    log_matches = (not missing) and prev_entry.term == args.prev_log_term
                    conflicting_term = prev_entry.term if prev_entry else None

                if not log_matches:
                    if missing:
                        return AppendEntriesResult(
                            term=self.state.current_term,
                            success=False,
                            follower_id=self.node_id,
                            conflict_index=self.state.last_log_index() + 1,
                        )
                    conflict_term = conflicting_term
                    conflict_index = args.prev_log_index
                    while conflict_index > 1:
                        earlier = self.state.get_entry(conflict_index - 1)
                        if earlier and earlier.term == conflict_term:
                            conflict_index -= 1
                        else:
                            break
                    return AppendEntriesResult(
                        term=self.state.current_term,
                        success=False,
                        follower_id=self.node_id,
                        conflict_index=conflict_index,
                        conflict_term=conflict_term,
                    )

            for e in args.entries:
                existing = self.state.get_entry(e.index)
                if existing and existing.term != e.term:
                    self.state.truncate_from(e.index)
                    existing = None
                if not existing and e.index > self.state.last_included_index:
                    self.state.append_entries([e])

            if args.leader_commit > self.commit_index:
                self.commit_index = min(args.leader_commit, self.state.last_log_index())
                self._apply_committed()

            return AppendEntriesResult(term=self.state.current_term, success=True, follower_id=self.node_id)

    async def handle_install_snapshot(self, args: InstallSnapshotArgs) -> InstallSnapshotResult:
        async with self._lock:
            if args.term < self.state.current_term:
                return InstallSnapshotResult(term=self.state.current_term, follower_id=self.node_id)

            self._become_follower(args.term, leader_id=args.leader_id)

            if args.last_included_index <= self.state.last_included_index:
                return InstallSnapshotResult(term=self.state.current_term, follower_id=self.node_id)

            self.state_machine.restore(args.data.get("kv", {}))
            members = args.data.get("members")
            if members:
                self.peer_ids = [m for m in members if m != self.node_id]
            self.state.save_snapshot(args.last_included_index, args.last_included_term, args.data)
            self.commit_index = max(self.commit_index, args.last_included_index)
            self.last_applied = max(self.last_applied, args.last_included_index)

            logger.info(
                "[%s] installed snapshot through index %d from %s (members: %s)",
                self.node_id, args.last_included_index, args.leader_id, members,
            )
            return InstallSnapshotResult(term=self.state.current_term, follower_id=self.node_id)

    # ---------- replication (leader side) ----------

    async def _send_heartbeats_once(self) -> None:
        await asyncio.gather(*[self._replicate_to(p) for p in list(self.peer_ids)], return_exceptions=True)

    async def _confirm_leadership(self, term_at_start: int) -> int:
        """One replication round to every peer; returns how many peers (not
        counting self) acknowledged it while we were still leader in
        `term_at_start`. Used both to advance commitIndex and, via
        linearizable_read, to prove a majority still recognizes us before
        trusting a local read."""
        results = await asyncio.gather(
            *[self._replicate_to(p) for p in list(self.peer_ids)], return_exceptions=True
        )
        return sum(1 for r in results if r is True)

    async def _replicate_to(self, peer_id: str) -> bool:
        """Replicate pending entries (or a snapshot, if the peer has fallen
        behind the compaction point) to one peer. Returns True iff the
        peer acknowledged success for the current term."""
        async with self._lock:
            if self.role != Role.LEADER or peer_id not in self.peer_ids:
                return False
            next_idx = self.next_index.get(peer_id, self.state.last_log_index() + 1)
            needs_snapshot = next_idx <= self.state.last_included_index

            if needs_snapshot:
                snapshot_args = InstallSnapshotArgs(
                    term=self.state.current_term,
                    leader_id=self.node_id,
                    last_included_index=self.state.last_included_index,
                    last_included_term=self.state.last_included_term,
                    data=self.state.load_snapshot_data() or {},
                )
            else:
                prev_index = next_idx - 1
                if prev_index <= 0:
                    prev_term = 0
                elif prev_index == self.state.last_included_index:
                    prev_term = self.state.last_included_term
                else:
                    prev_entry = self.state.get_entry(prev_index)
                    prev_term = prev_entry.term if prev_entry else 0
                entries = self.state.entries_from(next_idx)
                append_args = AppendEntriesArgs(
                    term=self.state.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                )

        if needs_snapshot:
            snap_result = await self.transport.send_install_snapshot(peer_id, snapshot_args)
            if snap_result is None:
                return False
            async with self._lock:
                if snap_result.term > self.state.current_term:
                    self._become_follower(snap_result.term)
                    return False
                if self.role != Role.LEADER or peer_id not in self.peer_ids:
                    return False
                self.match_index[peer_id] = snapshot_args.last_included_index
                self.next_index[peer_id] = snapshot_args.last_included_index + 1
                return True

        result = await self.transport.send_append_entries(peer_id, append_args)
        if result is None:
            return False

        async with self._lock:
            if result.term > self.state.current_term:
                self._become_follower(result.term)
                return False
            if self.role != Role.LEADER or peer_id not in self.peer_ids:
                return False
            if result.success:
                self.match_index[peer_id] = prev_index + len(entries)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self._advance_commit_index()
                return True
            elif result.conflict_index is not None:
                self.next_index[peer_id] = max(1, result.conflict_index)
            else:
                self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)
            return False

    def _advance_commit_index(self) -> None:
        for n in range(self.state.last_log_index(), self.commit_index, -1):
            entry = self.state.get_entry(n)
            if entry is None or entry.term != self.state.current_term:
                continue  # never commit by counting replicas of an older term's entry
            count = 1 + sum(1 for p in self.peer_ids if self.match_index.get(p, 0) >= n)
            if count >= self._quorum_size():
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.state.get_entry(self.last_applied)
            if entry and entry.command and entry.command.get("op") == "config":
                self._apply_config_change(entry.command)
                result = {"ok": True}
            elif entry and entry.command:
                result = self.state_machine.apply(entry.command)
            else:
                result = None
            fut = self._apply_waiters.pop(self.last_applied, None)
            if fut and not fut.done():
                fut.set_result(result)
        self._maybe_snapshot()

    def _apply_config_change(self, command: dict) -> None:
        action = command.get("action")
        target_id = command.get("node_id")
        if action == "add":
            if target_id != self.node_id and target_id not in self.peer_ids:
                self.peer_ids.append(target_id)
                self.next_index.setdefault(target_id, self.state.last_log_index() + 1)
                self.match_index.setdefault(target_id, 0)
                address = command.get("address")
                if address:
                    self.transport.add_peer(target_id, address)
            logger.info("[%s] applied config change: add %s", self.node_id, target_id)
        elif action == "remove":
            if target_id == self.node_id:
                # We've been voted off the cluster. Stop participating
                # entirely rather than becoming a rogue node that can
                # still solicit (pre-)votes from our ex-peers — without
                # this, a removed follower would eventually time out and
                # keep trying to get itself elected forever.
                logger.info("[%s] applied config change: removed from the cluster, stopping", self.node_id)
                self.stop()
            elif target_id in self.peer_ids:
                # Subtlety: this method runs at the exact moment the
                # removal entry becomes committed. If we just drop the
                # target from peer_ids/next_index right here, the normal
                # replication loop will never speak to it again — which
                # means it never receives the one heartbeat that would
                # have told it "this is committed, apply it and stop
                # yourself." So we capture what a normal reply to it
                # would look like right now, apply the removal locally,
                # and send that one message out-of-band afterward.
                departing_next_index = self.next_index.get(target_id, self.state.last_log_index() + 1)
                term_now = self.state.current_term
                commit_now = self.commit_index
                self.peer_ids.remove(target_id)
                self.next_index.pop(target_id, None)
                self.match_index.pop(target_id, None)
                asyncio.create_task(
                    self._send_departure_notice(target_id, term_now, commit_now, departing_next_index)
                )
                logger.info("[%s] applied config change: remove %s", self.node_id, target_id)
        self._pending_config_change = False

    async def _send_departure_notice(self, target_id: str, term: int, commit_index: int, next_idx: int) -> None:
        """Best-effort final message to a node we just removed, so it
        learns its own removal committed (and stops itself) instead of
        simply going silent — see the comment in _apply_config_change.
        A few quick attempts, not indefinite retries: if it's genuinely
        unreachable, it stays stopped-but-unaware, which is harmless
        since every remaining member has already forgotten it and won't
        grant it votes regardless of what it believes about itself."""
        async with self._lock:
            prev_index = next_idx - 1
            prev_term = 0
            if prev_index > 0:
                if prev_index == self.state.last_included_index:
                    prev_term = self.state.last_included_term
                else:
                    entry = self.state.get_entry(prev_index)
                    prev_term = entry.term if entry else 0
            entries = self.state.entries_from(next_idx)
            args = AppendEntriesArgs(
                term=term, leader_id=self.node_id, prev_log_index=prev_index,
                prev_log_term=prev_term, entries=entries, leader_commit=commit_index,
            )
        for attempt in range(3):
            result = await self.transport.send_append_entries(target_id, args)
            if result is not None:
                break
            await asyncio.sleep(0.1)
        self.transport.remove_peer(target_id)

    def _maybe_snapshot(self) -> None:
        if self.last_applied - self.state.last_included_index < self.snapshot_threshold:
            return
        entry = self.state.get_entry(self.last_applied)
        term_at_index = entry.term if entry else self.state.last_included_term
        payload = {
            "kv": self.state_machine.snapshot(),
            "members": [self.node_id] + list(self.peer_ids),
        }
        self.state.save_snapshot(self.last_applied, term_at_index, payload)
        logger.info("[%s] snapshotted through index %d; log compacted", self.node_id, self.last_applied)

    # ---------- client-facing ----------

    async def submit_command(self, command: dict) -> tuple[bool, Optional[str], Optional[Any]]:
        """Append `command` to the log if we're leader and wait for it to commit.

        Returns (success, leader_hint, result). When we're not leader,
        leader_hint carries our best guess at who is, so a client/proxy
        can redirect instead of retrying blindly.
        """
        async with self._lock:
            if self.role != Role.LEADER:
                return False, self.leader_id, None
            index = self.state.last_log_index() + 1
            entry = LogEntry(term=self.state.current_term, index=index, command=command)
            self.state.append_entries([entry])
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._apply_waiters[index] = fut

        await self._send_heartbeats_once()  # kick off replication immediately

        try:
            result = await asyncio.wait_for(fut, timeout=5.0)
            return True, None, result
        except asyncio.TimeoutError:
            return False, self.leader_id, None

    async def propose_config_change(
        self, action: str, node_id: str, address: Optional[str] = None
    ) -> tuple[bool, Optional[str]]:
        """Add or remove a cluster member. Only one change may be in
        flight at a time (see the class docstring for why); a second
        request while one is pending is rejected outright so the caller
        can retry shortly rather than the two changes racing."""
        async with self._lock:
            if self.role != Role.LEADER:
                return False, self.leader_id
            if self._pending_config_change:
                return False, None
            if action == "add" and (node_id == self.node_id or node_id in self.peer_ids):
                return False, None  # already a member
            if action == "remove" and node_id not in self.peer_ids:
                return False, None  # not a member (self-removal isn't supported)
            self._pending_config_change = True
            index = self.state.last_log_index() + 1
            command = {"op": "config", "action": action, "node_id": node_id, "address": address}
            entry = LogEntry(term=self.state.current_term, index=index, command=command)
            self.state.append_entries([entry])
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._apply_waiters[index] = fut

        await self._send_heartbeats_once()

        try:
            await asyncio.wait_for(fut, timeout=5.0)
            return True, None
        except asyncio.TimeoutError:
            return False, self.leader_id

    async def linearizable_read(self, key: str) -> tuple[bool, Optional[str], Optional[Any]]:
        """A read guaranteed to reflect every write committed before this
        call started — the "read index" protocol (Raft paper, Section 6).

        A node that merely *thinks* it's leader (e.g. it's been split into
        a minority partition and a new leader has since been elected
        elsewhere) must not answer from local state, since that state can
        be arbitrarily stale. So before trusting our own data, we run one
        replication round and require a live majority to have acked it —
        proof nobody with a higher term exists that we don't know about.
        """
        async with self._lock:
            if self.role != Role.LEADER:
                return False, self.leader_id, None
            read_index = self.commit_index
            term_at_start = self.state.current_term

        acked = await self._confirm_leadership(term_at_start)

        async with self._lock:
            if self.role != Role.LEADER or self.state.current_term != term_at_start:
                return False, self.leader_id, None
            still_leader = (1 + acked) >= self._quorum_size()
        if not still_leader:
            return False, self.leader_id, None

        deadline = time.monotonic() + 2.0
        while self.last_applied < read_index:
            if time.monotonic() > deadline:
                return False, self.leader_id, None
            await asyncio.sleep(0.01)

        return True, None, self.state_machine.get(key)
