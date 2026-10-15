# Keel

A distributed, fault-tolerant key-value store built on the **Raft consensus
algorithm** — the same primitive underneath etcd, Consul, and CockroachDB's
range replication. Kill any minority of nodes (including the leader) and the
cluster keeps serving reads and writes correctly; kill a majority and it
correctly *stops* serving writes rather than risking split-brain.

This exists to demonstrate the systems fundamentals that a portfolio full of
LLM/agent/RAG projects doesn't touch: consensus, replication, log compaction,
and the consistency/availability tradeoffs every real distributed system has
to make.

## What's actually implemented (and verified — see below)

- **Leader election** with randomized timeouts, term numbers, and vote
  safety (a node only grants its vote if the candidate's log is at least
  as up-to-date as its own).
- **Log replication** via AppendEntries, with conflict detection and
  fast-backtracking (a follower tells the leader exactly where its log
  diverges instead of the leader decrementing one entry at a time).
- **The commit-index safety rule**: a leader only commits by counting
  replicas of entries from its *own* term — never an older term's — which
  is the specific rule (Figure 8 in the Raft paper) that most naive
  implementations get wrong. A new leader appends a no-op entry on
  election specifically to make this safe without waiting on a client.
- **Linearizable reads** via the "read index" protocol (Raft paper,
  Section 6): a leader that's been split into a minority partition will
  keep believing it's leader, but it can't *prove* a live majority still
  agrees — so a linearizable read runs one replication round and refuses
  to answer if it can't confirm a quorum. A plain (non-linearizable) read
  is also available for callers who want speed over that guarantee.
- **Log compaction via snapshots** (Section 7): once a node has applied
  more than `snapshot_threshold` entries past its last snapshot, it
  serializes the state machine, discards the log entries below that
  point, and can bring a far-behind peer up to date with one
  InstallSnapshot RPC instead of replaying its entire history.
- **Write-ahead persistence**: term, vote, log, and now snapshots survive
  a `kill -9` and process restart — this is what makes the safety
  guarantees actually hold across crashes, not just in the happy path.
- **PreVote** (Ongaro's dissertation, Section 9.6): before bumping its
  term and calling a real election, a node runs a non-binding straw
  poll. Peers refuse it outright if they currently believe a leader is
  alive, regardless of the requester's term. A node that's been
  partitioned off keeps failing this harmlessly for as long as it's cut
  off — its term never climbs, so reconnecting it can't force a
  spurious re-election on a healthy cluster.
- **Cluster membership changes** (Raft paper, Section 6; single-server
  approach from the dissertation, Section 4.3 — the same restriction
  etcd uses): nodes can be added or removed from a live cluster one at a
  time. A configuration change is just another log entry, applied only
  once committed, which is what keeps it safe without needing full
  joint consensus — the old and new configurations are never both
  "live" for majority-counting at once. A newly-added node bootstraps
  through the exact same InstallSnapshot/AppendEntries machinery used
  for any lagging follower; a removed node is sent one final message
  carrying its own removal's commit so it stops soliciting votes from
  its ex-peers instead of lingering as a rogue candidate.

## Verified, not just written

- **Deterministic correctness tests** (`tests/test_raft_core.py`, 8
  tests) using an in-memory fake network that can simulate partitions
  and lagging nodes precisely:
  - basic election + replication
  - leader failover preserves already-committed writes
  - a 2-node minority (out of 5) can never elect a leader or commit a
    write — Raft choosing consistency over availability under a
    partition (the "CP" side of CAP)
  - an isolated ex-leader's *linearizable* read correctly refuses, even
    though its naive local read would happily return stale data
  - a node reconnecting after missing a compaction cycle catches up via
    InstallSnapshot, confirmed by checking it never replayed the raw
    entries
  - a node partitioned off since before the cluster's first election
    never bumps its own term (proving PreVote is actually gating it),
    and reconnecting it doesn't disturb the healthy leader's term or role
  - a brand-new node added to a running cluster catches up on
    already-written data and then actually counts toward quorum
  - removing a node shrinks the quorum requirement and stops the
    removed node from continuing to seek votes
- A live demo (`scripts/demo_membership.py`) that starts a 3-node
  cluster, adds two brand-new nodes one at a time (each starting with
  an empty log and no known peers), kills two followers simultaneously
  to prove the resulting 5-node cluster tolerates 2 failures where the
  original 3-node one could only tolerate 1, then formally removes the
  two dead nodes and shows the quorum requirement correctly shrinks
  back down to match the 3 nodes actually alive.
- **A real multi-node demo** (`scripts/demo.py`) that starts five OS
  subprocesses and, over actual HTTP (no mocks): elects a leader,
  replicates a write, kills the leader process outright, elects a new
  one, forces log compaction with a burst of writes, restarts the killed
  node and watches it reload its own pre-crash WAL from disk and then
  catch up via InstallSnapshot, and finally demonstrates a linearizable
  read succeeding on the real leader and correctly refusing on a
  non-leader.
- All of the above were run repeatedly during development, not just
  once — including catching and fixing a real deadlock (the leader path
  used to hold its state lock across an `await` on the very replication
  calls it needed the lock to record), a test-harness bug (stale
  processes from earlier crashed runs squatting on the demo's ports,
  which looked exactly like a broken election until traced to its root
  cause), and a genuine race in membership changes (a removed node
  never learned its own removal had committed, because the leader
  stopped talking to it in the same instant that news would have
  reached it — fixed by sending one final message before cutting it
  off). All three are the kind of thing worth being able to talk
  through.

## What's deliberately not implemented (yet)

Being upfront about this is part of the point — a real reviewer will ask:

- **No non-voting "learner" phase for new members.** A newly-added node
  counts toward quorum immediately on commit rather than first catching
  up as a non-voting learner and only then being promoted. Production
  systems add the learner phase specifically to avoid a brief window
  where a slow-to-catch-up new member could affect availability. Noted
  in `node.py`'s module docstring at the point this matters.

## Dashboard

`dashboard/index.html` is a single, dependency-free HTML file — open it
directly in a browser, no build step. Point it at any one node's address
and it discovers the rest of the cluster from that node's `/status`
response (which now includes its known peer addresses), then polls
everyone every 600ms: a hero strip for "who's leader, what term, how
many nodes reachable," an instrument-panel grid of per-node term/log/
commit/snapshot state, and a scrolling event log that only writes an
entry when something actually changes (a role flip, a term bump, a
compaction) rather than logging every poll.

It needs CORS enabled on the nodes it talks to, since the dashboard's
origin (a local file, or whatever serves it) differs from each node's —
`transport.py` enables it for all origins, which is fine for a local
monitoring tool but worth tightening before exposing any of this beyond
localhost.

I don't have a browser in this environment to screenshot it, so I
verified the parts I could: the embedded JS parses cleanly (`node
--check`), the HTML tag structure balances, and — the part that
actually matters — I drove the dashboard's real fetch/discovery/render
logic from Node against a live 5-node cluster and confirmed single-seed
discovery finds all 5 addresses, every field the UI reads is present
with the expected shape, and leader detection resolves correctly once
an election completes. What I can't personally confirm is that it looks
right — that's the one piece here I'd actually want you to eyeball.

## Running it

```bash
pip install -r requirements.txt

# Real 5-node cluster over HTTP, with the full scripted demo (crash,
# failover, compaction, snapshot recovery, linearizable reads):
python3 scripts/demo.py

# Dynamic membership: add two brand-new nodes live, prove the bigger
# cluster tolerates more failures, then clean up the dead ones:
python3 scripts/demo_membership.py

# Or leave a cluster running and poke at it yourself:
python3 scripts/run_local_cluster.py
curl http://localhost:9101/status
curl -X PUT http://localhost:9101/kv/foo -d '{"value": 42}'
curl http://localhost:9102/kv/foo
curl "http://localhost:9101/kv/foo?linearizable=true"
curl -X POST http://localhost:9101/admin/add_node \
  -d '{"node_id": "node6", "address": "http://localhost:9106"}'
```

Run the deterministic test suite (no networking, no subprocesses):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Layout

```
keel/
  models.py         RPC + log entry schemas (pydantic)
  state.py          persistent term/vote/log/snapshot with a write-ahead log
  state_machine.py  the replicated KV map
  node.py           the actual Raft algorithm — PreVote, election,
                     replication, linearizable reads, snapshotting,
                     membership changes, safety
  transport.py      FastAPI server (inbound RPCs, /kv, /status,
                     /admin/add_node, /admin/remove_node) + httpx
                     client (outbound)
  server.py         wires a node + transport into a runnable process;
                     --config is optional, for nodes joining an
                     existing cluster with no initial peers
scripts/
  demo.py           starts 5 nodes and proves election/replication/
                     compaction/recovery/linearizable-reads, live
  demo_membership.py  starts 3 nodes, adds 2 live, proves improved
                       fault tolerance, then cleans up dead ones
  run_local_cluster.py  starts 5 nodes and leaves them running
tests/
  test_raft_core.py 8 tests: election, failover, partition safety,
                     linearizable reads, snapshot catch-up, PreVote,
                     and membership changes
dashboard/
  index.html        live cluster monitor — open directly, no build step
```

## Why this, and why Raft specifically

Companies whose interview loops probe for systems fundamentals — the
infra/platform teams at places like GitLab, NetApp, Qualcomm, NVIDIA, and
Canonical, or distributed-database shops like PsiQuantum's classical stack
or CockroachDB — routinely ask candidates to reason about consensus,
replication, and CAP tradeoffs. Having actually built the thing, including
the specific safety rule that catches people out (committing across terms)
and the specific extension that closes the "stale leader" hole in reads
(read index), is a very different conversation than having read about it.
