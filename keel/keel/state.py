"""Persistent Raft state, durable across process restarts.

Raft requires currentTerm, votedFor, and the log to survive a crash —
a node that forgets who it voted for in a term could double-vote after
restarting, silently breaking the algorithm's safety guarantees. This
module treats every mutation as an entry in a small write-ahead log
(JSON Lines) plus an atomically-replaced metadata file, so a killed
`-9`'d process comes back with exactly the state it had before it died.

It also owns log compaction: once a snapshot is taken (save_snapshot),
entries at or below last_included_index are discarded from the log file
and are no longer retrievable — a follower that still needs them gets
sent the snapshot instead (InstallSnapshot RPC, handled in node.py).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .models import LogEntry


class PersistentState:
    def __init__(self, node_id: str, data_dir: str):
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._meta_path = self.data_dir / "meta.json"
        self._log_path = self.data_dir / "log.jsonl"
        self._snapshot_path = self.data_dir / "snapshot.json"

        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self.log: list[LogEntry] = []  # only entries with index > last_included_index

        self._load()

    def _load(self) -> None:
        if self._meta_path.exists():
            meta = json.loads(self._meta_path.read_text())
            self.current_term = meta.get("current_term", 0)
            self.voted_for = meta.get("voted_for")
            self.last_included_index = meta.get("last_included_index", 0)
            self.last_included_term = meta.get("last_included_term", 0)
        if self._log_path.exists():
            with self._log_path.open() as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entry = LogEntry(**json.loads(line))
                        if entry.index > self.last_included_index:
                            self.log.append(entry)

    def _persist_meta(self) -> None:
        tmp = self._meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
        }))
        os.replace(tmp, self._meta_path)  # atomic on POSIX

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self.current_term = term
        self.voted_for = voted_for
        self._persist_meta()

    def append_entries(self, entries: list[LogEntry]) -> None:
        if not entries:
            return
        with self._log_path.open("a") as f:
            for e in entries:
                f.write(e.model_dump_json() + "\n")
        self.log.extend(entries)

    def truncate_from(self, index: int) -> None:
        """Delete every entry at `index` and after, rewriting the log file.

        Used when a follower's log conflicts with the leader's: the
        follower must discard its own (uncommitted, since it never
        reached quorum) entries before accepting the leader's version.
        """
        keep = [e for e in self.log if e.index < index]
        self._rewrite_log(keep)

    def _rewrite_log(self, entries: list[LogEntry]) -> None:
        self.log = entries
        tmp = self._log_path.with_suffix(".tmp")
        with tmp.open("w") as f:
            for e in entries:
                f.write(e.model_dump_json() + "\n")
        os.replace(tmp, self._log_path)

    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else self.last_included_index

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else self.last_included_term

    def get_entry(self, index: int) -> Optional[LogEntry]:
        if index <= self.last_included_index:
            return None  # compacted away — caller should use the snapshot
        for e in self.log:
            if e.index == index:
                return e
        return None

    def entries_from(self, index: int) -> list[LogEntry]:
        return [e for e in self.log if e.index >= index]

    # ---------- snapshotting ----------

    def save_snapshot(self, last_included_index: int, last_included_term: int, data: dict) -> None:
        tmp = self._snapshot_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, self._snapshot_path)

        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self._persist_meta()

        keep = [e for e in self.log if e.index > last_included_index]
        self._rewrite_log(keep)

    def load_snapshot_data(self) -> Optional[dict]:
        if self._snapshot_path.exists():
            return json.loads(self._snapshot_path.read_text())
        return None
