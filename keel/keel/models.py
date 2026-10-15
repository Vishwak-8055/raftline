"""Core data models for the Raft consensus protocol.

These mirror the RPC arguments/results defined in the Raft paper
(Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm")
almost field-for-field, so the implementation in node.py can be read
side-by-side with the paper's Figure 2 (and Section 7 for snapshots).
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry(BaseModel):
    term: int
    index: int
    # None command == a no-op entry (appended by a new leader to anchor
    # commit-index advancement — see node.py's _start_election).
    command: Optional[dict[str, Any]] = None


class RequestVoteArgs(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteResult(BaseModel):
    term: int
    vote_granted: bool
    voter_id: str


class AppendEntriesArgs(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesResult(BaseModel):
    term: int
    success: bool
    follower_id: str
    conflict_index: Optional[int] = None
    conflict_term: Optional[int] = None


class InstallSnapshotArgs(BaseModel):
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: dict[str, Any]  # full state-machine contents at the snapshot point


class InstallSnapshotResult(BaseModel):
    term: int
    follower_id: str


class PutValueRequest(BaseModel):
    value: Any

class PreVoteArgs(BaseModel):
    term: int  # the term the candidate WOULD use if it proceeds to a real election
    candidate_id: str
    last_log_index: int
    last_log_term: int


class PreVoteResult(BaseModel):
    term: int
    vote_granted: bool
    voter_id: str

class ConfigChangeRequest(BaseModel):
    node_id: str
    address: Optional[str] = None  # required for add, ignored for remove

