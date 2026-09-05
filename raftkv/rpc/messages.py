"""
RPC message definitions for the two RPCs Raft is built on:

  - RequestVote: used during leader election, candidates ask for votes
  - AppendEntries: used by the leader both to replicate log entries AND
    as a heartbeat (empty entries list) to tell followers "I'm still alive"

These are plain dataclasses that serialize to/from JSON -- no framework,
so it's easy to see exactly what's going over the wire.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict] = field(default_factory=list)  # serialized LogEntry dicts
    leader_commit: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    # Fast-backtrack hint: lets the leader skip straight to the right
    # next_index instead of decrementing one-by-one on every conflict.
    conflict_index: int | None = None
    conflict_term: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)
