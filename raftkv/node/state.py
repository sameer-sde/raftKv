"""
Core Raft data structures.

Every node tracks:
  - Persistent state (survives restarts): current_term, voted_for, log
  - Volatile state (reset on restart): commit_index, last_applied
  - Leader-only volatile state: next_index, match_index per follower

This module has zero networking/threading in it on purpose -- pure data
structures and state transitions, so they're trivial to unit test without
spinning up real servers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict  # e.g. {"op": "SET", "key": "x", "value": "1"}

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(term=d["term"], index=d["index"], command=d["command"])


@dataclass
class PersistentState:
    """State that MUST survive a crash/restart -- written to disk before responding to any RPC."""

    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)

    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def get_entry(self, index: int) -> LogEntry | None:
        """1-indexed, matching the Raft paper's convention."""
        if 1 <= index <= len(self.log):
            return self.log[index - 1]
        return None

    def append_entries(self, entries: list[LogEntry]) -> None:
        self.log.extend(entries)

    def truncate_from(self, index: int) -> None:
        """Remove all entries from `index` onward (1-indexed) -- used to resolve log conflicts."""
        self.log = self.log[: index - 1]


@dataclass
class VolatileState:
    """Reset every time the process restarts -- does not need to be persisted."""

    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderVolatileState:
    """Only meaningful while this node is the leader; rebuilt fresh on election."""

    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)
