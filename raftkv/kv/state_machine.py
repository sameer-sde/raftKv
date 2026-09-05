"""
The KV store: the actual "state machine" that Raft's replicated log drives.

This is intentionally simple -- a dict protected by a lock. The whole
point of Raft is that by the time an entry reaches `apply()`, every node
in the cluster has already agreed (via majority commit) that this
operation happens next, in this exact order. That agreement is the hard
part; applying it to a dict is trivial by comparison, which is exactly
how it should be -- consensus and storage are cleanly separated.
"""

from __future__ import annotations

import threading

from raftkv.node.state import LogEntry


class KVStateMachine:
    def __init__(self):
        self._store: dict[str, str] = {}
        self._lock = threading.Lock()
        self._applied_count = 0

    def apply(self, entry: LogEntry) -> None:
        """Called by RaftNode.on_commit for every entry, in commit order."""
        command = entry.command
        op = command.get("op")
        with self._lock:
            if op == "SET":
                self._store[command["key"]] = command["value"]
            elif op == "DELETE":
                self._store.pop(command["key"], None)
            # GET is read-only and never goes through the log -- see KVClient.get()
            self._applied_count += 1

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._store.get(key)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._store)

    def applied_count(self) -> int:
        with self._lock:
            return self._applied_count
