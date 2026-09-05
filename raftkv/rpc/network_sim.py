"""
Network partition simulator.

Since all nodes run on localhost, we can't use real firewall rules to
simulate a network partition. Instead, this is a shared, thread-safe
registry that RaftNode checks before making any RPC call: if the sender
and recipient are in different partition groups, the call is treated as
if it failed (returns None), exactly matching what a real network
partition looks like to the algorithm.

This is the same technique MIT's 6.824 Raft test suite uses -- a
deterministic, controllable network layer is what makes partition testing
possible at all; you can't reliably reproduce real network partitions in
a test environment.
"""

from __future__ import annotations

import threading


class NetworkSimulator:
    def __init__(self):
        self._lock = threading.Lock()
        self._groups: list[set[str]] | None = None  # None = fully connected (no partition)

    def partition(self, groups: list[list[str]]) -> None:
        """Split the cluster into isolated groups -- nodes can only reach others in their own group."""
        with self._lock:
            self._groups = [set(g) for g in groups]

    def heal(self) -> None:
        """Restore full connectivity."""
        with self._lock:
            self._groups = None

    def is_connected(self, node_a: str, node_b: str) -> bool:
        with self._lock:
            if self._groups is None:
                return True
            for group in self._groups:
                if node_a in group and node_b in group:
                    return True
            return False
