"""
KV client: what an external application actually talks to.

Raft only allows writes through the leader. A client doesn't necessarily
know who the leader is (it changes after every election), so this client
tries whichever node it's pointed at, and if that node says "I'm not the
leader, try X instead," it follows the redirect automatically -- this is
exactly how real Raft-backed systems like etcd behave from a client's
perspective.
"""

from __future__ import annotations

import time

from raftkv.rpc.client import RPCClient


class KVClient:
    def __init__(self, cluster_addresses: dict[str, str], max_redirects: int = 5):
        self.addresses = cluster_addresses
        self.rpc = RPCClient(timeout_seconds=1.0)
        self.max_redirects = max_redirects
        self._known_leader: str | None = None

    def _try_nodes_for_write(self, command: dict) -> tuple[bool, str]:
        """
        Tries the last-known leader first (fast path), then falls back to
        asking every node "are you the leader?" until one says yes.
        """
        candidates = [self._known_leader] if self._known_leader else []
        candidates += [nid for nid in self.addresses if nid != self._known_leader]

        for _ in range(self.max_redirects):
            for node_id in candidates:
                if node_id is None:
                    continue
                address = self.addresses[node_id]
                state = self.rpc.get(address, "/state")
                if state is None:
                    continue  # node unreachable, try the next one
                if state.get("role") != "leader":
                    continue  # not the leader, try the next one
                # Found the leader -- submit via the internal /submit endpoint
                response = self.rpc.call(address, "/submit", command)
                if response and response.get("accepted"):
                    self._known_leader = node_id
                    return True, f"committed via {node_id}"
            time.sleep(0.1)  # brief pause -- an election might be in progress
        return False, "no leader found after retries -- cluster may be electing"

    def set(self, key: str, value: str, wait_for_commit: bool = True) -> tuple[bool, str]:
        ok, msg = self._try_nodes_for_write({"op": "SET", "key": key, "value": value})
        if ok and wait_for_commit:
            self._wait_until_visible(key, value)
        return ok, msg

    def delete(self, key: str) -> tuple[bool, str]:
        return self._try_nodes_for_write({"op": "DELETE", "key": key})

    def get(self, key: str, node_id: str | None = None) -> str | None:
        """
        Reads can be served by any node (including followers) for
        simplicity here -- note this means a read can be very slightly
        stale if it hits a follower that hasn't received the latest
        replication yet. Real production Raft systems solve this with
        "read index" or lease-based reads; documented here as a known
        simplification, not an oversight.

        Tries every node in the cluster until one responds, rather than
        giving up after the first (possibly dead) node -- important since
        each CLI invocation is a fresh process with no memory of which
        node was the leader last time.
        """
        candidates = [node_id] if node_id else []
        candidates += [nid for nid in self.addresses if nid != node_id]

        for nid in candidates:
            if nid is None:
                continue
            response = self.rpc.call(self.addresses[nid], "/get", {"key": key})
            if response is not None:
                return response.get("value")
        return None  # every node in the cluster was unreachable

    def _wait_until_visible(self, key: str, expected_value: str, timeout: float = 2.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.get(key, node_id=self._known_leader) == expected_value:
                return True
            time.sleep(0.02)
        return False
