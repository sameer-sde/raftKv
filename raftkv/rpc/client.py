"""
RPC transport layer, client side.

Sends RequestVote/AppendEntries calls to peer nodes over plain HTTP.
Uses a short timeout on purpose: in Raft, a peer that's slow to respond
should be treated the same as a peer that's down -- the algorithm is
built to tolerate this, so we fail fast rather than hang.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error


class RPCClient:
    def __init__(self, timeout_seconds: float = 0.5):
        self.timeout = timeout_seconds

    def call(self, address: str, endpoint: str, payload: dict) -> dict | None:
        """
        Returns the peer's JSON response, or None if the call failed
        (timeout, connection refused, peer down, etc.) -- Raft is designed
        to make progress even when some peers are unreachable, so callers
        must handle None gracefully rather than treating it as fatal.
        """
        url = f"http://{address}{endpoint}"
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, ConnectionRefusedError, TimeoutError, OSError):
            return None

    def get(self, address: str, endpoint: str) -> dict | None:
        url = f"http://{address}{endpoint}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, ConnectionRefusedError, TimeoutError, OSError):
            return None
