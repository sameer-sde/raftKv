"""
Disk persistence for Raft's persistent state.

The Raft paper is explicit: current_term, voted_for, and the log MUST be
persisted to stable storage BEFORE responding to any RPC that depends on
them. Otherwise a node could vote twice in the same term after a crash,
or "forget" committed entries -- both are safety violations, not just bugs.

This uses atomic write-to-tmp-then-rename (same pattern you used in Vektr
for index persistence) so a crash mid-write never corrupts the state file.
"""

from __future__ import annotations

import json
import os

from raftkv.node.state import PersistentState, LogEntry


class Storage:
    def __init__(self, data_dir: str, node_id: str):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"{node_id}_state.json")

    def save(self, state: PersistentState) -> None:
        data = {
            "current_term": state.current_term,
            "voted_for": state.voted_for,
            "log": [entry.to_dict() for entry in state.log],
        }
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())  # force to physical disk, not just OS buffer
        os.replace(tmp_path, self.path)  # atomic rename -- no partial-write window

    def load(self) -> PersistentState:
        if not os.path.exists(self.path):
            return PersistentState()
        with open(self.path, "r") as f:
            data = json.load(f)
        return PersistentState(
            current_term=data["current_term"],
            voted_for=data["voted_for"],
            log=[LogEntry.from_dict(e) for e in data["log"]],
        )

    def exists(self) -> bool:
        return os.path.exists(self.path)
