"""
A runnable Raft node process: wires RaftNode's logic to an actual
listening RPC server, plus the KVStateMachine that consumes committed
log entries. This is what `scripts/run_node.py` and the test harness
both use to spin up real, independent node processes/threads.
"""

from __future__ import annotations

from raftkv.node.raft import RaftNode, NodeConfig
from raftkv.rpc.server import RPCServer
from raftkv.kv.state_machine import KVStateMachine


class RaftServer:
    def __init__(self, config: NodeConfig, network_sim=None):
        self.kv = KVStateMachine()
        self.node = RaftNode(config, on_commit=self.kv.apply, network_sim=network_sim)
        host, port_str = config.address.split(":")
        self.rpc_server = RPCServer(
            host=host,
            port=int(port_str),
            request_vote_callback=self.node.handle_request_vote,
            append_entries_callback=self.node.handle_append_entries,
            get_state_callback=self.node.get_state_snapshot,
            submit_callback=self._handle_submit,
            get_value_callback=self._handle_get,
        )

    def _handle_submit(self, command: dict) -> dict:
        accepted, leader_hint = self.node.submit(command)
        return {"accepted": accepted, "leader_hint": leader_hint}

    def _handle_get(self, body: dict) -> dict:
        return {"value": self.kv.get(body.get("key"))}

    def start(self) -> None:
        self.rpc_server.start()
        self.node.start()

    def stop(self) -> None:
        self.node.stop()
        self.rpc_server.stop()
