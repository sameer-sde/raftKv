"""
Test cluster helper: spins up N real RaftServer instances (real threads,
real sockets on localhost) with a shared NetworkSimulator, for use in
fault-injection tests. This is genuinely running the algorithm, not
mocking it -- every test using this hits real HTTP endpoints.
"""

from __future__ import annotations

import shutil
import time

from raftkv.node.server import RaftServer
from raftkv.node.raft import NodeConfig
from raftkv.node.state import Role
from raftkv.rpc.client import RPCClient
from raftkv.rpc.network_sim import NetworkSimulator


class TestCluster:
    def __init__(self, node_ids: list[str], base_port: int, data_dir: str):
        self.data_dir = data_dir
        shutil.rmtree(data_dir, ignore_errors=True)

        self.addresses = {nid: f"localhost:{base_port + i}" for i, nid in enumerate(node_ids)}
        self.network_sim = NetworkSimulator()
        self.client = RPCClient()
        self.servers: dict[str, RaftServer] = {}

        for node_id in node_ids:
            self._create_server(node_id)

    def _create_server(self, node_id: str) -> None:
        peers = {nid: addr for nid, addr in self.addresses.items() if nid != node_id}
        config = NodeConfig(
            node_id=node_id, address=self.addresses[node_id], peers=peers, data_dir=self.data_dir
        )
        server = RaftServer(config, network_sim=self.network_sim)
        self.servers[node_id] = server

    def start_all(self) -> None:
        for server in self.servers.values():
            server.start()

    def stop_all(self) -> None:
        for server in self.servers.values():
            server.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def kill(self, node_id: str) -> None:
        """Simulates a hard crash -- stops the server, does NOT clean up its data dir."""
        self.servers[node_id].stop()

    def restart(self, node_id: str) -> None:
        """Simulates a crashed node coming back -- reloads from disk via Storage.load()."""
        self._create_server(node_id)
        self.servers[node_id].start()

    def get_state(self, node_id: str) -> dict | None:
        return self.client.get(self.addresses[node_id], "/state")

    def get_all_states(self) -> dict[str, dict]:
        return {nid: self.get_state(nid) for nid in self.addresses}

    def find_leader(self, timeout: float = 2.0) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for node_id in self.addresses:
                state = self.get_state(node_id)
                if state and state.get("role") == "leader":
                    return node_id
            time.sleep(0.05)
        return None

    def wait_for_commit(self, node_id: str, min_index: int, timeout: float = 2.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.get_state(node_id)
            if state and state.get("commit_index", 0) >= min_index:
                return True
            time.sleep(0.05)
        return False
