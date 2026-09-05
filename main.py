"""
RaftKV CLI.

Run a real 3-node cluster as three separate OS processes (not just
threads inside one test) by opening three terminal tabs:

    Terminal 1: python main.py node --id n1 --port 8001 --peers n2=localhost:8002,n3=localhost:8003
    Terminal 2: python main.py node --id n2 --port 8002 --peers n1=localhost:8001,n3=localhost:8003
    Terminal 3: python main.py node --id n3 --port 8003 --peers n1=localhost:8001,n2=localhost:8002

Then, from a fourth terminal, talk to the cluster:

    python main.py client set foo bar --cluster n1=localhost:8001,n2=localhost:8002,n3=localhost:8003
    python main.py client get foo --cluster n1=localhost:8001,n2=localhost:8002,n3=localhost:8003

Or run everything in-process for a quick local demo:

    python main.py demo
"""

from __future__ import annotations

import argparse
import sys
import time

from raftkv.node.server import RaftServer
from raftkv.node.raft import NodeConfig
from raftkv.kv.client import KVClient


def parse_peer_list(peer_str: str) -> dict[str, str]:
    """Parses 'n2=localhost:8002,n3=localhost:8003' into a dict."""
    peers = {}
    if not peer_str:
        return peers
    for pair in peer_str.split(","):
        node_id, address = pair.split("=")
        peers[node_id.strip()] = address.strip()
    return peers


def cmd_node(args: argparse.Namespace) -> None:
    peers = parse_peer_list(args.peers)
    config = NodeConfig(
        node_id=args.id,
        address=f"localhost:{args.port}",
        peers=peers,
        data_dir=args.data_dir,
    )
    server = RaftServer(config)
    server.start()
    print(f"Node '{args.id}' listening on localhost:{args.port}, peers={list(peers.keys())}")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
            state = server.node.get_state_snapshot()
            print(f"  [{args.id}] role={state['role']} term={state['term']} "
                  f"leader={state['leader_id']} log_len={state['log_length']} "
                  f"commit={state['commit_index']}")
    except KeyboardInterrupt:
        print(f"\nStopping node '{args.id}'...")
        server.stop()


def cmd_client(args: argparse.Namespace) -> None:
    cluster_addresses = parse_peer_list(args.cluster)
    client = KVClient(cluster_addresses)

    if args.action == "set":
        ok, msg = client.set(args.key, args.value)
        print(f"SET {args.key}={args.value}: accepted={ok} ({msg})")
    elif args.action == "get":
        value = client.get(args.key)
        print(f"GET {args.key}: {value}")
    elif args.action == "delete":
        ok, msg = client.delete(args.key)
        print(f"DELETE {args.key}: accepted={ok} ({msg})")


def cmd_demo(args: argparse.Namespace) -> None:
    """Runs a full 3-node cluster in-process for a quick, dependency-free demo."""
    from tests.cluster_helper import TestCluster

    print("Starting a 3-node RaftKV cluster in-process...")
    cluster = TestCluster(["n1", "n2", "n3"], base_port=8101, data_dir="/tmp/raftkv_demo")
    cluster.start_all()
    time.sleep(0.5)

    leader = cluster.find_leader()
    print(f"Leader elected: {leader}\n")

    client = KVClient(cluster.addresses)
    print("SET name=RaftKV ->", client.set("name", "RaftKV"))
    print("GET name ->", client.get("name"))
    print("SET language=Python ->", client.set("language", "Python"))
    print("GET language ->", client.get("language"))

    print(f"\nKilling leader {leader} to demonstrate failover...")
    cluster.kill(leader)
    client._known_leader = None
    print("SET after crash ->", client.set("survived_crash", "yes"))
    print("GET survived_crash ->", client.get("survived_crash"))

    cluster.stop_all()
    print("\nDemo complete.")


def main():
    parser = argparse.ArgumentParser(description="RaftKV -- distributed key-value store")
    subparsers = parser.add_subparsers(dest="command", required=True)

    node_parser = subparsers.add_parser("node", help="Run a single Raft node process")
    node_parser.add_argument("--id", required=True)
    node_parser.add_argument("--port", type=int, required=True)
    node_parser.add_argument("--peers", default="", help="comma-separated id=host:port list")
    node_parser.add_argument("--data-dir", default="/tmp/raftkv_data")
    node_parser.set_defaults(func=cmd_node)

    client_parser = subparsers.add_parser("client", help="Talk to a running cluster")
    client_parser.add_argument("action", choices=["set", "get", "delete"])
    client_parser.add_argument("key")
    client_parser.add_argument("value", nargs="?", default=None)
    client_parser.add_argument("--cluster", required=True, help="comma-separated id=host:port list")
    client_parser.set_defaults(func=cmd_client)

    demo_parser = subparsers.add_parser("demo", help="Run an in-process demo cluster")
    demo_parser.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
