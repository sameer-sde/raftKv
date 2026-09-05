"""
RaftKV benchmark suite.

Measures the same kind of hard, quantified numbers as your other projects
(Vektr's recall@10, Sentinel's PR-AUC, Rate Limiter's req/s):

  - Write throughput (ops/sec) through the real consensus + replication path
  - Commit latency (client submits -> majority commit), p50/p95/p99
  - Leader re-election time after a simulated crash
  - Read throughput (served locally, no consensus round-trip needed)

Run with: python -m tests.benchmark
"""

from __future__ import annotations

import statistics
import time

from tests.cluster_helper import TestCluster
from raftkv.kv.client import KVClient


def benchmark_write_throughput(num_writes: int = 200) -> dict:
    cluster = TestCluster(["n1", "n2", "n3"], base_port=9600, data_dir="/tmp/raftkv_bench_throughput")
    cluster.start_all()
    try:
        cluster.find_leader()
        client = KVClient(cluster.addresses)

        latencies_ms = []
        start = time.perf_counter()
        for i in range(num_writes):
            t0 = time.perf_counter()
            ok, _ = client.set(f"key{i}", f"value{i}", wait_for_commit=True)
            latencies_ms.append((time.perf_counter() - t0) * 1000)
            assert ok, f"write {i} failed"
        total_elapsed = time.perf_counter() - start

        sorted_lat = sorted(latencies_ms)
        return {
            "num_writes": num_writes,
            "total_seconds": round(total_elapsed, 3),
            "throughput_ops_sec": round(num_writes / total_elapsed, 1),
            "p50_latency_ms": round(sorted_lat[len(sorted_lat) // 2], 2),
            "p95_latency_ms": round(sorted_lat[int(len(sorted_lat) * 0.95)], 2),
            "p99_latency_ms": round(sorted_lat[int(len(sorted_lat) * 0.99)], 2),
        }
    finally:
        cluster.stop_all()


def benchmark_election_time(num_trials: int = 5) -> dict:
    election_times = []
    for trial in range(num_trials):
        cluster = TestCluster(["n1", "n2", "n3"], base_port=9650 + trial * 10, data_dir=f"/tmp/raftkv_bench_election_{trial}")
        cluster.start_all()
        try:
            leader = cluster.find_leader()
            cluster.kill(leader)

            start = time.perf_counter()
            remaining = [n for n in cluster.servers if n != leader]
            new_leader = None
            while time.perf_counter() - start < 3.0:
                for n in remaining:
                    s = cluster.get_state(n)
                    if s and s.get("role") == "leader":
                        new_leader = n
                        break
                if new_leader:
                    break
                time.sleep(0.005)
            elapsed = time.perf_counter() - start
            if new_leader:
                election_times.append(elapsed * 1000)
        finally:
            cluster.stop_all()

    return {
        "num_trials": num_trials,
        "successful_elections": len(election_times),
        "avg_election_ms": round(statistics.mean(election_times), 2) if election_times else None,
        "max_election_ms": round(max(election_times), 2) if election_times else None,
        "min_election_ms": round(min(election_times), 2) if election_times else None,
    }


def benchmark_read_throughput(num_reads: int = 500) -> dict:
    cluster = TestCluster(["n1", "n2", "n3"], base_port=9750, data_dir="/tmp/raftkv_bench_reads")
    cluster.start_all()
    try:
        cluster.find_leader()
        client = KVClient(cluster.addresses)
        client.set("bench_key", "bench_value")

        start = time.perf_counter()
        for _ in range(num_reads):
            value = client.get("bench_key")
            assert value == "bench_value"
        elapsed = time.perf_counter() - start

        return {
            "num_reads": num_reads,
            "total_seconds": round(elapsed, 3),
            "throughput_ops_sec": round(num_reads / elapsed, 1),
        }
    finally:
        cluster.stop_all()


def run_all_benchmarks() -> None:
    print("\n" + "=" * 60)
    print("RAFTKV BENCHMARK REPORT")
    print("=" * 60)

    print("\n[1/3] Write throughput (consensus + replication path)...")
    write_results = benchmark_write_throughput()
    for k, v in write_results.items():
        print(f"  {k}: {v}")

    print("\n[2/3] Leader re-election time after crash...")
    election_results = benchmark_election_time()
    for k, v in election_results.items():
        print(f"  {k}: {v}")

    print("\n[3/3] Read throughput (local, no consensus round-trip)...")
    read_results = benchmark_read_throughput()
    for k, v in read_results.items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    run_all_benchmarks()
