"""
Fault injection tests -- the real point of building this project.

Anyone can write code that replicates data when everything works. The
actual claim Raft makes is that it stays CORRECT when things fail:
leader crashes mid-operation, network partitions, nodes restart with
stale state. These tests prove the implementation actually honors that.

Run with: python -m tests.fault_injection
"""

from __future__ import annotations

import time

from tests.cluster_helper import TestCluster


def test_leader_crash_triggers_reelection_no_data_loss():
    print("\n[TEST] Leader crash mid-operation -> re-election, no data loss")
    cluster = TestCluster(["n1", "n2", "n3"], base_port=9301, data_dir="/tmp/raftkv_test1")
    cluster.start_all()
    try:
        leader1 = cluster.find_leader()
        assert leader1 is not None, "no leader elected initially"
        print(f"  Initial leader: {leader1}")

        ok, _ = cluster.servers[leader1].node.submit({"op": "SET", "key": "a", "value": "1"})
        assert ok
        assert cluster.wait_for_commit(leader1, 1), "first write never committed"
        print("  Wrote key 'a' -- committed on initial leader")

        print(f"  Killing leader {leader1}...")
        cluster.kill(leader1)

        remaining = [n for n in cluster.servers if n != leader1]
        new_leader = None
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            for n in remaining:
                state = cluster.get_state(n)
                if state and state.get("role") == "leader":
                    new_leader = n
                    break
            if new_leader:
                break
            time.sleep(0.05)

        assert new_leader is not None, "no new leader elected after crash"
        assert new_leader != leader1
        print(f"  New leader elected: {new_leader} (took over from crashed {leader1})")

        ok, _ = cluster.servers[new_leader].node.submit({"op": "SET", "key": "b", "value": "2"})
        assert ok
        assert cluster.wait_for_commit(new_leader, 2), "second write never committed under new leader"
        print("  Wrote key 'b' under new leader -- committed successfully")
        print("  PASS: cluster survived leader crash with zero data loss")
    finally:
        cluster.stop_all()


def test_minority_partition_cannot_commit():
    print("\n[TEST] Network partition -> minority side cannot commit writes")
    cluster = TestCluster(["n1", "n2", "n3"], base_port=9401, data_dir="/tmp/raftkv_test2")
    cluster.start_all()
    try:
        leader = cluster.find_leader()
        assert leader is not None
        print(f"  Leader before partition: {leader}")

        others = [n for n in cluster.servers if n != leader]
        # Partition: leader alone vs. the other two (leader ends up in the minority)
        cluster.network_sim.partition([[leader], others])
        print(f"  Partitioned network: {{{leader}}} (minority) | {{{', '.join(others)}}} (majority)")
        time.sleep(0.5)

        # The old leader still THINKS it's leader (hasn't heard otherwise yet) --
        # it will accept the write into its own log, but must not be able to commit it.
        ok, _ = cluster.servers[leader].node.submit({"op": "SET", "key": "x", "value": "should_not_commit"})
        print(f"  Submitted write to isolated old leader {leader}: accepted={ok}")
        time.sleep(0.5)
        state = cluster.get_state(leader)
        print(f"  Isolated leader's commit_index: {state['commit_index']} (must still be 0)")
        assert state["commit_index"] == 0, "SAFETY VIOLATION: minority leader committed a write!"

        # Meanwhile the majority side should elect its own new leader and make progress.
        new_leader = None
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            for n in others:
                s = cluster.get_state(n)
                if s and s.get("role") == "leader":
                    new_leader = n
                    break
            if new_leader:
                break
            time.sleep(0.05)
        assert new_leader is not None, "majority side never elected a leader"
        print(f"  Majority side elected its own leader: {new_leader}")

        ok, _ = cluster.servers[new_leader].node.submit({"op": "SET", "key": "y", "value": "committed_ok"})
        assert cluster.wait_for_commit(new_leader, 1), "majority side failed to commit despite quorum"
        print("  Majority side successfully committed a write during the partition")
        print("  PASS: minority partition correctly blocked from committing; majority made progress")
    finally:
        cluster.network_sim.heal()
        cluster.stop_all()


def test_crashed_node_catches_up_after_restart():
    print("\n[TEST] Crashed node restarts and catches up to the cluster")
    cluster = TestCluster(["n1", "n2", "n3"], base_port=9501, data_dir="/tmp/raftkv_test3")
    cluster.start_all()
    try:
        leader = cluster.find_leader()
        assert leader is not None
        follower = [n for n in cluster.servers if n != leader][0]

        for i in range(3):
            ok, _ = cluster.servers[leader].node.submit({"op": "SET", "key": f"k{i}", "value": str(i)})
            assert ok
        assert cluster.wait_for_commit(leader, 3)
        print(f"  Wrote 3 entries via leader {leader}, all committed")

        print(f"  Crashing follower {follower}...")
        cluster.kill(follower)
        time.sleep(0.2)

        # Cluster keeps making progress while the follower is down.
        ok, _ = cluster.servers[leader].node.submit({"op": "SET", "key": "k3", "value": "3"})
        assert cluster.wait_for_commit(leader, 4)
        print(f"  Wrote 1 more entry while {follower} was down -- leader is now at index 4")

        print(f"  Restarting {follower} (reloading from disk)...")
        cluster.restart(follower)

        caught_up = cluster.wait_for_commit(follower, 4, timeout=3.0)
        assert caught_up, f"{follower} never caught up after restart"
        state = cluster.get_state(follower)
        print(f"  {follower} caught up: commit_index={state['commit_index']}, log_length={state['log_length']}")
        print("  PASS: restarted node successfully caught up via replication")
    finally:
        cluster.stop_all()


if __name__ == "__main__":
    test_leader_crash_triggers_reelection_no_data_loss()
    test_minority_partition_cannot_commit()
    test_crashed_node_catches_up_after_restart()
    print("\n" + "=" * 60)
    print("ALL FAULT-INJECTION TESTS PASSED")
    print("=" * 60)
