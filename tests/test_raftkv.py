"""
Unit tests for pure logic components (no sockets/threads -- those are
covered by tests/fault_injection.py as integration tests instead).

Run with: python -m unittest discover tests -v
"""

import shutil
import unittest

from raftkv.node.state import PersistentState, LogEntry, Role
from raftkv.storage.persistence import Storage
from raftkv.rpc.messages import RequestVoteRequest, AppendEntriesRequest
from raftkv.rpc.network_sim import NetworkSimulator
from raftkv.kv.state_machine import KVStateMachine


class TestPersistentState(unittest.TestCase):
    def test_empty_log_defaults(self):
        state = PersistentState()
        self.assertEqual(state.last_log_index(), 0)
        self.assertEqual(state.last_log_term(), 0)
        self.assertIsNone(state.get_entry(1))

    def test_append_and_index(self):
        state = PersistentState()
        state.append_entries([LogEntry(term=1, index=1, command={"op": "SET"})])
        self.assertEqual(state.last_log_index(), 1)
        self.assertEqual(state.last_log_term(), 1)

    def test_truncate_from(self):
        state = PersistentState()
        state.append_entries(
            [
                LogEntry(term=1, index=1, command={}),
                LogEntry(term=1, index=2, command={}),
                LogEntry(term=2, index=3, command={}),
            ]
        )
        state.truncate_from(2)
        self.assertEqual(state.last_log_index(), 1)
        self.assertIsNone(state.get_entry(2))

    def test_get_entry_1_indexed(self):
        state = PersistentState()
        state.append_entries([LogEntry(term=5, index=1, command={"k": "v"})])
        entry = state.get_entry(1)
        self.assertEqual(entry.term, 5)
        self.assertIsNone(state.get_entry(0))
        self.assertIsNone(state.get_entry(2))


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.data_dir = "/tmp/raftkv_unittest_storage"
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_save_and_load_roundtrip(self):
        storage = Storage(self.data_dir, "test_node")
        state = PersistentState(current_term=7, voted_for="peer1")
        state.append_entries([LogEntry(term=7, index=1, command={"op": "SET", "key": "a", "value": "1"})])
        storage.save(state)

        reloaded = storage.load()
        self.assertEqual(reloaded.current_term, 7)
        self.assertEqual(reloaded.voted_for, "peer1")
        self.assertEqual(len(reloaded.log), 1)

    def test_load_without_existing_file_returns_fresh_state(self):
        storage = Storage(self.data_dir, "brand_new_node")
        state = storage.load()
        self.assertEqual(state.current_term, 0)
        self.assertIsNone(state.voted_for)
        self.assertEqual(len(state.log), 0)

    def test_atomic_write_leaves_no_tmp_file_on_success(self):
        import os

        storage = Storage(self.data_dir, "test_node2")
        storage.save(PersistentState(current_term=1))
        self.assertFalse(os.path.exists(storage.path + ".tmp"))
        self.assertTrue(os.path.exists(storage.path))


class TestRPCMessages(unittest.TestCase):
    def test_request_vote_serialization(self):
        req = RequestVoteRequest(term=1, candidate_id="n1", last_log_index=0, last_log_term=0)
        d = req.to_dict()
        self.assertEqual(d["term"], 1)
        self.assertEqual(d["candidate_id"], "n1")

    def test_append_entries_default_empty_entries(self):
        req = AppendEntriesRequest(term=1, leader_id="n1", prev_log_index=0, prev_log_term=0)
        self.assertEqual(req.entries, [])
        self.assertEqual(req.leader_commit, 0)


class TestNetworkSimulator(unittest.TestCase):
    def test_fully_connected_by_default(self):
        sim = NetworkSimulator()
        self.assertTrue(sim.is_connected("n1", "n2"))

    def test_partition_isolates_groups(self):
        sim = NetworkSimulator()
        sim.partition([["n1"], ["n2", "n3"]])
        self.assertFalse(sim.is_connected("n1", "n2"))
        self.assertTrue(sim.is_connected("n2", "n3"))

    def test_heal_restores_connectivity(self):
        sim = NetworkSimulator()
        sim.partition([["n1"], ["n2", "n3"]])
        sim.heal()
        self.assertTrue(sim.is_connected("n1", "n2"))


class TestKVStateMachine(unittest.TestCase):
    def test_set_and_get(self):
        kv = KVStateMachine()
        kv.apply(LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "value": "1"}))
        self.assertEqual(kv.get("x"), "1")

    def test_delete(self):
        kv = KVStateMachine()
        kv.apply(LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "value": "1"}))
        kv.apply(LogEntry(term=1, index=2, command={"op": "DELETE", "key": "x"}))
        self.assertIsNone(kv.get("x"))

    def test_delete_nonexistent_key_does_not_raise(self):
        kv = KVStateMachine()
        kv.apply(LogEntry(term=1, index=1, command={"op": "DELETE", "key": "nonexistent"}))
        self.assertIsNone(kv.get("nonexistent"))

    def test_applied_count_tracks_operations(self):
        kv = KVStateMachine()
        kv.apply(LogEntry(term=1, index=1, command={"op": "SET", "key": "a", "value": "1"}))
        kv.apply(LogEntry(term=1, index=2, command={"op": "SET", "key": "b", "value": "2"}))
        self.assertEqual(kv.applied_count(), 2)

    def test_snapshot_returns_copy(self):
        kv = KVStateMachine()
        kv.apply(LogEntry(term=1, index=1, command={"op": "SET", "key": "a", "value": "1"}))
        snap = kv.snapshot()
        snap["a"] = "modified"
        self.assertEqual(kv.get("a"), "1")  # original unaffected by mutating the snapshot


if __name__ == "__main__":
    unittest.main()
