"""
The Raft node: leader election + the role state machine.

This is the heart of the algorithm. Key correctness rules encoded here
directly from the Raft paper (Ongaro & Ousterhout, 2014):

  1. A node votes for at most one candidate per term (persisted before
     responding, so a crash+restart can't cause a double vote).
  2. A node only grants a vote if the candidate's log is at least as
     up-to-date as its own (the "election restriction" -- this is what
     guarantees a leader always has all committed entries).
  3. Randomized election timeouts prevent split votes from repeating
     forever (if every node timed out at exactly the same moment, you'd
     get a new election, a new split vote, forever).
  4. Seeing a higher term in ANY message immediately reverts a node to
     follower and updates its term -- terms only move forward, never back.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass

from raftkv.node.state import PersistentState, VolatileState, LeaderVolatileState, Role, LogEntry
from raftkv.storage.persistence import Storage
from raftkv.rpc.client import RPCClient
from raftkv.rpc.messages import (
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)


ELECTION_TIMEOUT_RANGE = (0.15, 0.30)  # seconds -- randomized to avoid split-vote livelock
HEARTBEAT_INTERVAL = 0.05  # leader sends heartbeats well inside the election timeout


@dataclass
class NodeConfig:
    node_id: str
    address: str  # "host:port" -- this node's own address
    peers: dict[str, str]  # node_id -> "host:port" for every OTHER node
    data_dir: str = "/tmp/raftkv_data"


class RaftNode:
    def __init__(self, config: NodeConfig, on_commit=None, network_sim=None):
        self.config = config
        self.storage = Storage(config.data_dir, config.node_id)
        self.rpc_client = RPCClient()
        self.network_sim = network_sim  # optional -- None means always fully connected

        self.persistent = self.storage.load()  # survives restart
        self.volatile = VolatileState()
        self.leader_volatile = LeaderVolatileState()

        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self._lock = threading.RLock()
        self._last_heartbeat_received = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._stopped = False

        # Called with each committed LogEntry -- this is how the KV layer
        # applies committed commands to its state machine.
        self.on_commit = on_commit

        self._election_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None

    # ---------- lifecycle ----------

    def start(self) -> None:
        self._stopped = False
        self._election_thread = threading.Thread(target=self._election_timer_loop, daemon=True)
        self._election_thread.start()

    def stop(self) -> None:
        self._stopped = True

    # ---------- helpers ----------

    @staticmethod
    def _random_timeout() -> float:
        return random.uniform(*ELECTION_TIMEOUT_RANGE)

    def _reset_election_timer(self) -> None:
        self._last_heartbeat_received = time.monotonic()
        self._election_timeout = self._random_timeout()

    def _become_follower(self, term: int) -> None:
        """Terms only move forward. Seeing a higher term always reverts to follower."""
        self.role = Role.FOLLOWER
        self.persistent.current_term = term
        self.persistent.voted_for = None
        self.storage.save(self.persistent)

    # ---------- election timer (runs continuously on a background thread) ----------

    def _election_timer_loop(self) -> None:
        while not self._stopped:
            time.sleep(0.01)
            with self._lock:
                if self.role == Role.LEADER:
                    continue
                elapsed = time.monotonic() - self._last_heartbeat_received
                should_start = elapsed >= self._election_timeout
            # IMPORTANT: _start_election() is called OUTSIDE the lock. It makes
            # blocking network calls to peers (up to rpc timeout each); holding
            # the lock across those calls would prevent this node's own RPC
            # handlers (handle_request_vote / handle_append_entries) from
            # acquiring the lock, stalling the whole node during its own
            # election attempt. This was a real bug caught by fault-injection
            # testing -- see README for details.
            if should_start:
                self._start_election()

    def _start_election(self) -> None:
        """Transition to candidate and request votes from all peers."""
        self.role = Role.CANDIDATE
        self.persistent.current_term += 1
        self.persistent.voted_for = self.config.node_id
        self.storage.save(self.persistent)
        self._reset_election_timer()

        current_term = self.persistent.current_term
        votes_received = {self.config.node_id}  # vote for self

        request = RequestVoteRequest(
            term=current_term,
            candidate_id=self.config.node_id,
            last_log_index=self.persistent.last_log_index(),
            last_log_term=self.persistent.last_log_term(),
        )

        # Request votes from every peer in parallel-ish (sequential is fine
        # here since each call has a short timeout and peers are few).
        for peer_id, peer_address in self.config.peers.items():
            if self.network_sim and not self.network_sim.is_connected(self.config.node_id, peer_id):
                continue  # simulated partition -- treat exactly like an unreachable peer
            response_dict = self.rpc_client.call(peer_address, "/request_vote", request.to_dict())
            if response_dict is None:
                continue  # peer unreachable -- Raft tolerates this
            response = RequestVoteResponse(**response_dict)

            with self._lock:
                if response.term > self.persistent.current_term:
                    self._become_follower(response.term)
                    return
                if self.role != Role.CANDIDATE or self.persistent.current_term != current_term:
                    return  # a newer election started or we already stepped down
                if response.vote_granted:
                    votes_received.add(peer_id)

        majority = (len(self.config.peers) + 1) // 2 + 1
        with self._lock:
            if self.role == Role.CANDIDATE and len(votes_received) >= majority:
                self._become_leader()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.config.node_id
        last_index = self.persistent.last_log_index()
        self.leader_volatile.next_index = {p: last_index + 1 for p in self.config.peers}
        self.leader_volatile.match_index = {p: 0 for p in self.config.peers}

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    # ---------- heartbeat loop (leader only) ----------

    def _heartbeat_loop(self) -> None:
        while not self._stopped:
            with self._lock:
                if self.role != Role.LEADER:
                    return
            self._send_append_entries_to_all()
            time.sleep(HEARTBEAT_INTERVAL)

    def _send_append_entries_to_all(self) -> None:
        """Leader sends AppendEntries (heartbeat, or with real entries) to every peer."""
        with self._lock:
            if self.role != Role.LEADER:
                return
            current_term = self.persistent.current_term

        for peer_id, peer_address in self.config.peers.items():
            threading.Thread(
                target=self._replicate_to_peer, args=(peer_id, peer_address, current_term), daemon=True
            ).start()

    def _replicate_to_peer(self, peer_id: str, peer_address: str, term: int) -> None:
        if self.network_sim and not self.network_sim.is_connected(self.config.node_id, peer_id):
            return  # simulated partition -- treat exactly like an unreachable peer
        with self._lock:
            if self.role != Role.LEADER or self.persistent.current_term != term:
                return
            next_idx = self.leader_volatile.next_index.get(peer_id, self.persistent.last_log_index() + 1)
            prev_log_index = next_idx - 1
            prev_entry = self.persistent.get_entry(prev_log_index)
            prev_log_term = prev_entry.term if prev_entry else 0

            entries_to_send = []
            idx = next_idx
            while True:
                entry = self.persistent.get_entry(idx)
                if entry is None:
                    break
                entries_to_send.append(entry.to_dict())
                idx += 1

            request = AppendEntriesRequest(
                term=term,
                leader_id=self.config.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_to_send,
                leader_commit=self.volatile.commit_index,
            )

        response_dict = self.rpc_client.call(peer_address, "/append_entries", request.to_dict())
        if response_dict is None:
            return  # peer unreachable this round -- will retry on the next heartbeat tick

        response = AppendEntriesResponse(**response_dict)
        with self._lock:
            if response.term > self.persistent.current_term:
                self._become_follower(response.term)
                return
            if self.role != Role.LEADER or self.persistent.current_term != term:
                return

            if response.success:
                self.leader_volatile.match_index[peer_id] = prev_log_index + len(entries_to_send)
                self.leader_volatile.next_index[peer_id] = self.leader_volatile.match_index[peer_id] + 1
                self._advance_commit_index()
            else:
                # Fast backtrack using the conflict hint instead of decrementing by 1 each time.
                if response.conflict_index is not None:
                    self.leader_volatile.next_index[peer_id] = max(1, response.conflict_index)
                else:
                    self.leader_volatile.next_index[peer_id] = max(1, next_idx - 1)

    def _advance_commit_index(self) -> None:
        """A leader commits an entry once it's replicated on a majority of nodes."""
        majority = (len(self.config.peers) + 1) // 2 + 1
        for candidate_index in range(self.persistent.last_log_index(), self.volatile.commit_index, -1):
            entry = self.persistent.get_entry(candidate_index)
            if entry is None or entry.term != self.persistent.current_term:
                # Raft safety rule: a leader can only commit entries from its OWN term
                # directly; earlier-term entries get committed indirectly once a
                # same-term entry after them commits.
                continue
            replicated_count = 1  # the leader itself
            replicated_count += sum(
                1 for idx in self.leader_volatile.match_index.values() if idx >= candidate_index
            )
            if replicated_count >= majority:
                self._apply_committed_up_to(candidate_index)
                break

    def _apply_committed_up_to(self, index: int) -> None:
        if index <= self.volatile.commit_index:
            return
        for i in range(self.volatile.commit_index + 1, index + 1):
            entry = self.persistent.get_entry(i)
            if entry and self.on_commit:
                self.on_commit(entry)
        self.volatile.commit_index = index

    # ---------- client-facing: submit a new command (leader only) ----------

    def submit(self, command: dict) -> tuple[bool, str | None]:
        """
        Appends a command to the log if this node is the leader. Returns
        (accepted, redirect_leader_id). Does NOT block until committed --
        callers needing that guarantee should poll get_state_snapshot()
        or check commit_index, matching how real Raft clients behave.

        Triggers replication immediately rather than waiting for the next
        periodic heartbeat tick -- this is what real Raft implementations
        do to keep write latency close to one network round-trip instead
        of being bottlenecked by the heartbeat interval.
        """
        with self._lock:
            if self.role != Role.LEADER:
                return False, self.leader_id
            entry = LogEntry(
                term=self.persistent.current_term,
                index=self.persistent.last_log_index() + 1,
                command=command,
            )
            self.persistent.append_entries([entry])
            self.storage.save(self.persistent)
        self._send_append_entries_to_all()  # immediate replication, outside the lock
        return True, self.config.node_id

    # ---------- RPC handlers (called by the RPC server when a peer contacts us) ----------

    def handle_append_entries(self, body: dict) -> dict:
        request = AppendEntriesRequest(**body)
        with self._lock:
            if request.term < self.persistent.current_term:
                return AppendEntriesResponse(term=self.persistent.current_term, success=False).to_dict()

            # Any valid AppendEntries from a current-or-newer leader resets our timer
            # and demotes us to follower (covers candidates and stale leaders too).
            if request.term > self.persistent.current_term:
                self._become_follower(request.term)
            self.role = Role.FOLLOWER
            self.leader_id = request.leader_id
            self._reset_election_timer()

            # Consistency check: do we have an entry at prev_log_index with matching term?
            if request.prev_log_index > 0:
                prev_entry = self.persistent.get_entry(request.prev_log_index)
                if prev_entry is None:
                    return AppendEntriesResponse(
                        term=self.persistent.current_term,
                        success=False,
                        conflict_index=self.persistent.last_log_index() + 1,
                    ).to_dict()
                if prev_entry.term != request.prev_log_term:
                    # Find the first index of the conflicting term for a fast backtrack.
                    conflict_term = prev_entry.term
                    conflict_index = request.prev_log_index
                    while conflict_index > 1 and self.persistent.get_entry(conflict_index - 1).term == conflict_term:
                        conflict_index -= 1
                    return AppendEntriesResponse(
                        term=self.persistent.current_term,
                        success=False,
                        conflict_index=conflict_index,
                        conflict_term=conflict_term,
                    ).to_dict()

            # Append new entries, truncating any conflicting existing ones first.
            insert_index = request.prev_log_index + 1
            for offset, entry_dict in enumerate(request.entries):
                entry = LogEntry.from_dict(entry_dict)
                existing = self.persistent.get_entry(insert_index + offset)
                if existing and existing.term != entry.term:
                    self.persistent.truncate_from(insert_index + offset)
                    existing = None
                if existing is None:
                    self.persistent.append_entries([entry])
            self.storage.save(self.persistent)

            if request.leader_commit > self.volatile.commit_index:
                new_commit = min(request.leader_commit, self.persistent.last_log_index())
                self._apply_committed_up_to(new_commit)

            return AppendEntriesResponse(term=self.persistent.current_term, success=True).to_dict()

    def handle_request_vote(self, body: dict) -> dict:
        request = RequestVoteRequest(**body)
        with self._lock:
            if request.term > self.persistent.current_term:
                self._become_follower(request.term)

            if request.term < self.persistent.current_term:
                return RequestVoteResponse(term=self.persistent.current_term, vote_granted=False).to_dict()

            already_voted_for_other = (
                self.persistent.voted_for is not None
                and self.persistent.voted_for != request.candidate_id
            )
            candidate_log_ok = (
                request.last_log_term > self.persistent.last_log_term()
                or (
                    request.last_log_term == self.persistent.last_log_term()
                    and request.last_log_index >= self.persistent.last_log_index()
                )
            )

            if not already_voted_for_other and candidate_log_ok:
                self.persistent.voted_for = request.candidate_id
                self.storage.save(self.persistent)
                self._reset_election_timer()
                return RequestVoteResponse(term=self.persistent.current_term, vote_granted=True).to_dict()

            return RequestVoteResponse(term=self.persistent.current_term, vote_granted=False).to_dict()

    def get_state_snapshot(self) -> dict:
        with self._lock:
            return {
                "node_id": self.config.node_id,
                "role": self.role.value,
                "term": self.persistent.current_term,
                "leader_id": self.leader_id,
                "log_length": len(self.persistent.log),
                "commit_index": self.volatile.commit_index,
            }
