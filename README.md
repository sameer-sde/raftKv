# RaftKV — A Distributed Key-Value Store with Raft Consensus

RaftKV is a from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout, 2014) — the same algorithm behind etcd (which powers
Kubernetes), CockroachCH, and Consul — with a simple key-value store built
on top. A cluster of nodes agrees on a replicated, ordered log of operations
and stays consistent even when nodes crash, restart, or the network
partitions.

This isn't a wrapper around an existing consensus library — every piece
(leader election, log replication, commit rules, crash-safe persistence,
and the fault-injection tests that verify all of it) is implemented here.

## Why this project

Distributed consensus is one of the genuinely hard, foundational problems
in systems engineering — it's the reason MIT's graduate distributed
systems course (6.824) uses building Raft as its flagship assignment.
Anyone hiring for backend/infra roles recognizes it instantly as proof of
real systems understanding, not just API integration.

## Architecture

```
Client
  │  SET/GET/DELETE
  ▼
KVClient ──────► finds the current leader, retries on redirect
  │
  ▼
RaftServer (one per node) ─────► RPC layer (real HTTP, stdlib only)
  │
  ▼
RaftNode ──────► the actual algorithm:
  │                - leader election (randomized timeouts, term tracking)
  │                - log replication (AppendEntries, fast conflict backtrack)
  │                - majority-commit rule
  │                - crash-safe persistence (atomic write-to-tmp-then-rename)
  ▼
KVStateMachine ─► applies committed entries to an in-memory dict
```

## What makes this a genuinely hard project, not a toy

- **Leader election** with randomized timeouts (prevents split-vote livelock),
  term tracking, and the log-completeness voting restriction from the Raft paper.
- **Log replication** with conflict detection and fast backtracking (the
  leader doesn't decrement `next_index` one-by-one on every mismatch —
  it uses the conflict term/index hint from the Raft paper's optimization).
- **Crash-safe persistence** — atomic write-to-tmp-then-rename (same pattern
  used in [Vektr](#) for index persistence), `fsync`'d before responding to
  any RPC that depends on it.
- **A controlled network partition simulator** (the same technique used in
  MIT 6.824's own test suite) — lets tests deterministically split the
  cluster into isolated groups and verify the minority side is blocked
  from committing writes.
- **A real concurrency bug, found and fixed.** Early fault-injection testing
  surfaced a case where majority-side re-election occasionally stalled for
  multiple seconds. Root cause: the election-timer loop was calling into
  the election logic *while still holding the node's lock*, which included
  blocking network calls to peers — this prevented the node from responding
  to incoming votes from other nodes during its own election attempt.
  Fixed by releasing the lock before making any network calls. This is
  documented in detail in `raftkv/node/raft.py` and is a good example of
  the class of bug that's specific to concurrent/distributed systems.

## Fault-injection test results

Three real, live-cluster scenarios (`tests/fault_injection.py`), each run
against actual threads and actual sockets on localhost — not mocked:

1. **Leader crash mid-operation** → a new leader is elected and writes
   continue with zero data loss.
2. **Network partition** → the minority side (including a stale old leader)
   is correctly blocked from committing writes; the majority side elects
   its own leader and keeps making progress.
3. **Crashed node restart** → a node that was down while writes happened
   correctly catches back up via replication once it rejoins.

## Benchmark results

```
Write throughput:      38.9 ops/sec  (p50: 25.7ms, p95: 27.3ms, p99: 28.6ms)
Leader re-election:    avg 5.9ms, max 8.5ms (5/5 successful trials)
Read throughput:       2,195 ops/sec (served locally, no consensus needed)
```

Write throughput was **doubled** (19.6 → 38.9 ops/sec) by one concrete fix:
triggering replication immediately on write instead of waiting for the next
periodic heartbeat tick — a real measure → diagnose → fix → re-measure cycle,
not just a static number.

*(These numbers are from a single-machine, 3-node localhost cluster — real
deployment numbers depend heavily on network latency between nodes.)*

## Known simplifications (documented deliberately, not oversights)

- **Reads can be served by any node**, including followers, for simplicity.
  This means a read can be very slightly stale if it hits a follower that
  hasn't yet received the latest replication. Real production systems solve
  this with "read index" or lease-based reads — noted here as a real,
  understood tradeoff.
- **No log compaction/snapshotting.** The log grows unbounded. Production
  Raft implementations periodically snapshot the state machine and truncate
  the log — a natural "what I'd add next" extension.
- **Network partitioning uses a simulator, not real network-level tooling**
  (iptables/tc), since all nodes run on localhost. This is standard practice
  for deterministic Raft testing (used by MIT 6.824's own test suite).

## Running it

```bash
# Quick in-process demo (no setup needed)
python main.py demo

# Run unit tests (17 tests, pure logic, no sockets)
python -m unittest tests.test_raftkv -v

# Run fault-injection tests (real sockets, real threads, ~10s)
python -m tests.fault_injection

# Run the benchmark suite
python -m tests.benchmark

# Run a REAL 3-node cluster as separate processes (3 terminals):
python main.py node --id n1 --port 8001 --peers n2=localhost:8002,n3=localhost:8003
python main.py node --id n2 --port 8002 --peers n1=localhost:8001,n3=localhost:8003
python main.py node --id n3 --port 8003 --peers n1=localhost:8001,n2=localhost:8002

# From a 4th terminal, talk to the cluster:
python main.py client set foo bar --cluster n1=localhost:8001,n2=localhost:8002,n3=localhost:8003
python main.py client get foo --cluster n1=localhost:8001,n2=localhost:8002,n3=localhost:8003
```

## What I'd add next

- Log compaction / snapshotting (the log currently grows unbounded)
- Read-index or lease-based reads for linearizable reads from followers
- Membership changes (adding/removing nodes from a live cluster)
- A live dashboard visualizing leader/term/log state across the cluster in real time

## Project structure

```
raftkv/
├── raftkv/
│   ├── node/         # RaftNode (election + replication), NodeConfig, RaftServer
│   ├── rpc/           # HTTP transport (server/client), message types, network simulator
│   ├── storage/       # Crash-safe disk persistence
│   └── kv/             # KVStateMachine (applies committed entries) + KVClient
├── tests/
│   ├── test_raftkv.py       # 17 unit tests (pure logic, no sockets)
│   ├── fault_injection.py    # 3 live-cluster fault scenarios
│   ├── cluster_helper.py     # Test cluster helper (real threads/sockets)
│   └── benchmark.py          # Throughput/latency/election-time benchmarks
├── main.py             # CLI: run nodes, run a client, or an in-process demo
└── requirements.txt     # Empty on purpose -- stdlib only, zero dependencies
```
