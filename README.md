# ShardScale

ShardScale is a distributed in-memory key-value store written in Go, built to demonstrate core distributed-systems engineering patterns:

- Consistent hashing with virtual nodes
- Dynamic membership with heartbeat-based failure detection
- Background data rebalancing on topology change
- Replication across owners
- Tunable quorum consistency (`N`, `W`, `R`) for reads and writes

> Current implementation scope: **Phases 0–6** (through quorum reads/writes).

---

## Why this project

ShardScale is intentionally designed as a practical systems-engineering project:

- Focuses on correctness, observability, and bounded concurrency
- Surfaces real trade-offs between latency, availability, and consistency
- Keeps architecture understandable (no consensus or vector-clock layer yet)

This makes it useful for learning, benchmarking, and interview discussion.

---

## Core capabilities

- **Deterministic partitioning** via consistent hash ring (`internal/ring`)
- **Replication-aware owner selection** (`GetOwners(key, N)`)
- **Quorum writes** with fast-fail and timeout bounds (`internal/router`)
- **Quorum reads** with parallel fan-out and not-found quorum handling
- **Topology updates** on node join/failure (`internal/membership`)
- **Live rebalance** with bounded worker pool and safe-delete checks (`internal/rebalance`)
- **Operational metrics** with atomic counters (`/metrics`)

---

## High-level architecture

Each node runs the same stack:

1. HTTP server
2. API handlers
3. Router (quorum coordinator)
4. Local store (`map + RWMutex`)
5. Consistent hash ring
6. Membership monitor (heartbeats)
7. Rebalancer
8. Metrics collector

Composition entry points:

- `cmd/server/main.go`
- `internal/node/node.go`

---

## Consistency model (`N`, `W`, `R`)

- `N` = replication factor (`REPLICATION_FACTOR`)
- `W` = write quorum (`WRITE_QUORUM`)
- `R` = read quorum (`READ_QUORUM`)

### Startup validation

- `W > N` => startup error
- `R > N` => startup error
- `R + W <= N` => startup warning (weak consistency profile)

### Practical rule

If `R + W > N`, read and write sets overlap under normal operation, reducing stale-read risk.

---

## Quick start

### Prerequisites

- Go 1.22+

### Single node

```bash
NODE_ID=node1 NODE_ADDR=localhost:8080 PEERS=node1=localhost:8080 \
VIRTUAL_NODES=50 REPLICATION_FACTOR=1 WRITE_QUORUM=1 READ_QUORUM=1 \
HEARTBEAT_INTERVAL=2s HEARTBEAT_TIMEOUT=6s \
go run ./cmd/server
```

### 3-node cluster (`N=3, W=2, R=2`)

Run each command in a separate terminal.

**Node 1**
```bash
NODE_ID=node1 NODE_ADDR=localhost:8080 \
PEERS=node2=localhost:8081,node3=localhost:8082 \
VIRTUAL_NODES=50 REPLICATION_FACTOR=3 WRITE_QUORUM=2 READ_QUORUM=2 \
HEARTBEAT_INTERVAL=2s HEARTBEAT_TIMEOUT=6s \
go run ./cmd/server
```

**Node 2**
```bash
NODE_ID=node2 NODE_ADDR=localhost:8081 \
PEERS=node1=localhost:8080,node3=localhost:8082 \
VIRTUAL_NODES=50 REPLICATION_FACTOR=3 WRITE_QUORUM=2 READ_QUORUM=2 \
HEARTBEAT_INTERVAL=2s HEARTBEAT_TIMEOUT=6s \
go run ./cmd/server
```

**Node 3**
```bash
NODE_ID=node3 NODE_ADDR=localhost:8082 \
PEERS=node1=localhost:8080,node2=localhost:8081 \
VIRTUAL_NODES=50 REPLICATION_FACTOR=3 WRITE_QUORUM=2 READ_QUORUM=2 \
HEARTBEAT_INTERVAL=2s HEARTBEAT_TIMEOUT=6s \
go run ./cmd/server
```

---

## API

### Public endpoints

- `PUT /kv/{key}`
- `GET /kv/{key}`
- `GET /metrics`
- `GET /health`

### Internal endpoints

- `POST /internal/join`
- `POST /internal/transfer`
- `POST /internal/replicate`
- `GET /internal/read/{key}`
- `GET /internal/heartbeat`

### Example usage

```bash
# write
curl -X PUT http://localhost:8080/kv/user:42 \
  -H "Content-Type: application/json" \
  -d '{"value":"alice"}'

# read
curl http://localhost:8081/kv/user:42

# health
curl http://localhost:8080/health

# metrics
curl -s http://localhost:8080/metrics
```

---

## Rebalancing behavior

Rebalance is triggered when:

- a node joins (`/internal/join`)
- a peer is removed after heartbeat timeout

Execution characteristics:

- single-flight guard (`inProgress`) prevents concurrent rebalance runs
- local snapshot is taken first, then tasks run via bounded worker pool
- key deletion happens only after successful transfer and value re-validation
- failures are retried and logged; convergence is eventual

---

## Observability

`GET /metrics` exposes:

- traffic: `total_requests`, `total_reads`, `total_writes`, `failed_requests`
- consistency: `replication_factor`, `write_quorum`, `read_quorum`
- quorum outcomes: `quorum_failures_total`, `successful_quorum_writes_total`, `successful_quorum_reads_total`
- replication/rebalance: `replica_writes_total`, `replica_failures_total`, `keys_migrated`, `migration_duration_ms`, `rebalance_in_progress`
- membership: `failed_nodes_detected`, `membership_changes`

---

## Benchmarking

A complete benchmarking workflow is included in:

- `docs/BENCHMARKING.md`

Included benchmark artifacts (sample run outputs) live in:

- `bench/results/`

Bench scenarios cover:

- steady-state throughput/latency
- rebalance impact under load
- failover behavior
- distribution skew checks

---

## Repository layout

```text
cmd/server/main.go           # process entrypoint and config validation
internal/api/                # HTTP handlers and endpoint contracts
internal/router/             # quorum read/write orchestration
internal/ring/               # consistent hashing and owner lookup
internal/store/              # thread-safe local key-value store
internal/membership/         # heartbeat and failure detection
internal/rebalance/          # migration and replica sync
internal/metrics/            # atomic metrics state
internal/server/             # HTTP server wrapper + middleware
docs/                        # architecture and phase documentation
bench/                       # benchmark playbook + outputs
```

---

## Deep technical docs

- `docs/ARCHITECTURE.md`
- `docs/DESIGN_CHOICES.md`
- `docs/PHASE6.md`
- `docs/QUICK_REFERENCE.md`
- `docs/internal/README.md` (per-file deep dives for every `internal/*.go`)
- `docs/interview prep.md` (project interview Q&A pack)

---

## Current limitations (intentional)

- In-memory only (no persistence across restarts)
- No consensus protocol (Raft/Paxos)
- No vector clocks or versioned conflict resolution
- No read-repair/anti-entropy background reconciliation
- Eventual membership convergence (possible transient view differences)

These are deliberate scope boundaries for the current phase and keep the implementation focused and explainable.

---

## Roadmap ideas

Potential next milestones:

- durable storage (WAL + snapshots)
- versioned values and read-repair
- anti-entropy background sync
- stronger membership coordination
- richer load/chaos testing automation

---

## Running with race detector

```bash
go run -race ./cmd/server
```

---

## Status

✅ Implemented and documented through Phase 6.

If you are evaluating this repo, start with `docs/QUICK_REFERENCE.md`, then `docs/ARCHITECTURE.md`, then the `docs/internal/` deep dives.
