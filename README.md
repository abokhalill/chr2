# Chronon

[![Crates.io](https://img.shields.io/crates/v/chronon.svg)](https://crates.io/crates/chronon)
[![Documentation](https://docs.rs/chronon/badge.svg)](https://docs.rs/chronon)
[![License](https://img.shields.io/crates/l/chronon.svg)](LICENSE-MIT)

Deterministic execution kernel with crash-safe replication and exactly-once side effects.

## Guarantees

| Property | Mechanism |
|----------|-----------|
| Durability | Quorum writes survive f failures in 2f+1 cluster |
| Consistency | Linearizable via VSR consensus |
| Exactly-once | Durable outbox with epoch-fenced execution |
| Determinism | Hash-chained log, BTreeMap iteration, consensus timestamps |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Chronon Node                          │
├──────────────────────────────────────────────────────────────┤
│  VsrNode ──> Executor ──> ChrApplication                     │
│     │           │              │                             │
│     ▼           ▼              ▼                             │
│  Manifest    Snapshots    Outbox (BTreeMap)                  │
│  (fencing)  (compaction)  (deterministic order)              │
└──────────────────────────────────────────────────────────────┘
```

Control plane (heartbeats, elections) decoupled from data plane (writes, durability). Disk stalls don't trigger elections.

## Core Types

| Type | Purpose |
|------|---------|
| `LogIndex` | Strongly-typed log position. Not `u64`. |
| `ViewId` | Consensus term. Monotonic across elections. |
| `CommitState` | `Empty` vs `At(LogIndex)`. None ≠ 0. |
| `DurableEpoch` | Fencing token from `Manifest.highest_view`. |

## Components

### `engine/`

| Module | Purpose |
|--------|---------|
| `VirtualDisk` | Unified durability trait. Writes speculative until `barrier()` |
| `SyncDisk` | O_DSYNC pwritev. Implicit barrier per write |
| `IoUringDisk` | io_uring + O_DIRECT. Explicit fsync barrier |
| `LogWriter` | Single-writer append-only log with hash chain |
| `DurabilityWorker` | Background thread, accepts `Box<dyn VirtualDisk>` |
| `CommittedState` | Atomic commit tracking. `CommitState` API. |

### `vsr/`

| Module | Purpose |
|--------|---------|
| `VsrNode` | VSR primary/backup with view changes |
| `QuorumTracker` | Per-index ack tracking for commit advancement |
| `Manifest` | Durable view/vote state. Fences zombie leaders |
| `ReplicatedSessionMap` | Exactly-once client dedupe. Survives failover. |

### `kernel/`

| Module | Purpose |
|--------|---------|
| `Executor` | Deterministic state machine driver |
| `VsrAuthority` | Durable fencing for side effect execution |
| `SideEffectManager` | Epoch-fenced outbox execution. `lease == durable` or die. |
| `Outbox` | BTreeMap. Deterministic iteration across replicas. |
| `ChrApplication` | Your application trait |

### `chaos/`

Jepsen-grade testing: partitions, kills, clock skew. 145 tests. All pass.

## Usage

```rust
use chronon::kernel::traits::{ChrApplication, ApplyContext, Event, SideEffect, SnapshotStream};

struct MyApp;

impl ChrApplication for MyApp {
    type State = MyState;
    type QueryRequest = MyQuery;
    type QueryResponse = MyResponse;
    type Error = MyError;

    fn apply(
        &self,
        state: &Self::State,
        event: Event,
        ctx: &ApplyContext,
    ) -> Result<(Self::State, Vec<SideEffect>), Self::Error> {
        let new_state = state.clone();
        Ok((new_state, vec![]))
    }

    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse {
        todo!()
    }

    fn snapshot(&self, state: &Self::State) -> SnapshotStream {
        todo!()
    }

    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error> {
        todo!()
    }

    fn genesis(&self) -> Self::State {
        MyState::default()
    }
}
```

## Build

```bash
cargo build --release
cargo build --release --features io_uring  # Linux 5.1+
cargo test
```

## Principles

- **Crash-only**: No graceful shutdown. Crash anywhere, recover everywhere.
- **Hash chains**: Every entry links to predecessor. Corruption detectable.
- **Explicit durability**: `barrier()` or it didn't happen.
- **Fencing**: View numbers gate all leader operations. Zombies die fast.
