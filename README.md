# Chronon

**A deterministic, crash-safe distributed state machine with exactly-once side effects.**

Chronon is a Rust implementation of a replicated log and consensus engine designed for systems that cannot afford to lose data or execute side effects twice. It combines Viewstamped Replication (VSR) with a durable outbox pattern to guarantee exactly-once semantics even across crashes and leader failovers.

## Core Guarantees

| Property | Guarantee |
|----------|-----------|
| **Durability** | All committed entries survive any single-node failure |
| **Consistency** | Linearizable reads and writes via quorum consensus |
| **Exactly-Once** | Side effects execute exactly once, even after crashes |
| **Determinism** | Replicas converge to identical state from identical logs |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Chronon Node                              │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐  │
│  │  VSR Layer  │───>│   Kernel    │───>│   Application (Bank)    │  │
│  │  (Consensus)│    │  (Executor) │    │   ChrApplication trait  │  │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘  │
│         │                  │                       │                │
│         ▼                  ▼                       ▼                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐  │
│  │  Log Writer │    │  Snapshots  │    │   Durable Outbox        │  │
│  │  (O_DSYNC)  │    │ (Compaction)│    │   (Side Effects)        │  │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Control Plane vs Data Plane

Chronon architecturally divorces the **control plane** (heartbeats, elections, view changes) from the **data plane** (log writes, durability). This ensures:

- Heartbeats are never blocked by disk I/O
- Elections don't trigger during transient disk stalls  
- Tail latency on the data path doesn't cascade to cluster availability

## Key Components

### Storage Engine (`engine/`)

- **LogWriter**: Append-only log with `O_DSYNC` for synchronous durability
- **LogReader**: Lock-free concurrent reads with visibility guarantees
- **Recovery**: Crash recovery with torn-write detection and hash chain verification
- **IoUringWriter**: Optional `io_uring` backend with O_DIRECT and DMA buffer pools
- **DurabilityWorker**: Background thread for async durability (non-blocking writes)

### Consensus Layer (`vsr/`)

- **VsrNode**: Full VSR implementation with view changes and leader election
- **QuorumTracker**: Per-entry quorum tracking for commit advancement
- **SessionMap**: Client session tracking for exactly-once request semantics
- **Durable Fencing**: Manifest-based view persistence prevents zombie leaders

### Kernel (`kernel/`)

- **Executor**: Single-threaded deterministic state machine driver
- **SideEffectManager**: Fenced execution of durable side effects
- **Snapshots**: Point-in-time state capture with log compaction
- **ChrApplication trait**: Your application implements this

### Chaos Testing (`chaos/`)

- **ChaosNetwork**: Simulated network with partitions, latency, packet loss
- **Nemesis**: Automated fault injection (kill nodes, partition quorum)
- **Checker**: History recording for linearizability verification

## The Durable Outbox Pattern

Side effects (emails, webhooks, external API calls) are notoriously hard in distributed systems. Chronon solves this with a **durable outbox**:

1. Application emits side effect *intents* (not executions) during state transitions
2. Intents are stored in the replicated `Outbox` (part of application state)
3. Only the **Primary** executes effects, protected by fencing tokens
4. After execution, an `AcknowledgeEffect` event is committed
5. On failover, the new Primary re-executes any unacknowledged effects

**Result**: At-least-once delivery with application-level idempotency → exactly-once semantics.

## Usage

### Implementing the Application Trait

```rust
use chr::kernel::traits::{ChrApplication, ApplyContext, Event, SideEffect, SnapshotStream};
use std::error::Error;

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
        // Deserialize event.payload, apply to state, return new state + side effects
        let new_state = state.clone();
        Ok((new_state, vec![]))
    }
    
    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse {
        // Handle read-only queries
        todo!()
    }
    
    fn snapshot(&self, state: &Self::State) -> SnapshotStream {
        // Serialize state for persistence
        todo!()
    }
    
    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error> {
        // Deserialize state from snapshot
        todo!()
    }
    
    fn genesis(&self) -> Self::State {
        MyState::default()
    }
}
```

### Running the Crash Test Demo

```bash
# Run the built-in crash test harness
cargo run --release

# Or run individual phases:
cargo run --release -- clean    # Remove existing log
cargo run --release -- write    # Append entries (may crash randomly)
cargo run --release -- recover  # Run recovery and report state
cargo run --release -- test     # Full crash test cycle
```

## Building

```bash
# Standard build
cargo build --release

# With io_uring support (Linux 5.1+)
cargo build --release --features io_uring

# Run tests
cargo test

# Run specific test modules
cargo test engine::    # Storage engine tests
cargo test kernel::    # Executor and snapshot tests
cargo test vsr::       # Consensus layer tests
```

## Performance Characteristics

- **Group Commit**: Batches multiple client requests into single `fdatasync`
- **Zero-Copy I/O**: Optional O_DIRECT with 4KB-aligned DMA buffer pools
- **Lock-Free Reads**: Readers never block writers
- **Async Durability**: Background durability thread decouples I/O from consensus

## Design Principles

1. **Crash-only design**: No graceful shutdown logic. Crash anywhere, recover everywhere.
2. **Hash chains**: Every entry links to its predecessor. Corruption is detectable.
3. **Explicit durability**: No implicit caching. `fdatasync` or it didn't happen.
4. **Fencing everywhere**: View numbers, lease tokens, manifest versions. Zombies die fast.
5. **Test under chaos**: If it doesn't survive Nemesis, it doesn't ship.

## Status

Chronon is under active development. The core consensus and durability layers are functional with comprehensive test coverage.
