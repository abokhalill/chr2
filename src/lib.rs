//! Chronon: Deterministic execution kernel with crash-safe replication.
//!
//! This crate provides the building blocks for deterministic state machine
//! replication with exactly-once side effect execution.
//!
//! # Modules
//!
//! - [`engine`]: Storage engine with crash-safe append-only log, recovery, and durability.
//! - [`kernel`]: Deterministic executor, application trait, side effect management, snapshots.
//! - [`vsr`]: Viewstamped Replication consensus protocol implementation.
//! - [`chaos`]: Chaos testing framework for adversarial fault injection.

pub mod engine;
pub mod kernel;
pub mod vsr;
pub mod chaos;
