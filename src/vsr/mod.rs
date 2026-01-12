#![allow(clippy::collapsible_if)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::while_let_loop)]

pub mod client;
pub mod error;
pub mod message;
pub mod network;
pub mod node;
pub mod quorum;

#[cfg(test)]
mod tests;

pub use client::{ChrClient, ClientSession, PendingRequest, SessionMap};
pub use error::VsrError;
pub use message::{ClientRequest, ClientResponse, ClientResult, LogEntrySummary, VsrMessage};
pub use network::MockNetwork;
pub use node::{DoViewChangeInfo, NodeRole, VsrNode, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL};
pub use quorum::QuorumTracker;
