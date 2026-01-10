use serde::{Deserialize, Serialize};

/// A prepared entry for batch replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedEntry {
    /// Log index of this entry.
    pub index: u64,
    /// Payload data.
    pub payload: Vec<u8>,
}

/// VSR protocol messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VsrMessage {
    /// Prepare message sent by Primary to Backups.
    ///
    /// Contains the log entry to be replicated and the current commit point.
    Prepare {
        /// Current view number (for view change detection).
        view: u64,
        /// Log index of this entry.
        index: u64,
        /// Payload data to be replicated.
        payload: Vec<u8>,
        /// Primary's current committed index (piggyback commit notification).
        /// None means no entries have been committed yet.
        /// CRITICAL: Backups must NOT advance commit when this is None.
        commit_index: Option<u64>,
        /// Consensus timestamp (nanoseconds since Unix epoch).
        /// Assigned by Primary, agreed upon by quorum.
        /// Used for deterministic block_time in application logic.
        timestamp_ns: u64,
    },
    
    /// PrepareBatch message sent by Primary to Backups for group commit.
    ///
    /// Contains multiple log entries to be replicated atomically.
    /// Backups MUST acknowledge each entry individually for quorum tracking.
    PrepareBatch {
        /// Current view number.
        view: u64,
        /// First index in the batch.
        start_index: u64,
        /// Entries in the batch (indices are start_index, start_index+1, ...).
        entries: Vec<PreparedEntry>,
        /// Primary's current committed index (piggyback commit notification).
        /// None means no entries have been committed yet.
        /// CRITICAL: Backups must NOT advance commit when this is None.
        commit_index: Option<u64>,
        /// Consensus timestamp (nanoseconds since Unix epoch).
        /// Assigned by Primary when batch is created, agreed upon by quorum.
        /// All entries in the batch share this timestamp for deterministic execution.
        timestamp_ns: u64,
    },

    /// PrepareOk response sent by Backup to Primary.
    ///
    /// Indicates the Backup has durably stored the entry.
    PrepareOk {
        /// Log index that was successfully stored.
        index: u64,
        /// Node ID of the responding backup.
        node_id: u32,
    },

    /// Commit notification / Heartbeat message.
    ///
    /// Sent by Primary to Backups either:
    /// - After quorum is reached (commit notification)
    /// - Periodically when idle (heartbeat to prevent election timeout)
    Commit {
        /// View number.
        view: u64,
        /// New committed index.
        commit_index: u64,
    },

    /// StartViewChange message broadcast when a node suspects leader failure.
    ///
    /// This initiates the view change protocol.
    StartViewChange {
        /// The new view being proposed.
        new_view: u64,
        /// Node ID of the node initiating the view change.
        node_id: u32,
    },

    /// DoViewChange message sent to the new primary during view change.
    ///
    /// Contains the node's log state for reconciliation.
    DoViewChange {
        /// The new view number.
        new_view: u64,
        /// Node ID of the sender.
        node_id: u32,
        /// Last committed index on this node.
        commit_index: u64,
        /// Last log index on this node.
        last_log_index: u64,
        /// Hash of the last log entry (for verification).
        last_log_hash: [u8; 16],
        /// Log entries from commit_index+1 to last_log_index (for reconciliation).
        /// In a full implementation, this would be fetched separately.
        log_suffix: Vec<LogEntrySummary>,
    },

    /// StartView message sent by new Primary to establish the new view.
    ///
    /// Contains the reconciled log state for backups to synchronize.
    StartView {
        /// The new view number.
        new_view: u64,
        /// Node ID of the new primary.
        primary_id: u32,
        /// The committed index in the new view.
        commit_index: u64,
        /// The last log index in the new view.
        last_log_index: u64,
        /// Log entries that backups may be missing.
        log_entries: Vec<LogEntrySummary>,
    },
    
    /// CatchUpRequest sent by Backup to Primary when it detects a log gap.
    ///
    /// The backup requests entries from `from_index` to `to_index` (inclusive).
    /// This happens when a backup receives a Prepare with index > next_expected_index.
    CatchUpRequest {
        /// Current view number.
        view: u64,
        /// Node ID of the requesting backup.
        node_id: u32,
        /// First missing index (inclusive).
        from_index: u64,
        /// Last missing index (inclusive).
        to_index: u64,
    },
    
    /// CatchUpResponse sent by Primary to Backup with missing entries.
    ///
    /// Contains the requested log entries for the backup to replay.
    CatchUpResponse {
        /// Current view number.
        view: u64,
        /// Entries being sent (may be a subset if too large).
        entries: Vec<CatchUpEntry>,
        /// True if there are more entries to fetch after these.
        has_more: bool,
        /// Current commit index on primary.
        commit_index: u64,
    },
}

/// Entry in a catch-up response with full metadata for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatchUpEntry {
    /// Log index.
    pub index: u64,
    /// Payload data.
    pub payload: Vec<u8>,
    /// Consensus timestamp for deterministic replay.
    pub timestamp_ns: u64,
    /// Stream ID.
    pub stream_id: u64,
    /// Entry flags.
    pub flags: u16,
}

/// Summary of a log entry for view change reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntrySummary {
    /// Log index.
    pub index: u64,
    /// Payload data.
    pub payload: Vec<u8>,
}

/// Client request envelope with idempotency support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    /// Unique client identifier.
    pub client_id: u64,
    /// Monotonically increasing sequence number per client.
    pub sequence_number: u64,
    /// The actual request payload.
    pub payload: Vec<u8>,
}

/// Result of processing a client request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResult {
    /// Request was successfully applied.
    Success {
        /// Log index where the request was committed.
        log_index: u64,
    },
    /// Request failed with an error.
    Error {
        /// Error message.
        message: String,
    },
    /// This node is not the Primary - redirect to leader.
    NotThePrimary {
        /// Hint about which node is the current Primary.
        leader_hint: Option<u32>,
    },
    /// Request is pending (not yet committed).
    Pending,
}

/// Client response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    /// The sequence number this response is for.
    pub sequence_number: u64,
    /// The result of the request.
    pub result: ClientResult,
}

impl VsrMessage {
    /// Serialize message to bytes using bincode.
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("VsrMessage serialization should not fail")
    }

    /// Deserialize message from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    /// Get the index associated with this message (if any).
    pub fn index(&self) -> Option<u64> {
        match self {
            VsrMessage::Prepare { index, .. } => Some(*index),
            VsrMessage::PrepareBatch { start_index, entries, .. } => {
                // Return the last index in the batch
                if entries.is_empty() {
                    Some(*start_index)
                } else {
                    Some(start_index + entries.len() as u64 - 1)
                }
            }
            VsrMessage::PrepareOk { index, .. } => Some(*index),
            VsrMessage::Commit { .. } => None,
            VsrMessage::StartViewChange { .. } => None,
            VsrMessage::DoViewChange { .. } => None,
            VsrMessage::StartView { .. } => None,
            VsrMessage::CatchUpRequest { to_index, .. } => Some(*to_index),
            VsrMessage::CatchUpResponse { entries, .. } => {
                entries.last().map(|e| e.index)
            }
        }
    }
}
