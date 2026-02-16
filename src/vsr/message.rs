use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedEntry {
    pub index: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VsrMessage {
    Prepare {
        view: u64,
        index: u64,
        payload: Vec<u8>,
        commit_index: Option<u64>, // None = no commits yet
        timestamp_ns: u64,
    },
    PrepareBatch {
        view: u64,
        start_index: u64,
        entries: Vec<PreparedEntry>,
        commit_index: Option<u64>,
        timestamp_ns: u64,
    },
    PrepareOk {
        index: u64,
        node_id: u32,
    },
    Commit {
        view: u64,
        commit_index: Option<u64>,
    },
    StartViewChange {
        new_view: u64,
        node_id: u32,
    },
    DoViewChange {
        new_view: u64,
        node_id: u32,
        commit_index: u64,
        last_log_index: u64,
        last_log_hash: [u8; 16],
        log_suffix: Vec<LogEntrySummary>,
    },
    StartView {
        new_view: u64,
        primary_id: u32,
        commit_index: u64,
        last_log_index: u64,
        log_entries: Vec<LogEntrySummary>,
    },
    CatchUpRequest {
        view: u64,
        node_id: u32,
        from_index: u64,
        to_index: u64,
    },
    CatchUpResponse {
        view: u64,
        entries: Vec<CatchUpEntry>,
        has_more: bool,
        commit_index: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatchUpEntry {
    pub index: u64,
    pub payload: Vec<u8>,
    pub timestamp_ns: u64,
    pub stream_id: u64,
    pub flags: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntrySummary {
    pub index: u64,
    pub payload: Vec<u8>,
    pub timestamp_ns: u64,
    pub stream_id: u64,
    pub flags: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    pub client_id: u64,
    pub sequence_number: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResult {
    Success { log_index: u64 },
    Error { message: String },
    NotThePrimary { leader_hint: Option<u32> },
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    pub sequence_number: u64,
    pub result: ClientResult,
}

impl VsrMessage {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialization failed")
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    pub fn index(&self) -> Option<u64> {
        match self {
            VsrMessage::Prepare { index, .. } => Some(*index),
            VsrMessage::PrepareBatch {
                start_index,
                entries,
                ..
            } => {
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
            VsrMessage::CatchUpResponse { entries, .. } => entries.last().map(|e| e.index),
        }
    }
}
