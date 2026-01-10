use crate::kernel::traits::SideEffect;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorStatus {
    Running,
    Halted,
}

#[derive(Debug)]
pub enum StepResult {
    Idle,
    Applied { index: u64, side_effects: Vec<SideEffect> },
    Rejected { index: u64, error: String },
}

#[derive(Debug)]
pub enum FatalError {
    PoisonPill { index: u64, message: String },
    Halted,
    ReadError(String),
    DeserializeError { index: u64, message: String },
    SnapshotError(String),
    NoValidSnapshot,
    LogBehindSnapshot { committed_index: Option<u64>, snapshot_index: u64 },
    ChainBridgeMismatch { entry_index: u64, expected_hash: [u8; 16], actual_hash: [u8; 16] },
    LogGap { snapshot_index: u64, missing_index: u64, found_index: u64 },
    RestoreError(String),
}

impl std::fmt::Display for FatalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FatalError::PoisonPill { index, message } => {
                write!(f, "Poison pill at index {}: {}", index, message)
            }
            FatalError::Halted => write!(f, "Executor is halted"),
            FatalError::ReadError(msg) => write!(f, "Read error: {}", msg),
            FatalError::DeserializeError { index, message } => {
                write!(f, "Deserialize error at index {}: {}", index, message)
            }
            FatalError::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
            FatalError::NoValidSnapshot => write!(f, "No valid snapshot found"),
            FatalError::LogBehindSnapshot { committed_index, snapshot_index } => {
                write!(
                    f,
                    "Log behind snapshot: committed_index={:?}, snapshot_index={}",
                    committed_index, snapshot_index
                )
            }
            FatalError::ChainBridgeMismatch { entry_index, expected_hash, actual_hash } => {
                write!(
                    f,
                    "Chain bridge mismatch at index {}: expected {:?}, got {:?}",
                    entry_index, expected_hash, actual_hash
                )
            }
            FatalError::LogGap { snapshot_index, missing_index, found_index } => {
                write!(
                    f,
                    "Log gap after snapshot {}: missing index {}, found index {}",
                    snapshot_index, missing_index, found_index
                )
            }
            FatalError::RestoreError(msg) => write!(f, "Restore error: {}", msg),
        }
    }
}

impl std::error::Error for FatalError {}
