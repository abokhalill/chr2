use std::fmt;

use crate::engine::durability::DurabilityError;
use crate::engine::errors::FatalError;

#[derive(Debug)]
pub enum VsrError {
    NotPrimary,
    NotBackup,
    EmptyBatch,
    ViewMismatch { expected: u64, received: u64 },
    IndexMismatch { expected: u64, received: u64 },
    LogAppendFailed(FatalError),
    DurabilityFailed(DurabilityError),
    LogReadFailed(String),
    FencingViolation { view: u64, fence: u64 },
    InvalidCatchUpRange { from: u64, to: u64 },
    NoLogReader,
}

impl fmt::Display for VsrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VsrError::NotPrimary => write!(f, "Operation requires Primary role"),
            VsrError::NotBackup => write!(f, "Operation requires Backup role"),
            VsrError::EmptyBatch => write!(f, "Cannot submit empty batch"),
            VsrError::ViewMismatch { expected, received } => {
                write!(
                    f,
                    "View mismatch: expected {}, received {}",
                    expected, received
                )
            }
            VsrError::IndexMismatch { expected, received } => {
                write!(
                    f,
                    "Index mismatch: expected {}, received {}",
                    expected, received
                )
            }
            VsrError::LogAppendFailed(e) => write!(f, "Log append failed: {}", e),
            VsrError::DurabilityFailed(e) => write!(f, "Durability operation failed: {}", e),
            VsrError::LogReadFailed(msg) => write!(f, "Log read failed: {}", msg),
            VsrError::FencingViolation { view, fence } => {
                write!(
                    f,
                    "Fencing violation: view {} rejected by fence {}",
                    view, fence
                )
            }
            VsrError::InvalidCatchUpRange { from, to } => {
                write!(f, "Invalid catch-up range: {} to {}", from, to)
            }
            VsrError::NoLogReader => write!(f, "No LogReader available"),
        }
    }
}

impl std::error::Error for VsrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VsrError::LogAppendFailed(e) => Some(e),
            VsrError::DurabilityFailed(e) => Some(e),
            _ => None,
        }
    }
}

impl From<FatalError> for VsrError {
    fn from(e: FatalError) -> Self {
        VsrError::LogAppendFailed(e)
    }
}

impl From<DurabilityError> for VsrError {
    fn from(e: DurabilityError) -> Self {
        VsrError::DurabilityFailed(e)
    }
}

impl From<std::io::Error> for VsrError {
    fn from(e: std::io::Error) -> Self {
        VsrError::LogAppendFailed(FatalError::IoError(e))
    }
}
