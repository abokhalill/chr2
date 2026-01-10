//! Typed errors for VSR operations.

use std::fmt;

use crate::engine::durability::DurabilityError;
use crate::engine::errors::FatalError;

/// Errors that can occur during VSR protocol operations.
#[derive(Debug)]
pub enum VsrError {
    /// Operation requires Primary role but node is not Primary.
    NotPrimary,
    /// Operation requires Backup role but node is not Backup.
    NotBackup,
    /// Empty batch submitted.
    EmptyBatch,
    /// View mismatch - received message from different view.
    ViewMismatch { expected: u64, received: u64 },
    /// Index mismatch - received entry with unexpected index.
    IndexMismatch { expected: u64, received: u64 },
    /// Log append failed.
    LogAppendFailed(FatalError),
    /// Durability operation failed.
    DurabilityFailed(DurabilityError),
    /// Log read failed.
    LogReadFailed(String),
    /// Fencing violation - operation rejected due to view fence.
    FencingViolation { view: u64, fence: u64 },
    /// Catch-up range invalid.
    InvalidCatchUpRange { from: u64, to: u64 },
    /// No log reader available.
    NoLogReader,
}

impl fmt::Display for VsrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VsrError::NotPrimary => write!(f, "Operation requires Primary role"),
            VsrError::NotBackup => write!(f, "Operation requires Backup role"),
            VsrError::EmptyBatch => write!(f, "Cannot submit empty batch"),
            VsrError::ViewMismatch { expected, received } => {
                write!(f, "View mismatch: expected {}, received {}", expected, received)
            }
            VsrError::IndexMismatch { expected, received } => {
                write!(f, "Index mismatch: expected {}, received {}", expected, received)
            }
            VsrError::LogAppendFailed(e) => write!(f, "Log append failed: {}", e),
            VsrError::DurabilityFailed(e) => write!(f, "Durability operation failed: {}", e),
            VsrError::LogReadFailed(msg) => write!(f, "Log read failed: {}", msg),
            VsrError::FencingViolation { view, fence } => {
                write!(f, "Fencing violation: view {} rejected by fence {}", view, fence)
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
