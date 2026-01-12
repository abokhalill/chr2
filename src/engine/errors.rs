use std::fmt;
use std::io;

/// Fatal errors that violate invariants and require immediate halt.
/// Per invariants.md: "Any invariant violation → immediate panic"
#[derive(Debug)]
pub enum FatalError {
    /// Broken hash chain: header.previous_hash mismatch
    /// Violates: Cryptographic Continuity (invariants.md I.2)
    BrokenChain {
        index: u64,
        expected: [u8; 16],
        found: [u8; 16],
    },

    /// Mid-log corruption: checksum failure with valid data ahead
    /// Violates: Immutable History (invariants.md I.1)
    MidLogCorruption { offset: u64, index: u64 },

    /// Zero-hole: zeros followed by non-zero data
    /// Violates: Gapless Monotonicity (invariants.md I.3)
    ZeroHole { zero_offset: u64, data_offset: u64 },

    /// Index gap or duplicate
    /// Violates: Gapless Monotonicity (invariants.md I.3)
    MonotonicityViolation { expected: u64, found: u64 },

    /// View ID decreased
    /// Violates: View Supersession (invariants.md III.2)
    ViewRegression {
        previous_view: u64,
        current_view: u64,
    },

    /// Payload size exceeds maximum
    /// Per recovery.md: Max Payload Size = 64 MB
    PayloadTooLarge { size: u32, max: u32 },

    /// IO error during critical operation
    IoError(io::Error),

    /// Internal invariant violation in the consensus/durability layer.
    /// This indicates a bug or corruption that cannot be recovered.
    InvariantViolation {
        component: &'static str,
        message: String,
    },
}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FatalError::BrokenChain {
                index,
                expected,
                found,
            } => {
                write!(
                    f,
                    "FATAL: Broken chain at index {}. Expected prev_hash {:02x?}, found {:02x?}",
                    index, expected, found
                )
            }
            FatalError::MidLogCorruption { offset, index } => {
                write!(
                    f,
                    "FATAL: Mid-log corruption at offset {} (after index {})",
                    offset, index
                )
            }
            FatalError::ZeroHole {
                zero_offset,
                data_offset,
            } => {
                write!(
                    f,
                    "FATAL: Zero-hole detected. Zeros at offset {}, data at offset {}",
                    zero_offset, data_offset
                )
            }
            FatalError::MonotonicityViolation { expected, found } => {
                write!(
                    f,
                    "FATAL: Monotonicity violation. Expected index {}, found {}",
                    expected, found
                )
            }
            FatalError::ViewRegression {
                previous_view,
                current_view,
            } => {
                write!(
                    f,
                    "FATAL: View regression. Previous view {}, current view {}",
                    previous_view, current_view
                )
            }
            FatalError::PayloadTooLarge { size, max } => {
                write!(f, "FATAL: Payload size {} exceeds maximum {}", size, max)
            }
            FatalError::IoError(e) => {
                write!(f, "FATAL: IO error: {}", e)
            }
            FatalError::InvariantViolation { component, message } => {
                write!(
                    f,
                    "FATAL: Invariant violation in {}: {}",
                    component, message
                )
            }
        }
    }
}

impl std::error::Error for FatalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FatalError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for FatalError {
    fn from(e: io::Error) -> Self {
        FatalError::IoError(e)
    }
}

/// Recoverable conditions that can be handled by truncation.
/// Per recovery.md VI: Only torn writes at the tail are recoverable.
#[derive(Debug)]
pub enum RecoverableError {
    /// Header CRC mismatch at tail
    HeaderCrcMismatch { offset: u64 },

    /// Payload hash mismatch at tail
    PayloadHashMismatch { offset: u64, index: u64 },

    /// Incomplete read (EOF before expected bytes)
    IncompleteRead {
        offset: u64,
        expected: usize,
        got: usize,
    },
}

impl fmt::Display for RecoverableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoverableError::HeaderCrcMismatch { offset } => {
                write!(f, "Recoverable: Header CRC mismatch at offset {}", offset)
            }
            RecoverableError::PayloadHashMismatch { offset, index } => {
                write!(
                    f,
                    "Recoverable: Payload hash mismatch at offset {} (index {})",
                    offset, index
                )
            }
            RecoverableError::IncompleteRead {
                offset,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Recoverable: Incomplete read at offset {}. Expected {} bytes, got {}",
                    offset, expected, got
                )
            }
        }
    }
}
