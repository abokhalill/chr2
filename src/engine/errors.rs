use std::fmt;
use std::io;

/// Fatal errors requiring immediate halt. See invariants.md.
#[derive(Debug)]
pub enum FatalError {
    BrokenChain {
        index: u64,
        expected: [u8; 16],
        found: [u8; 16],
    },
    MidLogCorruption {
        offset: u64,
        index: u64,
    },
    ZeroHole {
        zero_offset: u64,
        data_offset: u64,
    },
    MonotonicityViolation {
        expected: u64,
        found: u64,
    },
    ViewRegression {
        previous_view: u64,
        current_view: u64,
    },
    PayloadTooLarge {
        size: u32,
        max: u32,
    },
    IoError(io::Error),
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

/// Recoverable via truncation (torn writes at tail only).
#[derive(Debug)]
pub enum RecoverableError {
    HeaderCrcMismatch {
        offset: u64,
    },
    PayloadHashMismatch {
        offset: u64,
        index: u64,
    },
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
