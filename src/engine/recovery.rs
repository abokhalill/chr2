use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::engine::errors::{FatalError, RecoverableError};
use crate::engine::format::{
    compute_chain_hash, compute_payload_hash, frame_size, is_sentinel_magic, verify_sentinel,
    LogHeader, LogMetadata, GENESIS_HASH, HEADER_SIZE, LOG_METADATA_SIZE, MAX_PAYLOAD_SIZE,
    SENTINEL_SIZE,
};

#[derive(Debug)]
pub enum RecoveryOutcome {
    CleanEmpty {
        base_index: u64,
        base_prev_hash: [u8; 16],
    },
    Clean {
        last_index: u64,
        next_offset: u64,
        tail_hash: [u8; 16],
        highest_view: u64,
        base_index: u64,
        base_prev_hash: [u8; 16],
    },
    Truncated {
        last_valid_index: u64,
        truncated_at: u64,
        new_offset: u64,
        tail_hash: [u8; 16],
        highest_view: u64,
        base_index: u64,
        base_prev_hash: [u8; 16],
    },
}

pub struct LogRecovery {
    file: File,
    file_size: u64,
}

impl LogRecovery {
    pub fn open(path: &Path) -> io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();

        Ok(Some(LogRecovery { file, file_size }))
    }

    /// Panics on fatal corruption (mid-log corruption, broken chain, etc).
    pub fn scan(mut self) -> Result<RecoveryOutcome, FatalError> {
        let mut hash_accumulator = GENESIS_HASH;
        let mut expected_index: u64 = 0;
        let mut scan_offset: u64 = 0;
        let mut highest_view: u64 = 0;
        let mut _last_valid_offset: u64 = 0;
        let mut base_index: u64 = 0;
        let mut base_prev_hash: [u8; 16] = GENESIS_HASH;

        if self.file_size == 0 {
            return Ok(RecoveryOutcome::CleanEmpty {
                base_index: 0,
                base_prev_hash: GENESIS_HASH,
            });
        }

        if self.file_size >= LOG_METADATA_SIZE as u64 {
            let mut meta_buf = [0u8; LOG_METADATA_SIZE];
            self.file.seek(SeekFrom::Start(0))?;
            let bytes_read = self.file.read(&mut meta_buf)?;

            if bytes_read == LOG_METADATA_SIZE {
                let metadata = LogMetadata::from_bytes(&meta_buf);
                if metadata.verify_magic() && metadata.verify_checksum() {
                    base_index = metadata.base_index;
                    base_prev_hash = metadata.base_prev_hash;
                    hash_accumulator = metadata.base_prev_hash;
                    expected_index = metadata.base_index;
                    scan_offset = LOG_METADATA_SIZE as u64;

                    if self.file_size == LOG_METADATA_SIZE as u64 {
                        return Ok(RecoveryOutcome::CleanEmpty {
                            base_index,
                            base_prev_hash,
                        });
                    }
                }
            }
        }

        let mut header_buf = [0u8; HEADER_SIZE];

        loop {
            if scan_offset >= self.file_size {
                if expected_index == base_index {
                    return Ok(RecoveryOutcome::CleanEmpty {
                        base_index,
                        base_prev_hash,
                    });
                } else {
                    return Ok(RecoveryOutcome::Clean {
                        last_index: expected_index - 1,
                        next_offset: scan_offset,
                        tail_hash: hash_accumulator,
                        highest_view,
                        base_index,
                        base_prev_hash,
                    });
                }
            }

            self.file.seek(SeekFrom::Start(scan_offset))?;
            let bytes_read = self.file.read(&mut header_buf)?;

            // FORBIDDEN STATE F1: Sentinel without corresponding durable entry.
            if bytes_read >= SENTINEL_SIZE && is_sentinel_magic(&header_buf[0..4]) {
                let mut sentinel_buf = [0u8; SENTINEL_SIZE];
                sentinel_buf.copy_from_slice(&header_buf[0..SENTINEL_SIZE]);

                if expected_index > base_index && verify_sentinel(&sentinel_buf, expected_index - 1)
                {
                    return Ok(RecoveryOutcome::Clean {
                        last_index: expected_index - 1,
                        next_offset: scan_offset,
                        tail_hash: hash_accumulator,
                        highest_view,
                        base_index,
                        base_prev_hash,
                    });
                } else if expected_index == base_index {
                    return Ok(RecoveryOutcome::CleanEmpty {
                        base_index,
                        base_prev_hash,
                    });
                }
                // F1 violation: sentinel without valid corresponding entry.
                panic!(
                    "FATAL: Forbidden state F1 - Sentinel at offset {} claims index that doesn't match expected {}. \
                     Sentinel exists without valid corresponding entry.",
                    scan_offset, expected_index.saturating_sub(1)
                );
            }

            if bytes_read < HEADER_SIZE {
                return self.handle_potential_torn_write(
                    scan_offset,
                    expected_index,
                    hash_accumulator,
                    highest_view,
                    base_index,
                    base_prev_hash,
                    RecoverableError::IncompleteRead {
                        offset: scan_offset,
                        expected: HEADER_SIZE,
                        got: bytes_read,
                    },
                );
            }

            if header_buf.iter().all(|&b| b == 0) {
                return self.verify_zero_tail(
                    scan_offset,
                    expected_index,
                    hash_accumulator,
                    highest_view,
                    base_index,
                    base_prev_hash,
                );
            }

            let header = LogHeader::from_bytes(&header_buf);

            if !header.verify_checksum() {
                return self.handle_potential_torn_write(
                    scan_offset,
                    expected_index,
                    hash_accumulator,
                    highest_view,
                    base_index,
                    base_prev_hash,
                    RecoverableError::HeaderCrcMismatch {
                        offset: scan_offset,
                    },
                );
            }

            // FATAL: Monotonicity violation
            if header.index != expected_index {
                panic!(
                    "{}",
                    FatalError::MonotonicityViolation {
                        expected: expected_index,
                        found: header.index,
                    }
                );
            }

            // FATAL: Broken hash chain
            if header.prev_hash != hash_accumulator {
                panic!(
                    "{}",
                    FatalError::BrokenChain {
                        index: header.index,
                        expected: hash_accumulator,
                        found: header.prev_hash,
                    }
                );
            }

            // FATAL: View regression
            if header.view_id < highest_view {
                panic!(
                    "{}",
                    FatalError::ViewRegression {
                        previous_view: highest_view,
                        current_view: header.view_id,
                    }
                );
            }
            highest_view = header.view_id;

            if header.payload_size > MAX_PAYLOAD_SIZE {
                panic!(
                    "{}",
                    FatalError::PayloadTooLarge {
                        size: header.payload_size,
                        max: MAX_PAYLOAD_SIZE,
                    }
                );
            }

            let payload_offset = scan_offset + HEADER_SIZE as u64;
            let mut payload = vec![0u8; header.payload_size as usize];

            self.file.seek(SeekFrom::Start(payload_offset))?;
            let payload_read = self.file.read(&mut payload)?;

            if payload_read < header.payload_size as usize {
                return self.handle_potential_torn_write(
                    scan_offset,
                    expected_index,
                    hash_accumulator,
                    highest_view,
                    base_index,
                    base_prev_hash,
                    RecoverableError::IncompleteRead {
                        offset: payload_offset,
                        expected: header.payload_size as usize,
                        got: payload_read,
                    },
                );
            }

            let computed_hash = compute_payload_hash(&payload);
            if computed_hash != header.payload_hash {
                return self.handle_potential_torn_write(
                    scan_offset,
                    expected_index,
                    hash_accumulator,
                    highest_view,
                    base_index,
                    base_prev_hash,
                    RecoverableError::PayloadHashMismatch {
                        offset: scan_offset,
                        index: header.index,
                    },
                );
            }

            _last_valid_offset = scan_offset;
            hash_accumulator = compute_chain_hash(&header, &payload);
            expected_index += 1;
            scan_offset += frame_size(header.payload_size) as u64;
        }
    }

    /// Zero followed by non-zero = FATAL (zero hole).
    fn verify_zero_tail(
        &mut self,
        zero_offset: u64,
        expected_index: u64,
        hash_accumulator: [u8; 16],
        highest_view: u64,
        base_index: u64,
        base_prev_hash: [u8; 16],
    ) -> Result<RecoveryOutcome, FatalError> {
        const CHUNK_SIZE: usize = 4096;
        let mut buf = [0u8; CHUNK_SIZE];
        let mut offset = zero_offset + HEADER_SIZE as u64;

        while offset < self.file_size {
            self.file.seek(SeekFrom::Start(offset))?;
            let to_read = std::cmp::min(CHUNK_SIZE, (self.file_size - offset) as usize);
            let bytes_read = self.file.read(&mut buf[..to_read])?;

            if bytes_read == 0 {
                break;
            }

            if let Some(pos) = buf[..bytes_read].iter().position(|&b| b != 0) {
                panic!(
                    "{}",
                    FatalError::ZeroHole {
                        zero_offset,
                        data_offset: offset + pos as u64,
                    }
                );
            }

            offset += bytes_read as u64;
        }

        if expected_index == base_index {
            Ok(RecoveryOutcome::CleanEmpty {
                base_index,
                base_prev_hash,
            })
        } else {
            Ok(RecoveryOutcome::Clean {
                last_index: expected_index - 1,
                next_offset: zero_offset,
                tail_hash: hash_accumulator,
                highest_view,
                base_index,
                base_prev_hash,
            })
        }
    }

    /// Torn write recovery: truncate if at tail with no valid headers ahead.
    fn handle_potential_torn_write(
        &mut self,
        failure_offset: u64,
        expected_index: u64,
        hash_accumulator: [u8; 16],
        highest_view: u64,
        base_index: u64,
        base_prev_hash: [u8; 16],
        _error: RecoverableError,
    ) -> Result<RecoveryOutcome, FatalError> {
        // Mid-log corruption if valid header exists ahead.
        if self.has_valid_header_ahead(failure_offset, expected_index)? {
            panic!(
                "{}",
                FatalError::MidLogCorruption {
                    offset: failure_offset,
                    index: expected_index.saturating_sub(1),
                }
            );
        }

        self.truncate_to(failure_offset)?;

        eprintln!(
            "Recovered torn write at index {}. Log truncated to size {}.",
            expected_index, failure_offset
        );

        if expected_index == base_index {
            Ok(RecoveryOutcome::Truncated {
                last_valid_index: base_index,
                truncated_at: failure_offset,
                new_offset: if base_index > 0 {
                    LOG_METADATA_SIZE as u64
                } else {
                    0
                },
                tail_hash: base_prev_hash,
                highest_view: 0,
                base_index,
                base_prev_hash,
            })
        } else {
            Ok(RecoveryOutcome::Truncated {
                last_valid_index: expected_index - 1,
                truncated_at: failure_offset,
                new_offset: failure_offset,
                tail_hash: hash_accumulator,
                highest_view,
                base_index,
                base_prev_hash,
            })
        }
    }

    fn has_valid_header_ahead(
        &mut self,
        start_offset: u64,
        expected_min_index: u64,
    ) -> io::Result<bool> {
        const SCAN_AHEAD_BYTES: u64 = 10_000 * 64;

        let end_offset = std::cmp::min(self.file_size, start_offset + SCAN_AHEAD_BYTES);
        let mut header_buf = [0u8; HEADER_SIZE];
        let mut offset = start_offset + 8; // Start at next aligned position

        while offset + HEADER_SIZE as u64 <= end_offset {
            self.file.seek(SeekFrom::Start(offset))?;
            let bytes_read = self.file.read(&mut header_buf)?;

            if bytes_read < HEADER_SIZE {
                break;
            }

            if header_buf.iter().all(|&b| b == 0) {
                offset += 8;
                continue;
            }

            if self.is_valid_header_candidate(&header_buf, expected_min_index) {
                return Ok(true);
            }

            offset += 8;
        }

        Ok(false)
    }

    fn is_valid_header_candidate(&self, buf: &[u8; HEADER_SIZE], expected_min_index: u64) -> bool {
        let header = LogHeader::from_bytes(buf);
        if !header.verify_checksum() {
            return false;
        }
        if header.payload_size > MAX_PAYLOAD_SIZE {
            return false;
        }
        if header.schema_version == 0 || header.schema_version > 100 {
            return false;
        }
        if header.index < expected_min_index {
            return false;
        }
        true
    }

    fn truncate_to(&self, len: u64) -> io::Result<()> {
        let result = unsafe { libc::ftruncate(self.file.as_raw_fd(), len as libc::off_t) };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        let sync_result = unsafe { libc::fdatasync(self.file.as_raw_fd()) };

        if sync_result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::log::LogWriter;
    use std::fs;

    #[test]
    fn test_recovery_empty_file() {
        let path = Path::new("/tmp/chr_recovery_empty.log");
        let _ = fs::remove_file(path);

        // Create empty file
        File::create(path).unwrap();

        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let outcome = recovery.scan().unwrap();

        match outcome {
            RecoveryOutcome::CleanEmpty { base_index: 0, .. } => {}
            _ => panic!("Expected CleanEmpty, got {:?}", outcome),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_recovery_with_entries() {
        let path = Path::new("/tmp/chr_recovery_entries.log");
        let _ = fs::remove_file(path);

        // Write some entries
        {
            let mut writer = LogWriter::create(path, 1).unwrap();
            for i in 0..5 {
                let payload = format!("entry {}", i);
                writer
                    .append(payload.as_bytes(), 0, 0, 1_000_000_000 + i * 1_000_000_000)
                    .unwrap();
            }
        }

        // Recover
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let outcome = recovery.scan().unwrap();

        match outcome {
            RecoveryOutcome::Clean { last_index, .. } => {
                assert_eq!(last_index, 4);
            }
            _ => panic!("Expected Clean, got {:?}", outcome),
        }

        let _ = fs::remove_file(path);
    }
}
