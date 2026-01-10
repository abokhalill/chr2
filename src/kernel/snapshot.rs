use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub const SNAPSHOT_MAGIC: [u8; 4] = [0x53, 0x4E, 0x41, 0x50]; // "SNAP"
pub const SNAPSHOT_VERSION: u16 = 1;
pub const SNAPSHOT_HEADER_SIZE: usize = 64;

/// 64-byte header + variable state. chain_hash bridges hash chain across compaction.
#[derive(Clone, Debug)]
pub struct SnapshotManifest {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub chain_hash: [u8; 16],
    pub state: Vec<u8>,
    pub side_effects_dropped: bool,
}

#[derive(Debug)]
pub enum SnapshotError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    HeaderChecksumMismatch { expected: u32, actual: u32 },
    StateChecksumMismatch { expected: u32, actual: u32 },
    FileTooSmall,
    StateSizeMismatch { expected: u64, actual: usize },
    SerializeError(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Io(e) => write!(f, "IO error: {}", e),
            SnapshotError::InvalidMagic => write!(f, "Invalid snapshot magic"),
            SnapshotError::UnsupportedVersion(v) => write!(f, "Unsupported version: {}", v),
            SnapshotError::HeaderChecksumMismatch { expected, actual } => {
                write!(f, "Header checksum mismatch: expected {}, got {}", expected, actual)
            }
            SnapshotError::StateChecksumMismatch { expected, actual } => {
                write!(f, "State checksum mismatch: expected {}, got {}", expected, actual)
            }
            SnapshotError::FileTooSmall => write!(f, "File too small for snapshot header"),
            SnapshotError::StateSizeMismatch { expected, actual } => {
                write!(f, "State size mismatch: expected {}, got {}", expected, actual)
            }
            SnapshotError::SerializeError(msg) => write!(f, "Serialize error: {}", msg),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<std::io::Error> for SnapshotError {
    fn from(e: std::io::Error) -> Self {
        SnapshotError::Io(e)
    }
}

impl SnapshotManifest {
    pub fn new(
        last_included_index: u64,
        last_included_term: u64,
        chain_hash: [u8; 16],
        state: Vec<u8>,
    ) -> Self {
        SnapshotManifest {
            last_included_index,
            last_included_term,
            chain_hash,
            state,
            side_effects_dropped: true,
        }
    }

    fn serialize_header(&self) -> [u8; SNAPSHOT_HEADER_SIZE] {
        let mut header = [0u8; SNAPSHOT_HEADER_SIZE];

        header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
        header[4..6].copy_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&0u16.to_le_bytes());
        header[8..16].copy_from_slice(&self.last_included_index.to_le_bytes());
        header[16..24].copy_from_slice(&self.last_included_term.to_le_bytes());
        header[24..40].copy_from_slice(&self.chain_hash);
        header[40..48].copy_from_slice(&(self.state.len() as u64).to_le_bytes());
        let state_checksum = crc32c::crc32c(&self.state);
        header[48..52].copy_from_slice(&state_checksum.to_le_bytes());
        header[56] = if self.side_effects_dropped { 1 } else { 0 };
        let header_checksum = crc32c::crc32c(&header[0..52]);
        header[52..56].copy_from_slice(&header_checksum.to_le_bytes());

        header
    }

    /// Atomic: write temp → fsync → rename. No partial snapshots on disk.
    pub fn save_to_file(&self, path: &Path) -> Result<(), SnapshotError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = path.with_extension("snap.tmp");

        {
            let mut file = File::create(&temp_path)?;
            let header = self.serialize_header();
            file.write_all(&header)?;
            file.write_all(&self.state)?;
            file.sync_all()?;
        }

        fs::rename(&temp_path, path)?;

        // fsync directory to ensure rename survives power loss.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let result = unsafe { libc::fsync(dir.as_raw_fd()) };
                if result < 0 {
                    return Err(SnapshotError::Io(std::io::Error::last_os_error()));
                }
            }
        }

        Ok(())
    }

    pub fn load_from_file(path: &Path) -> Result<Self, SnapshotError> {
        let mut file = File::open(path)?;
        let mut header = [0u8; SNAPSHOT_HEADER_SIZE];
        let bytes_read = file.read(&mut header)?;

        if bytes_read < SNAPSHOT_HEADER_SIZE {
            return Err(SnapshotError::FileTooSmall);
        }

        if header[0..4] != SNAPSHOT_MAGIC {
            return Err(SnapshotError::InvalidMagic);
        }

        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }

        let stored_header_checksum = u32::from_le_bytes([header[52], header[53], header[54], header[55]]);
        let computed_header_checksum = crc32c::crc32c(&header[0..52]);
        if stored_header_checksum != computed_header_checksum {
            return Err(SnapshotError::HeaderChecksumMismatch {
                expected: stored_header_checksum,
                actual: computed_header_checksum,
            });
        }

        let last_included_index = u64::from_le_bytes([
            header[8], header[9], header[10], header[11],
            header[12], header[13], header[14], header[15],
        ]);
        let last_included_term = u64::from_le_bytes([
            header[16], header[17], header[18], header[19],
            header[20], header[21], header[22], header[23],
        ]);
        let mut chain_hash = [0u8; 16];
        chain_hash.copy_from_slice(&header[24..40]);
        let state_size = u64::from_le_bytes([
            header[40], header[41], header[42], header[43],
            header[44], header[45], header[46], header[47],
        ]);
        let stored_state_checksum = u32::from_le_bytes([header[48], header[49], header[50], header[51]]);
        let side_effects_dropped = header[56] != 0;

        let mut state = vec![0u8; state_size as usize];
        let state_bytes_read = file.read(&mut state)?;

        if state_bytes_read != state_size as usize {
            return Err(SnapshotError::StateSizeMismatch {
                expected: state_size,
                actual: state_bytes_read,
            });
        }

        let computed_state_checksum = crc32c::crc32c(&state);
        if stored_state_checksum != computed_state_checksum {
            return Err(SnapshotError::StateChecksumMismatch {
                expected: stored_state_checksum,
                actual: computed_state_checksum,
            });
        }

        Ok(SnapshotManifest {
            last_included_index,
            last_included_term,
            chain_hash,
            state,
            side_effects_dropped,
        })
    }

    pub fn filename_for_index(index: u64) -> String {
        format!("snapshot_{:020}.snap", index)
    }

    pub fn index_from_filename(filename: &str) -> Option<u64> {
        if !filename.starts_with("snapshot_") || !filename.ends_with(".snap") { return None; }
        if filename.len() != 34 { return None; }
        filename[9..29].parse().ok()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_snapshot_path() -> PathBuf {
        PathBuf::from("/tmp/chr_snapshot_test.snap")
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let path = temp_snapshot_path();
        let _ = fs::remove_file(&path);

        let manifest = SnapshotManifest::new(
            100,
            5,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            b"test state data".to_vec(),
        );

        // Save
        manifest.save_to_file(&path).unwrap();

        // Load
        let loaded = SnapshotManifest::load_from_file(&path).unwrap();

        assert_eq!(loaded.last_included_index, 100);
        assert_eq!(loaded.last_included_term, 5);
        assert_eq!(loaded.chain_hash, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(loaded.state, b"test state data");
        assert!(loaded.side_effects_dropped);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_invalid_magic() {
        let path = temp_snapshot_path();
        let _ = fs::remove_file(&path);

        // Write garbage
        let mut file = File::create(&path).unwrap();
        file.write_all(&[0u8; 64]).unwrap();
        drop(file);

        let result = SnapshotManifest::load_from_file(&path);
        assert!(matches!(result, Err(SnapshotError::InvalidMagic)));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_corrupted_header() {
        let path = PathBuf::from("/tmp/chr_snapshot_test_corrupted_header.snap");
        let _ = fs::remove_file(&path);

        let manifest = SnapshotManifest::new(100, 5, [0u8; 16], b"state".to_vec());
        manifest.save_to_file(&path).unwrap();

        // Corrupt the header
        let mut data = fs::read(&path).unwrap();
        data[10] ^= 0xFF; // Flip some bits in last_included_index
        fs::write(&path, &data).unwrap();

        let result = SnapshotManifest::load_from_file(&path);
        assert!(matches!(result, Err(SnapshotError::HeaderChecksumMismatch { .. })));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_corrupted_state() {
        let path = PathBuf::from("/tmp/chr_snapshot_test_corrupted_state.snap");
        let _ = fs::remove_file(&path);

        let manifest = SnapshotManifest::new(100, 5, [0u8; 16], b"state data here".to_vec());
        manifest.save_to_file(&path).unwrap();

        // Corrupt the state (after header)
        let mut data = fs::read(&path).unwrap();
        data[70] ^= 0xFF; // Flip some bits in state payload
        fs::write(&path, &data).unwrap();

        let result = SnapshotManifest::load_from_file(&path);
        assert!(matches!(result, Err(SnapshotError::StateChecksumMismatch { .. })));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_filename_generation() {
        assert_eq!(
            SnapshotManifest::filename_for_index(0),
            "snapshot_00000000000000000000.snap"
        );
        assert_eq!(
            SnapshotManifest::filename_for_index(12345),
            "snapshot_00000000000000012345.snap"
        );
        assert_eq!(
            SnapshotManifest::filename_for_index(u64::MAX),
            "snapshot_18446744073709551615.snap"
        );
    }

    #[test]
    fn test_snapshot_filename_parsing() {
        assert_eq!(
            SnapshotManifest::index_from_filename("snapshot_00000000000000000000.snap"),
            Some(0)
        );
        assert_eq!(
            SnapshotManifest::index_from_filename("snapshot_00000000000000012345.snap"),
            Some(12345)
        );
        assert_eq!(
            SnapshotManifest::index_from_filename("invalid.snap"),
            None
        );
        assert_eq!(
            SnapshotManifest::index_from_filename("snapshot_abc.snap"),
            None
        );
    }

    #[test]
    fn test_snapshot_atomic_write() {
        let path = temp_snapshot_path();
        let temp_path = path.with_extension("snap.tmp");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&temp_path);

        let manifest = SnapshotManifest::new(100, 5, [0u8; 16], b"state".to_vec());
        manifest.save_to_file(&path).unwrap();

        // Temp file should not exist after successful save
        assert!(!temp_path.exists());
        // Final file should exist
        assert!(path.exists());

        let _ = fs::remove_file(&path);
    }
}
