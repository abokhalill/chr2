use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

pub const MANIFEST_MAGIC: [u8; 4] = [0x50, 0x4D, 0x41, 0x4E];
pub const MANIFEST_VERSION: u32 = 1;
pub const MANIFEST_SIZE: usize = 64;
pub const NO_VOTE_NODE: u32 = u32::MAX;

/// Consensus fencing state. Must be durable before protocol actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableState {
    pub highest_view: u64,
    pub voted_for_view: u64,
    pub voted_for_node: u32,
    pub last_log_index: u64,
    pub last_log_view: u64,
}

impl Default for DurableState {
    fn default() -> Self {
        DurableState {
            highest_view: 0,
            voted_for_view: 0,
            voted_for_node: NO_VOTE_NODE,
            last_log_index: 0,
            last_log_view: 0,
        }
    }
}

impl DurableState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_bytes(&self) -> [u8; MANIFEST_SIZE] {
        let mut bytes = [0u8; MANIFEST_SIZE];

        // Magic (0..4)
        bytes[0..4].copy_from_slice(&MANIFEST_MAGIC);

        // Version (4..8)
        bytes[4..8].copy_from_slice(&MANIFEST_VERSION.to_le_bytes());

        // highest_view (8..16)
        bytes[8..16].copy_from_slice(&self.highest_view.to_le_bytes());

        // voted_for_view (16..24)
        bytes[16..24].copy_from_slice(&self.voted_for_view.to_le_bytes());

        // voted_for_node (24..28)
        bytes[24..28].copy_from_slice(&self.voted_for_node.to_le_bytes());

        // you get the picture

        // last_log_index (32..40)
        bytes[32..40].copy_from_slice(&self.last_log_index.to_le_bytes());

        // last_log_view (40..48)
        bytes[40..48].copy_from_slice(&self.last_log_view.to_le_bytes());

        // Compute checksum of bytes [0..48]
        let checksum = crc32c::crc32c(&bytes[0..48]);
        bytes[48..52].copy_from_slice(&checksum.to_le_bytes());

        // reserved (52..64) - zeros

        bytes
    }

    pub fn from_bytes(bytes: &[u8; MANIFEST_SIZE]) -> Option<Self> {
        // Verify magic
        if bytes[0..4] != MANIFEST_MAGIC {
            return None;
        }

        // Verify version
        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != MANIFEST_VERSION {
            return None;
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([bytes[48], bytes[49], bytes[50], bytes[51]]);
        let computed_checksum = crc32c::crc32c(&bytes[0..48]);
        if stored_checksum != computed_checksum {
            return None;
        }

        // Parse fields
        let highest_view = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        let voted_for_view = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);

        let voted_for_node = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);

        let last_log_index = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]);

        let last_log_view = u64::from_le_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
        ]);

        Some(DurableState {
            highest_view,
            voted_for_view,
            voted_for_node,
            last_log_index,
            last_log_view,
        })
    }

    #[inline]
    pub fn check_view_fence(&self, msg_view: u64) -> Result<(), ViewFenceError> {
        if msg_view < self.highest_view {
            Err(ViewFenceError::StaleView {
                msg_view,
                highest_view: self.highest_view,
            })
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn check_vote_fence(&self, view: u64, candidate: u32) -> Result<(), VoteFenceError> {
        if self.voted_for_view == view && self.voted_for_node != NO_VOTE_NODE {
            if self.voted_for_node != candidate {
                return Err(VoteFenceError::AlreadyVoted {
                    view,
                    voted_for: self.voted_for_node,
                    requested: candidate,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewFenceError {
    StaleView { msg_view: u64, highest_view: u64 },
}

impl std::fmt::Display for ViewFenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewFenceError::StaleView {
                msg_view,
                highest_view,
            } => {
                write!(
                    f,
                    "Stale view: message view {} < highest seen view {}",
                    msg_view, highest_view
                )
            }
        }
    }
}

impl std::error::Error for ViewFenceError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteFenceError {
    AlreadyVoted {
        view: u64,
        voted_for: u32,
        requested: u32,
    },
}

impl std::fmt::Display for VoteFenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VoteFenceError::AlreadyVoted {
                view,
                voted_for,
                requested,
            } => {
                write!(
                    f,
                    "Already voted in view {}: voted for node {}, cannot vote for node {}",
                    view, voted_for, requested
                )
            }
        }
    }
}

impl std::error::Error for VoteFenceError {}

/// Crash-safe via write-tmp-fsync-rename-fsync-dir.
#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    state: DurableState,
}

impl Manifest {
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.exists() {
            // Load existing manifest
            let mut file = File::open(path)?;
            let mut bytes = [0u8; MANIFEST_SIZE];
            file.read_exact(&mut bytes)?;

            let state = DurableState::from_bytes(&bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Manifest file corrupted: invalid magic, version, or checksum",
                )
            })?;

            Ok(Manifest {
                path: path.to_path_buf(),
                state,
            })
        } else {
            // Create new manifest
            let manifest = Manifest {
                path: path.to_path_buf(),
                state: DurableState::new(),
            };
            manifest.persist()?;
            Ok(manifest)
        }
    }

    #[inline]
    pub fn state(&self) -> &DurableState {
        &self.state
    }

    #[inline]
    pub fn highest_view(&self) -> u64 {
        self.state.highest_view
    }

    #[inline]
    pub fn check_view_fence(&self, msg_view: u64) -> Result<(), ViewFenceError> {
        self.state.check_view_fence(msg_view)
    }

    #[inline]
    pub fn check_vote_fence(&self, view: u64, candidate: u32) -> Result<(), VoteFenceError> {
        self.state.check_vote_fence(view, candidate)
    }

    pub fn advance_view(&mut self, new_view: u64) -> io::Result<bool> {
        if new_view <= self.state.highest_view {
            return Ok(false);
        }

        self.state.highest_view = new_view;
        // Clear vote when advancing to new view
        if new_view > self.state.voted_for_view {
            self.state.voted_for_view = 0;
            self.state.voted_for_node = NO_VOTE_NODE;
        }
        self.persist()?;
        Ok(true)
    }

    pub fn record_vote(&mut self, view: u64, node_id: u32) -> io::Result<()> {
        // Check fence first
        self.state
            .check_vote_fence(view, node_id)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        // Update state
        self.state.voted_for_view = view;
        self.state.voted_for_node = node_id;

        // Also advance highest_view if needed
        if view > self.state.highest_view {
            self.state.highest_view = view;
        }

        self.persist()
    }

    pub fn update_log_position(&mut self, last_index: u64, last_view: u64) -> io::Result<()> {
        self.state.last_log_index = last_index;
        self.state.last_log_view = last_view;
        self.persist()
    }

    fn persist(&self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("chr.tmp");

        {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;

            tmp_file.write_all(&self.state.to_bytes())?;

            let result = unsafe { libc::fdatasync(tmp_file.as_raw_fd()) };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        fs::rename(&tmp_path, &self.path)?;

        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                unsafe { libc::fsync(dir.as_raw_fd()) };
            }
        }

        Ok(())
    }
}

pub fn validate_manifest_against_log(
    manifest: &DurableState,
    log_highest_view: u64,
    log_last_index: u64,
) -> Result<(), String> {
    // Check 1: Manifest view must not be behind log
    // This would indicate manifest corruption or incomplete write
    if manifest.highest_view < log_highest_view {
        return Err(format!(
            "FATAL: Manifest highest_view ({}) < log highest_view ({}). \
             Manifest may be corrupted or from an older state.",
            manifest.highest_view, log_highest_view
        ));
    }

    // Check 2: If manifest has log position, verify it's not impossibly ahead
    // (This is a sanity check - manifest.last_log_index should be <= actual log length)
    if manifest.last_log_index > log_last_index + 1 {
        // Allow +1 for the case where manifest was written mid-append
        return Err(format!(
            "FATAL: Manifest last_log_index ({}) > log last_index ({}) + 1. \
             Manifest may be corrupted.",
            manifest.last_log_index, log_last_index
        ));
    }

    Ok(())
}

#[derive(Debug)]
pub struct RecoveryWithManifest {
    pub manifest: Manifest,
    pub was_created: bool,
    pub highest_view: u64,
}

impl Manifest {
    pub fn open_for_recovery(
        path: &Path,
        log_highest_view: u64,
        log_last_index: u64,
    ) -> io::Result<RecoveryWithManifest> {
        let was_created = !path.exists();
        let mut manifest = Self::open(path)?;

        // Validate manifest against log state
        validate_manifest_against_log(manifest.state(), log_highest_view, log_last_index)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // If log has a higher view than manifest (shouldn't happen normally,
        // but could if manifest write failed), update manifest
        if log_highest_view > manifest.highest_view() {
            manifest.advance_view(log_highest_view)?;
        }

        // Update log position in manifest
        let log_view = manifest.state().last_log_view.max(log_highest_view);
        manifest.update_log_position(log_last_index, log_view)?;

        let highest_view = manifest.highest_view();

        Ok(RecoveryWithManifest {
            manifest,
            was_created,
            highest_view,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_durable_state_roundtrip() {
        let state = DurableState {
            highest_view: 42,
            voted_for_view: 42,
            voted_for_node: 1,
            last_log_index: 1000,
            last_log_view: 41,
        };

        let bytes = state.to_bytes();
        let recovered = DurableState::from_bytes(&bytes).unwrap();

        assert_eq!(state, recovered);
    }

    #[test]
    fn test_durable_state_invalid_magic() {
        let mut bytes = [0u8; MANIFEST_SIZE];
        bytes[0..4].copy_from_slice(b"XXXX"); // Wrong magic

        assert!(DurableState::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_durable_state_invalid_checksum() {
        let state = DurableState::new();
        let mut bytes = state.to_bytes();
        bytes[48] ^= 0xFF; // Corrupt checksum

        assert!(DurableState::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_view_fence_check() {
        let state = DurableState {
            highest_view: 10,
            ..Default::default()
        };

        // Should accept view >= highest_view
        assert!(state.check_view_fence(10).is_ok());
        assert!(state.check_view_fence(11).is_ok());
        assert!(state.check_view_fence(100).is_ok());

        // Should reject view < highest_view
        assert!(state.check_view_fence(9).is_err());
        assert!(state.check_view_fence(0).is_err());
    }

    #[test]
    fn test_vote_fence_check() {
        let state = DurableState {
            highest_view: 10,
            voted_for_view: 10,
            voted_for_node: 1,
            ..Default::default()
        };

        // Should accept same vote
        assert!(state.check_vote_fence(10, 1).is_ok());

        // Should reject different node in same view
        assert!(state.check_vote_fence(10, 2).is_err());

        // Should accept vote in different view
        assert!(state.check_vote_fence(11, 2).is_ok());
        assert!(state.check_vote_fence(9, 2).is_ok());
    }

    #[test]
    fn test_manifest_create_and_load() {
        let path = Path::new("/tmp/chr_manifest_test.chr");
        let _ = fs::remove_file(path);

        // Create new manifest
        {
            let mut manifest = Manifest::open(path).unwrap();
            assert_eq!(manifest.highest_view(), 0);

            manifest.advance_view(5).unwrap();
            assert_eq!(manifest.highest_view(), 5);
        }

        // Reload and verify persistence
        {
            let manifest = Manifest::open(path).unwrap();
            assert_eq!(manifest.highest_view(), 5);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_manifest_vote_persistence() {
        let path = Path::new("/tmp/chr_manifest_vote_test.chr");
        let _ = fs::remove_file(path);

        // Create and vote
        {
            let mut manifest = Manifest::open(path).unwrap();
            manifest.record_vote(10, 2).unwrap();
        }

        // Reload and verify
        {
            let manifest = Manifest::open(path).unwrap();
            let state = manifest.state();
            assert_eq!(state.voted_for_view, 10);
            assert_eq!(state.voted_for_node, 2);
            assert_eq!(state.highest_view, 10);
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_manifest_rejects_double_vote() {
        let path = Path::new("/tmp/chr_manifest_double_vote.chr");
        let _ = fs::remove_file(path);

        let mut manifest = Manifest::open(path).unwrap();

        // First vote succeeds
        manifest.record_vote(10, 1).unwrap();

        // Same vote succeeds (idempotent)
        manifest.record_vote(10, 1).unwrap();

        // Different node in same view fails
        let result = manifest.record_vote(10, 2);
        assert!(result.is_err());

        // Vote in new view succeeds
        manifest.record_vote(11, 2).unwrap();

        let _ = fs::remove_file(path);
    }
}
