use std::path::{Path, PathBuf};

use crate::engine::reader::{LogReader, ReadError};
use crate::kernel::snapshot::SnapshotManifest;
use crate::kernel::traits::{ChrApplication, SnapshotStream};

use super::error::{ExecutorStatus, FatalError, StepResult};
use super::Executor;
use super::DEFAULT_SNAPSHOT_THRESHOLD;

impl<A: ChrApplication> Executor<A> {
    /// Recover executor state from snapshot + log suffix.
    ///
    /// # Algorithm
    ///
    /// Step A: Restore Base State
    /// - Find latest valid snapshot (descending index, CRC validation)
    /// - If no snapshot: use genesis state, next_index = 0
    /// - If snapshot: restore state, next_index = snapshot.last_included_index + 1
    ///
    /// Step B: Log Authority Reconstruction
    /// - Determine committed_index from log
    /// - Assert committed_index >= snapshot.last_included_index
    ///
    /// Step C: Chain Bridge Validation
    /// - Read entry at next_index
    /// - Assert entry.prev_hash == snapshot.chain_hash
    ///
    /// Step D: Catch-Up Replay
    /// - Replay entries [next_index ..= committed_index]
    /// - Side effects are collected but NOT executed
    pub fn recover(
        mut reader: LogReader,
        app: A,
        snapshot_dir: PathBuf,
    ) -> Result<Self, FatalError> {
        let snapshot = Self::find_latest_valid_snapshot(&snapshot_dir)?;

        let (state, next_index, last_snapshot_index, expected_chain_hash) = match snapshot {
            None => {
                let state = app.genesis();
                (state, 0u64, None, None)
            }
            Some(manifest) => {
                let stream = SnapshotStream {
                    schema_version: 1,
                    data: manifest.state.clone(),
                };
                let state = app.restore(stream).map_err(|e| {
                    FatalError::RestoreError(format!("Failed to restore state: {}", e))
                })?;
                let next_idx = manifest.last_included_index + 1;
                let snap_idx = manifest.last_included_index;
                let chain_hash = manifest.chain_hash;
                (state, next_idx, Some(snap_idx), Some(chain_hash))
            }
        };

        let committed_index = reader.committed_index();

        if let Some(snap_idx) = last_snapshot_index {
            match committed_index {
                Some(c) if c < snap_idx => {
                    return Err(FatalError::LogBehindSnapshot {
                        committed_index: Some(c),
                        snapshot_index: snap_idx,
                    });
                }
                None => {
                    return Err(FatalError::LogBehindSnapshot {
                        committed_index: None,
                        snapshot_index: snap_idx,
                    });
                }
                _ => {}
            }
        }

        if let (Some(snap_idx), Some(expected_hash)) = (last_snapshot_index, expected_chain_hash) {
            let bridge_index = snap_idx + 1;

            match reader.read(bridge_index) {
                Ok(_entry) => {
                    let actual_prev_hash = Self::get_entry_prev_hash(&mut reader, bridge_index)?;

                    if actual_prev_hash != expected_hash {
                        return Err(FatalError::ChainBridgeMismatch {
                            entry_index: bridge_index,
                            expected_hash,
                            actual_hash: actual_prev_hash,
                        });
                    }
                }
                Err(ReadError::IndexNotCommitted { .. }) | Err(ReadError::IndexNotFound { .. }) => {
                    if let Some(c) = committed_index {
                        if c > snap_idx {
                            return Err(FatalError::LogGap {
                                snapshot_index: snap_idx,
                                missing_index: bridge_index,
                                found_index: c,
                            });
                        }
                    }
                }
                Err(e) => {
                    return Err(FatalError::ReadError(format!(
                        "Failed to read bridge entry at index {}: {}",
                        bridge_index, e
                    )));
                }
            }
        }

        let mut executor = Executor {
            reader,
            app,
            state,
            next_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        };

        loop {
            match executor.step()? {
                StepResult::Idle => break,
                StepResult::Applied { .. } => {}
                StepResult::Rejected { .. } => {}
            }
        }

        Ok(executor)
    }

    pub(super) fn find_latest_valid_snapshot(
        snapshot_dir: &Path,
    ) -> Result<Option<SnapshotManifest>, FatalError> {
        use std::fs;

        if !snapshot_dir.exists() {
            return Ok(None);
        }

        let mut snapshot_indices: Vec<u64> = Vec::new();

        let entries = fs::read_dir(snapshot_dir).map_err(|e| {
            FatalError::SnapshotError(format!("Failed to read snapshot directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                FatalError::SnapshotError(format!("Failed to read directory entry: {}", e))
            })?;

            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(index) = SnapshotManifest::index_from_filename(&filename_str) {
                snapshot_indices.push(index);
            }
        }

        snapshot_indices.sort_by(|a, b| b.cmp(a));

        for index in snapshot_indices {
            let filename = SnapshotManifest::filename_for_index(index);
            let path = snapshot_dir.join(&filename);

            match SnapshotManifest::load_from_file(&path) {
                Ok(manifest) => {
                    return Ok(Some(manifest));
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Snapshot at index {} failed validation: {}",
                        index, e
                    );
                }
            }
        }

        Ok(None)
    }

    pub(super) fn get_entry_prev_hash(
        reader: &mut LogReader,
        index: u64,
    ) -> Result<[u8; 16], FatalError> {
        let entry = reader.read(index).map_err(|e| {
            FatalError::ReadError(format!(
                "Failed to read entry at index {} to get prev_hash: {}",
                index, e
            ))
        })?;

        Ok(entry.prev_hash)
    }
}
