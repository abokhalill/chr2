use std::path::Path;

use crate::kernel::snapshot::SnapshotManifest;
use crate::kernel::traits::ChrApplication;

use super::error::{ExecutorStatus, FatalError};
use super::Executor;

impl<A: ChrApplication> Executor<A> {
    pub fn should_snapshot(&self) -> bool {
        if self.next_index == 0 {
            return false;
        }

        let last_applied = self.next_index - 1;
        let entries_since_snapshot = match self.last_snapshot_index {
            Some(idx) => last_applied.saturating_sub(idx),
            None => last_applied + 1, // All entries since genesis
        };

        entries_since_snapshot >= self.snapshot_threshold
    }

    pub fn last_applied_index(&self) -> Option<u64> {
        if self.next_index == 0 {
            None
        } else {
            Some(self.next_index - 1)
        }
    }

    pub fn take_snapshot(&mut self) -> Result<u64, FatalError> {
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }

        let snapshot_index = match self.last_applied_index() {
            Some(idx) => idx,
            None => return Err(FatalError::SnapshotError("Cannot take snapshot: no entries applied yet".to_string())),
        };

        let committed = self.reader.committed_index();
        match committed {
            Some(c) if snapshot_index > c => {
                return Err(FatalError::SnapshotError(format!("Invariant violation: snapshot_index {} > committed_index {}", snapshot_index, c)));
            }
            None => return Err(FatalError::SnapshotError("Cannot take snapshot: no entries committed".to_string())),
            _ => {}
        }

        let state_clone = self.state.clone();
        let chain_hash = self.reader.get_chain_hash(snapshot_index).map_err(|e| {
            FatalError::SnapshotError(format!("Failed to get chain_hash for index {}: {}", snapshot_index, e))
        })?;
        let snapshot_stream = self.app.snapshot(&state_clone);
        let last_included_term = 0u64;

        let manifest = SnapshotManifest::new(
            snapshot_index,
            last_included_term,
            chain_hash,
            snapshot_stream.data,
        );

        let filename = SnapshotManifest::filename_for_index(snapshot_index);
        let snapshot_path = self.snapshot_dir.join(&filename);
        manifest.save_to_file(&snapshot_path).map_err(|e| FatalError::SnapshotError(format!("Failed to save snapshot: {}", e)))?;
        self.last_snapshot_index = Some(snapshot_index);
        Ok(snapshot_index)
    }

    pub fn snapshot_dir(&self) -> &Path { &self.snapshot_dir }
    pub fn last_snapshot_index(&self) -> Option<u64> { self.last_snapshot_index }
    pub fn set_snapshot_threshold(&mut self, threshold: u64) { self.snapshot_threshold = threshold; }
    
    pub fn truncate_log_to_snapshot(&mut self) -> Result<u64, FatalError> {
        use crate::engine::log::LogWriter;
        
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }
        
        let snapshot_index = match self.last_snapshot_index {
            Some(idx) => idx,
            None => return Err(FatalError::SnapshotError("Cannot truncate log: no snapshot has been taken".to_string())),
        };
        
        let new_base_index = snapshot_index + 1;
        let chain_hash = self.reader.get_chain_hash(snapshot_index).map_err(|e| {
            FatalError::SnapshotError(format!("Failed to get chain_hash for snapshot index {}: {}", snapshot_index, e))
        })?;
        
        let new_base_offset = match self.reader.get_authoritative_offset(new_base_index) {
            Some(offset) => offset,
            None => return Ok(snapshot_index),
        };
        
        let log_path = self.reader.path().to_path_buf();
        LogWriter::truncate_prefix(&log_path, new_base_index, new_base_offset, chain_hash)
            .map_err(|e| FatalError::SnapshotError(format!("Failed to truncate log: {}", e)))?;
        self.cleanup_old_snapshots(snapshot_index)?;
        
        Ok(snapshot_index)
    }
    
    fn cleanup_old_snapshots(&self, keep_index: u64) -> Result<(), FatalError> {
        use std::fs;
        let keep_filename = SnapshotManifest::filename_for_index(keep_index);
        let entries = match fs::read_dir(&self.snapshot_dir) {
            Ok(e) => e,
            Err(e) => { eprintln!("Warning: Could not read snapshot directory: {}", e); return Ok(()); }
        };
        for entry in entries.flatten() {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            if filename_str.ends_with(".snap") && filename_str != keep_filename {
                if let Err(e) = fs::remove_file(entry.path()) {
                    eprintln!("Warning: Failed to delete old snapshot {}: {}", filename_str, e);
                }
            }
        }
        Ok(())
    }
    
    pub fn compact(&mut self) -> Result<u64, FatalError> {
        let snapshot_index = self.take_snapshot()?;
        self.truncate_log_to_snapshot()?;
        Ok(snapshot_index)
    }
}
