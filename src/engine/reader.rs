use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::engine::format::{
    compute_chain_hash, compute_payload_hash, frame_size, LogHeader, LogMetadata,
    HEADER_SIZE, LOG_METADATA_SIZE,
};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogEntry {
    pub index: u64,
    pub view_id: u64,
    pub stream_id: u64,
    pub payload: Vec<u8>,
    /// Consensus timestamp (nanos). Assigned by Primary, agreed by quorum.
    pub timestamp_ns: u64,
    pub flags: u16,
    pub schema_version: u16,
    /// For deterministic random seed derivation.
    pub prev_hash: [u8; 16],
}

#[derive(Debug)]
pub enum ReadError {
    /// ENFORCES F3: Reader cannot observe uncommitted entries.
    IndexNotCommitted { requested: u64, committed: Option<u64> },
    IndexNotFound { requested: u64 },
    IndexTruncated { requested: u64, base_index: u64 },
    Io(io::Error),
    ValidationFailed { index: u64, reason: &'static str },
    TruncatedDuringRead { index: u64 },
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::IndexNotCommitted { requested, committed } => {
                write!(f, "Index {} not committed (committed: {:?})", requested, committed)
            }
            ReadError::IndexNotFound { requested } => {
                write!(f, "Index {} not found", requested)
            }
            ReadError::IndexTruncated { requested, base_index } => {
                write!(f, "Index {} was truncated (base_index: {})", requested, base_index)
            }
            ReadError::Io(e) => write!(f, "IO error: {}", e),
            ReadError::ValidationFailed { index, reason } => {
                write!(f, "Validation failed at index {}: {}", index, reason)
            }
            ReadError::TruncatedDuringRead { index } => {
                write!(f, "Entry {} truncated during read (concurrent recovery)", index)
            }
        }
    }
}

impl std::error::Error for ReadError {}

/// Visibility bridge enforcing the commit contract.
/// Writers: Release store. Readers: Acquire load. u64::MAX = no commits.
pub struct CommittedState {
    committed_index: AtomicU64,
}

impl CommittedState {
    pub fn new() -> Self {
        CommittedState {
            committed_index: AtomicU64::new(u64::MAX),
        }
    }

    #[allow(dead_code)]
    pub fn from_recovered(last_index: u64) -> Self {
        CommittedState {
            committed_index: AtomicU64::new(last_index),
        }
    }

    /// Acquire load. Returns None if no entries committed.
    #[inline]
    pub fn committed_index(&self) -> Option<u64> {
        let idx = self.committed_index.load(Ordering::Acquire);
        if idx == u64::MAX {
            None
        } else {
            Some(idx)
        }
    }

    /// Release store. Called ONLY after fdatasync success.
    #[inline]
    pub fn advance(&self, new_index: u64) {
        self.committed_index.store(new_index, Ordering::Release);
    }
}

impl Default for CommittedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only log reader. Multiple instances allowed. Never observes uncommitted entries.
pub struct LogReader {
    file: File,
    path: std::path::PathBuf,
    /// ENFORCES F3: All read bounds checked against this.
    committed_state: Arc<CommittedState>,
    /// Lazy index-to-offset cache for O(1) lookups.
    index_offsets: Vec<u64>,
    /// First index in file. 0 for fresh logs, snapshot+1 for truncated.
    base_index: u64,
    /// Chain hash of entry (base_index - 1). For chain continuity after truncation.
    base_prev_hash: [u8; 16],
}

#[allow(dead_code)]
impl LogReader {
    pub fn open(path: &Path, committed_state: Arc<CommittedState>) -> io::Result<Self> {
        let mut file = File::open(path)?;
        
        // Check file size to determine if it has a metadata header
        let file_size = file.seek(SeekFrom::End(0))?;
        
        let (base_index, base_prev_hash) = if file_size >= LOG_METADATA_SIZE as u64 {
            file.seek(SeekFrom::Start(0))?;
            let mut meta_buf = [0u8; LOG_METADATA_SIZE];
            let bytes_read = file.read(&mut meta_buf)?;
            
            if bytes_read == LOG_METADATA_SIZE {
                let metadata = LogMetadata::from_bytes(&meta_buf);
                if metadata.verify_magic() && metadata.verify_checksum() {
                    (metadata.base_index, metadata.base_prev_hash)
                } else {
                    (0, crate::engine::format::GENESIS_HASH)
                }
            } else {
                (0, crate::engine::format::GENESIS_HASH)
            }
        } else {
            (0, crate::engine::format::GENESIS_HASH)
        };
        
        Ok(LogReader {
            file,
            path: path.to_path_buf(),
            committed_state,
            index_offsets: Vec::new(),
            base_index,
            base_prev_hash,
        })
    }
    
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn committed_index(&self) -> Option<u64> {
        self.committed_state.committed_index()
    }

    /// Returns IndexNotCommitted if beyond committed, IndexTruncated if compacted.
    pub fn read(&mut self, index: u64) -> Result<LogEntry, ReadError> {
        if index < self.base_index {
            return Err(ReadError::IndexTruncated {
                requested: index,
                base_index: self.base_index,
            });
        }

        // VISIBILITY CHECK: Acquire load - the ONLY source of truth.
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: None,
            });
        }
        
        if index > committed {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: Some(committed),
            });
        }

        let offset = self.get_offset_for_index(index)?;
        self.read_entry_at_offset(offset, index)
    }

    /// Read [start, end] inclusive. Clamps to committed_index snapshot.
    pub fn read_range(&mut self, start: u64, end: u64) -> Result<Vec<LogEntry>, ReadError> {
        // Snapshot committed_index ONCE for consistent view.
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: start,
                committed: None,
            });
        }

        let effective_end = end.min(committed);
        
        if start > effective_end {
            return Err(ReadError::IndexNotCommitted {
                requested: start,
                committed: Some(committed),
            });
        }

        let mut entries = Vec::with_capacity((effective_end - start + 1) as usize);
        
        for idx in start..=effective_end {
            match self.read_entry_internal(idx) {
                Ok(entry) => entries.push(entry),
                Err(ReadError::TruncatedDuringRead { .. }) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    /// Returns entries [0, committed_index] at snapshot time.
    pub fn scan_all(&mut self) -> Result<Vec<LogEntry>, ReadError> {
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Ok(Vec::new());
        }

        self.read_range(0, committed)
    }

    /// Builds index-to-offset cache lazily. For truncated logs, cache[0] = base_index.
    fn get_offset_for_index(&mut self, index: u64) -> Result<u64, ReadError> {
        if index < self.base_index {
            return Err(ReadError::IndexTruncated {
                requested: index,
                base_index: self.base_index,
            });
        }

        let cache_pos = (index - self.base_index) as usize;

        if cache_pos < self.index_offsets.len() {
            return Ok(self.index_offsets[cache_pos]);
        }

        let start_cache_pos = self.index_offsets.len();
        let start_index = self.base_index + start_cache_pos as u64;
        
        let start_offset = if self.index_offsets.is_empty() {
            if self.base_index > 0 { LOG_METADATA_SIZE as u64 } else { 0 }
        } else {
            let last_cache_idx = self.index_offsets.len() - 1;
            let last_offset = self.index_offsets[last_cache_idx];
            let header = self.read_header_at_offset(last_offset)?;
            last_offset + frame_size(header.payload_size) as u64
        };

        let mut offset = start_offset;
        for logical_idx in start_index..=index {
            let file_len = self.file.seek(SeekFrom::End(0))?;
            if offset >= file_len {
                return Err(ReadError::IndexNotFound { requested: index });
            }

            let header = self.read_header_at_offset(offset)?;
            
            if header.index != logical_idx {
                return Err(ReadError::ValidationFailed {
                    index: logical_idx,
                    reason: "Index mismatch in sequential scan",
                });
            }

            self.index_offsets.push(offset);
            offset += frame_size(header.payload_size) as u64;
        }

        Ok(self.index_offsets[cache_pos])
    }

    fn read_header_at_offset(&mut self, offset: u64) -> Result<LogHeader, ReadError> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut header_buf = [0u8; HEADER_SIZE];
        let bytes_read = self.file.read(&mut header_buf)?;
        
        if bytes_read < HEADER_SIZE {
            return Err(ReadError::TruncatedDuringRead { index: u64::MAX });
        }

        let header = LogHeader::from_bytes(&header_buf);
        
        if !header.verify_checksum() {
            return Err(ReadError::ValidationFailed {
                index: header.index,
                reason: "Header CRC mismatch",
            });
        }

        Ok(header)
    }

    fn read_entry_at_offset(&mut self, offset: u64, expected_index: u64) -> Result<LogEntry, ReadError> {
        let header = self.read_header_at_offset(offset)?;
        
        if header.index != expected_index {
            return Err(ReadError::ValidationFailed {
                index: expected_index,
                reason: "Index mismatch",
            });
        }

        // Read payload
        let payload_offset = offset + HEADER_SIZE as u64;
        self.file.seek(SeekFrom::Start(payload_offset))?;
        
        let mut payload = vec![0u8; header.payload_size as usize];
        let bytes_read = self.file.read(&mut payload)?;
        
        if bytes_read < header.payload_size as usize {
            return Err(ReadError::TruncatedDuringRead { index: expected_index });
        }

        let computed_hash = compute_payload_hash(&payload);
        if computed_hash != header.payload_hash {
            return Err(ReadError::ValidationFailed {
                index: expected_index,
                reason: "Payload hash mismatch",
            });
        }

        Ok(LogEntry {
            index: header.index,
            view_id: header.view_id,
            stream_id: header.stream_id,
            payload,
            timestamp_ns: header.timestamp_ns,
            flags: header.flags,
            schema_version: header.schema_version,
            prev_hash: header.prev_hash,
        })
    }

    fn read_entry_internal(&mut self, index: u64) -> Result<LogEntry, ReadError> {
        let offset = self.get_offset_for_index(index)?;
        self.read_entry_at_offset(offset, index)
    }

    #[inline]
    pub fn is_readable(&self, index: u64) -> bool {
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        committed != u64::MAX && index <= committed
    }

    #[inline]
    pub fn len(&self) -> u64 {
        match self.committed_index() {
            Some(idx) => idx + 1,
            None => 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.committed_index().is_none()
    }

    /// Chain hash bridges the hash chain across snapshot compaction.
    /// Computed as: BLAKE3(Header[4..64] || Payload)[0..16]
    pub fn get_chain_hash(&mut self, index: u64) -> Result<[u8; 16], ReadError> {
        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        
        if committed == u64::MAX {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: None,
            });
        }
        
        if index > committed {
            return Err(ReadError::IndexNotCommitted {
                requested: index,
                committed: Some(committed),
            });
        }

        let offset = self.get_offset_for_index(index)?;
        let header = self.read_header_at_offset(offset)?;
        let payload_offset = offset + HEADER_SIZE as u64;
        self.file.seek(SeekFrom::Start(payload_offset))?;
        
        let mut payload = vec![0u8; header.payload_size as usize];
        let bytes_read = self.file.read(&mut payload)?;
        
        if bytes_read < header.payload_size as usize {
            return Err(ReadError::TruncatedDuringRead { index });
        }

        Ok(compute_chain_hash(&header, &payload))
    }

    /// The "Cut Point" for log truncation. Executor uses this for compaction.
    pub fn get_authoritative_offset(&mut self, index: u64) -> Option<u64> {
        if index < self.base_index {
            return None;
        }

        let committed = self.committed_state.committed_index.load(Ordering::Acquire);
        if committed == u64::MAX || index > committed {
            return None;
        }
        self.get_offset_for_index(index).ok()
    }

    #[inline]
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    #[inline]
    pub fn base_prev_hash(&self) -> [u8; 16] {
        self.base_prev_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_log(path: &Path, committed_state: &Arc<CommittedState>, entries: &[&[u8]]) {
        use crate::engine::log::LogWriter;
        
        // Create writer - but we need to use the shared committed state
        // For testing, we'll create entries and manually update committed state
        let mut writer = LogWriter::create(path, 1).unwrap();
        
        for (i, payload) in entries.iter().enumerate() {
            writer.append(payload, 0, 0, 1_000_000_000 + i as u64 * 1_000_000_000).unwrap();
            // Update shared committed state
            committed_state.advance(i as u64);
        }
    }

    #[test]
    fn test_reader_respects_committed_index() {
        // ENFORCES F3: Reader cannot observe uncommitted entries
        let path = Path::new("/tmp/chr_reader_test_committed.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Create log with entries
        create_test_log(path, &committed_state, &[b"entry0", b"entry1", b"entry2"]);

        // Open reader
        let mut reader = LogReader::open(path, committed_state.clone()).unwrap();

        // Should be able to read committed entries
        assert!(reader.read(0).is_ok());
        assert!(reader.read(1).is_ok());
        assert!(reader.read(2).is_ok());

        // committed_index is 2, so index 3 should fail
        match reader.read(3) {
            Err(ReadError::IndexNotCommitted { requested: 3, committed: Some(2) }) => {}
            other => panic!("Expected IndexNotCommitted, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_empty_log() {
        let path = Path::new("/tmp/chr_reader_test_empty.log");
        let _ = fs::remove_file(path);

        // Create empty file
        fs::File::create(path).unwrap();

        let committed_state = Arc::new(CommittedState::new());
        let mut reader = LogReader::open(path, committed_state).unwrap();

        // No entries committed
        assert!(reader.is_empty());
        assert_eq!(reader.len(), 0);
        
        match reader.read(0) {
            Err(ReadError::IndexNotCommitted { requested: 0, committed: None }) => {}
            other => panic!("Expected IndexNotCommitted with None, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_scan_all() {
        let path = Path::new("/tmp/chr_reader_test_scan.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"a", b"b", b"c", b"d", b"e"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        let entries = reader.scan_all().unwrap();

        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].payload, b"a");
        assert_eq!(entries[4].payload, b"e");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_range_clamped_to_committed() {
        // ENFORCES F3: Range reads clamped to committed_index
        let path = Path::new("/tmp/chr_reader_test_range.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"0", b"1", b"2"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        
        // Request range [0, 100] but only 3 entries committed
        let entries = reader.read_range(0, 100).unwrap();
        assert_eq!(entries.len(), 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_visibility_ordering() {
        // Test that Acquire/Release ordering works correctly
        let path = Path::new("/tmp/chr_reader_test_ordering.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Initially no entries
        {
            fs::File::create(path).unwrap();
            let reader = LogReader::open(path, committed_state.clone()).unwrap();
            assert!(reader.is_empty());
        }

        // Add entries and advance committed state
        create_test_log(path, &committed_state, &[b"first", b"second"]);

        // New reader should see committed entries
        let reader = LogReader::open(path, committed_state.clone()).unwrap();
        assert_eq!(reader.len(), 2);
        assert!(reader.is_readable(0));
        assert!(reader.is_readable(1));
        assert!(!reader.is_readable(2));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_concurrent_reader_writer() {
        // Test reader/writer interleaving
        let path = Path::new("/tmp/chr_reader_test_concurrent.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        
        // Create initial entries
        create_test_log(path, &committed_state, &[b"initial"]);

        // Reader in separate scope
        let mut reader = LogReader::open(path, committed_state.clone()).unwrap();
        
        // Reader sees 1 entry
        assert_eq!(reader.len(), 1);
        let entry = reader.read(0).unwrap();
        assert_eq!(entry.payload, b"initial");

        // Simulate writer appending more (we'll just advance committed state)
        // In real usage, writer would append and then advance
        committed_state.advance(1);
        committed_state.advance(2);

        // Reader should now see the new committed index
        // (though we didn't actually write the entries, this tests visibility)
        assert_eq!(reader.committed_index(), Some(2));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_multiple_readers() {
        // Multiple readers can coexist
        let path = Path::new("/tmp/chr_reader_test_multi.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"shared"]);

        let mut reader1 = LogReader::open(path, committed_state.clone()).unwrap();
        let mut reader2 = LogReader::open(path, committed_state.clone()).unwrap();

        // Both readers see the same committed state
        assert_eq!(reader1.committed_index(), reader2.committed_index());
        
        let entry1 = reader1.read(0).unwrap();
        let entry2 = reader2.read(0).unwrap();
        
        assert_eq!(entry1.payload, entry2.payload);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reader_validates_entries() {
        // Reader validates CRC and payload hash
        let path = Path::new("/tmp/chr_reader_test_validate.log");
        let _ = fs::remove_file(path);

        let committed_state = Arc::new(CommittedState::new());
        create_test_log(path, &committed_state, &[b"valid_entry"]);

        let mut reader = LogReader::open(path, committed_state).unwrap();
        let entry = reader.read(0).unwrap();
        
        assert_eq!(entry.payload, b"valid_entry");
        assert_eq!(entry.index, 0);

        let _ = fs::remove_file(path);
    }
}
