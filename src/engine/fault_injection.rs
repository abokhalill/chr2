use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::engine::format::{
    calculate_padding, compute_chain_hash, frame_size,
    LogHeader, GENESIS_HASH, HEADER_SIZE, MAX_PAYLOAD_SIZE,
};

/// Fault injection modes for testing.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum FaultMode {
    /// Write only the header, leave payload as zeros/garbage.
    HeaderOnly,
    /// Write header + partial payload (specified number of bytes).
    PartialPayload(usize),
    /// Write complete entry but corrupt the header CRC after writing.
    CorruptHeaderCrc,
    /// Write complete entry but corrupt the payload after writing.
    CorruptPayload,
    /// Write complete entry but corrupt the prev_hash field.
    CorruptPrevHash,
    /// Write zeros in the middle of the log (zero-hole).
    ZeroHole(usize),
}

/// A fault-injecting log writer for testing recovery robustness.
#[allow(dead_code)]
pub struct FaultingLogWriter {
    file: File,
    write_offset: u64,
    next_index: u64,
    tail_hash: [u8; 16],
    view_id: u64,
}

#[allow(dead_code)]
impl FaultingLogWriter {
    /// Open a log file for fault injection testing.
    pub fn open(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(FaultingLogWriter {
            file,
            write_offset,
            next_index,
            tail_hash,
            view_id,
        })
    }

    /// Create a new empty log file for fault injection testing.
    pub fn create(path: &Path, view_id: u64) -> io::Result<Self> {
        Self::open(path, 0, 0, GENESIS_HASH, view_id)
    }

    /// Append a valid entry (no fault injection).
    pub fn append_valid(&mut self, payload: &[u8]) -> io::Result<u64> {
        if payload.len() > MAX_PAYLOAD_SIZE as usize {
            panic!("Payload too large");
        }

        let index = self.next_index;
        let header = LogHeader::new(
            index,
            self.view_id,
            0,
            self.tail_hash,
            payload,
            1_000_000_000, // timestamp_ns
            0,
            1,
        );

        let header_bytes = header.as_bytes();
        let padding_len = calculate_padding(payload.len() as u32);
        let padding = vec![0u8; padding_len];

        self.file.seek(SeekFrom::Start(self.write_offset))?;
        self.file.write_all(header_bytes)?;
        self.file.write_all(payload)?;
        self.file.write_all(&padding)?;
        self.file.sync_all()?;

        let expected_bytes = frame_size(payload.len() as u32);
        self.tail_hash = compute_chain_hash(&header, payload);
        self.next_index += 1;
        self.write_offset += expected_bytes as u64;

        Ok(index)
    }

    /// Append an entry with fault injection.
    pub fn append_faulted(&mut self, payload: &[u8], fault: FaultMode) -> io::Result<u64> {
        if payload.len() > MAX_PAYLOAD_SIZE as usize {
            panic!("Payload too large");
        }

        let index = self.next_index;
        let header = LogHeader::new(
            index,
            self.view_id,
            0,
            self.tail_hash,
            payload,
            1_000_000_000, // timestamp_ns
            0,
            1,
        );

        let header_bytes = header.as_bytes();
        let _expected_bytes = frame_size(payload.len() as u32);

        match fault {
            FaultMode::HeaderOnly => {
                // Write only the header, no payload
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(header_bytes)?;
                self.file.sync_all()?;
                // Don't update state - simulates crash after partial write
            }

            FaultMode::PartialPayload(bytes) => {
                // Write header + partial payload
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(header_bytes)?;
                let to_write = std::cmp::min(bytes, payload.len());
                self.file.write_all(&payload[..to_write])?;
                self.file.sync_all()?;
                // Don't update state - simulates crash after partial write
            }

            FaultMode::CorruptHeaderCrc => {
                // Write complete entry, then corrupt the CRC
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(header_bytes)?;
                self.file.write_all(payload)?;
                let padding_len = calculate_padding(payload.len() as u32);
                self.file.write_all(&vec![0u8; padding_len])?;
                self.file.sync_all()?;

                // Corrupt the CRC (first 4 bytes of header)
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
                self.file.sync_all()?;
                // Don't update state - this is corruption
            }

            FaultMode::CorruptPayload => {
                // Write complete entry, then corrupt the payload
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(header_bytes)?;
                self.file.write_all(payload)?;
                let padding_len = calculate_padding(payload.len() as u32);
                self.file.write_all(&vec![0u8; padding_len])?;
                self.file.sync_all()?;

                // Corrupt the payload (write garbage at payload offset)
                let payload_offset = self.write_offset + HEADER_SIZE as u64;
                self.file.seek(SeekFrom::Start(payload_offset))?;
                self.file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF])?;
                self.file.sync_all()?;
                // Don't update state - this is corruption
            }

            FaultMode::CorruptPrevHash => {
                // Write complete entry, then corrupt the prev_hash field
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(header_bytes)?;
                self.file.write_all(payload)?;
                let padding_len = calculate_padding(payload.len() as u32);
                self.file.write_all(&vec![0u8; padding_len])?;
                self.file.sync_all()?;

                // Corrupt prev_hash (bytes 32-48 of header)
                let prev_hash_offset = self.write_offset + 32;
                self.file.seek(SeekFrom::Start(prev_hash_offset))?;
                self.file.write_all(&[0xBA, 0xAD, 0xF0, 0x0D])?;
                self.file.sync_all()?;
                // Don't update state - this is corruption
            }

            FaultMode::ZeroHole(size) => {
                // Write zeros at current offset (simulates sparse file / partial zeroing)
                self.file.seek(SeekFrom::Start(self.write_offset))?;
                self.file.write_all(&vec![0u8; size])?;
                self.file.sync_all()?;
                // Don't update state
            }
        }

        Ok(index)
    }

    /// Get current write offset.
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Get next index.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Get tail hash.
    pub fn tail_hash(&self) -> [u8; 16] {
        self.tail_hash
    }

    /// Manually advance state (for testing specific scenarios).
    pub fn advance_state(&mut self, payload: &[u8]) {
        let header = LogHeader::new(
            self.next_index,
            self.view_id,
            0,
            self.tail_hash,
            payload,
            1_000_000_000, // timestamp_ns
            0,
            1,
        );
        self.tail_hash = compute_chain_hash(&header, payload);
        self.next_index += 1;
        self.write_offset += frame_size(payload.len() as u32) as u64;
    }

    /// Simulate write reordering: write entry N+1 before entry N.
    /// This simulates what happens when the drive reorders writes.
    /// Returns (offset_n, offset_n1) for verification.
    pub fn append_reordered_pair(
        &mut self,
        payload_n: &[u8],
        payload_n1: &[u8],
    ) -> io::Result<(u64, u64)> {
        // Calculate what entry N would look like
        let index_n = self.next_index;
        let header_n = LogHeader::new(
            index_n,
            self.view_id,
            0,
            self.tail_hash,
            payload_n,
            1_000_000_000, // timestamp_ns
            0,
            1,
        );
        let offset_n = self.write_offset;
        let frame_size_n = frame_size(payload_n.len() as u32);

        // Calculate what entry N+1 would look like (using N's chain hash)
        let chain_hash_n = compute_chain_hash(&header_n, payload_n);
        let index_n1 = index_n + 1;
        let header_n1 = LogHeader::new(
            index_n1,
            self.view_id,
            0,
            chain_hash_n,
            payload_n1,
            2_000_000_000, // timestamp_ns
            0,
            1,
        );
        let offset_n1 = offset_n + frame_size_n as u64;
        let frame_size_n1 = frame_size(payload_n1.len() as u32);

        // REORDER: Write N+1 first, then N
        // This simulates drive reordering where N+1 persists before N
        
        // Write entry N+1 at its correct offset
        self.file.seek(SeekFrom::Start(offset_n1))?;
        self.file.write_all(header_n1.as_bytes())?;
        self.file.write_all(payload_n1)?;
        let padding_n1 = vec![0u8; calculate_padding(payload_n1.len() as u32)];
        self.file.write_all(&padding_n1)?;
        self.file.sync_all()?;

        // Now write entry N at its correct offset
        self.file.seek(SeekFrom::Start(offset_n))?;
        self.file.write_all(header_n.as_bytes())?;
        self.file.write_all(payload_n)?;
        let padding_n = vec![0u8; calculate_padding(payload_n.len() as u32)];
        self.file.write_all(&padding_n)?;
        self.file.sync_all()?;

        // Update state as if both were written normally
        self.tail_hash = compute_chain_hash(&header_n1, payload_n1);
        self.next_index += 2;
        self.write_offset += (frame_size_n + frame_size_n1) as u64;

        Ok((offset_n, offset_n1))
    }

    /// Simulate partial reordering: write N+1 but leave N as zeros.
    /// This is the dangerous case where power loss occurs after N+1 persists but before N.
    pub fn append_orphaned_entry(&mut self, payload_n: &[u8], payload_n1: &[u8]) -> io::Result<u64> {
        // Calculate what entry N would look like
        let index_n = self.next_index;
        let header_n = LogHeader::new(
            index_n,
            self.view_id,
            0,
            self.tail_hash,
            payload_n,
            1_000_000_000, // timestamp_ns
            0,
            1,
        );
        let offset_n = self.write_offset;
        let frame_size_n = frame_size(payload_n.len() as u32);

        // Calculate what entry N+1 would look like
        let chain_hash_n = compute_chain_hash(&header_n, payload_n);
        let index_n1 = index_n + 1;
        let header_n1 = LogHeader::new(
            index_n1,
            self.view_id,
            0,
            chain_hash_n,
            payload_n1,
            2_000_000_000, // timestamp_ns
            0,
            1,
        );
        let offset_n1 = offset_n + frame_size_n as u64;

        // Write zeros at entry N's location (simulates N not persisting)
        self.file.seek(SeekFrom::Start(offset_n))?;
        self.file.write_all(&vec![0u8; frame_size_n])?;
        
        // Write entry N+1 at its correct offset
        self.file.seek(SeekFrom::Start(offset_n1))?;
        self.file.write_all(header_n1.as_bytes())?;
        self.file.write_all(payload_n1)?;
        let padding_n1 = vec![0u8; calculate_padding(payload_n1.len() as u32)];
        self.file.write_all(&padding_n1)?;
        self.file.sync_all()?;

        // Don't update state - this simulates crash before memory update

        Ok(offset_n1)
    }
}

/// Inject corruption at a specific offset in an existing log file.
#[allow(dead_code)]
pub fn inject_corruption(path: &Path, offset: u64, corruption: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(corruption)?;
    file.sync_all()?;
    Ok(())
}

/// Inject zeros at a specific offset (zero-hole).
#[allow(dead_code)]
pub fn inject_zero_hole(path: &Path, offset: u64, size: usize) -> io::Result<()> {
    inject_corruption(path, offset, &vec![0u8; size])
}

/// Truncate file to specific size without proper recovery.
#[allow(dead_code)]
pub fn force_truncate(path: &Path, size: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(size)?;
    file.sync_all()?;
    Ok(())
}

/// Read raw bytes from log file for inspection.
#[allow(dead_code)]
pub fn read_raw(path: &Path, offset: u64, size: usize) -> io::Result<Vec<u8>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; size];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::recovery::{LogRecovery, RecoveryOutcome};
    use std::fs;

    #[test]
    fn test_partial_write_recovery() {
        let path = Path::new("/tmp/chr_fault_partial.log");
        let _ = fs::remove_file(path);

        // Write 5 valid entries
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..5 {
                writer.append_valid(format!("entry {}", i).as_bytes()).unwrap();
            }
            // Write partial entry (header only)
            writer.append_faulted(b"partial entry", FaultMode::HeaderOnly).unwrap();
        }

        // Recovery should truncate the partial entry
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let outcome = recovery.scan().unwrap();

        match outcome {
            RecoveryOutcome::Truncated { last_valid_index, .. } => {
                assert_eq!(last_valid_index, 4, "Should have 5 valid entries (0-4)");
            }
            RecoveryOutcome::Clean { last_index, .. } => {
                // This is also acceptable if the partial write didn't extend the file
                assert_eq!(last_index, 4);
            }
            _ => panic!("Expected Truncated or Clean, got {:?}", outcome),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    #[should_panic(expected = "Mid-log corruption")]
    fn test_mid_log_corruption_panics() {
        let path = Path::new("/tmp/chr_fault_midlog.log");
        let _ = fs::remove_file(path);

        // Write 10 valid entries with fixed-size payload for predictable offsets
        let mut offsets = Vec::new();
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..10 {
                offsets.push(writer.write_offset());
                // Use fixed 8-byte payload for no padding
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
        }

        // Corrupt entry 5's header CRC (first 4 bytes)
        inject_corruption(path, offsets[5], &[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();

        // Recovery should panic with Mid-log corruption
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let _ = recovery.scan(); // Should panic

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_broken_chain_detection() {
        // To trigger BrokenChain, we need to corrupt prev_hash AND recalculate the CRC.
        // This is a more sophisticated test that crafts a valid-CRC header with wrong prev_hash.
        let path = Path::new("/tmp/chr_fault_chain.log");
        let _ = fs::remove_file(path);

        // Write 5 valid entries, tracking offsets
        let mut offsets = Vec::new();
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..5 {
                offsets.push(writer.write_offset());
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
        }

        // Read entry 3's header, corrupt prev_hash, recalculate CRC, write back
        let entry_3_offset = offsets[3];
        let header_bytes = read_raw(path, entry_3_offset, HEADER_SIZE).unwrap();
        let mut header_arr: [u8; HEADER_SIZE] = header_bytes.try_into().unwrap();
        
        // Corrupt prev_hash (bytes 32-48)
        header_arr[32] = 0xBA;
        header_arr[33] = 0xAD;
        header_arr[34] = 0xF0;
        header_arr[35] = 0x0D;
        
        // Recalculate CRC for bytes [4..64]
        let new_crc = crc32c::crc32c(&header_arr[4..]);
        header_arr[0..4].copy_from_slice(&new_crc.to_le_bytes());
        
        // Write back the corrupted header with valid CRC
        inject_corruption(path, entry_3_offset, &header_arr).unwrap();

        // Recovery should panic with Broken chain
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let result = std::panic::catch_unwind(|| {
            let _ = recovery.scan();
        });
        
        assert!(result.is_err(), "Expected panic on broken chain");
        let _ = fs::remove_file(path);
    }

    #[test]
    #[should_panic(expected = "Zero-hole")]
    fn test_zero_hole_panics() {
        let path = Path::new("/tmp/chr_fault_zerohole.log");
        let _ = fs::remove_file(path);

        // Write 10 valid entries, tracking offsets
        let mut offsets = Vec::new();
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..10 {
                offsets.push(writer.write_offset());
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
        }

        // Inject zeros at entry 5's header (HEADER_SIZE bytes of zeros)
        // This creates a zero header followed by entry 6+ which is non-zero
        inject_zero_hole(path, offsets[5], HEADER_SIZE).unwrap();

        // Recovery should panic with Zero-hole
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let _ = recovery.scan(); // Should panic

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_tail_corruption_truncates() {
        let path = Path::new("/tmp/chr_fault_tail.log");
        let _ = fs::remove_file(path);

        // Write 5 valid entries, tracking offsets
        let mut offsets = Vec::new();
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..5 {
                offsets.push(writer.write_offset());
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
        }

        // Corrupt the last entry's payload (this is at the tail, so recoverable)
        let entry_4_payload_offset = offsets[4] + HEADER_SIZE as u64;
        inject_corruption(path, entry_4_payload_offset, &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();

        // Recovery should truncate entry 4
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let outcome = recovery.scan().unwrap();

        match outcome {
            RecoveryOutcome::Truncated { last_valid_index, .. } => {
                assert_eq!(last_valid_index, 3, "Should truncate to entry 3");
            }
            _ => panic!("Expected Truncated, got {:?}", outcome),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reordered_writes_both_present() {
        // Test that reordered writes (N+1 written before N) still recover correctly
        // when both entries are present on disk.
        let path = Path::new("/tmp/chr_fault_reorder.log");
        let _ = fs::remove_file(path);

        // Write some valid entries first
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..3 {
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
            // Now write a reordered pair (N+1 persists before N, but both end up on disk)
            writer.append_reordered_pair(b"entry_n", b"entry_n1").unwrap();
        }

        // Recovery should succeed - both entries are valid
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let outcome = recovery.scan().unwrap();

        match outcome {
            RecoveryOutcome::Clean { last_index, .. } => {
                assert_eq!(last_index, 4, "Should have 5 entries (0-4)");
            }
            _ => panic!("Expected Clean, got {:?}", outcome),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    #[should_panic(expected = "Zero-hole")]
    fn test_orphaned_entry_panics() {
        // Test that an orphaned entry (N+1 persists, N is zeros) triggers ZeroHole panic.
        // This simulates the dangerous reordering case where power loss occurs
        // after N+1 persists but before N.
        let path = Path::new("/tmp/chr_fault_orphan.log");
        let _ = fs::remove_file(path);

        // Write some valid entries first
        {
            let mut writer = FaultingLogWriter::create(path, 1).unwrap();
            for i in 0..3 {
                writer.append_valid(format!("entry{:02}", i).as_bytes()).unwrap();
            }
            // Write orphaned entry: N is zeros, N+1 is valid
            writer.append_orphaned_entry(b"entry_n", b"entry_n1").unwrap();
        }

        // Recovery should panic with ZeroHole (zeros at N, valid data at N+1)
        let recovery = LogRecovery::open(path).unwrap().unwrap();
        let _ = recovery.scan(); // Should panic

        let _ = fs::remove_file(path);
    }
}
