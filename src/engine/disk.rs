//! Unified durability abstraction. Writes are speculative until barrier().

use std::io;

use crate::engine::errors::FatalError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteToken(pub u64);

impl WriteToken {
    #[inline]
    pub fn from_index(index: u64) -> Self {
        WriteToken(index)
    }

    #[inline]
    pub fn index(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BarrierResult {
    pub durable_index: u64,
    pub entries_synced: u64,
}

#[derive(Debug, Clone)]
pub enum DiskCompletion {
    WriteComplete {
        token: WriteToken,
    },
    BarrierComplete {
        result: BarrierResult,
    },
    Error {
        token: Option<WriteToken>,
        error: String,
    },
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub payload: Vec<u8>,
    pub stream_id: u64,
    pub flags: u16,
    pub timestamp_ns: u64,
}

impl LogEntry {
    pub fn new(payload: Vec<u8>, timestamp_ns: u64) -> Self {
        Self {
            payload,
            stream_id: 0,
            flags: 0,
            timestamp_ns,
        }
    }

    pub fn with_metadata(payload: Vec<u8>, stream_id: u64, flags: u16, timestamp_ns: u64) -> Self {
        Self {
            payload,
            stream_id,
            flags,
            timestamp_ns,
        }
    }
}

/// Single-writer durability contract. barrier() is the only durability operation.
pub trait VirtualDisk: Send {
    fn submit_write(&mut self, entry: LogEntry) -> io::Result<WriteToken>;
    fn submit_write_batch(&mut self, entries: &[LogEntry]) -> io::Result<WriteToken>;
    fn barrier(&mut self, up_to: WriteToken) -> Result<BarrierResult, FatalError>;
    fn barrier_all(&mut self) -> Result<BarrierResult, FatalError>;
    fn committed_index(&self) -> Option<u64>;
    fn next_index(&self) -> u64;
    fn tail_hash(&self) -> [u8; 16];
    fn view_id(&self) -> u64;
    fn set_view_id(&mut self, view_id: u64);
    fn write_offset(&self) -> u64;
    fn transfer_ownership(&mut self);
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

/// O_DSYNC wrapper. Each pwritev is implicitly a barrier.
#[allow(dead_code)]
pub struct SyncDisk {
    writer: crate::engine::log::LogWriter,
    highest_submitted: Option<WriteToken>,
    o_dsync_enabled: bool,
}

impl SyncDisk {
    pub fn new(writer: crate::engine::log::LogWriter) -> Self {
        Self {
            writer,
            highest_submitted: None,
            o_dsync_enabled: true,
        }
    }

    pub fn with_dsync_flag(writer: crate::engine::log::LogWriter, o_dsync: bool) -> Self {
        Self {
            writer,
            highest_submitted: None,
            o_dsync_enabled: o_dsync,
        }
    }

    pub fn writer(&self) -> &crate::engine::log::LogWriter {
        &self.writer
    }

    pub fn writer_mut(&mut self) -> &mut crate::engine::log::LogWriter {
        &mut self.writer
    }

    pub fn into_inner(self) -> crate::engine::log::LogWriter {
        self.writer
    }
}

impl VirtualDisk for SyncDisk {
    fn submit_write(&mut self, entry: LogEntry) -> io::Result<WriteToken> {
        let index = self.writer.append(
            &entry.payload,
            entry.stream_id,
            entry.flags,
            entry.timestamp_ns,
        )?;
        let token = WriteToken::from_index(index);
        self.highest_submitted = Some(token);
        Ok(token)
    }

    fn submit_write_batch(&mut self, entries: &[LogEntry]) -> io::Result<WriteToken> {
        if entries.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Empty batch"));
        }

        // Convert to the format expected by LogWriter
        let entries_with_meta: Vec<(Vec<u8>, u64, u16)> = entries
            .iter()
            .map(|e| (e.payload.clone(), e.stream_id, e.flags))
            .collect();

        let timestamp_ns = entries.first().map(|e| e.timestamp_ns).unwrap_or(0);

        let last_index = self
            .writer
            .append_batch_with_metadata(&entries_with_meta, timestamp_ns)
            .map_err(|e| match e {
                FatalError::IoError(io_err) => io_err,
                FatalError::PayloadTooLarge { size, max } => io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Payload size {} exceeds max {}", size, max),
                ),
                _ => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
            })?;

        let token = WriteToken::from_index(last_index);
        self.highest_submitted = Some(token);
        Ok(token)
    }

    fn barrier(&mut self, up_to: WriteToken) -> Result<BarrierResult, FatalError> {
        // With O_DSYNC, writes are already durable. The barrier is a no-op
        // but we verify the token is valid and return the result.
        let committed = self.writer.committed_index();

        // Verify the requested token is actually committed
        match committed {
            Some(idx) if idx >= up_to.index() => {
                Ok(BarrierResult {
                    durable_index: idx,
                    entries_synced: 0, // Already synced by O_DSYNC
                })
            }
            Some(idx) => {
                // This shouldn't happen with O_DSYNC - writes should be durable on return
                Err(FatalError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "Barrier requested for {} but only {} is committed",
                        up_to.index(),
                        idx
                    ),
                )))
            }
            None => {
                if up_to.index() == 0 {
                    // Edge case: barrier on first entry before any writes
                    Err(FatalError::IoError(io::Error::new(
                        io::ErrorKind::Other,
                        "No entries committed yet",
                    )))
                } else {
                    Err(FatalError::IoError(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Barrier requested for {} but no entries committed",
                            up_to.index()
                        ),
                    )))
                }
            }
        }
    }

    fn barrier_all(&mut self) -> Result<BarrierResult, FatalError> {
        match self.highest_submitted {
            Some(token) => self.barrier(token),
            None => {
                // No writes submitted, return current state
                Ok(BarrierResult {
                    durable_index: self.writer.committed_index().unwrap_or(0),
                    entries_synced: 0,
                })
            }
        }
    }

    fn committed_index(&self) -> Option<u64> {
        self.writer.committed_index()
    }

    fn next_index(&self) -> u64 {
        self.writer.next_index()
    }

    fn tail_hash(&self) -> [u8; 16] {
        self.writer.tail_hash()
    }

    fn view_id(&self) -> u64 {
        self.writer.view_id()
    }

    fn set_view_id(&mut self, _view_id: u64) {
        // LogWriter doesn't support changing view_id after creation.
        // This would require reopening the file or adding a setter.
        // For now, this is a limitation we document.
        unimplemented!("LogWriter does not support changing view_id after creation")
    }

    fn write_offset(&self) -> u64 {
        self.writer.write_offset()
    }

    fn transfer_ownership(&mut self) {
        self.writer.transfer_ownership()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.writer.truncate(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_sync_disk_single_write() {
        let path = Path::new("/tmp/chr_test_sync_disk_single.log");
        let _ = fs::remove_file(path);

        let writer = crate::engine::log::LogWriter::create(path, 1).unwrap();
        let mut disk = SyncDisk::new(writer);

        let entry = LogEntry::new(b"hello world".to_vec(), 1_000_000_000);
        let token = disk.submit_write(entry).unwrap();

        assert_eq!(token.index(), 0);
        assert_eq!(disk.committed_index(), Some(0));
        assert_eq!(disk.next_index(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_sync_disk_batch_write() {
        let path = Path::new("/tmp/chr_test_sync_disk_batch.log");
        let _ = fs::remove_file(path);

        let writer = crate::engine::log::LogWriter::create(path, 1).unwrap();
        let mut disk = SyncDisk::new(writer);

        let entries: Vec<LogEntry> = (0..10)
            .map(|i| LogEntry::new(format!("entry_{}", i).into_bytes(), 1_000_000_000))
            .collect();

        let token = disk.submit_write_batch(&entries).unwrap();

        assert_eq!(token.index(), 9);
        assert_eq!(disk.committed_index(), Some(9));
        assert_eq!(disk.next_index(), 10);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_sync_disk_barrier() {
        let path = Path::new("/tmp/chr_test_sync_disk_barrier.log");
        let _ = fs::remove_file(path);

        let writer = crate::engine::log::LogWriter::create(path, 1).unwrap();
        let mut disk = SyncDisk::new(writer);

        // Write some entries
        for i in 0..5 {
            let entry = LogEntry::new(format!("entry_{}", i).into_bytes(), 1_000_000_000);
            disk.submit_write(entry).unwrap();
        }

        // Barrier should succeed (already durable with O_DSYNC)
        let result = disk.barrier(WriteToken::from_index(4)).unwrap();
        assert_eq!(result.durable_index, 4);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_sync_disk_barrier_all() {
        let path = Path::new("/tmp/chr_test_sync_disk_barrier_all.log");
        let _ = fs::remove_file(path);

        let writer = crate::engine::log::LogWriter::create(path, 1).unwrap();
        let mut disk = SyncDisk::new(writer);

        // Write some entries
        for i in 0..5 {
            let entry = LogEntry::new(format!("entry_{}", i).into_bytes(), 1_000_000_000);
            disk.submit_write(entry).unwrap();
        }

        // Barrier all should succeed
        let result = disk.barrier_all().unwrap();
        assert_eq!(result.durable_index, 4);

        let _ = fs::remove_file(path);
    }
}

/// io_uring async wrapper. Writes are speculative until barrier().
#[cfg(feature = "io_uring")]
pub struct IoUringDisk {
    writer: crate::engine::uring::IoUringWriter,
    highest_submitted: Option<WriteToken>,
}

#[cfg(feature = "io_uring")]
impl IoUringDisk {
    pub fn new(writer: crate::engine::uring::IoUringWriter) -> Self {
        Self {
            writer,
            highest_submitted: None,
        }
    }

    pub fn create(path: &std::path::Path, view_id: u64) -> io::Result<Self> {
        let writer = crate::engine::uring::IoUringWriter::create(path, view_id, None)?;
        Ok(Self::new(writer))
    }

    pub fn create_direct(path: &std::path::Path, view_id: u64) -> io::Result<Self> {
        let writer = crate::engine::uring::IoUringWriter::create_direct(path, view_id, None)?;
        Ok(Self::new(writer))
    }

    pub fn writer(&self) -> &crate::engine::uring::IoUringWriter {
        &self.writer
    }

    pub fn writer_mut(&mut self) -> &mut crate::engine::uring::IoUringWriter {
        &mut self.writer
    }

    pub fn into_inner(self) -> crate::engine::uring::IoUringWriter {
        self.writer
    }

    pub fn is_direct_io(&self) -> bool {
        self.writer.is_direct_io()
    }
}

#[cfg(feature = "io_uring")]
impl VirtualDisk for IoUringDisk {
    fn submit_write(&mut self, entry: LogEntry) -> io::Result<WriteToken> {
        let index = self
            .writer
            .submit_write(&entry.payload, entry.timestamp_ns)?;
        let token = WriteToken::from_index(index);
        self.highest_submitted = Some(token);
        Ok(token)
    }

    fn submit_write_batch(&mut self, entries: &[LogEntry]) -> io::Result<WriteToken> {
        if entries.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Empty batch"));
        }

        let payloads: Vec<Vec<u8>> = entries.iter().map(|e| e.payload.clone()).collect();
        let timestamp_ns = entries.first().map(|e| e.timestamp_ns).unwrap_or(0);

        let last_index = self.writer.submit_write_batch(&payloads, timestamp_ns)?;
        let token = WriteToken::from_index(last_index);
        self.highest_submitted = Some(token);
        Ok(token)
    }

    fn barrier(&mut self, up_to: WriteToken) -> Result<BarrierResult, FatalError> {
        let durable_index = self.writer.flush().map_err(FatalError::IoError)?;

        if durable_index >= up_to.index() {
            Ok(BarrierResult {
                durable_index,
                entries_synced: durable_index
                    .saturating_sub(self.writer.committed_index().unwrap_or(0)),
            })
        } else {
            Err(FatalError::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Barrier requested for {} but only {} became durable",
                    up_to.index(),
                    durable_index
                ),
            )))
        }
    }

    fn barrier_all(&mut self) -> Result<BarrierResult, FatalError> {
        let durable_index = self.writer.flush().map_err(FatalError::IoError)?;
        Ok(BarrierResult {
            durable_index,
            entries_synced: 0, // We don't track this precisely
        })
    }

    fn committed_index(&self) -> Option<u64> {
        self.writer.committed_index()
    }

    fn next_index(&self) -> u64 {
        self.writer.next_index()
    }

    fn tail_hash(&self) -> [u8; 16] {
        self.writer.tail_hash()
    }

    fn view_id(&self) -> u64 {
        self.writer.view_id()
    }

    fn set_view_id(&mut self, view_id: u64) {
        self.writer.set_view_id(view_id)
    }

    fn write_offset(&self) -> u64 {
        0 // TODO: expose from IoUringWriter
    }

    fn transfer_ownership(&mut self) {
        // TODO: add to IoUringWriter
    }

    fn truncate(&mut self, _len: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringWriter does not support truncate",
        ))
    }
}

#[cfg(all(test, feature = "io_uring"))]
mod io_uring_tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_io_uring_disk_single_write() {
        let path = Path::new("/tmp/chr_test_io_uring_disk_single.log");
        let _ = fs::remove_file(path);

        let mut disk = IoUringDisk::create(path, 1).unwrap();

        let entry = LogEntry::new(b"hello world".to_vec(), 1_000_000_000);
        let token = disk.submit_write(entry).unwrap();

        assert_eq!(token.index(), 0);
        // Not durable yet
        assert_eq!(disk.committed_index(), None);

        // Barrier makes it durable
        let result = disk.barrier(token).unwrap();
        assert_eq!(result.durable_index, 0);
        assert_eq!(disk.committed_index(), Some(0));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_io_uring_disk_batch_write() {
        let path = Path::new("/tmp/chr_test_io_uring_disk_batch.log");
        let _ = fs::remove_file(path);

        let mut disk = IoUringDisk::create(path, 1).unwrap();

        let entries: Vec<LogEntry> = (0..10)
            .map(|i| LogEntry::new(format!("entry_{}", i).into_bytes(), 1_000_000_000))
            .collect();

        let token = disk.submit_write_batch(&entries).unwrap();

        assert_eq!(token.index(), 9);
        // Not durable yet
        assert_eq!(disk.committed_index(), None);

        // Barrier makes it durable
        let result = disk.barrier_all().unwrap();
        assert_eq!(result.durable_index, 9);
        assert_eq!(disk.committed_index(), Some(9));

        let _ = fs::remove_file(path);
    }
}
