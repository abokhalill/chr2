//! VirtualDisk: Unified durability abstraction for Chronon.
//!
//! This module defines the core durability contract that all storage backends must implement.
//! The key insight is that durability is a **barrier operation**, not an implicit property of writes.
//!
//! # Design Principles
//!
//! 1. **Explicit Durability**: Writes are speculative until a barrier is called.
//! 2. **Uniform Semantics**: Whether using O_DSYNC, fdatasync, or io_uring fsync,
//!    the API contract is identical.
//! 3. **Single Authority**: Only `barrier()` establishes durability; `committed_index`
//!    is updated only from barrier completion.
//!
//! # Comparison to TigerBeetle/FoundationDB
//!
//! - TigerBeetle: Makes durability barriers explicit and uniform across all IO paths.
//! - FoundationDB: Storage layer has explicit commit boundaries; no mixing of
//!   "implicit durability" with "explicit commit".
//!
//! This abstraction eliminates the previous contradiction where:
//! - `LogWriter` relied on O_DSYNC (durability-by-open-flag)
//! - `IoUringWriter` relied on explicit fsync (durability-by-barrier)
//! - `Manifest` used fdatasync + rename (durability-by-protocol)

use std::io;

use crate::engine::errors::FatalError;

/// Token representing a submitted but not-yet-durable write.
///
/// Tokens are ordered: a barrier on token T makes all writes with tokens <= T durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteToken(pub u64);

impl WriteToken {
    /// Create a new write token from a log index.
    #[inline]
    pub fn from_index(index: u64) -> Self {
        WriteToken(index)
    }

    /// Get the underlying index value.
    #[inline]
    pub fn index(&self) -> u64 {
        self.0
    }
}

/// Result of a durability barrier operation.
#[derive(Debug, Clone, Copy)]
pub struct BarrierResult {
    /// The highest index that is now durable.
    pub durable_index: u64,
    /// Number of entries made durable by this barrier.
    pub entries_synced: u64,
}

/// Completion notification for async disk operations.
#[derive(Debug, Clone)]
pub enum DiskCompletion {
    /// A write has completed (but is not yet durable).
    WriteComplete { token: WriteToken },
    /// A barrier has completed, making writes durable.
    BarrierComplete { result: BarrierResult },
    /// An error occurred.
    Error { token: Option<WriteToken>, error: String },
}

/// Entry to be written to the log.
///
/// This struct owns the payload data and metadata needed to construct a log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// Stream ID for multi-tenancy routing.
    pub stream_id: u64,
    /// Entry flags.
    pub flags: u16,
    /// Consensus timestamp (nanoseconds since epoch).
    pub timestamp_ns: u64,
}

impl LogEntry {
    /// Create a new log entry with default stream_id and flags.
    pub fn new(payload: Vec<u8>, timestamp_ns: u64) -> Self {
        Self {
            payload,
            stream_id: 0,
            flags: 0,
            timestamp_ns,
        }
    }

    /// Create a new log entry with full metadata.
    pub fn with_metadata(payload: Vec<u8>, stream_id: u64, flags: u16, timestamp_ns: u64) -> Self {
        Self {
            payload,
            stream_id,
            flags,
            timestamp_ns,
        }
    }
}

/// The core durability abstraction for Chronon's storage layer.
///
/// # Contract
///
/// 1. **Ordering**: Writes are ordered by their returned `WriteToken`.
/// 2. **Speculation**: Writes are speculative until a barrier completes.
/// 3. **Barrier Semantics**: `barrier(token)` makes all writes with tokens <= `token` durable.
/// 4. **Atomicity**: Each write is atomic (all-or-nothing at the entry level).
/// 5. **Visibility**: `committed_index()` returns only indices that have passed a barrier.
///
/// # Implementations
///
/// - `SyncDisk`: Uses pwritev + O_DSYNC or fdatasync. Barrier is implicit or explicit.
/// - `IoUringDisk`: Uses io_uring Write + Fsync. True async submission/completion.
///
/// # Thread Safety
///
/// Implementations enforce single-writer semantics. Only one thread may call
/// write/barrier methods. `committed_index()` may be called from any thread.
pub trait VirtualDisk: Send {
    /// Submit a single entry for writing.
    ///
    /// The write is speculative until a barrier is called.
    /// Returns a token that can be used to establish durability.
    ///
    /// # Errors
    /// Returns an error if the write cannot be submitted (e.g., payload too large).
    fn submit_write(&mut self, entry: LogEntry) -> io::Result<WriteToken>;

    /// Submit a batch of entries for writing.
    ///
    /// All entries are written with a single syscall where possible.
    /// Returns the token for the last entry in the batch.
    ///
    /// # Errors
    /// Returns an error if any entry cannot be submitted.
    fn submit_write_batch(&mut self, entries: &[LogEntry]) -> io::Result<WriteToken>;

    /// Establish durability for all writes up to and including `up_to`.
    ///
    /// This is the **only** operation that makes writes durable.
    /// After this returns successfully, all entries with tokens <= `up_to` are
    /// guaranteed to survive a crash.
    ///
    /// # Semantics
    ///
    /// - Blocking implementations: This call blocks until durability is established.
    /// - Async implementations: This submits the barrier and waits for completion.
    ///
    /// # Errors
    /// Returns an error if durability cannot be established. This is a **fatal** error;
    /// the caller should halt or retry with extreme caution.
    fn barrier(&mut self, up_to: WriteToken) -> Result<BarrierResult, FatalError>;

    /// Establish durability for all pending writes.
    ///
    /// Convenience method equivalent to `barrier(highest_pending_token)`.
    fn barrier_all(&mut self) -> Result<BarrierResult, FatalError>;

    /// Get the highest durable index.
    ///
    /// This value is updated **only** after a successful barrier.
    /// Uses Acquire ordering for cross-thread visibility.
    ///
    /// Returns `None` if no entries have been made durable yet.
    fn committed_index(&self) -> Option<u64>;

    /// Get the next index that will be assigned.
    ///
    /// This is speculative and may be ahead of `committed_index()`.
    fn next_index(&self) -> u64;

    /// Get the current tail hash for chain continuity.
    fn tail_hash(&self) -> [u8; 16];

    /// Get the current view ID.
    fn view_id(&self) -> u64;

    /// Set the view ID for new entries.
    fn set_view_id(&mut self, view_id: u64);

    /// Get the current write offset in the file.
    fn write_offset(&self) -> u64;

    /// Transfer ownership to the current thread.
    ///
    /// Used when moving the disk to a dedicated worker thread.
    fn transfer_ownership(&mut self);

    /// Truncate the log to the specified length.
    ///
    /// Used by recovery to remove torn writes.
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

/// Synchronous disk implementation using pwritev + O_DSYNC.
///
/// This wraps `LogWriter` and expresses its durability semantics through the
/// `VirtualDisk` contract. With O_DSYNC, each pwritev is implicitly a barrier,
/// but we still expose the barrier API for uniformity.
///
/// # Durability Model
///
/// - `submit_write`: Performs pwritev with O_DSYNC. Write is durable on return.
/// - `barrier`: No-op for O_DSYNC (already durable), but updates committed_index.
///
/// This makes the "O_DSYNC means durable" assumption explicit in the type system.
pub struct SyncDisk {
    /// The underlying log writer.
    writer: crate::engine::log::LogWriter,
    /// Highest token submitted (may be ahead of committed if not using O_DSYNC).
    highest_submitted: Option<WriteToken>,
    /// Whether O_DSYNC is in use (barrier is implicit).
    o_dsync_enabled: bool,
}

impl SyncDisk {
    /// Create a new SyncDisk wrapping an existing LogWriter.
    ///
    /// Assumes O_DSYNC is enabled (the default for LogWriter).
    pub fn new(writer: crate::engine::log::LogWriter) -> Self {
        Self {
            writer,
            highest_submitted: None,
            o_dsync_enabled: true,
        }
    }

    /// Create a SyncDisk with explicit O_DSYNC flag.
    ///
    /// If `o_dsync` is false, explicit barriers are required.
    pub fn with_dsync_flag(writer: crate::engine::log::LogWriter, o_dsync: bool) -> Self {
        Self {
            writer,
            highest_submitted: None,
            o_dsync_enabled: o_dsync,
        }
    }

    /// Get a reference to the underlying LogWriter.
    pub fn writer(&self) -> &crate::engine::log::LogWriter {
        &self.writer
    }

    /// Get a mutable reference to the underlying LogWriter.
    pub fn writer_mut(&mut self) -> &mut crate::engine::log::LogWriter {
        &mut self.writer
    }

    /// Consume self and return the underlying LogWriter.
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Empty batch",
            ));
        }

        // Convert to the format expected by LogWriter
        let entries_with_meta: Vec<(Vec<u8>, u64, u16)> = entries
            .iter()
            .map(|e| (e.payload.clone(), e.stream_id, e.flags))
            .collect();

        let timestamp_ns = entries.first().map(|e| e.timestamp_ns).unwrap_or(0);

        let last_index = self.writer
            .append_batch_with_metadata(&entries_with_meta, timestamp_ns)
            .map_err(|e| match e {
                FatalError::IoError(io_err) => io_err,
                FatalError::PayloadTooLarge { size, max } => {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Payload size {} exceeds max {}", size, max),
                    )
                }
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
                        format!("Barrier requested for {} but no entries committed", up_to.index()),
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
    use std::path::Path;
    use std::fs;

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

// =============================================================================
// IoUringDisk: True async durability via io_uring
// =============================================================================

/// io_uring-based disk implementation with explicit barrier semantics.
///
/// Unlike `SyncDisk`, writes are truly asynchronous:
/// - `submit_write` queues a write to the io_uring ring (non-blocking)
/// - `barrier` submits an fsync and waits for completion
///
/// # Durability Model
///
/// - `submit_write`: Submits write to io_uring. NOT durable until barrier.
/// - `barrier`: Submits fsync with IO_DRAIN, waits for completion.
///
/// This is the "true async" model that TigerBeetle uses.
#[cfg(feature = "io_uring")]
pub struct IoUringDisk {
    /// The underlying io_uring writer.
    writer: crate::engine::uring::IoUringWriter,
    /// Highest token submitted (for barrier_all).
    highest_submitted: Option<WriteToken>,
}

#[cfg(feature = "io_uring")]
impl IoUringDisk {
    /// Create a new IoUringDisk wrapping an existing IoUringWriter.
    pub fn new(writer: crate::engine::uring::IoUringWriter) -> Self {
        Self {
            writer,
            highest_submitted: None,
        }
    }

    /// Create a new IoUringDisk from a path.
    pub fn create(path: &std::path::Path, view_id: u64) -> io::Result<Self> {
        let writer = crate::engine::uring::IoUringWriter::create(path, view_id, None)?;
        Ok(Self::new(writer))
    }

    /// Create a new IoUringDisk with O_DIRECT.
    pub fn create_direct(path: &std::path::Path, view_id: u64) -> io::Result<Self> {
        let writer = crate::engine::uring::IoUringWriter::create_direct(path, view_id, None)?;
        Ok(Self::new(writer))
    }

    /// Get a reference to the underlying IoUringWriter.
    pub fn writer(&self) -> &crate::engine::uring::IoUringWriter {
        &self.writer
    }

    /// Get a mutable reference to the underlying IoUringWriter.
    pub fn writer_mut(&mut self) -> &mut crate::engine::uring::IoUringWriter {
        &mut self.writer
    }

    /// Consume self and return the underlying IoUringWriter.
    pub fn into_inner(self) -> crate::engine::uring::IoUringWriter {
        self.writer
    }

    /// Check if using O_DIRECT mode.
    pub fn is_direct_io(&self) -> bool {
        self.writer.is_direct_io()
    }
}

#[cfg(feature = "io_uring")]
impl VirtualDisk for IoUringDisk {
    fn submit_write(&mut self, entry: LogEntry) -> io::Result<WriteToken> {
        let index = self.writer.submit_write(&entry.payload, entry.timestamp_ns)?;
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
        // Flush ensures all writes up to the highest pending are durable
        let durable_index = self.writer.flush().map_err(FatalError::IoError)?;

        if durable_index >= up_to.index() {
            Ok(BarrierResult {
                durable_index,
                entries_synced: durable_index.saturating_sub(
                    self.writer.committed_index().unwrap_or(0)
                ),
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
        // IoUringWriter doesn't expose write_offset directly
        // This is a limitation we need to address
        0 // Placeholder - would need to add this to IoUringWriter
    }

    fn transfer_ownership(&mut self) {
        // IoUringWriter doesn't have transfer_ownership
        // This is a limitation we need to address
    }

    fn truncate(&mut self, _len: u64) -> io::Result<()> {
        // IoUringWriter doesn't have truncate
        // This is a limitation we need to address
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IoUringWriter does not support truncate",
        ))
    }
}

#[cfg(all(test, feature = "io_uring"))]
mod io_uring_tests {
    use super::*;
    use std::path::Path;
    use std::fs;

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
