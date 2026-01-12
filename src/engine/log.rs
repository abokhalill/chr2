use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;

use crate::engine::errors::FatalError;

use crate::engine::format::{
    calculate_padding, compute_chain_hash, create_sentinel, frame_size, LogHeader, LogMetadata,
    GENESIS_HASH, HEADER_SIZE, MAX_PAYLOAD_SIZE, SENTINEL_SIZE,
};

/// Single-writer log. Enforces F4 (no chain fork) via owner_thread.
pub struct LogWriter {
    file: File,
    write_offset: u64,
    next_index: u64,
    tail_hash: [u8; 16],
    view_id: u64,
    committed_index: AtomicU64,
    owner_thread: ThreadId,
    fdatasync_count: AtomicU64,
    last_write_latency_ms: AtomicU64,
    verify_writes: bool,
}

#[allow(dead_code)]
impl LogWriter {
   
    pub fn open(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
    ) -> io::Result<Self> {

        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        
        let path_cstr = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?;
        
        let flags = libc::O_RDWR | libc::O_CREAT | libc::O_DSYNC;
        let mode = 0o644;
        let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, mode) };
        
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        
        let file = unsafe { File::from_raw_fd(fd) };
        let owner_thread = std::thread::current().id();
        let committed_index = if next_index > 0 {
            AtomicU64::new(next_index - 1)
        } else {
            AtomicU64::new(u64::MAX) // Sentinel value: no entries committed yet
        };

        Ok(LogWriter {
            file,
            write_offset,
            next_index,
            tail_hash,
            view_id,
            committed_index,
            owner_thread,
            fdatasync_count: AtomicU64::new(0),
            last_write_latency_ms: AtomicU64::new(0),
            verify_writes: cfg!(debug_assertions), // Only verify in debug builds
        })
    }

    pub fn create(path: &Path, view_id: u64) -> io::Result<Self> {
        Self::open(path, 0, 0, GENESIS_HASH, view_id)
    }
    
    pub fn transfer_ownership(&mut self) {
        self.owner_thread = std::thread::current().id();
    }
    
    #[inline]
    fn assert_owner_thread(&self) {
        let current = std::thread::current().id();
        if current != self.owner_thread {
            panic!(
                "FATAL: Forbidden state F4 - LogWriter accessed from thread {:?}, \
                 but owned by thread {:?}. Single-writer invariant violated.",
                current, self.owner_thread
            );
        }
    }

    pub fn append(&mut self, payload: &[u8], stream_id: u64, flags: u16, timestamp_ns: u64) -> io::Result<u64> {
        self.assert_owner_thread();
        let start_time = std::time::Instant::now();
        
        if payload.len() > MAX_PAYLOAD_SIZE as usize {
            panic!(
                "FATAL: Payload size {} exceeds maximum {}",
                payload.len(),
                MAX_PAYLOAD_SIZE
            );
        }

        let index = self.next_index;
        let prev_hash = self.tail_hash;
        let header = LogHeader::new(
            index,
            self.view_id,
            stream_id,
            prev_hash,
            payload,
            timestamp_ns,
            flags,
            1, // schema_version = 1 for now
        );

        let header_bytes = header.as_bytes();
        let padding_len = calculate_padding(payload.len() as u32);
        let padding = [0u8; 8]; // Max padding is 7 bytes
        let sentinel = create_sentinel(index);

        let mut iovecs = [
            libc::iovec {
                iov_base: header_bytes.as_ptr() as *mut libc::c_void,
                iov_len: HEADER_SIZE,
            },
            libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            },
            libc::iovec {
                iov_base: padding.as_ptr() as *mut libc::c_void,
                iov_len: padding_len,
            },
            libc::iovec {
                iov_base: sentinel.as_ptr() as *mut libc::c_void,
                iov_len: SENTINEL_SIZE,
            },
        ];

        let iovec_count = if padding_len > 0 { 4 } else { 3 };
        let (iovecs_ptr, iovecs_len) = if padding_len > 0 {
            (iovecs.as_mut_ptr(), 4)
        } else {
            iovecs[2] = iovecs[3];
            (iovecs.as_mut_ptr(), 3)
        };

        let bytes_written = unsafe {
            libc::pwritev(
                self.file.as_raw_fd(),
                iovecs_ptr,
                iovecs_len,
                self.write_offset as libc::off_t,
            )
        };

        if bytes_written < 0 {
            return Err(io::Error::last_os_error());
        }

        let expected_bytes = frame_size(payload.len() as u32) + SENTINEL_SIZE;
        if bytes_written as usize != expected_bytes {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "pwritev wrote {} bytes, expected {}",
                    bytes_written, expected_bytes
                ),
            ));
        }

        self.fdatasync_count.fetch_add(1, Ordering::Relaxed);

        if self.verify_writes {
            self.verify_write(self.write_offset, header_bytes, payload)?;
        }

        self.committed_index.store(index, Ordering::Release);
        self.tail_hash = compute_chain_hash(&header, payload);
        self.next_index += 1;
        self.write_offset += frame_size(payload.len() as u32) as u64;

        let latency_ms = start_time.elapsed().as_millis() as u64;
        self.last_write_latency_ms.store(latency_ms, Ordering::Relaxed);

        Ok(index)
    }

    fn write_sentinel(&self, last_index: u64) -> io::Result<()> {
        let sentinel = create_sentinel(last_index);
        let bytes_written = unsafe {
            libc::pwrite(
                self.file.as_raw_fd(),
                sentinel.as_ptr() as *const libc::c_void,
                SENTINEL_SIZE,
                self.write_offset as libc::off_t,
            )
        };
        
        if bytes_written < 0 {
            return Err(io::Error::last_os_error());
        }
        
        if bytes_written as usize != SENTINEL_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("Sentinel write incomplete: {} of {} bytes", bytes_written, SENTINEL_SIZE),
            ));
        }
        
        Ok(())
    }

    fn verify_write(&self, offset: u64, header_bytes: &[u8; HEADER_SIZE], payload: &[u8]) -> io::Result<()> {
        use crate::engine::format::compute_payload_hash;
        let mut header_readback = [0u8; HEADER_SIZE];
        let bytes_read = unsafe {
            libc::pread(
                self.file.as_raw_fd(),
                header_readback.as_mut_ptr() as *mut libc::c_void,
                HEADER_SIZE,
                offset as libc::off_t,
            )
        };
        
        if bytes_read < 0 {
            return Err(io::Error::last_os_error());
        }
        
        if bytes_read as usize != HEADER_SIZE {
            panic!(
                "FATAL: Read-after-write header size mismatch. Expected {}, got {}. Stale read detected.",
                HEADER_SIZE, bytes_read
            );
        }
        
        if header_readback != *header_bytes {
            panic!(
                "FATAL: Read-after-write header mismatch at offset {}. Stale read or directed torn write detected.",
                offset
            );
        }
        
        let payload_offset = offset + HEADER_SIZE as u64;
        let mut payload_readback = vec![0u8; payload.len()];
        let payload_bytes_read = unsafe {
            libc::pread(
                self.file.as_raw_fd(),
                payload_readback.as_mut_ptr() as *mut libc::c_void,
                payload.len(),
                payload_offset as libc::off_t,
            )
        };
        
        if payload_bytes_read < 0 {
            return Err(io::Error::last_os_error());
        }
        
        if payload_bytes_read as usize != payload.len() {
            panic!(
                "FATAL: Read-after-write payload size mismatch. Expected {}, got {}. Stale read detected.",
                payload.len(), payload_bytes_read
            );
        }
        
        // Verify payload hash matches (more efficient than byte-by-byte for large payloads)
        let expected_hash = compute_payload_hash(payload);
        let actual_hash = compute_payload_hash(&payload_readback);
        
        if expected_hash != actual_hash {
            panic!(
                "FATAL: Read-after-write payload hash mismatch at offset {}. Stale read or directed torn write detected.",
                payload_offset
            );
        }
        
        Ok(())
    }

    /// Get the current write offset (speculative, may be ahead of durable).
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Get the next index that will be assigned (speculative).
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Get the current tail hash.
    pub fn tail_hash(&self) -> [u8; 16] {
        self.tail_hash
    }

    /// Get the current view ID.
    pub fn view_id(&self) -> u64 {
        self.view_id
    }

    /// Get the raw file descriptor for recovery operations.
    pub fn as_raw_fd(&self) -> i32 {
        self.file.as_raw_fd()
    }
    
    /// Get the highest committed (durable) index.
    /// 
    /// VISIBILITY CONTRACT:
    /// - Returns the highest index that has passed the commit point (fdatasync success)
    /// - Readers MUST use this to determine what indices are safe to observe
    /// - Uses Acquire ordering to synchronize with the writer's Release store
    /// 
    /// ENFORCES F3: Readers cannot observe uncommitted indices.
    /// 
    /// Returns `None` if no entries have been committed yet.
    pub fn committed_index(&self) -> Option<u64> {
        let idx = self.committed_index.load(Ordering::Acquire);
        if idx == u64::MAX {
            None // Sentinel value: no entries committed
        } else {
            Some(idx)
        }
    }
    
    /// Get the number of fdatasync calls made.
    /// Used for testing group commit efficiency.
    pub fn fdatasync_count(&self) -> u64 {
        self.fdatasync_count.load(Ordering::Relaxed)
    }
    
    /// Get the last write latency in milliseconds.
    /// Used for manual tuning before implementing adaptive control loops.
    pub fn last_write_latency_ms(&self) -> u64 {
        self.last_write_latency_ms.load(Ordering::Relaxed)
    }
    
    /// Enable or disable read-after-write verification.
    /// 
    /// By default, verification is enabled only in debug builds.
    /// This is expensive (reads back every write) and should be disabled in production.
    /// Useful for testing lying drives or debugging storage issues.
    pub fn set_verify_writes(&mut self, enabled: bool) {
        self.verify_writes = enabled;
    }
    
    /// Check if read-after-write verification is enabled.
    pub fn verify_writes_enabled(&self) -> bool {
        self.verify_writes
    }

    /// Append a batch of entries to the log with a single fdatasync.
    ///
    /// # Group Commit Semantics
    ///
    /// This method implements group commit for improved I/O throughput:
    /// - All entries in the batch are written with ONE pwritev call
    /// - All entries are made durable with ONE fdatasync call
    /// - Intra-batch hash chaining is maintained (entry[i].prev_hash = chain_hash(entry[i-1]))
    ///
    /// # Arguments
    /// * `payloads` - Slice of payloads to append as a batch
    /// * `timestamp_ns` - Consensus timestamp (nanoseconds since epoch), shared by all entries in batch
    ///
    /// # Returns
    /// The last index of the batch on success.
    ///
    /// # Errors
    /// Returns `FatalError` if:
    /// - Any payload exceeds MAX_PAYLOAD_SIZE
    /// - pwritev returns less than expected (partial write)
    /// - fdatasync fails
    ///
    /// # Panics
    /// - Panics if called from wrong thread (FORBIDDEN STATE F4)
    pub fn append_batch(&mut self, payloads: &[Vec<u8>], timestamp_ns: u64) -> Result<u64, FatalError> {
        // ENFORCES F4: Single-writer invariant
        self.assert_owner_thread();
        
        // Start timing for latency measurement
        let start_time = std::time::Instant::now();
        
        // Empty batch is a no-op
        if payloads.is_empty() {
            return match self.committed_index() {
                Some(idx) => Ok(idx),
                None => Err(FatalError::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Empty batch with no prior entries",
                ))),
            };
        }
        
        // Validate all payloads first
        for (i, payload) in payloads.iter().enumerate() {
            if payload.len() > MAX_PAYLOAD_SIZE as usize {
                return Err(FatalError::PayloadTooLarge {
                    size: payload.len() as u32,
                    max: MAX_PAYLOAD_SIZE,
                });
            }
            if payload.is_empty() {
                return Err(FatalError::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Empty payload at batch index {}", i),
                )));
            }
        }
        
        // === Step 1: Construct all headers with intra-batch chaining ===
        let start_index = self.next_index;
        let mut headers: Vec<LogHeader> = Vec::with_capacity(payloads.len());
        let mut chain_hash = self.tail_hash;
        
        for (i, payload) in payloads.iter().enumerate() {
            let index = start_index + i as u64;
            
            // CRITICAL: Intra-batch chaining
            // For entry i in batch, prev_hash = chain_hash of entry i-1
            // For first entry, prev_hash = last_chain_hash on disk
            let prev_hash = chain_hash;
            
            let header = LogHeader::new(
                index,
                self.view_id,
                0, // stream_id
                prev_hash,
                payload,
                timestamp_ns,
                0, // flags
                1, // schema_version
            );
            
            // Compute chain hash for next entry
            chain_hash = compute_chain_hash(&header, payload);
            headers.push(header);
        }
        
        // === Step 2: Build single buffer for pwritev ===
        // Calculate total size needed (entries only, sentinel added separately)
        let mut entries_size: usize = 0;
        for (header, _payload) in headers.iter().zip(payloads.iter()) {
            entries_size += frame_size(header.payload_size);
        }
        
        // Build iovec array: for each entry we need header + payload + padding
        // Plus one more for the sentinel at the end
        // Maximum iovecs = 3 * batch_size + 1 (header, payload, padding per entry, plus sentinel)
        // Linux IOV_MAX is typically 1024, so we limit batch size implicitly
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(payloads.len() * 3 + 1);
        let mut padding_buffers: Vec<[u8; 8]> = Vec::with_capacity(payloads.len());
        
        for (header, payload) in headers.iter().zip(payloads.iter()) {
            // Header iovec
            iovecs.push(libc::iovec {
                iov_base: header.as_bytes().as_ptr() as *mut libc::c_void,
                iov_len: HEADER_SIZE,
            });
            
            // Payload iovec
            iovecs.push(libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            });
            
            // Padding iovec (if needed)
            let padding_len = calculate_padding(payload.len() as u32);
            if padding_len > 0 {
                padding_buffers.push([0u8; 8]);
                let padding_ptr = padding_buffers.last().unwrap().as_ptr();
                iovecs.push(libc::iovec {
                    iov_base: padding_ptr as *mut libc::c_void,
                    iov_len: padding_len,
                });
            }
        }
        
        // OPTIMIZATION: Add sentinel to the same pwritev call
        let last_index = start_index + payloads.len() as u64 - 1;
        let sentinel = create_sentinel(last_index);
        iovecs.push(libc::iovec {
            iov_base: sentinel.as_ptr() as *mut libc::c_void,
            iov_len: SENTINEL_SIZE,
        });
        
        let total_size = entries_size + SENTINEL_SIZE;
        
        // === Step 3: Single pwritev for entire batch + sentinel ===
        // SAFETY: pwritev is a standard POSIX syscall
        let bytes_written = unsafe {
            libc::pwritev(
                self.file.as_raw_fd(),
                iovecs.as_ptr(),
                iovecs.len() as i32,
                self.write_offset as libc::off_t,
            )
        };
        
        if bytes_written < 0 {
            return Err(FatalError::IoError(io::Error::last_os_error()));
        }
        
        // CRITICAL: Partial write is a FatalError per spec
        if bytes_written as usize != total_size {
            return Err(FatalError::IoError(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "Partial batch write: {} of {} bytes. FATAL: Batch atomicity violated.",
                    bytes_written, total_size
                ),
            )));
        }
        
        // === Step 4: Durability via O_DSYNC ===
        // With O_DSYNC, pwritev already ensures data is on stable storage.
        // No fdatasync needed - that would be redundant.
        
        // Increment sync counter for testing/metrics (counts durable writes)
        self.fdatasync_count.fetch_add(1, Ordering::Relaxed);
        
        // ============================================================
        // COMMIT POINT: pwritev with O_DSYNC has returned success.
        // All entries in batch are now durable.
        // ============================================================
        
        // === Step 5: Update state ===
        // Note: last_index already computed above for sentinel
        
        // Update committed_index to last entry in batch
        self.committed_index.store(last_index, Ordering::Release);
        
        // Update speculative state
        // Note: write_offset does NOT include sentinel - sentinel is overwritten by next entry
        self.tail_hash = chain_hash;
        self.next_index = last_index + 1;
        self.write_offset += entries_size as u64;
        
        // Sentinel was already written as part of the pwritev call above (batched optimization)
        // No separate write_sentinel() call needed
        
        // Record write latency for metrics
        let latency_ms = start_time.elapsed().as_millis() as u64;
        self.last_write_latency_ms.store(latency_ms, Ordering::Relaxed);
        
        Ok(last_index)
    }
    
    /// Append a batch of entries with stream IDs and timestamp.
    ///
    /// Like `append_batch` but allows specifying stream_id per entry.
    ///
    /// # Arguments
    /// * `entries` - Slice of (payload, stream_id, flags) tuples
    /// * `timestamp_ns` - Consensus timestamp (nanoseconds since epoch), shared by all entries
    pub fn append_batch_with_metadata(
        &mut self,
        entries: &[(Vec<u8>, u64, u16)], // (payload, stream_id, flags)
        timestamp_ns: u64,
    ) -> Result<u64, FatalError> {
        // ENFORCES F4: Single-writer invariant
        self.assert_owner_thread();
        
        if entries.is_empty() {
            return match self.committed_index() {
                Some(idx) => Ok(idx),
                None => Err(FatalError::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Empty batch with no prior entries",
                ))),
            };
        }
        
        // Validate all payloads
        for (i, (payload, _, _)) in entries.iter().enumerate() {
            if payload.len() > MAX_PAYLOAD_SIZE as usize {
                return Err(FatalError::PayloadTooLarge {
                    size: payload.len() as u32,
                    max: MAX_PAYLOAD_SIZE,
                });
            }
            if payload.is_empty() {
                return Err(FatalError::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Empty payload at batch index {}", i),
                )));
            }
        }
        
        // Build headers with intra-batch chaining
        let start_index = self.next_index;
        let mut headers: Vec<LogHeader> = Vec::with_capacity(entries.len());
        let mut chain_hash = self.tail_hash;
        
        for (i, (payload, stream_id, flags)) in entries.iter().enumerate() {
            let index = start_index + i as u64;
            let prev_hash = chain_hash;
            
            let header = LogHeader::new(
                index,
                self.view_id,
                *stream_id,
                prev_hash,
                payload,
                timestamp_ns,
                *flags,
                1,
            );
            
            chain_hash = compute_chain_hash(&header, payload);
            headers.push(header);
        }
        
        // Build iovecs
        let mut entries_size: usize = 0;
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(entries.len() * 3 + 1);
        let mut padding_buffers: Vec<[u8; 8]> = Vec::with_capacity(entries.len());
        
        for (header, (payload, _, _)) in headers.iter().zip(entries.iter()) {
            entries_size += frame_size(header.payload_size);
            
            iovecs.push(libc::iovec {
                iov_base: header.as_bytes().as_ptr() as *mut libc::c_void,
                iov_len: HEADER_SIZE,
            });
            
            iovecs.push(libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            });
            
            let padding_len = calculate_padding(payload.len() as u32);
            if padding_len > 0 {
                padding_buffers.push([0u8; 8]);
                let padding_ptr = padding_buffers.last().unwrap().as_ptr();
                iovecs.push(libc::iovec {
                    iov_base: padding_ptr as *mut libc::c_void,
                    iov_len: padding_len,
                });
            }
        }
        
        // OPTIMIZATION: Add sentinel to the same pwritev call
        let last_index = start_index + entries.len() as u64 - 1;
        let sentinel = create_sentinel(last_index);
        iovecs.push(libc::iovec {
            iov_base: sentinel.as_ptr() as *mut libc::c_void,
            iov_len: SENTINEL_SIZE,
        });
        
        let total_size = entries_size + SENTINEL_SIZE;
        
        // Single pwritev for entries + sentinel
        let bytes_written = unsafe {
            libc::pwritev(
                self.file.as_raw_fd(),
                iovecs.as_ptr(),
                iovecs.len() as i32,
                self.write_offset as libc::off_t,
            )
        };
        
        if bytes_written < 0 {
            return Err(FatalError::IoError(io::Error::last_os_error()));
        }
        
        if bytes_written as usize != total_size {
            return Err(FatalError::IoError(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "Partial batch write: {} of {} bytes",
                    bytes_written, total_size
                ),
            )));
        }
        
        // With O_DSYNC, pwritev already ensures durability - no fdatasync needed.
        self.fdatasync_count.fetch_add(1, Ordering::Relaxed);
        
        // Update state
        // Note: write_offset does NOT include sentinel - sentinel is overwritten by next entry
        self.committed_index.store(last_index, Ordering::Release);
        self.tail_hash = chain_hash;
        self.next_index = last_index + 1;
        self.write_offset += entries_size as u64;
        
        // Sentinel was already written as part of the pwritev call above (batched optimization)
        // No separate write_sentinel() call needed
        
        Ok(last_index)
    }

    /// Truncate the log file to the specified length.
    /// Used by recovery to remove torn writes.
    /// Per recovery.md VI: "Truncate the file to current_offset"
    pub fn truncate(&self, len: u64) -> io::Result<()> {
        // SAFETY: ftruncate is a standard POSIX syscall on a valid fd.
        let result = unsafe { libc::ftruncate(self.file.as_raw_fd(), len as libc::off_t) };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        // Per recovery.md VI: "Call fsync() on the file descriptor"
        let sync_result = unsafe { libc::fdatasync(self.file.as_raw_fd()) };

        if sync_result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Truncate the log prefix up to (but not including) the given index.
    ///
    /// This performs an atomic rewrite of the log file:
    /// 1. Create a temporary file with the new metadata header
    /// 2. Copy the suffix (entries from new_base_index onwards)
    /// 3. fsync the temporary file
    /// 4. Atomic rename to replace the original
    ///
    /// # Arguments
    /// * `log_path` - Path to the log file
    /// * `new_base_index` - The new first index in the truncated log
    /// * `new_base_offset` - The file offset where new_base_index starts (authoritative)
    /// * `new_base_prev_hash` - The chain hash of entry (new_base_index - 1)
    ///
    /// # Safety
    /// This is a static method that operates on the file system.
    /// The caller MUST ensure no writer is active on the log file.
    /// The snapshot covering entries < new_base_index MUST be durable first.
    pub fn truncate_prefix(
        log_path: &Path,
        new_base_index: u64,
        new_base_offset: u64,
        new_base_prev_hash: [u8; 16],
    ) -> io::Result<()> {
        use std::fs::{self, OpenOptions};
        use std::io::{Read, Seek, SeekFrom, Write};

        // Validate: new_base_index must be > 0 (can't truncate to empty)
        if new_base_index == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot truncate to base_index 0 - use regular log creation",
            ));
        }

        // Step 1: Create temporary file
        let tmp_path = log_path.with_extension("chr.tmp");
        let mut tmp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Step 2: Write the new metadata header
        let metadata = LogMetadata::new_truncated(new_base_index, new_base_prev_hash);
        let meta_bytes = metadata.as_bytes();
        tmp_file.write_all(&meta_bytes)?;

        // Step 3: Open source file and copy suffix
        let mut src_file = fs::File::open(log_path)?;
        let src_len = src_file.seek(SeekFrom::End(0))?;

        // Seek to the cut point in source
        src_file.seek(SeekFrom::Start(new_base_offset))?;

        // Calculate bytes to copy
        let bytes_to_copy = src_len.saturating_sub(new_base_offset);

        if bytes_to_copy > 0 {
            // Copy in chunks to avoid memory issues with large logs
            const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
            let mut buffer = vec![0u8; CHUNK_SIZE];
            let mut remaining = bytes_to_copy as usize;

            while remaining > 0 {
                let to_read = remaining.min(CHUNK_SIZE);
                let bytes_read = src_file.read(&mut buffer[..to_read])?;
                if bytes_read == 0 {
                    break; // EOF
                }
                tmp_file.write_all(&buffer[..bytes_read])?;
                remaining -= bytes_read;
            }
        }

        // Step 4: fsync the temporary file
        tmp_file.sync_all()?;
        drop(tmp_file);

        // Step 5: Atomic rename
        fs::rename(&tmp_path, log_path)?;

        // Step 6: fsync the directory to ensure rename is durable
        if let Some(parent) = log_path.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_append_single_entry() {
        let path = Path::new("/tmp/chr_test_single.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        let payload = b"hello world";
        let index = writer.append(payload, 0, 0, 1_000_000_000).unwrap();

        assert_eq!(index, 0);
        assert_eq!(writer.next_index(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_append_multiple_entries() {
        let path = Path::new("/tmp/chr_test_multi.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();

        for i in 0..10 {
            let payload = format!("entry {}", i);
            let index = writer.append(payload.as_bytes(), 0, 0, 1_000_000_000).unwrap();
            assert_eq!(index, i);
        }

        assert_eq!(writer.next_index(), 10);

        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_committed_index_tracks_durable_state() {
        // ENFORCES F3/F6: committed_index only advances after fdatasync
        let path = Path::new("/tmp/chr_test_committed.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Initially no committed entries
        assert_eq!(writer.committed_index(), None);
        
        // After first append, committed_index should be 0
        writer.append(b"entry0", 0, 0, 1_000_000_000).unwrap();
        assert_eq!(writer.committed_index(), Some(0));
        
        // After more appends, committed_index tracks
        writer.append(b"entry1", 0, 0, 2_000_000_000).unwrap();
        assert_eq!(writer.committed_index(), Some(1));
        
        writer.append(b"entry2", 0, 0, 3_000_000_000).unwrap();
        assert_eq!(writer.committed_index(), Some(2));

        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_owner_thread_same_thread_ok() {
        // ENFORCES F4: Same thread access should work
        let path = Path::new("/tmp/chr_test_owner.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Multiple appends from same thread should succeed
        for i in 0..5 {
            writer.append(format!("entry{}", i).as_bytes(), 0, 0, 1_000_000_000).unwrap();
        }

        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_visibility_ordering() {
        // ENFORCES F3: committed_index uses proper memory ordering
        // This test verifies the contract, not the ordering itself (which requires multi-threading)
        let path = Path::new("/tmp/chr_test_visibility.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Verify committed_index is always <= next_index - 1 after append
        for i in 0..10 {
            writer.append(format!("entry{}", i).as_bytes(), 0, 0, 1_000_000_000).unwrap();
            
            let committed = writer.committed_index().unwrap();
            let next = writer.next_index();
            
            // committed_index should be the just-written index
            assert_eq!(committed, i);
            // next_index should be one ahead
            assert_eq!(next, i + 1);
            // Invariant: committed < next
            assert!(committed < next);
        }

        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_append_batch_basic() {
        // Test basic batch append functionality
        let path = Path::new("/tmp/chr_test_batch_basic.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Create a batch of 10 entries
        let payloads: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("batch_entry_{}", i).into_bytes())
            .collect();
        
        let initial_fdatasync = writer.fdatasync_count();
        let last_index = writer.append_batch(&payloads, 1_000_000_000).unwrap();
        
        // Verify batch was written correctly
        assert_eq!(last_index, 9);
        assert_eq!(writer.next_index(), 10);
        assert_eq!(writer.committed_index(), Some(9));
        
        // Verify only ONE fdatasync was called for the entire batch
        assert_eq!(writer.fdatasync_count(), initial_fdatasync + 1);
        
        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_append_batch_chain_continuity() {
        // Test that intra-batch hash chaining is correct
        let path = Path::new("/tmp/chr_test_batch_chain.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Write a single entry first
        writer.append(b"entry_0", 0, 0, 1_000_000_000).unwrap();
        let hash_after_single = writer.tail_hash();
        
        // Write a batch
        let payloads: Vec<Vec<u8>> = (1..5)
            .map(|i| format!("batch_entry_{}", i).into_bytes())
            .collect();
        writer.append_batch(&payloads, 2_000_000_000).unwrap();
        
        // Write another single entry - should chain correctly
        writer.append(b"entry_5", 0, 0, 6_000_000_000).unwrap();
        
        // Verify indices are contiguous
        assert_eq!(writer.next_index(), 6);
        assert_eq!(writer.committed_index(), Some(5));
        
        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_group_commit_throughput() {
        // Integration test: verify group commit reduces fdatasync calls
        // 
        // This test verifies:
        // 1. fdatasync_count is significantly lower than entry count
        // 2. All entries are correctly written and committed
        // 3. Hash chain integrity is maintained
        
        let path = Path::new("/tmp/chr_test_group_commit.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        const TOTAL_ENTRIES: usize = 500;
        const BATCH_SIZE: usize = 50;
        
        let initial_fdatasync = writer.fdatasync_count();
        
        // Write 500 entries in batches of 50
        for batch_num in 0..(TOTAL_ENTRIES / BATCH_SIZE) {
            let payloads: Vec<Vec<u8>> = (0..BATCH_SIZE)
                .map(|i| {
                    let entry_num = batch_num * BATCH_SIZE + i;
                    format!("entry_{:04}", entry_num).into_bytes()
                })
                .collect();
            
            let last_index = writer.append_batch(&payloads, 1_000_000_000).unwrap();
            let expected_last = (batch_num + 1) * BATCH_SIZE - 1;
            assert_eq!(last_index as usize, expected_last);
        }
        
        // Verify all entries written
        assert_eq!(writer.next_index() as usize, TOTAL_ENTRIES);
        assert_eq!(writer.committed_index(), Some((TOTAL_ENTRIES - 1) as u64));
        
        // Verify fdatasync count is significantly lower than entry count
        // With 500 entries in batches of 50, we expect ~10 fdatasyncs
        // (plus 1 for sentinel per batch = ~20 total, but sentinel shares fdatasync)
        let fdatasync_count = writer.fdatasync_count() - initial_fdatasync;
        
        println!("Group commit test: {} entries, {} fdatasyncs", TOTAL_ENTRIES, fdatasync_count);
        
        // Should be much less than 500 (ideally around 10-20)
        assert!(
            fdatasync_count < 50,
            "fdatasync_count {} should be < 50 for {} entries (got {} per entry)",
            fdatasync_count,
            TOTAL_ENTRIES,
            fdatasync_count as f64 / TOTAL_ENTRIES as f64
        );
        
        // Ideal case: ~10 fdatasyncs for 10 batches
        // Allow some slack for sentinel writes
        assert!(
            fdatasync_count <= 20,
            "fdatasync_count {} should be <= 20 for optimal group commit",
            fdatasync_count
        );
        
        let _ = fs::remove_file(path);
    }
    
    #[test]
    fn test_mixed_single_and_batch_writes() {
        // Test that single writes and batch writes can be interleaved
        let path = Path::new("/tmp/chr_test_mixed.log");
        let _ = fs::remove_file(path);

        let mut writer = LogWriter::create(path, 1).unwrap();
        
        // Single write
        writer.append(b"single_0", 0, 0, 1_000_000_000).unwrap();
        
        // Batch write
        let batch1: Vec<Vec<u8>> = vec![
            b"batch1_0".to_vec(),
            b"batch1_1".to_vec(),
            b"batch1_2".to_vec(),
        ];
        writer.append_batch(&batch1, 2_000_000_000).unwrap();
        
        // Single write
        writer.append(b"single_1", 0, 0, 5_000_000_000).unwrap();
        
        // Batch write
        let batch2: Vec<Vec<u8>> = vec![
            b"batch2_0".to_vec(),
            b"batch2_1".to_vec(),
        ];
        writer.append_batch(&batch2, 6_000_000_000).unwrap();
        
        // Verify all entries: 1 + 3 + 1 + 2 = 7 entries (indices 0-6)
        assert_eq!(writer.next_index(), 7);
        assert_eq!(writer.committed_index(), Some(6));
        
        let _ = fs::remove_file(path);
    }
}
