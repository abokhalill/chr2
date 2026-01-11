#[cfg(feature = "io_uring")]
use io_uring::{opcode, types, IoUring, Probe};

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;

use crate::engine::errors::FatalError;
use crate::engine::format::{
    calculate_padding, compute_chain_hash, create_sentinel, frame_size, LogHeader, LogMetadata,
    GENESIS_HASH, HEADER_SIZE, LOG_METADATA_SIZE, MAX_PAYLOAD_SIZE, SENTINEL_SIZE,
};

pub const DEFAULT_QUEUE_DEPTH: u32 = 128;
pub const MAX_BATCH_WRITES: usize = 64;
const TAG_WRITE: u64 = 1;
const TAG_FSYNC: u64 = 2;
/// O_DIRECT requires buffer address and file offset to be aligned.
pub const DMA_ALIGNMENT: usize = 4096;
pub const DEFAULT_DMA_BUFFER_SIZE: usize = 64 * 1024;
pub const DEFAULT_DMA_POOL_SIZE: usize = 32;

/// 4KB-aligned buffer for O_DIRECT I/O.
#[cfg(feature = "io_uring")]
pub struct DmaBuffer {
    ptr: *mut u8,
    capacity: usize,
    len: usize,
    layout: std::alloc::Layout,
}

#[cfg(feature = "io_uring")]
impl DmaBuffer {
    pub fn new(min_capacity: usize) -> io::Result<Self> {
        let capacity = (min_capacity + DMA_ALIGNMENT - 1) & !(DMA_ALIGNMENT - 1);
        
        let layout = std::alloc::Layout::from_size_align(capacity, DMA_ALIGNMENT)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "DMA buffer allocation failed"));
        }
        
        Ok(DmaBuffer {
            ptr,
            capacity,
            len: 0,
            layout,
        })
    }
    
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }
    pub fn as_ptr(&self) -> *const u8 { self.ptr }
    pub fn as_mut_ptr(&mut self) -> *mut u8 { self.ptr }
    pub fn as_slice(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.ptr, self.len) } }
    pub fn as_mut_slice(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) } }
    pub fn set_len(&mut self, len: usize) { assert!(len <= self.capacity); self.len = len; }
    pub fn clear(&mut self) { self.len = 0; }
    
    pub fn write(&mut self, data: &[u8]) -> usize {
        let available = self.capacity - self.len;
        let to_write = data.len().min(available);
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.ptr.add(self.len),
                to_write,
            );
        }
        
        self.len += to_write;
        to_write
    }
    
    pub fn pad_to_alignment(&mut self) -> usize {
        let aligned_len = (self.len + DMA_ALIGNMENT - 1) & !(DMA_ALIGNMENT - 1);
        let padding = aligned_len - self.len;
        
        if padding > 0 && aligned_len <= self.capacity {
            unsafe {
                std::ptr::write_bytes(self.ptr.add(self.len), 0, padding);
            }
            self.len = aligned_len;
        }
        
        padding
    }
    
    pub fn aligned_len(&self) -> usize {
        (self.len + DMA_ALIGNMENT - 1) & !(DMA_ALIGNMENT - 1)
    }
}

#[cfg(feature = "io_uring")]
impl Drop for DmaBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                std::alloc::dealloc(self.ptr, self.layout);
            }
        }
    }
}

#[cfg(feature = "io_uring")]
unsafe impl Send for DmaBuffer {}

#[cfg(feature = "io_uring")]
pub struct DmaBufferPool {
    free: Vec<DmaBuffer>,
    default_size: usize,
    max_size: usize,
    total_allocations: u64,
    pool_hits: u64,
}

#[cfg(feature = "io_uring")]
impl DmaBufferPool {
    pub fn new(default_size: usize, max_size: usize) -> Self {
        DmaBufferPool {
            free: Vec::with_capacity(max_size),
            default_size,
            max_size,
            total_allocations: 0,
            pool_hits: 0,
        }
    }
    
    pub fn acquire(&mut self, min_size: usize) -> io::Result<DmaBuffer> {
        let required_size = min_size.max(self.default_size);
        if let Some(idx) = self.free.iter().position(|b| b.capacity() >= required_size) {
            self.pool_hits += 1;
            let mut buf = self.free.swap_remove(idx);
            buf.clear();
            return Ok(buf);
        }
        
        self.total_allocations += 1;
        DmaBuffer::new(required_size)
    }
    
    pub fn release(&mut self, mut buffer: DmaBuffer) {
        buffer.clear();
        if self.free.len() < self.max_size { self.free.push(buffer); }
    }
    
    pub fn stats(&self) -> (u64, u64, usize) {
        (self.total_allocations, self.pool_hits, self.free.len())
    }
}

#[derive(Debug)]
struct PendingWrite {
    index: u64,
    offset: u64,
    size: usize,
    completed: bool,
}

#[derive(Debug)]
struct PendingFsync {
    up_to_index: u64,
    completed: bool,
}

/// O_DIRECT: 4KB-aligned buffers, bypasses page cache. Non-O_DIRECT: buffered I/O with FSYNC.
#[cfg(feature = "io_uring")]
pub struct IoUringWriter {
    ring: IoUring,
    fd: RawFd,
    _file: File,
    write_offset: u64,
    next_index: u64,
    tail_hash: [u8; 16],
    view_id: u64,
    committed_index: AtomicU64,
    owner_thread: ThreadId,
    pending_writes: VecDeque<PendingWrite>,
    pending_fsyncs: VecDeque<PendingFsync>,
    writes_since_fsync: usize,
    fsync_count: AtomicU64,
    dma_pool: Option<DmaBufferPool>,
    pending_dma_buffers: VecDeque<DmaBuffer>,
    write_buffers: VecDeque<Vec<u8>>,
}

#[cfg(feature = "io_uring")]
impl IoUringWriter {
    pub fn open(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
        queue_depth: Option<u32>,
    ) -> io::Result<Self> {
        Self::open_with_options(path, next_index, write_offset, tail_hash, view_id, queue_depth, false)
    }
    
    /// O_DIRECT: write_offset MUST be 4KB-aligned.
    pub fn open_direct(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
        queue_depth: Option<u32>,
    ) -> io::Result<Self> {
        if write_offset % DMA_ALIGNMENT as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("write_offset {} is not {}-byte aligned for O_DIRECT", write_offset, DMA_ALIGNMENT),
            ));
        }
        Self::open_with_options(path, next_index, write_offset, tail_hash, view_id, queue_depth, true)
    }
    
    fn open_with_options(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
        queue_depth: Option<u32>,
        use_direct_io: bool,
    ) -> io::Result<Self> {
        let depth = queue_depth.unwrap_or(DEFAULT_QUEUE_DEPTH);
        let ring = IoUring::new(depth)?;
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        
        if !probe.is_supported(opcode::Write::CODE) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring WRITE not supported",
            ));
        }
        if !probe.is_supported(opcode::Fsync::CODE) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring FSYNC not supported",
            ));
        }
        
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        
        let path_cstr = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?;
        
        let mut flags = libc::O_RDWR | libc::O_CREAT;
        if use_direct_io {
            flags |= libc::O_DIRECT;
        }
        let mode = 0o644;
        
        let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, mode) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        
        let file = unsafe { File::from_raw_fd(fd) };
        let owner_thread = std::thread::current().id();
        
        let committed_index = if next_index > 0 { AtomicU64::new(next_index - 1) } else { AtomicU64::new(u64::MAX) };
        let dma_pool = if use_direct_io {
            Some(DmaBufferPool::new(DEFAULT_DMA_BUFFER_SIZE, DEFAULT_DMA_POOL_SIZE))
        } else {
            None
        };
        
        Ok(IoUringWriter {
            ring,
            fd,
            _file: file,
            write_offset,
            next_index,
            tail_hash,
            view_id,
            committed_index,
            owner_thread,
            pending_writes: VecDeque::new(),
            pending_fsyncs: VecDeque::new(),
            writes_since_fsync: 0,
            fsync_count: AtomicU64::new(0),
            dma_pool,
            pending_dma_buffers: VecDeque::new(),
            write_buffers: VecDeque::new(),
        })
    }
    
    pub fn is_direct_io(&self) -> bool { self.dma_pool.is_some() }
    pub fn create(path: &Path, view_id: u64, queue_depth: Option<u32>) -> io::Result<Self> { Self::open(path, 0, 0, GENESIS_HASH, view_id, queue_depth) }
    pub fn create_direct(path: &Path, view_id: u64, queue_depth: Option<u32>) -> io::Result<Self> { Self::open_direct(path, 0, 0, GENESIS_HASH, view_id, queue_depth) }
    pub fn dma_pool_stats(&self) -> Option<(u64, u64, usize)> { self.dma_pool.as_ref().map(|p| p.stats()) }
    
    /// NOT durable until flush() completes. O_DIRECT pads to 4KB.
    pub fn submit_write(&mut self, payload: &[u8], timestamp_ns: u64) -> io::Result<u64> {
        self.check_owner_thread()?;
        
        if payload.len() > MAX_PAYLOAD_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Payload size {} exceeds max {}", payload.len(), MAX_PAYLOAD_SIZE),
            ));
        }
        
        let index = self.next_index;
        let prev_hash = self.tail_hash;
        
        // Build the header
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
        
        // Serialize to buffer
        let header_bytes = header.as_bytes();
        let padding_size = calculate_padding(payload.len() as u32);
        // Include sentinel in the write for recovery compatibility
        let sentinel = create_sentinel(index);
        let logical_size = HEADER_SIZE + payload.len() + padding_size + SENTINEL_SIZE;
        
        let offset = self.write_offset;
        
        // Choose buffer strategy based on O_DIRECT mode
        if let Some(ref mut pool) = self.dma_pool {
            // O_DIRECT mode: use DMA-aligned buffer
            // Round up to 4KB alignment for O_DIRECT
            let aligned_size = (logical_size + DMA_ALIGNMENT - 1) & !(DMA_ALIGNMENT - 1);
            let mut dma_buf = pool.acquire(aligned_size)?;
            
            // Write header + payload + padding + sentinel
            dma_buf.write(header_bytes);
            dma_buf.write(payload);
            // Pad to 8-byte alignment
            if padding_size > 0 {
                let padding = [0u8; 8];
                dma_buf.write(&padding[..padding_size]);
            }
            // Write sentinel for recovery compatibility
            dma_buf.write(&sentinel);
            // Pad to 4KB alignment for O_DIRECT
            dma_buf.pad_to_alignment();
            
            let write_size = dma_buf.len();
            
            // Submit write to ring
            let write_op = opcode::Write::new(types::Fd(self.fd), dma_buf.as_ptr(), write_size as u32)
                .offset(offset)
                .build()
                .user_data(TAG_WRITE | (index << 8));
            
            unsafe {
                self.ring
                    .submission()
                    .push(&write_op)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            }
            
            // Track pending write (use aligned size for offset advancement)
            self.pending_writes.push_back(PendingWrite {
                index,
                offset,
                size: write_size,
                completed: false,
            });
            
            // Keep DMA buffer alive until completion
            self.pending_dma_buffers.push_back(dma_buf);
            
            // Update speculative state (advance by aligned size for O_DIRECT)
            self.tail_hash = compute_chain_hash(&header, payload);
            self.next_index += 1;
            self.write_offset += write_size as u64;
            self.writes_since_fsync += 1;
        } else {
            // Standard mode: use regular Vec buffer (includes sentinel)
            let mut buffer = vec![0u8; logical_size];
            buffer[..HEADER_SIZE].copy_from_slice(header_bytes);
            buffer[HEADER_SIZE..HEADER_SIZE + payload.len()].copy_from_slice(payload);
            // Padding is already zeroed, write sentinel at end
            let sentinel_offset = HEADER_SIZE + payload.len() + padding_size;
            buffer[sentinel_offset..sentinel_offset + SENTINEL_SIZE].copy_from_slice(&sentinel);
            
            // Submit write to ring
            let write_op = opcode::Write::new(types::Fd(self.fd), buffer.as_ptr(), logical_size as u32)
                .offset(offset)
                .build()
                .user_data(TAG_WRITE | (index << 8));
            
            unsafe {
                self.ring
                    .submission()
                    .push(&write_op)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
            }
            
            // Track pending write
            self.pending_writes.push_back(PendingWrite {
                index,
                offset,
                size: logical_size,
                completed: false,
            });
            
            // Keep buffer alive until completion
            self.write_buffers.push_back(buffer);
            
            // Update speculative state
            self.tail_hash = compute_chain_hash(&header, payload);
            self.next_index += 1;
            self.write_offset += logical_size as u64;
            self.writes_since_fsync += 1;
        }
        
        // Submit to kernel
        self.ring.submit()?;
        
        Ok(index)
    }
    
    /// Submit a batch of writes to the ring.
    ///
    /// All writes are submitted with a single syscall for efficiency.
    /// Returns the last index in the batch.
    ///
    /// In O_DIRECT mode, each entry is padded to 4KB alignment.
    pub fn submit_write_batch(&mut self, payloads: &[Vec<u8>], timestamp_ns: u64) -> io::Result<u64> {
        self.check_owner_thread()?;
        
        if payloads.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Empty batch"));
        }
        
        let start_index = self.next_index;
        let mut chain_hash = self.tail_hash;
        let use_dma = self.dma_pool.is_some();
        
        for (i, payload) in payloads.iter().enumerate() {
            if payload.len() > MAX_PAYLOAD_SIZE as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Payload {} size {} exceeds max", i, payload.len()),
                ));
            }
            
            let index = start_index + i as u64;
            
            let header = LogHeader::new(
                index,
                self.view_id,
                0,
                chain_hash,
                payload,
                timestamp_ns,
                0,
                1,
            );
            
            let header_bytes = header.as_bytes();
            let padding_size = calculate_padding(payload.len() as u32);
            // Include sentinel for recovery compatibility
            let sentinel = create_sentinel(index);
            let logical_size = HEADER_SIZE + payload.len() + padding_size + SENTINEL_SIZE;
            
            let offset = self.write_offset;
            
            if use_dma {
                // O_DIRECT mode: use DMA-aligned buffer
                let aligned_size = (logical_size + DMA_ALIGNMENT - 1) & !(DMA_ALIGNMENT - 1);
                let mut dma_buf = self.dma_pool.as_mut().unwrap().acquire(aligned_size)?;
                
                // Write header + payload + padding + sentinel
                dma_buf.write(header_bytes);
                dma_buf.write(payload);
                if padding_size > 0 {
                    let padding = [0u8; 8];
                    dma_buf.write(&padding[..padding_size]);
                }
                dma_buf.write(&sentinel);
                dma_buf.pad_to_alignment();
                
                let write_size = dma_buf.len();
                
                let write_op = opcode::Write::new(types::Fd(self.fd), dma_buf.as_ptr(), write_size as u32)
                    .offset(offset)
                    .build()
                    .user_data(TAG_WRITE | (index << 8));
                
                unsafe {
                    self.ring
                        .submission()
                        .push(&write_op)
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
                }
                
                self.pending_writes.push_back(PendingWrite {
                    index,
                    offset,
                    size: write_size,
                    completed: false,
                });
                
                self.pending_dma_buffers.push_back(dma_buf);
                self.write_offset += write_size as u64;
            } else {
                // Standard mode: use regular Vec buffer (includes sentinel)
                let mut buffer = vec![0u8; logical_size];
                buffer[..HEADER_SIZE].copy_from_slice(header_bytes);
                buffer[HEADER_SIZE..HEADER_SIZE + payload.len()].copy_from_slice(payload);
                // Write sentinel at end
                let sentinel_offset = HEADER_SIZE + payload.len() + padding_size;
                buffer[sentinel_offset..sentinel_offset + SENTINEL_SIZE].copy_from_slice(&sentinel);
                
                let write_op = opcode::Write::new(types::Fd(self.fd), buffer.as_ptr(), logical_size as u32)
                    .offset(offset)
                    .build()
                    .user_data(TAG_WRITE | (index << 8));
                
                unsafe {
                    self.ring
                        .submission()
                        .push(&write_op)
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
                }
                
                self.pending_writes.push_back(PendingWrite {
                    index,
                    offset,
                    size: logical_size,
                    completed: false,
                });
                
                self.write_buffers.push_back(buffer);
                self.write_offset += logical_size as u64;
            }
            
            chain_hash = compute_chain_hash(&header, payload);
        }
        
        self.tail_hash = chain_hash;
        let last_index = self.next_index + payloads.len() as u64 - 1;
        self.next_index += payloads.len() as u64;
        self.writes_since_fsync += payloads.len();
        
        // Single submit for entire batch
        self.ring.submit()?;
        
        Ok(last_index)
    }
    
    /// Submit an fsync and wait for all pending writes to complete.
    ///
    /// This is the durability barrier. After this returns successfully,
    /// all previously submitted writes are guaranteed to be on stable storage.
    ///
    /// Returns the highest durable index.
    pub fn flush(&mut self) -> io::Result<u64> {
        self.check_owner_thread()?;
        
        if self.pending_writes.is_empty() {
            return Ok(self.committed_index.load(Ordering::Acquire));
        }
        
        let highest_pending = self.pending_writes.back().map(|w| w.index).unwrap_or(0);
        
        // Submit fsync with IO_DRAIN to ensure ordering
        let fsync_op = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .flags(io_uring::squeue::Flags::IO_DRAIN)
            .user_data(TAG_FSYNC | (highest_pending << 8));
        
        unsafe {
            self.ring
                .submission()
                .push(&fsync_op)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "SQ full"))?;
        }
        
        self.pending_fsyncs.push_back(PendingFsync {
            up_to_index: highest_pending,
            completed: false,
        });
        
        self.ring.submit()?;
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.writes_since_fsync = 0;
        
        // Wait for fsync completion
        self.wait_for_fsync(highest_pending)?;
        
        Ok(highest_pending)
    }
    
    /// Poll for completions without blocking.
    ///
    /// Returns the number of completions processed.
    /// In O_DIRECT mode, completed DMA buffers are returned to the pool.
    pub fn poll_completions(&mut self) -> io::Result<usize> {
        let mut count = 0;
        let use_dma = self.dma_pool.is_some();
        
        while let Some(cqe) = self.ring.completion().next() {
            let user_data = cqe.user_data();
            let tag = user_data & 0xFF;
            let index = user_data >> 8;
            
            if cqe.result() < 0 {
                return Err(io::Error::from_raw_os_error(-cqe.result()));
            }
            
            match tag {
                TAG_WRITE => {
                    // Mark write as completed
                    for pw in self.pending_writes.iter_mut() {
                        if pw.index == index {
                            pw.completed = true;
                            break;
                        }
                    }
                    // Clean up completed writes from front and recycle buffers
                    while self.pending_writes.front().map(|w| w.completed).unwrap_or(false) {
                        self.pending_writes.pop_front();
                        
                        if use_dma {
                            // Return DMA buffer to pool for reuse
                            if let Some(dma_buf) = self.pending_dma_buffers.pop_front() {
                                if let Some(ref mut pool) = self.dma_pool {
                                    pool.release(dma_buf);
                                }
                            }
                        } else {
                            // Drop regular buffer
                            self.write_buffers.pop_front();
                        }
                    }
                }
                TAG_FSYNC => {
                    // Mark fsync as completed and advance committed_index
                    for pf in self.pending_fsyncs.iter_mut() {
                        if pf.up_to_index == index {
                            pf.completed = true;
                            self.committed_index.store(index, Ordering::Release);
                            break;
                        }
                    }
                    // Clean up completed fsyncs
                    while self.pending_fsyncs.front().map(|f| f.completed).unwrap_or(false) {
                        self.pending_fsyncs.pop_front();
                    }
                }
                _ => {}
            }
            
            count += 1;
        }
        
        Ok(count)
    }
    
    /// Wait for a specific fsync to complete.
    fn wait_for_fsync(&mut self, up_to_index: u64) -> io::Result<()> {
        loop {
            // Check if already completed
            // Note: u64::MAX means "no commits yet", so we can't use >= comparison
            let current = self.committed_index.load(Ordering::Acquire);
            if current != u64::MAX && current >= up_to_index {
                return Ok(());
            }
            
            // Wait for at least one completion
            self.ring.submit_and_wait(1)?;
            self.poll_completions()?;
        }
    }
    
    /// Check that we're on the owner thread.
    fn check_owner_thread(&self) -> io::Result<()> {
        if std::thread::current().id() != self.owner_thread {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "IoUringWriter accessed from wrong thread",
            ));
        }
        Ok(())
    }
    
    /// Get the next index that will be assigned.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }
    
    /// Get the highest committed (durable) index.
    pub fn committed_index(&self) -> Option<u64> {
        let idx = self.committed_index.load(Ordering::Acquire);
        if idx == u64::MAX {
            None
        } else {
            Some(idx)
        }
    }
    
    /// Get the current tail hash.
    pub fn tail_hash(&self) -> [u8; 16] {
        self.tail_hash
    }
    
    /// Get the number of fsync operations performed.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(Ordering::Relaxed)
    }
    
    /// Get the current view ID.
    pub fn view_id(&self) -> u64 {
        self.view_id
    }
    
    /// Set the view ID.
    pub fn set_view_id(&mut self, view_id: u64) {
        self.view_id = view_id;
    }
    
    /// Check if there are pending operations.
    pub fn has_pending(&self) -> bool {
        !self.pending_writes.is_empty() || !self.pending_fsyncs.is_empty()
    }
    
    /// Get the number of pending writes.
    pub fn pending_write_count(&self) -> usize {
        self.pending_writes.len()
    }
}

/// Synchronous wrapper for IoUringWriter that matches LogWriter's API.
///
/// This provides a drop-in replacement for LogWriter while using io_uring
/// under the hood. Each append call submits a write and immediately flushes.
#[cfg(feature = "io_uring")]
pub struct IoUringLogWriter {
    inner: IoUringWriter,
}

#[cfg(feature = "io_uring")]
impl IoUringLogWriter {
    /// Open or create a log file.
    pub fn open(
        path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
    ) -> io::Result<Self> {
        Ok(IoUringLogWriter {
            inner: IoUringWriter::open(path, next_index, write_offset, tail_hash, view_id, None)?,
        })
    }
    
    /// Create a new empty log file.
    pub fn create(path: &Path, view_id: u64) -> io::Result<Self> {
        Ok(IoUringLogWriter {
            inner: IoUringWriter::create(path, view_id, None)?,
        })
    }
    
    /// Append a single entry (synchronous - waits for durability).
    pub fn append(
        &mut self,
        payload: &[u8],
        _stream_id: u32,
        _flags: u32,
        timestamp_ns: u64,
    ) -> io::Result<u64> {
        let index = self.inner.submit_write(payload, timestamp_ns)?;
        self.inner.flush()?;
        Ok(index)
    }
    
    /// Append a batch of entries (synchronous - waits for durability).
    pub fn append_batch(&mut self, payloads: &[Vec<u8>], timestamp_ns: u64) -> io::Result<u64> {
        let last_index = self.inner.submit_write_batch(payloads, timestamp_ns)?;
        self.inner.flush()?;
        Ok(last_index)
    }
    
    /// Get the next index.
    pub fn next_index(&self) -> u64 {
        self.inner.next_index()
    }
    
    /// Get the committed index.
    pub fn committed_index(&self) -> Option<u64> {
        self.inner.committed_index()
    }
    
    /// Get the tail hash.
    pub fn tail_hash(&self) -> [u8; 16] {
        self.inner.tail_hash()
    }
    
    /// Get the fsync count.
    pub fn fsync_count(&self) -> u64 {
        self.inner.fsync_count()
    }
    
    /// Get the view ID.
    pub fn view_id(&self) -> u64 {
        self.inner.view_id()
    }
    
    /// Set the view ID.
    pub fn set_view_id(&mut self, view_id: u64) {
        self.inner.set_view_id(view_id);
    }
}

#[cfg(all(test, feature = "io_uring"))]
mod tests {
    use super::*;
    use std::fs;
    
    #[test]
    fn test_uring_writer_basic() {
        let path = Path::new("/tmp/chr_uring_test.log");
        let _ = fs::remove_file(path);
        
        let mut writer = IoUringWriter::create(path, 1, None).unwrap();
        
        // Submit some writes
        let idx0 = writer.submit_write(b"hello", 1_000_000_000).unwrap();
        let idx1 = writer.submit_write(b"world", 2_000_000_000).unwrap();
        
        assert_eq!(idx0, 0);
        assert_eq!(idx1, 1);
        assert_eq!(writer.next_index(), 2);
        
        // Not durable yet
        assert_eq!(writer.committed_index(), None);
        
        // Flush to make durable
        let committed = writer.flush().unwrap();
        assert_eq!(committed, 1);
        assert_eq!(writer.committed_index(), Some(1));
        
        fs::remove_file(path).unwrap();
    }
    
    #[test]
    fn test_uring_writer_batch() {
        let path = Path::new("/tmp/chr_uring_batch_test.log");
        let _ = fs::remove_file(path);
        
        let mut writer = IoUringWriter::create(path, 1, None).unwrap();
        
        let payloads: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("entry_{}", i).into_bytes())
            .collect();
        
        let last_idx = writer.submit_write_batch(&payloads, 1_000_000_000).unwrap();
        assert_eq!(last_idx, 9);
        assert_eq!(writer.next_index(), 10);
        
        // Flush
        let committed = writer.flush().unwrap();
        assert_eq!(committed, 9);
        
        // Only one fsync for the batch
        assert_eq!(writer.fsync_count(), 1);
        
        fs::remove_file(path).unwrap();
    }
    
    #[test]
    fn test_uring_log_writer_compat() {
        let path = Path::new("/tmp/chr_uring_compat_test.log");
        let _ = fs::remove_file(path);
        
        let mut writer = IoUringLogWriter::create(path, 1).unwrap();
        
        // Use same API as LogWriter
        let idx0 = writer.append(b"entry_0", 0, 0, 1_000_000_000).unwrap();
        let idx1 = writer.append(b"entry_1", 0, 0, 2_000_000_000).unwrap();
        
        assert_eq!(idx0, 0);
        assert_eq!(idx1, 1);
        assert_eq!(writer.committed_index(), Some(1));
        
        fs::remove_file(path).unwrap();
    }
    
    #[test]
    fn test_uring_writer_direct_io() {
        // Note: O_DIRECT may fail on some filesystems (e.g., tmpfs)
        // This test uses /tmp which is usually ext4 or similar
        let path = Path::new("/tmp/chr_uring_direct_test.log");
        let _ = fs::remove_file(path);
        
        // Try to create with O_DIRECT - may fail on unsupported filesystems
        let result = IoUringWriter::create_direct(path, 1, None);
        
        match result {
            Ok(mut writer) => {
                // Verify O_DIRECT mode is enabled
                assert!(writer.is_direct_io());
                
                // Submit some writes
                let idx0 = writer.submit_write(b"hello_direct", 1_000_000_000).unwrap();
                let idx1 = writer.submit_write(b"world_direct", 2_000_000_000).unwrap();
                
                assert_eq!(idx0, 0);
                assert_eq!(idx1, 1);
                
                // Verify write offset is 4KB-aligned (due to O_DIRECT padding)
                assert_eq!(writer.write_offset % DMA_ALIGNMENT as u64, 0);
                
                // Flush to make durable
                let committed = writer.flush().unwrap();
                assert_eq!(committed, 1);
                
                // Check DMA pool stats
                let (allocs, hits, free) = writer.dma_pool_stats().unwrap();
                assert!(allocs >= 2, "Should have allocated at least 2 buffers");
                // After flush, buffers should be returned to pool
                assert!(free >= 2 || hits > 0, "Buffers should be recycled");
                
                fs::remove_file(path).unwrap();
            }
            Err(e) => {
                // O_DIRECT not supported on this filesystem - skip test
                eprintln!("O_DIRECT not supported: {} - skipping test", e);
            }
        }
    }
    
    #[test]
    fn test_uring_writer_direct_io_batch() {
        let path = Path::new("/tmp/chr_uring_direct_batch_test.log");
        let _ = fs::remove_file(path);
        
        let result = IoUringWriter::create_direct(path, 1, None);
        
        match result {
            Ok(mut writer) => {
                assert!(writer.is_direct_io());
                
                let payloads: Vec<Vec<u8>> = (0..5)
                    .map(|i| format!("direct_entry_{}", i).into_bytes())
                    .collect();
                
                let last_idx = writer.submit_write_batch(&payloads, 1_000_000_000).unwrap();
                assert_eq!(last_idx, 4);
                
                // All offsets should be 4KB-aligned
                assert_eq!(writer.write_offset % DMA_ALIGNMENT as u64, 0);
                
                let committed = writer.flush().unwrap();
                assert_eq!(committed, 4);
                
                // Only one fsync for the batch
                assert_eq!(writer.fsync_count(), 1);
                
                fs::remove_file(path).unwrap();
            }
            Err(e) => {
                eprintln!("O_DIRECT not supported: {} - skipping test", e);
            }
        }
    }
    
    #[test]
    fn test_dma_buffer_alignment() {
        // Test DMA buffer allocation and alignment
        let buf = DmaBuffer::new(100).unwrap();
        
        // Capacity should be rounded up to 4KB
        assert_eq!(buf.capacity(), DMA_ALIGNMENT);
        
        // Pointer should be 4KB-aligned
        assert_eq!(buf.as_ptr() as usize % DMA_ALIGNMENT, 0);
        
        // Test larger allocation
        let buf2 = DmaBuffer::new(5000).unwrap();
        assert_eq!(buf2.capacity(), 8192); // 2 * 4KB
        assert_eq!(buf2.as_ptr() as usize % DMA_ALIGNMENT, 0);
    }
    
    #[test]
    fn test_dma_buffer_pool() {
        let mut pool = DmaBufferPool::new(DEFAULT_DMA_BUFFER_SIZE, 4);
        
        // Acquire some buffers
        let buf1 = pool.acquire(1000).unwrap();
        let buf2 = pool.acquire(1000).unwrap();
        
        let (allocs, hits, free) = pool.stats();
        assert_eq!(allocs, 2);
        assert_eq!(hits, 0);
        assert_eq!(free, 0);
        
        // Release buffers back to pool
        pool.release(buf1);
        pool.release(buf2);
        
        let (_, _, free) = pool.stats();
        assert_eq!(free, 2);
        
        // Acquire again - should reuse from pool
        let _buf3 = pool.acquire(1000).unwrap();
        
        let (allocs, hits, _) = pool.stats();
        assert_eq!(allocs, 2); // No new allocations
        assert_eq!(hits, 1);   // One pool hit
    }
}
