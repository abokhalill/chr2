use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::engine::log::LogWriter;

/// Unique identifier for a durability request batch.
pub type BatchId = u64;

/// Errors that can occur in the durability layer.
#[derive(Debug, Clone)]
pub enum DurabilityError {
    /// Worker is not running (shutdown or never started).
    WorkerNotRunning,
    /// Channel to worker is disconnected.
    ChannelDisconnected,
    /// Worker thread panicked.
    WorkerPanicked,
    /// Empty batch submitted.
    EmptyBatch,
}

impl fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurabilityError::WorkerNotRunning => write!(f, "DurabilityWorker is not running"),
            DurabilityError::ChannelDisconnected => write!(f, "DurabilityWorker channel disconnected"),
            DurabilityError::WorkerPanicked => write!(f, "DurabilityWorker thread panicked"),
            DurabilityError::EmptyBatch => write!(f, "Cannot submit empty batch"),
        }
    }
}

impl std::error::Error for DurabilityError {}

/// Request sent to the DurabilityWorker.
#[derive(Debug)]
pub enum DurabilityRequest {
    /// Append a batch of entries to the log.
    AppendBatch {
        /// Unique batch identifier for correlation.
        batch_id: BatchId,
        /// Payloads to append.
        payloads: Vec<Vec<u8>>,
        /// Consensus timestamp (nanoseconds since epoch).
        timestamp_ns: u64,
        /// Reserved start index (from handle's atomic reservation).
        /// INVARIANT: Writer's next_index must equal this when processing.
        reserved_start_index: u64,
    },
    /// Append a single entry to the log.
    Append {
        /// Unique batch identifier for correlation.
        batch_id: BatchId,
        /// Payload to append.
        payload: Vec<u8>,
        /// Stream ID for the entry.
        stream_id: u64,
        /// Flags for the entry.
        flags: u16,
        /// Consensus timestamp (nanoseconds since epoch).
        timestamp_ns: u64,
        /// Reserved index (from handle's atomic reservation).
        /// INVARIANT: Writer's next_index must equal this when processing.
        reserved_index: u64,
    },
    /// Shutdown the worker gracefully.
    Shutdown,
}

/// Result of a durability operation.
#[derive(Debug, Clone)]
pub enum DurabilityResult {
    /// Batch append succeeded.
    BatchSuccess {
        /// First index in the batch.
        start_index: u64,
        /// Last index in the batch.
        last_index: u64,
    },
    /// Single append succeeded.
    AppendSuccess {
        /// Index of the appended entry.
        index: u64,
    },
    /// Operation failed.
    Error {
        /// Error message.
        message: String,
    },
}

/// Completion notification from the DurabilityWorker.
#[derive(Debug, Clone)]
pub struct DurabilityCompletion {
    /// Batch ID that completed.
    pub batch_id: BatchId,
    /// Result of the operation.
    pub result: DurabilityResult,
}

/// Handle for submitting work to the DurabilityWorker.
///
/// This is the interface used by VsrNode to enqueue durability work
/// without blocking the control plane.
#[derive(Clone)]
pub struct DurabilityHandle {
    /// Channel for sending requests to the worker.
    request_tx: Sender<DurabilityRequest>,
    /// Next batch ID to assign.
    next_batch_id: Arc<AtomicU64>,
    /// Whether the worker is running.
    running: Arc<AtomicBool>,
    /// Current next_index (speculative, updated on completion).
    /// This allows VsrNode to know what indices will be assigned.
    next_index: Arc<AtomicU64>,
}

impl DurabilityHandle {
    /// Submit a batch of entries for durable append.
    ///
    /// Returns immediately with the batch_id and predicted (start_index, last_index).
    /// The actual completion will be delivered on the completion channel.
    ///
    /// # Returns
    /// - `Ok((batch_id, start_index, last_index))` - Request enqueued successfully
    /// - `Err(DurabilityError)` - Worker is not running or channel is disconnected
    pub fn submit_batch(
        &self,
        payloads: Vec<Vec<u8>>,
        timestamp_ns: u64,
    ) -> Result<(BatchId, u64, u64), DurabilityError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DurabilityError::WorkerNotRunning);
        }

        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        let count = payloads.len() as u64;
        
        // Reserve indices atomically
        let start_index = self.next_index.fetch_add(count, Ordering::SeqCst);
        let last_index = start_index + count - 1;

        let request = DurabilityRequest::AppendBatch {
            batch_id,
            payloads,
            timestamp_ns,
            reserved_start_index: start_index,
        };

        self.request_tx
            .send(request)
            .map_err(|_| DurabilityError::ChannelDisconnected)?;

        Ok((batch_id, start_index, last_index))
    }

    /// Submit a single entry for durable append.
    ///
    /// Returns immediately with the batch_id and predicted index.
    pub fn submit_single(
        &self,
        payload: Vec<u8>,
        stream_id: u64,
        flags: u16,
        timestamp_ns: u64,
    ) -> Result<(BatchId, u64), DurabilityError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DurabilityError::WorkerNotRunning);
        }

        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        
        // Reserve index atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);

        let request = DurabilityRequest::Append {
            batch_id,
            payload,
            stream_id,
            flags,
            timestamp_ns,
            reserved_index: index,
        };

        self.request_tx
            .send(request)
            .map_err(|_| DurabilityError::ChannelDisconnected)?;

        Ok((batch_id, index))
    }

    /// Request graceful shutdown of the worker.
    pub fn shutdown(&self) -> Result<(), DurabilityError> {
        self.running.store(false, Ordering::SeqCst);
        self.request_tx
            .send(DurabilityRequest::Shutdown)
            .map_err(|_| DurabilityError::ChannelDisconnected)
    }

    /// Check if the worker is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get the current speculative next_index.
    ///
    /// This is the index that will be assigned to the next entry.
    /// Note: This may be ahead of what's actually durable.
    pub fn next_index(&self) -> u64 {
        self.next_index.load(Ordering::SeqCst)
    }
}

/// The DurabilityWorker background thread.
///
/// Owns the LogWriter and processes durability requests in a dedicated thread.
pub struct DurabilityWorker {
    /// Handle for submitting work.
    handle: DurabilityHandle,
    /// Channel for receiving completions.
    completion_rx: Receiver<DurabilityCompletion>,
    /// When true, the worker will pause before processing requests.
    stall_flag: Arc<AtomicBool>,
    /// Join handle for the worker thread.
    thread_handle: Option<JoinHandle<()>>,
}

impl DurabilityWorker {
    /// Create a new DurabilityWorker with a fresh log file.
    ///
    /// # Arguments
    /// * `log_path` - Path to the log file
    /// * `view_id` - Initial view ID
    ///
    /// # Returns
    /// The worker instance with handle and completion receiver.
    pub fn create(log_path: &Path, view_id: u64) -> std::io::Result<Self> {
        let writer = LogWriter::create(log_path, view_id)?;
        let next_index = writer.next_index();
        Self::spawn_with_writer(writer, next_index)
    }

    /// Create a new DurabilityWorker with an existing log file (recovery).
    ///
    /// # Arguments
    /// * `log_path` - Path to the log file
    /// * `next_index` - Next index to write (from recovery)
    /// * `write_offset` - Offset to start writing (from recovery)
    /// * `tail_hash` - Hash accumulator state (from recovery)
    /// * `view_id` - Current view ID
    pub fn open(
        log_path: &Path,
        next_index: u64,
        write_offset: u64,
        tail_hash: [u8; 16],
        view_id: u64,
    ) -> std::io::Result<Self> {
        let writer = LogWriter::open(log_path, next_index, write_offset, tail_hash, view_id)?;
        Self::spawn_with_writer(writer, next_index)
    }

    /// Spawn the worker thread with an existing LogWriter.
    fn spawn_with_writer(writer: LogWriter, initial_next_index: u64) -> std::io::Result<Self> {
        let (request_tx, request_rx) = mpsc::channel::<DurabilityRequest>();
        let (completion_tx, completion_rx) = mpsc::channel::<DurabilityCompletion>();

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let stall_flag = Arc::new(AtomicBool::new(false));
        let stall_flag_clone = stall_flag.clone();

        let next_index = Arc::new(AtomicU64::new(initial_next_index));

        let handle = DurabilityHandle {
            request_tx,
            next_batch_id: Arc::new(AtomicU64::new(0)),
            running,
            next_index,
        };

        // Spawn the worker thread
        let thread_handle = thread::Builder::new()
            .name("durability-worker".to_string())
            .spawn(move || {
                Self::worker_loop(writer, request_rx, completion_tx, running_clone, stall_flag_clone);
            })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(DurabilityWorker {
            handle,
            completion_rx,
            stall_flag,
            thread_handle: Some(thread_handle),
        })
    }

    /// The main worker loop - runs in a dedicated thread.
    fn worker_loop(
        mut writer: LogWriter,
        request_rx: Receiver<DurabilityRequest>,
        completion_tx: Sender<DurabilityCompletion>,
        running: Arc<AtomicBool>,
        stall_flag: Arc<AtomicBool>,
    ) {
        // Transfer ownership of the LogWriter to this thread.
        // This is safe because the LogWriter was moved into this thread
        // and no other thread has access to it.
        writer.transfer_ownership();
        
        while running.load(Ordering::SeqCst) {
            match request_rx.recv() {
                Ok(request) => {
                    if !matches!(request, DurabilityRequest::Shutdown) {
                        while stall_flag.load(Ordering::SeqCst) && running.load(Ordering::SeqCst) {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }

                    let completion = Self::process_request(&mut writer, request);
                    
                    // If shutdown, exit after processing
                    if completion.is_none() {
                        break;
                    }
                    
                    if let Some(c) = completion {
                        // Send completion - if receiver is gone, just exit
                        if completion_tx.send(c).is_err() {
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Channel disconnected - exit
                    break;
                }
            }
        }

        running.store(false, Ordering::SeqCst);
    }

    /// Process a single durability request.
    ///
    /// Returns None for Shutdown, Some(completion) for other requests.
    fn process_request(
        writer: &mut LogWriter,
        request: DurabilityRequest,
    ) -> Option<DurabilityCompletion> {
        match request {
            DurabilityRequest::AppendBatch {
                batch_id,
                payloads,
                timestamp_ns,
                reserved_start_index,
            } => {
                // INVARIANT: Reserved index must match writer's next_index
                // This ensures the handle's atomic reservation is consistent with
                // the actual log state. Mismatch indicates a bug or corruption.
                let writer_next_index = writer.next_index();
                if writer_next_index != reserved_start_index {
                    panic!(
                        "FATAL: DurabilityWorker index mismatch! \
                         Reserved start_index={}, writer next_index={}. \
                         This indicates a bug in index reservation or requests processed out of order.",
                        reserved_start_index, writer_next_index
                    );
                }
                
                let result = match writer.append_batch(&payloads, timestamp_ns) {
                    Ok(last_index) => DurabilityResult::BatchSuccess {
                        start_index: reserved_start_index,
                        last_index,
                    },
                    Err(e) => DurabilityResult::Error {
                        message: format!("{}", e),
                    },
                };
                Some(DurabilityCompletion { batch_id, result })
            }
            DurabilityRequest::Append {
                batch_id,
                payload,
                stream_id,
                flags,
                timestamp_ns,
                reserved_index,
            } => {
                // INVARIANT: Reserved index must match writer's next_index
                let writer_next_index = writer.next_index();
                if writer_next_index != reserved_index {
                    panic!(
                        "FATAL: DurabilityWorker index mismatch! \
                         Reserved index={}, writer next_index={}. \
                         This indicates a bug in index reservation or requests processed out of order.",
                        reserved_index, writer_next_index
                    );
                }
                
                let result = match writer.append(&payload, stream_id, flags, timestamp_ns) {
                    Ok(index) => DurabilityResult::AppendSuccess { index },
                    Err(e) => DurabilityResult::Error {
                        message: e.to_string(),
                    },
                };
                Some(DurabilityCompletion { batch_id, result })
            }
            DurabilityRequest::Shutdown => None,
        }
    }

    /// Get a clone of the handle for submitting work.
    pub fn handle(&self) -> DurabilityHandle {
        self.handle.clone()
    }

    /// Try to receive a completion without blocking.
    ///
    /// Returns:
    /// - `Ok(Some(completion))` - A completion is available
    /// - `Ok(None)` - No completion available right now
    /// - `Err(DurabilityError)` - Channel disconnected (worker died)
    pub fn try_recv_completion(&self) -> Result<Option<DurabilityCompletion>, DurabilityError> {
        match self.completion_rx.try_recv() {
            Ok(completion) => Ok(Some(completion)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err(DurabilityError::ChannelDisconnected)
            }
        }
    }

    /// Receive a completion, blocking until one is available.
    ///
    /// Returns:
    /// - `Ok(completion)` - A completion is available
    /// - `Err(DurabilityError)` - Channel disconnected (worker died)
    pub fn recv_completion(&self) -> Result<DurabilityCompletion, DurabilityError> {
        self.completion_rx
            .recv()
            .map_err(|_| DurabilityError::ChannelDisconnected)
    }

    /// Drain all available completions without blocking.
    ///
    /// Returns a vector of all completions that were ready.
    pub fn drain_completions(&self) -> Vec<DurabilityCompletion> {
        let mut completions = Vec::new();
        while let Ok(Some(c)) = self.try_recv_completion() {
            completions.push(c);
        }
        completions
    }

    /// Shutdown the worker and wait for it to finish.
    pub fn shutdown_and_join(mut self) -> Result<(), DurabilityError> {
        self.handle.shutdown()?;
        
        if let Some(handle) = self.thread_handle.take() {
            handle
                .join()
                .map_err(|_| DurabilityError::WorkerPanicked)?;
        }
        
        Ok(())
    }

    pub fn set_stalled(&self, stalled: bool) {
        self.stall_flag.store(stalled, Ordering::SeqCst);
    }

    pub fn is_stalled(&self) -> bool {
        self.stall_flag.load(Ordering::SeqCst)
    }

    /// Check if the worker is still running.
    pub fn is_running(&self) -> bool {
        self.handle.is_running()
    }
}

impl Drop for DurabilityWorker {
    fn drop(&mut self) {
        // Signal shutdown
        let _ = self.handle.shutdown();
        
        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_durability_worker_basic() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        
        let worker = DurabilityWorker::create(&log_path, 0).unwrap();
        let handle = worker.handle();
        
        // Submit a batch
        let payloads = vec![b"hello".to_vec(), b"world".to_vec()];
        let (batch_id, start_idx, last_idx) = handle
            .submit_batch(payloads, 12345)
            .unwrap();
        
        assert_eq!(batch_id, 0);
        assert_eq!(start_idx, 0);
        assert_eq!(last_idx, 1);
        
        // Wait for completion
        let completion = worker.recv_completion().unwrap();
        assert_eq!(completion.batch_id, 0);
        
        match completion.result {
            DurabilityResult::BatchSuccess { start_index, last_index } => {
                assert_eq!(start_index, 0);
                assert_eq!(last_index, 1);
            }
            _ => panic!("Expected BatchSuccess"),
        }
        
        // Shutdown
        worker.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_durability_worker_single_append() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        
        let worker = DurabilityWorker::create(&log_path, 0).unwrap();
        let handle = worker.handle();
        
        // Submit single entries
        let (batch_id1, idx1) = handle
            .submit_single(b"entry1".to_vec(), 0, 0, 1000)
            .unwrap();
        let (batch_id2, idx2) = handle
            .submit_single(b"entry2".to_vec(), 0, 0, 2000)
            .unwrap();
        
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        
        // Wait for completions
        let c1 = worker.recv_completion().unwrap();
        let c2 = worker.recv_completion().unwrap();
        
        assert_eq!(c1.batch_id, batch_id1);
        assert_eq!(c2.batch_id, batch_id2);
        
        worker.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_durability_worker_multiple_batches() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        
        let worker = DurabilityWorker::create(&log_path, 0).unwrap();
        let handle = worker.handle();
        
        // Submit multiple batches
        let (_, start1, last1) = handle
            .submit_batch(vec![b"a".to_vec(), b"b".to_vec()], 1000)
            .unwrap();
        let (_, start2, last2) = handle
            .submit_batch(vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()], 2000)
            .unwrap();
        
        assert_eq!(start1, 0);
        assert_eq!(last1, 1);
        assert_eq!(start2, 2);
        assert_eq!(last2, 4);
        
        // Drain completions
        std::thread::sleep(Duration::from_millis(50));
        let completions = worker.drain_completions();
        assert_eq!(completions.len(), 2);
        
        worker.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_durability_worker_handle_clone() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        
        let worker = DurabilityWorker::create(&log_path, 0).unwrap();
        let handle1 = worker.handle();
        let handle2 = handle1.clone();
        
        // Both handles should work
        let (_, idx1, _) = handle1
            .submit_batch(vec![b"from_handle1".to_vec()], 1000)
            .unwrap();
        let (_, idx2, _) = handle2
            .submit_batch(vec![b"from_handle2".to_vec()], 2000)
            .unwrap();
        
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        
        worker.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_durability_worker_shutdown_rejects_new_work() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        
        let worker = DurabilityWorker::create(&log_path, 0).unwrap();
        let handle = worker.handle();
        
        // Shutdown
        handle.shutdown().unwrap();
        
        // New submissions should fail
        std::thread::sleep(Duration::from_millis(10));
        let result = handle.submit_batch(vec![b"should_fail".to_vec()], 1000);
        assert!(result.is_err());
    }
}
