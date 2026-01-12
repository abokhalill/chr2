use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use crate::engine::durability::{
    BatchId, DurabilityCompletion, DurabilityHandle, DurabilityResult,
};
use crate::engine::log::LogWriter;
use crate::engine::manifest::{Manifest, ViewFenceError};
use crate::engine::reader::{CommittedState, LogReader};
use crate::kernel::executor::Executor;
use crate::kernel::traits::ChrApplication;

use super::client::SessionMap;
use super::error::VsrError;
use super::message::{
    ClientRequest, ClientResponse, ClientResult, LogEntrySummary, PreparedEntry, VsrMessage,
};
use super::network::NetworkEndpoint;
use super::quorum::{NodeBitset, QuorumTracker};

/// Default maximum in-flight requests (memory safety).
pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 5000;

/// Default maximum replication gap (latency safety).
pub const DEFAULT_MAX_REPLICATION_GAP: u64 = 10_000;

/// Heartbeat interval: Primary sends heartbeat if idle for this duration.
/// Increased from 50ms to 100ms to reduce network overhead while maintaining
/// responsive failure detection with the corresponding ELECTION_TIMEOUT.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// Election timeout: Backup starts view change if no message from Primary for this duration.
/// Increased from 200ms to 1000ms to avoid false elections during disk stalls.
/// With async durability, heartbeats are decoupled from I/O, so this can be longer.
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

/// Maximum batch size in bytes (64KB).
pub const MAX_BATCH_SIZE: usize = 64 * 1024;

/// Maximum batch delay before forced flush (5ms).
pub const MAX_BATCH_DELAY: Duration = Duration::from_millis(5);

/// Request entry in the MultiQueue.
#[derive(Debug, Clone)]
struct QueuedRequest {
    /// Payload data.
    payload: Vec<u8>,
    /// Client ID for tracking.
    client_id: u64,
    /// Sequence number for idempotency.
    sequence_number: u64,
}

/// Request batcher with fair scheduling (MultiQueue).
///
/// Accumulates client requests in per-client queues and flushes them
/// using round-robin scheduling to ensure fairness.
///
/// # Fairness Guarantee
/// When flushing a batch, requests are pulled round-robin from each client's
/// queue. This prevents a single client from dominating a batch.
///
/// # Flush Triggers
/// - Total payload size reaches MAX_BATCH_SIZE, OR
/// - Time since first request exceeds MAX_BATCH_DELAY
#[derive(Debug)]
pub struct RequestBatcher {
    /// Per-client request queues for fair scheduling.
    queues: HashMap<u64, std::collections::VecDeque<QueuedRequest>>,
    /// Order of clients for round-robin (maintains insertion order).
    client_order: Vec<u64>,
    /// Total size of all pending payloads.
    pending_size: usize,
    /// Total count of pending requests.
    pending_count: usize,
    /// Time when the first request in current batch arrived.
    batch_start: Option<Instant>,
    /// Current position in round-robin rotation.
    round_robin_index: usize,
}

impl RequestBatcher {
    /// Create a new empty batcher.
    pub fn new() -> Self {
        RequestBatcher {
            queues: HashMap::new(),
            client_order: Vec::new(),
            pending_size: 0,
            pending_count: 0,
            batch_start: None,
            round_robin_index: 0,
        }
    }

    /// Add a request to the batch.
    ///
    /// Returns true if the batch should be flushed (size threshold reached).
    pub fn add(&mut self, payload: Vec<u8>, client_id: u64, sequence_number: u64) -> bool {
        let payload_size = payload.len();

        // Record batch start time on first request
        if self.pending_count == 0 {
            self.batch_start = Some(Instant::now());
        }

        // Add client to order if not already present
        if !self.queues.contains_key(&client_id) {
            self.client_order.push(client_id);
        }

        // Add request to client's queue
        let queue = self
            .queues
            .entry(client_id)
            .or_insert_with(std::collections::VecDeque::new);
        queue.push_back(QueuedRequest {
            payload,
            client_id,
            sequence_number,
        });

        self.pending_size += payload_size;
        self.pending_count += 1;

        // Check if size threshold reached
        self.pending_size >= MAX_BATCH_SIZE
    }

    /// Check if the batch should be flushed due to timeout.
    pub fn should_flush_timeout(&self) -> bool {
        if let Some(start) = self.batch_start {
            self.pending_count > 0 && start.elapsed() >= MAX_BATCH_DELAY
        } else {
            false
        }
    }

    /// Check if there are pending requests.
    pub fn has_pending(&self) -> bool {
        self.pending_count > 0
    }

    /// Get the number of pending requests.
    pub fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Take the pending batch using round-robin fair scheduling.
    ///
    /// Returns (payloads, client_tracking) where client_tracking is
    /// Vec<(client_id, sequence_number)> in round-robin order.
    ///
    /// # Fairness
    /// Requests are pulled one at a time from each client's queue in rotation.
    /// This ensures that if Client A has 100 requests and Client B has 10,
    /// the batch will interleave them: A, B, A, B, A, B, ... rather than
    /// processing all of A's requests first.
    pub fn take_batch(&mut self) -> (Vec<Vec<u8>>, Vec<(u64, u64)>) {
        let mut payloads = Vec::with_capacity(self.pending_count);
        let mut clients = Vec::with_capacity(self.pending_count);

        // Round-robin: pull one request from each client in rotation
        let mut remaining = self.pending_count;
        while remaining > 0 {
            let mut pulled_this_round = 0;

            for client_id in &self.client_order {
                if let Some(queue) = self.queues.get_mut(client_id) {
                    if let Some(request) = queue.pop_front() {
                        payloads.push(request.payload);
                        clients.push((request.client_id, request.sequence_number));
                        remaining -= 1;
                        pulled_this_round += 1;
                    }
                }
            }

            // Safety: if we didn't pull anything, break to avoid infinite loop
            if pulled_this_round == 0 {
                break;
            }
        }

        // Reset state
        self.queues.clear();
        self.client_order.clear();
        self.pending_size = 0;
        self.pending_count = 0;
        self.batch_start = None;
        self.round_robin_index = 0;

        (payloads, clients)
    }

    /// Get the number of unique clients with pending requests.
    pub fn client_count(&self) -> usize {
        self.client_order.len()
    }
}

impl Default for RequestBatcher {
    fn default() -> Self {
        Self::new()
    }
}

/// Role of a VSR node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Primary node: receives client requests, broadcasts Prepare messages.
    Primary,
    /// Backup node: receives Prepare messages, responds with PrepareOk.
    Backup,
    /// View change in progress: node suspects leader failure.
    ViewChangeInProgress,
}

/// Collected DoViewChange message for view change reconciliation.
#[derive(Debug, Clone)]
pub struct DoViewChangeInfo {
    pub node_id: u32,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub last_log_hash: [u8; 16],
    pub log_suffix: Vec<LogEntrySummary>,
}

/// VSR Node state.
pub struct VsrNode<A: ChrApplication> {
    /// This node's ID.
    pub node_id: u32,
    /// Current role (Primary or Backup).
    pub role: NodeRole,
    /// Current view number.
    pub view: u64,
    /// Proposed view number during view change.
    pub proposed_view: u64,
    /// Log writer for local durability.
    pub writer: LogWriter,
    /// Log reader for catch-up requests (Primary only).
    /// This allows the Primary to read committed entries and send them to lagging backups.
    pub reader: Option<LogReader>,
    /// Shared committed state (for visibility to readers/executor).
    pub committed_state: Arc<CommittedState>,
    /// Quorum tracker (Primary only).
    pub quorum_tracker: Option<QuorumTracker>,
    /// Network endpoint for communication.
    pub network: NetworkEndpoint,
    /// Executor for applying committed entries.
    pub executor: Option<Executor<A>>,
    /// Next index expected from Primary (Backup only).
    pub next_expected_index: u64,
    /// Cluster size.
    pub cluster_size: u32,
    /// Last time we received a message from the Primary (Backup only).
    pub last_primary_contact: Instant,
    /// Last time we sent a message (Primary only, for heartbeat timing).
    pub last_sent: Instant,

    // =========================================================================
    // DURABLE FENCING STATE (CRITICAL FOR CORRECTNESS)
    // =========================================================================
    /// Durable consensus state manifest.
    ///
    /// INVARIANT: Before accepting any message with view V, we MUST have
    /// `manifest.highest_view() <= V`. Messages with lower views are rejected
    /// to prevent zombie leader interference.
    ///
    /// INVARIANT: Before voting in view V, we MUST persist the vote in the
    /// manifest. This prevents double-voting after crash/restart.
    pub manifest: Manifest,

    // =========================================================================
    // VIEW CHANGE STATE
    // =========================================================================
    /// Tracks StartViewChange votes per proposed view.
    /// Key: proposed_view, Value: bitset of node_ids that voted for this view.
    pub start_view_change_votes: HashMap<u64, NodeBitset>,
    /// Collected DoViewChange messages for the view we're becoming primary for.
    pub do_view_change_msgs: HashMap<u64, Vec<DoViewChangeInfo>>,
    /// Whether we've sent DoViewChange for the current proposed_view.
    pub sent_do_view_change: bool,

    // =========================================================================
    // CLIENT SESSION STATE
    // =========================================================================
    /// Session map for client request idempotency.
    pub session_map: SessionMap,
    /// Pending client requests waiting for commit.
    /// Key: log_index, Value: (client_id, sequence_number)
    pub pending_requests: HashMap<u64, (u64, u64)>,

    // =========================================================================
    // GROUP COMMIT STATE (Primary only)
    // =========================================================================
    /// Request batcher for group commit optimization.
    pub batcher: RequestBatcher,

    // =========================================================================
    // ADMISSION CONTROL (Backpressure)
    // =========================================================================
    /// Maximum in-flight requests (memory safety).
    pub max_inflight_requests: usize,
    /// Maximum replication gap before rejecting new requests (latency safety).
    pub max_replication_gap: u64,
    /// Current in-flight request count (requests received but not yet committed).
    /// Uses AtomicUsize for thread-safety.
    inflight_count: Arc<AtomicUsize>,
    /// Counter for rejected requests due to overload.
    pub rejected_count: u64,
    /// Counter for messages rejected due to view fencing.
    pub fenced_message_count: u64,

    // =========================================================================
    // CATCH-UP STATE (Backup only)
    // =========================================================================
    /// Whether a catch-up request is pending (to avoid duplicate requests).
    pub catch_up_pending: bool,
    /// Buffered entries received during catch-up (out of order).
    /// Key: index, Value: (payload, timestamp_ns, stream_id, flags)
    pub catch_up_buffer: HashMap<u64, (Vec<u8>, u64, u64, u16)>,

    // =========================================================================
    // ASYNC DURABILITY STATE (Optional - for non-blocking I/O)
    // =========================================================================
    /// Handle for submitting work to the DurabilityWorker (if using async mode).
    /// When Some, durability operations are non-blocking.
    /// When None, durability operations use the synchronous LogWriter directly.
    pub durability_handle: Option<DurabilityHandle>,

    /// Pending durability batches awaiting completion (Primary only).
    /// Key: batch_id, Value: (start_index, last_index, payloads, timestamp_ns, clients)
    /// Once durability completes, we broadcast PrepareBatch and update quorum tracker.
    pending_durability: HashMap<BatchId, PendingDurabilityBatch>,

    /// Pending backup durability operations awaiting completion (Backup only).
    /// Key: batch_id, Value: pending backup operation info.
    /// Once durability completes, we send PrepareOk and update commit index.
    pending_backup_durability: HashMap<BatchId, PendingBackupDurability>,
}

/// Tracks a batch that has been submitted to the DurabilityWorker but not yet confirmed durable.
/// Used by Primary for async batch submission.
#[derive(Debug)]
struct PendingDurabilityBatch {
    /// First index in the batch (speculative).
    start_index: u64,
    /// Last index in the batch (speculative).
    last_index: u64,
    /// Payloads for broadcasting PrepareBatch after durability.
    payloads: Vec<Vec<u8>>,
    /// Consensus timestamp for the batch.
    timestamp_ns: u64,
    /// Client tracking info: Vec<(client_id, sequence_number)>.
    clients: Vec<(u64, u64)>,
}

/// Tracks a backup durability operation pending completion.
/// Used by Backup for async Prepare/PrepareBatch handling.
#[derive(Debug)]
#[allow(dead_code)]
struct PendingBackupDurability {
    /// Node ID of the Primary that sent the Prepare.
    from_node: u32,
    /// Indices to send PrepareOk for after durability completes.
    indices: Vec<u64>,
    /// Commit index from the Primary (to update after durability).
    commit_index: Option<u64>,
    /// The last index written (for commit advancement).
    last_written_index: u64,
}

impl<A: ChrApplication> VsrNode<A> {
    /// Create a new Primary node with durable fencing.
    ///
    /// # Arguments
    /// * `manifest_path` - Path to the manifest file for durable fencing state
    ///
    /// # Panics
    /// Panics if the manifest cannot be opened/created (fatal for correctness).
    pub fn new_primary(
        node_id: u32,
        cluster_size: u32,
        view: u64,
        writer: LogWriter,
        reader: Option<LogReader>,
        committed_state: Arc<CommittedState>,
        network: NetworkEndpoint,
        executor: Option<Executor<A>>,
        manifest_path: &Path,
    ) -> std::io::Result<Self> {
        let quorum_tracker = Some(QuorumTracker::new(cluster_size, node_id));
        let now = Instant::now();

        // Open or create manifest for durable fencing
        let mut manifest = Manifest::open(manifest_path)?;

        // CRITICAL: Persist view before becoming Primary
        // This ensures zombie leaders from lower views are rejected after restart
        manifest.advance_view(view)?;

        Ok(VsrNode {
            node_id,
            role: NodeRole::Primary,
            view,
            proposed_view: view,
            writer,
            reader,
            committed_state,
            quorum_tracker,
            network,
            executor,
            next_expected_index: 0,
            cluster_size,
            last_primary_contact: now,
            last_sent: now,
            manifest,
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
            session_map: SessionMap::new(),
            pending_requests: HashMap::new(),
            batcher: RequestBatcher::new(),
            max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
            max_replication_gap: DEFAULT_MAX_REPLICATION_GAP,
            inflight_count: Arc::new(AtomicUsize::new(0)),
            rejected_count: 0,
            fenced_message_count: 0,
            catch_up_pending: false,
            catch_up_buffer: HashMap::new(),
            durability_handle: None,
            pending_durability: HashMap::new(),
            pending_backup_durability: HashMap::new(),
        })
    }

    /// Create a new Backup node with durable fencing.
    ///
    /// # Arguments
    /// * `manifest_path` - Path to the manifest file for durable fencing state
    ///
    /// # Panics
    /// Panics if the manifest cannot be opened/created (fatal for correctness).
    pub fn new_backup(
        node_id: u32,
        cluster_size: u32,
        view: u64,
        writer: LogWriter,
        committed_state: Arc<CommittedState>,
        network: NetworkEndpoint,
        executor: Option<Executor<A>>,
        manifest_path: &Path,
    ) -> std::io::Result<Self> {
        let now = Instant::now();

        // Open or create manifest for durable fencing
        let mut manifest = Manifest::open(manifest_path)?;

        // CRITICAL: Persist view before accepting messages
        manifest.advance_view(view)?;

        Ok(VsrNode {
            node_id,
            role: NodeRole::Backup,
            view,
            proposed_view: view,
            writer,
            reader: None, // Backups don't serve catch-up requests
            committed_state,
            quorum_tracker: None,
            network,
            executor,
            next_expected_index: 0,
            cluster_size,
            last_primary_contact: now,
            last_sent: now,
            manifest,
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
            session_map: SessionMap::new(),
            pending_requests: HashMap::new(),
            batcher: RequestBatcher::new(),
            max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
            max_replication_gap: DEFAULT_MAX_REPLICATION_GAP,
            inflight_count: Arc::new(AtomicUsize::new(0)),
            rejected_count: 0,
            fenced_message_count: 0,
            catch_up_pending: false,
            catch_up_buffer: HashMap::new(),
            durability_handle: None,
            pending_durability: HashMap::new(),
            pending_backup_durability: HashMap::new(),
        })
    }

    /// Get the durable highest view from the manifest.
    ///
    /// This is the authoritative view fence - any message with a lower view
    /// MUST be rejected.
    #[inline]
    pub fn durable_highest_view(&self) -> u64 {
        self.manifest.highest_view()
    }

    /// Check if a message view passes the durable fence.
    ///
    /// Returns Ok(()) if the message can be processed.
    /// Returns Err(ViewFenceError) if the message should be rejected.
    #[inline]
    pub fn check_view_fence(&self, msg_view: u64) -> Result<(), ViewFenceError> {
        self.manifest.check_view_fence(msg_view)
    }

    /// Set the maximum in-flight requests limit.
    pub fn set_max_inflight_requests(&mut self, max: usize) {
        self.max_inflight_requests = max;
    }

    /// Set the maximum replication gap limit.
    pub fn set_max_replication_gap(&mut self, max: u64) {
        self.max_replication_gap = max;
    }

    /// Get the current in-flight request count.
    pub fn inflight_count(&self) -> usize {
        self.inflight_count.load(Ordering::SeqCst)
    }

    /// Get the current replication gap (last_appended - committed).
    pub fn replication_gap(&self) -> u64 {
        let last_appended = self.writer.next_index().saturating_sub(1);
        let committed = self.committed_state.committed_index().unwrap_or(0);
        last_appended.saturating_sub(committed)
    }

    /// Check if the system is overloaded (either in-flight or replication gap exceeded).
    pub fn is_overloaded(&self) -> bool {
        self.inflight_count() >= self.max_inflight_requests
            || self.replication_gap() >= self.max_replication_gap
    }

    /// Get the number of rejected requests.
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count
    }

    // =========================================================================
    // ASYNC DURABILITY MODE
    // =========================================================================

    /// Enable async durability mode by providing a DurabilityHandle.
    ///
    /// When enabled, durability operations (submit_batch, flush_batch) will
    /// enqueue work to the DurabilityWorker instead of blocking on disk I/O.
    /// The caller must periodically call `process_durability_completions()`
    /// to handle completed durability operations.
    ///
    /// # Arguments
    /// * `handle` - Handle to the DurabilityWorker
    pub fn enable_async_durability(&mut self, handle: DurabilityHandle) {
        self.durability_handle = Some(handle);
    }

    /// Disable async durability mode, returning to synchronous LogWriter calls.
    pub fn disable_async_durability(&mut self) {
        self.durability_handle = None;
        self.pending_durability.clear();
    }

    /// Check if async durability mode is enabled.
    pub fn is_async_durability_enabled(&self) -> bool {
        self.durability_handle.is_some()
    }

    /// Get the number of pending durability batches.
    pub fn pending_durability_count(&self) -> usize {
        self.pending_durability.len()
    }

    /// Process durability completions from the DurabilityWorker.
    ///
    /// This should be called periodically (e.g., in the tick loop) when
    /// async durability mode is enabled. For each completed batch:
    /// 1. Record local writes in quorum tracker
    /// 2. Broadcast PrepareBatch to backups
    /// 3. Track pending requests for client responses
    ///
    /// Returns the number of completions processed.
    pub fn process_durability_completions(
        &mut self,
        completions: &[DurabilityCompletion],
    ) -> usize {
        let mut processed = 0;

        for completion in completions {
            if let Some(pending) = self.pending_durability.remove(&completion.batch_id) {
                match &completion.result {
                    DurabilityResult::BatchSuccess {
                        start_index,
                        last_index,
                    } => {
                        // Verify indices match our expectations
                        // INVARIANT: Reserved indices must match actual written indices
                        // Violation indicates a bug in index reservation or a corrupted state
                        if *start_index != pending.start_index || *last_index != pending.last_index
                        {
                            panic!(
                                "FATAL: Node {}: Durability index mismatch! Expected {}-{}, got {}-{}. \
                                 This indicates a bug in index reservation or corrupted state.",
                                self.node_id, pending.start_index, pending.last_index,
                                start_index, last_index
                            );
                        }

                        // Step 1: Record local writes in quorum tracker
                        if let Some(ref mut tracker) = self.quorum_tracker {
                            for idx in *start_index..=*last_index {
                                tracker.record_local_write(idx);
                            }
                        }

                        // Step 2: Broadcast PrepareBatch to all Backups
                        let commit_index = self.committed_state.committed_index();
                        let entries: Vec<PreparedEntry> = pending
                            .payloads
                            .iter()
                            .enumerate()
                            .map(|(i, payload)| PreparedEntry {
                                index: start_index + i as u64,
                                payload: payload.clone(),
                            })
                            .collect();

                        let prepare_batch = VsrMessage::PrepareBatch {
                            view: self.view,
                            start_index: *start_index,
                            entries,
                            commit_index,
                            timestamp_ns: pending.timestamp_ns,
                        };

                        self.network.broadcast(prepare_batch);
                        self.last_sent = Instant::now();

                        // Step 3: Track pending requests for client responses
                        for (i, (client_id, sequence_number)) in
                            pending.clients.into_iter().enumerate()
                        {
                            let log_index = start_index + i as u64;
                            self.pending_requests
                                .insert(log_index, (client_id, sequence_number));
                        }

                        processed += 1;
                    }
                    DurabilityResult::AppendSuccess { index } => {
                        // Single append - similar logic but for one entry
                        if let Some(ref mut tracker) = self.quorum_tracker {
                            tracker.record_local_write(*index);
                        }

                        // For single appends, we'd need different tracking
                        // This path is less common for Primary batching
                        processed += 1;
                    }
                    DurabilityResult::Error { message } => {
                        // Durability errors are fatal - we cannot proceed without durable writes
                        // The node must halt to prevent data loss or inconsistency
                        panic!(
                            "FATAL: Node {}: Durability error for batch {}: {}. \
                             Cannot proceed without durable writes.",
                            self.node_id, completion.batch_id, message
                        );
                    }
                }
            } else if let Some(pending) =
                self.pending_backup_durability.remove(&completion.batch_id)
            {
                // Backup durability completion
                match &completion.result {
                    DurabilityResult::BatchSuccess {
                        start_index: _,
                        last_index,
                    }
                    | DurabilityResult::AppendSuccess { index: last_index } => {
                        // Step 1: Send PrepareOk for each index
                        for idx in &pending.indices {
                            let prepare_ok = VsrMessage::PrepareOk {
                                index: *idx,
                                node_id: self.node_id,
                            };
                            self.network.send_to(pending.from_node, prepare_ok);
                        }

                        // Step 2: Update committed_index if Primary has committed
                        if let Some(primary_commit) = pending.commit_index {
                            let current_committed = self.committed_state.committed_index();
                            let should_advance = match current_committed {
                                None => true,
                                Some(c) => primary_commit > c,
                            };

                            if should_advance {
                                let new_commit = primary_commit.min(*last_index);
                                self.committed_state.advance(new_commit);
                                self.apply_committed_entries(current_committed, new_commit);
                            }
                        }

                        processed += 1;
                    }
                    DurabilityResult::Error { message } => {
                        // Backup durability errors are also fatal - we cannot safely continue
                        // without durable writes. The node must halt.
                        panic!(
                            "FATAL: Node {}: Backup durability error for batch {}: {}. \
                             Cannot proceed without durable writes.",
                            self.node_id, completion.batch_id, message
                        );
                    }
                }
            }
        }

        processed
    }

    /// Get the number of pending backup durability operations.
    pub fn pending_backup_durability_count(&self) -> usize {
        self.pending_backup_durability.len()
    }

    // =========================================================================
    // PRIMARY ROLE
    // =========================================================================

    /// Submit a new entry (Primary only).
    ///
    /// 1. Append to local log (Stage: Locally Durable)
    /// 2. Broadcast Prepare to all Backups
    /// 3. Record local write in QuorumTracker
    ///
    /// Returns the index of the submitted entry.
    pub fn submit(&mut self, payload: &[u8]) -> std::io::Result<u64> {
        assert_eq!(self.role, NodeRole::Primary, "Only Primary can submit");

        // Assign consensus timestamp (Primary's wall clock) BEFORE writing to log
        // This timestamp is persisted in the log header and replicated to backups
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Step 1: Append to local log with consensus timestamp
        let index = self.writer.append(payload, 0, 0, timestamp_ns)?;

        // Step 2: Record local write in quorum tracker
        if let Some(ref mut tracker) = self.quorum_tracker {
            tracker.record_local_write(index);
        }

        // Step 3: Broadcast Prepare to all Backups
        // CRITICAL: Pass Option<u64> directly - do NOT use unwrap_or(0)
        // Backups must distinguish "no commits yet" from "commit index 0"
        let commit_index = self.committed_state.committed_index();

        let prepare = VsrMessage::Prepare {
            view: self.view,
            index,
            payload: payload.to_vec(),
            commit_index,
            timestamp_ns,
        };

        self.network.broadcast(prepare);
        self.last_sent = Instant::now();

        Ok(index)
    }

    /// Submit a batch of entries (Primary only, for group commit).
    ///
    /// 1. Append all entries to local log with single fdatasync
    /// 2. Broadcast PrepareBatch to all Backups
    /// 3. Record local writes in QuorumTracker (per-entry)
    ///
    /// Returns the (start_index, last_index) of the submitted batch.
    pub fn submit_batch(&mut self, payloads: &[Vec<u8>]) -> Result<(u64, u64), VsrError> {
        assert_eq!(self.role, NodeRole::Primary, "Only Primary can submit");

        if payloads.is_empty() {
            return Err(VsrError::EmptyBatch);
        }

        // Assign consensus timestamp BEFORE writing to log
        // This timestamp is persisted in log headers and replicated to backups
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let start_index = self.writer.next_index();

        // Step 1: Append batch to local log (single fdatasync) with consensus timestamp
        let last_index = self.writer.append_batch(payloads, timestamp_ns)?;

        // Step 2: Record local writes in quorum tracker (per-entry tracking)
        if let Some(ref mut tracker) = self.quorum_tracker {
            for idx in start_index..=last_index {
                tracker.record_local_write(idx);
            }
        }

        // Step 3: Broadcast PrepareBatch to all Backups
        // CRITICAL: Pass Option<u64> directly - do NOT use unwrap_or(0)
        // Backups must distinguish "no commits yet" from "commit index 0"
        let commit_index = self.committed_state.committed_index();
        let entries: Vec<PreparedEntry> = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| PreparedEntry {
                index: start_index + i as u64,
                payload: payload.clone(),
            })
            .collect();

        let prepare_batch = VsrMessage::PrepareBatch {
            view: self.view,
            start_index,
            entries,
            commit_index,
            timestamp_ns,
        };

        self.network.broadcast(prepare_batch);
        self.last_sent = Instant::now();

        Ok((start_index, last_index))
    }

    /// Flush the request batcher and submit as a batch.
    ///
    /// Called when:
    /// - Batch size threshold reached
    /// - Batch timeout elapsed
    ///
    /// # Async Durability Mode
    /// When async durability is enabled, this method enqueues the batch to the
    /// DurabilityWorker and returns immediately. The actual broadcast and tracking
    /// happens when `process_durability_completions()` is called with the completion.
    /// In this mode, returns an empty Vec (tracking is deferred).
    ///
    /// # Sync Mode
    /// When async durability is disabled, this method blocks on disk I/O and
    /// returns Vec of (log_index, client_id, sequence_number) for tracking.
    pub fn flush_batch(&mut self) -> Result<Vec<(u64, u64, u64)>, VsrError> {
        if !self.batcher.has_pending() {
            return Ok(Vec::new());
        }

        let (payloads, clients) = self.batcher.take_batch();

        // Check if async durability mode is enabled
        if let Some(ref handle) = self.durability_handle {
            // Async mode: enqueue to DurabilityWorker, return immediately
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            let (batch_id, start_index, last_index) =
                handle.submit_batch(payloads.clone(), timestamp_ns)?;

            // Track this pending batch - will be processed when durability completes
            self.pending_durability.insert(
                batch_id,
                PendingDurabilityBatch {
                    start_index,
                    last_index,
                    payloads,
                    timestamp_ns,
                    clients,
                },
            );

            // Return empty - tracking is deferred to process_durability_completions
            Ok(Vec::new())
        } else {
            // Sync mode: blocking I/O path (original behavior)
            let (start_index, _last_index) = self.submit_batch(&payloads)?;

            // Build tracking info: (log_index, client_id, sequence_number)
            let tracking: Vec<(u64, u64, u64)> = clients
                .into_iter()
                .enumerate()
                .map(|(i, (client_id, seq))| (start_index + i as u64, client_id, seq))
                .collect();

            Ok(tracking)
        }
    }

    // =========================================================================
    // CLIENT REQUEST HANDLING
    // =========================================================================

    /// Handle a client request with idempotency checking and admission control.
    ///
    /// # Admission Control Order (CRITICAL)
    /// 1. Check if Primary
    /// 2. Check SessionMap for duplicates (retries are ALWAYS free)
    /// 3. Check in-flight limit and replication gap (backpressure)
    /// 4. Accept and process the request
    ///
    /// Returns:
    /// - Cached response if this is a duplicate request
    /// - NotThePrimary if this node is not the Primary
    /// - Error("System Overloaded") if admission control rejects
    /// - Pending if the request was accepted and is being replicated
    /// - Success/Error after the request is committed
    pub fn handle_client_request(&mut self, request: &ClientRequest) -> ClientResponse {
        // Step 1: Check if we're the Primary
        if self.role != NodeRole::Primary {
            let leader_hint = if self.role == NodeRole::Backup {
                Some(self.primary_for_view(self.view))
            } else {
                None
            };

            return ClientResponse {
                sequence_number: request.sequence_number,
                result: ClientResult::NotThePrimary { leader_hint },
            };
        }

        // Step 2: Check for duplicate request (idempotency)
        // CRITICAL: This MUST happen BEFORE the in-flight check.
        // Retries of already-committed requests are ALWAYS free and never blocked.
        if let Some(cached) = self
            .session_map
            .check_duplicate(request.client_id, request.sequence_number)
        {
            return cached;
        }

        // Step 3: Admission control - check in-flight limit and replication gap
        // This only applies to NEW requests (not retries)
        if self.is_overloaded() {
            self.rejected_count += 1;
            return ClientResponse {
                sequence_number: request.sequence_number,
                result: ClientResult::Error {
                    message: "System Overloaded".to_string(),
                },
            };
        }

        // Step 4: Increment in-flight counter BEFORE submitting
        self.inflight_count.fetch_add(1, Ordering::SeqCst);

        // Step 5: Submit the request to the log
        match self.submit(&request.payload) {
            Ok(log_index) => {
                // Track this pending request
                self.pending_requests
                    .insert(log_index, (request.client_id, request.sequence_number));

                // Return pending - the client should wait for commit
                ClientResponse {
                    sequence_number: request.sequence_number,
                    result: ClientResult::Pending,
                }
            }
            Err(e) => {
                // Decrement in-flight counter on failure
                self.inflight_count.fetch_sub(1, Ordering::SeqCst);

                let response = ClientResponse {
                    sequence_number: request.sequence_number,
                    result: ClientResult::Error {
                        message: format!("Failed to append to log: {}", e),
                    },
                };

                // Record the error response for idempotency
                self.session_map
                    .record_response(request.client_id, response.clone());

                response
            }
        }
    }

    /// Check if a pending request has been committed and return its response.
    ///
    /// Call this after processing messages to get responses for committed requests.
    /// Also decrements the in-flight counter for each committed request.
    pub fn check_committed_requests(&mut self) -> Vec<(u64, ClientResponse)> {
        let committed_index = match self.committed_state.committed_index() {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let mut responses = Vec::new();

        // Find all pending requests that have been committed
        let committed_keys: Vec<u64> = self
            .pending_requests
            .keys()
            .filter(|&&idx| idx <= committed_index)
            .copied()
            .collect();

        for log_index in committed_keys {
            if let Some((client_id, sequence_number)) = self.pending_requests.remove(&log_index) {
                // Decrement in-flight counter for committed request
                self.inflight_count.fetch_sub(1, Ordering::SeqCst);

                let response = ClientResponse {
                    sequence_number,
                    result: ClientResult::Success { log_index },
                };

                // Record for idempotency
                self.session_map
                    .record_response(client_id, response.clone());

                responses.push((client_id, response));
            }
        }

        responses
    }

    /// Get the session map (for snapshotting).
    pub fn session_map(&self) -> &SessionMap {
        &self.session_map
    }

    /// Restore the session map (from snapshot).
    pub fn restore_session_map(&mut self, session_map: SessionMap) {
        self.session_map = session_map;
    }

    /// Handle a PrepareOk response (Primary only).
    ///
    /// 1. Update QuorumTracker
    /// 2. If quorum reached AND all previous indices committed: advance committed_index
    /// 3. Trigger executor.step() for newly committed entries
    pub fn handle_prepare_ok(&mut self, index: u64, from_node: u32) {
        assert_eq!(
            self.role,
            NodeRole::Primary,
            "Only Primary handles PrepareOk"
        );

        // Extract values from tracker first to avoid borrow issues
        let current_committed = self.committed_state.committed_index();
        let (reached_quorum, committable) = {
            let tracker = self
                .quorum_tracker
                .as_mut()
                .expect("Primary must have tracker");
            let reached = tracker.record_prepare_ok(index, from_node);
            let committable = if reached {
                tracker.committable_index_from(current_committed)
            } else {
                None
            };
            (reached, committable)
        };

        if reached_quorum {
            if let Some(committable) = committable {
                let current_committed = self.committed_state.committed_index();

                // Only advance if this is higher than current
                let should_advance = match current_committed {
                    None => true,
                    Some(c) => committable > c,
                };

                if should_advance {
                    // Advance committed_index
                    self.committed_state.advance(committable);

                    // Trigger executor for newly committed entries
                    self.apply_committed_entries(current_committed, committable);

                    // Broadcast commit notification to backups
                    let commit_msg = VsrMessage::Commit {
                        view: self.view,
                        commit_index: committable,
                    };
                    self.network.broadcast(commit_msg);
                    self.last_sent = Instant::now();

                    // Garbage collect old tracking data
                    if let Some(ref mut tracker) = self.quorum_tracker {
                        tracker.gc(committable);
                    }
                }
            }
        }
    }

    // =========================================================================
    // BACKUP ROLE
    // =========================================================================

    /// Handle a Prepare message (Backup only).
    ///
    /// # Fencing Invariant (CRITICAL)
    /// Before processing, we verify `view >= manifest.highest_view()`.
    /// This prevents zombie leaders from corrupting state after view change.
    ///
    /// 1. **DURABLE FENCE CHECK** - reject if view < highest_view
    /// 2. Verify msg.index == next_expected_index
    /// 3. Append payload to local log
    /// 4. Send PrepareOk back to Primary
    /// 5. Update local committed_index to match msg.commit_index
    pub fn handle_prepare(
        &mut self,
        from_node: u32,
        view: u64,
        index: u64,
        payload: &[u8],
        commit_index: Option<u64>,
        timestamp_ns: u64,
    ) -> Result<(), VsrError> {
        assert_eq!(self.role, NodeRole::Backup, "Only Backup handles Prepare");

        // ============================================================
        // STEP 0: DURABLE VIEW FENCE CHECK (CRITICAL FOR CORRECTNESS)
        // This MUST happen before any state mutation.
        // Reject messages from views lower than our durable highest_view.
        // ============================================================
        if let Err(_e) = self.manifest.check_view_fence(view) {
            self.fenced_message_count += 1;
            return Err(VsrError::FencingViolation {
                view,
                fence: self.manifest.highest_view(),
            });
        }

        // Reset election timeout - we heard from the Primary
        self.last_primary_contact = Instant::now();

        // Step 1: Verify view matches current view
        // (This is stricter than fence - we only accept current view)
        if view != self.view {
            return Err(VsrError::ViewMismatch {
                expected: self.view,
                received: view,
            });
        }

        // Step 2: Check for index gap and handle catch-up
        if index > self.next_expected_index {
            // Gap detected - buffer this entry and request catch-up
            self.catch_up_buffer
                .insert(index, (payload.to_vec(), timestamp_ns, 0, 0));

            if !self.catch_up_pending {
                // Send catch-up request to primary
                let catch_up_request = VsrMessage::CatchUpRequest {
                    view: self.view,
                    node_id: self.node_id,
                    from_index: self.next_expected_index,
                    to_index: index - 1,
                };
                self.network.send_to(from_node, catch_up_request);
                self.catch_up_pending = true;
            }

            return Ok(()); // Don't error - we're handling it
        } else if index < self.next_expected_index {
            // Duplicate or old entry - ignore but still ack
            let prepare_ok = VsrMessage::PrepareOk {
                index,
                node_id: self.node_id,
            };
            self.network.send_to(from_node, prepare_ok);
            return Ok(());
        }

        // Step 3: Append to local log with consensus timestamp from Primary
        let written_index = self.writer.append(payload, 0, 0, timestamp_ns)?;

        if written_index != index {
            return Err(VsrError::IndexMismatch {
                expected: index,
                received: written_index,
            });
        }

        self.next_expected_index = index + 1;

        // Step 4: Send PrepareOk back to Primary
        let prepare_ok = VsrMessage::PrepareOk {
            index,
            node_id: self.node_id,
        };
        self.network.send_to(from_node, prepare_ok);

        // Step 5: Update local committed_index
        // CRITICAL: Only advance if Primary has actually committed something (commit_index is Some)
        // This fixes the safety bug where backups would incorrectly commit index 0 before quorum
        if let Some(primary_commit) = commit_index {
            let current_committed = self.committed_state.committed_index();
            let should_advance = match current_committed {
                None => true, // Primary has commits, we have none - advance
                Some(c) => primary_commit > c,
            };

            if should_advance {
                // Only advance up to what we have locally
                let new_commit = primary_commit.min(index);
                self.committed_state.advance(new_commit);

                // Apply committed entries
                self.apply_committed_entries(current_committed, new_commit);
            }
        }
        // If commit_index is None, Primary has no commits yet - do NOT advance

        Ok(())
    }

    /// Handle a PrepareBatch message (Backup only).
    ///
    /// # Fencing Invariant (CRITICAL)
    /// Before processing, we verify `view >= manifest.highest_view()`.
    /// This prevents zombie leaders from corrupting state after view change.
    ///
    /// 1. **DURABLE FENCE CHECK** - reject if view < highest_view
    /// 2. Verify view and starting index
    /// 3. Append all entries to local log (single fdatasync via append_batch)
    /// 4. Send PrepareOk for EACH entry (per-entry quorum tracking)
    /// 5. Update local committed_index
    pub fn handle_prepare_batch(
        &mut self,
        from_node: u32,
        view: u64,
        start_index: u64,
        entries: &[PreparedEntry],
        commit_index: Option<u64>,
        timestamp_ns: u64,
    ) -> Result<(), VsrError> {
        assert_eq!(
            self.role,
            NodeRole::Backup,
            "Only Backup handles PrepareBatch"
        );

        // ============================================================
        // STEP 0: DURABLE VIEW FENCE CHECK (CRITICAL FOR CORRECTNESS)
        // This MUST happen before any state mutation.
        // ============================================================
        if let Err(_e) = self.manifest.check_view_fence(view) {
            self.fenced_message_count += 1;
            return Err(VsrError::FencingViolation {
                view,
                fence: self.manifest.highest_view(),
            });
        }

        // Reset election timeout
        self.last_primary_contact = Instant::now();

        // Step 1: Verify view matches current view
        if view != self.view {
            return Err(VsrError::ViewMismatch {
                expected: self.view,
                received: view,
            });
        }

        // Step 2: Verify starting index
        if start_index != self.next_expected_index {
            return Err(VsrError::IndexMismatch {
                expected: self.next_expected_index,
                received: start_index,
            });
        }

        if entries.is_empty() {
            return Err(VsrError::EmptyBatch);
        }

        // Verify entries are contiguous
        for (i, entry) in entries.iter().enumerate() {
            let expected_idx = start_index + i as u64;
            if entry.index != expected_idx {
                return Err(VsrError::IndexMismatch {
                    expected: expected_idx,
                    received: entry.index,
                });
            }
        }

        // Collect indices for PrepareOk
        let indices: Vec<u64> = entries.iter().map(|e| e.index).collect();
        let expected_last = start_index + entries.len() as u64 - 1;

        // Step 3: Append batch to local log
        // Check if async durability mode is enabled
        if let Some(ref handle) = self.durability_handle {
            // Async mode: enqueue to DurabilityWorker, defer PrepareOk until completion
            let payloads: Vec<Vec<u8>> = entries.iter().map(|e| e.payload.clone()).collect();

            let (batch_id, _start_idx, last_idx) = handle.submit_batch(payloads, timestamp_ns)?;

            // Update next_expected_index speculatively
            self.next_expected_index = last_idx + 1;

            // Track this pending backup durability operation
            self.pending_backup_durability.insert(
                batch_id,
                PendingBackupDurability {
                    from_node,
                    indices,
                    commit_index,
                    last_written_index: last_idx,
                },
            );

            // PrepareOk and commit advancement deferred to process_durability_completions
            Ok(())
        } else {
            // Sync mode: blocking I/O path (original behavior)
            let payloads: Vec<Vec<u8>> = entries.iter().map(|e| e.payload.clone()).collect();
            let last_index = self.writer.append_batch(&payloads, timestamp_ns)?;

            if last_index != expected_last {
                return Err(VsrError::IndexMismatch {
                    expected: expected_last,
                    received: last_index,
                });
            }

            self.next_expected_index = last_index + 1;

            // Step 4: Send PrepareOk for EACH entry (per-entry quorum tracking)
            // This is critical: even though entries arrive in a batch, quorum
            // tracking must be at the entry level for correct commit advancement.
            for idx in indices {
                let prepare_ok = VsrMessage::PrepareOk {
                    index: idx,
                    node_id: self.node_id,
                };
                self.network.send_to(from_node, prepare_ok);
            }

            // Step 5: Update local committed_index
            // CRITICAL: Only advance if Primary has actually committed something (commit_index is Some)
            // This fixes the safety bug where backups would incorrectly commit before quorum
            if let Some(primary_commit) = commit_index {
                let current_committed = self.committed_state.committed_index();
                let should_advance = match current_committed {
                    None => true, // Primary has commits, we have none - advance
                    Some(c) => primary_commit > c,
                };

                if should_advance {
                    // Only advance up to what we have locally
                    let new_commit = primary_commit.min(last_index);
                    self.committed_state.advance(new_commit);
                    self.apply_committed_entries(current_committed, new_commit);
                }
            }
            // If commit_index is None, Primary has no commits yet - do NOT advance

            Ok(())
        }
    }

    // =========================================================================
    // COMMON
    // =========================================================================

    /// Apply committed entries to the executor.
    fn apply_committed_entries(&mut self, old_committed: Option<u64>, new_committed: u64) {
        if let Some(ref mut executor) = self.executor {
            let start = match old_committed {
                None => 0,
                Some(c) => c + 1,
            };

            for _idx in start..=new_committed {
                // Step the executor for each newly committed entry
                match executor.step() {
                    Ok(_) => {}
                    Err(e) => {
                        // Executor errors are fatal - the state machine is corrupted
                        // or the log is inconsistent. Node must halt.
                        panic!(
                            "FATAL: Node {}: Executor error during apply: {:?}. \
                             State machine may be corrupted.",
                            self.node_id, e
                        );
                    }
                }
            }
        }
    }

    // =========================================================================
    // CATCH-UP PROTOCOL
    // =========================================================================

    /// Handle a CatchUpRequest (Primary only).
    ///
    /// Reads the requested entries from the log using LogReader and sends them to the backup.
    ///
    /// # Safety
    /// Only sends committed entries - entries that have passed the commit point and are
    /// durable on a quorum of nodes. This ensures backups receive correct data.
    pub fn handle_catch_up_request(
        &mut self,
        from_node: u32,
        view: u64,
        _node_id: u32,
        from_index: u64,
        to_index: u64,
    ) -> Result<(), VsrError> {
        if self.role != NodeRole::Primary {
            return Err(VsrError::NotPrimary);
        }

        if view != self.view {
            return Err(VsrError::ViewMismatch {
                expected: self.view,
                received: view,
            });
        }

        // Get the LogReader - Primary must have one
        let reader = self.reader.as_mut().ok_or(VsrError::NoLogReader)?;

        // Get committed index - only send committed entries
        let committed = match self.committed_state.committed_index() {
            Some(idx) => idx,
            None => {
                // No entries committed yet - nothing to send
                return Ok(());
            }
        };

        // Can only send committed entries
        let actual_to = to_index.min(committed);
        if from_index > actual_to {
            // Nothing to send yet - entries not committed
            return Ok(());
        }

        // Limit batch size to avoid huge messages (100 entries or 1MB, whichever is smaller)
        const MAX_CATCH_UP_ENTRIES: u64 = 100;
        let batch_end = actual_to.min(from_index + MAX_CATCH_UP_ENTRIES - 1);

        // Read entries from the log using LogReader
        let log_entries = reader.read_range(from_index, batch_end).map_err(|e| {
            VsrError::LogReadFailed(format!(
                "Failed to read [{}, {}]: {}",
                from_index, batch_end, e
            ))
        })?;

        // Convert LogEntry to CatchUpEntry
        let entries: Vec<super::message::CatchUpEntry> = log_entries
            .into_iter()
            .map(|entry| super::message::CatchUpEntry {
                index: entry.index,
                payload: entry.payload,
                timestamp_ns: entry.timestamp_ns,
                stream_id: entry.stream_id,
                flags: entry.flags,
            })
            .collect();

        let has_more = batch_end < actual_to;

        let response = VsrMessage::CatchUpResponse {
            view: self.view,
            entries,
            has_more,
            commit_index: committed,
        };

        self.network.send_to(from_node, response);
        Ok(())
    }

    /// Handle a CatchUpResponse (Backup only).
    ///
    /// Applies the received entries to fill the gap, then drains the buffer.
    pub fn handle_catch_up_response(
        &mut self,
        from_node: u32,
        view: u64,
        entries: &[super::message::CatchUpEntry],
        has_more: bool,
        commit_index: u64,
    ) -> Result<(), VsrError> {
        if self.role != NodeRole::Backup {
            return Err(VsrError::NotBackup);
        }

        if view != self.view {
            return Err(VsrError::ViewMismatch {
                expected: self.view,
                received: view,
            });
        }

        // Reset election timeout
        self.last_primary_contact = Instant::now();

        // Apply entries in order
        for entry in entries {
            if entry.index == self.next_expected_index {
                // Write to log
                self.writer.append(
                    &entry.payload,
                    entry.stream_id,
                    entry.flags,
                    entry.timestamp_ns,
                )?;
                self.next_expected_index = entry.index + 1;

                // Send PrepareOk for this entry
                let prepare_ok = VsrMessage::PrepareOk {
                    index: entry.index,
                    node_id: self.node_id,
                };
                self.network.send_to(from_node, prepare_ok);
            }
        }

        // Try to drain buffered entries
        self.drain_catch_up_buffer(from_node);

        // If there are more entries to fetch, request them
        if has_more {
            let last_received = entries
                .last()
                .map(|e| e.index)
                .unwrap_or(self.next_expected_index);
            let catch_up_request = VsrMessage::CatchUpRequest {
                view: self.view,
                node_id: self.node_id,
                from_index: last_received + 1,
                to_index: commit_index, // Request up to commit point
            };
            self.network.send_to(from_node, catch_up_request);
        } else {
            self.catch_up_pending = false;
        }

        // Update committed index
        let current_committed = self.committed_state.committed_index();
        let should_advance = match current_committed {
            None => commit_index > 0,
            Some(c) => commit_index > c,
        };

        if should_advance {
            let new_commit = commit_index.min(self.next_expected_index.saturating_sub(1));
            if new_commit > current_committed.unwrap_or(0) || current_committed.is_none() {
                self.committed_state.advance(new_commit);
                self.apply_committed_entries(current_committed, new_commit);
            }
        }

        Ok(())
    }

    /// Drain buffered entries that can now be applied.
    fn drain_catch_up_buffer(&mut self, primary_node: u32) {
        loop {
            if let Some((payload, timestamp_ns, stream_id, flags)) =
                self.catch_up_buffer.remove(&self.next_expected_index)
            {
                let index = self.next_expected_index;

                // Write to log
                if let Err(e) = self.writer.append(&payload, stream_id, flags, timestamp_ns) {
                    // Log append failure is fatal - we cannot proceed without durable writes
                    panic!(
                        "FATAL: Node {}: Failed to append buffered entry {}: {}. \
                         Cannot proceed without durable writes.",
                        self.node_id, index, e
                    );
                }

                self.next_expected_index = index + 1;

                // Send PrepareOk
                let prepare_ok = VsrMessage::PrepareOk {
                    index,
                    node_id: self.node_id,
                };
                self.network.send_to(primary_node, prepare_ok);
            } else {
                break;
            }
        }
    }

    /// Process one incoming message.
    ///
    /// Returns true if a message was processed.
    pub fn process_one(&mut self) -> bool {
        if let Some((from_node, msg)) = self.network.try_recv() {
            match msg {
                VsrMessage::Prepare {
                    view,
                    index,
                    payload,
                    commit_index,
                    timestamp_ns,
                } => {
                    if self.role == NodeRole::Backup {
                        if let Err(e) = self.handle_prepare(
                            from_node,
                            view,
                            index,
                            &payload,
                            commit_index,
                            timestamp_ns,
                        ) {
                            warn!(node_id = self.node_id, view, index, error = %e, "Prepare error");
                        }
                    }
                }
                VsrMessage::PrepareBatch {
                    view,
                    start_index,
                    entries,
                    commit_index,
                    timestamp_ns,
                } => {
                    if self.role == NodeRole::Backup {
                        if let Err(e) = self.handle_prepare_batch(
                            from_node,
                            view,
                            start_index,
                            &entries,
                            commit_index,
                            timestamp_ns,
                        ) {
                            warn!(node_id = self.node_id, view, start_index, error = %e, "PrepareBatch error");
                        }
                    }
                }
                VsrMessage::PrepareOk { index, node_id } => {
                    if self.role == NodeRole::Primary {
                        self.handle_prepare_ok(index, node_id);
                    }
                }
                VsrMessage::Commit { view, commit_index } => {
                    if self.role == NodeRole::Backup && view == self.view {
                        // Reset election timeout - we heard from the Primary
                        self.last_primary_contact = Instant::now();

                        let current = self.committed_state.committed_index();
                        if current.map(|c| commit_index > c).unwrap_or(true) {
                            self.committed_state.advance(commit_index);
                            self.apply_committed_entries(current, commit_index);
                        }
                    }
                }
                VsrMessage::StartViewChange { new_view, node_id } => {
                    self.handle_start_view_change(new_view, node_id);
                }
                VsrMessage::DoViewChange {
                    new_view,
                    node_id,
                    commit_index,
                    last_log_index,
                    last_log_hash,
                    log_suffix,
                } => {
                    self.handle_do_view_change(
                        new_view,
                        node_id,
                        commit_index,
                        last_log_index,
                        last_log_hash,
                        log_suffix,
                    );
                }
                VsrMessage::StartView {
                    new_view,
                    primary_id,
                    commit_index,
                    last_log_index,
                    log_entries,
                } => {
                    self.handle_start_view(
                        new_view,
                        primary_id,
                        commit_index,
                        last_log_index,
                        log_entries,
                    );
                }
                VsrMessage::CatchUpRequest {
                    view,
                    node_id,
                    from_index,
                    to_index,
                } => {
                    if self.role == NodeRole::Primary {
                        if let Err(e) = self
                            .handle_catch_up_request(from_node, view, node_id, from_index, to_index)
                        {
                            warn!(node_id = self.node_id, view, from_index, to_index, error = %e, "CatchUpRequest error");
                        }
                    }
                }
                VsrMessage::CatchUpResponse {
                    view,
                    entries,
                    has_more,
                    commit_index,
                } => {
                    if self.role == NodeRole::Backup {
                        if let Err(e) = self.handle_catch_up_response(
                            from_node,
                            view,
                            &entries,
                            has_more,
                            commit_index,
                        ) {
                            warn!(node_id = self.node_id, view, entries_count = entries.len(), error = %e, "CatchUpResponse error");
                        }
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Process all pending messages.
    pub fn process_all(&mut self) {
        while self.process_one() {}
    }

    /// Get the current committed index.
    pub fn committed_index(&self) -> Option<u64> {
        self.committed_state.committed_index()
    }

    /// Get the next index to be written.
    pub fn next_index(&self) -> u64 {
        self.writer.next_index()
    }

    /// Get the fdatasync count from the log writer.
    /// Used for testing group commit efficiency.
    pub fn fdatasync_count(&self) -> u64 {
        self.writer.fdatasync_count()
    }

    /// Get the current view number.
    ///
    /// This is the fencing token for SideEffectManager integration.
    /// When integrating SideEffectManager, call:
    /// - `manager.set_primary_with_token(true, node.current_view())` when becoming Primary
    /// - `manager.set_primary_with_token(false, node.current_view())` when stepping down
    /// - `manager.advance_fence(node.current_view())` on any view change
    pub fn current_view(&self) -> u64 {
        self.view
    }

    /// Check if this node is currently the Primary.
    ///
    /// For SideEffectManager integration, use this with `current_view()`:
    /// ```ignore
    /// manager.set_primary_with_token(node.is_primary(), node.current_view());
    /// ```
    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }

    // =========================================================================
    // HEARTBEAT AND FAILURE DETECTION
    // =========================================================================

    /// Periodic tick for heartbeat and failure detection.
    ///
    /// This should be called periodically (e.g., every 10-20ms).
    ///
    /// For Primary:
    /// - **FIRST**: Send heartbeat if idle (DECOUPLED from durability)
    /// - Then: Check for batch timeout and flush
    ///
    /// For Backup:
    /// - If no message from Primary for ELECTION_TIMEOUT, start view change.
    ///
    /// # Architectural Divorce: Control vs Data Plane
    /// Heartbeats are part of the **control plane** and must be sent regardless
    /// of pending durability work (data plane). This prevents false elections
    /// during disk stalls when async durability is enabled.
    ///
    /// Returns true if a state change occurred (e.g., started view change, batch flushed).
    pub fn tick(&mut self) -> bool {
        match self.role {
            NodeRole::Primary => {
                let mut state_changed = false;

                // ============================================================
                // CONTROL PLANE: Heartbeat (ALWAYS runs first, never blocked)
                // ============================================================
                // Send heartbeat if idle - this is decoupled from durability.
                // Even if disk I/O is stalled, heartbeats must continue to
                // prevent backups from starting unnecessary view changes.
                if self.last_sent.elapsed() >= HEARTBEAT_INTERVAL {
                    self.send_heartbeat();
                }

                // ============================================================
                // DATA PLANE: Batch flush (may be async or sync)
                // ============================================================
                // Check for batch timeout and flush.
                // In async mode, this enqueues to DurabilityWorker (non-blocking).
                // In sync mode, this blocks on disk I/O.
                if self.batcher.should_flush_timeout() {
                    match self.flush_batch() {
                        Ok(tracking) => {
                            // In sync mode, record pending requests immediately.
                            // In async mode, tracking is empty (deferred to completions).
                            for (log_index, client_id, sequence_number) in tracking {
                                self.pending_requests
                                    .insert(log_index, (client_id, sequence_number));
                            }
                            state_changed = true;
                        }
                        Err(e) => {
                            // Batch flush failure is fatal - we cannot proceed without durable writes
                            panic!(
                                "FATAL: Node {}: Batch flush error: {}. \
                                 Cannot proceed without durable writes.",
                                self.node_id, e
                            );
                        }
                    }
                }

                state_changed
            }
            NodeRole::Backup => {
                // Check for election timeout
                if self.last_primary_contact.elapsed() >= ELECTION_TIMEOUT {
                    self.start_view_change();
                    true
                } else {
                    false
                }
            }
            NodeRole::ViewChangeInProgress => {
                // Periodically re-broadcast StartViewChange
                // (In a full implementation, we'd have a separate timer for this)
                false
            }
        }
    }

    /// Send a heartbeat (Commit message) to all backups.
    fn send_heartbeat(&mut self) {
        let commit_index = self.committed_state.committed_index().unwrap_or(0);
        let heartbeat = VsrMessage::Commit {
            view: self.view,
            commit_index,
        };
        self.network.broadcast(heartbeat);
        self.last_sent = Instant::now();
    }

    /// Start the view change process.
    ///
    /// # Durable Fencing
    /// Before broadcasting StartViewChange, we persist the new view in the manifest.
    /// This ensures we never regress to a lower view after restart.
    ///
    /// Transitions to ViewChangeInProgress and broadcasts StartViewChange.
    fn start_view_change(&mut self) {
        // Increment proposed view
        self.proposed_view = self.view + 1;

        // ============================================================
        // DURABLE FENCE: Persist new view BEFORE broadcasting
        // This ensures we reject lower-view messages after restart.
        // ============================================================
        if let Err(e) = self.manifest.advance_view(self.proposed_view) {
            error!(node_id = self.node_id, proposed_view = self.proposed_view, error = %e, "FATAL: Failed to persist view to manifest");
            // Cannot proceed without durable fencing - this is a fatal error
            panic!("Failed to persist view change: {}", e);
        }

        self.role = NodeRole::ViewChangeInProgress;

        // Broadcast StartViewChange
        let msg = VsrMessage::StartViewChange {
            new_view: self.proposed_view,
            node_id: self.node_id,
        };
        self.network.broadcast(msg);

        info!(
            node_id = self.node_id,
            proposed_view = self.proposed_view,
            old_view = self.view,
            durable_view = self.manifest.highest_view(),
            "Started view change"
        );
    }

    /// Check if this node is in view change progress.
    pub fn is_view_change_in_progress(&self) -> bool {
        self.role == NodeRole::ViewChangeInProgress
    }

    /// Get the proposed view number (during view change).
    pub fn proposed_view(&self) -> u64 {
        self.proposed_view
    }

    // =========================================================================
    // VIEW CHANGE PROTOCOL
    // =========================================================================

    /// Determine the primary for a given view.
    /// Simple round-robin: primary = view % cluster_size
    fn primary_for_view(&self, view: u64) -> u32 {
        (view % self.cluster_size as u64) as u32
    }

    /// Handle a StartViewChange message.
    ///
    /// # Durable Fencing
    /// Before participating in view change, we persist the new view.
    /// This ensures we never accept messages from lower views after restart.
    ///
    /// Tracks votes and triggers view change when quorum is reached.
    fn handle_start_view_change(&mut self, new_view: u64, from_node: u32) {
        // ============================================================
        // DURABLE FENCE CHECK: Reject views lower than our durable fence
        // ============================================================
        if let Err(e) = self.manifest.check_view_fence(new_view) {
            self.fenced_message_count += 1;
            debug!(node_id = self.node_id, new_view, from_node, error = %e, "Rejecting StartViewChange - view fence");
            return;
        }

        // Ignore messages for views we've already passed (in-memory check)
        if new_view <= self.view {
            return;
        }

        // ============================================================
        // DURABLE FENCE: Persist new view BEFORE processing
        // This ensures we reject lower-view messages after restart.
        // ============================================================
        if let Err(e) = self.manifest.advance_view(new_view) {
            // Cannot proceed without durable fencing - this is fatal
            panic!(
                "FATAL: Node {}: Failed to persist view {} to manifest: {}. \
                 Cannot proceed without durable fencing.",
                self.node_id, new_view, e
            );
        }

        // Record the vote using bitset
        let votes = self
            .start_view_change_votes
            .entry(new_view)
            .or_insert_with(NodeBitset::new);
        votes.insert(from_node);

        // Also count our own vote if we're in view change for this view
        if self.role == NodeRole::ViewChangeInProgress && self.proposed_view == new_view {
            votes.insert(self.node_id);
        }

        let vote_count = votes.count();
        let quorum_size = (self.cluster_size / 2) + 1;

        debug!(
            node_id = self.node_id,
            new_view, from_node, vote_count, quorum_size, "StartViewChange vote received"
        );

        // Check if quorum reached
        if vote_count >= quorum_size {
            self.on_view_change_quorum(new_view);
        }
    }

    /// Called when quorum is reached for a view change.
    fn on_view_change_quorum(&mut self, new_view: u64) {
        let new_primary = self.primary_for_view(new_view);

        info!(
            node_id = self.node_id,
            new_view, new_primary, "View change quorum reached"
        );

        if new_primary == self.node_id {
            // I am the new primary - wait for DoViewChange messages
            self.role = NodeRole::ViewChangeInProgress;
            self.proposed_view = new_view;

            // Add our own DoViewChange info
            let my_info = self.create_do_view_change_info();
            let msgs = self
                .do_view_change_msgs
                .entry(new_view)
                .or_insert_with(Vec::new);
            if !msgs.iter().any(|m| m.node_id == self.node_id) {
                msgs.push(my_info);
            }

            // Check if we already have quorum of DoViewChange
            self.check_do_view_change_quorum(new_view);
        } else {
            // I am a backup for the new view - send DoViewChange to new primary
            if !self.sent_do_view_change || self.proposed_view != new_view {
                self.send_do_view_change(new_view, new_primary);
                self.sent_do_view_change = true;
                self.proposed_view = new_view;
            }
        }
    }

    /// Create DoViewChange info for this node.
    fn create_do_view_change_info(&mut self) -> DoViewChangeInfo {
        let commit_index = self.committed_state.committed_index().unwrap_or(0);
        let last_log_index = if self.writer.next_index() > 0 {
            self.writer.next_index() - 1
        } else {
            0
        };

        // Get the actual tail hash from the LogWriter
        // This is the chain hash of the last entry, used for log reconciliation
        let last_log_hash = self.writer.tail_hash();

        // Collect log suffix (uncommitted entries after commit_index)
        // These are entries that may need to be reconciled during view change
        let log_suffix = self.collect_log_suffix(commit_index, last_log_index);

        DoViewChangeInfo {
            node_id: self.node_id,
            commit_index,
            last_log_index,
            last_log_hash,
            log_suffix,
        }
    }

    /// Collect uncommitted log entries for DoViewChange message.
    /// Returns entries from (commit_index + 1) to last_log_index.
    fn collect_log_suffix(
        &mut self,
        commit_index: u64,
        last_log_index: u64,
    ) -> Vec<LogEntrySummary> {
        if last_log_index <= commit_index {
            return Vec::new();
        }

        // Use LogReader if available to read uncommitted entries
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => {
                // No reader available - return empty suffix
                // The new Primary will need to use catch-up protocol
                return Vec::new();
            }
        };

        // Read entries from commit_index + 1 to last_log_index
        // Note: These are uncommitted entries, so we read directly without visibility check
        let start = commit_index + 1;
        match reader.read_range(start, last_log_index) {
            Ok(entries) => entries
                .into_iter()
                .map(|e| LogEntrySummary {
                    index: e.index,
                    payload: e.payload,
                })
                .collect(),
            Err(e) => {
                warn!(
                    node_id = self.node_id,
                    start,
                    last_log_index,
                    error = %e,
                    "Failed to read log suffix"
                );
                Vec::new()
            }
        }
    }

    /// Send DoViewChange to the new primary.
    fn send_do_view_change(&mut self, new_view: u64, new_primary: u32) {
        let info = self.create_do_view_change_info();

        let msg = VsrMessage::DoViewChange {
            new_view,
            node_id: self.node_id,
            commit_index: info.commit_index,
            last_log_index: info.last_log_index,
            last_log_hash: info.last_log_hash,
            log_suffix: info.log_suffix,
        };

        self.network.send_to(new_primary, msg);

        debug!(
            node_id = self.node_id,
            new_view, new_primary, "Sent DoViewChange"
        );
    }

    /// Handle a DoViewChange message (new Primary only).
    fn handle_do_view_change(
        &mut self,
        new_view: u64,
        from_node: u32,
        commit_index: u64,
        last_log_index: u64,
        last_log_hash: [u8; 16],
        log_suffix: Vec<LogEntrySummary>,
    ) {
        // Only process if we're the designated primary for this view
        let expected_primary = self.primary_for_view(new_view);
        if expected_primary != self.node_id {
            return;
        }

        // Ignore if we've already moved past this view
        if new_view < self.proposed_view {
            return;
        }

        debug!(
            node_id = self.node_id,
            new_view, from_node, last_log_index, "Received DoViewChange"
        );

        // Store the DoViewChange info
        let info = DoViewChangeInfo {
            node_id: from_node,
            commit_index,
            last_log_index,
            last_log_hash,
            log_suffix,
        };

        let msgs = self
            .do_view_change_msgs
            .entry(new_view)
            .or_insert_with(Vec::new);
        if !msgs.iter().any(|m| m.node_id == from_node) {
            msgs.push(info);
        }

        self.check_do_view_change_quorum(new_view);
    }

    /// Check if we have quorum of DoViewChange messages and can become Primary.
    fn check_do_view_change_quorum(&mut self, new_view: u64) {
        let quorum_size = (self.cluster_size / 2) + 1;

        let msgs = match self.do_view_change_msgs.get(&new_view) {
            Some(m) => m,
            None => return,
        };

        if msgs.len() as u32 >= quorum_size {
            self.become_primary(new_view);
        }
    }

    /// Become the new Primary after collecting DoViewChange quorum.
    ///
    /// # Durable Fencing (CRITICAL)
    /// Before becoming Primary, we MUST persist the new view in the manifest.
    /// This ensures:
    /// 1. We reject messages from lower views after restart
    /// 2. Zombie leaders from lower views cannot corrupt our state
    fn become_primary(&mut self, new_view: u64) {
        // ============================================================
        // DURABLE FENCE: Persist view BEFORE becoming Primary
        // This is the critical fencing point for leader election.
        // ============================================================
        if let Err(e) = self.manifest.advance_view(new_view) {
            error!(node_id = self.node_id, new_view, error = %e, "FATAL: Failed to persist view before becoming Primary");
            // Cannot become Primary without durable fencing - this is fatal
            panic!("Failed to persist view for Primary transition: {}", e);
        }

        info!(
            node_id = self.node_id,
            new_view,
            durable_view = self.manifest.highest_view(),
            "Becoming Primary"
        );

        // Find the "Master Truth" - the log with the highest index
        // In case of tie, prefer the one with the lexicographically highest hash
        // This ensures deterministic selection across all nodes
        let msgs = self.do_view_change_msgs.get(&new_view).unwrap();
        let best = msgs
            .iter()
            .max_by(|a, b| match a.last_log_index.cmp(&b.last_log_index) {
                std::cmp::Ordering::Equal => a.last_log_hash.cmp(&b.last_log_hash),
                other => other,
            })
            .unwrap();

        let master_last_index = best.last_log_index;
        let _master_last_hash = best.last_log_hash;
        let master_commit_index = msgs.iter().map(|m| m.commit_index).max().unwrap_or(0);

        debug!(
            node_id = self.node_id,
            master_last_index, master_commit_index, "Master truth established for view change"
        );

        // Update our state
        self.view = new_view;
        self.proposed_view = new_view;
        self.role = NodeRole::Primary;
        self.quorum_tracker = Some(QuorumTracker::new(self.cluster_size, self.node_id));
        self.last_sent = Instant::now();

        // Update committed state if needed
        let current_committed = self.committed_state.committed_index().unwrap_or(0);
        if master_commit_index > current_committed {
            self.committed_state.advance(master_commit_index);
        }

        // Collect log entries to send to backups
        // For simplicity, we send entries from commit_index+1 to last_log_index
        let log_entries = self.collect_log_entries(master_commit_index + 1, master_last_index);

        // Broadcast StartView to all backups
        let msg = VsrMessage::StartView {
            new_view,
            primary_id: self.node_id,
            commit_index: master_commit_index,
            last_log_index: master_last_index,
            log_entries,
        };
        self.network.broadcast(msg);

        // Clear view change state
        self.start_view_change_votes.clear();
        self.do_view_change_msgs.clear();
        self.sent_do_view_change = false;
    }

    /// Collect log entries in a range for StartView message.
    ///
    /// Reads entries from the log using LogReader and returns them as LogEntrySummary.
    /// This is used during view change to send uncommitted entries to backups.
    fn collect_log_entries(&mut self, start: u64, end: u64) -> Vec<LogEntrySummary> {
        if start > end {
            return Vec::new();
        }

        // Use the LogReader to read entries
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => {
                warn!(
                    node_id = self.node_id,
                    "No LogReader available for collect_log_entries"
                );
                return Vec::new();
            }
        };

        // Read entries from the log
        match reader.read_range(start, end) {
            Ok(entries) => entries
                .into_iter()
                .map(|e| LogEntrySummary {
                    index: e.index,
                    payload: e.payload,
                })
                .collect(),
            Err(e) => {
                warn!(node_id = self.node_id, start, end, error = %e, "Failed to read log entries for view change");
                Vec::new()
            }
        }
    }

    /// Handle a StartView message (Backup transition).
    ///
    /// # Durable Fencing (CRITICAL)
    /// Before transitioning to Backup, we MUST persist the new view.
    /// This ensures we reject messages from lower views after restart.
    fn handle_start_view(
        &mut self,
        new_view: u64,
        primary_id: u32,
        commit_index: u64,
        last_log_index: u64,
        log_entries: Vec<LogEntrySummary>,
    ) {
        // ============================================================
        // DURABLE FENCE CHECK: Reject views lower than our durable fence
        // ============================================================
        if let Err(e) = self.manifest.check_view_fence(new_view) {
            self.fenced_message_count += 1;
            debug!(node_id = self.node_id, new_view, primary_id, error = %e, "Rejecting StartView - view fence");
            return;
        }

        // In-memory fencing: reject messages from old views
        if new_view <= self.view {
            debug!(
                node_id = self.node_id,
                new_view,
                current_view = self.view,
                "Ignoring StartView for old view"
            );
            return;
        }

        // ============================================================
        // DURABLE FENCE: Persist view BEFORE transitioning to Backup
        // ============================================================
        if let Err(e) = self.manifest.advance_view(new_view) {
            // Cannot proceed without durable fencing - this is fatal
            panic!(
                "FATAL: Node {}: Failed to persist view {} for StartView: {}. \
                 Cannot proceed without durable fencing.",
                self.node_id, new_view, e
            );
        }

        info!(
            node_id = self.node_id,
            new_view,
            primary_id,
            durable_view = self.manifest.highest_view(),
            "Received StartView"
        );

        // Transition to Backup role
        self.view = new_view;
        self.proposed_view = new_view;
        self.role = NodeRole::Backup;
        self.last_primary_contact = Instant::now();

        // Update committed state
        let current_committed = self.committed_state.committed_index().unwrap_or(0);
        if commit_index > current_committed {
            self.committed_state.advance(commit_index);
            self.apply_committed_entries(Some(current_committed), commit_index);
        }

        // Apply log entries from the new Primary to sync our log
        // These are uncommitted entries that the new Primary has but we might be missing
        if !log_entries.is_empty() {
            debug!(
                node_id = self.node_id,
                entries_count = log_entries.len(),
                first_index = log_entries.first().map(|e| e.index).unwrap_or(0),
                last_index = log_entries.last().map(|e| e.index).unwrap_or(0),
                "Applying log entries from StartView"
            );

            // Get consensus timestamp for these entries (use current time as fallback)
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            for entry in &log_entries {
                // Only append if this is the next expected entry
                if entry.index == self.next_expected_index {
                    if let Err(e) = self.writer.append(&entry.payload, 0, 0, timestamp_ns) {
                        // Log append failure is fatal
                        panic!(
                            "FATAL: Node {}: Failed to append entry {} from StartView: {}. \
                             Cannot proceed without durable writes.",
                            self.node_id, entry.index, e
                        );
                    }
                    self.next_expected_index = entry.index + 1;
                }
            }
        }

        // Update next_expected_index based on last_log_index from Primary
        // This ensures we're ready to receive new entries at the right index
        if last_log_index >= self.next_expected_index {
            // We might be behind - will need to catch up via normal Prepare/CatchUp
            debug!(
                node_id = self.node_id,
                next_expected = self.next_expected_index,
                primary_last = last_log_index,
                "After StartView - may need catch-up"
            );
        }

        // Clear view change state
        self.start_view_change_votes.clear();
        self.do_view_change_msgs.clear();
        self.sent_do_view_change = false;

        info!(node_id = self.node_id, new_view, "Transitioned to Backup");
    }
}
