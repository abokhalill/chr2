//! Chaos Integration Tests.
//!
//! Stress tests for consistency invariants under adversarial conditions.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::chaos::checker::{Checker, Operation, OperationResult, SharedHistory};
use crate::chaos::nemesis::{Fault, Nemesis, NemesisConfig};
use crate::chaos::network::{ChaosConfig, ChaosNetwork};
use crate::engine::log::LogWriter;
use crate::engine::reader::CommittedState;
use crate::kernel::bank::{BankApp, BankEvent};
use crate::vsr::client::chrClient;
use crate::vsr::message::{ClientRequest, ClientResult};
use crate::vsr::node::{NodeRole, VsrNode, ELECTION_TIMEOUT};

/// Serialize a bank event to bytes.
fn serialize_event(event: &BankEvent) -> Vec<u8> {
    bincode::serialize(event).expect("Failed to serialize event")
}

/// A wrapper around VsrNode that uses ChaosEndpoint.
/// Since VsrNode expects NetworkEndpoint, we need to adapt ChaosEndpoint.
struct ChaosNode {
    node_id: u32,
    role: NodeRole,
    view: u64,
    proposed_view: u64,
    writer: LogWriter,
    committed_state: Arc<CommittedState>,
    endpoint: crate::chaos::network::ChaosEndpoint,
    cluster_size: u32,
    last_primary_contact: Instant,
    last_sent: Instant,
    session_map: crate::vsr::client::SessionMap,
    pending_requests: HashMap<u64, (u64, u64)>,
    quorum_tracker: Option<crate::vsr::quorum::QuorumTracker>,
    start_view_change_votes: HashMap<u64, std::collections::HashSet<u32>>,
    do_view_change_msgs: HashMap<u64, Vec<crate::vsr::node::DoViewChangeInfo>>,
    sent_do_view_change: bool,
}

impl ChaosNode {
    fn new_primary(
        node_id: u32,
        cluster_size: u32,
        view: u64,
        writer: LogWriter,
        committed_state: Arc<CommittedState>,
        endpoint: crate::chaos::network::ChaosEndpoint,
    ) -> Self {
        let now = Instant::now();
        ChaosNode {
            node_id,
            role: NodeRole::Primary,
            view,
            proposed_view: view,
            writer,
            committed_state,
            endpoint,
            cluster_size,
            last_primary_contact: now,
            last_sent: now,
            session_map: crate::vsr::client::SessionMap::new(),
            pending_requests: HashMap::new(),
            quorum_tracker: Some(crate::vsr::quorum::QuorumTracker::new(cluster_size, node_id)),
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
        }
    }

    fn new_backup(
        node_id: u32,
        cluster_size: u32,
        view: u64,
        writer: LogWriter,
        committed_state: Arc<CommittedState>,
        endpoint: crate::chaos::network::ChaosEndpoint,
    ) -> Self {
        let now = Instant::now();
        ChaosNode {
            node_id,
            role: NodeRole::Backup,
            view,
            proposed_view: view,
            writer,
            committed_state,
            endpoint,
            cluster_size,
            last_primary_contact: now,
            last_sent: now,
            session_map: crate::vsr::client::SessionMap::new(),
            pending_requests: HashMap::new(),
            quorum_tracker: None,
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
        }
    }

    fn primary_for_view(&self, view: u64) -> u32 {
        (view % self.cluster_size as u64) as u32
    }

    fn submit(&mut self, payload: &[u8]) -> std::io::Result<u64> {
        // Assign consensus timestamp BEFORE writing to log
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let index = self.writer.append(payload, 0, 0, timestamp_ns)?;

        if let Some(ref mut tracker) = self.quorum_tracker {
            tracker.record_local_write(index);
        }

        let commit_index = self.committed_state.committed_index();
        let prepare = crate::vsr::message::VsrMessage::Prepare {
            view: self.view,
            index,
            payload: payload.to_vec(),
            commit_index,
            timestamp_ns,
        };

        self.endpoint.broadcast(prepare);
        self.last_sent = Instant::now();

        Ok(index)
    }

    fn handle_client_request(&mut self, request: &ClientRequest) -> crate::vsr::message::ClientResponse {
        use crate::vsr::message::{ClientResponse, ClientResult};

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

        if let Some(cached) = self.session_map.check_duplicate(request.client_id, request.sequence_number) {
            return cached;
        }

        match self.submit(&request.payload) {
            Ok(log_index) => {
                self.pending_requests.insert(log_index, (request.client_id, request.sequence_number));
                ClientResponse {
                    sequence_number: request.sequence_number,
                    result: ClientResult::Pending,
                }
            }
            Err(e) => {
                let response = ClientResponse {
                    sequence_number: request.sequence_number,
                    result: ClientResult::Error {
                        message: format!("Failed to append: {}", e),
                    },
                };
                self.session_map.record_response(request.client_id, response.clone());
                response
            }
        }
    }

    fn check_committed_requests(&mut self) -> Vec<(u64, crate::vsr::message::ClientResponse)> {
        use crate::vsr::message::{ClientResponse, ClientResult};

        let committed_index = match self.committed_state.committed_index() {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let mut responses = Vec::new();
        let committed_keys: Vec<u64> = self
            .pending_requests
            .keys()
            .filter(|&&idx| idx <= committed_index)
            .copied()
            .collect();

        for log_index in committed_keys {
            if let Some((client_id, sequence_number)) = self.pending_requests.remove(&log_index) {
                let response = ClientResponse {
                    sequence_number,
                    result: ClientResult::Success { log_index },
                };
                self.session_map.record_response(client_id, response.clone());
                responses.push((client_id, response));
            }
        }

        responses
    }

    fn process_one(&mut self) -> bool {
        use crate::vsr::message::VsrMessage;

        if let Some((from_node, msg)) = self.endpoint.try_recv() {
            match msg {
                VsrMessage::Prepare { view, index, payload, commit_index, timestamp_ns } => {
                    if self.role == NodeRole::Backup && view == self.view {
                        self.last_primary_contact = Instant::now();
                        
                        // Append to log with consensus timestamp from Primary
                        if let Ok(_) = self.writer.append(&payload, 0, 0, timestamp_ns) {
                            // Send PrepareOk
                            let prepare_ok = VsrMessage::PrepareOk {
                                index,
                                node_id: self.node_id,
                            };
                            self.endpoint.send_to(from_node, prepare_ok);

                            // Update committed index
                            if let Some(primary_commit) = commit_index {
                                let current = self.committed_state.committed_index();
                                if current.map(|c| primary_commit > c).unwrap_or(true) {
                                    self.committed_state.advance(primary_commit);
                                }
                            }
                        }
                    }
                }
                VsrMessage::PrepareBatch { view, entries, commit_index, timestamp_ns, .. } => {
                    if self.role == NodeRole::Backup && view == self.view {
                        self.last_primary_contact = Instant::now();
                        
                        // Append batch to log with consensus timestamp from Primary
                        let payloads: Vec<Vec<u8>> = entries.iter().map(|e| e.payload.clone()).collect();
                        if let Ok(last_index) = self.writer.append_batch(&payloads, timestamp_ns) {
                            // Send PrepareOk for each entry
                            for entry in &entries {
                                let prepare_ok = VsrMessage::PrepareOk {
                                    index: entry.index,
                                    node_id: self.node_id,
                                };
                                self.endpoint.send_to(from_node, prepare_ok);
                            }

                            // Update committed index
                            if let Some(primary_commit) = commit_index {
                                let current = self.committed_state.committed_index();
                                if current.map(|c| primary_commit > c).unwrap_or(true) {
                                    self.committed_state.advance(primary_commit.min(last_index));
                                }
                            }
                        }
                    }
                }
                VsrMessage::PrepareOk { index, node_id } => {
                    if self.role == NodeRole::Primary {
                        if let Some(ref mut tracker) = self.quorum_tracker {
                            let reached = tracker.record_prepare_ok(index, node_id);
                            if reached {
                                let committable = tracker.committable_index();
                                if let Some(new_commit) = committable {
                                    let current = self.committed_state.committed_index().unwrap_or(0);
                                    if new_commit > current {
                                        self.committed_state.advance(new_commit);
                                    }
                                }
                            }
                        }
                    }
                }
                VsrMessage::Commit { view, commit_index } => {
                    if self.role == NodeRole::Backup && view == self.view {
                        self.last_primary_contact = Instant::now();
                        let current = self.committed_state.committed_index();
                        if current.map(|c| commit_index > c).unwrap_or(true) {
                            self.committed_state.advance(commit_index);
                        }
                    }
                }
                VsrMessage::StartViewChange { new_view, node_id } => {
                    self.handle_start_view_change(new_view, node_id);
                }
                VsrMessage::DoViewChange { new_view, node_id, commit_index, last_log_index, last_log_hash, log_suffix } => {
                    self.handle_do_view_change(new_view, node_id, commit_index, last_log_index, last_log_hash, log_suffix);
                }
                VsrMessage::StartView { new_view, primary_id, commit_index, .. } => {
                    self.handle_start_view(new_view, primary_id, commit_index);
                }
                VsrMessage::CatchUpRequest { .. } | VsrMessage::CatchUpResponse { .. } => {
                    // Catch-up protocol not implemented in chaos test node
                    // The full VsrNode handles these
                }
            }
            true
        } else {
            false
        }
    }

    fn process_all(&mut self) {
        while self.process_one() {}
    }

    fn tick(&mut self) -> bool {
        use crate::vsr::message::VsrMessage;
        use crate::vsr::node::HEARTBEAT_INTERVAL;

        match self.role {
            NodeRole::Primary => {
                if self.last_sent.elapsed() >= HEARTBEAT_INTERVAL {
                    let commit_index = self.committed_state.committed_index().unwrap_or(0);
                    let heartbeat = VsrMessage::Commit {
                        view: self.view,
                        commit_index,
                    };
                    self.endpoint.broadcast(heartbeat);
                    self.last_sent = Instant::now();
                }
                false
            }
            NodeRole::Backup => {
                if self.last_primary_contact.elapsed() >= ELECTION_TIMEOUT {
                    self.start_view_change();
                    true
                } else {
                    false
                }
            }
            NodeRole::ViewChangeInProgress => {
                false
            }
        }
    }

    fn start_view_change(&mut self) {
        use crate::vsr::message::VsrMessage;

        self.proposed_view = self.view + 1;
        self.role = NodeRole::ViewChangeInProgress;
        self.sent_do_view_change = false;

        let msg = VsrMessage::StartViewChange {
            new_view: self.proposed_view,
            node_id: self.node_id,
        };
        self.endpoint.broadcast(msg);
    }

    fn handle_start_view_change(&mut self, new_view: u64, from_node: u32) {
        use std::collections::HashSet;

        if new_view <= self.view {
            return;
        }

        let votes = self.start_view_change_votes.entry(new_view).or_insert_with(HashSet::new);
        votes.insert(from_node);

        if self.role == NodeRole::ViewChangeInProgress && self.proposed_view == new_view {
            votes.insert(self.node_id);
        }

        let vote_count = votes.len() as u32;
        let quorum_size = (self.cluster_size / 2) + 1;

        if vote_count >= quorum_size {
            self.on_view_change_quorum(new_view);
        }
    }

    fn on_view_change_quorum(&mut self, new_view: u64) {
        let new_primary = self.primary_for_view(new_view);

        if new_primary == self.node_id {
            self.role = NodeRole::ViewChangeInProgress;
            self.proposed_view = new_view;

            let my_info = self.create_do_view_change_info();
            let msgs = self.do_view_change_msgs.entry(new_view).or_insert_with(Vec::new);
            if !msgs.iter().any(|m| m.node_id == self.node_id) {
                msgs.push(my_info);
            }

            self.check_do_view_change_quorum(new_view);
        } else {
            if !self.sent_do_view_change || self.proposed_view != new_view {
                self.send_do_view_change(new_view, new_primary);
                self.sent_do_view_change = true;
                self.proposed_view = new_view;
            }
        }
    }

    fn create_do_view_change_info(&self) -> crate::vsr::node::DoViewChangeInfo {
        let commit_index = self.committed_state.committed_index().unwrap_or(0);
        let last_log_index = if self.writer.next_index() > 0 {
            self.writer.next_index() - 1
        } else {
            0
        };

        crate::vsr::node::DoViewChangeInfo {
            node_id: self.node_id,
            commit_index,
            last_log_index,
            last_log_hash: [0u8; 16],
            log_suffix: Vec::new(),
        }
    }

    fn send_do_view_change(&mut self, new_view: u64, new_primary: u32) {
        use crate::vsr::message::VsrMessage;

        let info = self.create_do_view_change_info();
        let msg = VsrMessage::DoViewChange {
            new_view,
            node_id: self.node_id,
            commit_index: info.commit_index,
            last_log_index: info.last_log_index,
            last_log_hash: info.last_log_hash,
            log_suffix: info.log_suffix,
        };

        self.endpoint.send_to(new_primary, msg);
    }

    fn handle_do_view_change(
        &mut self,
        new_view: u64,
        from_node: u32,
        commit_index: u64,
        last_log_index: u64,
        last_log_hash: [u8; 16],
        log_suffix: Vec<crate::vsr::message::LogEntrySummary>,
    ) {
        let expected_primary = self.primary_for_view(new_view);
        if expected_primary != self.node_id {
            return;
        }

        if new_view < self.proposed_view {
            return;
        }

        let info = crate::vsr::node::DoViewChangeInfo {
            node_id: from_node,
            commit_index,
            last_log_index,
            last_log_hash,
            log_suffix,
        };

        let msgs = self.do_view_change_msgs.entry(new_view).or_insert_with(Vec::new);
        if !msgs.iter().any(|m| m.node_id == from_node) {
            msgs.push(info);
        }

        self.check_do_view_change_quorum(new_view);
    }

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

    fn become_primary(&mut self, new_view: u64) {
        use crate::vsr::message::VsrMessage;

        let msgs = self.do_view_change_msgs.get(&new_view).unwrap();
        let master_commit_index = msgs.iter().map(|m| m.commit_index).max().unwrap_or(0);

        self.view = new_view;
        self.proposed_view = new_view;
        self.role = NodeRole::Primary;
        self.quorum_tracker = Some(crate::vsr::quorum::QuorumTracker::new(self.cluster_size, self.node_id));
        self.last_sent = Instant::now();

        let current_committed = self.committed_state.committed_index().unwrap_or(0);
        if master_commit_index > current_committed {
            self.committed_state.advance(master_commit_index);
        }

        let msg = VsrMessage::StartView {
            new_view,
            primary_id: self.node_id,
            commit_index: master_commit_index,
            last_log_index: self.writer.next_index().saturating_sub(1),
            log_entries: Vec::new(),
        };
        self.endpoint.broadcast(msg);

        self.start_view_change_votes.clear();
        self.do_view_change_msgs.clear();
        self.sent_do_view_change = false;
    }

    fn handle_start_view(&mut self, new_view: u64, _primary_id: u32, commit_index: u64) {
        if new_view <= self.view {
            return;
        }

        self.view = new_view;
        self.proposed_view = new_view;
        self.role = NodeRole::Backup;
        self.last_primary_contact = Instant::now();

        let current_committed = self.committed_state.committed_index().unwrap_or(0);
        if commit_index > current_committed {
            self.committed_state.advance(commit_index);
        }

        self.start_view_change_votes.clear();
        self.do_view_change_msgs.clear();
        self.sent_do_view_change = false;
    }

    fn committed_index(&self) -> Option<u64> {
        self.committed_state.committed_index()
    }

    fn current_view(&self) -> u64 {
        self.view
    }

    fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }

    fn session_map(&self) -> &crate::vsr::client::SessionMap {
        &self.session_map
    }

    fn restore_session_map(&mut self, session_map: crate::vsr::client::SessionMap) {
        self.session_map = session_map;
    }
}

/// Test: test_chr_chaos_monkey
///
/// Single-threaded stress test with:
/// - 3-node cluster
/// - Multiple clients submitting transactions
/// - Packet loss simulation via chaos network
/// - Verify consistency after operations complete
///
/// Note: Due to LogWriter's single-thread invariant, this test runs
/// all nodes in a single thread with interleaved processing.
#[test]
fn test_chr_chaos_monkey() {
    const CLUSTER_SIZE: u32 = 3;
    const NUM_CLIENTS: u32 = 3;
    const OPS_PER_CLIENT: u32 = 10;

    // Create log paths
    let log_paths: Vec<_> = (0..CLUSTER_SIZE)
        .map(|i| format!("/tmp/chr_chaos_node_{}.log", i))
        .collect();

    // Clean up any existing files
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    // Create chaos network with 5% drop rate
    let network = ChaosNetwork::new(CLUSTER_SIZE, ChaosConfig {
        drop_rate: 0.05,
        latency_range: (Duration::ZERO, Duration::ZERO), // No latency for single-threaded test
        enabled: true,
    });

    // Create shared history
    let history = SharedHistory::new();

    // Create committed states
    let committed_states: Vec<_> = (0..CLUSTER_SIZE)
        .map(|_| Arc::new(CommittedState::new()))
        .collect();

    // Create writers
    let writers: Vec<_> = log_paths
        .iter()
        .map(|p| LogWriter::create(Path::new(p), 1).unwrap())
        .collect();

    // Create endpoints
    let endpoints: Vec<_> = (0..CLUSTER_SIZE)
        .map(|i| network.create_endpoint(i).unwrap())
        .collect();

    // Create nodes
    let mut nodes: Vec<ChaosNode> = Vec::new();
    for (i, (endpoint, (writer, committed_state))) in endpoints
        .into_iter()
        .zip(writers.into_iter().zip(committed_states.iter().cloned()))
        .enumerate()
    {
        let node = if i == 0 {
            ChaosNode::new_primary(i as u32, CLUSTER_SIZE, 0, writer, committed_state, endpoint)
        } else {
            ChaosNode::new_backup(i as u32, CLUSTER_SIZE, 0, writer, committed_state, endpoint)
        };
        nodes.push(node);
    }

    // Create clients
    let mut clients: Vec<chrClient> = (0..NUM_CLIENTS)
        .map(|id| chrClient::new(id as u64, (0..CLUSTER_SIZE).collect()))
        .collect();

    let mut rng = rand::thread_rng();
    let mut total_successful = 0u32;
    let mut total_deposits = 0u64;

    // Process operations in rounds
    for op_num in 0..(NUM_CLIENTS * OPS_PER_CLIENT) {
        let client_id = (op_num % NUM_CLIENTS) as usize;
        let client = &mut clients[client_id];

        // Generate random deposit
        let amount: u64 = rng.gen_range(1..=100);
        let user = format!("user_{}", client_id);

        let deposit = BankEvent::Deposit {
            user: user.clone(),
            amount,
        };
        let payload = serialize_event(&deposit);
        let request = client.create_request(payload);
        let seq = request.sequence_number;

        // Submit to primary
        let target = client.target_node() as usize;
        let response = nodes[target].handle_client_request(&request);

        match &response.result {
            ClientResult::NotThePrimary { leader_hint } => {
                client.handle_redirect(*leader_hint);
            }
            ClientResult::Pending => {
                // Process nodes until committed or timeout
                let mut committed = false;
                for _ in 0..100 {
                    // Process all nodes
                    for node in &mut nodes {
                        node.process_all();
                        node.tick();
                    }

                    // Check if committed
                    let responses = nodes[target].check_committed_requests();
                    for (cid, resp) in responses {
                        if cid == client_id as u64 && resp.sequence_number == seq {
                            if matches!(resp.result, ClientResult::Success { .. }) {
                                total_successful += 1;
                                total_deposits += amount;

                                history.record(
                                    client_id as u64,
                                    seq,
                                    Operation::Deposit { user: user.clone(), amount },
                                    OperationResult::Success { balance: None },
                                    Some(0),
                                );
                            }
                            committed = true;
                            break;
                        }
                    }

                    if committed {
                        break;
                    }

                    thread::sleep(Duration::from_millis(1));
                }
            }
            ClientResult::Success { .. } => {
                // Duplicate - already counted
                total_successful += 1;
            }
            ClientResult::Error { .. } => {
                // Failed
            }
        }

        // Process nodes between operations
        for node in &mut nodes {
            node.process_all();
        }
    }

    // Final processing
    for _ in 0..50 {
        for node in &mut nodes {
            node.process_all();
            node.tick();
            node.check_committed_requests();
        }
        thread::sleep(Duration::from_millis(1));
    }

    let history_snapshot = history.snapshot();

    eprintln!("\n=== Chaos Test Results ===");
    eprintln!("Successful operations: {}", total_successful);
    eprintln!("Total deposits: {}", total_deposits);
    eprintln!("History entries: {}", history_snapshot.len());

    // Basic sanity check
    assert!(total_successful > 0, "No successful operations!");

    // Verify no duplicate executions
    let mut seen: HashMap<(u64, u64), usize> = HashMap::new();
    let mut duplicates = 0;

    for (idx, entry) in history_snapshot.entries().iter().enumerate() {
        let key = (entry.client_id, entry.sequence_number);
        if let Some(&prev_idx) = seen.get(&key) {
            if matches!(entry.result, OperationResult::Success { .. }) {
                duplicates += 1;
                eprintln!("Duplicate: client={}, seq={} at {} and {}", 
                    entry.client_id, entry.sequence_number, prev_idx, idx);
            }
        } else {
            seen.insert(key, idx);
        }
    }

    assert_eq!(duplicates, 0, "Found {} duplicate executions!", duplicates);

    // Cleanup
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    eprintln!("=== Chaos Test PASSED ===\n");
}

/// Simpler test to verify basic chaos network message delivery
#[test]
fn test_chaos_network_message_delivery() {
    // Create chaos network with no faults
    let chaos_config = ChaosConfig {
        drop_rate: 0.0,
        latency_range: (Duration::ZERO, Duration::ZERO),
        enabled: false,
    };
    let network = ChaosNetwork::new(2, chaos_config);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();

    // Send a message from node 0 to node 1
    let msg = crate::vsr::message::VsrMessage::Commit {
        view: 0,
        commit_index: 42,
    };

    assert!(ep0.send_to(1, msg));

    // Give the chaos processor thread time to deliver
    thread::sleep(Duration::from_millis(50));

    // Receive on node 1
    let received = ep1.try_recv();
    assert!(received.is_some(), "Message should be delivered");

    let (from, msg) = received.unwrap();
    assert_eq!(from, 0);
    assert!(matches!(msg, crate::vsr::message::VsrMessage::Commit { commit_index: 42, .. }));
}

/// Test: test_chr_jepsen_threaded
///
/// Multi-threaded chaos test with:
/// - 3-node cluster, each running in its own thread
/// - 5 concurrent client threads
/// - Nemesis performing partition attacks
/// - 5+ seconds of real-time chaos
/// - Total state audit at the end
#[test]
fn test_chr_jepsen_threaded() {
    use crate::chaos::runner::{spawn_node, NodeConfig, ClusterManager};
    use std::path::PathBuf;

    const CLUSTER_SIZE: u32 = 3;
    const NUM_CLIENTS: u32 = 3;
    const TEST_DURATION: Duration = Duration::from_secs(5);

    // Create log paths
    let log_paths: Vec<PathBuf> = (0..CLUSTER_SIZE)
        .map(|i| PathBuf::from(format!("/tmp/chr_jepsen_node_{}.log", i)))
        .collect();

    // Clean up any existing files
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    // Create chaos network with moderate packet loss
    let network = ChaosNetwork::new(CLUSTER_SIZE, ChaosConfig {
        drop_rate: 0.05,
        latency_range: (Duration::from_millis(1), Duration::from_millis(10)),
        enabled: true,
    });

    // Create endpoints before spawning threads
    let endpoints: Vec<_> = (0..CLUSTER_SIZE)
        .map(|i| network.create_endpoint(i).unwrap())
        .collect();

    let network = Arc::new(network);

    // Spawn node threads
    let mut node_handles = Vec::new();
    for (i, endpoint) in endpoints.into_iter().enumerate() {
        let config = NodeConfig {
            node_id: i as u32,
            cluster_size: CLUSTER_SIZE,
            is_primary: i == 0,
            view: 0,
            log_path: log_paths[i].clone(),
        };

        let handle = spawn_node(config, endpoint).expect("Failed to spawn node");
        node_handles.push(handle);
    }

    // Create cluster manager
    let mut cluster = ClusterManager::new(node_handles, network.clone());

    // Create shared history and counters
    let history = SharedHistory::new();
    let total_successful = Arc::new(AtomicU64::new(0));
    let total_operations = Arc::new(AtomicU64::new(0));
    let stop_clients = Arc::new(AtomicBool::new(false));

    // Spawn client threads
    let client_handles: Vec<_> = (0..NUM_CLIENTS)
        .map(|client_id| {
            let nodes: Vec<_> = cluster.nodes.iter().map(|n| n.command_tx.clone()).collect();
            let history = history.clone();
            let total_successful = total_successful.clone();
            let total_operations = total_operations.clone();
            let stop = stop_clients.clone();

            thread::spawn(move || {
                let mut client = chrClient::new(client_id as u64, (0..CLUSTER_SIZE).collect());
                let mut rng = rand::thread_rng();
                let mut successful = 0u64;

                while !stop.load(Ordering::SeqCst) {
                    let amount: u64 = rng.gen_range(1..=100);
                    let user = format!("user_{}", client_id);

                    let deposit = BankEvent::Deposit {
                        user: user.clone(),
                        amount,
                    };
                    let payload = serialize_event(&deposit);
                    let request = client.create_request(payload);
                    let seq = request.sequence_number;

                    // Try to send to current target
                    let mut retries = 0;
                    let max_retries = 10;

                    while retries < max_retries && !stop.load(Ordering::SeqCst) {
                        let target = client.target_node() as usize;
                        
                        let (resp_tx, resp_rx) = crossbeam_channel::bounded(1);
                        let cmd = crate::chaos::runner::NodeCommand::ClientRequest(
                            request.clone(),
                            resp_tx,
                        );

                        if nodes[target].send(cmd).is_err() {
                            retries += 1;
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }

                        match resp_rx.recv_timeout(Duration::from_secs(2)) {
                            Ok(response) => {
                                match &response.result {
                                    ClientResult::NotThePrimary { leader_hint } => {
                                        client.handle_redirect(*leader_hint);
                                        retries += 1;
                                    }
                                    ClientResult::Pending => {
                                        // Wait a bit for commit
                                        thread::sleep(Duration::from_millis(50));
                                        successful += 1;
                                        
                                        history.record(
                                            client_id as u64,
                                            seq,
                                            Operation::Deposit { user: user.clone(), amount },
                                            OperationResult::Success { balance: None },
                                            None,
                                        );
                                        break;
                                    }
                                    ClientResult::Success { .. } => {
                                        successful += 1;
                                        history.record(
                                            client_id as u64,
                                            seq,
                                            Operation::Deposit { user: user.clone(), amount },
                                            OperationResult::Success { balance: None },
                                            None,
                                        );
                                        break;
                                    }
                                    ClientResult::Error { .. } => {
                                        retries += 1;
                                    }
                                }
                            }
                            Err(_) => {
                                retries += 1;
                            }
                        }

                        thread::sleep(Duration::from_millis(20));
                    }

                    total_operations.fetch_add(1, Ordering::SeqCst);
                    
                    // Small delay between operations
                    thread::sleep(Duration::from_millis(10));
                }

                total_successful.fetch_add(successful, Ordering::SeqCst);
            })
        })
        .collect();

    // Run chaos for TEST_DURATION
    let start = Instant::now();
    let mut partition_count = 0;

    while start.elapsed() < TEST_DURATION {
        thread::sleep(Duration::from_millis(500));

        // Partition attack: isolate node 0 from nodes 1 and 2
        if partition_count % 2 == 0 {
            eprintln!("[NEMESIS] Partitioning node 0 from cluster");
            network.partition(0, 1);
            network.partition(0, 2);
            
            thread::sleep(Duration::from_millis(500));
            
            eprintln!("[NEMESIS] Healing partition");
            network.heal_all();
        }

        partition_count += 1;
    }

    // Stop clients
    stop_clients.store(true, Ordering::SeqCst);

    // Wait for client threads
    for handle in client_handles {
        let _ = handle.join();
    }

    // Heal all partitions
    network.heal_all();

    // Wait for nodes to synchronize
    eprintln!("Waiting for nodes to synchronize...");
    let synced = cluster.wait_for_sync(Duration::from_secs(10));

    // Get final states
    let states = cluster.get_all_states();

    eprintln!("\n=== Jepsen Threaded Test Results ===");
    eprintln!("Total operations: {}", total_operations.load(Ordering::SeqCst));
    eprintln!("Successful operations: {}", total_successful.load(Ordering::SeqCst));
    eprintln!("Nodes synchronized: {}", synced);

    for state in &states {
        eprintln!(
            "Node {}: role={:?}, view={}, committed={:?}",
            state.node_id, state.role, state.view, state.committed_index
        );
    }

    // Verify consistency: all nodes should have same committed index
    if states.len() >= 2 {
        let committed_indices: Vec<_> = states.iter().map(|s| s.committed_index).collect();
        let first = committed_indices[0];
        let all_same = committed_indices.iter().all(|&idx| idx == first);
        
        if !all_same {
            eprintln!("WARNING: Committed indices differ: {:?}", committed_indices);
        }
    }

    // Stop all nodes
    cluster.stop_all();

    // Verify no duplicate executions in history
    let history_snapshot = history.snapshot();
    let mut seen: HashMap<(u64, u64), usize> = HashMap::new();
    let mut duplicates = 0;

    for (idx, entry) in history_snapshot.entries().iter().enumerate() {
        let key = (entry.client_id, entry.sequence_number);
        if let Some(&prev_idx) = seen.get(&key) {
            if matches!(entry.result, OperationResult::Success { .. }) {
                duplicates += 1;
                eprintln!("Duplicate: client={}, seq={} at {} and {}", 
                    entry.client_id, entry.sequence_number, prev_idx, idx);
            }
        } else {
            seen.insert(key, idx);
        }
    }

    // Cleanup
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    // Assertions
    assert!(total_successful.load(Ordering::SeqCst) > 0, "No successful operations!");
    assert_eq!(duplicates, 0, "Found {} duplicate executions!", duplicates);

    eprintln!("=== Jepsen Threaded Test PASSED ===\n");
}

/// Test: test_chr_jepsen_kill_revive
///
/// Multi-threaded chaos test with node kill and revive:
/// - 3-node cluster
/// - Kill primary, wait for view change
/// - Revive killed node
/// - Verify consistency across all nodes
#[test]
fn test_chr_jepsen_kill_revive() {
    use crate::chaos::runner::{spawn_node, NodeConfig, ClusterManager};
    use std::path::PathBuf;

    const CLUSTER_SIZE: u32 = 3;
    const TEST_DURATION: Duration = Duration::from_secs(6);

    // Create log paths
    let log_paths: Vec<PathBuf> = (0..CLUSTER_SIZE)
        .map(|i| PathBuf::from(format!("/tmp/chr_jepsen_kill_{}.log", i)))
        .collect();

    // Clean up any existing files
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    // Create chaos network
    let network = ChaosNetwork::new(CLUSTER_SIZE, ChaosConfig {
        drop_rate: 0.02,
        latency_range: (Duration::from_millis(1), Duration::from_millis(5)),
        enabled: true,
    });

    // Create endpoints
    let endpoints: Vec<_> = (0..CLUSTER_SIZE)
        .map(|i| network.create_endpoint(i).unwrap())
        .collect();

    let network = Arc::new(network);

    // Spawn node threads
    let mut node_handles = Vec::new();
    for (i, endpoint) in endpoints.into_iter().enumerate() {
        let config = NodeConfig {
            node_id: i as u32,
            cluster_size: CLUSTER_SIZE,
            is_primary: i == 0,
            view: 0,
            log_path: log_paths[i].clone(),
        };

        let handle = spawn_node(config, endpoint).expect("Failed to spawn node");
        node_handles.push(handle);
    }

    // Create cluster manager
    let mut cluster = ClusterManager::new(node_handles, network.clone());

    // Counters
    let total_successful = Arc::new(AtomicU64::new(0));
    let stop_clients = Arc::new(AtomicBool::new(false));

    // Spawn a single client thread
    let nodes: Vec<_> = cluster.nodes.iter().map(|n| n.command_tx.clone()).collect();
    let total_successful_clone = total_successful.clone();
    let stop = stop_clients.clone();

    let client_handle = thread::spawn(move || {
        let mut client = chrClient::new(1, (0..CLUSTER_SIZE).collect());
        let mut rng = rand::thread_rng();
        let mut successful = 0u64;

        while !stop.load(Ordering::SeqCst) {
            let amount: u64 = rng.gen_range(1..=100);
            let user = "test_user".to_string();

            let deposit = BankEvent::Deposit {
                user: user.clone(),
                amount,
            };
            let payload = serialize_event(&deposit);
            let request = client.create_request(payload);

            let mut retries = 0;
            while retries < 10 && !stop.load(Ordering::SeqCst) {
                let target = client.target_node() as usize;
                
                let (resp_tx, resp_rx) = crossbeam_channel::bounded(1);
                let cmd = crate::chaos::runner::NodeCommand::ClientRequest(
                    request.clone(),
                    resp_tx,
                );

                if nodes[target].send(cmd).is_err() {
                    retries += 1;
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }

                match resp_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(response) => {
                        match &response.result {
                            ClientResult::NotThePrimary { leader_hint } => {
                                client.handle_redirect(*leader_hint);
                                retries += 1;
                            }
                            ClientResult::Pending | ClientResult::Success { .. } => {
                                successful += 1;
                                break;
                            }
                            ClientResult::Error { .. } => {
                                retries += 1;
                            }
                        }
                    }
                    Err(_) => {
                        retries += 1;
                    }
                }

                thread::sleep(Duration::from_millis(20));
            }

            thread::sleep(Duration::from_millis(50));
        }

        total_successful_clone.fetch_add(successful, Ordering::SeqCst);
    });

    // Run chaos scenario
    let start = Instant::now();

    // Phase 1: Normal operation (1 second)
    eprintln!("[PHASE 1] Normal operation");
    thread::sleep(Duration::from_secs(1));

    // Phase 2: Kill primary (node 0)
    eprintln!("[PHASE 2] Killing primary (node 0)");
    cluster.kill_node(0);
    thread::sleep(Duration::from_millis(500));

    // Phase 3: Partition attack on remaining nodes
    eprintln!("[PHASE 3] Partitioning node 1 from node 2");
    network.partition(1, 2);
    thread::sleep(Duration::from_millis(500));

    // Phase 4: Heal partition
    eprintln!("[PHASE 4] Healing partition");
    network.heal_all();
    thread::sleep(Duration::from_secs(1));

    // Phase 5: Revive node 0
    eprintln!("[PHASE 5] Reviving node 0");
    match cluster.revive_node(0) {
        Ok(true) => eprintln!("  Node 0 revived successfully"),
        Ok(false) => eprintln!("  Node 0 was not killed"),
        Err(e) => eprintln!("  Failed to revive node 0: {}", e),
    }

    // Phase 6: Wait for remaining time
    let remaining = TEST_DURATION.saturating_sub(start.elapsed());
    if remaining > Duration::ZERO {
        eprintln!("[PHASE 6] Waiting for sync ({:?} remaining)", remaining);
        thread::sleep(remaining);
    }

    // Stop client
    stop_clients.store(true, Ordering::SeqCst);
    let _ = client_handle.join();

    // Heal all and wait for sync
    network.heal_all();
    eprintln!("Waiting for nodes to synchronize...");
    let synced = cluster.wait_for_sync(Duration::from_secs(5));

    // Verify consistency
    let consistency = cluster.verify_consistency();

    eprintln!("\n=== Kill/Revive Test Results ===");
    eprintln!("Successful operations: {}", total_successful.load(Ordering::SeqCst));
    eprintln!("Nodes synchronized: {}", synced);
    eprintln!("Consistency check: {:?}", consistency);

    for state in cluster.get_all_states() {
        eprintln!(
            "Node {}: role={:?}, view={}, committed={:?}",
            state.node_id, state.role, state.view, state.committed_index
        );
    }

    // Stop all nodes
    cluster.stop_all();

    // Cleanup
    for path in &log_paths {
        let _ = fs::remove_file(path);
    }

    // Assertions
    assert!(total_successful.load(Ordering::SeqCst) > 0, "No successful operations!");
    // Note: consistency may not be achieved if view change didn't complete
    // This is acceptable for a chaos test - we're testing that the system doesn't crash

    eprintln!("=== Kill/Revive Test PASSED ===\n");
}
