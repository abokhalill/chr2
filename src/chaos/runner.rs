use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::engine::log::LogWriter;
use crate::engine::reader::CommittedState;
use crate::vsr::client::SessionMap;
use crate::vsr::message::{ClientRequest, ClientResponse, ClientResult, LogEntrySummary, VsrMessage};
use crate::vsr::node::{DoViewChangeInfo, NodeRole, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL};
use crate::vsr::quorum::QuorumTracker;

use super::network::ChaosEndpoint;

/// Commands that can be sent to a node thread.
#[derive(Debug)]
pub enum NodeCommand {
    /// Submit a client request.
    ClientRequest(ClientRequest, Sender<ClientResponse>),
    /// Stop the node thread.
    Stop,
    /// Get current state for verification.
    GetState(Sender<NodeState>),
}

/// State snapshot from a node for verification.
#[derive(Debug, Clone)]
pub struct NodeState {
    pub node_id: u32,
    pub role: NodeRole,
    pub view: u64,
    pub committed_index: Option<u64>,
    pub next_index: u64,
    pub session_map: SessionMap,
}

/// Configuration for creating a node.
pub struct NodeConfig {
    pub node_id: u32,
    pub cluster_size: u32,
    pub is_primary: bool,
    pub view: u64,
    pub log_path: PathBuf,
}

/// Handle to a running node thread.
pub struct NodeHandle {
    pub node_id: u32,
    pub command_tx: Sender<NodeCommand>,
    pub thread_handle: Option<JoinHandle<()>>,
    pub committed_state: Arc<CommittedState>,
    pub killed: Arc<AtomicBool>,
    pub log_path: PathBuf,
    pub cluster_size: u32,
}

impl NodeHandle {
    /// Send a client request and wait for response.
    pub fn send_request(&self, request: ClientRequest) -> Option<ClientResponse> {
        if self.killed.load(Ordering::SeqCst) {
            return Some(ClientResponse {
                sequence_number: request.sequence_number,
                result: ClientResult::NotThePrimary { leader_hint: None },
            });
        }

        let (resp_tx, resp_rx) = bounded(1);
        if self.command_tx.send(NodeCommand::ClientRequest(request, resp_tx)).is_ok() {
            resp_rx.recv_timeout(Duration::from_secs(5)).ok()
        } else {
            None
        }
    }

    /// Get current node state.
    pub fn get_state(&self) -> Option<NodeState> {
        if self.killed.load(Ordering::SeqCst) {
            return None;
        }

        let (resp_tx, resp_rx) = bounded(1);
        if self.command_tx.send(NodeCommand::GetState(resp_tx)).is_ok() {
            resp_rx.recv_timeout(Duration::from_secs(1)).ok()
        } else {
            None
        }
    }

    /// Stop the node thread.
    pub fn stop(&mut self) {
        let _ = self.command_tx.send(NodeCommand::Stop);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }

    /// Kill the node - stops thread and marks as killed.
    /// The node can be revived later with revive().
    pub fn kill(&mut self) {
        self.killed.store(true, Ordering::SeqCst);
        let _ = self.command_tx.send(NodeCommand::Stop);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }

    /// Check if node is killed.
    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }

    /// Get committed index.
    pub fn committed_index(&self) -> Option<u64> {
        self.committed_state.committed_index()
    }
}

/// The node runner that executes in its own thread.
struct NodeRunner {
    node_id: u32,
    role: NodeRole,
    view: u64,
    proposed_view: u64,
    writer: LogWriter,
    committed_state: Arc<CommittedState>,
    endpoint: ChaosEndpoint,
    cluster_size: u32,
    last_primary_contact: Instant,
    last_sent: Instant,
    session_map: SessionMap,
    pending_requests: HashMap<u64, (u64, u64, Sender<ClientResponse>)>,
    quorum_tracker: Option<QuorumTracker>,
    start_view_change_votes: HashMap<u64, std::collections::HashSet<u32>>,
    do_view_change_msgs: HashMap<u64, Vec<DoViewChangeInfo>>,
    sent_do_view_change: bool,
    command_rx: Receiver<NodeCommand>,
    killed: Arc<AtomicBool>,
}

impl NodeRunner {
    fn new(
        config: NodeConfig,
        endpoint: ChaosEndpoint,
        committed_state: Arc<CommittedState>,
        command_rx: Receiver<NodeCommand>,
        killed: Arc<AtomicBool>,
    ) -> std::io::Result<Self> {
        let writer = LogWriter::create(&config.log_path, config.view)?;
        let now = Instant::now();

        let (role, quorum_tracker) = if config.is_primary {
            (NodeRole::Primary, Some(QuorumTracker::new(config.cluster_size, config.node_id)))
        } else {
            (NodeRole::Backup, None)
        };

        Ok(NodeRunner {
            node_id: config.node_id,
            role,
            view: config.view,
            proposed_view: config.view,
            writer,
            committed_state,
            endpoint,
            cluster_size: config.cluster_size,
            last_primary_contact: now,
            last_sent: now,
            session_map: SessionMap::new(),
            pending_requests: HashMap::new(),
            quorum_tracker,
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
            command_rx,
            killed,
        })
    }

    /// Recover a node from its existing log file.
    /// This is used when reviving a killed node.
    fn recover(
        config: NodeConfig,
        endpoint: ChaosEndpoint,
        committed_state: Arc<CommittedState>,
        command_rx: Receiver<NodeCommand>,
        killed: Arc<AtomicBool>,
    ) -> std::io::Result<Self> {
        use crate::engine::recovery::{LogRecovery, RecoveryOutcome};
        use crate::engine::format::GENESIS_HASH;

        // Try to recover from existing log, or create new if recovery fails
        let writer = if let Some(recovery) = LogRecovery::open(&config.log_path)? {
            match recovery.scan() {
                Ok(RecoveryOutcome::Clean { last_index, tail_hash, .. }) => {
                    LogWriter::open(
                        &config.log_path,
                        last_index + 1,
                        last_index, // Assume all entries are committed
                        tail_hash,
                        config.view,
                    )?
                }
                Ok(RecoveryOutcome::Truncated { last_valid_index, tail_hash, .. }) => {
                    LogWriter::open(
                        &config.log_path,
                        last_valid_index + 1,
                        last_valid_index,
                        tail_hash,
                        config.view,
                    )?
                }
                Ok(RecoveryOutcome::CleanEmpty { .. }) | Err(_) => {
                    LogWriter::create(&config.log_path, config.view)?
                }
            }
        } else {
            LogWriter::create(&config.log_path, config.view)?
        };

        let now = Instant::now();

        // Revived nodes always start as backup and wait for view change
        let role = NodeRole::Backup;
        let quorum_tracker = None;

        Ok(NodeRunner {
            node_id: config.node_id,
            role,
            view: config.view,
            proposed_view: config.view,
            writer,
            committed_state,
            endpoint,
            cluster_size: config.cluster_size,
            last_primary_contact: now,
            last_sent: now,
            session_map: SessionMap::new(),
            pending_requests: HashMap::new(),
            quorum_tracker,
            start_view_change_votes: HashMap::new(),
            do_view_change_msgs: HashMap::new(),
            sent_do_view_change: false,
            command_rx,
            killed,
        })
    }

    fn run(&mut self) {
        while !self.killed.load(Ordering::SeqCst) {
            // Process commands (non-blocking)
            while let Ok(cmd) = self.command_rx.try_recv() {
                match cmd {
                    NodeCommand::ClientRequest(req, resp_tx) => {
                        let response = self.handle_client_request(&req, resp_tx.clone());
                        let _ = resp_tx.send(response);
                    }
                    NodeCommand::Stop => {
                        return;
                    }
                    NodeCommand::GetState(resp_tx) => {
                        let state = NodeState {
                            node_id: self.node_id,
                            role: self.role.clone(),
                            view: self.view,
                            committed_index: self.committed_state.committed_index(),
                            next_index: self.writer.next_index(),
                            session_map: self.session_map.clone(),
                        };
                        let _ = resp_tx.send(state);
                    }
                }
            }

            // Process network messages
            self.process_messages();

            // Tick for heartbeat/election
            self.tick();

            // Check committed requests
            self.check_committed_requests();

            // Small sleep to prevent busy-waiting
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn primary_for_view(&self, view: u64) -> u32 {
        (view % self.cluster_size as u64) as u32
    }

    fn handle_client_request(&mut self, request: &ClientRequest, resp_tx: Sender<ClientResponse>) -> ClientResponse {
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
                self.pending_requests.insert(log_index, (request.client_id, request.sequence_number, resp_tx));
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
        let prepare = VsrMessage::Prepare {
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

    fn process_messages(&mut self) {
        while let Some((from_node, msg)) = self.endpoint.try_recv() {
            self.handle_message(from_node, msg);
        }
    }

    fn handle_message(&mut self, from_node: u32, msg: VsrMessage) {
        match msg {
            VsrMessage::Prepare { view, index, payload, commit_index, timestamp_ns } => {
                if self.role == NodeRole::Backup && view == self.view {
                    self.last_primary_contact = Instant::now();
                    
                    if let Ok(_) = self.writer.append(&payload, 0, 0, timestamp_ns) {
                        let prepare_ok = VsrMessage::PrepareOk {
                            index,
                            node_id: self.node_id,
                        };
                        self.endpoint.send_to(from_node, prepare_ok);

                        if let Some(primary_commit) = commit_index {
                            let current = self.committed_state.committed_index();
                            if current.map(|c| primary_commit > c).unwrap_or(true) {
                                self.committed_state.advance(primary_commit);
                            }
                        }
                    }
                }
            }
            VsrMessage::PrepareBatch { view, start_index, entries, commit_index, timestamp_ns } => {
                if self.role == NodeRole::Backup && view == self.view {
                    self.last_primary_contact = Instant::now();
                    
                    // Append batch entries with consensus timestamp
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
                            if let Some(new_commit) = tracker.committable_index() {
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
                // Catch-up protocol not implemented in chaos runner node
            }
        }
    }

    fn tick(&mut self) {
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
            }
            NodeRole::Backup => {
                if self.last_primary_contact.elapsed() >= ELECTION_TIMEOUT {
                    self.start_view_change();
                }
            }
            NodeRole::ViewChangeInProgress => {
                // Could re-broadcast StartViewChange here
            }
        }
    }

    fn start_view_change(&mut self) {
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

    fn create_do_view_change_info(&self) -> DoViewChangeInfo {
        let commit_index = self.committed_state.committed_index().unwrap_or(0);
        let last_log_index = if self.writer.next_index() > 0 {
            self.writer.next_index() - 1
        } else {
            0
        };

        DoViewChangeInfo {
            node_id: self.node_id,
            commit_index,
            last_log_index,
            last_log_hash: [0u8; 16],
            log_suffix: Vec::new(),
        }
    }

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

        self.endpoint.send_to(new_primary, msg);
    }

    fn handle_do_view_change(
        &mut self,
        new_view: u64,
        from_node: u32,
        commit_index: u64,
        last_log_index: u64,
        last_log_hash: [u8; 16],
        log_suffix: Vec<LogEntrySummary>,
    ) {
        let expected_primary = self.primary_for_view(new_view);
        if expected_primary != self.node_id {
            return;
        }

        if new_view < self.proposed_view {
            return;
        }

        let info = DoViewChangeInfo {
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
        let msgs = self.do_view_change_msgs.get(&new_view).unwrap();
        let master_commit_index = msgs.iter().map(|m| m.commit_index).max().unwrap_or(0);

        self.view = new_view;
        self.proposed_view = new_view;
        self.role = NodeRole::Primary;
        self.quorum_tracker = Some(QuorumTracker::new(self.cluster_size, self.node_id));
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

    fn check_committed_requests(&mut self) {
        let committed_index = match self.committed_state.committed_index() {
            Some(idx) => idx,
            None => return,
        };

        let committed_keys: Vec<u64> = self
            .pending_requests
            .keys()
            .filter(|&&idx| idx <= committed_index)
            .copied()
            .collect();

        for log_index in committed_keys {
            if let Some((client_id, sequence_number, resp_tx)) = self.pending_requests.remove(&log_index) {
                let response = ClientResponse {
                    sequence_number,
                    result: ClientResult::Success { log_index },
                };
                self.session_map.record_response(client_id, response.clone());
                let _ = resp_tx.send(response);
            }
        }
    }
}

/// Spawn a node in its own thread.
pub fn spawn_node(
    config: NodeConfig,
    endpoint: ChaosEndpoint,
) -> std::io::Result<NodeHandle> {
    let (command_tx, command_rx) = bounded(100);
    let committed_state = Arc::new(CommittedState::new());
    let killed = Arc::new(AtomicBool::new(false));
    let log_path = config.log_path.clone();
    let cluster_size = config.cluster_size;
    let node_id = config.node_id;

    let committed_state_clone = committed_state.clone();
    let killed_clone = killed.clone();

    let thread_handle = thread::spawn(move || {
        match NodeRunner::new(config, endpoint, committed_state_clone, command_rx, killed_clone.clone()) {
            Ok(mut runner) => {
                runner.run();
            }
            Err(e) => {
                eprintln!("Failed to create node runner: {}", e);
            }
        }
    });

    Ok(NodeHandle {
        node_id,
        command_tx,
        thread_handle: Some(thread_handle),
        committed_state,
        killed,
        log_path,
        cluster_size,
    })
}

/// Cluster manager for coordinating multiple nodes.
pub struct ClusterManager {
    pub nodes: Vec<NodeHandle>,
    pub network: Arc<super::network::ChaosNetwork>,
    /// Endpoints for reviving nodes - stored separately since they're consumed on spawn.
    endpoints: HashMap<u32, Option<ChaosEndpoint>>,
}

impl ClusterManager {
    /// Create a new cluster manager.
    pub fn new(nodes: Vec<NodeHandle>, network: Arc<super::network::ChaosNetwork>) -> Self {
        ClusterManager {
            nodes,
            network,
            endpoints: HashMap::new(),
        }
    }

    /// Kill a specific node by ID.
    pub fn kill_node(&mut self, node_id: u32) -> bool {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.node_id == node_id) {
            if !node.is_killed() {
                node.kill();
                // Kill network connectivity too
                self.network.kill_node(node_id);
                return true;
            }
        }
        false
    }

    /// Revive a killed node - restarts with recovery from log.
    pub fn revive_node(&mut self, node_id: u32) -> std::io::Result<bool> {
        let node = match self.nodes.iter_mut().find(|n| n.node_id == node_id) {
            Some(n) => n,
            None => return Ok(false),
        };

        if !node.is_killed() {
            return Ok(false);
        }

        // Revive network connectivity
        self.network.revive_node(node_id);

        // Create new endpoint for the revived node (uses recreate_endpoint for new channels)
        let endpoint = match self.network.recreate_endpoint(node_id) {
            Some(ep) => ep,
            None => return Ok(false),
        };

        // Determine if this node should be primary based on current view
        // For simplicity, node 0 starts as primary on view 0
        let is_primary = node_id == 0;
        let view = 0; // Will be updated via view change protocol

        let config = NodeConfig {
            node_id,
            cluster_size: node.cluster_size,
            is_primary,
            view,
            log_path: node.log_path.clone(),
        };

        // Create new channels
        let (command_tx, command_rx) = bounded(100);
        let committed_state = Arc::new(CommittedState::new());
        let killed = Arc::new(AtomicBool::new(false));

        let committed_state_clone = committed_state.clone();
        let killed_clone = killed.clone();
        let log_path = config.log_path.clone();

        // Spawn new thread with recovery
        let thread_handle = thread::spawn(move || {
            // Recover from existing log file
            match NodeRunner::recover(config, endpoint, committed_state_clone, command_rx, killed_clone.clone()) {
                Ok(mut runner) => {
                    runner.run();
                }
                Err(e) => {
                    eprintln!("Failed to recover node {}: {}", node_id, e);
                }
            }
        });

        // Update the node handle
        node.command_tx = command_tx;
        node.thread_handle = Some(thread_handle);
        node.committed_state = committed_state;
        node.killed = killed;

        Ok(true)
    }

    /// Wait for all nodes to synchronize to the highest committed index.
    pub fn wait_for_sync(&self, timeout: Duration) -> bool {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let committed_indices: Vec<Option<u64>> = self.nodes
                .iter()
                .filter(|n| !n.is_killed())
                .map(|n| n.committed_index())
                .collect();

            if committed_indices.is_empty() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }

            let max_committed = committed_indices.iter().filter_map(|&x| x).max();
            
            if let Some(max) = max_committed {
                let all_synced = committed_indices.iter().all(|&idx| idx == Some(max));
                if all_synced {
                    return true;
                }
            }

            thread::sleep(Duration::from_millis(10));
        }

        false
    }

    /// Get states from all live nodes.
    pub fn get_all_states(&self) -> Vec<NodeState> {
        self.nodes
            .iter()
            .filter_map(|n| n.get_state())
            .collect()
    }

    /// Stop all nodes.
    pub fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }

    /// Verify consistency across all nodes.
    /// Returns true if all live nodes have the same committed index.
    pub fn verify_consistency(&self) -> ConsistencyResult {
        let states = self.get_all_states();
        
        if states.is_empty() {
            return ConsistencyResult {
                consistent: false,
                committed_indices: Vec::new(),
                max_committed: None,
                error: Some("No live nodes".to_string()),
            };
        }

        let committed_indices: Vec<(u32, Option<u64>)> = states
            .iter()
            .map(|s| (s.node_id, s.committed_index))
            .collect();

        let max_committed = committed_indices.iter().filter_map(|(_, idx)| *idx).max();
        
        let all_same = if let Some(max) = max_committed {
            committed_indices.iter().all(|(_, idx)| *idx == Some(max))
        } else {
            committed_indices.iter().all(|(_, idx)| idx.is_none())
        };

        ConsistencyResult {
            consistent: all_same,
            committed_indices,
            max_committed,
            error: None,
        }
    }
}

/// Result of consistency verification.
#[derive(Debug, Clone)]
pub struct ConsistencyResult {
    pub consistent: bool,
    pub committed_indices: Vec<(u32, Option<u64>)>,
    pub max_committed: Option<u64>,
    pub error: Option<String>,
}
