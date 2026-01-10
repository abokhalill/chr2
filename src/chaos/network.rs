use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};
use rand::Rng;

use crate::vsr::message::VsrMessage;

/// Configuration for chaos network behavior.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of dropping a packet (0.0 - 1.0).
    pub drop_rate: f64,
    /// Range of latency to inject per packet.
    pub latency_range: (Duration, Duration),
    /// Whether chaos effects are enabled.
    pub enabled: bool,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        ChaosConfig {
            drop_rate: 0.0,
            latency_range: (Duration::ZERO, Duration::ZERO),
            enabled: true,
        }
    }
}

/// A chaos-enabled network endpoint for a single node.
pub struct ChaosEndpoint {
    /// This node's ID.
    pub node_id: u32,
    /// Receiver for incoming messages (after chaos processing).
    rx: Receiver<(u32, VsrMessage)>,
    /// Sender to the chaos processor.
    tx_to_chaos: Sender<(u32, u32, VsrMessage)>,
    /// Connection status to each node.
    connected: HashMap<u32, Arc<AtomicBool>>,
    /// Partition map (shared with ChaosNetwork).
    partition_map: Arc<RwLock<HashSet<(u32, u32)>>>,
    /// Whether this node is "killed".
    killed: Arc<AtomicBool>,
    /// Chaos configuration.
    config: Arc<RwLock<ChaosConfig>>,
    /// Statistics: messages sent.
    pub messages_sent: Arc<AtomicU64>,
    /// Statistics: messages dropped.
    pub messages_dropped: Arc<AtomicU64>,
}

impl ChaosEndpoint {
    /// Send a message to a specific node.
    pub fn send_to(&self, target_id: u32, msg: VsrMessage) -> bool {
        // Check if this node is killed
        if self.killed.load(Ordering::SeqCst) {
            return false;
        }

        // Check if connected
        if let Some(connected) = self.connected.get(&target_id) {
            if !connected.load(Ordering::SeqCst) {
                return false;
            }
        }

        // Check partition map
        {
            let partitions = self.partition_map.read().unwrap();
            if partitions.contains(&(self.node_id, target_id)) {
                self.messages_dropped.fetch_add(1, Ordering::SeqCst);
                return false;
            }
        }

        // Send to chaos processor
        self.messages_sent.fetch_add(1, Ordering::SeqCst);
        self.tx_to_chaos
            .send((self.node_id, target_id, msg))
            .is_ok()
    }

    /// Broadcast a message to all other nodes.
    pub fn broadcast(&self, msg: VsrMessage) -> usize {
        if self.killed.load(Ordering::SeqCst) {
            return 0;
        }

        let mut count = 0;
        for &target_id in self.connected.keys() {
            if self.send_to(target_id, msg.clone()) {
                count += 1;
            }
        }
        count
    }

    /// Try to receive a message (non-blocking).
    pub fn try_recv(&self) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) {
            return None;
        }
        self.rx.try_recv().ok()
    }

    /// Receive a message (blocking).
    pub fn recv(&self) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) {
            return None;
        }
        self.rx.recv().ok()
    }

    /// Receive with timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) {
            return None;
        }
        self.rx.recv_timeout(timeout).ok()
    }

    /// Check if this node is killed.
    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }
}

/// Chaos-enabled mock network.
pub struct ChaosNetwork {
    /// Number of nodes in the network.
    cluster_size: u32,
    /// Connection status between nodes.
    connections: HashMap<(u32, u32), Arc<AtomicBool>>,
    /// Partition map: set of (from, to) pairs that cannot communicate.
    partition_map: Arc<RwLock<HashSet<(u32, u32)>>>,
    /// Chaos configuration.
    config: Arc<RwLock<ChaosConfig>>,
    /// Senders to each node's inbox (after chaos processing).
    /// Wrapped in Arc<RwLock> for sharing with processor thread and updating on revive.
    node_senders: Arc<RwLock<HashMap<u32, Sender<(u32, VsrMessage)>>>>,
    /// Receivers for each node's inbox (taken when endpoint is created).
    /// Wrapped in Mutex for interior mutability when used via Arc.
    node_receivers: Mutex<HashMap<u32, Receiver<(u32, VsrMessage)>>>,
    /// Sender to the chaos processor thread.
    chaos_tx: Sender<(u32, u32, VsrMessage)>,
    /// Kill flags for each node.
    kill_flags: HashMap<u32, Arc<AtomicBool>>,
    /// Statistics per node.
    node_stats: HashMap<u32, (Arc<AtomicU64>, Arc<AtomicU64>)>,
    /// Handle to the chaos processor thread.
    _processor_handle: Option<thread::JoinHandle<()>>,
}

impl ChaosNetwork {
    /// Create a new chaos network with the specified number of nodes.
    pub fn new(cluster_size: u32, config: ChaosConfig) -> Self {
        let mut node_senders_map = HashMap::new();
        let mut node_receivers = HashMap::new();
        let mut connections = HashMap::new();
        let mut kill_flags = HashMap::new();
        let mut node_stats = HashMap::new();

        // Create a channel for each node
        for node_id in 0..cluster_size {
            let (tx, rx) = unbounded();
            node_senders_map.insert(node_id, tx);
            node_receivers.insert(node_id, rx);
            kill_flags.insert(node_id, Arc::new(AtomicBool::new(false)));
            node_stats.insert(
                node_id,
                (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))),
            );
        }

        // Create connection flags for all pairs
        for from in 0..cluster_size {
            for to in 0..cluster_size {
                if from != to {
                    connections.insert((from, to), Arc::new(AtomicBool::new(true)));
                }
            }
        }

        let partition_map = Arc::new(RwLock::new(HashSet::new()));
        let config = Arc::new(RwLock::new(config));
        let node_senders = Arc::new(RwLock::new(node_senders_map));

        // Create chaos processor channel
        let (chaos_tx, chaos_rx) = unbounded::<(u32, u32, VsrMessage)>();

        // Clone what we need for the processor thread
        let node_senders_clone = node_senders.clone();
        let config_clone = config.clone();
        let partition_map_clone = partition_map.clone();

        // Spawn chaos processor thread
        let processor_handle = thread::spawn(move || {
            Self::chaos_processor_shared(chaos_rx, node_senders_clone, config_clone, partition_map_clone);
        });

        ChaosNetwork {
            cluster_size,
            connections,
            partition_map,
            config,
            node_senders,
            node_receivers: Mutex::new(node_receivers),
            chaos_tx,
            kill_flags,
            node_stats,
            _processor_handle: Some(processor_handle),
        }
    }

    /// Chaos processor thread with shared senders map (for revive support).
    fn chaos_processor_shared(
        rx: Receiver<(u32, u32, VsrMessage)>,
        senders: Arc<RwLock<HashMap<u32, Sender<(u32, VsrMessage)>>>>,
        config: Arc<RwLock<ChaosConfig>>,
        partition_map: Arc<RwLock<HashSet<(u32, u32)>>>,
    ) {
        let mut rng = rand::thread_rng();

        while let Ok((from, to, msg)) = rx.recv() {
            let cfg = config.read().unwrap().clone();

            if !cfg.enabled {
                // Chaos disabled - deliver immediately
                let senders_guard = senders.read().unwrap();
                if let Some(tx) = senders_guard.get(&to) {
                    let _ = tx.send((from, msg));
                }
                continue;
            }

            // Check partition
            {
                let partitions = partition_map.read().unwrap();
                if partitions.contains(&(from, to)) {
                    continue; // Drop due to partition
                }
            }

            // Apply drop rate
            if cfg.drop_rate > 0.0 && rng.gen::<f64>() < cfg.drop_rate {
                continue; // Dropped
            }

            // Apply latency
            let (min_latency, max_latency) = cfg.latency_range;
            if max_latency > Duration::ZERO {
                let latency = if min_latency == max_latency {
                    min_latency
                } else {
                    let min_ms = min_latency.as_millis() as u64;
                    let max_ms = max_latency.as_millis() as u64;
                    Duration::from_millis(rng.gen_range(min_ms..=max_ms))
                };

                if latency > Duration::ZERO {
                    thread::sleep(latency);
                }
            }

            // Deliver message
            let senders_guard = senders.read().unwrap();
            if let Some(tx) = senders_guard.get(&to) {
                let _ = tx.send((from, msg));
            }
        }
    }

    /// Create a chaos-enabled network endpoint for a specific node.
    /// Uses interior mutability so it can be called via Arc.
    pub fn create_endpoint(&self, node_id: u32) -> Option<ChaosEndpoint> {
        let rx = self.node_receivers.lock().ok()?.remove(&node_id)?;

        // Build connected map
        let mut connected = HashMap::new();
        for (&(from, to), flag) in &self.connections {
            if from == node_id {
                connected.insert(to, flag.clone());
            }
        }

        let (sent, dropped) = self.node_stats.get(&node_id)?.clone();

        Some(ChaosEndpoint {
            node_id,
            rx,
            tx_to_chaos: self.chaos_tx.clone(),
            connected,
            partition_map: self.partition_map.clone(),
            killed: self.kill_flags.get(&node_id)?.clone(),
            config: self.config.clone(),
            messages_sent: sent,
            messages_dropped: dropped,
        })
    }

    /// Disconnect a node from the network.
    pub fn disconnect(&self, node_id: u32) {
        for (&(from, to), flag) in &self.connections {
            if from == node_id || to == node_id {
                flag.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Reconnect a node to the network.
    pub fn reconnect(&self, node_id: u32) {
        for (&(from, to), flag) in &self.connections {
            if from == node_id || to == node_id {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Kill a node (stops all message processing).
    pub fn kill_node(&self, node_id: u32) {
        if let Some(flag) = self.kill_flags.get(&node_id) {
            flag.store(true, Ordering::SeqCst);
        }
        self.disconnect(node_id);
    }

    /// Revive a killed node.
    pub fn revive_node(&self, node_id: u32) {
        if let Some(flag) = self.kill_flags.get(&node_id) {
            flag.store(false, Ordering::SeqCst);
        }
        self.reconnect(node_id);
    }

    /// Recreate an endpoint for a node (used when reviving).
    /// This creates a new channel for the node and updates the processor's senders map.
    pub fn recreate_endpoint(&self, node_id: u32) -> Option<ChaosEndpoint> {
        // Create a new channel for this node
        let (tx, rx) = unbounded();

        // Update the node sender in the processor's shared map
        {
            let mut senders = self.node_senders.write().ok()?;
            senders.insert(node_id, tx);
        }

        // Store receiver so create_endpoint can use it
        {
            let mut receivers = self.node_receivers.lock().ok()?;
            receivers.insert(node_id, rx);
        }

        // Now call create_endpoint which will take the receiver
        self.create_endpoint(node_id)
    }

    /// Add a network partition between two nodes.
    pub fn partition(&self, from: u32, to: u32) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.insert((from, to));
        partitions.insert((to, from)); // Bidirectional
    }

    /// Remove a network partition between two nodes.
    pub fn heal_partition(&self, from: u32, to: u32) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.remove(&(from, to));
        partitions.remove(&(to, from));
    }

    /// Heal all partitions.
    pub fn heal_all(&self) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.clear();

        // Also reconnect all nodes
        for flag in self.connections.values() {
            flag.store(true, Ordering::SeqCst);
        }
    }

    /// Update chaos configuration.
    pub fn set_config(&self, new_config: ChaosConfig) {
        let mut config = self.config.write().unwrap();
        *config = new_config;
    }

    /// Get current chaos configuration.
    pub fn get_config(&self) -> ChaosConfig {
        self.config.read().unwrap().clone()
    }

    /// Get cluster size.
    pub fn cluster_size(&self) -> u32 {
        self.cluster_size
    }

    /// Check if a node is killed.
    pub fn is_killed(&self, node_id: u32) -> bool {
        self.kill_flags
            .get(&node_id)
            .map(|f| f.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    /// Get the current primary for a view (for nemesis targeting).
    pub fn primary_for_view(&self, view: u64) -> u32 {
        (view % self.cluster_size as u64) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_network_basic() {
        let config = ChaosConfig::default();
        let network = ChaosNetwork::new(3, config);

        let ep0 = network.create_endpoint(0).unwrap();
        let ep1 = network.create_endpoint(1).unwrap();

        // Send a message
        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: 0,
        };
        assert!(ep0.send_to(1, msg));

        // Receive it
        thread::sleep(Duration::from_millis(10));
        let received = ep1.try_recv();
        assert!(received.is_some());
    }

    #[test]
    fn test_chaos_network_partition() {
        let config = ChaosConfig::default();
        let network = ChaosNetwork::new(3, config);

        let ep0 = network.create_endpoint(0).unwrap();
        let ep1 = network.create_endpoint(1).unwrap();

        // Partition nodes 0 and 1
        network.partition(0, 1);

        // Send should fail due to partition
        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: 0,
        };
        assert!(!ep0.send_to(1, msg.clone()));

        // Heal partition
        network.heal_partition(0, 1);

        // Now should work
        assert!(ep0.send_to(1, msg));
        thread::sleep(Duration::from_millis(10));
        assert!(ep1.try_recv().is_some());
    }

    #[test]
    fn test_chaos_network_kill_node() {
        let config = ChaosConfig::default();
        let network = ChaosNetwork::new(2, config);

        let ep0 = network.create_endpoint(0).unwrap();
        let ep1 = network.create_endpoint(1).unwrap();

        // Kill node 0
        network.kill_node(0);

        // Node 0 should not be able to send or receive
        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: 0,
        };
        assert!(!ep0.send_to(1, msg.clone()));
        assert!(ep0.try_recv().is_none());

        // Revive node 0
        network.revive_node(0);

        // Now should work
        assert!(ep0.send_to(1, msg));
    }
}
