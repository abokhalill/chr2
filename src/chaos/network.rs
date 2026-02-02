#![allow(clippy::type_complexity)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};
use rand::Rng;

use crate::vsr::message::VsrMessage;

#[derive(Debug, Clone)]
pub struct ChaosConfig {
    pub drop_rate: f64,
    pub latency_range: (Duration, Duration),
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

#[allow(dead_code)]
pub struct ChaosEndpoint {
    pub node_id: u32,
    rx: Receiver<(u32, VsrMessage)>,
    tx_to_chaos: Sender<(u32, u32, VsrMessage)>,
    connected: HashMap<u32, Arc<AtomicBool>>,
    partition_map: Arc<RwLock<HashSet<(u32, u32)>>>,
    killed: Arc<AtomicBool>,
    config: Arc<RwLock<ChaosConfig>>,
    pub messages_sent: Arc<AtomicU64>,
    pub messages_dropped: Arc<AtomicU64>,
}

impl ChaosEndpoint {
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

    pub fn try_recv(&self) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) { return None; }
        self.rx.try_recv().ok()
    }

    pub fn recv(&self) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) { return None; }
        self.rx.recv().ok()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<(u32, VsrMessage)> {
        if self.killed.load(Ordering::SeqCst) { return None; }
        self.rx.recv_timeout(timeout).ok()
    }

    pub fn is_killed(&self) -> bool { self.killed.load(Ordering::SeqCst) }
}

pub struct ChaosNetwork {
    cluster_size: u32,
    connections: HashMap<(u32, u32), Arc<AtomicBool>>,
    partition_map: Arc<RwLock<HashSet<(u32, u32)>>>,
    config: Arc<RwLock<ChaosConfig>>,
    node_senders: Arc<RwLock<HashMap<u32, Sender<(u32, VsrMessage)>>>>,
    node_receivers: Mutex<HashMap<u32, Receiver<(u32, VsrMessage)>>>,
    chaos_tx: Sender<(u32, u32, VsrMessage)>,
    kill_flags: HashMap<u32, Arc<AtomicBool>>,
    node_stats: HashMap<u32, (Arc<AtomicU64>, Arc<AtomicU64>)>,
    _processor_handle: Option<thread::JoinHandle<()>>,
}

impl ChaosNetwork {
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
            Self::chaos_processor_shared(
                chaos_rx,
                node_senders_clone,
                config_clone,
                partition_map_clone,
            );
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

    pub fn disconnect(&self, node_id: u32) {
        for (&(from, to), flag) in &self.connections {
            if from == node_id || to == node_id {
                flag.store(false, Ordering::SeqCst);
            }
        }
    }

    pub fn reconnect(&self, node_id: u32) {
        for (&(from, to), flag) in &self.connections {
            if from == node_id || to == node_id {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }

    pub fn kill_node(&self, node_id: u32) {
        if let Some(flag) = self.kill_flags.get(&node_id) {
            flag.store(true, Ordering::SeqCst);
        }
        self.disconnect(node_id);
    }

    pub fn revive_node(&self, node_id: u32) {
        if let Some(flag) = self.kill_flags.get(&node_id) {
            flag.store(false, Ordering::SeqCst);
        }
        self.reconnect(node_id);
    }

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

    pub fn partition(&self, from: u32, to: u32) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.insert((from, to));
        partitions.insert((to, from)); // Bidirectional
    }

    pub fn heal_partition(&self, from: u32, to: u32) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.remove(&(from, to));
        partitions.remove(&(to, from));
    }

    pub fn heal_all(&self) {
        let mut partitions = self.partition_map.write().unwrap();
        partitions.clear();

        // Also reconnect all nodes
        for flag in self.connections.values() {
            flag.store(true, Ordering::SeqCst);
        }
    }

    pub fn set_config(&self, new_config: ChaosConfig) {
        *self.config.write().unwrap() = new_config;
    }

    pub fn get_config(&self) -> ChaosConfig { self.config.read().unwrap().clone() }
    pub fn cluster_size(&self) -> u32 { self.cluster_size }

    pub fn is_killed(&self, node_id: u32) -> bool {
        self.kill_flags
            .get(&node_id)
            .map(|f| f.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

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

        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: Some(0),
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

        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: Some(0),
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

        let msg = VsrMessage::Commit {
            view: 0,
            commit_index: Some(0),
        };
        assert!(!ep0.send_to(1, msg.clone()));
        assert!(ep0.try_recv().is_none());

        // Revive node 0
        network.revive_node(0);

        // Now should work
        assert!(ep0.send_to(1, msg));
    }
}
