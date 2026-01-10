use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::message::VsrMessage;

/// A network endpoint for a single node.
pub struct NetworkEndpoint {
    /// This node's ID.
    pub node_id: u32,
    /// Receiver for incoming messages.
    pub rx: Receiver<(u32, VsrMessage)>,
    /// Senders to other nodes (keyed by node_id).
    pub tx_map: HashMap<u32, Sender<(u32, VsrMessage)>>,
    /// Connection status to each node (true = connected).
    pub connected: HashMap<u32, Arc<AtomicBool>>,
}

impl NetworkEndpoint {
    /// Send a message to a specific node.
    ///
    /// Returns true if the message was sent (connection is up).
    pub fn send_to(&self, target_id: u32, msg: VsrMessage) -> bool {
        // Check if connected
        if let Some(connected) = self.connected.get(&target_id) {
            if !connected.load(Ordering::SeqCst) {
                return false; // Disconnected
            }
        }

        if let Some(tx) = self.tx_map.get(&target_id) {
            tx.send((self.node_id, msg)).is_ok()
        } else {
            false
        }
    }

    /// Broadcast a message to all other nodes.
    ///
    /// Returns the number of nodes the message was sent to.
    pub fn broadcast(&self, msg: VsrMessage) -> usize {
        let mut count = 0;
        for (&target_id, tx) in &self.tx_map {
            // Check if connected
            if let Some(connected) = self.connected.get(&target_id) {
                if !connected.load(Ordering::SeqCst) {
                    continue; // Skip disconnected nodes
                }
            }

            if tx.send((self.node_id, msg.clone())).is_ok() {
                count += 1;
            }
        }
        count
    }

    /// Try to receive a message (non-blocking).
    pub fn try_recv(&self) -> Option<(u32, VsrMessage)> {
        self.rx.try_recv().ok()
    }

    /// Receive a message (blocking).
    pub fn recv(&self) -> Option<(u32, VsrMessage)> {
        self.rx.recv().ok()
    }

    /// Receive with timeout.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<(u32, VsrMessage)> {
        self.rx.recv_timeout(timeout).ok()
    }
}

/// Mock network that connects multiple nodes via channels.
pub struct MockNetwork {
    /// Number of nodes in the network.
    cluster_size: u32,
    /// Connection status between nodes.
    /// Key: (from_node, to_node), Value: connected flag.
    connections: HashMap<(u32, u32), Arc<AtomicBool>>,
    /// Senders for each node's inbox.
    node_senders: HashMap<u32, Sender<(u32, VsrMessage)>>,
    /// Receivers for each node's inbox (taken when endpoint is created).
    node_receivers: HashMap<u32, Receiver<(u32, VsrMessage)>>,
}

impl MockNetwork {
    /// Create a new mock network with the specified number of nodes.
    pub fn new(cluster_size: u32) -> Self {
        let mut node_senders = HashMap::new();
        let mut node_receivers = HashMap::new();
        let mut connections = HashMap::new();

        // Create a channel for each node
        for node_id in 0..cluster_size {
            let (tx, rx) = unbounded();
            node_senders.insert(node_id, tx);
            node_receivers.insert(node_id, rx);
        }

        // Create connection flags for all pairs
        for from in 0..cluster_size {
            for to in 0..cluster_size {
                if from != to {
                    connections.insert((from, to), Arc::new(AtomicBool::new(true)));
                }
            }
        }

        MockNetwork {
            cluster_size,
            connections,
            node_senders,
            node_receivers,
        }
    }

    /// Create a network endpoint for a specific node.
    ///
    /// This consumes the receiver for that node, so can only be called once per node.
    pub fn create_endpoint(&mut self, node_id: u32) -> Option<NetworkEndpoint> {
        let rx = self.node_receivers.remove(&node_id)?;

        // Build tx_map: senders to all other nodes
        let mut tx_map = HashMap::new();
        for (&id, tx) in &self.node_senders {
            if id != node_id {
                tx_map.insert(id, tx.clone());
            }
        }

        // Build connected map
        let mut connected = HashMap::new();
        for (&(from, to), flag) in &self.connections {
            if from == node_id {
                connected.insert(to, flag.clone());
            }
        }

        Some(NetworkEndpoint {
            node_id,
            rx,
            tx_map,
            connected,
        })
    }

    /// Disconnect a node from the network.
    ///
    /// Messages to/from this node will be dropped.
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

    /// Check if two nodes are connected.
    pub fn is_connected(&self, from: u32, to: u32) -> bool {
        self.connections
            .get(&(from, to))
            .map(|f| f.load(Ordering::SeqCst))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_network_basic() {
        let mut network = MockNetwork::new(3);

        let ep0 = network.create_endpoint(0).unwrap();
        let ep1 = network.create_endpoint(1).unwrap();
        let _ep2 = network.create_endpoint(2).unwrap();

        // Node 0 sends to Node 1
        let msg = VsrMessage::Prepare {
            view: 1,
            index: 0,
            payload: b"test".to_vec(),
            commit_index: None,
            timestamp_ns: 0,
        };

        assert!(ep0.send_to(1, msg.clone()));

        // Node 1 receives
        let (from, received) = ep1.recv_timeout(std::time::Duration::from_millis(100)).unwrap();
        assert_eq!(from, 0);
        assert!(matches!(received, VsrMessage::Prepare { index: 0, .. }));
    }

    #[test]
    fn test_mock_network_disconnect() {
        let mut network = MockNetwork::new(3);

        let ep0 = network.create_endpoint(0).unwrap();
        let ep2 = network.create_endpoint(2).unwrap();

        // Disconnect node 2
        network.disconnect(2);

        // Node 0 tries to send to Node 2 - should fail
        let msg = VsrMessage::Prepare {
            view: 1,
            index: 0,
            payload: b"test".to_vec(),
            commit_index: None,
            timestamp_ns: 0,
        };

        assert!(!ep0.send_to(2, msg.clone()));

        // Reconnect node 2
        network.reconnect(2);

        // Now should work
        assert!(ep0.send_to(2, msg));

        let (from, _) = ep2.recv_timeout(std::time::Duration::from_millis(100)).unwrap();
        assert_eq!(from, 0);
    }
}
