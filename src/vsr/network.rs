use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::message::VsrMessage;

pub struct NetworkEndpoint {
    pub node_id: u32,
    pub rx: Receiver<(u32, VsrMessage)>,
    pub tx_map: HashMap<u32, Sender<(u32, VsrMessage)>>,
    pub connected: HashMap<u32, Arc<AtomicBool>>,
}

impl NetworkEndpoint {
    pub fn send_to(&self, target_id: u32, msg: VsrMessage) -> bool {
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

    pub fn broadcast(&self, msg: VsrMessage) -> usize {
        let mut count = 0;
        for (&target_id, tx) in &self.tx_map {
            if let Some(connected) = self.connected.get(&target_id) {
                if !connected.load(Ordering::SeqCst) { continue; }
            }

            if tx.send((self.node_id, msg.clone())).is_ok() {
                count += 1;
            }
        }
        count
    }

    pub fn try_recv(&self) -> Option<(u32, VsrMessage)> { self.rx.try_recv().ok() }
    pub fn recv(&self) -> Option<(u32, VsrMessage)> { self.rx.recv().ok() }
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<(u32, VsrMessage)> {
        self.rx.recv_timeout(timeout).ok()
    }
}

#[allow(dead_code)]
pub struct MockNetwork {
    cluster_size: u32,
    connections: HashMap<(u32, u32), Arc<AtomicBool>>,
    node_senders: HashMap<u32, Sender<(u32, VsrMessage)>>,
    node_receivers: HashMap<u32, Receiver<(u32, VsrMessage)>>,
}

impl MockNetwork {
    pub fn new(cluster_size: u32) -> Self {
        let mut node_senders = HashMap::new();
        let mut node_receivers = HashMap::new();
        let mut connections = HashMap::new();

        for node_id in 0..cluster_size {
            let (tx, rx) = unbounded();
            node_senders.insert(node_id, tx);
            node_receivers.insert(node_id, rx);
        }

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

    pub fn create_endpoint(&mut self, node_id: u32) -> Option<NetworkEndpoint> {
        let rx = self.node_receivers.remove(&node_id)?;
        let mut tx_map = HashMap::new();
        for (&id, tx) in &self.node_senders {
            if id != node_id {
                tx_map.insert(id, tx.clone());
            }
        }

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
        let (from, received) = ep1
            .recv_timeout(std::time::Duration::from_millis(100))
            .unwrap();
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

        let (from, _) = ep2
            .recv_timeout(std::time::Duration::from_millis(100))
            .unwrap();
        assert_eq!(from, 0);
    }
}
