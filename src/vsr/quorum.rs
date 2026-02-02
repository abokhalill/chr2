use std::collections::HashMap;

pub const MAX_CLUSTER_SIZE: u32 = 64;

/// O(1) insert/count via popcount. 8 bytes vs HashSet's 48+.
#[derive(Clone, Copy, Default)]
pub struct NodeBitset(u64);

impl NodeBitset {
    #[inline]
    pub fn new() -> Self { NodeBitset(0) }

    #[inline]
    pub fn insert(&mut self, node_id: u32) {
        debug_assert!(node_id < MAX_CLUSTER_SIZE);
        self.0 |= 1u64 << node_id;
    }

    #[inline]
    pub fn count(&self) -> u32 { self.0.count_ones() }

    #[inline]
    pub fn contains(&self, node_id: u32) -> bool {
        debug_assert!(node_id < MAX_CLUSTER_SIZE);
        (self.0 & (1u64 << node_id)) != 0
    }
}

#[allow(dead_code)]
pub struct QuorumTracker {
    cluster_size: u32,
    quorum_size: u32,
    node_id: u32,
    pending: HashMap<u64, NodeBitset>,
    quorum_index: Option<u64>,
}

impl QuorumTracker {
    pub fn new(cluster_size: u32, node_id: u32) -> Self {
        assert!(cluster_size <= MAX_CLUSTER_SIZE);
        assert!(node_id < cluster_size);
        let quorum_size = (cluster_size / 2) + 1;

        QuorumTracker {
            cluster_size,
            quorum_size,
            node_id,
            pending: HashMap::new(),
            quorum_index: None,
        }
    }

    #[inline]
    pub fn record_local_write(&mut self, index: u64) {
        let votes = self.pending.entry(index).or_insert_with(NodeBitset::new);
        votes.insert(self.node_id);
    }

    #[inline]
    pub fn record_prepare_ok(&mut self, index: u64, node_id: u32) -> bool {
        let votes = self.pending.entry(index).or_insert_with(NodeBitset::new);
        votes.insert(node_id);

        let has_quorum = votes.count() >= self.quorum_size;

        if has_quorum {
            match self.quorum_index {
                None => self.quorum_index = Some(index),
                Some(current) if index > current => self.quorum_index = Some(index),
                _ => {}
            }
        }

        has_quorum
    }

    #[inline]
    pub fn has_quorum(&self, index: u64) -> bool {
        self.pending
            .get(&index)
            .map(|votes| votes.count() >= self.quorum_size)
            .unwrap_or(false)
    }

    pub fn quorum_index(&self) -> Option<u64> { self.quorum_index }

    pub fn committable_index_from(&self, current_committed: Option<u64>) -> Option<u64> {
        let quorum_idx = self.quorum_index?;
        let start = match current_committed {
            None => 0,
            Some(c) => c + 1,
        };

        let mut highest_committable = current_committed;
        for idx in start..=quorum_idx {
            if self.has_quorum(idx) { highest_committable = Some(idx); }
            else { break; }
        }
        highest_committable
    }

    pub fn committable_index(&self) -> Option<u64> { self.committable_index_from(None) }

    pub fn gc(&mut self, committed_index: u64) {
        self.pending.retain(|&idx, _| idx > committed_index);
    }

    pub fn vote_count(&self, index: u64) -> usize {
        self.pending
            .get(&index)
            .map(|v| v.count() as usize)
            .unwrap_or(0)
    }

    pub fn quorum_size(&self) -> u32 { self.quorum_size }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_tracker_basic() {
        // 3-node cluster, node 0 is primary
        let mut tracker = QuorumTracker::new(3, 0);

        // Quorum size should be 2 (majority of 3)
        assert_eq!(tracker.quorum_size(), 2);

        // Primary writes index 0
        tracker.record_local_write(0);
        assert!(!tracker.has_quorum(0)); // Only 1 vote

        // Backup 1 responds
        let reached = tracker.record_prepare_ok(0, 1);
        assert!(reached);
        assert!(tracker.has_quorum(0)); // 2 votes = quorum
    }

    #[test]
    fn test_quorum_tracker_in_order_commit() {
        let mut tracker = QuorumTracker::new(3, 0);

        // Write indices 0, 1, 2
        tracker.record_local_write(0);
        tracker.record_local_write(1);
        tracker.record_local_write(2);

        // Get quorum for index 2 first (out of order)
        tracker.record_prepare_ok(2, 1);
        assert!(tracker.has_quorum(2));

        // But committable_index should be None (index 0 and 1 don't have quorum)
        assert_eq!(tracker.committable_index(), None);

        // Get quorum for index 0
        tracker.record_prepare_ok(0, 1);
        assert_eq!(tracker.committable_index(), Some(0));

        // Get quorum for index 1
        tracker.record_prepare_ok(1, 1);
        assert_eq!(tracker.committable_index(), Some(2)); // Now all are committable
    }
}
