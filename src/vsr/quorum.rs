use std::collections::HashMap;

/// Maximum supported cluster size (64 nodes).
/// Using u64 bitset allows tracking up to 64 nodes efficiently.
pub const MAX_CLUSTER_SIZE: u32 = 64;

/// A compact bitset for tracking node acknowledgments.
/// 
/// Each bit position corresponds to a node ID.
/// Bit N is set if node N has acknowledged.
/// 
/// # Performance
/// - `insert`: O(1) - single bitwise OR
/// - `count`: O(1) - popcount instruction on modern CPUs
/// - Memory: 8 bytes vs ~48+ bytes for HashSet
#[derive(Clone, Copy, Default)]
pub struct NodeBitset(u64);

impl NodeBitset {
    /// Create an empty bitset.
    #[inline]
    pub fn new() -> Self {
        NodeBitset(0)
    }
    
    /// Set the bit for a node ID.
    #[inline]
    pub fn insert(&mut self, node_id: u32) {
        debug_assert!(node_id < MAX_CLUSTER_SIZE, "node_id exceeds MAX_CLUSTER_SIZE");
        self.0 |= 1u64 << node_id;
    }
    
    /// Count the number of set bits (number of acknowledging nodes).
    #[inline]
    pub fn count(&self) -> u32 {
        self.0.count_ones()
    }
    
    /// Check if a node has acknowledged.
    #[inline]
    pub fn contains(&self, node_id: u32) -> bool {
        debug_assert!(node_id < MAX_CLUSTER_SIZE, "node_id exceeds MAX_CLUSTER_SIZE");
        (self.0 & (1u64 << node_id)) != 0
    }
}

/// Tracks PrepareOk responses for pending log entries.
///
/// In a 3-node cluster, quorum is reached when Primary + 1 Backup have the entry.
/// Since Primary always has the entry (it wrote it), we need 1 PrepareOk from a Backup.
/// 
/// # Performance
/// Uses a fixed-size bitset (u64) instead of HashSet for tracking node acknowledgments.
/// This provides O(1) insert and count operations with minimal memory overhead.
pub struct QuorumTracker {
    /// Number of nodes in the cluster.
    cluster_size: u32,
    /// Quorum size (majority).
    quorum_size: u32,
    /// Node ID of this node (Primary).
    node_id: u32,
    /// Tracks which nodes have acknowledged each index.
    /// Key: log index, Value: bitset of node IDs that have acknowledged.
    pending: HashMap<u64, NodeBitset>,
    /// Highest index that has reached quorum.
    quorum_index: Option<u64>,
}

impl QuorumTracker {
    /// Create a new quorum tracker.
    ///
    /// # Arguments
    /// * `cluster_size` - Total number of nodes in the cluster (max 64)
    /// * `node_id` - This node's ID (Primary's ID)
    /// 
    /// # Panics
    /// Panics if cluster_size > MAX_CLUSTER_SIZE (64)
    pub fn new(cluster_size: u32, node_id: u32) -> Self {
        assert!(
            cluster_size <= MAX_CLUSTER_SIZE,
            "cluster_size {} exceeds MAX_CLUSTER_SIZE {}",
            cluster_size,
            MAX_CLUSTER_SIZE
        );
        assert!(
            node_id < cluster_size,
            "node_id {} must be less than cluster_size {}",
            node_id,
            cluster_size
        );
        
        // Quorum is majority: (n / 2) + 1
        let quorum_size = (cluster_size / 2) + 1;

        QuorumTracker {
            cluster_size,
            quorum_size,
            node_id,
            pending: HashMap::new(),
            quorum_index: None,
        }
    }

    /// Record that the Primary has written an entry locally.
    ///
    /// This is called when the Primary appends to its local log.
    /// The Primary's vote is implicit - it always has the entry.
    #[inline]
    pub fn record_local_write(&mut self, index: u64) {
        let votes = self.pending.entry(index).or_insert_with(NodeBitset::new);
        votes.insert(self.node_id);
    }

    /// Record a PrepareOk response from a Backup.
    ///
    /// Returns true if this response caused quorum to be reached for this index.
    #[inline]
    pub fn record_prepare_ok(&mut self, index: u64, node_id: u32) -> bool {
        let votes = self.pending.entry(index).or_insert_with(NodeBitset::new);
        votes.insert(node_id);

        let has_quorum = votes.count() >= self.quorum_size;

        if has_quorum {
            // Update quorum_index if this is higher
            match self.quorum_index {
                None => self.quorum_index = Some(index),
                Some(current) if index > current => self.quorum_index = Some(index),
                _ => {}
            }
        }

        has_quorum
    }

    /// Check if a specific index has reached quorum.
    #[inline]
    pub fn has_quorum(&self, index: u64) -> bool {
        self.pending
            .get(&index)
            .map(|votes| votes.count() >= self.quorum_size)
            .unwrap_or(false)
    }

    /// Get the highest index that has reached quorum.
    pub fn quorum_index(&self) -> Option<u64> {
        self.quorum_index
    }

    /// Get the highest index that can be committed.
    ///
    /// An index can only be committed if all previous indices have quorum.
    /// This ensures in-order commit.
    ///
    /// # Arguments
    /// * `current_committed` - The current committed index (None if nothing committed yet)
    pub fn committable_index_from(&self, current_committed: Option<u64>) -> Option<u64> {
        let quorum_idx = self.quorum_index?;

        // Start from the next index after current committed
        let start = match current_committed {
            None => 0,
            Some(c) => c + 1,
        };

        // Find the highest contiguous index with quorum
        let mut highest_committable = current_committed;

        for idx in start..=quorum_idx {
            if self.has_quorum(idx) {
                highest_committable = Some(idx);
            } else {
                // Gap found - can't commit beyond this
                break;
            }
        }

        highest_committable
    }

    /// Get the highest index that can be committed (starting from 0).
    pub fn committable_index(&self) -> Option<u64> {
        self.committable_index_from(None)
    }

    /// Clean up tracking for indices that have been committed.
    ///
    /// Call this after advancing committed_index to free memory.
    pub fn gc(&mut self, committed_index: u64) {
        self.pending.retain(|&idx, _| idx > committed_index);
    }

    /// Get the number of votes for a specific index.
    pub fn vote_count(&self, index: u64) -> usize {
        self.pending.get(&index).map(|v| v.count() as usize).unwrap_or(0)
    }

    /// Get the quorum size required.
    pub fn quorum_size(&self) -> u32 {
        self.quorum_size
    }
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
