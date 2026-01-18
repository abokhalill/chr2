//! Replicated SessionMap for exactly-once client request semantics.
//! Survives primary crashes via snapshot serialization. BTreeMap for determinism.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::message::{ClientResponse, ClientResult};

#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    pub window_size: u64,
    pub max_clients: usize,
    pub client_eviction_threshold: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig { window_size: 16, max_clients: 100_000, client_eviction_threshold: 1000 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientSessionState {
    pub highest_sequence: u64,
    pub lowest_retained: u64,
    pub cached_response: Option<CachedResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedResponse {
    pub sequence_number: u64,
    pub log_index: u64,
    pub result: CachedResult,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CachedResult {
    Success { log_index: u64 },
    Error { message: String },
}

impl From<&ClientResult> for CachedResult {
    fn from(result: &ClientResult) -> Self {
        match result {
            ClientResult::Success { log_index } => CachedResult::Success { log_index: *log_index },
            ClientResult::Error { message } => CachedResult::Error { message: message.clone() },
            ClientResult::NotThePrimary { .. } => CachedResult::Error { message: "Not the primary".to_string() },
            ClientResult::Pending => CachedResult::Error { message: "Request pending".to_string() },
        }
    }
}

impl From<CachedResult> for ClientResult {
    fn from(cached: CachedResult) -> Self {
        match cached {
            CachedResult::Success { log_index } => ClientResult::Success { log_index },
            CachedResult::Error { message } => ClientResult::Error { message },
        }
    }
}

#[derive(Debug)]
pub enum SessionCheckResult {
    New,
    Duplicate(ClientResponse),
    Stale { sequence: u64, lowest: u64 },
    OutOfOrder { sequence: u64, expected: u64 },
}

/// Replicated session map. BTreeMap for deterministic snapshot serialization.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedSessionMap {
    sessions: BTreeMap<u64, ClientSessionState>,
    #[serde(skip)]
    config: SessionConfig,
}

impl ReplicatedSessionMap {
    pub fn new() -> Self {
        ReplicatedSessionMap { sessions: BTreeMap::new(), config: SessionConfig::default() }
    }

    pub fn with_config(config: SessionConfig) -> Self {
        ReplicatedSessionMap { sessions: BTreeMap::new(), config }
    }

    pub fn set_config(&mut self, config: SessionConfig) {
        self.config = config;
    }

    #[inline]
    pub fn check(&self, client_id: u64, sequence_number: u64) -> SessionCheckResult {
        match self.sessions.get(&client_id) {
            None => {
                // New client - accept any sequence number
                SessionCheckResult::New
            }
            Some(session) => {
                if sequence_number > session.highest_sequence {
                    // New request (possibly with gap - we allow this for pipelining)
                    SessionCheckResult::New
                } else if sequence_number == session.highest_sequence {
                    // Duplicate of the last request
                    if let Some(ref cached) = session.cached_response {
                        if cached.sequence_number == sequence_number {
                            SessionCheckResult::Duplicate(ClientResponse {
                                sequence_number,
                                result: cached.result.clone().into(),
                            })
                        } else {
                            // Cache mismatch - treat as new (shouldn't happen)
                            SessionCheckResult::New
                        }
                    } else {
                        // No cached response - treat as new (shouldn't happen)
                        SessionCheckResult::New
                    }
                } else if sequence_number < session.lowest_retained {
                    // Ancient request - reject
                    SessionCheckResult::Stale {
                        sequence: sequence_number,
                        lowest: session.lowest_retained,
                    }
                } else {
                    // Request in the window but not the highest - this is a retry
                    // of an older request. We don't cache all responses, so we
                    // must reject with an error indicating the client should
                    // use a higher sequence number.
                    SessionCheckResult::OutOfOrder {
                        sequence: sequence_number,
                        expected: session.highest_sequence + 1,
                    }
                }
            }
        }
    }

    #[inline]
    pub fn record(
        &mut self,
        client_id: u64,
        sequence_number: u64,
        log_index: u64,
        result: &ClientResult,
    ) {
        let session = self.sessions.entry(client_id).or_insert_with(|| {
            ClientSessionState {
                highest_sequence: 0,
                lowest_retained: sequence_number.saturating_sub(self.config.window_size),
                cached_response: None,
            }
        });

        // Only update if this is a new highest sequence
        if sequence_number > session.highest_sequence {
            session.highest_sequence = sequence_number;
            
            // Update the window
            let new_lowest = sequence_number.saturating_sub(self.config.window_size);
            if new_lowest > session.lowest_retained {
                session.lowest_retained = new_lowest;
            }

            // Cache the response
            session.cached_response = Some(CachedResponse {
                sequence_number,
                log_index,
                result: result.into(),
            });
        }
    }

    /// Deterministic GC: evict oldest clients when over max_clients.
    pub fn gc(&mut self, _current_log_index: u64) -> usize {
        let max_clients = self.config.max_clients;
        
        if self.sessions.len() <= max_clients {
            return 0;
        }

        // Evict oldest clients (by lowest highest_sequence)
        let mut clients: Vec<_> = self.sessions
            .iter()
            .map(|(id, s)| (*id, s.highest_sequence))
            .collect();
        
        // Sort by highest_sequence (oldest first)
        clients.sort_by_key(|(_, seq)| *seq);

        // Evict until we're under the limit
        let to_evict = self.sessions.len() - max_clients;
        let mut evicted = 0;
        
        for (client_id, _) in clients.into_iter().take(to_evict) {
            self.sessions.remove(&client_id);
            evicted += 1;
        }

        evicted
    }

    #[inline]
    pub fn client_count(&self) -> usize { self.sessions.len() }
    #[inline]
    pub fn last_sequence(&self, client_id: u64) -> Option<u64> {
        self.sessions.get(&client_id).map(|s| s.highest_sequence)
    }
    #[inline]
    pub fn is_empty(&self) -> bool { self.sessions.is_empty() }
    #[cfg(test)]
    pub fn clear(&mut self) { self.sessions.clear(); }

    /// Merge (take max highest_sequence per client).
    pub fn merge(&mut self, other: &ReplicatedSessionMap) {
        for (client_id, other_session) in &other.sessions {
            match self.sessions.get_mut(client_id) {
                Some(session) => {
                    if other_session.highest_sequence > session.highest_sequence {
                        *session = other_session.clone();
                    }
                }
                None => {
                    self.sessions.insert(*client_id, other_session.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client_accepted() {
        let map = ReplicatedSessionMap::new();
        
        // New client should be accepted
        match map.check(1, 1) {
            SessionCheckResult::New => {}
            other => panic!("Expected New, got {:?}", other),
        }
    }

    #[test]
    fn test_duplicate_detection() {
        let mut map = ReplicatedSessionMap::new();
        
        // Record a request
        map.record(1, 1, 100, &ClientResult::Success { log_index: 100 });
        
        // Same sequence should be duplicate
        match map.check(1, 1) {
            SessionCheckResult::Duplicate(resp) => {
                assert_eq!(resp.sequence_number, 1);
                assert!(matches!(resp.result, ClientResult::Success { log_index: 100 }));
            }
            other => panic!("Expected Duplicate, got {:?}", other),
        }
        
        // Next sequence should be new
        match map.check(1, 2) {
            SessionCheckResult::New => {}
            other => panic!("Expected New, got {:?}", other),
        }
    }

    #[test]
    fn test_stale_request_rejected() {
        let mut map = ReplicatedSessionMap::with_config(SessionConfig {
            window_size: 5,
            ..Default::default()
        });
        
        // Record a high sequence number
        map.record(1, 100, 1000, &ClientResult::Success { log_index: 1000 });
        
        // Request below the window should be stale
        match map.check(1, 90) {
            SessionCheckResult::Stale { sequence: 90, lowest } => {
                assert!(lowest > 90);
            }
            other => panic!("Expected Stale, got {:?}", other),
        }
    }

    #[test]
    fn test_out_of_order_request() {
        let mut map = ReplicatedSessionMap::new();
        
        // Record sequence 10
        map.record(1, 10, 100, &ClientResult::Success { log_index: 100 });
        
        // Request for sequence 5 (in window but not highest) should be out of order
        match map.check(1, 5) {
            SessionCheckResult::OutOfOrder { sequence: 5, expected: 11 } => {}
            other => panic!("Expected OutOfOrder, got {:?}", other),
        }
    }

    #[test]
    fn test_gc_evicts_oldest_clients() {
        let mut map = ReplicatedSessionMap::with_config(SessionConfig {
            max_clients: 3,
            ..Default::default()
        });
        
        // Add 5 clients with different sequence numbers
        map.record(1, 10, 100, &ClientResult::Success { log_index: 100 });
        map.record(2, 20, 200, &ClientResult::Success { log_index: 200 });
        map.record(3, 30, 300, &ClientResult::Success { log_index: 300 });
        map.record(4, 40, 400, &ClientResult::Success { log_index: 400 });
        map.record(5, 50, 500, &ClientResult::Success { log_index: 500 });
        
        assert_eq!(map.client_count(), 5);
        
        // GC should evict 2 oldest clients (1 and 2)
        let evicted = map.gc(500);
        assert_eq!(evicted, 2);
        assert_eq!(map.client_count(), 3);
        
        // Clients 1 and 2 should be gone
        assert!(map.last_sequence(1).is_none());
        assert!(map.last_sequence(2).is_none());
        
        // Clients 3, 4, 5 should remain
        assert_eq!(map.last_sequence(3), Some(30));
        assert_eq!(map.last_sequence(4), Some(40));
        assert_eq!(map.last_sequence(5), Some(50));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut map = ReplicatedSessionMap::new();
        map.record(1, 10, 100, &ClientResult::Success { log_index: 100 });
        map.record(2, 20, 200, &ClientResult::Error { message: "test".to_string() });
        
        // Serialize
        let bytes = bincode::serialize(&map).unwrap();
        
        // Deserialize
        let mut restored: ReplicatedSessionMap = bincode::deserialize(&bytes).unwrap();
        restored.set_config(SessionConfig::default());
        
        // Verify state
        assert_eq!(restored.client_count(), 2);
        assert_eq!(restored.last_sequence(1), Some(10));
        assert_eq!(restored.last_sequence(2), Some(20));
        
        // Verify duplicate detection still works
        match restored.check(1, 10) {
            SessionCheckResult::Duplicate(resp) => {
                assert_eq!(resp.sequence_number, 10);
            }
            other => panic!("Expected Duplicate, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_takes_max() {
        let mut map1 = ReplicatedSessionMap::new();
        map1.record(1, 10, 100, &ClientResult::Success { log_index: 100 });
        map1.record(2, 20, 200, &ClientResult::Success { log_index: 200 });
        
        let mut map2 = ReplicatedSessionMap::new();
        map2.record(1, 15, 150, &ClientResult::Success { log_index: 150 }); // Higher
        map2.record(3, 30, 300, &ClientResult::Success { log_index: 300 }); // New client
        
        map1.merge(&map2);
        
        // Client 1 should have the higher sequence from map2
        assert_eq!(map1.last_sequence(1), Some(15));
        // Client 2 should be unchanged
        assert_eq!(map1.last_sequence(2), Some(20));
        // Client 3 should be added
        assert_eq!(map1.last_sequence(3), Some(30));
    }
}
