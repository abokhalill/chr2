use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::message::{ClientRequest, ClientResponse, ClientResult};

/// Session state for a single client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSession {
    /// Last processed sequence number.
    pub last_sequence_number: u64,
    /// Cached response for the last request.
    pub last_response: Option<ClientResponse>,
}

/// Session map for tracking client request idempotency.
///
/// This ensures exactly-once semantics by caching the last response
/// for each client and returning it for duplicate requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionMap {
    /// Map of client_id -> session state.
    sessions: HashMap<u64, ClientSession>,
}

impl SessionMap {
    /// Create a new empty session map.
    pub fn new() -> Self {
        SessionMap {
            sessions: HashMap::new(),
        }
    }

    /// Check if a request is a duplicate.
    ///
    /// Returns Some(cached_response) if this is a duplicate request,
    /// None if this is a new request that should be processed.
    pub fn check_duplicate(&self, client_id: u64, sequence_number: u64) -> Option<ClientResponse> {
        if let Some(session) = self.sessions.get(&client_id) {
            if sequence_number <= session.last_sequence_number {
                // This is a duplicate or old request
                if sequence_number == session.last_sequence_number {
                    // Return cached response for exact duplicate
                    return session.last_response.clone();
                } else {
                    // Old request - return error
                    return Some(ClientResponse {
                        sequence_number,
                        result: ClientResult::Error {
                            message: format!(
                                "Stale request: sequence {} < last processed {}",
                                sequence_number, session.last_sequence_number
                            ),
                        },
                    });
                }
            }
        }
        None
    }

    /// Record a processed request and its response.
    pub fn record_response(&mut self, client_id: u64, response: ClientResponse) {
        let session = self.sessions.entry(client_id).or_insert(ClientSession {
            last_sequence_number: 0,
            last_response: None,
        });

        if response.sequence_number > session.last_sequence_number {
            session.last_sequence_number = response.sequence_number;
            session.last_response = Some(response);
        }
    }

    /// Get the last sequence number for a client.
    pub fn last_sequence(&self, client_id: u64) -> u64 {
        self.sessions
            .get(&client_id)
            .map(|s| s.last_sequence_number)
            .unwrap_or(0)
    }

    /// Clear all sessions (for testing).
    pub fn clear(&mut self) {
        self.sessions.clear();
    }

    /// Get the number of tracked clients.
    pub fn client_count(&self) -> usize {
        self.sessions.len()
    }
}

/// Pending request tracker for async request handling.
#[derive(Debug)]
pub struct PendingRequest {
    /// The original request.
    pub request: ClientRequest,
    /// Log index where this request was appended.
    pub log_index: u64,
    /// When the request was submitted.
    pub submitted_at: Instant,
}

/// Client proxy for interacting with the VSR cluster.
///
/// Handles:
/// - Leader discovery and caching
/// - Automatic retry on NotThePrimary
/// - Exponential backoff for timeouts
pub struct chrClient {
    /// Unique client identifier.
    pub client_id: u64,
    /// Next sequence number to use.
    next_sequence: u64,
    /// Last known leader node ID.
    last_known_leader: Option<u32>,
    /// List of all node IDs in the cluster.
    cluster_nodes: Vec<u32>,
    /// Maximum number of retries.
    max_retries: u32,
    /// Base timeout for requests.
    base_timeout: Duration,
}

impl chrClient {
    /// Create a new client.
    pub fn new(client_id: u64, cluster_nodes: Vec<u32>) -> Self {
        chrClient {
            client_id,
            next_sequence: 1,
            last_known_leader: None,
            cluster_nodes,
            max_retries: 5,
            base_timeout: Duration::from_millis(100),
        }
    }

    /// Create a request with the next sequence number.
    pub fn create_request(&mut self, payload: Vec<u8>) -> ClientRequest {
        let seq = self.next_sequence;
        self.next_sequence += 1;

        ClientRequest {
            client_id: self.client_id,
            sequence_number: seq,
            payload,
        }
    }

    /// Create a request with a specific sequence number (for retries).
    pub fn create_request_with_seq(&self, payload: Vec<u8>, sequence_number: u64) -> ClientRequest {
        ClientRequest {
            client_id: self.client_id,
            sequence_number,
            payload,
        }
    }

    /// Get the current sequence number (without incrementing).
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Update the last known leader.
    pub fn update_leader(&mut self, leader_id: u32) {
        self.last_known_leader = Some(leader_id);
    }

    /// Get the last known leader.
    pub fn last_known_leader(&self) -> Option<u32> {
        self.last_known_leader
    }

    /// Get the target node for the next request.
    ///
    /// Returns the last known leader, or the first node if unknown.
    pub fn target_node(&self) -> u32 {
        self.last_known_leader.unwrap_or(self.cluster_nodes[0])
    }

    /// Handle a NotThePrimary response.
    ///
    /// Updates the leader cache and returns the new target.
    pub fn handle_redirect(&mut self, leader_hint: Option<u32>) -> u32 {
        if let Some(leader) = leader_hint {
            self.last_known_leader = Some(leader);
            leader
        } else {
            // No hint - try next node in round-robin
            let current = self.last_known_leader.unwrap_or(0);
            let next = ((current as usize + 1) % self.cluster_nodes.len()) as u32;
            self.last_known_leader = Some(next);
            next
        }
    }

    /// Calculate backoff duration for a retry attempt.
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let multiplier = 2u64.pow(attempt.min(5));
        self.base_timeout * multiplier as u32
    }

    /// Get the maximum number of retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
    
    /// Check if an error response indicates system overload.
    /// Returns true if the error message contains "System Overloaded".
    pub fn is_overload_error(response: &ClientResponse) -> bool {
        match &response.result {
            ClientResult::Error { message } => message.contains("System Overloaded"),
            _ => false,
        }
    }
    
    /// Calculate backoff duration for overload errors.
    /// Uses more aggressive backoff than regular retries to allow system recovery.
    pub fn overload_backoff_duration(&self, attempt: u32) -> Duration {
        // Start with 100ms base, double each time, cap at 3.2 seconds
        let base_ms = 100u64;
        let multiplier = 2u64.pow(attempt.min(5));
        Duration::from_millis(base_ms * multiplier)
    }
    
    /// Handle an overload error response.
    /// Returns the recommended wait duration before retrying.
    pub fn handle_overload(&self, attempt: u32) -> Duration {
        self.overload_backoff_duration(attempt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_map_duplicate_detection() {
        let mut session_map = SessionMap::new();

        // First request should not be a duplicate
        assert!(session_map.check_duplicate(1, 1).is_none());

        // Record the response
        session_map.record_response(
            1,
            ClientResponse {
                sequence_number: 1,
                result: ClientResult::Success { log_index: 0 },
            },
        );

        // Same request should now be a duplicate
        let dup = session_map.check_duplicate(1, 1);
        assert!(dup.is_some());
        assert!(matches!(
            dup.unwrap().result,
            ClientResult::Success { log_index: 0 }
        ));

        // Next sequence should not be a duplicate
        assert!(session_map.check_duplicate(1, 2).is_none());

        // Old sequence should return error
        let old = session_map.check_duplicate(1, 0);
        assert!(old.is_some());
        assert!(matches!(old.unwrap().result, ClientResult::Error { .. }));
    }

    #[test]
    fn test_chr_client_sequence_numbers() {
        let mut client = chrClient::new(42, vec![0, 1, 2]);

        let req1 = client.create_request(b"test1".to_vec());
        assert_eq!(req1.client_id, 42);
        assert_eq!(req1.sequence_number, 1);

        let req2 = client.create_request(b"test2".to_vec());
        assert_eq!(req2.sequence_number, 2);

        // Retry with same sequence
        let retry = client.create_request_with_seq(b"test2".to_vec(), 2);
        assert_eq!(retry.sequence_number, 2);
    }

    #[test]
    fn test_chr_client_leader_redirect() {
        let mut client = chrClient::new(1, vec![0, 1, 2]);

        // Initially no leader known
        assert_eq!(client.target_node(), 0);

        // Update leader
        client.update_leader(1);
        assert_eq!(client.target_node(), 1);

        // Handle redirect with hint
        let new_target = client.handle_redirect(Some(2));
        assert_eq!(new_target, 2);
        assert_eq!(client.target_node(), 2);

        // Handle redirect without hint (round-robin)
        let next = client.handle_redirect(None);
        assert_eq!(next, 0); // (2 + 1) % 3 = 0
    }
}
