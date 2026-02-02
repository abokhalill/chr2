use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::message::{ClientRequest, ClientResponse, ClientResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSession {
    pub last_sequence_number: u64,
    pub last_response: Option<ClientResponse>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionMap {
    sessions: HashMap<u64, ClientSession>,
}

impl SessionMap {
    pub fn new() -> Self {
        SessionMap {
            sessions: HashMap::new(),
        }
    }

    pub fn check_duplicate(&self, client_id: u64, sequence_number: u64) -> Option<ClientResponse> {
        if let Some(session) = self.sessions.get(&client_id) {
            if sequence_number <= session.last_sequence_number {
                if sequence_number == session.last_sequence_number {
                    return session.last_response.clone();
                } else {
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

    pub fn last_sequence(&self, client_id: u64) -> u64 {
        self.sessions
            .get(&client_id)
            .map(|s| s.last_sequence_number)
            .unwrap_or(0)
    }

    pub fn clear(&mut self) {
        self.sessions.clear();
    }
    pub fn client_count(&self) -> usize {
        self.sessions.len()
    }
}

#[derive(Debug)]
pub struct PendingRequest {
    pub request: ClientRequest,
    pub log_index: u64,
    pub submitted_at: Instant,
}

pub struct ChrClient {
    pub client_id: u64,
    next_sequence: u64,
    last_known_leader: Option<u32>,
    cluster_nodes: Vec<u32>,
    max_retries: u32,
    base_timeout: Duration,
}

impl ChrClient {
    pub fn new(client_id: u64, cluster_nodes: Vec<u32>) -> Self {
        ChrClient {
            client_id,
            next_sequence: 1,
            last_known_leader: None,
            cluster_nodes,
            max_retries: 5,
            base_timeout: Duration::from_millis(100),
        }
    }

    pub fn create_request(&mut self, payload: Vec<u8>) -> ClientRequest {
        let seq = self.next_sequence;
        self.next_sequence += 1;

        ClientRequest {
            client_id: self.client_id,
            sequence_number: seq,
            payload,
        }
    }

    pub fn create_request_with_seq(&self, payload: Vec<u8>, sequence_number: u64) -> ClientRequest {
        ClientRequest {
            client_id: self.client_id,
            sequence_number,
            payload,
        }
    }

    pub fn current_sequence(&self) -> u64 {
        self.next_sequence
    }
    pub fn update_leader(&mut self, leader_id: u32) {
        self.last_known_leader = Some(leader_id);
    }
    pub fn last_known_leader(&self) -> Option<u32> {
        self.last_known_leader
    }
    pub fn target_node(&self) -> u32 {
        self.last_known_leader.unwrap_or(self.cluster_nodes[0])
    }

    pub fn handle_redirect(&mut self, leader_hint: Option<u32>) -> u32 {
        if let Some(leader) = leader_hint {
            self.last_known_leader = Some(leader);
            leader
        } else {
            let current = self.last_known_leader.unwrap_or(0);
            let next = ((current as usize + 1) % self.cluster_nodes.len()) as u32;
            self.last_known_leader = Some(next);
            next
        }
    }

    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        self.base_timeout * 2u32.pow(attempt.min(5))
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    pub fn is_overload_error(response: &ClientResponse) -> bool {
        matches!(&response.result, ClientResult::Error { message } if message.contains("System Overloaded"))
    }

    pub fn overload_backoff_duration(&self, attempt: u32) -> Duration {
        Duration::from_millis(100 * 2u64.pow(attempt.min(5)))
    }

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
        let mut client = ChrClient::new(42, vec![0, 1, 2]);

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
        let mut client = ChrClient::new(1, vec![0, 1, 2]);

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
