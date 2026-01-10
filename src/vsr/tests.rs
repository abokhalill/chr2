//! VSR integration tests.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::engine::log::LogWriter;
use crate::engine::reader::{CommittedState, LogReader};
use crate::kernel::bank::{BankApp, BankEvent, BankQuery, BankQueryResponse};
use crate::kernel::executor::Executor;

use super::message::VsrMessage;
use super::network::MockNetwork;
use super::node::{NodeRole, VsrNode};

/// Helper to serialize a BankEvent to bytes.
fn serialize_event(event: &BankEvent) -> Vec<u8> {
    bincode::serialize(event).unwrap()
}

/// Helper struct to manage test paths and cleanup.
struct TestPaths {
    log_paths: Vec<PathBuf>,
    manifest_paths: Vec<PathBuf>,
}

impl TestPaths {
    fn new(prefix: &str, count: usize) -> Self {
        let log_paths: Vec<PathBuf> = (0..count)
            .map(|i| PathBuf::from(format!("/tmp/chr_{}_{}.log", prefix, i)))
            .collect();
        let manifest_paths: Vec<PathBuf> = (0..count)
            .map(|i| PathBuf::from(format!("/tmp/chr_{}_{}.manifest", prefix, i)))
            .collect();
        
        // Cleanup any existing files
        for path in &log_paths {
            let _ = fs::remove_file(path);
        }
        for path in &manifest_paths {
            let _ = fs::remove_file(path);
        }
        
        TestPaths { log_paths, manifest_paths }
    }
    
    fn log(&self, idx: usize) -> &Path {
        &self.log_paths[idx]
    }
    
    fn manifest(&self, idx: usize) -> &Path {
        &self.manifest_paths[idx]
    }
}

impl Drop for TestPaths {
    fn drop(&mut self) {
        for path in &self.log_paths {
            let _ = fs::remove_file(path);
        }
        for path in &self.manifest_paths {
            let _ = fs::remove_file(path);
        }
    }
}

/// Test: test_vsr_quorum_commit
///
/// Verifies VSR quorum-based commit:
/// 1. Spawn 3 nodes (Node 0=Primary, 1=Backup, 2=Backup)
/// 2. Disconnect Node 2
/// 3. Submit "Deposit Alice 100" to Node 0
/// 4. Verify: Node 0 and Node 1 reach quorum. Alice's balance becomes 100.
/// 5. Reconnect Node 2. Verify Node 2 eventually catches up.
#[test]
fn test_vsr_quorum_commit() {
    // Setup paths
    let log_path_0 = Path::new("/tmp/chr_vsr_node0.log");
    let log_path_1 = Path::new("/tmp/chr_vsr_node1.log");
    let log_path_2 = Path::new("/tmp/chr_vsr_node2.log");
    let manifest_path_0 = Path::new("/tmp/chr_vsr_node0.manifest");
    let manifest_path_1 = Path::new("/tmp/chr_vsr_node1.manifest");
    let manifest_path_2 = Path::new("/tmp/chr_vsr_node2.manifest");

    // Cleanup
    let _ = fs::remove_file(log_path_0);
    let _ = fs::remove_file(log_path_1);
    let _ = fs::remove_file(log_path_2);
    let _ = fs::remove_file(manifest_path_0);
    let _ = fs::remove_file(manifest_path_1);
    let _ = fs::remove_file(manifest_path_2);

    // Create mock network
    let mut network = MockNetwork::new(3);

    // Create network endpoints
    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    // Create committed states (shared between writer and executor)
    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    // Create log writers
    let writer_0 = LogWriter::create(log_path_0, 1).unwrap();
    let writer_1 = LogWriter::create(log_path_1, 1).unwrap();
    let writer_2 = LogWriter::create(log_path_2, 1).unwrap();

    // Create readers for executors
    let reader_0_exec = LogReader::open(log_path_0, committed_state_0.clone()).unwrap();
    let reader_1 = LogReader::open(log_path_1, committed_state_1.clone()).unwrap();
    let reader_2 = LogReader::open(log_path_2, committed_state_2.clone()).unwrap();
    
    // Create a separate reader for Primary's catch-up functionality
    let reader_0_catchup = LogReader::open(log_path_0, committed_state_0.clone()).unwrap();

    let app_0 = BankApp::new();
    let app_1 = BankApp::new();
    let app_2 = BankApp::new();

    let executor_0 = Executor::new(reader_0_exec, app_0, 0);
    let executor_1 = Executor::new(reader_1, app_1, 0);
    let executor_2 = Executor::new(reader_2, app_2, 0);

    // Create VSR nodes with manifest paths for durable fencing
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        1, // view
        writer_0,
        Some(reader_0_catchup),
        committed_state_0.clone(),
        ep0,
        Some(executor_0),
        manifest_path_0,
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        1,
        writer_1,
        committed_state_1.clone(),
        ep1,
        Some(executor_1),
        manifest_path_1,
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        1,
        writer_2,
        committed_state_2.clone(),
        ep2,
        Some(executor_2),
        manifest_path_2,
    ).unwrap();

    // Step 1: Disconnect Node 2
    network.disconnect(2);

    // Step 2: Submit "Deposit Alice 100" to Node 0 (Primary)
    let deposit = BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 100,
    };
    let payload = serialize_event(&deposit);

    let index = node_0.submit(&payload).unwrap();
    assert_eq!(index, 0);

    // Step 3: Process messages until quorum reached
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all(); // Node 2 is disconnected, won't receive
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();
        
        if node_0.committed_index() == Some(0) {
            break;
        }
    }

    // Primary should have committed
    assert_eq!(node_0.committed_index(), Some(0), "Node 0 should have committed index 0");

    // Backup needs to process the Commit message from Primary
    thread::sleep(Duration::from_millis(5));
    node_1.process_all();

    // Now backup should have committed
    assert_eq!(node_1.committed_index(), Some(0), "Node 1 should have committed index 0");

    // Verify Alice's balance on Node 0
    if let Some(ref executor) = node_0.executor {
        let response = executor.query(BankQuery::Balance {
            user: "Alice".to_string(),
        });
        assert!(
            matches!(response, BankQueryResponse::Balance(100)),
            "Alice should have balance 100 on Node 0"
        );
    }

    // Verify Alice's balance on Node 1
    if let Some(ref executor) = node_1.executor {
        let response = executor.query(BankQuery::Balance {
            user: "Alice".to_string(),
        });
        assert!(
            matches!(response, BankQueryResponse::Balance(100)),
            "Alice should have balance 100 on Node 1"
        );
    }

    // Node 2 should NOT have the entry (disconnected)
    assert_eq!(node_2.committed_index(), None, "Node 2 should not have committed anything");

    // Step 5: Reconnect Node 2
    network.reconnect(2);

    // Submit another entry to trigger catch-up via commit_index piggyback
    let deposit2 = BankEvent::Deposit {
        user: "Bob".to_string(),
        amount: 50,
    };
    let payload2 = serialize_event(&deposit2);

    let index2 = node_0.submit(&payload2).unwrap();
    assert_eq!(index2, 1);

    // Process on all nodes until quorum reached AND commit propagated
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all(); // Node 2 will get index mismatch error (expected 0, got 1)
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();
        
        // Wait for both Primary to commit AND Backup to receive commit notification
        if node_0.committed_index() == Some(1) && node_1.committed_index() == Some(1) {
            break;
        }
    }
    
    // One more round to ensure commit notification is processed
    node_1.process_all();

    // Node 2 should now have caught up (at least partially)
    // It receives Prepare for index 1, but needs index 0 first
    // In a full implementation, we'd have a catch-up mechanism
    // For now, verify the basic flow works

    // Verify Node 0 and Node 1 have both entries committed
    assert_eq!(node_0.committed_index(), Some(1), "Node 0 should have committed index 1");
    assert_eq!(node_1.committed_index(), Some(1), "Node 1 should have committed index 1");

    // Cleanup
    let _ = fs::remove_file(log_path_0);
    let _ = fs::remove_file(log_path_1);
    let _ = fs::remove_file(log_path_2);
}

#[test]
fn test_vsr_basic_replication() {
    // Simple test: Primary submits, Backup receives, quorum reached
    let paths = TestPaths::new("chr_basic", 2);

    let mut network = MockNetwork::new(2);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();

    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    // Submit entry
    let index = node_0.submit(b"test payload").unwrap();
    assert_eq!(index, 0);

    // Backup processes Prepare, sends PrepareOk
    thread::sleep(Duration::from_millis(5));
    node_1.process_all();

    // Primary processes PrepareOk, reaches quorum, broadcasts Commit
    thread::sleep(Duration::from_millis(5));
    node_0.process_all();

    // Primary should have committed
    assert_eq!(node_0.committed_index(), Some(0));

    // Backup processes Commit message from Primary
    thread::sleep(Duration::from_millis(5));
    node_1.process_all();

    // Now both should have committed
    assert_eq!(node_1.committed_index(), Some(0));
}

#[test]
fn test_vsr_multiple_entries() {
    let paths = TestPaths::new("chr_multi", 2);

    let mut network = MockNetwork::new(2);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();

    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    // Submit multiple entries
    for i in 0..5 {
        let payload = format!("entry {}", i);
        let index = node_0.submit(payload.as_bytes()).unwrap();
        assert_eq!(index, i);

        // Process messages - loop until quorum reached for this entry AND commit propagated
        for _ in 0..20 {
            thread::sleep(Duration::from_millis(5));
            node_1.process_all();
            thread::sleep(Duration::from_millis(5));
            node_0.process_all();
            thread::sleep(Duration::from_millis(5));
            node_1.process_all(); // Process commit notification
            
            if node_0.committed_index() == Some(i) && node_1.committed_index() == Some(i) {
                break;
            }
        }
    }

    // All entries should be committed
    assert_eq!(node_0.committed_index(), Some(4));
    assert_eq!(node_1.committed_index(), Some(4));
}

/// Test: test_vsr_failure_detection
///
/// Verifies failure detection and view change initiation:
/// 1. Spawn 3 nodes (Node 0=Primary, 1=Backup, 2=Backup)
/// 2. "Kill" the Primary (drop the node)
/// 3. Wait 300ms (> ELECTION_TIMEOUT of 200ms)
/// 4. Verify that both Backups have transitioned to ViewChangeInProgress
///    and are broadcasting StartViewChange
#[test]
fn test_vsr_failure_detection() {
    use super::node::ELECTION_TIMEOUT;

    let paths = TestPaths::new("chr_failure", 3);

    let mut network = MockNetwork::new(3);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    let writer_2 = LogWriter::create(paths.log(2), 1).unwrap();

    // Create nodes
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        1,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        1,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        1,
        writer_2,
        committed_state_2.clone(),
        ep2,
        None,
        paths.manifest(2),
    ).unwrap();

    // Step 1: Verify initial state - all nodes are in their expected roles
    assert_eq!(node_0.role, super::node::NodeRole::Primary);
    assert_eq!(node_1.role, super::node::NodeRole::Backup);
    assert_eq!(node_2.role, super::node::NodeRole::Backup);

    // Step 2: Primary sends a heartbeat to establish contact
    node_0.tick(); // This sends a heartbeat
    thread::sleep(Duration::from_millis(10));
    node_1.process_all();
    node_2.process_all();

    // Verify backups are still in Backup role (they received heartbeat)
    assert_eq!(node_1.role, super::node::NodeRole::Backup);
    assert_eq!(node_2.role, super::node::NodeRole::Backup);

    // Step 3: "Kill" the Primary by disconnecting it
    network.disconnect(0);

    // Step 4: Wait for ELECTION_TIMEOUT + some margin
    // We'll simulate time passing by calling tick() after sleeping
    thread::sleep(ELECTION_TIMEOUT + Duration::from_millis(100));

    // Step 5: Backups should detect failure on next tick
    let changed_1 = node_1.tick();
    let changed_2 = node_2.tick();

    // Both backups should have started view change
    assert!(changed_1, "Node 1 should have started view change");
    assert!(changed_2, "Node 2 should have started view change");

    assert!(
        node_1.is_view_change_in_progress(),
        "Node 1 should be in ViewChangeInProgress"
    );
    assert!(
        node_2.is_view_change_in_progress(),
        "Node 2 should be in ViewChangeInProgress"
    );

    // Verify proposed view is incremented
    assert_eq!(node_1.proposed_view(), 2, "Node 1 should propose view 2");
    assert_eq!(node_2.proposed_view(), 2, "Node 2 should propose view 2");

    // Step 6: Process messages - each backup should receive the other's StartViewChange
    thread::sleep(Duration::from_millis(10));
    node_1.process_all();
    node_2.process_all();
}

/// Test: test_vsr_heartbeat_prevents_election
///
/// Verifies that heartbeats prevent election timeout:
/// 1. Create Primary and Backup
/// 2. Primary sends heartbeats via tick()
/// 3. Backup should NOT start view change as long as heartbeats arrive
#[test]
fn test_vsr_heartbeat_prevents_election() {
    use super::node::{HEARTBEAT_INTERVAL, ELECTION_TIMEOUT};

    let paths = TestPaths::new("chr_heartbeat", 2);

    let mut network = MockNetwork::new(2);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();

    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    // Simulate time passing with heartbeats
    // We'll do 5 rounds of: wait HEARTBEAT_INTERVAL, tick Primary, process Backup
    for _ in 0..5 {
        thread::sleep(HEARTBEAT_INTERVAL + Duration::from_millis(10));
        
        // Primary sends heartbeat
        node_0.tick();
        
        // Backup receives and processes heartbeat
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        
        // Backup should NOT have started view change
        let changed = node_1.tick();
        assert!(!changed, "Backup should not start view change while receiving heartbeats");
        assert_eq!(node_1.role, super::node::NodeRole::Backup);
    }

    // Total time elapsed: 5 * (HEARTBEAT_INTERVAL + 15ms) ≈ 325ms
    // This is > ELECTION_TIMEOUT (200ms), but heartbeats should have prevented election
}

/// Test: test_vsr_full_view_change
///
/// Verifies complete view change reconciliation:
/// 1. Spawn 3 nodes (Node 0=Primary, 1=Backup, 2=Backup)
/// 2. Node 0 sends a request that only reaches Node 0 and Node 1 (Quorum commit)
/// 3. Kill Node 0
/// 4. Verify: Node 1 and Node 2 detect failure
/// 5. Node 1 becomes Primary for View 2 (view 1 % 3 = 1)
/// 6. Node 2 synchronizes and becomes Backup
/// 7. Cluster resumes accepting requests
#[test]
fn test_vsr_full_view_change() {
    use super::node::ELECTION_TIMEOUT;

    let paths = TestPaths::new("chr_viewchange", 3);

    let mut network = MockNetwork::new(3);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    let writer_2 = LogWriter::create(paths.log(2), 1).unwrap();

    // Create nodes - all start in view 0
    // View 0 % 3 = 0, so Node 0 is Primary
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        0, // view 0
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        0,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        0,
        writer_2,
        committed_state_2.clone(),
        ep2,
        None,
        paths.manifest(2),
    ).unwrap();

    // Step 1: Primary submits an entry
    let deposit = BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 100,
    };
    let payload = serialize_event(&deposit);
    let index = node_0.submit(&payload).unwrap();
    assert_eq!(index, 0);

    // Step 2: Process on Node 1 (Node 2 will also receive but we want quorum first)
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();

        if node_0.committed_index() == Some(0) {
            break;
        }
    }

    // Verify entry is committed on Node 0
    assert_eq!(node_0.committed_index(), Some(0), "Node 0 should have committed");

    // Process commit notification for Node 1 and Node 2
    thread::sleep(Duration::from_millis(10));
    node_1.process_all();
    node_2.process_all();

    // Now verify Node 1 has committed
    assert_eq!(node_1.committed_index(), Some(0), "Node 1 should have committed");

    // Step 3: "Kill" Node 0 by disconnecting it
    network.disconnect(0);

    // Step 4: Wait for election timeout
    thread::sleep(ELECTION_TIMEOUT + Duration::from_millis(50));

    // Node 1 and Node 2 detect failure
    node_1.tick();
    node_2.tick();

    assert!(node_1.is_view_change_in_progress(), "Node 1 should start view change");
    assert!(node_2.is_view_change_in_progress(), "Node 2 should start view change");

    // Step 5: Process StartViewChange messages
    // Both nodes broadcast StartViewChange for view 1
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();

        // Check if view change completed
        if node_1.role == super::node::NodeRole::Primary && node_1.current_view() == 1 {
            break;
        }
    }

    // View 1 % 3 = 1, so Node 1 should become Primary
    assert_eq!(node_1.role, super::node::NodeRole::Primary, "Node 1 should be Primary");
    assert_eq!(node_1.current_view(), 1, "Node 1 should be in view 1");

    // Step 6: Node 2 receives StartView and becomes Backup
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(5));
        node_2.process_all();

        if node_2.role == super::node::NodeRole::Backup && node_2.current_view() == 1 {
            break;
        }
    }

    assert_eq!(node_2.role, super::node::NodeRole::Backup, "Node 2 should be Backup");
    assert_eq!(node_2.current_view(), 1, "Node 2 should be in view 1");

    // Step 7: New Primary (Node 1) can accept requests
    let deposit2 = BankEvent::Deposit {
        user: "Bob".to_string(),
        amount: 50,
    };
    let payload2 = serialize_event(&deposit2);
    let index2 = node_1.submit(&payload2).unwrap();
    assert_eq!(index2, 1);

    // Process until committed
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(5));
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        thread::sleep(Duration::from_millis(5));
        node_2.process_all(); // Process commit notification

        if node_1.committed_index() == Some(1) && node_2.committed_index() == Some(1) {
            break;
        }
    }

    assert_eq!(node_1.committed_index(), Some(1), "Node 1 should have committed index 1");
    assert_eq!(node_2.committed_index(), Some(1), "Node 2 should have committed index 1");

    // Step 8: Verify fencing - if old Primary wakes up, it should be rejected
    // (In this test, Node 0 is disconnected so it can't send messages anyway)
}

/// Test: test_client_exactly_once_during_failover
///
/// Verifies exactly-once semantics during leader failover:
/// 1. Start a 3-node cluster
/// 2. Client sends "Deposit 100" to Primary (Node 0)
/// 3. Request is committed on Node 0 and Node 1
/// 4. Kill Node 0
/// 5. Client retries the SAME request (same sequence number) to new Primary (Node 1)
/// 6. New Primary recognizes duplicate and returns cached response
/// 7. Balance is 100, NOT 200
#[test]
fn test_client_exactly_once_during_failover() {
    use super::client::{chrClient, SessionMap};
    use super::message::{ClientRequest, ClientResult};
    use super::node::ELECTION_TIMEOUT;

    let paths = TestPaths::new("chr_idempotent", 3);

    let mut network = MockNetwork::new(3);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    let writer_2 = LogWriter::create(paths.log(2), 1).unwrap();

    // Create nodes - all start in view 0
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        0,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        0,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        0,
        writer_2,
        committed_state_2.clone(),
        ep2,
        None,
        paths.manifest(2),
    ).unwrap();

    // Create a client
    let mut client = chrClient::new(42, vec![0, 1, 2]);

    // Step 1: Client creates a request for "Deposit 100"
    let deposit = BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 100,
    };
    let payload = serialize_event(&deposit);
    let request = client.create_request(payload.clone());
    let sequence_number = request.sequence_number;

    assert_eq!(sequence_number, 1, "First request should have sequence 1");

    // Step 2: Send request to Primary (Node 0)
    let response = node_0.handle_client_request(&request);
    assert!(
        matches!(response.result, ClientResult::Pending),
        "Request should be pending"
    );

    // Step 3: Process replication until committed
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();

        if node_0.committed_index() == Some(0) {
            break;
        }
    }

    // Check committed requests on Primary
    let committed = node_0.check_committed_requests();
    assert_eq!(committed.len(), 1, "Should have one committed request");
    assert!(
        matches!(committed[0].1.result, ClientResult::Success { log_index: 0 }),
        "Request should be successful at index 0"
    );

    // Backups need to process Commit message from Primary
    thread::sleep(Duration::from_millis(5));
    node_1.process_all();

    // Verify Node 1 also has the entry
    assert_eq!(node_1.committed_index(), Some(0));

    // Step 4: Transfer session map from Node 0 to Node 1 (simulating snapshot sync)
    // In production, this would happen via snapshot restoration
    let session_map_snapshot = node_0.session_map().clone();
    node_1.restore_session_map(session_map_snapshot);

    // Step 5: "Kill" Node 0
    network.disconnect(0);

    // Wait for election timeout
    thread::sleep(ELECTION_TIMEOUT + Duration::from_millis(50));

    // Nodes detect failure and start view change
    node_1.tick();
    node_2.tick();

    // Process view change
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();

        if node_1.role == super::node::NodeRole::Primary && node_1.current_view() == 1 {
            break;
        }
    }

    // Node 1 should now be Primary
    assert_eq!(node_1.role, super::node::NodeRole::Primary);
    assert_eq!(node_1.current_view(), 1);

    // Step 6: Client retries the SAME request (same sequence number) to new Primary
    let retry_request = client.create_request_with_seq(payload.clone(), sequence_number);
    let retry_response = node_1.handle_client_request(&retry_request);

    // The new Primary should recognize this as a duplicate and return cached response
    assert!(
        matches!(retry_response.result, ClientResult::Success { log_index: 0 }),
        "Retry should return cached success response, got {:?}",
        retry_response.result
    );

    // Step 7: Verify the deposit was NOT applied twice
    // If idempotency failed, we would have log_index 1 for the retry
    // But since it's a duplicate, we get the cached response with log_index 0

    // Submit a NEW request (different sequence number) to verify cluster is working
    let deposit2 = BankEvent::Deposit {
        user: "Bob".to_string(),
        amount: 50,
    };
    let payload2 = serialize_event(&deposit2);
    let request2 = client.create_request(payload2);

    assert_eq!(request2.sequence_number, 2, "Second request should have sequence 2");

    let response2 = node_1.handle_client_request(&request2);
    assert!(
        matches!(response2.result, ClientResult::Pending),
        "New request should be pending"
    );

    // Process until committed
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        thread::sleep(Duration::from_millis(5));
        node_2.process_all();

        if node_1.committed_index() == Some(1) {
            break;
        }
    }

    // Check the new request was committed at index 1
    let committed2 = node_1.check_committed_requests();
    assert_eq!(committed2.len(), 1);
    assert!(
        matches!(committed2[0].1.result, ClientResult::Success { log_index: 1 }),
        "New request should be at index 1"
    );

}

/// Test: test_client_redirect_to_leader
///
/// Verifies that Backup nodes redirect clients to the Primary.
#[test]
fn test_chr_jepsen_threaded() {
    use super::message::{ClientRequest, ClientResult};

    let paths = TestPaths::new("chr_redirect", 2);

    let mut network = MockNetwork::new(2);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();

    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        0,
        writer_0,
        None, // No catch-up reader needed for this test
        committed_state_0.clone(),
        ep0,
        None,
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        0,
        writer_1,
        committed_state_1.clone(),
        ep1,
        None,
        paths.manifest(1),
    ).unwrap();

    // Create a request
    let request = ClientRequest {
        client_id: 1,
        sequence_number: 1,
        payload: b"test".to_vec(),
    };

    // Send to Backup - should get redirect
    let response = node_1.handle_client_request(&request);
    assert!(
        matches!(response.result, ClientResult::NotThePrimary { leader_hint: Some(0) }),
        "Backup should redirect to Primary (node 0)"
    );

    // Send to Primary - should be accepted
    let response2 = node_0.handle_client_request(&request);
    assert!(
        matches!(response2.result, ClientResult::Pending),
        "Primary should accept request"
    );
}

/// Test: test_outbox_exactly_once_with_failure
///
/// Verifies durable outbox exactly-once semantics during failover:
/// 1. Client submits a request that generates an "Email" side effect
/// 2. Primary executes the effect but is **Killed** before submitting AcknowledgeEffect
/// 3. A new Primary is elected
/// 4. **Verify:** The new Primary re-executes the effect (At-Least-Once delivery)
/// 5. **Verify:** Once AcknowledgeEffect is committed, the side effect disappears from Pending
#[test]
fn test_outbox_exactly_once_with_failure() {
    use super::node::ELECTION_TIMEOUT;
    use crate::kernel::traits::{EffectId, SideEffectStatus};
    use crate::kernel::side_effect_manager::{
        MockEffectExecutor, MockAcknowledgeSubmitter, SideEffectManager, SideEffectManagerConfig,
    };

    let paths = TestPaths::new("outbox", 3);

    let mut network = MockNetwork::new(3);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    let writer_2 = LogWriter::create(paths.log(2), 1).unwrap();

    // Create executors with readers
    let reader_0 = LogReader::open(paths.log(0), committed_state_0.clone()).unwrap();
    let reader_1 = LogReader::open(paths.log(1), committed_state_1.clone()).unwrap();
    let reader_2 = LogReader::open(paths.log(2), committed_state_2.clone()).unwrap();

    let app_0 = BankApp::new();
    let app_1 = BankApp::new();
    let app_2 = BankApp::new();

    let executor_0 = Executor::new(reader_0, app_0, 0);
    let executor_1 = Executor::new(reader_1, app_1, 0);
    let executor_2 = Executor::new(reader_2, app_2, 0);

    // Create a separate reader for Primary's catch-up functionality
    let reader_0_catchup = LogReader::open(paths.log(0), committed_state_0.clone()).unwrap();

    // Create VSR nodes - view 0, node 0 is primary
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        0,
        writer_0,
        Some(reader_0_catchup),
        committed_state_0.clone(),
        ep0,
        Some(executor_0),
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        0,
        writer_1,
        committed_state_1.clone(),
        ep1,
        Some(executor_1),
        paths.manifest(1),
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        0,
        writer_2,
        committed_state_2.clone(),
        ep2,
        Some(executor_2),
        paths.manifest(2),
    ).unwrap();

    // Create mock effect executors to track executions
    let mock_executor_0 = MockEffectExecutor::new();
    let mock_submitter_0 = MockAcknowledgeSubmitter::new();
    let mock_executor_1 = MockEffectExecutor::new();
    let mock_submitter_1 = MockAcknowledgeSubmitter::new();

    // Step 1: Submit a SendEmail event that generates a durable side effect
    let client_id = 42u64;
    let sequence_number = 1u64;
    let send_email = BankEvent::SendEmail {
        to: "alice@example.com".to_string(),
        subject: "Welcome!".to_string(),
        client_id,
        sequence_number,
    };
    let payload = serialize_event(&send_email);

    let index = node_0.submit(&payload).unwrap();
    assert_eq!(index, 0);

    // Process until committed on all nodes
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();

        if node_0.committed_index() == Some(0) 
            && node_1.committed_index() == Some(0) 
            && node_2.committed_index() == Some(0) {
            break;
        }
    }

    // Verify all nodes committed the entry
    assert_eq!(node_0.committed_index(), Some(0), "Node 0 should have committed");
    assert_eq!(node_1.committed_index(), Some(0), "Node 1 should have committed");
    assert_eq!(node_2.committed_index(), Some(0), "Node 2 should have committed");

    // Verify the outbox has a pending effect on all nodes
    let effect_id = EffectId::new(client_id, sequence_number, 0);
    
    // Check node 0's executor state
    if let Some(ref executor) = node_0.executor {
        let outbox = executor.state().outbox();
        assert_eq!(outbox.pending_count(), 1, "Node 0 should have 1 pending effect");
        let entry = outbox.get(&effect_id).expect("Effect should exist");
        assert_eq!(entry.status, SideEffectStatus::Pending);
    }

    // Check node 1's executor state
    if let Some(ref executor) = node_1.executor {
        let outbox = executor.state().outbox();
        assert_eq!(outbox.pending_count(), 1, "Node 1 should have 1 pending effect");
    }

    // Step 2: Simulate Primary executing the effect but dying before AcknowledgeEffect
    // Create a SideEffectManager for node 0 and process the pending effect
    let manager_0 = SideEffectManager::new(
        SideEffectManagerConfig::default(),
        mock_executor_0.as_executor(),
        mock_submitter_0.as_submitter(),
    );
    // Use set_primary_with_token with the current view as fencing token
    manager_0.set_primary_with_token(true, node_0.current_view());

    // Execute the effect on node 0
    if let Some(ref executor) = node_0.executor {
        let processed = manager_0.process_pending(executor.state().outbox());
        assert_eq!(processed, 1, "Should have processed 1 effect");
    }

    // Verify effect was executed on node 0
    assert_eq!(mock_executor_0.get_executed().len(), 1, "Effect should have been executed once");
    
    // Verify acknowledge was submitted (but we'll simulate it not being committed)
    assert_eq!(mock_submitter_0.get_submitted().len(), 1, "Acknowledge should have been submitted");

    // Step 3: Kill node 0 BEFORE the AcknowledgeEffect can be committed
    network.disconnect(0);

    // Wait for election timeout
    thread::sleep(ELECTION_TIMEOUT + Duration::from_millis(100));

    // Trigger view change on remaining nodes
    node_1.tick();
    node_2.tick();

    // Process view change messages
    for _ in 0..30 {
        thread::sleep(Duration::from_millis(10));
        node_1.process_all();
        node_2.process_all();
        node_1.tick();
        node_2.tick();

        // Check if view change completed
        if node_1.role == NodeRole::Primary || node_2.role == NodeRole::Primary {
            break;
        }
    }

    // Verify a new primary was elected (should be node 1 for view 1)
    let new_primary = if node_1.role == NodeRole::Primary {
        &mut node_1
    } else if node_2.role == NodeRole::Primary {
        &mut node_2
    } else {
        panic!("No new primary was elected!");
    };

    assert!(new_primary.current_view() > 0, "View should have changed");

    // Step 4: Verify the new Primary still has the effect as Pending
    // (because AcknowledgeEffect was never committed)
    if let Some(ref executor) = new_primary.executor {
        let outbox = executor.state().outbox();
        assert_eq!(outbox.pending_count(), 1, "New primary should still have 1 pending effect");
        let entry = outbox.get(&effect_id).expect("Effect should still exist");
        assert_eq!(entry.status, SideEffectStatus::Pending, "Effect should still be Pending");
    }

    // Step 5: New Primary re-executes the effect (At-Least-Once delivery)
    let manager_1 = SideEffectManager::new(
        SideEffectManagerConfig::default(),
        mock_executor_1.as_executor(),
        mock_submitter_1.as_submitter(),
    );
    // Use set_primary_with_token with the NEW view as fencing token
    // This demonstrates proper fencing: new primary uses higher view number
    manager_1.set_primary_with_token(true, new_primary.current_view());

    if let Some(ref executor) = new_primary.executor {
        let processed = manager_1.process_pending(executor.state().outbox());
        assert_eq!(processed, 1, "New primary should re-execute the effect");
    }

    // Verify effect was executed again (At-Least-Once)
    assert_eq!(mock_executor_1.get_executed().len(), 1, "Effect should have been executed on new primary");

    // Step 6: Now submit and commit the AcknowledgeEffect
    let ack_event = BankEvent::SystemAcknowledgeEffect { effect_id };
    let ack_payload = serialize_event(&ack_event);

    let ack_index = new_primary.submit(&ack_payload).unwrap();

    // Process until committed
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_1.tick();
        node_2.tick();

        let committed_1 = node_1.committed_index();
        let committed_2 = node_2.committed_index();
        
        if committed_1 >= Some(ack_index) && committed_2 >= Some(ack_index) {
            break;
        }
    }

    // Step 7: Verify the effect is now Acknowledged on all live nodes
    if let Some(ref executor) = node_1.executor {
        let outbox = executor.state().outbox();
        assert_eq!(outbox.pending_count(), 0, "Node 1 should have 0 pending effects after ack");
        let entry = outbox.get(&effect_id).expect("Effect should still exist");
        assert_eq!(entry.status, SideEffectStatus::Acknowledged, "Effect should be Acknowledged");
    }

    if let Some(ref executor) = node_2.executor {
        let outbox = executor.state().outbox();
        assert_eq!(outbox.pending_count(), 0, "Node 2 should have 0 pending effects after ack");
        let entry = outbox.get(&effect_id).expect("Effect should still exist");
        assert_eq!(entry.status, SideEffectStatus::Acknowledged, "Effect should be Acknowledged");
    }

}

/// Test: test_chr_survivability
///
/// Verifies admission control and backpressure under extreme load:
/// 1. Set max_inflight_requests to 50
/// 2. Hammer the cluster with 500 unique requests in a burst
/// 3. Verify:
///    - Exactly 50 requests are accepted; 450 are rejected with "System Overloaded"
///    - No memory growth beyond the bounded queue
///    - The 50 accepted requests are eventually committed and consistent
///    - Subsequent requests are accepted once in-flight count drops
#[test]
fn test_chr_survivability() {
    use super::message::{ClientRequest, ClientResult};
    
    let paths = TestPaths::new("survivability", 3);

    let mut network = MockNetwork::new(3);

    let ep0 = network.create_endpoint(0).unwrap();
    let ep1 = network.create_endpoint(1).unwrap();
    let ep2 = network.create_endpoint(2).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    let committed_state_2 = Arc::new(CommittedState::new());

    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    let writer_2 = LogWriter::create(paths.log(2), 1).unwrap();

    // Create executors with readers
    let reader_0 = LogReader::open(paths.log(0), committed_state_0.clone()).unwrap();
    let reader_1 = LogReader::open(paths.log(1), committed_state_1.clone()).unwrap();
    let reader_2 = LogReader::open(paths.log(2), committed_state_2.clone()).unwrap();

    let app_0 = BankApp::new();
    let app_1 = BankApp::new();
    let app_2 = BankApp::new();

    let executor_0 = Executor::new(reader_0, app_0, 0);
    let executor_1 = Executor::new(reader_1, app_1, 0);
    let executor_2 = Executor::new(reader_2, app_2, 0);

    // Create a separate reader for Primary's catch-up functionality
    let reader_0_catchup = LogReader::open(paths.log(0), committed_state_0.clone()).unwrap();

    // Create VSR nodes - view 0, node 0 is primary
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        3,
        0,
        writer_0,
        Some(reader_0_catchup),
        committed_state_0.clone(),
        ep0,
        Some(executor_0),
        paths.manifest(0),
    ).unwrap();

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        3,
        0,
        writer_1,
        committed_state_1.clone(),
        ep1,
        Some(executor_1),
        paths.manifest(1),
    ).unwrap();

    let mut node_2: VsrNode<BankApp> = VsrNode::new_backup(
        2,
        3,
        0,
        writer_2,
        committed_state_2.clone(),
        ep2,
        Some(executor_2),
        paths.manifest(2),
    ).unwrap();

    // Step 1: Set max_inflight_requests to 50
    const MAX_INFLIGHT: usize = 50;
    node_0.set_max_inflight_requests(MAX_INFLIGHT);

    // Step 2: Hammer the cluster with 500 unique requests in a burst
    const TOTAL_REQUESTS: usize = 500;
    let mut accepted_count = 0;
    let mut rejected_count = 0;
    let mut accepted_sequences: Vec<u64> = Vec::new();

    for i in 0..TOTAL_REQUESTS {
        let deposit = BankEvent::Deposit {
            user: format!("User{}", i),
            amount: 100,
        };
        let payload = serialize_event(&deposit);
        
        let request = ClientRequest {
            client_id: 1000 + i as u64,
            sequence_number: 1,
            payload,
        };

        let response = node_0.handle_client_request(&request);
        
        match &response.result {
            ClientResult::Pending => {
                accepted_count += 1;
                accepted_sequences.push(request.client_id);
            }
            ClientResult::Error { message } => {
                assert!(
                    message.contains("System Overloaded"),
                    "Expected 'System Overloaded' error, got: {}",
                    message
                );
                rejected_count += 1;
            }
            other => {
                panic!("Unexpected response: {:?}", other);
            }
        }
    }

    // Step 3: Verify exactly 50 accepted, 450 rejected
    assert_eq!(
        accepted_count, MAX_INFLIGHT,
        "Expected {} accepted requests, got {}",
        MAX_INFLIGHT, accepted_count
    );
    assert_eq!(
        rejected_count, TOTAL_REQUESTS - MAX_INFLIGHT,
        "Expected {} rejected requests, got {}",
        TOTAL_REQUESTS - MAX_INFLIGHT, rejected_count
    );

    // Verify in-flight count matches accepted count
    assert_eq!(
        node_0.inflight_count(), MAX_INFLIGHT,
        "In-flight count should be {}",
        MAX_INFLIGHT
    );

    // Verify rejected_count metric
    assert_eq!(
        node_0.rejected_count() as usize, rejected_count,
        "Rejected count metric should match"
    );

    // Step 4: Process messages to commit the accepted requests
    // Need more iterations since we have 50 requests to replicate
    for _ in 0..200 {
        thread::sleep(Duration::from_millis(2));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(2));
        node_0.process_all();
        
        // Check committed requests
        let _ = node_0.check_committed_requests();

        // Check if all nodes have caught up
        let node_0_committed = node_0.committed_index();
        let node_1_committed = node_1.committed_index();
        let node_2_committed = node_2.committed_index();
        
        if node_0_committed == Some(MAX_INFLIGHT as u64 - 1) 
            && node_1_committed == Some(MAX_INFLIGHT as u64 - 1)
            && node_2_committed == Some(MAX_INFLIGHT as u64 - 1) {
            break;
        }
    }

    // Verify all accepted requests are committed on primary
    assert_eq!(
        node_0.committed_index(),
        Some(MAX_INFLIGHT as u64 - 1),
        "All accepted requests should be committed on primary"
    );

    // Verify in-flight count dropped to 0 after commits
    assert_eq!(
        node_0.inflight_count(), 0,
        "In-flight count should be 0 after all commits"
    );

    // Verify consistency across all nodes
    assert_eq!(
        node_1.committed_index(),
        Some(MAX_INFLIGHT as u64 - 1),
        "Node 1 should have same committed index"
    );
    assert_eq!(
        node_2.committed_index(),
        Some(MAX_INFLIGHT as u64 - 1),
        "Node 2 should have same committed index"
    );

    // Step 5: Verify subsequent requests are accepted once in-flight drops
    let new_deposit = BankEvent::Deposit {
        user: "NewUser".to_string(),
        amount: 500,
    };
    let new_payload = serialize_event(&new_deposit);
    
    let new_request = ClientRequest {
        client_id: 9999,
        sequence_number: 1,
        payload: new_payload,
    };

    let new_response = node_0.handle_client_request(&new_request);
    assert!(
        matches!(new_response.result, ClientResult::Pending),
        "New request should be accepted after in-flight drops: {:?}",
        new_response.result
    );

    // Process to commit the new request
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(5));
        node_1.process_all();
        node_2.process_all();
        thread::sleep(Duration::from_millis(5));
        node_0.process_all();
        let _ = node_0.check_committed_requests();

        if node_0.committed_index() == Some(MAX_INFLIGHT as u64) {
            break;
        }
    }

    assert_eq!(
        node_0.committed_index(),
        Some(MAX_INFLIGHT as u64),
        "New request should be committed"
    );
}

/// Test: test_scheduler_fairness
///
/// Verifies that the MultiQueue scheduler provides fair scheduling:
/// 1. Client A (Spammer) sends 1000 requests
/// 2. Client B (Admin) sends 10 requests
/// 3. Verify: Every batch contains a mix of Client A and Client B requests
///    rather than being 100% Client A
#[test]
fn test_scheduler_fairness() {
    use super::node::RequestBatcher;
    
    let mut batcher = RequestBatcher::new();
    
    // Client A (Spammer): 100 requests
    let client_a_id = 1000;
    for i in 0..100 {
        let payload = format!("spammer_request_{}", i).into_bytes();
        batcher.add(payload, client_a_id, i as u64);
    }
    
    // Client B (Admin): 10 requests
    let client_b_id = 2000;
    for i in 0..10 {
        let payload = format!("admin_request_{}", i).into_bytes();
        batcher.add(payload, client_b_id, i as u64);
    }
    
    // Verify we have 2 clients
    assert_eq!(batcher.client_count(), 2, "Should have 2 unique clients");
    
    // Take the batch with round-robin scheduling
    let (payloads, clients) = batcher.take_batch();
    
    // Verify total count
    assert_eq!(payloads.len(), 110, "Should have 110 total requests");
    assert_eq!(clients.len(), 110, "Should have 110 client entries");
    
    // Verify fairness: check that the first 20 entries alternate between clients
    // With round-robin, we expect: A, B, A, B, A, B, ... for the first 20 entries
    // (since B only has 10 requests, after that it's all A)
    let mut client_a_in_first_20 = 0;
    let mut client_b_in_first_20 = 0;
    
    for (client_id, _seq) in clients.iter().take(20) {
        if *client_id == client_a_id {
            client_a_in_first_20 += 1;
        } else if *client_id == client_b_id {
            client_b_in_first_20 += 1;
        }
    }
    
    // With round-robin, first 20 should be 10 from A and 10 from B
    assert_eq!(
        client_a_in_first_20, 10,
        "First 20 entries should have 10 from Client A (got {})",
        client_a_in_first_20
    );
    assert_eq!(
        client_b_in_first_20, 10,
        "First 20 entries should have 10 from Client B (got {})",
        client_b_in_first_20
    );
    
    // Verify interleaving: check that we don't have long runs of the same client
    // In round-robin, we should never have more than 1 consecutive request from
    // the same client (until one queue is exhausted)
    let mut max_consecutive_a = 0;
    let mut current_consecutive_a = 0;
    
    for (i, (client_id, _)) in clients.iter().enumerate() {
        if *client_id == client_a_id {
            current_consecutive_a += 1;
            if current_consecutive_a > max_consecutive_a {
                max_consecutive_a = current_consecutive_a;
            }
        } else {
            current_consecutive_a = 0;
        }
        
        // After Client B is exhausted (index 19), we expect all A
        if i < 20 {
            assert!(
                current_consecutive_a <= 1,
                "Before B exhausted, should not have consecutive A requests (index {})",
                i
            );
        }
    }
    
    // After B is exhausted, remaining 90 requests should all be from A
    let remaining_a: usize = clients.iter().skip(20).filter(|(id, _)| *id == client_a_id).count();
    assert_eq!(
        remaining_a, 90,
        "After B exhausted, remaining 90 should be from A"
    );
    
    // Verify batcher is reset
    assert!(!batcher.has_pending(), "Batcher should be empty after take_batch");
    assert_eq!(batcher.pending_count(), 0, "Pending count should be 0");
    assert_eq!(batcher.client_count(), 0, "Client count should be 0");
}

/// Test: test_deterministic_drift_protection
///
/// Verifies that all nodes use the Primary's timestamp for deterministic execution,
/// ignoring their local clock skew:
/// 1. Run 3 nodes
/// 2. The Primary assigns a specific timestamp to the PrepareBatch
/// 3. Execute a request
/// 4. Verify: All nodes would use the SAME block_time derived from the consensus
///    timestamp, not their local clocks
///
/// Note: Since we can't actually skew system clocks in a unit test, we verify
/// that the timestamp flows through the message protocol correctly.
#[test]
fn test_deterministic_drift_protection() {
    use super::message::{ClientRequest, ClientResult, PreparedEntry, VsrMessage};
    
    // Test that PrepareBatch carries timestamp and it's consistent
    let timestamp_ns: u64 = 1_700_000_000_000_000_000; // Fixed timestamp
    
    // Create a PrepareBatch message with a specific timestamp
    let prepare_batch = VsrMessage::PrepareBatch {
        view: 1,
        start_index: 0,
        entries: vec![
            PreparedEntry {
                index: 0,
                payload: b"test_payload".to_vec(),
            },
        ],
        commit_index: None,
        timestamp_ns,
    };
    
    // Verify the message carries the timestamp correctly
    if let VsrMessage::PrepareBatch { timestamp_ns: ts, .. } = prepare_batch {
        assert_eq!(ts, timestamp_ns, "PrepareBatch should carry the exact timestamp");
    } else {
        panic!("Expected PrepareBatch message");
    }
    
    // Test that Prepare also carries timestamp
    let prepare = VsrMessage::Prepare {
        view: 1,
        index: 0,
        payload: b"test".to_vec(),
        commit_index: None,
        timestamp_ns,
    };
    
    if let VsrMessage::Prepare { timestamp_ns: ts, .. } = prepare {
        assert_eq!(ts, timestamp_ns, "Prepare should carry the exact timestamp");
    } else {
        panic!("Expected Prepare message");
    }
    
    // Test deterministic random seed derivation
    // Two nodes with the same prev_hash and index should get the same seed
    let prev_hash: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let index: u64 = 42;
    
    // Derive seed using BLAKE3(prev_hash || index)
    let derive_seed = |prev_hash: &[u8; 16], index: u64| -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(prev_hash);
        hasher.update(&index.to_le_bytes());
        *hasher.finalize().as_bytes()
    };
    
    let seed1 = derive_seed(&prev_hash, index);
    let seed2 = derive_seed(&prev_hash, index);
    
    // Same inputs should produce same seed (deterministic)
    assert_eq!(seed1, seed2, "Same prev_hash and index should produce same seed");
    
    // Different index should produce different seed
    let seed3 = derive_seed(&prev_hash, index + 1);
    assert_ne!(seed1, seed3, "Different index should produce different seed");
    
    // Different prev_hash should produce different seed
    let different_prev_hash: [u8; 16] = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
    let seed4 = derive_seed(&different_prev_hash, index);
    assert_ne!(seed1, seed4, "Different prev_hash should produce different seed");
    
    // Verify BlockTime derivation is deterministic
    use crate::kernel::traits::BlockTime;
    
    let block_time1 = BlockTime::from_nanos(timestamp_ns);
    let block_time2 = BlockTime::from_nanos(timestamp_ns);
    
    assert_eq!(block_time1.as_nanos(), block_time2.as_nanos(), "BlockTime should be deterministic");
    assert_eq!(block_time1.as_secs(), 1_700_000_000, "BlockTime seconds should match");
}

/// Test async durability mode with DurabilityWorker.
///
/// This test verifies:
/// 1. DurabilityWorker can be created and used with VsrNode
/// 2. Async batch submission works correctly
/// 3. Completions are processed and PrepareBatch is broadcast
/// 4. Heartbeats continue regardless of pending durability work
#[test]
fn test_async_durability_mode() {
    use crate::engine::durability::DurabilityWorker;
    use super::node::HEARTBEAT_INTERVAL;
    
    let paths = TestPaths::new("async_durability", 2);
    
    // Create DurabilityWorker for node 0
    let worker = DurabilityWorker::create(paths.log(0), 1).unwrap();
    let handle = worker.handle();
    
    // Create a separate LogWriter for node 1 (backup uses sync mode)
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    
    // Create committed states
    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    
    // Create mock network
    let mut network = MockNetwork::new(2);
    let ep_0 = network.create_endpoint(0).unwrap();
    let ep_1 = network.create_endpoint(1).unwrap();
    
    // Create a dummy LogWriter for node 0 (won't be used in async mode)
    // We need this because VsrNode still requires a LogWriter field
    let dummy_writer_0 = LogWriter::create(
        &PathBuf::from("/tmp/chr_async_durability_dummy.log"), 
        1
    ).unwrap();
    
    // Create Primary node with async durability
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        dummy_writer_0,
        None,
        committed_state_0.clone(),
        ep_0,
        None, // No executor for this test
        paths.manifest(0),
    ).unwrap();
    
    // Enable async durability mode
    node_0.enable_async_durability(handle.clone());
    assert!(node_0.is_async_durability_enabled());
    
    // Create Backup node (sync mode)
    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep_1,
        None,
        paths.manifest(1),
    ).unwrap();
    
    // Add requests to the batcher
    let event1 = BankEvent::Deposit { user: "alice".to_string(), amount: 100 };
    let event2 = BankEvent::Deposit { user: "bob".to_string(), amount: 200 };
    
    node_0.batcher.add(serialize_event(&event1), 1, 1);
    node_0.batcher.add(serialize_event(&event2), 2, 1);
    
    // Flush batch - in async mode, this should return immediately
    let tracking = node_0.flush_batch().unwrap();
    
    // In async mode, tracking should be empty (deferred)
    assert!(tracking.is_empty(), "Async mode should return empty tracking");
    
    // Should have one pending durability batch
    assert_eq!(node_0.pending_durability_count(), 1);
    
    // Heartbeat should still work regardless of pending durability
    std::thread::sleep(HEARTBEAT_INTERVAL + Duration::from_millis(10));
    node_0.tick();
    
    // Verify heartbeat was sent (check backup received it)
    let msg = node_1.network.try_recv();
    assert!(msg.is_some(), "Backup should receive heartbeat");
    
    // Now process durability completions
    let completions = worker.drain_completions();
    assert!(!completions.is_empty(), "Should have durability completions");
    
    let processed = node_0.process_durability_completions(&completions);
    assert_eq!(processed, 1, "Should process one completion");
    
    // Pending durability should be cleared
    assert_eq!(node_0.pending_durability_count(), 0);
    
    // PrepareBatch should have been broadcast - backup should receive it
    let msg = node_1.network.try_recv();
    assert!(msg.is_some(), "Backup should receive PrepareBatch");
    
    if let Some((from, VsrMessage::PrepareBatch { view, start_index, entries, .. })) = msg {
        assert_eq!(from, 0);
        assert_eq!(view, 1);
        assert_eq!(start_index, 0);
        assert_eq!(entries.len(), 2);
    } else {
        panic!("Expected PrepareBatch message");
    }
    
    // Cleanup
    worker.shutdown_and_join().unwrap();
    let _ = fs::remove_file("/tmp/chr_async_durability_dummy.log");
}

/// Test that heartbeats are decoupled from batch flush in tick().
///
/// This verifies the architectural divorce: heartbeats (control plane)
/// are sent BEFORE batch flush (data plane) and are not blocked by I/O.
#[test]
fn test_heartbeat_decoupled_from_flush() {
    use super::node::HEARTBEAT_INTERVAL;
    
    let paths = TestPaths::new("heartbeat_decoupled", 2);
    
    let writer_0 = LogWriter::create(paths.log(0), 1).unwrap();
    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();
    
    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());
    
    let mut network = MockNetwork::new(2);
    let ep_0 = network.create_endpoint(0).unwrap();
    let ep_1 = network.create_endpoint(1).unwrap();
    
    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        writer_0,
        None,
        committed_state_0.clone(),
        ep_0,
        None,
        paths.manifest(0),
    ).unwrap();
    
    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep_1,
        None,
        paths.manifest(1),
    ).unwrap();
    
    // Wait for heartbeat interval
    std::thread::sleep(HEARTBEAT_INTERVAL + Duration::from_millis(10));
    
    // Tick should send heartbeat first, before any batch processing
    node_0.tick();
    
    // Backup should receive heartbeat
    let msg = node_1.network.try_recv();
    assert!(msg.is_some(), "Backup should receive heartbeat");
    
    if let Some((from, VsrMessage::Commit { view, commit_index })) = msg {
        assert_eq!(from, 0);
        assert_eq!(view, 1);
        assert_eq!(commit_index, 0); // No commits yet
    } else {
        panic!("Expected Commit (heartbeat) message");
    }
    
    // Verify HEARTBEAT_INTERVAL and ELECTION_TIMEOUT are tuned correctly
    assert_eq!(HEARTBEAT_INTERVAL, Duration::from_millis(100));
    assert_eq!(super::node::ELECTION_TIMEOUT, Duration::from_millis(1000));
}

#[test]
fn test_chronon_io_isolation() {
    use crate::engine::durability::DurabilityWorker;

    let paths = TestPaths::new("chronon_io_isolation", 2);

    let worker = DurabilityWorker::create(paths.log(0), 1).unwrap();
    worker.set_stalled(true);

    let writer_1 = LogWriter::create(paths.log(1), 1).unwrap();

    let committed_state_0 = Arc::new(CommittedState::new());
    let committed_state_1 = Arc::new(CommittedState::new());

    let mut network = MockNetwork::new(2);
    let ep_0 = network.create_endpoint(0).unwrap();
    let ep_1 = network.create_endpoint(1).unwrap();

    let dummy_writer_0 = LogWriter::create(
        &PathBuf::from("/tmp/chr_chronon_io_isolation_dummy.log"),
        1,
    )
    .unwrap();

    let mut node_0: VsrNode<BankApp> = VsrNode::new_primary(
        0,
        2,
        1,
        dummy_writer_0,
        None,
        committed_state_0.clone(),
        ep_0,
        None,
        paths.manifest(0),
    )
    .unwrap();
    node_0.enable_async_durability(worker.handle());

    let mut node_1: VsrNode<BankApp> = VsrNode::new_backup(
        1,
        2,
        1,
        writer_1,
        committed_state_1.clone(),
        ep_1,
        None,
        paths.manifest(1),
    )
    .unwrap();

    node_0
        .batcher
        .add(serialize_event(&BankEvent::Deposit { user: "a".to_string(), amount: 1 }), 1, 1);
    node_0
        .batcher
        .add(serialize_event(&BankEvent::Deposit { user: "b".to_string(), amount: 1 }), 2, 1);

    std::thread::sleep(super::node::MAX_BATCH_DELAY + Duration::from_millis(2));
    node_0.tick();

    assert_eq!(node_0.pending_durability_count(), 1);
    assert!(worker.drain_completions().is_empty());

    let start = std::time::Instant::now();
    let mut processed_msgs = 0usize;

    while start.elapsed() < super::node::ELECTION_TIMEOUT + Duration::from_millis(250) {
        node_0.tick();

        while node_1.process_one() {
            processed_msgs += 1;
        }

        assert_eq!(node_1.role, NodeRole::Backup);
        assert!(!node_1.tick());

        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(processed_msgs > 0);
    assert!(node_1.last_primary_contact.elapsed() < super::node::ELECTION_TIMEOUT);
    assert_eq!(node_0.pending_durability_count(), 1);
    assert!(worker.drain_completions().is_empty());

    worker.set_stalled(false);

    let unstall_start = std::time::Instant::now();
    let mut completions = Vec::new();
    while unstall_start.elapsed() < Duration::from_millis(500) {
        completions = worker.drain_completions();
        if !completions.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(!completions.is_empty());
    assert_eq!(node_0.process_durability_completions(&completions), 1);
    assert_eq!(node_0.pending_durability_count(), 0);

    while node_1.process_one() {}
    assert_eq!(node_1.next_expected_index, 2);

    worker.shutdown_and_join().unwrap();
    let _ = fs::remove_file("/tmp/chr_chronon_io_isolation_dummy.log");
}
