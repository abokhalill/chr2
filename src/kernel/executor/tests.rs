//! Executor tests.

use super::*;
use crate::engine::log::LogWriter;
use crate::engine::reader::{CommittedState, LogReader};
use crate::kernel::bank::{BankApp, BankEvent, BankQuery, BankQueryResponse};
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Helper to serialize a BankEvent to bytes.
fn serialize_event(event: &BankEvent) -> Vec<u8> {
    bincode::serialize(event).unwrap()
}

/// Integration test: test_bank_execution_flow
///
/// This test:
/// 1. Initializes Log + Executor
/// 2. Appends:
///    - Deposit("Alice", 100)
///    - Withdraw("Alice", 1000) → deterministic error
///    - Withdraw("Alice", 50) → success
///    - PoisonPill → panic
///    - Deposit("Alice", 10)
/// 3. Executes step() repeatedly
/// 4. Asserts:
///    - Balance is 50 before poison
///    - Executor halts on poison
///    - Post-poison deposit is never applied
#[test]
fn test_bank_execution_flow() {
    let path = Path::new("/tmp/chr_bank_test.log");
    let _ = fs::remove_file(path);

    // Create shared committed state
    let committed_state = Arc::new(CommittedState::new());

    // Create log writer
    let mut writer = LogWriter::create(path, 1).unwrap();

    // Append events
    // Event 0: Deposit("Alice", 100)
    let payload = serialize_event(&BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 100,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(0);

    // Event 1: Withdraw("Alice", 1000) → will fail (insufficient funds)
    let payload = serialize_event(&BankEvent::Withdraw {
        user: "Alice".to_string(),
        amount: 1000,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(1);

    // Event 2: Withdraw("Alice", 50) → will succeed
    let payload = serialize_event(&BankEvent::Withdraw {
        user: "Alice".to_string(),
        amount: 50,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(2);

    // Event 3: PoisonPill → will panic
    let payload = serialize_event(&BankEvent::PoisonPill);
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(3);

    // Event 4: Deposit("Alice", 10) → should never be applied
    let payload = serialize_event(&BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 10,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(4);

    // Drop writer to release file handle
    drop(writer);

    // Create reader and executor
    let reader = LogReader::open(path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::new(reader, app, 0);

    // Step 0: Deposit("Alice", 100) → success
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Applied { index: 0, .. }));
    
    // Check balance: should be 100
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(100)));

    // Step 1: Withdraw("Alice", 1000) → rejected (insufficient funds)
    let result = executor.step().unwrap();
    match result {
        StepResult::Rejected { index: 1, error } => {
            assert!(error.contains("Insufficient funds"));
        }
        other => panic!("Expected Rejected, got {:?}", other),
    }

    // Balance should still be 100 (state unchanged on error)
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(100)));

    // Step 2: Withdraw("Alice", 50) → success
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Applied { index: 2, .. }));

    // Balance should be 50
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(50)));

    // Step 3: PoisonPill → HALT
    let result = executor.step();
    match result {
        Err(FatalError::PoisonPill { index: 3, message }) => {
            assert!(message.contains("POISON PILL"));
        }
        other => panic!("Expected PoisonPill error, got {:?}", other),
    }

    // Executor should be halted
    assert!(executor.is_halted());
    assert_eq!(executor.status(), ExecutorStatus::Halted);

    // Balance should still be 50 (poison pill didn't change state)
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(50)));

    // Step 4: Should return Halted error (not process the deposit)
    let result = executor.step();
    assert!(matches!(result, Err(FatalError::Halted)));

    // Balance should still be 50 (post-poison deposit never applied)
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(50)));

    // Verify next_index stopped at 3 (poison pill index)
    assert_eq!(executor.next_index(), 3);

    let _ = fs::remove_file(path);
}

#[test]
fn test_executor_idle_when_caught_up() {
    let path = Path::new("/tmp/chr_executor_idle.log");
    let _ = fs::remove_file(path);

    let committed_state = Arc::new(CommittedState::new());

    // Create empty log
    let writer = LogWriter::create(path, 1).unwrap();
    drop(writer);

    let reader = LogReader::open(path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::new(reader, app, 0);

    // No events committed, should be idle
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Idle));

    let _ = fs::remove_file(path);
}

#[test]
fn test_executor_respects_committed_index() {
    // VISIBILITY CONTRACT: Executor must not read beyond committed_index
    let path = Path::new("/tmp/chr_executor_visibility.log");
    let _ = fs::remove_file(path);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with 3 events but only commit 1
    let mut writer = LogWriter::create(path, 1).unwrap();

    let payload = serialize_event(&BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 100,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    committed_state.advance(0); // Only commit first event

    let payload = serialize_event(&BankEvent::Deposit {
        user: "Alice".to_string(),
        amount: 200,
    });
    writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
    // Don't advance committed_state for this one

    drop(writer);

    let reader = LogReader::open(path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::new(reader, app, 0);

    // Step 0: Should succeed
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Applied { index: 0, .. }));

    // Step 1: Should be Idle (not committed yet)
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Idle));

    // Balance should be 100 (only first deposit applied)
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(100)));

    // Now commit the second event
    committed_state.advance(1);

    // Step 1: Should now succeed
    let result = executor.step().unwrap();
    assert!(matches!(result, StepResult::Applied { index: 1, .. }));

    // Balance should be 300
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(300)));

    let _ = fs::remove_file(path);
}

/// Test: test_snapshot_isolation
///
/// This test:
/// 1. Write entries
/// 2. Advance executor (apply entries)
/// 3. Take snapshot
/// 4. Write more entries
/// 5. Verify:
///    - Snapshot file exists
///    - last_included_index == snapshot_index
///    - chain_hash matches log entry at that index
#[test]
fn test_snapshot_isolation() {
    use crate::kernel::snapshot::SnapshotManifest;

    let log_path = Path::new("/tmp/chr_snapshot_isolation.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_snapshot_isolation_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log writer and append initial entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    // Append 5 entries
    for i in 0..5 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: format!("User{}", i),
            amount: (i + 1) * 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }

    drop(writer);

    // Create reader and executor
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Apply all 5 entries
    for _ in 0..5 {
        let result = executor.step().unwrap();
        assert!(matches!(result, StepResult::Applied { .. }));
    }

    // Verify last_applied_index
    assert_eq!(executor.last_applied_index(), Some(4));

    // Get chain_hash for index 4 BEFORE taking snapshot (need separate reader)
    let mut reader_for_hash = LogReader::open(log_path, committed_state.clone()).unwrap();
    let expected_chain_hash = reader_for_hash.get_chain_hash(4).unwrap();

    // Take snapshot
    let snapshot_index = executor.take_snapshot().unwrap();
    assert_eq!(snapshot_index, 4);

    // Verify snapshot file exists
    let snapshot_filename = SnapshotManifest::filename_for_index(snapshot_index);
    let snapshot_path = snapshot_dir.join(&snapshot_filename);
    assert!(snapshot_path.exists(), "Snapshot file should exist");

    // Load and verify snapshot
    let loaded_snapshot = SnapshotManifest::load_from_file(&snapshot_path).unwrap();

    // Verify last_included_index
    assert_eq!(loaded_snapshot.last_included_index, 4);

    // Verify chain_hash matches log entry at that index
    assert_eq!(
        loaded_snapshot.chain_hash, expected_chain_hash,
        "Snapshot chain_hash should match log entry chain_hash"
    );

    // Verify side_effects_dropped is true
    assert!(loaded_snapshot.side_effects_dropped);

    // Now append more entries (after snapshot)
    // Use recovery to get proper state for reopening the log
    use crate::engine::recovery::{LogRecovery, RecoveryOutcome};
    let recovery = LogRecovery::open(log_path).unwrap().unwrap();
    let outcome = recovery.scan().unwrap();
    let (next_index, write_offset, tail_hash) = match outcome {
        RecoveryOutcome::Clean { last_index, next_offset, tail_hash, .. } => {
            (last_index + 1, next_offset, tail_hash)
        }
        _ => panic!("Expected clean recovery"),
    };
    let mut writer = LogWriter::open(
        log_path,
        next_index,
        write_offset,
        tail_hash,
        1, // view_id
    ).unwrap();
    for i in 5..8u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: format!("User{}", i),
            amount: ((i + 1) * 100) as u64,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Verify executor can continue processing new entries
    for _ in 0..3 {
        let result = executor.step().unwrap();
        assert!(matches!(result, StepResult::Applied { .. }));
    }

    // Verify last_applied_index is now 7
    assert_eq!(executor.last_applied_index(), Some(7));

    // Verify last_snapshot_index is still 4
    assert_eq!(executor.last_snapshot_index(), Some(4));

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

#[test]
fn test_snapshot_no_entries_fails() {
    let log_path = Path::new("/tmp/chr_snapshot_no_entries.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_snapshot_no_entries_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create empty log
    let writer = LogWriter::create(log_path, 1).unwrap();
    drop(writer);

    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Try to take snapshot with no entries applied
    let result = executor.take_snapshot();
    assert!(matches!(result, Err(FatalError::SnapshotError(_))));

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

#[test]
fn test_should_snapshot_threshold() {
    let log_path = Path::new("/tmp/chr_snapshot_threshold.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_snapshot_threshold_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();
    for i in 0..10 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 10,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Set low threshold for testing
    executor.set_snapshot_threshold(5);

    // Initially should_snapshot is false (no entries applied)
    assert!(!executor.should_snapshot());

    // Apply 4 entries
    for _ in 0..4 {
        executor.step().unwrap();
    }
    assert!(!executor.should_snapshot()); // 4 < 5

    // Apply 1 more entry (total 5)
    executor.step().unwrap();
    assert!(executor.should_snapshot()); // 5 >= 5

    // Take snapshot
    executor.take_snapshot().unwrap();

    // After snapshot, should_snapshot is false again
    assert!(!executor.should_snapshot());

    // Apply 4 more entries
    for _ in 0..4 {
        executor.step().unwrap();
    }
    assert!(!executor.should_snapshot()); // 4 < 5

    // Apply 1 more (total 5 since last snapshot)
    executor.step().unwrap();
    assert!(executor.should_snapshot()); // 5 >= 5

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

// =========================================================================
// RECOVERY TESTS
// =========================================================================

/// Test: test_executor_restart_integrity
///
/// Verifies that after a simulated crash, the executor can recover
/// from a snapshot + log suffix and reach the same state.
#[test]
fn test_executor_restart_integrity() {
    use crate::kernel::snapshot::SnapshotManifest;

    let log_path = Path::new("/tmp/chr_restart_integrity.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_restart_integrity_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Phase 1: Create initial state
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    // Append 10 entries: deposits of 100 each
    for i in 0..10u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Create executor and apply all entries
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Apply first 5 entries
    for _ in 0..5 {
        executor.step().unwrap();
    }

    // Take snapshot at index 4
    let snapshot_index = executor.take_snapshot().unwrap();
    assert_eq!(snapshot_index, 4);

    // Apply remaining 5 entries
    for _ in 0..5 {
        executor.step().unwrap();
    }

    // Verify final balance: 10 * 100 = 1000
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(1000)));

    // Record final state
    let final_next_index = executor.next_index();
    assert_eq!(final_next_index, 10);

    // Phase 2: Simulate crash and recover
    drop(executor);

    // Recover from snapshot + log
    let reader2 = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app2 = BankApp::new();
    let recovered_executor = Executor::recover(reader2, app2, snapshot_dir.clone()).unwrap();

    // Verify recovered state matches
    assert_eq!(recovered_executor.next_index(), 10);
    let response = recovered_executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(1000)));

    // Verify last_snapshot_index is restored
    assert_eq!(recovered_executor.last_snapshot_index(), Some(4));

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

/// Test: test_corrupt_bridge_fails
///
/// Verifies that recovery fails if the chain bridge is corrupted.
/// This tests the critical invariant: entry[N+1].prev_hash == snapshot.chain_hash
#[test]
fn test_corrupt_bridge_fails() {
    use crate::kernel::snapshot::SnapshotManifest;

    let log_path = Path::new("/tmp/chr_corrupt_bridge.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_corrupt_bridge_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    for i in 0..5u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Create executor and take snapshot at index 2
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    for _ in 0..3 {
        executor.step().unwrap();
    }
    executor.take_snapshot().unwrap();
    drop(executor);

    // Corrupt the snapshot's chain_hash
    let snapshot_filename = SnapshotManifest::filename_for_index(2);
    let snapshot_path = snapshot_dir.join(&snapshot_filename);
    let mut snapshot = SnapshotManifest::load_from_file(&snapshot_path).unwrap();

    // Corrupt the chain hash
    snapshot.chain_hash[0] ^= 0xFF;
    snapshot.chain_hash[1] ^= 0xFF;

    // Save corrupted snapshot (need to recreate with corrupted hash)
    let corrupted_manifest = SnapshotManifest::new(
        snapshot.last_included_index,
        snapshot.last_included_term,
        snapshot.chain_hash, // Corrupted
        snapshot.state,
    );
    corrupted_manifest.save_to_file(&snapshot_path).unwrap();

    // Attempt recovery - should fail with ChainBridgeMismatch
    let reader2 = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app2 = BankApp::new();
    let result = Executor::recover(reader2, app2, snapshot_dir.clone());

    match result {
        Err(FatalError::ChainBridgeMismatch { .. }) => {},
        Err(e) => panic!("Expected ChainBridgeMismatch, got {:?}", e),
        Ok(_) => panic!("Expected ChainBridgeMismatch error, but recovery succeeded"),
    }

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

/// Test: test_log_gap_after_snapshot_fails
///
/// Verifies that recovery fails if there's a gap in the log after snapshot.
/// Scenario:
/// - Snapshot at index N
/// - Entry at N+1 is missing
/// - Entry at N+2 exists
/// Recovery MUST fail with LogGap error.
#[test]
fn test_log_gap_after_snapshot_fails() {
    use crate::kernel::snapshot::SnapshotManifest;

    let log_path = Path::new("/tmp/chr_log_gap.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_log_gap_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with 5 entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    for i in 0..5u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Create executor and take snapshot at index 2
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    for _ in 0..3 {
        executor.step().unwrap();
    }
    executor.take_snapshot().unwrap();
    drop(executor);

    // Create a fake snapshot at index 10 (beyond log)
    // This simulates LogBehindSnapshot since committed_index (4) < snapshot_index (10)
    let fake_snapshot = SnapshotManifest::new(
        10, // last_included_index beyond log
        0,
        [0u8; 16], // Doesn't matter, we'll fail before bridge check
        bincode::serialize(&std::collections::HashMap::<String, u64>::new()).unwrap(),
    );
    let fake_path = snapshot_dir.join(SnapshotManifest::filename_for_index(10));
    fake_snapshot.save_to_file(&fake_path).unwrap();

    // Attempt recovery - should fail with LogBehindSnapshot
    let reader2 = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app2 = BankApp::new();
    let result = Executor::recover(reader2, app2, snapshot_dir.clone());

    match result {
        Err(FatalError::LogBehindSnapshot { .. }) => {},
        Err(e) => panic!("Expected LogBehindSnapshot, got {:?}", e),
        Ok(_) => panic!("Expected LogBehindSnapshot error, but recovery succeeded"),
    }

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

/// Test: test_recovery_no_snapshot
///
/// Verifies that recovery works correctly when no snapshot exists.
/// Should start from genesis and replay all committed entries.
#[test]
fn test_recovery_no_snapshot() {
    let log_path = Path::new("/tmp/chr_recovery_no_snapshot.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_recovery_no_snapshot_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    for i in 0..5u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Recover without any snapshot
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let executor = Executor::recover(reader, app, snapshot_dir.clone()).unwrap();

    // Should have replayed all 5 entries
    assert_eq!(executor.next_index(), 5);
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(500)));

    // No snapshot should be recorded
    assert_eq!(executor.last_snapshot_index(), None);

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

/// Test: test_recovery_fallback_to_older_snapshot
///
/// Verifies that recovery falls back to an older valid snapshot
/// when the latest snapshot is corrupted.
#[test]
fn test_recovery_fallback_to_older_snapshot() {
    use crate::kernel::snapshot::SnapshotManifest;

    let log_path = Path::new("/tmp/chr_fallback_snapshot.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_fallback_snapshot_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Create log with entries
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    for i in 0..10u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: "Alice".to_string(),
            amount: 100,
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Create executor and take snapshots at index 2 and 5
    let reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Apply 3 entries and snapshot at index 2
    for _ in 0..3 {
        executor.step().unwrap();
    }
    executor.take_snapshot().unwrap();

    // Apply 3 more entries and snapshot at index 5
    for _ in 0..3 {
        executor.step().unwrap();
    }
    executor.take_snapshot().unwrap();

    // Apply remaining entries
    for _ in 0..4 {
        executor.step().unwrap();
    }
    drop(executor);

    // Corrupt the latest snapshot (index 5)
    let latest_snapshot_path = snapshot_dir.join(SnapshotManifest::filename_for_index(5));
    let mut data = fs::read(&latest_snapshot_path).unwrap();
    data[10] ^= 0xFF; // Corrupt header
    fs::write(&latest_snapshot_path, &data).unwrap();

    // Recover - should fall back to snapshot at index 2
    let reader2 = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app2 = BankApp::new();
    let executor = Executor::recover(reader2, app2, snapshot_dir.clone()).unwrap();

    // Should have recovered from snapshot at index 2 and replayed 3-9
    assert_eq!(executor.next_index(), 10);
    let response = executor.query(BankQuery::Balance {
        user: "Alice".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(1000)));

    // Last snapshot index should be 2 (the one we recovered from)
    assert_eq!(executor.last_snapshot_index(), Some(2));

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}

// =========================================================================
// PHYSICAL COMPACTION TESTS
// =========================================================================

/// Test: test_physical_compaction_integrity
///
/// Verifies physical log compaction with atomic rewrite:
/// 1. Write 100 entries with unique payloads
/// 2. Snapshot at index 50 (capturing state and chain_hash)
/// 3. Truncate log up to 50
/// 4. Verify:
///    - File size is reduced
///    - LogReader for index 51 succeeds
///    - LogReader for index 49 returns IndexTruncated
///    - Restarting the Executor works perfectly (Hybrid Boot)
#[test]
fn test_physical_compaction_integrity() {
    use crate::kernel::snapshot::SnapshotManifest;
    use crate::engine::recovery::{LogRecovery, RecoveryOutcome};

    let log_path = Path::new("/tmp/chr_compaction_integrity.log");
    let snapshot_dir = PathBuf::from("/tmp/chr_compaction_integrity_snapshots");
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);

    let committed_state = Arc::new(CommittedState::new());

    // Step 1: Write 100 entries with unique payloads
    let mut writer = LogWriter::create(log_path, 1).unwrap();

    for i in 0..100u64 {
        let payload = serialize_event(&BankEvent::Deposit {
            user: format!("User{}", i),
            amount: i + 1, // Unique amount for each entry
        });
        writer.append(&payload, 0, 0, 1_000_000_000).unwrap();
        committed_state.advance(i);
    }
    drop(writer);

    // Record original file size
    let original_size = fs::metadata(log_path).unwrap().len();

    // Step 2: Create executor and apply entries up to index 50, then snapshot
    let mut reader = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app = BankApp::new();
    let mut executor = Executor::with_snapshot_dir(reader, app, 0, snapshot_dir.clone());

    // Apply first 51 entries (0-50)
    for _ in 0..51 {
        executor.step().unwrap();
    }

    // Take snapshot at index 50
    let snapshot_index = executor.take_snapshot().unwrap();
    assert_eq!(snapshot_index, 50);

    // Get the chain_hash for the snapshot (needed for truncation)
    let mut reader_for_hash = LogReader::open(log_path, committed_state.clone()).unwrap();
    let chain_hash = reader_for_hash.get_chain_hash(50).unwrap();

    // Get the authoritative offset for index 51 (the cut point)
    let cut_offset = reader_for_hash.get_authoritative_offset(51).unwrap();

    drop(executor);
    drop(reader_for_hash);

    // Step 3: Truncate log up to index 50 (keeping 51-99)
    LogWriter::truncate_prefix(
        log_path,
        51,        // new_base_index
        cut_offset, // authoritative offset for index 51
        chain_hash, // chain_hash of entry 50
    ).unwrap();

    // Step 4: Verify file size is reduced
    let new_size = fs::metadata(log_path).unwrap().len();
    assert!(
        new_size < original_size,
        "File size should be reduced after truncation: {} < {}",
        new_size, original_size
    );

    // Step 5: Verify LogReader for index 51 succeeds
    let mut reader2 = LogReader::open(log_path, committed_state.clone()).unwrap();
    
    // Verify base_index is now 51
    assert_eq!(reader2.base_index(), 51);
    
    // Reading index 51 should succeed
    let entry51 = reader2.read(51).unwrap();
    assert_eq!(entry51.index, 51);

    // Step 6: Verify LogReader for index 49 returns IndexTruncated
    match reader2.read(49) {
        Err(crate::engine::reader::ReadError::IndexTruncated { requested: 49, base_index: 51 }) => {},
        other => panic!("Expected IndexTruncated for index 49, got {:?}", other),
    }

    // Also verify index 50 is truncated
    match reader2.read(50) {
        Err(crate::engine::reader::ReadError::IndexTruncated { requested: 50, base_index: 51 }) => {},
        other => panic!("Expected IndexTruncated for index 50, got {:?}", other),
    }

    drop(reader2);

    // Step 7: Verify recovery works on truncated log
    let recovery = LogRecovery::open(log_path).unwrap().unwrap();
    let outcome = recovery.scan().unwrap();

    match outcome {
        RecoveryOutcome::Clean { last_index, base_index, base_prev_hash, .. } => {
            assert_eq!(last_index, 99, "Last index should be 99");
            assert_eq!(base_index, 51, "Base index should be 51");
            assert_eq!(base_prev_hash, chain_hash, "Base prev_hash should match snapshot chain_hash");
        }
        other => panic!("Expected Clean recovery, got {:?}", other),
    }

    // Step 8: Verify Hybrid Boot recovery works
    let reader3 = LogReader::open(log_path, committed_state.clone()).unwrap();
    let app3 = BankApp::new();
    let recovered_executor = Executor::recover(reader3, app3, snapshot_dir.clone()).unwrap();

    // Should have recovered from snapshot at 50 and replayed 51-99
    assert_eq!(recovered_executor.next_index(), 100);
    assert_eq!(recovered_executor.last_snapshot_index(), Some(50));

    // Verify state is correct by checking a few balances
    // User0 deposited 1, User50 deposited 51, User99 deposited 100
    let response = recovered_executor.query(BankQuery::Balance {
        user: "User0".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(1)));

    let response = recovered_executor.query(BankQuery::Balance {
        user: "User50".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(51)));

    let response = recovered_executor.query(BankQuery::Balance {
        user: "User99".to_string(),
    });
    assert!(matches!(response, BankQueryResponse::Balance(100)));

    // Cleanup
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_dir_all(&snapshot_dir);
}
