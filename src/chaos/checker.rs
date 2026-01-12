use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Types of operations that can be recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Deposit operation.
    Deposit { user: String, amount: u64 },
    /// Withdrawal operation.
    Withdrawal { user: String, amount: u64 },
    /// Balance query.
    Query { user: String },
}

/// Result of an operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationResult {
    /// Operation succeeded.
    Success {
        /// For queries, this is the balance. For deposits/withdrawals, this is the new balance.
        balance: Option<u64>,
    },
    /// Operation failed.
    Failure { reason: String },
    /// Operation is pending (not yet committed).
    Pending,
    /// Operation was redirected to another node.
    Redirected { to_node: Option<u32> },
}

/// A single entry in the operation history.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// When the operation was initiated.
    pub timestamp: Instant,
    /// Client that initiated the operation.
    pub client_id: u64,
    /// Sequence number for this client.
    pub sequence_number: u64,
    /// The operation performed.
    pub operation: Operation,
    /// The result of the operation.
    pub result: OperationResult,
    /// Log index where this was committed (if applicable).
    pub log_index: Option<u64>,
}

/// Global history log for all operations.
#[derive(Debug, Default)]
pub struct History {
    /// All recorded entries.
    entries: Vec<HistoryEntry>,
    /// Index by client_id and sequence_number.
    by_client: HashMap<(u64, u64), usize>,
}

impl History {
    pub fn new() -> Self {
        History {
            entries: Vec::new(),
            by_client: HashMap::new(),
        }
    }

    /// Record an operation.
    pub fn record(
        &mut self,
        client_id: u64,
        sequence_number: u64,
        operation: Operation,
        result: OperationResult,
        log_index: Option<u64>,
    ) {
        let entry = HistoryEntry {
            timestamp: Instant::now(),
            client_id,
            sequence_number,
            operation,
            result,
            log_index,
        };

        let idx = self.entries.len();
        self.entries.push(entry);
        self.by_client.insert((client_id, sequence_number), idx);
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    /// Get entry by client and sequence number.
    pub fn get(&self, client_id: u64, sequence_number: u64) -> Option<&HistoryEntry> {
        self.by_client
            .get(&(client_id, sequence_number))
            .and_then(|&idx| self.entries.get(idx))
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.by_client.clear();
    }
}

/// Thread-safe history wrapper.
pub struct SharedHistory {
    inner: Arc<Mutex<History>>,
}

impl SharedHistory {
    /// Create a new shared history.
    pub fn new() -> Self {
        SharedHistory {
            inner: Arc::new(Mutex::new(History::new())),
        }
    }

    /// Record an operation.
    pub fn record(
        &self,
        client_id: u64,
        sequence_number: u64,
        operation: Operation,
        result: OperationResult,
        log_index: Option<u64>,
    ) {
        let mut history = self.inner.lock().unwrap();
        history.record(client_id, sequence_number, operation, result, log_index);
    }

    /// Get a clone of the inner history for analysis.
    pub fn snapshot(&self) -> History {
        let history = self.inner.lock().unwrap();
        History {
            entries: history.entries.clone(),
            by_client: history.by_client.clone(),
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    /// Clone the Arc for sharing.
    pub fn clone_arc(&self) -> Self {
        SharedHistory {
            inner: self.inner.clone(),
        }
    }
}

impl Default for SharedHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedHistory {
    fn clone(&self) -> Self {
        self.clone_arc()
    }
}

/// Result of a consistency check.
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Whether all checks passed.
    pub passed: bool,
    /// List of violations found.
    pub violations: Vec<Violation>,
    /// Statistics about the check.
    pub stats: CheckStats,
}

/// A consistency violation.
#[derive(Debug, Clone)]
pub struct Violation {
    /// Type of violation.
    pub kind: ViolationKind,
    /// Description of the violation.
    pub description: String,
    /// Related history entries (by index).
    pub related_entries: Vec<usize>,
}

/// Types of consistency violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationKind {
    /// Value was not conserved (money appeared or disappeared).
    ValueNotConserved,
    /// Balance decreased without a withdrawal (time travel).
    TimeTravel,
    /// Duplicate operation was executed twice.
    DuplicateExecution,
    /// Operation ordering violation.
    OrderingViolation,
}

/// Statistics from the consistency check.
#[derive(Debug, Clone, Default)]
pub struct CheckStats {
    /// Total operations checked.
    pub total_operations: usize,
    /// Successful operations.
    pub successful_operations: usize,
    /// Failed operations.
    pub failed_operations: usize,
    /// Redirected operations.
    pub redirected_operations: usize,
    /// Total deposits.
    pub total_deposits: u64,
    /// Total withdrawals.
    pub total_withdrawals: u64,
    /// Final total balance.
    pub final_total_balance: u64,
    /// Expected total balance.
    pub expected_total_balance: u64,
}

/// The consistency checker (Oracle).
pub struct Checker {
    /// Initial balances for each user.
    initial_balances: HashMap<String, u64>,
}

impl Checker {
    /// Create a new checker with initial balances.
    pub fn new(initial_balances: HashMap<String, u64>) -> Self {
        Checker { initial_balances }
    }

    /// Create a checker with no initial balances.
    pub fn empty() -> Self {
        Checker {
            initial_balances: HashMap::new(),
        }
    }

    /// Verify linearizability and consistency invariants.
    pub fn verify(&self, history: &History, final_balances: &HashMap<String, u64>) -> CheckResult {
        let mut violations = Vec::new();
        let mut stats = CheckStats::default();

        // Track per-user balance observations for time travel detection
        let mut user_observations: HashMap<String, Vec<(usize, u64)>> = HashMap::new();

        // Track successful operations by (client_id, sequence_number) for duplicate detection
        let mut successful_ops: HashMap<(u64, u64), usize> = HashMap::new();

        // Process all entries
        for (idx, entry) in history.entries().iter().enumerate() {
            stats.total_operations += 1;

            match &entry.result {
                OperationResult::Success { balance } => {
                    stats.successful_operations += 1;

                    // Check for duplicate execution
                    let key = (entry.client_id, entry.sequence_number);
                    if let Some(&prev_idx) = successful_ops.get(&key) {
                        violations.push(Violation {
                            kind: ViolationKind::DuplicateExecution,
                            description: format!(
                                "Operation (client={}, seq={}) executed twice at entries {} and {}",
                                entry.client_id, entry.sequence_number, prev_idx, idx
                            ),
                            related_entries: vec![prev_idx, idx],
                        });
                    } else {
                        successful_ops.insert(key, idx);
                    }

                    // Track deposits and withdrawals
                    match &entry.operation {
                        Operation::Deposit { user, amount } => {
                            stats.total_deposits += amount;
                            if let Some(bal) = balance {
                                user_observations
                                    .entry(user.clone())
                                    .or_default()
                                    .push((idx, *bal));
                            }
                        }
                        Operation::Withdrawal { user, amount } => {
                            stats.total_withdrawals += amount;
                            if let Some(bal) = balance {
                                user_observations
                                    .entry(user.clone())
                                    .or_default()
                                    .push((idx, *bal));
                            }
                        }
                        Operation::Query { user } => {
                            if let Some(bal) = balance {
                                user_observations
                                    .entry(user.clone())
                                    .or_default()
                                    .push((idx, *bal));
                            }
                        }
                    }
                }
                OperationResult::Failure { .. } => {
                    stats.failed_operations += 1;
                }
                OperationResult::Redirected { .. } => {
                    stats.redirected_operations += 1;
                }
                OperationResult::Pending => {
                    // Pending operations don't count
                }
            }
        }

        // Check time travel: balance should not decrease without withdrawal
        for (user, observations) in &user_observations {
            let mut last_balance: Option<u64> = None;
            let mut last_idx: Option<usize> = None;

            for &(idx, balance) in observations {
                if let Some(prev_balance) = last_balance {
                    // Check if balance decreased
                    if balance < prev_balance {
                        // Check if there was a withdrawal between these observations
                        let had_withdrawal =
                            history.entries()[last_idx.unwrap()..=idx].iter().any(|e| {
                                matches!(
                                    (&e.operation, &e.result),
                                    (
                                        Operation::Withdrawal { user: u, .. },
                                        OperationResult::Success { .. }
                                    ) if u == user
                                )
                            });

                        if !had_withdrawal {
                            violations.push(Violation {
                                kind: ViolationKind::TimeTravel,
                                description: format!(
                                    "User {} balance decreased from {} to {} without withdrawal (entries {} to {})",
                                    user, prev_balance, balance, last_idx.unwrap(), idx
                                ),
                                related_entries: vec![last_idx.unwrap(), idx],
                            });
                        }
                    }
                }
                last_balance = Some(balance);
                last_idx = Some(idx);
            }
        }

        // Check conservation of value
        let initial_total: u64 = self.initial_balances.values().sum();
        let expected_total = initial_total + stats.total_deposits - stats.total_withdrawals;
        let actual_total: u64 = final_balances.values().sum();

        stats.expected_total_balance = expected_total;
        stats.final_total_balance = actual_total;

        if expected_total != actual_total {
            violations.push(Violation {
                kind: ViolationKind::ValueNotConserved,
                description: format!(
                    "Value not conserved: initial={}, deposits={}, withdrawals={}, expected={}, actual={}",
                    initial_total, stats.total_deposits, stats.total_withdrawals, expected_total, actual_total
                ),
                related_entries: vec![],
            });
        }

        CheckResult {
            passed: violations.is_empty(),
            violations,
            stats,
        }
    }

    /// Quick check that just verifies value conservation.
    pub fn verify_conservation(
        &self,
        total_deposits: u64,
        total_withdrawals: u64,
        final_total: u64,
    ) -> bool {
        let initial_total: u64 = self.initial_balances.values().sum();
        let expected = initial_total + total_deposits - total_withdrawals;
        expected == final_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_recording() {
        let mut history = History::new();

        history.record(
            1,
            1,
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            OperationResult::Success { balance: Some(100) },
            Some(0),
        );

        assert_eq!(history.len(), 1);
        let entry = history.get(1, 1).unwrap();
        assert_eq!(entry.client_id, 1);
        assert_eq!(entry.sequence_number, 1);
    }

    #[test]
    fn test_checker_conservation() {
        let checker = Checker::empty();

        let mut history = History::new();

        // Deposit 100
        history.record(
            1,
            1,
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            OperationResult::Success { balance: Some(100) },
            Some(0),
        );

        // Deposit 50
        history.record(
            1,
            2,
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 50,
            },
            OperationResult::Success { balance: Some(150) },
            Some(1),
        );

        // Withdraw 30
        history.record(
            1,
            3,
            Operation::Withdrawal {
                user: "Alice".to_string(),
                amount: 30,
            },
            OperationResult::Success { balance: Some(120) },
            Some(2),
        );

        let mut final_balances = HashMap::new();
        final_balances.insert("Alice".to_string(), 120);

        let result = checker.verify(&history, &final_balances);
        assert!(result.passed, "Violations: {:?}", result.violations);
        assert_eq!(result.stats.total_deposits, 150);
        assert_eq!(result.stats.total_withdrawals, 30);
    }

    #[test]
    fn test_checker_detects_value_loss() {
        let checker = Checker::empty();

        let mut history = History::new();

        // Deposit 100
        history.record(
            1,
            1,
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            OperationResult::Success { balance: Some(100) },
            Some(0),
        );

        // Final balance is wrong (should be 100, but is 50)
        let mut final_balances = HashMap::new();
        final_balances.insert("Alice".to_string(), 50);

        let result = checker.verify(&history, &final_balances);
        assert!(!result.passed);
        assert!(result
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::ValueNotConserved));
    }

    #[test]
    fn test_checker_detects_duplicate() {
        let checker = Checker::empty();

        let mut history = History::new();

        // Same operation recorded twice as successful
        history.record(
            1,
            1,
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            OperationResult::Success { balance: Some(100) },
            Some(0),
        );

        history.record(
            1,
            1, // Same client_id and sequence_number
            Operation::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            OperationResult::Success { balance: Some(200) },
            Some(1),
        );

        let mut final_balances = HashMap::new();
        final_balances.insert("Alice".to_string(), 200);

        let result = checker.verify(&history, &final_balances);
        assert!(!result.passed);
        assert!(result
            .violations
            .iter()
            .any(|v| v.kind == ViolationKind::DuplicateExecution));
    }
}
