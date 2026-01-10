use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::Rng;

use super::network::ChaosNetwork;

/// Types of faults the Nemesis can inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Kill the current primary node.
    KillPrimary,
    /// Partition the primary from a majority of backups.
    PartitionQuorum,
    /// Heal all network issues.
    Heal,
    /// No operation (do nothing this tick).
    Noop,
}

/// Record of a fault injection event.
#[derive(Debug, Clone)]
pub struct FaultEvent {
    /// When the fault was injected.
    pub timestamp: Instant,
    /// Type of fault.
    pub fault: Fault,
    /// Target node (if applicable).
    pub target_node: Option<u32>,
    /// Additional details.
    pub details: String,
}

/// Configuration for the Nemesis.
#[derive(Debug, Clone)]
pub struct NemesisConfig {
    /// How often to consider injecting a fault.
    pub tick_interval: Duration,
    /// Probability of injecting a fault on each tick (0.0 - 1.0).
    pub fault_probability: f64,
    /// Weights for each fault type (KillPrimary, PartitionQuorum, Heal).
    pub fault_weights: [f64; 3],
    /// Minimum time between faults.
    pub min_fault_interval: Duration,
    /// Whether to automatically heal after some time.
    pub auto_heal: bool,
    /// Time after which to auto-heal.
    pub auto_heal_delay: Duration,
}

impl Default for NemesisConfig {
    fn default() -> Self {
        NemesisConfig {
            tick_interval: Duration::from_millis(100),
            fault_probability: 0.3,
            fault_weights: [1.0, 1.0, 2.0], // Favor healing
            min_fault_interval: Duration::from_millis(500),
            auto_heal: true,
            auto_heal_delay: Duration::from_millis(300),
        }
    }
}

/// The Nemesis fault injector.
pub struct Nemesis {
    /// Reference to the chaos network.
    network: Arc<ChaosNetwork>,
    /// Configuration.
    config: NemesisConfig,
    /// Current view (tracked for primary targeting).
    current_view: Arc<AtomicU64>,
    /// Whether the nemesis is running.
    running: Arc<AtomicBool>,
    /// History of fault events.
    history: Arc<std::sync::Mutex<Vec<FaultEvent>>>,
    /// Last fault injection time.
    last_fault_time: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Last non-heal fault time (for auto-heal).
    last_disruption_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl Nemesis {
    /// Create a new Nemesis.
    pub fn new(network: Arc<ChaosNetwork>, config: NemesisConfig) -> Self {
        Nemesis {
            network,
            config,
            current_view: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            history: Arc::new(std::sync::Mutex::new(Vec::new())),
            last_fault_time: Arc::new(std::sync::Mutex::new(None)),
            last_disruption_time: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Update the current view (call this when view changes are detected).
    pub fn set_view(&self, view: u64) {
        self.current_view.store(view, Ordering::SeqCst);
    }

    /// Get the current view.
    pub fn get_view(&self) -> u64 {
        self.current_view.load(Ordering::SeqCst)
    }

    /// Start the nemesis thread.
    pub fn start(&self) -> thread::JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);

        let network = self.network.clone();
        let config = self.config.clone();
        let current_view = self.current_view.clone();
        let running = self.running.clone();
        let history = self.history.clone();
        let last_fault_time = self.last_fault_time.clone();
        let last_disruption_time = self.last_disruption_time.clone();

        thread::spawn(move || {
            let mut rng = rand::thread_rng();

            while running.load(Ordering::SeqCst) {
                thread::sleep(config.tick_interval);

                // Check if we should auto-heal
                if config.auto_heal {
                    let should_heal = {
                        let last = last_disruption_time.lock().unwrap();
                        last.map(|t| t.elapsed() >= config.auto_heal_delay)
                            .unwrap_or(false)
                    };

                    if should_heal {
                        Self::inject_fault_static(
                            &network,
                            Fault::Heal,
                            current_view.load(Ordering::SeqCst),
                            &history,
                        );
                        *last_disruption_time.lock().unwrap() = None;
                        continue;
                    }
                }

                // Check minimum fault interval
                {
                    let last = last_fault_time.lock().unwrap();
                    if let Some(t) = *last {
                        if t.elapsed() < config.min_fault_interval {
                            continue;
                        }
                    }
                }

                // Decide whether to inject a fault
                if rng.gen::<f64>() >= config.fault_probability {
                    continue;
                }

                // Choose a fault type based on weights
                let fault = Self::choose_fault(&config.fault_weights, &mut rng);

                // Inject the fault
                let view = current_view.load(Ordering::SeqCst);
                Self::inject_fault_static(&network, fault, view, &history);

                // Update timing
                *last_fault_time.lock().unwrap() = Some(Instant::now());
                if fault != Fault::Heal && fault != Fault::Noop {
                    *last_disruption_time.lock().unwrap() = Some(Instant::now());
                }
            }
        })
    }

    /// Stop the nemesis.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the nemesis is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Manually inject a specific fault.
    pub fn inject(&self, fault: Fault) {
        let view = self.current_view.load(Ordering::SeqCst);
        Self::inject_fault_static(&self.network, fault, view, &self.history);
    }

    /// Get the fault history.
    pub fn get_history(&self) -> Vec<FaultEvent> {
        self.history.lock().unwrap().clone()
    }

    /// Clear the fault history.
    pub fn clear_history(&self) {
        self.history.lock().unwrap().clear();
    }

    /// Choose a fault type based on weights.
    fn choose_fault(weights: &[f64; 3], rng: &mut impl Rng) -> Fault {
        let total: f64 = weights.iter().sum();
        let mut r = rng.gen::<f64>() * total;

        if r < weights[0] {
            return Fault::KillPrimary;
        }
        r -= weights[0];

        if r < weights[1] {
            return Fault::PartitionQuorum;
        }

        Fault::Heal
    }

    /// Inject a fault (static version for thread).
    fn inject_fault_static(
        network: &ChaosNetwork,
        fault: Fault,
        view: u64,
        history: &std::sync::Mutex<Vec<FaultEvent>>,
    ) {
        let cluster_size = network.cluster_size();
        let primary = network.primary_for_view(view);

        let (target_node, details) = match fault {
            Fault::KillPrimary => {
                // Kill the current primary
                network.kill_node(primary);
                (
                    Some(primary),
                    format!("Killed primary node {} (view {})", primary, view),
                )
            }
            Fault::PartitionQuorum => {
                // Partition primary from majority of backups
                let mut backups: Vec<u32> = (0..cluster_size).filter(|&n| n != primary).collect();
                let quorum_size = (cluster_size / 2) + 1;
                let partition_count = quorum_size.min(backups.len() as u32);

                // Shuffle and take partition_count backups to partition
                backups.shuffle(&mut rand::thread_rng());
                let partitioned: Vec<u32> = backups.into_iter().take(partition_count as usize).collect();

                for &backup in &partitioned {
                    network.partition(primary, backup);
                }

                (
                    Some(primary),
                    format!(
                        "Partitioned primary {} from backups {:?} (view {})",
                        primary, partitioned, view
                    ),
                )
            }
            Fault::Heal => {
                // Heal all partitions and revive all nodes
                network.heal_all();
                for node_id in 0..cluster_size {
                    network.revive_node(node_id);
                }
                (None, "Healed all network issues".to_string())
            }
            Fault::Noop => (None, "No operation".to_string()),
        };

        // Record the event
        let event = FaultEvent {
            timestamp: Instant::now(),
            fault,
            target_node,
            details: details.clone(),
        };

        history.lock().unwrap().push(event);

        eprintln!("[NEMESIS] {}", details);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaos::network::ChaosConfig;

    #[test]
    fn test_nemesis_kill_primary() {
        let chaos_config = ChaosConfig::default();
        let network = Arc::new(ChaosNetwork::new(3, chaos_config));
        let nemesis_config = NemesisConfig::default();
        let nemesis = Nemesis::new(network.clone(), nemesis_config);

        // Inject kill primary fault
        nemesis.inject(Fault::KillPrimary);

        // Check that primary (node 0 for view 0) is killed
        assert!(network.is_killed(0));

        // Check history
        let history = nemesis.get_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].fault, Fault::KillPrimary);
    }

    #[test]
    fn test_nemesis_heal() {
        let chaos_config = ChaosConfig::default();
        let network = Arc::new(ChaosNetwork::new(3, chaos_config));
        let nemesis_config = NemesisConfig::default();
        let nemesis = Nemesis::new(network.clone(), nemesis_config);

        // Kill a node first
        network.kill_node(1);
        assert!(network.is_killed(1));

        // Heal
        nemesis.inject(Fault::Heal);

        // Node should be revived
        assert!(!network.is_killed(1));
    }
}
