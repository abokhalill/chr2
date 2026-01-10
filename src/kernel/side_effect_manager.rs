use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::kernel::traits::{EffectId, Outbox, SideEffect};

#[derive(Debug, Clone)]
pub enum EffectExecutionError {
    ExecutionFailed(String),
    Timeout,
    ServiceUnavailable(String),
    Cancelled,
}

impl fmt::Display for EffectExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EffectExecutionError::ExecutionFailed(msg) => write!(f, "Effect execution failed: {}", msg),
            EffectExecutionError::Timeout => write!(f, "Effect execution timed out"),
            EffectExecutionError::ServiceUnavailable(svc) => write!(f, "Service unavailable: {}", svc),
            EffectExecutionError::Cancelled => write!(f, "Effect execution cancelled"),
        }
    }
}

impl std::error::Error for EffectExecutionError {}

#[derive(Debug, Clone)]
pub enum AcknowledgeError {
    SubmissionFailed(String),
    NotPrimary,
    FencingViolation,
}

impl fmt::Display for AcknowledgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AcknowledgeError::SubmissionFailed(msg) => write!(f, "Acknowledge submission failed: {}", msg),
            AcknowledgeError::NotPrimary => write!(f, "Node is no longer Primary"),
            AcknowledgeError::FencingViolation => write!(f, "Fencing violation during acknowledge"),
        }
    }
}

impl std::error::Error for AcknowledgeError {}

pub type EffectExecutor = Box<dyn Fn(&EffectId, &SideEffect) -> Result<(), EffectExecutionError> + Send + Sync>;
pub type AcknowledgeSubmitter = Box<dyn Fn(EffectId) -> Result<(), AcknowledgeError> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct SideEffectManagerConfig {
    pub poll_interval: Duration,
    pub max_effects_per_cycle: usize,
    pub execution_timeout: Duration,
}

impl Default for SideEffectManagerConfig {
    fn default() -> Self {
        SideEffectManagerConfig {
            poll_interval: Duration::from_millis(10),
            max_effects_per_cycle: 100,
            execution_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
pub struct InFlightTracker {
    in_flight: HashSet<EffectId>,
    start_times: std::collections::HashMap<EffectId, Instant>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        InFlightTracker { in_flight: HashSet::new(), start_times: std::collections::HashMap::new() }
    }
    
    pub fn start(&mut self, id: EffectId) -> bool {
        if self.in_flight.contains(&id) {
            return false;
        }
        self.in_flight.insert(id);
        self.start_times.insert(id, Instant::now());
        true
    }
    
    pub fn complete(&mut self, id: &EffectId) {
        self.in_flight.remove(id);
        self.start_times.remove(id);
    }
    
    pub fn is_in_flight(&self, id: &EffectId) -> bool { self.in_flight.contains(id) }
    
    pub fn timed_out(&self, timeout: Duration) -> Vec<EffectId> {
        let now = Instant::now();
        self.start_times
            .iter()
            .filter(|(_, start)| now.duration_since(**start) > timeout)
            .map(|(id, _)| *id)
            .collect()
    }
    
    pub fn clear_timed_out(&mut self, timeout: Duration) {
        let timed_out = self.timed_out(timeout);
        for id in timed_out {
            self.in_flight.remove(&id);
            self.start_times.remove(&id);
        }
    }
}

pub struct SideEffectManager {
    config: SideEffectManagerConfig,
    in_flight: Arc<Mutex<InFlightTracker>>,
    is_primary: Arc<AtomicBool>,
    fence_token: Arc<AtomicU64>,
    lease_token: Arc<AtomicU64>,
    executor: Arc<EffectExecutor>,
    submitter: Arc<AcknowledgeSubmitter>,
    running: Arc<AtomicBool>,
    effects_executed: Arc<std::sync::atomic::AtomicU64>,
    acks_submitted: Arc<std::sync::atomic::AtomicU64>,
}

impl SideEffectManager {
    pub fn new(
        config: SideEffectManagerConfig,
        executor: EffectExecutor,
        submitter: AcknowledgeSubmitter,
    ) -> Self {
        SideEffectManager {
            config,
            in_flight: Arc::new(Mutex::new(InFlightTracker::new())),
            is_primary: Arc::new(AtomicBool::new(false)),
            fence_token: Arc::new(AtomicU64::new(0)),
            lease_token: Arc::new(AtomicU64::new(u64::MAX)),
            executor: Arc::new(executor),
            submitter: Arc::new(submitter),
            running: Arc::new(AtomicBool::new(false)),
            effects_executed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            acks_submitted: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
    
    pub fn advance_fence(&self, token: u64) {
        let mut cur = self.fence_token.load(Ordering::SeqCst);
        while token > cur {
            match self.fence_token.compare_exchange(cur, token, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => {
                    if let Ok(mut tracker) = self.in_flight.lock() {
                        *tracker = InFlightTracker::new();
                    }
                    break;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn set_primary_with_token(&self, is_primary: bool, token: u64) {
        self.advance_fence(token);
        self.is_primary.store(is_primary, Ordering::SeqCst);
        if is_primary {
            self.lease_token.store(token, Ordering::SeqCst);
        } else {
            self.lease_token.store(u64::MAX, Ordering::SeqCst);
        }

        if !is_primary {
            if let Ok(mut tracker) = self.in_flight.lock() {
                *tracker = InFlightTracker::new();
            }
        }
    }

    pub fn set_primary(&self, is_primary: bool) {
        let token = self.fence_token.load(Ordering::SeqCst);
        self.set_primary_with_token(is_primary, token);
    }
    
    pub fn is_primary(&self) -> bool {
        if !self.is_primary.load(Ordering::SeqCst) {
            return false;
        }
        let fence = self.fence_token.load(Ordering::SeqCst);
        let lease = self.lease_token.load(Ordering::SeqCst);
        lease == fence
    }
    
    pub fn process_pending(&self, outbox: &Outbox) -> usize {
        if !self.is_primary() {
            return 0;
        }
 
        let fence_snapshot = self.fence_token.load(Ordering::SeqCst);
        let lease_snapshot = self.lease_token.load(Ordering::SeqCst);
        if lease_snapshot != fence_snapshot {
            return 0;
        }
        
        let pending = outbox.pending_effects();
        let mut processed = 0;
        
        let mut tracker = match self.in_flight.lock() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        tracker.clear_timed_out(self.config.execution_timeout);
        
        for (effect_id, entry) in pending.iter().take(self.config.max_effects_per_cycle) {
            if !self.is_primary.load(Ordering::SeqCst) {
                break;
            }
            if self.fence_token.load(Ordering::SeqCst) != fence_snapshot {
                break;
            }
            if self.lease_token.load(Ordering::SeqCst) != lease_snapshot {
                break;
            }

            if tracker.is_in_flight(effect_id) { continue; }
            if !tracker.start(*effect_id) { continue; }
            drop(tracker);
            
            let result = (self.executor)(effect_id, &entry.effect);
             
            if result.is_ok() {
                self.effects_executed.fetch_add(1, Ordering::Relaxed);
                
                if self.is_primary.load(Ordering::SeqCst)
                    && self.fence_token.load(Ordering::SeqCst) == fence_snapshot
                    && self.lease_token.load(Ordering::SeqCst) == lease_snapshot
                {
                    if (self.submitter)(*effect_id).is_ok() {
                        self.acks_submitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            tracker = match self.in_flight.lock() {
                Ok(t) => t,
                Err(_) => return processed,
            };
            tracker.complete(effect_id);
            
            processed += 1;
        }
        
        processed
    }
    
    pub fn effects_executed(&self) -> u64 { self.effects_executed.load(Ordering::Relaxed) }
    pub fn acks_submitted(&self) -> u64 { self.acks_submitted.load(Ordering::Relaxed) }
    pub fn is_running(&self) -> bool { self.running.load(Ordering::SeqCst) }
    pub fn stop(&self) { self.running.store(false, Ordering::SeqCst); }
}

#[derive(Debug, Default)]
pub struct MockEffectExecutor {
    pub executed: Arc<Mutex<Vec<(EffectId, SideEffect)>>>,
    pub should_succeed: Arc<AtomicBool>,
}

impl MockEffectExecutor {
    pub fn new() -> Self {
        MockEffectExecutor {
            executed: Arc::new(Mutex::new(Vec::new())),
            should_succeed: Arc::new(AtomicBool::new(true)),
        }
    }
    
    pub fn as_executor(&self) -> EffectExecutor {
        let executed = self.executed.clone();
        let should_succeed = self.should_succeed.clone();
        
        Box::new(move |id: &EffectId, effect: &SideEffect| {
            if let Ok(mut list) = executed.lock() {
                list.push((*id, effect.clone()));
            }
            if should_succeed.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(EffectExecutionError::ExecutionFailed("Mock failure".to_string()))
            }
        })
    }
    
    pub fn get_executed(&self) -> Vec<(EffectId, SideEffect)> {
        self.executed.lock().map(|l| l.clone()).unwrap_or_default()
    }
    
    pub fn set_should_succeed(&self, succeed: bool) {
        self.should_succeed.store(succeed, Ordering::Relaxed);
    }
    
    pub fn clear(&self) {
        if let Ok(mut list) = self.executed.lock() {
            list.clear();
        }
    }
}

#[derive(Debug, Default)]
pub struct MockAcknowledgeSubmitter {
    pub submitted: Arc<Mutex<Vec<EffectId>>>,
    pub should_succeed: Arc<AtomicBool>,
}

impl MockAcknowledgeSubmitter {
    pub fn new() -> Self {
        MockAcknowledgeSubmitter {
            submitted: Arc::new(Mutex::new(Vec::new())),
            should_succeed: Arc::new(AtomicBool::new(true)),
        }
    }
    
    pub fn as_submitter(&self) -> AcknowledgeSubmitter {
        let submitted = self.submitted.clone();
        let should_succeed = self.should_succeed.clone();
        
        Box::new(move |id: EffectId| {
            if let Ok(mut list) = submitted.lock() {
                list.push(id);
            }
            if should_succeed.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(AcknowledgeError::SubmissionFailed("Mock failure".to_string()))
            }
        })
    }
    
    pub fn get_submitted(&self) -> Vec<EffectId> {
        self.submitted.lock().map(|l| l.clone()).unwrap_or_default()
    }
    
    pub fn set_should_succeed(&self, succeed: bool) {
        self.should_succeed.store(succeed, Ordering::Relaxed);
    }
    
    pub fn clear(&self) {
        if let Ok(mut list) = self.submitted.lock() {
            list.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::traits::SideEffectStatus;
    
    #[test]
    fn test_in_flight_tracker() {
        let mut tracker = InFlightTracker::new();
        let id = EffectId::new(1, 1, 0);
        
        // Start tracking
        assert!(tracker.start(id));
        assert!(tracker.is_in_flight(&id));
        
        // Can't start again
        assert!(!tracker.start(id));
        
        // Complete
        tracker.complete(&id);
        assert!(!tracker.is_in_flight(&id));
        
        // Can start again after completion
        assert!(tracker.start(id));
    }
    
    #[test]
    fn test_side_effect_manager_primary_fencing() {
        let mock_executor = MockEffectExecutor::new();
        let mock_submitter = MockAcknowledgeSubmitter::new();
        
        let manager = SideEffectManager::new(
            SideEffectManagerConfig::default(),
            mock_executor.as_executor(),
            mock_submitter.as_submitter(),
        );
        
        // Create outbox with pending effect
        let mut outbox = Outbox::new();
        let effect_id = EffectId::new(1, 1, 0);
        let effect = SideEffect::Emit {
            channel: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        outbox.add_pending(effect_id, effect, 0);
        
        // Not primary - should not execute
        manager.set_primary(false);
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 0);
        assert_eq!(mock_executor.get_executed().len(), 0);
        
        // Become primary - should execute
        manager.set_primary(true);
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 1);
        assert_eq!(mock_executor.get_executed().len(), 1);
        assert_eq!(mock_submitter.get_submitted().len(), 1);
    }
    
    #[test]
    fn test_side_effect_manager_skips_acknowledged() {
        let mock_executor = MockEffectExecutor::new();
        let mock_submitter = MockAcknowledgeSubmitter::new();
        
        let manager = SideEffectManager::new(
            SideEffectManagerConfig::default(),
            mock_executor.as_executor(),
            mock_submitter.as_submitter(),
        );
        manager.set_primary(true);
        
        // Create outbox with acknowledged effect
        let mut outbox = Outbox::new();
        let effect_id = EffectId::new(1, 1, 0);
        let effect = SideEffect::Emit {
            channel: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        outbox.add_pending(effect_id, effect, 0);
        outbox.acknowledge(&effect_id);
        
        // Should not execute acknowledged effects
        let processed = manager.process_pending(&outbox);
        assert_eq!(processed, 0);
        assert_eq!(mock_executor.get_executed().len(), 0);
    }

     #[test]
     fn test_side_effect_manager_fencing_token_blocks_old_primary() {
         let mock_executor = MockEffectExecutor::new();
         let mock_submitter = MockAcknowledgeSubmitter::new();
 
         let manager = SideEffectManager::new(
             SideEffectManagerConfig::default(),
             mock_executor.as_executor(),
             mock_submitter.as_submitter(),
         );

         let mut outbox = Outbox::new();
         let effect_id_1 = EffectId::new(1, 1, 0);
         let effect_1 = SideEffect::Emit {
             channel: "test".to_string(),
             payload: vec![1],
         };
         outbox.add_pending(effect_id_1, effect_1, 0);

         manager.set_primary_with_token(true, 0);
         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 1);
         assert_eq!(mock_executor.get_executed().len(), 1);

         outbox.acknowledge(&effect_id_1);

         let effect_id_2 = EffectId::new(2, 1, 0);
         let effect_2 = SideEffect::Emit {
             channel: "test".to_string(),
             payload: vec![2],
         };
         outbox.add_pending(effect_id_2, effect_2, 0);

         manager.advance_fence(1);

         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 0);
         assert_eq!(mock_executor.get_executed().len(), 1);

         manager.set_primary_with_token(true, 1);
         let processed = manager.process_pending(&outbox);
         assert_eq!(processed, 1);
         assert_eq!(mock_executor.get_executed().len(), 2);
     }
}
