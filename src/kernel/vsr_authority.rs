//! Durable fencing for side effect execution.
//! Ensures side effects execute only under legitimate Primary authority.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub trait VsrAuthority: Send + Sync {
    fn durable_view(&self) -> u64;
    fn is_primary_in_view(&self, view: u64) -> bool;

    fn can_execute_effect(&self, queued_view: u64) -> bool {
        self.is_primary_in_view(queued_view)
    }
}

pub struct SimpleVsrAuthority {
    current_view: AtomicU64,
    is_primary: AtomicBool,
}

impl SimpleVsrAuthority {
    pub fn new() -> Self {
        SimpleVsrAuthority {
            current_view: AtomicU64::new(0),
            is_primary: AtomicBool::new(false),
        }
    }

    pub fn with_state(view: u64, is_primary: bool) -> Self {
        SimpleVsrAuthority {
            current_view: AtomicU64::new(view),
            is_primary: AtomicBool::new(is_primary),
        }
    }

    pub fn advance_view(&self, new_view: u64) {
        let mut current = self.current_view.load(Ordering::SeqCst);
        while new_view > current {
            match self.current_view.compare_exchange_weak(
                current,
                new_view,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.is_primary.store(false, Ordering::SeqCst);
                    break;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn set_primary(&self, is_primary: bool) {
        self.is_primary.store(is_primary, Ordering::SeqCst);
    }

    pub fn become_primary_in_view(&self, view: u64) {
        self.advance_view(view);
        self.current_view.store(view, Ordering::SeqCst);
        self.is_primary.store(true, Ordering::SeqCst);
    }

    pub fn step_down(&self) {
        self.is_primary.store(false, Ordering::SeqCst);
    }
}

impl Default for SimpleVsrAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl VsrAuthority for SimpleVsrAuthority {
    fn durable_view(&self) -> u64 {
        self.current_view.load(Ordering::SeqCst)
    }

    fn is_primary_in_view(&self, view: u64) -> bool {
        let current = self.current_view.load(Ordering::SeqCst);
        let is_primary = self.is_primary.load(Ordering::SeqCst);
        is_primary && view == current
    }
}

impl VsrAuthority for Arc<SimpleVsrAuthority> {
    fn durable_view(&self) -> u64 {
        (**self).durable_view()
    }

    fn is_primary_in_view(&self, view: u64) -> bool {
        (**self).is_primary_in_view(view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_authority_initial_state() {
        let auth = SimpleVsrAuthority::new();
        assert_eq!(auth.durable_view(), 0);
        assert!(!auth.is_primary_in_view(0));
    }

    #[test]
    fn test_simple_authority_become_primary() {
        let auth = SimpleVsrAuthority::new();
        auth.become_primary_in_view(1);

        assert_eq!(auth.durable_view(), 1);
        assert!(auth.is_primary_in_view(1));
        assert!(!auth.is_primary_in_view(0)); // Old view
        assert!(!auth.is_primary_in_view(2)); // Future view
    }

    #[test]
    fn test_simple_authority_view_advance_clears_primary() {
        let auth = SimpleVsrAuthority::new();
        auth.become_primary_in_view(1);
        assert!(auth.is_primary_in_view(1));

        // Advance view should clear Primary status
        auth.advance_view(2);
        assert_eq!(auth.durable_view(), 2);
        assert!(!auth.is_primary_in_view(2));
    }

    #[test]
    fn test_simple_authority_step_down() {
        let auth = SimpleVsrAuthority::new();
        auth.become_primary_in_view(1);
        assert!(auth.is_primary_in_view(1));

        auth.step_down();
        assert!(!auth.is_primary_in_view(1));
        assert_eq!(auth.durable_view(), 1); // View unchanged
    }

    #[test]
    fn test_simple_authority_cannot_go_backwards() {
        let auth = SimpleVsrAuthority::new();
        auth.advance_view(5);
        assert_eq!(auth.durable_view(), 5);

        auth.advance_view(3); // Should be ignored
        assert_eq!(auth.durable_view(), 5);
    }

    #[test]
    fn test_can_execute_effect() {
        let auth = SimpleVsrAuthority::new();
        auth.become_primary_in_view(1);

        // Can execute effects queued in current view
        assert!(auth.can_execute_effect(1));

        // Cannot execute effects queued in old view
        assert!(!auth.can_execute_effect(0));

        // Cannot execute effects queued in future view
        assert!(!auth.can_execute_effect(2));

        // After stepping down, cannot execute
        auth.step_down();
        assert!(!auth.can_execute_effect(1));
    }
}
