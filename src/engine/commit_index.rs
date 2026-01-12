use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

const NO_COMMIT_SENTINEL: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommitIndex {
    None,
    At(u64),
}

impl CommitIndex {
    #[inline]
    pub fn from_option(opt: Option<u64>) -> Self {
        match opt {
            Some(idx) => CommitIndex::At(idx),
            None => CommitIndex::None,
        }
    }

    #[inline]
    pub fn to_option(self) -> Option<u64> {
        match self {
            CommitIndex::None => None,
            CommitIndex::At(idx) => Some(idx),
        }
    }

    #[inline]
    pub fn to_raw(self) -> u64 {
        match self {
            CommitIndex::None => NO_COMMIT_SENTINEL,
            CommitIndex::At(idx) => idx,
        }
    }

    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        if raw == NO_COMMIT_SENTINEL {
            CommitIndex::None
        } else {
            CommitIndex::At(raw)
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        matches!(self, CommitIndex::At(_))
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, CommitIndex::None)
    }

    #[inline]
    pub fn unwrap(self) -> u64 {
        match self {
            CommitIndex::At(idx) => idx,
            CommitIndex::None => panic!("Called unwrap on CommitIndex::None"),
        }
    }

    #[inline]
    pub fn unwrap_or(self, default: u64) -> u64 {
        match self {
            CommitIndex::At(idx) => idx,
            CommitIndex::None => default,
        }
    }

    #[inline]
    pub fn includes(&self, log_index: u64) -> bool {
        match self {
            CommitIndex::None => false,
            CommitIndex::At(committed) => log_index <= *committed,
        }
    }

    #[inline]
    pub fn advance(&mut self, new_index: u64) -> bool {
        match self {
            CommitIndex::None => {
                *self = CommitIndex::At(new_index);
                true
            }
            CommitIndex::At(current) => {
                if new_index > *current {
                    *current = new_index;
                    true
                } else {
                    false
                }
            }
        }
    }
}

impl Default for CommitIndex {
    fn default() -> Self {
        CommitIndex::None
    }
}

impl PartialOrd for CommitIndex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CommitIndex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (CommitIndex::None, CommitIndex::None) => std::cmp::Ordering::Equal,
            (CommitIndex::None, CommitIndex::At(_)) => std::cmp::Ordering::Less,
            (CommitIndex::At(_), CommitIndex::None) => std::cmp::Ordering::Greater,
            (CommitIndex::At(a), CommitIndex::At(b)) => a.cmp(b),
        }
    }
}

impl From<Option<u64>> for CommitIndex {
    fn from(opt: Option<u64>) -> Self {
        CommitIndex::from_option(opt)
    }
}

impl From<CommitIndex> for Option<u64> {
    fn from(ci: CommitIndex) -> Self {
        ci.to_option()
    }
}

pub struct AtomicCommitIndex {
    inner: AtomicU64,
}

impl AtomicCommitIndex {
    pub const fn new() -> Self {
        AtomicCommitIndex {
            inner: AtomicU64::new(NO_COMMIT_SENTINEL),
        }
    }

    pub fn from_commit_index(ci: CommitIndex) -> Self {
        AtomicCommitIndex {
            inner: AtomicU64::new(ci.to_raw()),
        }
    }

    #[inline]
    pub fn load(&self) -> CommitIndex {
        CommitIndex::from_raw(self.inner.load(Ordering::Acquire))
    }

    #[inline]
    pub fn store(&self, ci: CommitIndex) {
        self.inner.store(ci.to_raw(), Ordering::Release);
    }

    pub fn advance_if_greater(&self, new_index: u64) -> CommitIndex {
        loop {
            let current_raw = self.inner.load(Ordering::Acquire);
            let current = CommitIndex::from_raw(current_raw);
            
            let should_update = match current {
                CommitIndex::None => true,
                CommitIndex::At(idx) => new_index > idx,
            };
            
            if !should_update {
                return current;
            }
            
            match self.inner.compare_exchange_weak(
                current_raw,
                new_index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return current,
                Err(_) => continue,
            }
        }
    }
}

impl Default for AtomicCommitIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_index_none() {
        let ci = CommitIndex::None;
        assert!(ci.is_none());
        assert!(!ci.is_some());
        assert_eq!(ci.to_option(), None);
        assert_eq!(ci.to_raw(), u64::MAX);
    }

    #[test]
    fn test_commit_index_at_zero() {
        let ci = CommitIndex::At(0);
        assert!(ci.is_some());
        assert!(!ci.is_none());
        assert_eq!(ci.to_option(), Some(0));
        assert_eq!(ci.unwrap(), 0);
    }

    #[test]
    fn test_commit_index_ordering() {
        assert!(CommitIndex::None < CommitIndex::At(0));
        assert!(CommitIndex::At(0) < CommitIndex::At(1));
        assert!(CommitIndex::At(1) < CommitIndex::At(100));
    }

    #[test]
    fn test_commit_index_includes() {
        let ci = CommitIndex::At(5);
        assert!(ci.includes(0));
        assert!(ci.includes(5));
        assert!(!ci.includes(6));
        
        let none = CommitIndex::None;
        assert!(!none.includes(0));
    }

    #[test]
    fn test_commit_index_advance() {
        let mut ci = CommitIndex::None;
        assert!(ci.advance(0));
        assert_eq!(ci, CommitIndex::At(0));
        
        assert!(ci.advance(5));
        assert_eq!(ci, CommitIndex::At(5));
        
        assert!(!ci.advance(3)); // Can't go backwards
        assert_eq!(ci, CommitIndex::At(5));
    }

    #[test]
    fn test_atomic_commit_index() {
        let aci = AtomicCommitIndex::new();
        assert_eq!(aci.load(), CommitIndex::None);
        
        aci.store(CommitIndex::At(10));
        assert_eq!(aci.load(), CommitIndex::At(10));
        
        let prev = aci.advance_if_greater(15);
        assert_eq!(prev, CommitIndex::At(10));
        assert_eq!(aci.load(), CommitIndex::At(15));
        
        let prev = aci.advance_if_greater(5); // Should not advance
        assert_eq!(prev, CommitIndex::At(15));
        assert_eq!(aci.load(), CommitIndex::At(15));
    }

    #[test]
    fn test_from_option_conversions() {
        assert_eq!(CommitIndex::from(None), CommitIndex::None);
        assert_eq!(CommitIndex::from(Some(42)), CommitIndex::At(42));
        
        let ci: Option<u64> = CommitIndex::At(42).into();
        assert_eq!(ci, Some(42));
        
        let ci: Option<u64> = CommitIndex::None.into();
        assert_eq!(ci, None);
    }
}
