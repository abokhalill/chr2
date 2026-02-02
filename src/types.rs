use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct LogIndex(pub u64);

impl LogIndex {
    #[inline]
    pub const fn new(index: u64) -> Self {
        LogIndex(index)
    }
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
    #[inline]
    pub const fn next(self) -> Self {
        LogIndex(self.0 + 1)
    }
    #[inline]
    pub const fn saturating_sub(self, rhs: u64) -> Self {
        LogIndex(self.0.saturating_sub(rhs))
    }
    #[inline]
    pub const fn is_before(self, other: Self) -> bool {
        self.0 < other.0
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogIndex({})", self.0)
    }
}

impl From<u64> for LogIndex {
    #[inline]
    fn from(v: u64) -> Self {
        LogIndex(v)
    }
}

impl From<LogIndex> for u64 {
    #[inline]
    fn from(idx: LogIndex) -> Self {
        idx.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ViewId(pub u64);

impl ViewId {
    #[inline]
    pub const fn new(view: u64) -> Self {
        ViewId(view)
    }
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
    #[inline]
    pub const fn next(self) -> Self {
        ViewId(self.0 + 1)
    }
    #[inline]
    pub const fn is_stale(self, fence: Self) -> bool {
        self.0 < fence.0
    }
}

impl fmt::Display for ViewId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ViewId({})", self.0)
    }
}

impl From<u64> for ViewId {
    #[inline]
    fn from(v: u64) -> Self {
        ViewId(v)
    }
}

impl From<ViewId> for u64 {
    #[inline]
    fn from(view: ViewId) -> Self {
        view.0
    }
}

/// Empty != At(0). Fixes "None is not 0" bugs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitState {
    #[default]
    Empty,
    At(LogIndex),
}

impl CommitState {
    #[inline]
    pub const fn empty() -> Self {
        CommitState::Empty
    }
    #[inline]
    pub const fn at(index: LogIndex) -> Self {
        CommitState::At(index)
    }
    #[inline]
    pub const fn index(&self) -> Option<LogIndex> {
        match self {
            CommitState::Empty => None,
            CommitState::At(idx) => Some(*idx),
        }
    }
    #[inline]
    pub const fn has_commits(&self) -> bool {
        matches!(self, CommitState::At(_))
    }
    #[inline]
    pub const fn is_committed(&self, index: LogIndex) -> bool {
        match self {
            CommitState::Empty => false,
            CommitState::At(committed) => index.0 <= committed.0,
        }
    }

    pub fn try_advance(self, new_index: LogIndex) -> Result<Self, CommitAdvanceError> {
        match self {
            CommitState::Empty => Ok(CommitState::At(new_index)),
            CommitState::At(current) => {
                if new_index.0 > current.0 {
                    Ok(CommitState::At(new_index))
                } else if new_index.0 == current.0 {
                    Ok(self)
                } else {
                    Err(CommitAdvanceError::Regression {
                        current: current.0,
                        attempted: new_index.0,
                    })
                }
            }
        }
    }

    #[inline]
    pub fn merge(self, remote: Self) -> (Self, bool) {
        match (self, remote) {
            (CommitState::Empty, CommitState::Empty) => (CommitState::Empty, false),
            (CommitState::Empty, CommitState::At(idx)) => (CommitState::At(idx), true),
            (CommitState::At(idx), CommitState::Empty) => (CommitState::At(idx), false),
            (CommitState::At(local), CommitState::At(remote_idx)) => {
                if remote_idx.0 > local.0 {
                    (CommitState::At(remote_idx), true)
                } else {
                    (CommitState::At(local), false)
                }
            }
        }
    }

    #[inline]
    pub const fn to_wire(&self) -> Option<u64> {
        match self {
            CommitState::Empty => None,
            CommitState::At(idx) => Some(idx.0),
        }
    }
    #[inline]
    pub const fn from_wire(wire: Option<u64>) -> Self {
        match wire {
            None => CommitState::Empty,
            Some(n) => CommitState::At(LogIndex(n)),
        }
    }
}

impl fmt::Display for CommitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitState::Empty => write!(f, "Empty"),
            CommitState::At(idx) => write!(f, "At({})", idx.0),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitAdvanceError {
    Regression { current: u64, attempted: u64 },
}

impl fmt::Display for CommitAdvanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitAdvanceError::Regression { current, attempted } => {
                write!(f, "Commit regression: {}→{}", current, attempted)
            }
        }
    }
}

impl std::error::Error for CommitAdvanceError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct DurableEpoch(pub u64);

impl DurableEpoch {
    #[inline]
    pub const fn from_view(view: ViewId) -> Self {
        DurableEpoch(view.0)
    }
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
    #[inline]
    pub const fn is_stale(self, current: Self) -> bool {
        self.0 < current.0
    }
    #[cfg(test)]
    pub const fn invalid() -> Self {
        DurableEpoch(u64::MAX)
    }
}

impl fmt::Display for DurableEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

impl From<ViewId> for DurableEpoch {
    #[inline]
    fn from(view: ViewId) -> Self {
        DurableEpoch::from_view(view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_state_empty_is_not_zero() {
        let empty = CommitState::Empty;
        let at_zero = CommitState::At(LogIndex(0));

        // CRITICAL: Empty and At(0) are different states
        assert_ne!(empty, at_zero);

        // Empty has no index
        assert_eq!(empty.index(), None);
        assert!(!empty.has_commits());
        assert!(!empty.is_committed(LogIndex(0)));

        // At(0) has index 0 committed
        assert_eq!(at_zero.index(), Some(LogIndex(0)));
        assert!(at_zero.has_commits());
        assert!(at_zero.is_committed(LogIndex(0)));
    }

    #[test]
    fn test_commit_state_monotonic_advance() {
        let state = CommitState::Empty;

        // Empty -> At(0) is valid
        let state = state.try_advance(LogIndex(0)).unwrap();
        assert_eq!(state, CommitState::At(LogIndex(0)));

        // At(0) -> At(5) is valid
        let state = state.try_advance(LogIndex(5)).unwrap();
        assert_eq!(state, CommitState::At(LogIndex(5)));

        // At(5) -> At(3) is INVALID (regression)
        let result = state.try_advance(LogIndex(3));
        assert!(matches!(result, Err(CommitAdvanceError::Regression { .. })));

        // At(5) -> At(5) is idempotent
        let state = state.try_advance(LogIndex(5)).unwrap();
        assert_eq!(state, CommitState::At(LogIndex(5)));
    }

    #[test]
    fn test_commit_state_wire_format() {
        // Empty -> None
        assert_eq!(CommitState::Empty.to_wire(), None);
        assert_eq!(CommitState::from_wire(None), CommitState::Empty);

        // At(42) -> Some(42)
        assert_eq!(CommitState::At(LogIndex(42)).to_wire(), Some(42));
        assert_eq!(
            CommitState::from_wire(Some(42)),
            CommitState::At(LogIndex(42))
        );
    }

    #[test]
    fn test_commit_state_merge() {
        // Empty merge Empty = Empty
        let (merged, advanced) = CommitState::Empty.merge(CommitState::Empty);
        assert_eq!(merged, CommitState::Empty);
        assert!(!advanced);

        // Empty merge At(5) = At(5), advanced
        let (merged, advanced) = CommitState::Empty.merge(CommitState::At(LogIndex(5)));
        assert_eq!(merged, CommitState::At(LogIndex(5)));
        assert!(advanced);

        // At(5) merge Empty = At(5), not advanced
        let (merged, advanced) = CommitState::At(LogIndex(5)).merge(CommitState::Empty);
        assert_eq!(merged, CommitState::At(LogIndex(5)));
        assert!(!advanced);

        // At(5) merge At(3) = At(5), not advanced (local wins)
        let (merged, advanced) = CommitState::At(LogIndex(5)).merge(CommitState::At(LogIndex(3)));
        assert_eq!(merged, CommitState::At(LogIndex(5)));
        assert!(!advanced);

        // At(3) merge At(5) = At(5), advanced (remote wins)
        let (merged, advanced) = CommitState::At(LogIndex(3)).merge(CommitState::At(LogIndex(5)));
        assert_eq!(merged, CommitState::At(LogIndex(5)));
        assert!(advanced);
    }

    #[test]
    fn test_durable_epoch_stale_check() {
        let epoch_5 = DurableEpoch(5);
        let epoch_10 = DurableEpoch(10);

        assert!(epoch_5.is_stale(epoch_10));
        assert!(!epoch_10.is_stale(epoch_5));
        assert!(!epoch_5.is_stale(epoch_5)); // Equal is not stale
    }

    #[test]
    fn test_view_id_stale_check() {
        let view_5 = ViewId(5);
        let fence_10 = ViewId(10);

        assert!(view_5.is_stale(fence_10));
        assert!(!fence_10.is_stale(view_5));
    }
}
