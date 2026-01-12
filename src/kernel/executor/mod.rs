mod error;
mod recovery;
mod snapshot_ops;
mod step;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use crate::engine::reader::LogReader;
use crate::kernel::traits::ChrApplication;

pub use error::{ExecutorStatus, FatalError, StepResult};

pub(crate) const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 1000;

pub struct Executor<A: ChrApplication> {
    pub(crate) reader: LogReader,
    pub(crate) app: A,
    pub(crate) state: A::State,
    pub(crate) next_index: u64,
    pub(crate) status: ExecutorStatus,
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) last_snapshot_index: Option<u64>,
    pub(crate) snapshot_threshold: u64,
}

impl<A: ChrApplication> Executor<A> {
    pub fn new(reader: LogReader, app: A, start_index: u64) -> Self {
        Self::with_snapshot_dir(reader, app, start_index, PathBuf::from("snapshots"))
    }

    pub fn with_snapshot_dir(
        reader: LogReader,
        app: A,
        start_index: u64,
        snapshot_dir: PathBuf,
    ) -> Self {
        let state = app.genesis();
        Executor {
            reader,
            app,
            state,
            next_index: start_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index: None,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        }
    }

    pub fn with_state(
        reader: LogReader,
        app: A,
        state: A::State,
        start_index: u64,
        snapshot_dir: PathBuf,
        last_snapshot_index: Option<u64>,
    ) -> Self {
        Executor {
            reader,
            app,
            state,
            next_index: start_index,
            status: ExecutorStatus::Running,
            snapshot_dir,
            last_snapshot_index,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
        }
    }

    pub fn state(&self) -> &A::State {
        &self.state
    }
    pub fn next_index(&self) -> u64 {
        self.next_index
    }
    pub fn status(&self) -> ExecutorStatus {
        self.status
    }
    pub fn is_halted(&self) -> bool {
        self.status == ExecutorStatus::Halted
    }
    pub fn query(&self, request: A::QueryRequest) -> A::QueryResponse {
        self.app.query(&self.state, request)
    }
}
