use std::panic::{self, AssertUnwindSafe};

use crate::engine::reader::ReadError;
use crate::kernel::traits::{ApplyContext, BlockTime, ChrApplication, Event, EventFlags, EventHeader};

use super::error::{ExecutorStatus, FatalError, StepResult};
use super::Executor;

impl<A: ChrApplication> Executor<A> {
    pub fn step(&mut self) -> Result<StepResult, FatalError> {
        if self.status == ExecutorStatus::Halted {
            return Err(FatalError::Halted);
        }

        let committed = match self.reader.committed_index() {
            Some(idx) => idx,
            None => return Ok(StepResult::Idle),
        };

        if self.next_index > committed {
            return Ok(StepResult::Idle);
        }

        let log_entry = match self.reader.read(self.next_index) {
            Ok(entry) => entry,
            Err(ReadError::IndexNotCommitted { .. }) => return Ok(StepResult::Idle),
            Err(ReadError::TruncatedDuringRead { index }) => {
                return Err(FatalError::ReadError(format!("Entry {} truncated during read", index)));
            }
            Err(e) => return Err(FatalError::ReadError(e.to_string())),
        };

        let current_index = self.next_index;
        let event = Event {
            header: EventHeader {
                index: log_entry.index,
                view_id: log_entry.view_id,
                stream_id: log_entry.stream_id,
                schema_version: log_entry.schema_version,
                flags: EventFlags::from(log_entry.flags),
            },
            payload: log_entry.payload,
        };

        let block_time = BlockTime::from_nanos(log_entry.timestamp_ns);
        let random_seed = self.derive_random_seed_from_prev_hash(&log_entry.prev_hash, current_index);

        let ctx = ApplyContext::new(block_time, random_seed, current_index, log_entry.view_id);

        let apply_result = panic::catch_unwind(AssertUnwindSafe(|| {
            self.app.apply(&self.state, event, &ctx)
        }));

        match apply_result {
            Ok(Ok((new_state, side_effects))) => {
                self.state = new_state;
                self.next_index += 1;
                Ok(StepResult::Applied { index: current_index, side_effects })
            }
            Ok(Err(app_error)) => {
                self.next_index += 1;
                Ok(StepResult::Rejected { index: current_index, error: app_error.to_string() })
            }
            Err(panic_info) => {
                self.status = ExecutorStatus::Halted;

                let message = if let Some(msg) = panic_info.downcast_ref::<&str>() {
                    msg.to_string()
                } else if let Some(msg) = panic_info.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "<unknown panic>".to_string()
                };

                Err(FatalError::PoisonPill { index: current_index, message })
            }
        }
    }

    pub(super) fn derive_random_seed_from_prev_hash(&self, prev_hash: &[u8; 16], index: u64) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(prev_hash);
        hasher.update(&index.to_le_bytes());
        *hasher.finalize().as_bytes()
    }
}
