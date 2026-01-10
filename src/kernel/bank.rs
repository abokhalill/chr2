use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::kernel::traits::{
    ApplyContext, ChrApplication, EffectId, Event, Outbox, SideEffect, 
    SnapshotStream,
};

#[derive(Clone, Debug, Default)]
pub struct BankState {
    pub balances: HashMap<String, u64>,
    pub outbox: Outbox,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BankSnapshotData {
    balances: HashMap<String, u64>,
    outbox: Outbox,
}

impl BankState {
    pub fn balance(&self, user: &str) -> u64 { self.balances.get(user).copied().unwrap_or(0) }
    pub fn outbox(&self) -> &Outbox { &self.outbox }
    pub fn outbox_mut(&mut self) -> &mut Outbox { &mut self.outbox }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BankEvent {
    Deposit { user: String, amount: u64 },
    Withdraw { user: String, amount: u64 },
    SendEmail { to: String, subject: String, client_id: u64, sequence_number: u64 },
    SystemAcknowledgeEffect { effect_id: EffectId },
    PoisonPill,
}

#[derive(Clone, Debug)]
pub enum BankError {
    InsufficientFunds { user: String, requested: u64, available: u64 },
    DeserializeError(String),
    SnapshotError(String),
}

impl fmt::Display for BankError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BankError::InsufficientFunds {
                user,
                requested,
                available,
            } => {
                write!(
                    f,
                    "Insufficient funds for {}: requested {}, available {}",
                    user, requested, available
                )
            }
            BankError::DeserializeError(msg) => write!(f, "Deserialize error: {}", msg),
            BankError::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
        }
    }
}

impl std::error::Error for BankError {}

#[derive(Clone, Debug)]
pub enum BankQuery {
    Balance { user: String },
    AllBalances,
}

#[derive(Clone, Debug)]
pub enum BankQueryResponse {
    Balance(u64),
    AllBalances(HashMap<String, u64>),
}

pub struct BankApp;

impl BankApp {
    pub fn new() -> Self {
        BankApp
    }
}

impl Default for BankApp {
    fn default() -> Self {
        Self::new()
    }
}

impl ChrApplication for BankApp {
    type State = BankState;
    type QueryRequest = BankQuery;
    type QueryResponse = BankQueryResponse;
    type Error = BankError;

    fn apply(
        &self,
        state: &Self::State,
        event: Event,
        ctx: &ApplyContext,
    ) -> Result<(Self::State, Vec<SideEffect>), Self::Error> {
        let bank_event: BankEvent = bincode::deserialize(&event.payload)
            .map_err(|e| BankError::DeserializeError(e.to_string()))?;

        match bank_event {
            BankEvent::Deposit { user, amount } => {
                let mut new_state = state.clone();
                let balance = new_state.balances.entry(user.clone()).or_insert(0);
                *balance = balance.saturating_add(amount);
                let side_effects = vec![SideEffect::Log {
                    level: crate::kernel::traits::LogLevel::Info,
                    message: format!("Deposited {} to {}", amount, user),
                }];
                Ok((new_state, side_effects))
            }

            BankEvent::Withdraw { user, amount } => {
                let current_balance = state.balance(&user);
                if current_balance < amount {
                    return Err(BankError::InsufficientFunds {
                        user, requested: amount, available: current_balance,
                    });
                }
                let mut new_state = state.clone();
                let balance = new_state.balances.entry(user.clone()).or_insert(0);
                *balance -= amount;
                let side_effects = vec![SideEffect::Log {
                    level: crate::kernel::traits::LogLevel::Info,
                    message: format!("Withdrew {} from {}", amount, user),
                }];
                Ok((new_state, side_effects))
            }
            
            BankEvent::SendEmail { to, subject, client_id, sequence_number } => {
                let mut new_state = state.clone();
                let effect_id = EffectId::new(client_id, sequence_number, 0);
                let effect = SideEffect::Emit {
                    channel: "email".to_string(),
                    payload: format!("To: {}\nSubject: {}", to, subject).into_bytes(),
                };
                new_state.outbox.add_pending(effect_id, effect.clone(), ctx.event_index());
                Ok((new_state, vec![effect]))
            }
            
            BankEvent::SystemAcknowledgeEffect { effect_id } => {
                let mut new_state = state.clone();
                let _ = new_state.outbox.acknowledge(&effect_id);
                Ok((new_state, vec![]))
            }

            BankEvent::PoisonPill => {
                panic!("POISON PILL: Intentional panic for testing");
            }
        }
    }

    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse {
        match request {
            BankQuery::Balance { user } => BankQueryResponse::Balance(state.balance(&user)),
            BankQuery::AllBalances => BankQueryResponse::AllBalances(state.balances.clone()),
        }
    }

    fn snapshot(&self, state: &Self::State) -> SnapshotStream {
        let snapshot_data = BankSnapshotData {
            balances: state.balances.clone(),
            outbox: state.outbox.clone(),
        };
        let data = bincode::serialize(&snapshot_data).unwrap_or_default();
        SnapshotStream {
            schema_version: 2, // Bumped version for outbox support
            data,
        }
    }

    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error> {
        match stream.schema_version {
            1 => {
                let balances: HashMap<String, u64> = bincode::deserialize(&stream.data)
                    .map_err(|e| BankError::SnapshotError(e.to_string()))?;
                Ok(BankState { balances, outbox: Outbox::new() })
            }
            2 => {
                let snapshot_data: BankSnapshotData = bincode::deserialize(&stream.data)
                    .map_err(|e| BankError::SnapshotError(e.to_string()))?;
                Ok(BankState { balances: snapshot_data.balances, outbox: snapshot_data.outbox })
            }
            _ => Err(BankError::SnapshotError(format!("Unknown schema version: {}", stream.schema_version))),
        }
    }

    fn genesis(&self) -> Self::State {
        BankState::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::traits::BlockTime;

    fn make_ctx(index: u64) -> ApplyContext {
        ApplyContext::new(BlockTime::from_nanos(1_000_000_000), [0u8; 32], index, 1)
    }

    fn make_event(bank_event: BankEvent, index: u64) -> Event {
        use crate::kernel::traits::{EventFlags, EventHeader};

        Event {
            header: EventHeader {
                index,
                view_id: 1,
                stream_id: 0,
                schema_version: 1,
                flags: EventFlags::default(),
            },
            payload: bincode::serialize(&bank_event).unwrap(),
        }
    }

    #[test]
    fn test_deposit() {
        let app = BankApp::new();
        let state = app.genesis();
        let ctx = make_ctx(0);

        let event = make_event(
            BankEvent::Deposit {
                user: "Alice".to_string(),
                amount: 100,
            },
            0,
        );

        let (new_state, _) = app.apply(&state, event, &ctx).unwrap();
        assert_eq!(new_state.balance("Alice"), 100);
    }

    #[test]
    fn test_withdraw_success() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let ctx = make_ctx(0);
        let event = make_event(
            BankEvent::Withdraw {
                user: "Alice".to_string(),
                amount: 50,
            },
            0,
        );

        let (new_state, _) = app.apply(&state, event, &ctx).unwrap();
        assert_eq!(new_state.balance("Alice"), 50);
    }

    #[test]
    fn test_withdraw_insufficient_funds() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let ctx = make_ctx(0);
        let event = make_event(
            BankEvent::Withdraw {
                user: "Alice".to_string(),
                amount: 200,
            },
            0,
        );

        let result = app.apply(&state, event, &ctx);
        assert!(matches!(result, Err(BankError::InsufficientFunds { .. })));
    }

    #[test]
    #[should_panic(expected = "POISON PILL")]
    fn test_poison_pill_panics() {
        let app = BankApp::new();
        let state = app.genesis();
        let ctx = make_ctx(0);

        let event = make_event(BankEvent::PoisonPill, 0);
        let _ = app.apply(&state, event, &ctx);
    }

    #[test]
    fn test_snapshot_restore() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);
        state.balances.insert("Bob".to_string(), 200);

        let snapshot = app.snapshot(&state);
        let restored = app.restore(snapshot).unwrap();

        assert_eq!(restored.balance("Alice"), 100);
        assert_eq!(restored.balance("Bob"), 200);
    }

    #[test]
    fn test_query_balance() {
        let app = BankApp::new();
        let mut state = app.genesis();
        state.balances.insert("Alice".to_string(), 100);

        let response = app.query(&state, BankQuery::Balance { user: "Alice".to_string() });
        assert!(matches!(response, BankQueryResponse::Balance(100)));
    }
}
