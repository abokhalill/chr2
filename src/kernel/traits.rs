use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockTime(pub u64);

impl BlockTime {
    pub fn from_nanos(nanos: u64) -> Self {
        BlockTime(nanos)
    }
    pub fn as_nanos(&self) -> u64 {
        self.0
    }
    pub fn as_millis(&self) -> u64 {
        self.0 / 1_000_000
    }
    pub fn as_secs(&self) -> u64 {
        self.0 / 1_000_000_000
    }
}

pub struct ApplyContext {
    block_time: BlockTime,
    random_seed: [u8; 32],
    event_index: u64,
    view_id: u64,
}

impl ApplyContext {
    pub fn new(
        block_time: BlockTime,
        random_seed: [u8; 32],
        event_index: u64,
        view_id: u64,
    ) -> Self {
        ApplyContext {
            block_time,
            random_seed,
            event_index,
            view_id,
        }
    }

    #[inline]
    pub fn block_time(&self) -> BlockTime {
        self.block_time
    }
    #[inline]
    pub fn random_seed(&self) -> [u8; 32] {
        self.random_seed
    }
    #[inline]
    pub fn event_index(&self) -> u64 {
        self.event_index
    }
    #[inline]
    pub fn view_id(&self) -> u64 {
        self.view_id
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct EventFlags {
    pub config_change: bool,
    pub tombstone: bool,
    pub checkpoint: bool,
}

impl From<u16> for EventFlags {
    fn from(bits: u16) -> Self {
        EventFlags {
            config_change: bits & 0x01 != 0,
            tombstone: bits & 0x02 != 0,
            checkpoint: bits & 0x04 != 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EventHeader {
    pub index: u64,
    pub view_id: u64,
    pub stream_id: u64,
    pub schema_version: u16,
    pub flags: EventFlags,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub header: EventHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct ScopedState<'step, S> {
    state: &'step S,
    _marker: std::marker::PhantomData<&'step ()>,
}

impl<'step, S> ScopedState<'step, S> {
    pub fn new(state: &'step S) -> Self {
        ScopedState {
            state,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn get(&self) -> &'step S {
        self.state
    }
}

#[derive(Debug)]
pub struct ScopedEvent<'step> {
    pub header: EventHeader,
    pub payload: &'step [u8],
}

impl<'step> ScopedEvent<'step> {
    pub fn from_event(event: &'step Event) -> Self {
        ScopedEvent {
            header: event.header,
            payload: &event.payload,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectId(pub [u8; 16]);

impl EffectId {
    pub fn new(client_id: u64, sequence_number: u64, sub_index: u32) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&client_id.to_le_bytes());
        hasher.update(&sequence_number.to_le_bytes());
        hasher.update(&sub_index.to_le_bytes());
        let hash = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&hash.as_bytes()[..16]);
        EffectId(id)
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        EffectId(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Display for EffectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0[..8] {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffectStatus {
    Pending,
    Acknowledged,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub effect: SideEffect,
    pub status: SideEffectStatus,
    pub created_at_index: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Outbox {
    entries: HashMap<EffectId, OutboxEntry>,
}

impl Outbox {
    pub fn new() -> Self {
        Outbox {
            entries: HashMap::new(),
        }
    }

    pub fn add_pending(&mut self, id: EffectId, effect: SideEffect, created_at_index: u64) {
        self.entries.insert(
            id,
            OutboxEntry {
                effect,
                status: SideEffectStatus::Pending,
                created_at_index,
            },
        );
    }

    pub fn acknowledge(&mut self, id: &EffectId) -> bool {
        if let Some(entry) = self.entries.get_mut(id) {
            if entry.status == SideEffectStatus::Pending {
                entry.status = SideEffectStatus::Acknowledged;
                return true;
            }
        }
        false
    }

    pub fn pending_effects(&self) -> Vec<(EffectId, &OutboxEntry)> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.status == SideEffectStatus::Pending)
            .map(|(id, entry)| (*id, entry))
            .collect()
    }

    pub fn get(&self, id: &EffectId) -> Option<&OutboxEntry> {
        self.entries.get(id)
    }
    pub fn contains(&self, id: &EffectId) -> bool {
        self.entries.contains_key(id)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn pending_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.status == SideEffectStatus::Pending)
            .count()
    }

    pub fn compact(&mut self, before_index: u64) {
        self.entries.retain(|_, entry| {
            entry.status == SideEffectStatus::Pending || entry.created_at_index >= before_index
        });
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SystemEvent {
    AcknowledgeEffect { effect_id: EffectId },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SideEffect {
    Emit { channel: String, payload: Vec<u8> },
    Schedule { delay_ms: u64, payload: Vec<u8> },
    Fetch { request_id: u64, uri: String },
    Log { level: LogLevel, message: String },
}

#[derive(Clone, Debug)]
pub struct SnapshotStream {
    pub schema_version: u32,
    pub data: Vec<u8>,
}

pub trait ChrApplication: Send + Sync + 'static {
    type State: Clone + Send + Sync;
    type QueryRequest: Send;
    type QueryResponse: Send;
    type Error: Error + Send + Sync;

    fn apply(
        &self,
        state: &Self::State,
        event: Event,
        ctx: &ApplyContext,
    ) -> Result<(Self::State, Vec<SideEffect>), Self::Error>;

    fn query(&self, state: &Self::State, request: Self::QueryRequest) -> Self::QueryResponse;

    fn snapshot(&self, state: &Self::State) -> SnapshotStream;
    fn restore(&self, stream: SnapshotStream) -> Result<Self::State, Self::Error>;
    fn genesis(&self) -> Self::State;
}
