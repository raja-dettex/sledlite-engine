use std::sync::{Arc, Mutex};

use raft::{Result as RaftResult, eraftpb::{ConfState, Entry, HardState, Snapshot}, storage::{RaftState, Storage}};


#[derive(Clone)]
pub struct RaftLogStore { 
    inner: Arc<Mutex<Inner>>
}

pub struct Inner { 
    hard_state: HardState,
    entries: Vec<Entry>
}

impl RaftLogStore { 
    pub fn new() -> Self { 
        let mut entries = Vec::new();
        let mut dummy = Entry::default();
        dummy.set_index(0);
        dummy.set_term(0);
        entries.push(dummy);
        Self { 
            inner: Arc::new(Mutex::new(Inner { 
                hard_state: HardState::default(),
                entries
            }))
        } 
    }

    pub fn append(&self, entries: &[Entry]) { 
        let mut inner = self.inner.lock().unwrap();
        inner.entries.extend_from_slice(entries);
    }

    pub fn set_hard_state(&self, hard_state : HardState) { 
        let mut inner = self.inner.lock().unwrap();
        inner.hard_state = hard_state;
    }
}

impl Storage for RaftLogStore {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let inner =  self.inner.lock().unwrap();
        Ok(RaftState { 
            hard_state: inner.hard_state.clone(),
            conf_state: ConfState::default()
         })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .entries
            .iter()
            .filter(|e| e.index >= low && e.index < high)
            .cloned()
            .collect()
        )
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let inner = self.inner.lock().unwrap();
        inner.entries.iter().find(|e| e.index == idx).map(|e| e.term)
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.first().unwrap().index + 1)
    }

    fn last_index(&self) -> RaftResult<u64> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.last().map(|e| e.index).unwrap_or(0))
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> RaftResult<raft::prelude::Snapshot> {
        Ok(Snapshot::default())
    }
}