use std::vec;

use raft::prelude::Message;
use raft::storage::MemStorage;
use raft::{Config, RawNode};

use crate::{command::Command};
use slog::{Drain, Logger};
use slog_async;
use slog_term;

fn create_logger() -> Logger {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Logger::root(drain, slog::o!())
}
pub struct SplitEvent { 
    pub old_region_id: u64, 
    pub split_key: Vec<u8>,
    pub new_region_id: u64
}

pub struct RegionMeta { 
    pub id: u64,
    pub start_key: Vec<u8>, // inclusive,
    pub end_key: Vec<u8>
}

impl RegionMeta { 
    pub fn contains(&self, key: Vec<u8>) -> bool { 
        (self.start_key.is_empty() || key >= self.start_key) 
            && (self.end_key.is_empty() || key < self.end_key)
    }
}
pub struct Region { 
    pub meta: RegionMeta,
    pub raft: RawNode<MemStorage>,
    pub storage: MemStorage
}

impl Region { 
    pub fn new(id: u64, start_key: Vec<u8>, end_key: Vec<u8>, peers: Vec<u64> ) -> Self { 
        let storage = MemStorage::new_with_conf_state(
            (peers, vec![])
        );
        let cfg = Config { 
            id,
            election_tick: 50,
            heartbeat_tick: 3,
            ..Default::default()
        };
        let logger = create_logger();
        let raft = RawNode::new(&cfg, storage.clone(), &logger).unwrap();
        
        Self { 
            meta: RegionMeta { id, start_key, end_key },
            raft,
            storage
        }
    }

    pub fn tick(&mut self) { 
        self.raft.tick();
    }
    pub fn propose(&mut self, cmd: Command) { 
        let encoded = cmd.encode();
        self.raft.propose(vec![], encoded).unwrap();
    }

    pub fn propose_split(&mut self, key: Vec<u8>, new_region_id: u64) { 
        let encoded = Command::Split { key, new_region_id }.encode();
        self.raft.propose(vec![], encoded).unwrap();
    }

    pub fn on_ready(&mut self) -> (Vec<Message>, Vec<Command>){
        let mut messages = Vec::new();
        let mut commands = Vec::new();
        while self.raft.has_ready() { 
            let mut ready = self.raft.ready();
            //  Persist new entries
            if !ready.entries().is_empty() {
                let mut storage = self.storage.wl();
                storage.append(ready.entries()).unwrap();
            }

            // Persist HardState
            if let Some(hs) = ready.hs() {
                let mut storage = self.storage.wl();
                storage.set_hardstate(hs.clone());
            }
            let heartbeat_messages = ready.take_messages();
            let persisted_messages = ready.take_persisted_messages();   
            messages.extend_from_slice(&heartbeat_messages);
            messages.extend_from_slice(&persisted_messages);
            // apply committed entries
            for entry in ready.committed_entries() {
                if entry.data.is_empty() {
                    continue;
                }
                let cmd = Command::decode(&entry.data);
                commands.push(cmd);
            }

            self.raft.advance(ready);
        }
        (messages, commands)
    }

    
}