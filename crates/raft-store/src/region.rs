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
pub struct Region { 
    pub id: u64,
    pub raft: RawNode<MemStorage>
}

impl Region { 
    pub fn new(id: u64) -> Self { 
        let storage = MemStorage::new_with_conf_state(
            (vec![id], vec![])
        );
        let cfg = Config { 
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        let logger = create_logger();
        let raft = RawNode::new(&cfg, storage, &logger).unwrap();
        
        Self { 
            id,
            raft
        }
    }

    pub fn tick(&mut self) { 
        self.raft.tick();
    }
    pub fn propose(&mut self, cmd: Command) { 
        let encoded = cmd.encode();
        self.raft.propose(vec![], encoded).unwrap();
    }

    pub fn on_ready(&mut self) {
        if !self.raft.has_ready() {
            return;
        }

        let ready = self.raft.ready();

        if !ready.messages().is_empty() {
            // ignore networking for now
        }

        for entry in ready.committed_entries() {
            if entry.data.is_empty() {
                continue;
            }

            let cmd = Command::decode(&entry.data);
            println!("Region {} applied: {:?}", self.id, cmd);
        }

        self.raft.advance(ready);
    }
}