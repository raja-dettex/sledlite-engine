mod region;
mod store;
mod command;

use store::RaftStore;
use command::Command;

fn main() {
    let mut store = RaftStore::new();

    store.create_region(1);
    store.create_region(2);

    // tick to elect leaders
    for _ in 0..50 {
        store.tick_all();
    }

    // propose to region 1
    store.propose(1, Command::Put {
        key: b"k1".to_vec(),
        val: b"v1".to_vec(),
    });

    // propose to region 2
    store.propose(2, Command::Put {
        key: b"k2".to_vec(),
        val: b"v2".to_vec(),
    });

    for _ in 0..50 {
        store.tick_all();
    }
}