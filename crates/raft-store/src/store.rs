use std::collections::HashMap;

use crate::region::{Region, SplitEvent};
use crate::command::Command;

pub struct RaftStore {
    pub regions: HashMap<u64, Region>
}

impl RaftStore {
    pub fn new() -> Self {
        Self {
            regions: HashMap::new(),
        }
    }

    pub fn create_region(&mut self, region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>, peers: Vec<u64>) { 
        let region = Region::new(region_id, start_key, end_key, peers);
        self.regions.insert(region_id, region);
    }

    pub fn tick_all(&mut self) {
        let mut events = vec![];
        for region in self.regions.values_mut() { 
            region.tick();
            
        }
        for event in events { 
            self.handle_split(event);
        }
    }
    pub fn handle_split(&mut self, event: SplitEvent) { 
        let old_region_id = event.old_region_id; 
        let new_region_id = event.new_region_id;
        let old = self.regions.get_mut(&old_region_id).unwrap();
        let old_end = old.meta.end_key.clone();
        old.meta.end_key = event.split_key.clone();
        let new_region = Region::new(new_region_id, event.split_key.clone(), old_end, vec![]);
        self.regions.insert(new_region_id, new_region);
        println!("Region {} split into {}", event.old_region_id, event.new_region_id);
    }

    

    pub fn propose(&mut self, region_id: u64, cmd: Command) { 
        if let Some(region) = self.regions.get_mut(&region_id) { 
            region.propose(cmd);
        }
    }

    pub fn route(&mut self, key: Vec<u8>) -> Option<&mut Region>{ 
        for region in self.regions.values_mut() { 
            if region.meta.contains(key.clone()) { 
                return Some(region)
            }
        }
        None
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) { 
        if let Some(region) = self.route(key.clone()) { 
            region.propose(Command::Put { key, val: value })
        } else { 
            println!("no region found for key")
        }
    }
}