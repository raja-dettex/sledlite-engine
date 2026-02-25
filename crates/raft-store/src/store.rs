use std::collections::HashMap;

use crate::region::Region;
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

    pub fn create_region(&mut self, region_id: u64) { 
        let region = Region::new(region_id);
        self.regions.insert(region_id, region);
    }

    pub fn tick_all(&mut self) {
        for region in self.regions.values_mut() { 
            region.tick();
            region.on_ready();
        }
    }

    pub fn propose(&mut self, region_id: u64, cmd: Command) { 
        if let Some(region) = self.regions.get_mut(&region_id) { 
            region.propose(cmd);
        }
    }
}