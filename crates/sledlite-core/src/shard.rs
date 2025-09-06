use std::{cell::UnsafeCell, collections::BTreeMap, io::Error, path::PathBuf, sync::Arc};

use crate::{engine::{Config, Engine}, shard};

pub struct ShardInstance { 
    engine: Engine,
    meta: ShardMeta    
}

#[derive(Debug, Clone)]
pub struct ShardMeta { 
    dir: PathBuf, 
    memtable_max_bytes: usize
}

impl ShardInstance { 
    pub fn open(meta: ShardMeta) -> std::io::Result<Self>{ 
        match Engine::open(Config{dir: meta.clone().dir, memtable_max_bytes: meta.clone().memtable_max_bytes}) {
            Ok(sled_engine) => Ok(Self{engine: sled_engine, meta: meta}),
            Err(err) => Err(err),
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> { 
        self.engine.put(key, val)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> { 
        self.engine.get(key)
    }
}

pub struct ShardManager { 
    ranges: BTreeMap<Vec<u8>, usize>,
    shard_ids: BTreeMap<usize, Arc<UnsafeCell<ShardInstance>>>   
}

const MAX_KEYSPACE: usize = 256;

impl ShardManager { 
    pub fn new(shard_count: usize, memtable_max_bytes: usize) -> std::io::Result<Self> { 
        let step = MAX_KEYSPACE  / shard_count;
        let mut ranges = BTreeMap::new();
        let mut shard_ids = BTreeMap::new();
        for i in 0..shard_count { 
            let start_byte = (i * step) as u8;
            let end_byte = if i + 1 == shard_count { 
                0u8
            } else { 
                ((i + 1) * step) as u8
            };
            let start = [start_byte];
             let mut end = vec![];
            if end_byte != 0 || i + 1 == shard_count {
                end = vec![end_byte];
            }
            let meta = ShardMeta { dir: PathBuf::from(format!("./temp/shard{}", i)), memtable_max_bytes  };
            let instance = Arc::new(UnsafeCell::new(ShardInstance::open(meta)?));
            shard_ids.insert(i, instance);
            ranges.insert(start.clone().to_vec(), i);
        }
        Ok(Self { 
            ranges, 
            shard_ids
        })
    }

    fn shard_key(&self, key: &[u8]) -> Option<usize>{
        if key.is_empty() { 
            return None;
        } 
        let pk = vec![key[0]];
        self.ranges.range(..=pk).next_back().map(|(_, &id)| id)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<Option<Vec<u8>>, Error>{ 
        if let Some(id ) = self.shard_key(key.clone()) { 
            
            if let Some(shared) = self.shard_ids.get(&id) { 
                let shard_instance = unsafe { &mut *shared.get() };
                shard_instance.put(key, val)
            } else { 
                Err(Error::new(std::io::ErrorKind::Other, "no shard"))
            }
        } else { 
                Err(Error::new(std::io::ErrorKind::Other, "invalid key"))

        }
    }


    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>{ 
        if let Some(id ) = self.shard_key(key.clone())  { 
            
            if let Some(shared) = self.shard_ids.get(&id) { 
                let shard_instance = unsafe { &mut *shared.get() };
                shard_instance.get(key)
            } else { 
                Err(Error::new(std::io::ErrorKind::Other, "no shard"))
    
            }
        } else { 
                Err(Error::new(std::io::ErrorKind::Other, "invalid key"))

        }
    }
    
}





