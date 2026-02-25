use std::{error::Error, fs::{create_dir_all, read_dir}, io::ErrorKind, path::PathBuf, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc}};

use chrono::Timelike;

use crate::{radix::{RadixError, RadixTree}, sst::{SSTReader, SSTWriter}, wal::{WalOp, WalReader, WalWriter}};
#[derive(Clone)]
pub struct Config { 
    pub dir: PathBuf,
    pub memtable_max_bytes : usize
}


// todo: handle concurrent flushing of memtable snapshot into disc with the safest way possible

pub struct Engine {
    wal_path: PathBuf, 
    wal : WalWriter,
    dir: PathBuf,
    memtable : Arc<RadixTree>,
    memtable_bytes : AtomicUsize,
    sst_readers: Vec<(PathBuf, SSTReader)>,
    cfg : Config,
    next_lsn : AtomicU64
}



impl Engine { 

    /**
     * Initializes the storage engine.
     * * # Steps:
     * 1. Creates the data directory if it doesn't exist.
     * 2. Opens the WAL for appending new operations.
     * 3. Scans the directory for existing `sst-*.dat` files and loads them into readers.
     * 4. Triggers `replay_records()` to recover any data from the WAL into the memtable.
     */
    pub fn open(cfg: Config) -> std::io::Result<Self> { 
        println!("openging the engien");
        create_dir_all(cfg.dir.clone())?;
        let wal_path = cfg.dir.clone().join("wal.log");
        println!("trying to open wal writer");
        let mut wal = WalWriter::open(wal_path.clone(), false)?;
        println!("wal writer opened");
        let mut sst_readers = Vec::new();
        let mut sst_paths: Vec<PathBuf> = read_dir(cfg.dir.clone())?
            .filter_map(|rd| rd.ok().map(|r| r.path()))
            .filter(|path| path.is_file() 
                && path.file_name().and_then(|os_str| os_str.to_str())
                .map(|s| s.starts_with("sst-") && s.ends_with(".dat"))
                .unwrap_or(false)
            ).collect();
        sst_paths.sort();
        println!("sst paths : {:?}", sst_paths);
        for path in sst_paths { 
            let sst_reader = SSTReader::open(path.clone())?;
            sst_readers.push((path, sst_reader));
        }
        let memtable = Arc::new(RadixTree::new());
        let lsn = wal.lsn.load(Ordering::SeqCst);
        println!("val of lsn {lsn}");
        let next_lsn = wal.appendable_lsn.load(Ordering::SeqCst) as u64;
        println!("next lsn {next_lsn}");
        let mut engine = Self {
            wal_path, 
            wal,
            dir: cfg.dir.clone(),
            memtable,
            memtable_bytes: AtomicUsize::new(0),
            sst_readers,
            cfg,
            next_lsn: AtomicU64::new(next_lsn + 1)
        };
        if let Err(err) = engine.replay_records(){ 
            println!("error while replaying wal records : {:?}", err);
        }
        Ok(engine)
    }


    /**
     * Recovers the engine state after a crash or restart.
     * * It reads all records from the `wal.log`, sorts them by LSN to ensure 
     * correct operation order, and applies them to the in-memory RadixTree.
     */
    pub fn replay_records(&mut self) -> std::io::Result<()>{ 
        println!("opening wal reader");
        let mut wal_reader = WalReader::open(self.wal_path.clone())?;
        println!("reading wal reader");
        if self.wal_path.metadata()?.len() < 9 {
            return Err(std::io::Error::new(ErrorKind::InvalidData, "invalid wal data"));
        }
        let mut wal_records = wal_reader.read_all().expect("reading wal records failed");
        wal_records.sort_by_key(|w| w.lsn);
        println!("wal records {wal_records:#?}");
        for record in wal_records { 
            match record.op { 
                WalOp::Put => { 
                    self.memtable.put(&record.key, record.value.unwrap().to_vec()).map_err(|err| {
                        println!("error {err:?}");
                        std::io::Error::new(std::io::ErrorKind::Other, "memtable insertion failed ")
                    })?; 
                },
                WalOp::Delete => { self.memtable.remove(&record.key).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "memtable deletion failed ")
                    })?; 
                } 
            }
        }
        Ok(())  
    }


    
    fn memtable_dump(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> { 
        self.memtable.iter_all()
    }


    /**
     * Moves data from memory to permanent storage.
     * * # Workflow:
     * 1. Dumps all current key-value pairs from the RadixTree.
     * 2. Writes them to a new SSTable file named with a unique timestamp.
     * 3. Atomically resets the memtable and clears the `memtable_bytes` counter.
     * 4. Truncates the WAL, as the logged data is now safely persisted in an SSTable.
     * 5. Adds the new SSTable to the list of active readers.
     */
    fn flush_memtable(&mut self) -> std::io::Result<()>{ 
        println!("flushing");
        let k_v_iters = self.memtable_dump();
        let sst_id = chrono::Utc::now().nanosecond();
        let sst_path = self.dir.join(format!("sst-{}.dat", sst_id));
        let mut sst_writer = SSTWriter::open(sst_path.clone())?;
        sst_writer.write_all(k_v_iters)?;

        // clear the memtable
        self.memtable = Arc::new(RadixTree::new());
        self.memtable_bytes.store(0, Ordering::SeqCst);

        // rotate the wal
        let wal_path = self.dir.join("wal.log");
        self.wal = WalWriter::open(wal_path, true)?;
        let sst_reader = SSTReader::open(sst_path.clone())?;
        self.sst_readers.push((sst_path, sst_reader));
        Ok(())
    }


    /**
     * Searches for a key across all storage layers.
     * * # Search Order:
     * 1. **Memtable:** Checks the latest in-memory writes.
     * 2. **SSTables:** If not found, searches SSTables from newest to oldest 
     * (reverse order) to ensure the most recent version of a key is returned.
     */
    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> { 
        // lets do full scan of the memtable first
        let result = self.memtable.get(key);
        if let Err(err) = result { 
            return Err(std::io::Error::new::<String>(ErrorKind::InvalidInput, err.into()));
        }
        if let Ok(Some(val)) = result { 
            return Ok(Some(val));
        }
        for &mut (_, ref mut sst_reader) in self.sst_readers.iter_mut().rev() { 
            if let Some(val) = sst_reader.get(key)? { 
                return Ok(Some(val));
            }
        }
        
        Ok(None)
    }

    
    /**
     * Writes a key-value pair to the engine.
     * * # Logic:
     * 1. Checks if the memtable has exceeded `memtable_max_bytes`. If so, triggers a flush.
     * 2. Writes the operation to the WAL first (Write-Ahead) for durability.
     * 3. Updates the in-memory RadixTree.
     * 4. Increments the global LSN.
     */
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> std::io::Result<Option<Vec<u8>>> {     
        // check wheather the memtable is full
        let curr_memtable_bytes = self.memtable_bytes.load(Ordering::SeqCst);
        println!("current memtable bytes : {curr_memtable_bytes}");
        if curr_memtable_bytes + key.len() + val.len() >= self.cfg.memtable_max_bytes { 
            self.flush_memtable()?;
        }

        match self.memtable.put(key, val.to_vec()) { 
            Ok(Some(value)) => { 
                println!("ncrementing the memtable bytes");
                self.memtable_bytes.fetch_add(key.len() + val.len(), Ordering::SeqCst);
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);               
                self.wal.append_put(next_lsn, key, &val)?;
                return Ok(Some(value));
            },
            Ok(None) =>  { 
                self.memtable_bytes.fetch_add(key.len() + val.len(), Ordering::SeqCst);
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);
                self.wal.append_put(next_lsn , key, &val)?;
                Ok(None)
            },
            Err(e) => Err(std::io::Error::new::<String>(ErrorKind::Other, e.into())) 
        }
    }


    /**
     * Removes a key from the engine.
     * * Similar to `put`, it logs a `Delete` operation to the WAL and 
     * removes the key from the memtable. 
     * * Note: For full LSM-tree correctness, deletes in SSTables are usually 
     * handled via "tombstones" during compaction.: Not yet implemented : to be done
     */
    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {     
        
        match self.memtable.remove(key) { 
            Ok(Some(value)) => { 
                println!("ncrementing the memtable bytes");
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);               
                self.wal.append_delete(next_lsn, key)?;
                return Ok(Some(value));
            },
            Ok(None) =>  { 
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);
                self.wal.append_delete(next_lsn , key)?;
                Ok(None)
            },
            Err(e) => Err(std::io::Error::new::<String>(ErrorKind::Other, e.into())) 
        }
    }
} 
