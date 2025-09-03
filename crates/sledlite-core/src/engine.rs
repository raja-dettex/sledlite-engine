use std::{error::Error, fs::{create_dir_all, read_dir}, io::ErrorKind, path::PathBuf, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc}};

use chrono::Timelike;

use crate::{radix::{RadixError, RadixTree}, sst::{SSTReader, SSTWriter}, wal::{WalOp, WalReader, WalWriter}};
#[derive(Clone)]
pub struct Config { 
    pub dir: PathBuf,
    pub memtable_max_bytes : usize
}


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
        let mut engine = Self {
            wal_path, 
            wal,
            dir: cfg.dir.clone(),
            memtable,
            memtable_bytes: AtomicUsize::new(0),
            sst_readers,
            cfg,
            next_lsn: AtomicU64::new(0)
        };
        if let Err(err) = engine.replay_records(){ 
            println!("error while replaying wal records : {:?}", err);
        }
        Ok(engine)
    }

    pub fn replay_records(&mut self) -> std::io::Result<()>{ 
        println!("opening wal reader");
        let mut wal_reader = WalReader::open(self.wal_path.clone())?;
        println!("reading wal reader");
        if self.wal_path.metadata()?.len() < 9 {
            return Err(std::io::Error::new(ErrorKind::InvalidData, "invalid wal data"));
        }
        let mut wal_records = wal_reader.read_all().expect("reading wal records failed");
        wal_records.sort_by_key(|w| w.checksum);
        for record in wal_records { 
            match record.op { 
                WalOp::Put => { 
                    self.memtable.insert(&record.key, record.value.unwrap().to_vec()).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "memtable insertion failed ")
                    })?; 
                },
                WalOp::Delete => { self.memtable.remove(&record.key).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "memtable insertion failed ")
                    })?; 
                } 
            }
        }
        Ok(())  
    }

    fn memtable_dump(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> { 
        self.memtable.iter_all()
    }

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

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> { 
        // lets do full scan of the memtable first
        if let Some(val) = self.memtable.get(key) { 
            return Ok(Some(val));
        }
        for &mut (_, ref mut sst_reader) in self.sst_readers.iter_mut().rev() { 
            if let Some(val) = sst_reader.get(key)? { 
                return Ok(Some(val));
            }
        }
        
        Ok(None)
    }

    

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> std::io::Result<Option<Vec<u8>>> {     
        // check wheather the memtable is full
        let curr_memtable_bytes = self.memtable_bytes.load(Ordering::SeqCst);
        println!("current memtable bytes : {curr_memtable_bytes}");
        if curr_memtable_bytes + key.len() + val.len() >= self.cfg.memtable_max_bytes { 
            self.flush_memtable()?;
        }

        match self.memtable.insert(key, val.to_vec()) { 
            Ok(Some(val)) => { 
                println!("ncrementing the memtable bytes");
                self.memtable_bytes.fetch_add(key.len() + val.len(), Ordering::SeqCst);
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);               
                self.wal.append_put(next_lsn, key, &val)?;
                return Ok(Some(val));
            },
            Ok(None) =>  { 
                self.memtable_bytes.fetch_add(key.len() + val.len(), Ordering::SeqCst);
                let next_lsn = self.next_lsn.fetch_add(1 as u64, Ordering::SeqCst);
                self.wal.append_put(next_lsn , key, &val)?;
                Ok(None)
            },
            Err(e) => Err(std::io::Error::new(ErrorKind::ConnectionAborted, "unable to write")) 
        }
    }
} 
