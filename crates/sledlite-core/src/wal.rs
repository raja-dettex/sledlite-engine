use std::{fs::{File, OpenOptions}, io::{ErrorKind, IoSlice, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::atomic::{AtomicUsize, Ordering}};
use crc32fast::Hasher;
use std::os::windows::fs::FileExt;


#[derive(Debug)]
pub enum WalOp { 
    Put = 1,
    Delete = 2
}

// binary serialized to files 
// op : 1 byte [u8;1] ; key_len : [u8; 4], key: [u8; key_len]; v_len : [u8; 4]; value: [u8; v_len]


impl From<u8> for WalOp {
    fn from(value: u8) -> Self {
        match value { 
            1 => Self::Put,
            2 => Self::Delete,
            _ => Self::Put
        }
    }
}

impl Into<u8> for WalOp {
    fn into(self) -> u8 {
        match self { 
            WalOp::Put => 1 as u8,
            WalOp::Delete => 2 as u8
        }
    }
}

pub struct WalWriter { 
    file: File,
    path: PathBuf,
    log_end: AtomicUsize
}

impl WalWriter { 
    pub fn open<P: AsRef<Path>>(path: P, should_truncate: bool) -> std::io::Result<Self> { 
        let file = OpenOptions::new()
            .create(true)
            .truncate(should_truncate)
            .write(true)
            .open(path.as_ref())?;
        Ok(Self { 
            file, 
            path: path.as_ref().to_path_buf(),
            log_end: AtomicUsize::new(0)
        })
    }
    pub fn append_put(&mut self, lsn: u64, key: &[u8], value: &[u8]) -> std::io::Result<()> { 
        self.append_record(lsn, WalOp::Put, key, Some(value))
    }

    pub fn append_delete(&mut self, lsn: u64, key: &[u8]) -> std::io::Result<()> { 
        self.append_record(lsn, WalOp::Delete, key, None)
    }

    pub fn append_record(&mut self, lsn: u64, op: WalOp, key: &[u8], val: Option<&[u8]>) -> std::io::Result<()>{ 
        let mut bufs = Vec::new();
        let mut hasher = Hasher::new();

        let op_b = [op as u8];
        let lsn_bytes = lsn.to_be_bytes();
        bufs.extend(&lsn_bytes);
        bufs.extend(&op_b);
        hasher.update(&op_b);
        // update key length
        let klen = (key.len() as u32).to_be_bytes();
        bufs.extend(&klen);
        hasher.update(&klen);
        bufs.extend(key);

        hasher.update(key);
        match val { 
            Some(value) => { 
                let vlen = (value.len() as u32).to_be_bytes().to_vec();
                bufs.extend(vlen.clone());
                //{bufs.push(IoSlice::new(&owned_bufs));}
                hasher.update(&vlen);
                 
                bufs.extend(value);
                
                hasher.update(value);
            },
            None => { 
                
                let null_byte = 0u32.to_be_bytes();
                bufs.extend(null_byte);
               
                hasher.update(&0u32.to_be_bytes());
            }
        }
        let hash = hasher.finalize();
        let hash_bytes= hash.to_be_bytes();
        
        bufs.extend(hash_bytes);
        let buf_len = bufs.len();
        let mut offset = self.log_end.fetch_add(buf_len, Ordering::SeqCst) as u64;
        println!("log end is {offset}");
        let mut written = 0usize;
        while !bufs.is_empty() { 
            match self.file.seek_write(&bufs[written..], offset + written as u64) {
                Ok(0) => break,
                Ok(n) => { 
                    written += n;
                }
                Err(_) => todo!(),
            }
        }
        self.file.sync_data()?;
        Ok(())
    }
    
    

}


pub struct WalReader { 
    file: File, 
    path: PathBuf
}

pub struct WalRecord { 
    pub checksum : u64,
    pub op: WalOp, 
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>
}


impl WalReader { 
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        Ok(Self { 
            file, 
            path: path.as_ref().to_path_buf()
        })
    }

    pub fn read_all(&mut self) -> std::io::Result<Vec<WalRecord>> { 
        
        let mut records = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        loop { 
            let mut lsn_buf= [0u8; 8];
            println!("reading lsn buff");
            if let Err(e) = self.file.read_exact(&mut lsn_buf) {
                // EOF
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }
            let lsn = u64::from_be_bytes(lsn_buf);
            println!("the lsn is {lsn}");
            let mut op_buf = [0u8; 1];
            if let Err(e) = self.file.read_exact(&mut op_buf) { 
                if e.kind() == ErrorKind::UnexpectedEof { 
                    break;
                } else { 
                    return Err(e);
                }
            }
            let walop = WalOp::from(op_buf[0]);
            println!("the op is {walop:?}");
            let mut klen_buf = [0u8; 4];
            self.file.read_exact(&mut klen_buf)?;
            let klen = u32::from_be_bytes(klen_buf) as usize;
            println!("key len : {klen}");
            let mut key_buf = vec![0u8; klen];
            self.file.read_exact(&mut key_buf)?;
            println!("the key is {key_buf:?}");
            let mut vlen_buf = [0u8; 4];
            self.file.read_exact(&mut vlen_buf)?;
            let vlen = u32::from_be_bytes(vlen_buf) as usize;
            println!("v len is {vlen}");
            let mut val = if vlen > 0 {
                let mut v = vec![0u8; vlen];
                self.file.read_exact(&mut v)?;
                Some(v)
            } else {
                None
            };
            println!("reading crc buff");
            // crc
            let mut crcbuf = [0u8; 4];
            if let Err(e) = self.file.read_exact(&mut crcbuf) {
                // truncated record — stop replay at truncated tail
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }
            println!("crc buff : {crcbuf:?}");
            let crc = u32::from_be_bytes(crcbuf);
            println!("crc is {crc}");
            // validate crc
            let mut hasher = Hasher::new();
            hasher.update(&op_buf);
            hasher.update(&klen_buf);
            hasher.update(&key_buf);
            hasher.update(&vlen_buf);
            if let Some(ref vv) = val {
                hasher.update(vv);
            }
            let calc = hasher.finalize();
            if calc != crc {
                // corrupted at tail — stop processing to be safe
                println!("crc unmatched");
                break;
            }
            println!("crc matched");

            records.push(WalRecord {
                checksum: lsn,
                op : walop,
                key: key_buf,
                value: val,
            });
        }

        Ok(records)
    }
}