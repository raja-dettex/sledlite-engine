use std::{fs::{File, OpenOptions}, io::{Seek, Read}, path::{Path, PathBuf}, sync::atomic::{AtomicUsize, Ordering}};

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
            WalOp::Delete => 2 as u8,
        }
    }
}

pub struct WalWriter { 
    file: File,
    path: PathBuf,
    pub lsn: AtomicUsize,
    pub appendable_lsn: AtomicUsize
}

impl WalWriter { 

    /**
     * Opens or creates a WAL file at the specified path.
     * * If `should_truncate` is true, the file is cleared. 
     * * On opening, it reads the first 16 bytes to initialize the LSN (Log Sequence Number)
     * and the Appendable LSN from the file header.
     */
    pub fn open<P: AsRef<Path>>(path: P, should_truncate: bool) -> std::io::Result<Self> { 
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .truncate(should_truncate)
            .open(path.as_ref())?;
        let lsn = Self::lsn(&mut file);
        let appendable_lsn = Self::appendable_lsn(&mut file);
        println!("wal writer lsn {lsn}");
        
        Ok(Self { 
            file,
            path: path.as_ref().to_path_buf(),
            lsn: AtomicUsize::new(lsn as usize),
            appendable_lsn: AtomicUsize::new(appendable_lsn as usize)
        })
    }


    #[inline]
    pub fn lsn(file: &mut File) -> u64 {
        let mut lsn_buf = [0u8; 8]; 
        match file.read_exact(&mut lsn_buf) { 
            Ok(_) => u64::from_be_bytes(lsn_buf),
            Err(err) => { 
                println!("in eror block {err:?}");
                return 16;
            }
        }
    }

    #[inline]
    pub fn appendable_lsn(file: &mut File) -> u64 {
        let mut buf = [0u8; 8]; 
        match file.seek_read(&mut buf, 8) { 
            Ok(_) => u64::from_be_bytes(buf),
            Err(err) => { 
                println!("in eror block {err:?}");
                return 0;
            }
        }
    }


    /**
     * Appends a 'Put' operation to the log.
     * * This maps to a key-value insertion or update.
     */
    pub fn append_put(&mut self, lsn: u64, key: &[u8], value: &[u8]) -> std::io::Result<()> { 
        self.append_record(lsn, WalOp::Put, key, Some(value))
    }


    /**
     * Appends a 'Delete' operation to the log.
     * * This marks a key for removal, storing only the key with no value.
     */
    pub fn append_delete(&mut self, lsn: u64, key: &[u8]) -> std::io::Result<()> { 
        self.append_record(lsn, WalOp::Delete, key, None)
    }



    /**
     * Low-level method that serializes a record and writes it to disk.
     * * # Binary Format:
     * [LSN (8B)][Op (1B)][KeyLen (4B)][Key (NB)][ValLen (4B)][Value (MB)][CRC32 (4B)]
     * * # Process:
     * 1. Calculates a CRC32 checksum for data integrity.
     * 2. Uses `seek_write` for thread-safe-ish positioning via offsets.
     * 3. Updates the file header (first 16 bytes) with the new LSNs.
     * 4. Calls `sync_data()` to ensure the OS flushes the write to physical hardware.
     */
    pub fn append_record(&mut self, lsn: u64, wal_op: WalOp, key: &[u8], value: Option<&[u8]>) -> std::io::Result<()> { 
        let mut buf: Vec<u8> = Vec::new();
        let mut hasher = Hasher::new();
        let op_b = [wal_op as u8];
        let lsn_bytes = lsn.to_be_bytes();
        buf.extend(&lsn_bytes);
        buf.extend(&op_b);
        hasher.update(&op_b);
        let key_len = key.len() as u32;
        let key_len_bytes = key_len.to_be_bytes();
        buf.extend(&key_len_bytes);
        hasher.update(&key_len_bytes);
        buf.extend(key);
        hasher.update(key);
        match value { 
            Some(val) => { 
                let v_len_bytes = (val.len() as u32).to_be_bytes();
                buf.extend(&v_len_bytes);
                hasher.update(&v_len_bytes);
                buf.extend(val);
                hasher.update(val);
            },
            None => { 
                let v_len_bytes = 0u32.to_be_bytes();
                buf.extend(&v_len_bytes);
                hasher.update(&v_len_bytes);
            }
        }

        let hash = hasher.finalize();
        let hash_bytes = hash.to_be_bytes();
        buf.extend(&hash_bytes);
        let buf_len = buf.len();
        let offset = self.lsn.fetch_add(buf_len, Ordering::SeqCst) as u64;
        let mut written = 0usize;
        while !buf.is_empty() { 
            match self.file.seek_write(&buf[written..], offset + written as u64) { 
                Ok(0) => break,
                Ok(n) => written = written + n,
                Err(_) => todo!()
            }
        }
        let fetch_lsn = self.lsn.load(Ordering::SeqCst) as u64;
        self.appendable_lsn.swap(lsn as usize, Ordering::SeqCst);
        println!("updating lsn: {lsn}");
        let _ = self.file.seek_write(&fetch_lsn.to_be_bytes(), 0)?;
        let _ = self.file.seek_write(&lsn.to_be_bytes(), 8);
        self.file.sync_data()?;
        Ok(())
    }
}


pub struct WalReader {
    file: File, 
    path: PathBuf
}

#[derive(Debug)]
pub struct WalRecord { 
    pub lsn: u64,
    pub op: WalOp,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>
}

impl WalReader { 

    /**
     * Opens a WAL file for recovery or inspection.
     * * Does not modify the file; opens in read-only mode.
     */
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        let file = OpenOptions::new().read(true).open(path.as_ref().to_path_buf())?;
        Ok(Self { 
            file, 
            path: path.as_ref().to_path_buf()
        })
    }


    /**
     * Parses the entire WAL file and returns a list of valid records.
     * * # Safety & Integrity:
     * * Skips the 16-byte header to begin reading records.
     * * For every record, it re-calculates the CRC32 checksum. 
     * * If a checksum mismatch is detected (indicating a partial write or corruption), 
     * it stops reading and returns the records collected so far.
     * * Handles `UnexpectedEof` gracefully to determine the end of the log.
     */
    pub fn read_all(&mut self) -> std::io::Result<Vec<WalRecord>> { 

        let mut records = Vec::new();
        self.file.seek(std::io::SeekFrom::Start(16))?;
        loop { 
            let mut lsn_buf = [0u8; 8];
            if let Err(e)  = self.file.read_exact(&mut lsn_buf) { 
                if e.kind() == std::io::ErrorKind::UnexpectedEof { 
                    break;
                } else { 
                    return Err(e);
                }
            }
            let mut lsn = u64::from_be_bytes(lsn_buf);
            let mut op_buf = [0u8];
            if let Err(e) = self.file.read_exact(&mut op_buf) { 
                if e.kind() == std::io::ErrorKind::UnexpectedEof { 
                    break;
                } else { 
                    return Err(e);
                }
            }
            let op = WalOp::from(op_buf[0]);
            let mut key_len_buf = [0u8; 4];
            self.file.read_exact(&mut key_len_buf)?;
            let mut key_len = u32::from_be_bytes(key_len_buf) as usize;
            let mut key_buf = vec![0u8; key_len];
            self.file.read_exact(&mut key_buf)?;
            let mut val_len_buf = [0u8; 4];
            self.file.read_exact(&mut val_len_buf)?;
            let val_len = u32::from_be_bytes(val_len_buf) as usize;
            let val = if val_len > 0 { 
                let mut val_buf = vec![0u8; val_len];
                self.file.read_exact(&mut val_buf)?;
                Some(val_buf)
            } else { 
                None
            };
            // validate the crc 
            let mut crc_buf = [0u8; 4];
            self.file.read_exact(&mut crc_buf)?;
            let crc = u32::from_be_bytes(crc_buf);
            let mut hasher = Hasher::new(); 
            hasher.update(&op_buf);
            hasher.update(&key_len_buf);
            hasher.update(&key_buf);
            hasher.update(&val_len_buf);
            if let Some(ref v) = val { 
                hasher.update(v);
            }
            let calc = hasher.finalize();
            if calc != crc { 
                // corrupted, stop reading to be safe
                break;
            }
            records.push(WalRecord {
                lsn,
                op,
                key: key_buf,
                value: val
            });
        }
        Ok(records)
    }
}