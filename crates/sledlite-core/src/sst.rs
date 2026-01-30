use std::{collections::BTreeMap, fs::{File, OpenOptions}, io::{Seek, SeekFrom}, path::{Path, PathBuf}};
use std::io::{Read, Write};

pub struct SSTWriter { 
    file: File,
    path: PathBuf,
    offsets: Vec<(Vec<u8>, u64)> // hold the offsets of the key to the file    
}

impl SSTWriter { 
    /**
     * Creates a new SSTWriter at the specified path.
     * * If a file already exists at the path, it will be truncated (cleared).
     * The writer maintains an internal `offsets` vector to build the index 
     * after the data block is written.
     */
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;
        Ok(Self { 
            file,
            path: path.as_ref().to_path_buf(),
            offsets: Vec::new()
        })
    }

    /**
     * Persists a collection of key-value pairs to disk in a structured format.
     * * The file structure generated is as follows:
     * 1. Data Block: [KeyLen][Key][ValLen][Value] repeated N times.
     * 2. Index Block: [KeyLen][Key][OffsetInFile] repeated N times.
     * 3. Footer: [IndexOffset (8B)][IndexLength (8B)].
     * * # Arguments
     * * `entries` - A vector of (Key, Value) pairs. Should ideally be sorted 
     * lexicographically for standard SSTable behavior.
     */

    pub fn write_all(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> std::io::Result<()> { 
        let entries_len = entries.len() as u64;
        self.file.write_all(&entries_len.to_be_bytes())?;
        for (k,v) in entries { 
            let offset = self.file.stream_position()?;
            self.offsets.push((k.clone(), offset));
            let key_len = k.len() as u32;
            self.file.write_all(&key_len.to_be_bytes())?;
            self.file.write_all(&k.clone())?;
            let val_len = v.len() as u32;
            self.file.write_all(&val_len.to_be_bytes())?;
            self.file.write_all(&v.clone())?;
        }
        let index_offset = self.file.stream_position()?;
        let index_len = self.offsets.len() as u64;
        for (key, offset) in &self.offsets { 
            let key_len = key.len() as u32;
            self.file.write_all(&key_len.to_be_bytes())?;
            self.file.write_all(key)?;
            self.file.write_all(&offset.to_be_bytes())?;
        } 
        self.file.write_all(&index_offset.to_be_bytes())?;
        self.file.write_all(&index_len.to_be_bytes())?;
        Ok(())
        
    }
}

pub struct SSTReader { 
    file: File,
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>
}

impl SSTReader { 

    /**
     * Opens an existing SSTable file and loads its index into memory.
     * * This method performs a "tail-read":
     * 1. Seeks to the last 16 bytes of the file to find the Index Offset.
     * 2. Jumps to that offset to read the BTreeMap of keys to file positions.
     * * This allows the reader to know where every key is located without 
     * scanning the entire data block.
     */
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        let mut file = OpenOptions::new().read(true).open(path.as_ref())?;
        let mut indexes = BTreeMap::new();
        let size = file.metadata()?.len();
        file.seek(SeekFrom::Start(size - 16))?;
        let mut index_offset_buf = [0u8; 8];
        let mut index_len_buf = [0u8; 8];
        file.read_exact(&mut index_offset_buf)?;
        file.read_exact(&mut index_len_buf)?;
        let index_offset = u64::from_be_bytes(index_offset_buf);
        let index_len = u64::from_be_bytes(index_len_buf);
        file.seek(SeekFrom::Start(index_offset))?;
        for i in 0..index_len { 
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len= u32::from_be_bytes(key_len_buf);
            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)?;
            let mut offset_buf = [0u8; 8];
            file.read_exact(&mut offset_buf)?;
            let offset = u64::from_be_bytes(offset_buf);
            indexes.insert(key_buf.to_vec(), offset);
        }
        Ok(Self { 
            file,
            path: path.as_ref().to_path_buf(),
            index: indexes
        })
    }

    /**
     * Retrieves a value for a specific key by querying the in-memory index.
     * * # Performance
     * * Index Lookup: O(log n) via BTreeMap.
     * * Disk Access: O(1) seek and read.
     * * # Returns
     * * `Ok(Some(Vec<u8>))` if the key is found in the index and successfully read from disk.
     * * `Ok(None)` if the key does not exist in this SSTable.
     */

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> { 
        if let Some(offset) = self.index.get(key) {
            self.file.seek(SeekFrom::Start(*offset))?;
            let mut klen_buf = [0u8; 4];
            self.file.read_exact(&mut klen_buf)?;
            let klen = u32::from_be_bytes(klen_buf);
            let mut key_buf = vec![0u8; klen as usize];
            self.file.read_exact(&mut key_buf)?;
            
            let mut vlen_buf = [0u8; 4];
            self.file.read_exact(&mut vlen_buf)?;
            let vlen = u32::from_be_bytes(vlen_buf);
            let mut value_buf = vec![0u8; vlen as usize];
            self.file.read_exact(&mut value_buf)?;
            return Ok(Some(value_buf));

        }
        Ok(None)
    }
}