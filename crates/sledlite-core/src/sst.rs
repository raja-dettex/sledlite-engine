use std::{collections::BTreeMap, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}};

pub struct SSTWriter { 
    file: File, 
    path: PathBuf,
    offsets : Vec<(Vec<u8>, u64)> // offset is the file offset where the key begins
}

impl SSTWriter { 
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;
        Ok(Self { 
            file,
            path: path.as_ref().to_path_buf(),
            offsets: Vec::new()
        })
    }

    pub fn write_all(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> std::io::Result<()>{
        //let mut offsets = Vec::new();
        //self.file.seek(SeekFrom::Start(0))?;
        let mut entries_len = entries.len() as u64;
        self.file.write(&entries_len.to_be_bytes())?;
        for (k, v) in entries.iter() { 
            let offset = self.file.stream_position()?;
            self.offsets.push((k.clone(), offset));
            let key_len = k.len() as u32;
            self.file.write_all(&key_len.to_be_bytes())?;
            self.file.write_all(&k.clone())?;
            let v_len = v.len() as u32;
            self.file.write_all(&v_len.to_be_bytes())?;
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
    index : BTreeMap<Vec<u8>, u64>
}


impl SSTReader { 
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> { 
        println!("opemning sst");
        let mut indexes = BTreeMap::new();
        let mut file = OpenOptions::new().read(true).open(path.as_ref())?;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::Start(size - 16))?;
        let mut index_offset_buf = [0u8; 8];
        let mut index_num_buff =[0u8; 8];
        file.read_exact(&mut index_offset_buf)?;
        file.read_exact(&mut index_num_buff)?;
        let index_offset = u64::from_be_bytes(index_offset_buf);
        let index_num = u64::from_be_bytes(index_num_buff) as usize;
        println!("index offset {} and index number : {}", index_offset, index_num);
        file.seek(SeekFrom::Start(index_offset))?;
        for i in 0..index_num { 
            let mut klen_buf = [0u8; 4];
            file.read_exact(&mut klen_buf)?;
            let klen = u32::from_be_bytes(klen_buf);
            let mut key_buf = vec![0u8; klen as usize ];
            file.read_exact(&mut key_buf)?;
            let mut offset_buf = [0u8; 8];
            file.read_exact(&mut offset_buf)?;
            let offset = u64::from_be_bytes(offset_buf);
            indexes.insert(key_buf, offset);
        }
        Ok(Self { 
            file, 
            path: path.as_ref().to_path_buf(),
            index: indexes
        })
    }

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