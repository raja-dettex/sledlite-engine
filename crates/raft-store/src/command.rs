use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Clone)]
pub enum Command { 
    Put { key: Vec<u8>, val: Vec<u8>},
    Delete { key: Vec<u8>},
    Split { key: Vec<u8>, new_region_id: u64}
}

impl Command { 
    pub fn encode(&self) -> Vec<u8> { 
        let mut buf = BytesMut::new();
        match self { 
            Command::Put { key, val } => { 
                buf.put_u8(1);
                buf.put_u32(key.len() as u32);
                buf.extend_from_slice(&key);
                buf.put_u32(val.len() as u32);
                buf.extend_from_slice(&val);
            },
            Command::Delete { key } => { 
                buf.put_u8(2);
                buf.put_u32(key.len() as u32);
                buf.extend_from_slice(&key);
            },
            Command::Split { key, new_region_id } => { 
                buf.put_u8(3);
                buf.put_u32(key.len() as u32);
                buf.extend_from_slice(&key);
                buf.put_u64(*new_region_id);
            }
        }
        buf.to_vec()
    }

    pub fn decode(mut data: &[u8]) -> Self { 
        let tag = data.get_u8();
        let key_len = data.get_u32();
        let mut key = vec![0u8; key_len as usize];
        data.copy_to_slice(&mut key);
        match tag { 
            1=> { 
                let val_len = data.get_u32();
                let mut val = vec![0u8; val_len as usize];
                data.copy_to_slice(&mut val);
                return Self::Put{key, val};
            },
            2 => { 
                return Self::Delete { key };
            },
            3 => { 
                let new_region_id = data.get_u64();
                return Self::Split { key, new_region_id }
            }
            _ => todo!()
        }
    }
}