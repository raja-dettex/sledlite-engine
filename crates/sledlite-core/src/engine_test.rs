use std::{env::temp_dir, path::PathBuf};

use crate::engine::{Config, Engine};

#[test]
pub fn test_insert_and_get() { 
    let dir  = PathBuf::from("./temp");
    println!("directory : {:?}", dir.to_str());
    let config = Config { 
        dir,
        memtable_max_bytes: 100
    };
    let mut engine = Engine::open(config).expect("expected to build the engine");
    // engine.put(b"hello", b"world").expect("insertioin failed");
    // engine.put(b"hey", b"there").expect("insertioin failed");
    // engine.put(b"on", b"it").expect("insertioin failed");
    // engine.put(b"you", b"listen").expect("insertioin failed");
    // for i in 0..40 { 
    //     let key = format!("key-{i}");
    //     let val = format!("val-{i}");
    //     engine.put(key.as_bytes(), val.as_bytes()).expect("failed to insert");
    // }
    
    let val_utf = engine.get(b"key-39").expect("failed to fetch").expect("at least some value");
    let val = String::from_utf8_lossy(&val_utf);
    println!("fetched the value is {:?}", val.to_string());
}