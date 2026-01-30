use std::path::PathBuf;

use crate::engine::{Config, Engine};

#[test]
pub fn engine_test_put_and_get() { 
    let dir = PathBuf::from("./temp");
    let config = Config { 
        dir,
        memtable_max_bytes: 100
    };
    let mut engine = Engine::open(config).expect("can not open engine");
    // for i in 0..38 { 
    //     let _ = engine.put(format!("key-{}", i).as_bytes(), format!("val-{}", i).as_bytes()).expect("put the value");
    // }
    //let _ = engine.put(b"key-36", b"updated-36").expect("updated value");
    //let deleted_val = engine.delete(b"key-25").expect("delete failed");
    //println!("delete value is {deleted_val:?}");
    let val = engine.get(b"key-25").expect("could not get a value"); //.expect("some value atleast");
    println!("{val:?}");
    //println!("value {}", String::from_utf8_lossy(&val).to_string())
}   