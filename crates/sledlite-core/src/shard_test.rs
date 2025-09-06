use std::fmt::format;

use serde::{Deserialize, Serialize};

use crate::shard::ShardManager;

#[derive(Debug, Serialize, Deserialize)]
pub struct Animal { 
    color: String, 
    has_claw: bool
}

//#[test]
pub fn test_shard() { 
    let mut shard_manager = ShardManager::new(10, 100).expect("error opening shard manager");
    // 1st insertion
    // let _ = shard_manager.put(b"hello", b"world").expect("got the value");
    // let _ = shard_manager.put(b"hey", b"there").expect("got the value");
    let value = shard_manager.get(b"hello").expect("shol get world").expect("some value");
    let value2= shard_manager.get(b"hey").expect("shol get world").expect("some value");
    println!("hello: {}, hey: {}", String::from_utf8_lossy(&value), String::from_utf8_lossy(&value2));
}



#[test]
pub fn another_test_shard() { 
    let mut shard_manager = ShardManager::new(10, 100).expect("error opening shard manager");
    // for i in 0..100 { 
    //     let key = format!("key-{i}");
    //     let value = serde_json::to_vec(&Animal{ color: format!("red-{i}"), has_claw: true}).expect("serialize into vec failed");
    //     let _ = shard_manager.put(key.as_bytes(), &value);
    // }
    let val = shard_manager.get(b"key-90").expect("error fetching hte vlaue").expect("at leaset some value");
    let val_des : Animal = serde_json::from_slice(&val).expect("des failed of the fetched vlakue ");
    println!("val des is {val_des:?}");
}
