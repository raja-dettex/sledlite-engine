use std::{fmt::format, sync::{Arc, Mutex}, thread};

use crate::{node::Node, radix::{RadixError, RadixTree}};

//#[test]
pub fn radix_test() { 
    let radix_tree = RadixTree::new();
    // first insertion 
    let result = radix_tree.insert("hello".as_bytes(), "world".as_bytes().to_vec());
    println!("result : {result:?}");

    // next insertion 
    let another = radix_tree.insert("hello".as_bytes(), "there".as_bytes().to_vec());
    let val_bytes = another.unwrap().unwrap();
    println!("next {:?}", String::from_utf8_lossy(&val_bytes));
    assert_eq!(val_bytes, "world".as_bytes().to_vec(), "should match");

    let removed = radix_tree.remove("hello".as_bytes()).expect("expected some value").expect("some at least");
    println!("removed value: {}", String::from_utf8_lossy(&removed));
}

//#[test]
pub fn concurrent_read_and_writes() { 
    let radix_tree = RadixTree::new();
    let shared_radix = Arc::new(Mutex::new(radix_tree));
    let rt1 = shared_radix.clone();
    let rt2 = shared_radix.clone();
    let mut handles = Vec::new();
    for i in  0..8 { 
        let rt_clone = rt1.clone();
        let handle = thread::spawn(move || {
            let key = format!("key-{}", i);
            let val = format!("val-{}", i); 
            let _= rt_clone.lock().unwrap().insert(key.as_bytes(), val.as_bytes().to_vec());
        });
        handles.push(handle);
    }
    for handle in handles { 
        handle.join().unwrap();
    }
}