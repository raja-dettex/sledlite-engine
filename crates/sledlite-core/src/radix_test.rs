use crate::radix::RadixTree;



#[test]
pub fn test_radix() { 

    let tree = RadixTree::new();
    let res = tree.insert(&(b"hello".to_vec()), b"there".into());
    assert!(res.is_ok());
    if let Ok(val) = res { 
        assert_eq!(val, None);
    }
    let value = tree.get(&(b"hello".to_vec())).unwrap().unwrap();
    assert_eq!(value, b"there".to_vec());
    let key: Vec<u8> = vec![1, 2, 3];
    let value: Vec<u8> = vec![4,5,6];
    

    let res = tree.insert(&key, value.clone());
    assert!(res.is_ok());
    if let Ok(val) = res { 
        assert_eq!(val, None);
    }

    let val = tree.get(&key).unwrap().unwrap();
    assert_eq!(value, val);
}

#[test]
pub fn test_radix_re_insertion_will_fail() {    
    let tree = RadixTree::new();
    let key: Vec<u8> = vec![1, 2, 3];
    let val: Vec<u8> = vec![4,5,6];
    

    let res = tree.insert(&key, val.clone());
    assert!(res.is_ok());
    if let Ok(val) = res { 
        assert_eq!(val, None);
    }

    let reinsertion_res  = tree.insert(&key, val.clone());
    assert!(reinsertion_res.is_err());
    if let Err(err) = reinsertion_res { 
        match err { 
            crate::radix::RadixError::AlreadyWritten { value } => assert_eq!(value, val),
           _ => {}
        }
    }  
}


#[test]
pub fn test_radix_re_insertion_will_success_after_slot_reclaimed_by_removal() {    
    let tree = RadixTree::new();
    let key: Vec<u8> = vec![1, 2, 3];
    let val: Vec<u8> = vec![4,5,6];
    

    let inserted = tree.insert(&key, val.clone());
    assert!(inserted.is_ok());
    if let Ok(val) = inserted { 
        assert_eq!(val, None);
    }
    let removed = tree.remove(&key);
    assert!(removed.is_ok());
    if let Ok(Some(removed_val)) = removed { 
        assert_eq!(removed_val, val);
    }
    let another_val = vec![7,8,9];
    let reinsertion_res  = tree.insert(&key, another_val);
    assert!(reinsertion_res.is_ok());
    if let Ok(old_val) = reinsertion_res { 
        assert_eq!(old_val, None);
    }
}


#[test]
pub fn test_radix_put() {    
    let tree = RadixTree::new();
    let key: Vec<u8> = vec![1, 2, 3];
    let val: Vec<u8> = vec![4,5,6];
    

    let inserted = tree.insert(&key, val.clone());
    assert!(inserted.is_ok());
    if let Ok(val) = inserted { 
        assert_eq!(val, None);
    }

    // check get returns the updated value
    let fetched = tree.get(&key);
    assert!(fetched.is_ok());
    if let Ok(Some(value)) = fetched { 
        assert_eq!(value, val);
    }
    
    let another_val = vec![7,8,9];
    let updated  = tree.put(&key, another_val.clone());
    assert!(updated.is_ok());
    if let Ok(Some(updated_val)) = updated { 
        assert_eq!(updated_val, another_val.clone());
    }


    // check get returns the updated value
    let fetched_after_update = tree.get(&key);
    assert!(fetched_after_update.is_ok());
    if let Ok(Some(value)) = fetched_after_update { 
        assert_eq!(value, another_val);
    }
}



