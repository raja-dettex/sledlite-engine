use std::{sync::atomic::Ordering};

use crate::node::Node;
use crossbeam_epoch::{Atomic, Owned, Shared};

#[derive(Debug)]
pub struct RadixTree { 
    pub root: Atomic<Node>
}

#[derive(Debug)]
pub enum RadixError { 
    InvalidKey,
    Failed{ failed_garbage_value: Vec<u8> },
    AlreadyWritten{ value : Vec<u8>}    
}

impl RadixTree { 
    pub fn new() -> Self { 
        Self { 
            root: Atomic::new(Node::new())
        }
    }

    /**
     * Retrieves the value associated with a given key from the Radix Tree.
     * * This method is lock-free and uses Epoch-Based Reclamation (EBR) via `crossbeam_epoch` 
     * to ensure memory safety during concurrent reads and writes.
     * * # Arguments
     * * `key` - A byte slice representing the path to the desired node.
     * * # Returns
     * * `Ok(Some(Vec<u8>))` if the key exists and has an associated value.
     * * `Ok(None)` if the key path does not exist or the terminal node has no value.
     * * `Err(RadixError)` if the key is empty slice.
     * * # Safety
     * Traversal relies on `unsafe` dereferencing of `Shared` pointers. This is safe 
     * because the `guard` prevents any node from being physically deallocated 
     * while the search is in progress.
     */

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RadixError> {
        if key.is_empty() { 
            return Err(RadixError::InvalidKey);
        } 
        let guard = crossbeam_epoch::pin();
        let mut curr_shared = self.root.load(Ordering::SeqCst,&guard);
        if curr_shared.is_null() { 
            return Ok(None);
        }
        for &b in key { 
            let curr_node = unsafe { curr_shared.deref()};
            let next = curr_node.get(b).load(Ordering::SeqCst, &guard);
            if next.is_null() { 
                return Ok(None);
            }
            curr_shared = next;
        }
        let shared_value = (unsafe { curr_shared.deref()}).value().load(Ordering::SeqCst, &guard);
        
        let value = unsafe { shared_value.deref()}.clone().to_vec();
        Ok(Some(value))
    }

    /**
     * inserts a value associated with a given key in the Radix Tree.
     * * This method is lock-free and uses Epoch-Based Reclamation (EBR) via `crossbeam_epoch` 
     * to ensure memory safety during concurrent reads and writes.
     * * # Arguments
     * * `key` - A byte slice representing the path to the desired node.
     * * # Returns
     * * `Ok(Some(Vec<u8>))` if the key exists and has an associated value and it will
     * always return `Ok(None)`
     * * `Ok(None)` if the terminal node has no value.
     * * `Err(RadixError)` if the key is empty and if the node is already occupied.
     * * # Safety
     * Traversal relies on `unsafe` dereferencing of `Shared` pointers. This is safe 
     * because the `guard` prevents any node from being physically deallocated 
     * while the search is in progress.
     */
    pub fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<Option<Vec<u8>>, RadixError>{
        if key.is_empty() {
            return Err(RadixError::InvalidKey);
        }
        let guard = crossbeam_epoch::pin();
        let mut curr_shared = self.root.load(Ordering::SeqCst, &guard);
        if curr_shared.is_null() { 
            let new_root = Owned::new(Node::new());
            match self.root.compare_exchange(
                curr_shared, new_root, 
                Ordering::SeqCst, 
                Ordering::SeqCst, 
                &guard) { 
                    Ok(shared) => curr_shared = shared,
                    Err(e) => curr_shared = e.current
                }
        }

        for &b in key { 
            let curr_node = unsafe { curr_shared.deref()};
            let next = curr_node.get(b);
            let mut next_shared = next.load(Ordering::SeqCst, &guard);
            if next_shared.is_null() { 
                let new_next = Owned::new(Node::new());
                match next.compare_exchange(
                    next_shared,
                    new_next, 
                    Ordering::SeqCst, 
                    Ordering::SeqCst,
                    &guard) { 
                        Ok(shared) => next_shared = shared,
                        Err(e) => next_shared = e.current
                    }
            }

            curr_shared = next_shared;
        }

        // at tail end swap the value 
        let curr_node = unsafe { curr_shared.deref()};
        let old_value_shared = curr_node.value().swap(Owned::new(value), Ordering::SeqCst, &guard);
        if old_value_shared.is_null() {
            return Ok(None)
        } else { 
            let old_vec = unsafe { old_value_shared.deref()}.clone();
            return Err(RadixError::AlreadyWritten { value: old_vec });
        }
    }

    /**
     * updates the value associated with a given key from the Radix Tree.
     * walks down the tree and does atomic compare exchange on the given slot. 
     * * This method is lock-free and uses Epoch-Based Reclamation (EBR) via `crossbeam_epoch` 
     * to ensure memory safety during concurrent reads and writes.
     * * # Arguments
     * * `key` - A byte slice representing the path to the desired node.
     * * # Returns
     * * `Ok(Some(Vec<u8>))` if the terminal node is updatd with the new value
     * * `Err(RadixError)` if the key is empty slice or the cas fails at the given slot.
     * * # Safety
     * Traversal relies on `unsafe` dereferencing of `Shared` pointers. This is safe 
     * because the `guard` prevents any node from being physically deallocated 
     * while the search is in progress.
     */

    pub fn put(&self, key: &[u8], value: Vec<u8>) -> Result<Option<Vec<u8>>, RadixError>{
        if key.is_empty() { 
            return Err(RadixError::InvalidKey);
        }
        let guard = crossbeam_epoch::pin();
        let mut curr_shared = self.root.load(Ordering::SeqCst, &guard);
        if curr_shared.is_null() { 
            let new_root = Owned::new(Node::new());
            match self.root.compare_exchange(
                curr_shared, new_root, 
                Ordering::SeqCst, 
                Ordering::SeqCst, 
                &guard) { 
                    Ok(shared) => curr_shared = shared,
                    Err(e) => curr_shared = e.current
                }
        }

        for &b in key { 
            let curr_node = unsafe { curr_shared.deref()};
            let next = curr_node.get(b);
            let mut next_shared = next.load(Ordering::SeqCst, &guard);
            if next_shared.is_null() { 
                let new_next = Owned::new(Node::new());
                match next.compare_exchange(
                    next_shared,
                    new_next, 
                    Ordering::SeqCst, 
                    Ordering::SeqCst,
                    &guard) { 
                        Ok(shared) => next_shared = shared,
                        Err(e) => next_shared = e.current
                    }
            }

            curr_shared = next_shared;
        }

        // at tail end swap the value 
        let curr_node = unsafe { curr_shared.deref()};
        let curr_shared_value = curr_node.value().load(Ordering::SeqCst, &guard);
        match curr_node.value().compare_exchange(
            curr_shared_value, 
            Owned::new(value), 
            Ordering::SeqCst, 
            Ordering::SeqCst, 
            &guard) {
                Ok(shared) => { 
                    let updated_vec = unsafe {shared.deref() }.clone();
                    return Ok(Some(updated_vec)); 
                },
                Err(e) => { 
                    let current_vec = unsafe { e.current.deref()}.clone();
                    return Err(RadixError::Failed { failed_garbage_value: current_vec });
                }
            }
        
    }

    /**
     * walks down the tree along the key path and swap the slot with null.
     * * This method is lock-free and uses Epoch-Based Reclamation (EBR) via `crossbeam_epoch` 
     * to ensure memory safety during concurrent reads and writes.
     * * # Arguments
     * * `key` - A byte slice representing the path to the desired node.
     * * # Returns
     * * `Ok(Some(Vec<u8>))` if the key exists and had an associated value.
     * * `Ok(None)` if the key path does not exist or the terminal node has no value.
     * * `Err(RadixError)` if the key is empty slice.
     * * # Safety
     * Traversal relies on `unsafe` dereferencing of `Shared` pointers. This is safe 
     * because the `guard` prevents any node from being physically deallocated 
     * while the search is in progress.
     */

    pub fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RadixError> {
        if key.is_empty() {
            return Err(RadixError::InvalidKey);
        } 
        let guard = crossbeam_epoch::pin();
        let mut curr_shared = self.root.load(Ordering::SeqCst, &guard);
        if curr_shared.is_null() {
            return Ok(None)
        }
        for &b in key { 
            let curr_node  = unsafe {curr_shared.deref()};
            let next_shared = curr_node.get(b).load(Ordering::SeqCst, &guard);
            if next_shared.is_null() { 
                return Ok(None)
            }
            curr_shared = next_shared;
        }

        let curr_node = unsafe { curr_shared.deref()};
        let old_val_shared = curr_node.value().swap(Shared::null(), Ordering::SeqCst, &guard);
        if old_val_shared.is_null() { 
            return Ok(None)
        } else { 
            let old_vec = unsafe { old_val_shared.deref()};
            let old_vec_clone = old_vec.clone();
            unsafe { guard.defer_destroy(old_val_shared); }
            return Ok(Some(old_vec_clone))
        }
    }
}