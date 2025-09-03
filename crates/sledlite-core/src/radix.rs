use std::sync::atomic::Ordering;

use crossbeam_epoch::{Atomic, Owned, Shared};
use crate::node::{Node, BRANCH};


pub struct RadixTree { 
    root: Atomic<Node>
}

#[derive(Debug)]
pub enum RadixError { 
    InvalidKey,
    Failed{ failed_garbage_value: Vec<u8> } 
}
impl RadixTree { 
    pub fn new() -> Self { 
        Self { 
            root: Atomic::new(Node::new())
        }
    }


    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> { 
        let guard = crossbeam_epoch::pin();
        let mut cur_shared = self.root.load(Ordering::SeqCst, &guard);
        if cur_shared.is_null() { 
            return None;
        }
        for &b in key { 
            let cur_node = unsafe { cur_shared.deref()};
            let next = cur_node.child(b).load(Ordering::SeqCst, &guard);
            if next.is_null() { 
                return None
            }
            cur_shared = next;
        }

        let current_value = (unsafe { cur_shared.deref() }).value.load(Ordering::SeqCst, &guard);
        if current_value.is_null() { 
            return None
        } else { 
            let value = unsafe { current_value.deref()};
            Some(value.clone())
        }
        
    }

    pub fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<Option<Vec<u8>>, RadixError>{ 
        if key.is_empty() { 
            return Err(RadixError::InvalidKey);
        }
        let guard = crossbeam_epoch::pin();
        let mut curr = self.root.load(Ordering::SeqCst, &guard);
        if curr.is_null() { 
            let new_root = Owned::new(Node::new());
            match self.root.compare_exchange(curr, new_root, Ordering::SeqCst, Ordering::SeqCst, &guard) { 
                Ok(shared) => curr = shared,
                Err(e) => curr = e.current
            }
        }

        for &b in key { 
            let current_node = unsafe { curr.deref()};
            let next = current_node.child(b);
            let mut next_node = next.load(Ordering::SeqCst, &guard);
            if next_node.is_null() { 
                let new_node = Owned::new(Node::new());
                match next.compare_exchange(next_node, new_node, Ordering::SeqCst, Ordering::SeqCst, &guard) {
                    Ok(shared) => next_node = shared,
                    Err(e) => next_node = e.current,
                }
            }
            curr = next_node;
        }


        // next just swap the value of the terminating node, 
        let curr_node = unsafe { curr.deref()};
        let old_val_shared = curr_node.value.swap(Owned::new(value), Ordering::SeqCst, &guard);
        if old_val_shared.is_null() { 
            return Ok(None)
        } else { 
            let old_vec = unsafe { old_val_shared.deref() };
            let old_vec_clone = old_vec.clone();
            unsafe { guard.defer_destroy(old_val_shared);}
            Ok(Some(old_vec_clone))
        }
        
    }

    pub fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RadixError> {
        if key.is_empty() { 
            return Err(RadixError::InvalidKey)
        }
        let guard = crossbeam_epoch::pin(); 
        let mut curr = self.root.load(Ordering::SeqCst, &guard);
        if curr.is_null() { 
            return Ok(None);
        }
        for &b in key { 
            let curr_node = unsafe { curr.deref()};
            let next = curr_node.child(b);
            let next_node = next.load(Ordering::SeqCst, &guard);
            if next_node.is_null() { return Ok(None);}
            curr = next_node;
        }
        println!("passed till this");
        let curr_node = unsafe { curr.deref()};

        let old_shared = curr_node.value.swap(Shared::null(), Ordering::SeqCst, &guard);
        if old_shared.is_null() {
            Ok(None)
        } else {
            let old_vec = unsafe { old_shared.deref().clone() };
            unsafe {
                guard.defer_destroy(old_shared);
            }
            Ok(Some(old_vec))
        }
    }


    pub fn iter_all(&self) -> Vec<(Vec<u8>, Vec<u8>)>{ 
        let mut out = Vec::new();
        let guard = crossbeam_epoch::pin();
        let root_shared = self.root.load(Ordering::SeqCst, &guard);
        if root_shared.is_null() { 
            return out;
        }
        let mut stack : Vec<(Shared<Node>, Vec<u8>)> = Vec::new();
        stack.push((root_shared, Vec::new()));
        while let Some((shared_node, prefix)) = stack.pop() { 
            let node_ref = unsafe { shared_node.deref()};
            let v_ptr = node_ref.value.load(Ordering::SeqCst, &guard);
            if !v_ptr.is_null() { 
                let value = unsafe { v_ptr.deref()};
                out.push((prefix.clone(), value.clone()));
            }

            for idx in (0..BRANCH).rev() { 
                let atomic_child = node_ref.child(idx as u8);
                let shared_child = atomic_child.load(Ordering::SeqCst, &guard);
                if !shared_child.is_null() { 
                    let mut new_prefix = prefix.clone();
                    new_prefix.push(idx as u8);
                    stack.push((shared_child, new_prefix));
                }
            }
        }

        out
    }
}