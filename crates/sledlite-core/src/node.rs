use crossbeam_epoch::{Atomic};


pub const BRANCH_CAPACITY: usize = 256;

#[derive(Debug)]
pub struct Node { 
    children: Box<[Atomic<Node>]>,
    value: Atomic<Vec<u8>>
}


impl Node { 
    pub fn new() -> Self { 
        let children = vec![Atomic::null(); BRANCH_CAPACITY];
        Self { 
            children: children.into_boxed_slice(),
            value: Atomic::null()
        }
    }

    pub fn get(&self, b: u8) -> &Atomic<Node>{ 
        &self.children[b as usize]
    }

    pub fn value(&self) -> &Atomic<Vec<u8>> { 
        &self.value
    }
}