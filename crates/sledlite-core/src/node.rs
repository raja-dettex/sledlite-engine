use crossbeam_epoch::{Atomic};

pub struct Node  { 
    pub children: Box<[Atomic<Node>]>,
    pub value: Atomic<Vec<u8>>
}


pub const BRANCH : usize = 256;

impl Node { 
    pub fn new() -> Self { 
        let mut children = Vec::with_capacity(BRANCH);
        for i in 0..BRANCH { 
            children.push(Atomic::null());
        }
        Self { 
            children: children.into_boxed_slice(),
            value: Atomic::null()
        }
    }

    pub fn child(&self, idx: u8) -> &Atomic<Node> { 
        &self.children[idx as usize]
    }
}

