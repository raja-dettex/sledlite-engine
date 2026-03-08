mod region;
mod store;
mod command;
pub mod transport;
pub mod node;

use std::collections::HashMap;


use crate::{node::Node, region::Region, transport::Transport};


#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let id: u64 = args[1].parse().unwrap();
    let addr = args[2].clone();
    let mut peers = HashMap::new();
    peers.insert(1, "127.0.0.1:5001".to_string());
    peers.insert(2, "127.0.0.1:5002".to_string());
    peers.insert(3, "127.0.0.1:5003".to_string());
    let transport = Transport { peers };
    let region = Region::new(id, b"a".to_vec(), b"e".to_vec(), vec![1,2,3]);
    let mut node = Node::new(id, region, transport);
    node.run(addr).await;
}