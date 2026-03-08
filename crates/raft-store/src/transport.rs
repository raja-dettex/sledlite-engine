use std::{collections::HashMap};
use tokio::{io::AsyncReadExt, net::TcpListener};

use raft::eraftpb::Message;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc::Sender};
use protobuf::Message as PbMessage;
pub struct Transport { 
    pub peers: HashMap<u64, String>
}


impl Transport { 
    pub async fn send(&self, to: u64, msg: Message) { 
        for peer in self.peers.get(&to) { 
             match TcpStream::connect(peer).await {
                Ok(mut stream) => {
                    let data = msg.write_to_bytes().unwrap();
                    let len = data.len() as u32;

                    if let Err(e) = stream.write_u32(len).await {
                        println!("write len failed: {:?}", e);
                        return;
                    }

                    if let Err(e) = stream.write_all(&data).await {
                        println!("write body failed: {:?}", e);
                    }
                }
                Err(e) => {
                    println!("connect to {} failed: {:?}", peer, e);
                }
            }
        }
    }

    pub async fn recv_loop(listener: TcpListener, sender: Sender<Message>) { 
        loop { 
            if let Ok((mut stream, _)) = listener.accept().await {
                let sender_clone = sender.clone();
                tokio::spawn(async move { 
                    let len = stream.read_u32().await.unwrap();
                    let mut buf = vec![0; len as usize];
                    let _ = stream.read_exact(&mut buf).await;
                    let msg = Message::parse_from_bytes(&buf).unwrap();
                    let _ = sender_clone.clone().send(msg).await;
                });
            }
        }
    }
}