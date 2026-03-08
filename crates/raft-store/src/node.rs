use std::time::Duration;

use tokio::{net::TcpListener, sync::mpsc};

use crate::{region::Region, transport::Transport};

pub struct Node { 
    id: u64,
    region: Region,
    transport: Transport
}

impl Node { 
    pub fn new(id: u64, region: Region, transport: Transport) -> Self { 
        Self { 
            id,region,transport
        }
    }

    pub async fn run(&mut self, listen_addr: String) { 
        println!("starting the main loop");
        let listener = TcpListener::bind(listen_addr).await.unwrap();
        let (tx, mut rx) = mpsc::channel(1024);
        tokio::spawn(async move { 
            Transport::recv_loop(listener, tx).await;
        });
        let mut ticker = tokio::time::interval(Duration::from_millis(200));

        loop { 
            //ticker.tick().await;
            // first process incoming messages
            tokio::select! {
                _ = ticker.tick() => {
                    self.region.tick();
                }

                Some(msg) = rx.recv() => {
                    self.region.raft.step(msg).unwrap();
                }
            }
            //self.region.tick();
            let (messages, commands ) = self.region.on_ready();
            // apply the commands
            for command in commands { 
                println!("command is being applied on region {}, {:?}", self.region.meta.id, command);
            }
            // and brodcast messages to other peers
            for message in messages { 
                let _ = self.transport.send(message.to, message).await;
            }
        }
    }
}