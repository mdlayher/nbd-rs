use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

extern crate nbd_rs;
use nbd_rs::{Connection, Export};

/// A symbolic constant for 1 MiB.
#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("[::]:10809")
        .await
        .expect("failed to listen");

    loop {
        let (socket, addr) = listener.accept().await.expect("failed to accept");

        tokio::spawn(async move {
            // TODO(mdlayher): allow multiple exports, lock export per client?
            let export = Export {
                name: "mdlayher nbd-rs".to_string(),
                description: "An NBD server written in Rust".to_string(),
                size: 256 * MiB,
                block_size: 512,
                readonly: true,
            };

            process(socket, addr, export).await;
        });
    }
}

async fn process(socket: TcpStream, addr: SocketAddr, export: Export) {
    let mut conn = match Connection::handshake(socket, export).await {
        Ok(conn) => match conn {
            Some(conn) => conn,
            None => {
                println!("{:?}: sent metadata, skipping transmission", addr);
                return;
            }
        },
        Err(err) => {
            println!("{:?}: handshake error: {}", addr, err);
            return;
        }
    };

    println!("{:?}: starting data transmission", addr);
    if let Err(err) = conn.transmit().await {
        println!("{:?}: transmit error: {}", addr, err);
    }
}
