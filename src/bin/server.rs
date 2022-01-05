use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

extern crate nbd_rs;
use nbd_rs::{Export, Exports, Result, ServerConnection};

/// A symbolic constant for 1 MiB.
#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("[::]:10809")
        .await
        .expect("failed to listen");

    let exports = Arc::new(Exports::multiple(
        Export {
            name: "mdlayher nbd-rs".to_string(),
            description: "An NBD server written in Rust".to_string(),
            size: 256 * MiB,
            block_size: 512,
            readonly: true,
        },
        vec![
            Export {
                name: "big".to_string(),
                description: "A large export".to_string(),
                size: 1024 * MiB,
                block_size: 4096,
                readonly: true,
            },
            Export {
                name: "small".to_string(),
                description: "A small export".to_string(),
                size: MiB,
                block_size: 512,
                readonly: true,
            },
        ],
    ));

    loop {
        let (socket, addr) = listener.accept().await.expect("failed to accept");

        let exports = exports.clone();
        tokio::spawn(async move {
            let mut conn = ServerConnection::new(socket);
            if let Err(err) = process(&mut conn, &exports).await {
                println!("{:?}: error: {}", addr, err);
            }
        });
    }
}

async fn process(conn: &mut ServerConnection<TcpStream>, exports: &Exports) -> Result<()> {
    // Perform the initial handshake. The client and server may negotiate back
    // and forth several times.
    if let None = conn.handshake(exports).await? {
        // Client didn't wish to initiate data transmission, do nothing.
        return Ok(());
    }

    // Client is ready to transmit data.
    conn.transmit().await
}
