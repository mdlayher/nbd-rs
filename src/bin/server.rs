use std::fs::File;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

extern crate nbd_rs;
use nbd_rs::{Export, Exports, Result, ServerConnection};

/// A symbolic constant for 1 GiB.
#[allow(non_upper_case_globals)]
const GiB: u64 = 1 << 30;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("[::]:10809")
        .await
        .expect("failed to listen");

    let exports = Arc::new(Exports::single(Export {
        name: "mdlayher nbd-rs".to_string(),
        description: "An NBD server written in Rust".to_string(),
        size: 4 * GiB,
        block_size: 4096,
        readonly: true,
    }));

    let device = Arc::new(Mutex::new(
        File::open("disk.img").expect("failed to open disk file"),
    ));

    loop {
        let (socket, addr) = listener.accept().await.expect("failed to accept");

        // Set TCP_NODELAY, per:
        // https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#protocol-phases.
        socket.set_nodelay(true).expect("failed to set TCP_NODELAY");

        let exports = exports.clone();
        let device = device.clone();
        tokio::spawn(async move {
            let conn = ServerConnection::new(socket);
            println!("{:?}: client connected", addr);

            if let Err(err) = process(conn, addr, &exports, device).await {
                println!("{:?}: error: {:?}", addr, err);
                return;
            }

            println!("{:?}: client disconnected", addr);
        });
    }
}

async fn process(
    conn: ServerConnection<TcpStream>,
    addr: SocketAddr,
    exports: &Exports,
    device: Arc<Mutex<File>>,
) -> Result<()> {
    // Perform the initial handshake. The client and server may negotiate back
    // and forth several times.
    let (conn, export) = match conn.handshake(exports).await? {
        Some(conn) => conn,
        // Client didn't wish to initiate data transmission, do nothing.
        None => return Ok(()),
    };

    // TODO(mdlayher): encode device lock state into the handshake so we can
    // an error to clients when an export is in use.
    println!("{:?}: waiting for lock on export {}...", addr, export.name);
    let device = device.lock().await;

    println!(
        "{:?}: lock acquired, starting I/O for export {:?}",
        addr, export
    );

    // TODO(mdlayher): use an enum to enforce read-only access when the export
    // says it is read-only.
    if export.readonly {
        // Client is ready to transmit data.
        conn.transmit(&*device).await
    } else {
        Err("this server only supports read-only exports".into())
    }
}
