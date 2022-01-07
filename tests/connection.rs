use std::sync::Arc;
use tokio::net;

use mdl_nbd::{Client, Export, Exports, ServerConnection};

/// A symbolic constant for 1 MiB.
#[allow(non_upper_case_globals)]
pub const MiB: u64 = 1 << 20;

#[tokio::test]
async fn info() {
    // Start a locally bound TCP listener and connect to it via another
    // socket so we can perform client/server testing.
    let listener = net::TcpListener::bind("localhost:0")
        .await
        .expect("failed to listen");

    let client = Client::<net::TcpStream>::connect(
        listener
            .local_addr()
            .expect("failed to get listener address"),
    );

    let exports = Arc::new(Exports::single(Export {
        name: "foo".to_string(),
        description: "bar".to_string(),
        size: 256 * MiB,
        block_size: 512,
        readonly: true,
    }));

    let server_exports = exports.clone();
    let server_handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("failed to accept");

        // TODO(mdlayher): make tests for data transmission phase later.
        if ServerConnection::new(socket)
            .handshake(&server_exports)
            .await
            .expect("failed to perform server handshake")
            .is_some()
        {
            panic!("server should not have negotiated data transmission")
        }
    });

    let client_exports = exports.clone();
    let client_handle = tokio::spawn(async move {
        let mut client = client.await.expect("failed to complete client handshake");

        let export = client
            .info(None)
            .await
            .expect("failed to fetch default export")
            .expect("no default export was found");
        let got_exports = Exports::single(export);

        assert_eq!(
            *client_exports, got_exports,
            "unexpected export received by client"
        );
    });

    client_handle.await.expect("failed to run client");
    server_handle.await.expect("failed to run server");
}
