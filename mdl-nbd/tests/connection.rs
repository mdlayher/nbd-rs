use std::collections::HashSet;
use std::io::Cursor;
use tokio::net;

use mdl_nbd::{Client, Devices, Export, ServerConnection};

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

    let client = Client::connect(
        listener
            .local_addr()
            .expect("failed to get listener address"),
    );

    let export = Export::new("foo", 256 * MiB).description("bar").readonly();

    let server_export = export.clone();
    let server_handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("failed to accept");

        let devices: Devices<Cursor<&mut [u8]>> = Devices::new(
            server_export,
            Box::new(|_path| panic!("should never open device")),
        );

        // TODO(mdlayher): make tests for data transmission phase later.
        if ServerConnection::new(socket)
            .handshake(&devices, &HashSet::new())
            .await
            .expect("failed to perform server handshake")
            .is_some()
        {
            panic!("server should not have negotiated data transmission")
        }
    });

    let client_handle = tokio::spawn(async move {
        let mut client = client.await.expect("failed to complete client handshake");

        let got_export = client
            .info(None)
            .await
            .expect("failed to fetch default export")
            .expect("no default export was found");

        assert_eq!(export, got_export, "unexpected export received by client");
    });

    client_handle.await.expect("failed to run client");
    server_handle.await.expect("failed to run server");
}

#[tokio::test]
async fn go() {
    // Start a locally bound TCP listener and connect to it via another
    // socket so we can perform client/server testing.
    let listener = net::TcpListener::bind("localhost:0")
        .await
        .expect("failed to listen");

    let client = Client::connect(
        listener
            .local_addr()
            .expect("failed to get listener address"),
    );

    let export = Export::new("foo", MiB).description("bar").readonly();

    let server_export = export.clone();
    let server_handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("failed to accept");

        let devices = Devices::new(
            server_export,
            Box::new(|_path| Ok(Cursor::new(vec![0u8; MiB as usize]))),
        );

        // TODO(mdlayher): make tests for data transmission phase later.
        let (conn, export) = ServerConnection::new(socket)
            .handshake(&devices, &HashSet::new())
            .await
            .expect("failed to perform server handshake")
            .expect("failed to negotiate data transmission");

        let open = devices
            .get(&export.name)
            .expect("device handle was not found");

        let device = open(&export.name).expect("failed to open device");
        conn.transmit(device)
            .await
            .expect("failed to transmit data");
    });

    let client_handle = tokio::spawn(async move {
        let client = client.await.expect("failed to complete client handshake");

        let (conn, got_export) = client
            .go(None)
            .await
            .expect("failed to fetch default export")
            .expect("could not start data transmission on default export");

        conn.disconnect().await.expect("failed to disconnect");

        assert_eq!(export, got_export, "unexpected export received by client");
    });

    client_handle.await.expect("failed to run client");
    server_handle.await.expect("failed to run server");
}
