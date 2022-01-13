extern crate mdl_nbd;
use mdl_nbd::Client;

// This application doesn't do much yet since many of the NBD client features
// are incomplete. It just tests that we can successfully negotiate a connection
// with the default export and then disconnect from I/O.

#[tokio::main]
async fn main() {
    let client = Client::connect("[::1]:10809")
        .await
        .expect("failed to perform handshake");

    let (conn, export) = client
        .go(None)
        .await
        .expect("failed to get default export")
        .expect("failed to open I/O connection for default export");

    dbg!(export);

    conn.disconnect().await.expect("failed to disconnect");
}
