extern crate mdl_nbd;
use mdl_nbd::Client;

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
