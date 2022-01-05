use tokio::net::TcpStream;

extern crate nbd_rs;
use nbd_rs::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::handshake(
        TcpStream::connect("[::1]:10809")
            .await
            .expect("failed to connect"),
    )
    .await
    .expect("failed to perform handshake");

    let export = client
        .info(None)
        .await
        .expect("failed to get default export");
    dbg!(export);
}
