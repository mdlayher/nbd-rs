extern crate mdl_nbd;
use mdl_nbd::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("[::1]:10809")
        .await
        .expect("failed to perform handshake");

    let export = client
        .info(None)
        .await
        .expect("failed to get default export");
    dbg!(export);
}
