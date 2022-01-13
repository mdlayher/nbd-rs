extern crate mdl_nbd;
extern crate stderrlog;
extern crate tokio;

use mdl_nbd::{Devices, Server};

// Snippet for testing:
//
// sudo modprobe nbd && sudo nbd-client ::1 && sudo dd if=/dev/nbd0 of=/dev/null && for i in `seq 0 15`; do sudo nbd-client -d /dev/nbd$i; done

#[tokio::main]
async fn main() {
    stderrlog::new()
        .color(stderrlog::ColorChoice::Never)
        .verbosity(3)
        .init()
        .unwrap();

    let devices = Devices::file("disk.img").expect("failed to open file device");

    let server = Server::bind("[::]:10809")
        .await
        .expect("failed to bind TCP listener");

    server
        .run(devices)
        .await
        .expect("failed to accept connections");
}
