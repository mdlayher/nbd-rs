extern crate mdl_nbd;
extern crate stderrlog;
extern crate tokio;

use mdl_nbd::{Devices, Server};

// Useful snippets for testing:
//
// Allocate a test disk image to export:
// $ fallocate -l 4GiB disk.img
//
// Run the server in release mode and export that disk image:
// $ cargo run --release
//
// In another terminal, negotiate a connection in userspace, hand it off to the
// Linux kernel NBD client, run a dd read test into /dev/null, then disconnect
// from all NBD devices:
// $ sudo modprobe nbd && sudo nbd-client ::1 && sudo dd bs=1M if=/dev/nbd0 of=/dev/null && for i in `seq 0 15`; do sudo nbd-client -d /dev/nbd$i; done

#[tokio::main]
async fn main() {
    // Enable the crate debug logs via stderr.
    stderrlog::new()
        .color(stderrlog::ColorChoice::Never)
        .verbosity(3)
        .init()
        .unwrap();

    let devices = Devices::file("disk.img").expect("failed to open disk.img as export");

    let server = Server::bind("[::]:10809")
        .await
        .expect("failed to bind TCP listener");

    server
        .run(devices)
        .await
        .expect("failed to accept connections");
}
