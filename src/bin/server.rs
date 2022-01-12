use std::fs::OpenOptions;

extern crate mdl_nbd;
use mdl_nbd::{Devices, Export, Server};

/// A symbolic constant for 1 GiB.
#[allow(non_upper_case_globals)]
const GiB: u64 = 1 << 30;

// Snippet for testing:
//
// sudo modprobe nbd && sudo nbd-client ::1 && sudo dd if=/dev/nbd0 of=/dev/null && for i in `seq 0 15`; do sudo nbd-client -d /dev/nbd$i; done

#[tokio::main]
async fn main() {
    let devices = Devices::new(
        Export::new("mdlayher nbd-rs".to_string(), 4 * GiB)
            .description("An NBD server written in Rust".to_string()),
        Box::new(|| {
            // TODO(mdlayher): don't hard-code.
            let f = OpenOptions::new().read(true).write(true).open("disk.img")?;
            Ok(f)
        }),
    );

    let server = Server::bind("[::]:10809")
        .await
        .expect("failed to bind TCP listener");

    server
        .run(devices)
        .await
        .expect("failed to accept connections");
}
