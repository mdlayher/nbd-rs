//! An asynchronous Rust client and server implementation of the [Network Block
//! Device (NBD)](https://en.wikipedia.org/wiki/Network_block_device) protocol.

extern crate bitflags;
extern crate num_derive;

mod client;
mod consts;
mod frame;
mod handshake;
mod server;
mod transmit;

pub use client::Client;
pub use handshake::frame::{Export, Exports};
pub use server::{ServerConnection, ServerIoConnection};

/// A generic Error produced by this crate.
///
/// Unstable and subject to change.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A Result specialized for use in this crate.
pub type Result<T> = std::result::Result<T, Error>;
