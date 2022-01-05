//! An asynchronous Rust client and server implementation of the [Network Block
//! Device (NBD)](https://en.wikipedia.org/wiki/Network_block_device) protocol.

extern crate bitflags;
extern crate num_derive;

mod connection;
mod consts;
mod frame;

pub use connection::{Client, ServerConnection};
pub use frame::{Export, Exports};

/// A generic Error produced by this crate.
///
/// Unstable and subject to change.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A Result specialized for use in this crate.
pub type Result<T> = std::result::Result<T, Error>;
