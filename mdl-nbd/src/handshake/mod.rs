//! Modules related to the handshake phase of the Network Block Device (NBD)
//! protocol.

mod connection;
pub(crate) use connection::RawConnection;

pub(crate) mod frame;
