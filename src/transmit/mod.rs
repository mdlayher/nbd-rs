//! Modules related to the data transmission phase of the Network Block Device
//! (NBD) protocol.

mod connection;
pub(crate) use connection::{Device, RawIoConnection};

mod frame;
pub(crate) use frame::FrameType;
