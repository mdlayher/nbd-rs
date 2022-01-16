//! Modules related to the data transmission phase of the Network Block Device
//! (NBD) protocol.

mod connection;
pub(crate) use connection::RawIoConnection;

mod device;
pub(crate) use device::Device;

mod frame;
pub(crate) use frame::{Frame, FrameType};
