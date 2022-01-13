use bytes::Buf;
use std::fmt;
use std::io::{self, Cursor, Read};
use std::result;

use crate::handshake::frame::FrameType as HandshakeFrameType;
use crate::transmit::FrameType as TransmitFrameType;

/// Contains error information encountered while dealing with Frames.
#[derive(Debug)]
pub enum Error {
    /// A sentinel which indicates more data must be read from a stream to parse
    /// an entire Frame.
    Incomplete,

    /// A sentinel which indicates a particular operation is unsupported for a
    /// given type of device.
    Unsupported,

    /// An error during the NBD protocol handshake.
    HandshakeProtocol(HandshakeFrameType),

    /// An error during NBD protocol data transmission.
    TransmitProtocol(TransmitFrameType),

    Other(crate::Error),
}

/// A specialized result for returning Errors.
pub type Result<T> = result::Result<T, Error>;

// Functions for consuming fixed amounts of data from `src` or returning
// `Error::Incomplete` when necessary.

pub fn get_u16(src: &mut Cursor<&[u8]>) -> Result<u16> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u16())
}

pub fn get_u32(src: &mut Cursor<&[u8]>) -> Result<u32> {
    if src.remaining() < 4 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u32())
}

pub fn get_u64(src: &mut Cursor<&[u8]>) -> Result<u64> {
    if src.remaining() < 8 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u64())
}

pub fn get_exact(src: &mut Cursor<&[u8]>, dst: &mut [u8]) -> Result<()> {
    if src.remaining() < dst.len() {
        return Err(Error::Incomplete);
    }

    Ok(src.read_exact(dst)?)
}

pub fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<()> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

// TODO(mdlayher): these From implementations are copied from tokio examples.
// Switch to another error crate to simplify things?

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<io::Error> for Error {
    fn from(src: io::Error) -> Error {
        Error::Other(src.into())
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Unsupported => "device does not support this operation".fmt(fmt),
            Error::HandshakeProtocol(frame_type) => {
                write!(fmt, "protocol error for handshake frame {frame_type:?}")
            }
            Error::TransmitProtocol(frame_type) => {
                write!(fmt, "protocol error for transmit frame {frame_type:?}")
            }
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
