use bitflags::bitflags;
use bytes::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::io::Cursor;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::consts::*;
use crate::frame::*;

// TODO(mdlayher): make private later if possible.

/// The type of a `Frame` for error reporting.
#[derive(Debug)]
pub enum FrameType {
    Request,
}

/// Possible error number values for I/O requests.
#[repr(u32)]
#[derive(Debug)]
pub(crate) enum Errno {
    None = NBD_OK,
    Invalid = NBD_EINVAL,
    NotSupported = NBD_ENOTSUP,
}

impl<T> From<Result<T>> for Errno {
    /// Converts `Result<T>` into the equivalent `Errno` value.
    fn from(src: Result<T>) -> Self {
        match src {
            Ok(_) => Errno::None,
            Err(err) => err.into(),
        }
    }
}

impl From<Error> for Errno {
    /// Converts `Error` into the equivalent `Errno` value.
    fn from(src: Error) -> Self {
        match src {
            Error::Unsupported => Errno::NotSupported,
            _ => Errno::Invalid,
        }
    }
}

/// An NBD transmission data frame sent between client and server. Note that the
/// frame types here do not necessarily correspond to the NBD specification, but
/// are used to chunk up logical operations in this library.
#[derive(Debug)]
pub(crate) enum Frame<'a> {
    // Control operations.
    Disconnect,

    // A generic errno response message with no body.
    ErrorResponse(Errno),

    // Read operations.
    ReadRequest(Header),
    ReadResponse(usize),

    // Write operations; WriteResponse is used for all requests.
    FlushRequest(Header),
    WriteRequest(Header, &'a [u8]),
}

/// An opaque value used by clients and server to denote matching requests and
/// responses.
type Handle<'a> = &'a [u8];

/// The header for each data transmission operation.
#[derive(Debug)]
pub(crate) struct Header {
    pub(crate) flags: CommandFlags,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

/// The type of an I/O operation as specified by the protocol.
#[repr(u16)]
#[derive(Debug, FromPrimitive)]
pub(crate) enum IoType {
    Disconnect = NBD_CMD_DISC,
    Flush = NBD_CMD_FLUSH,
    Read = NBD_CMD_READ,
    Write = NBD_CMD_WRITE,
}

bitflags! {
    /// An I/O command flag present in a `Header`.
    //
    // TODO(mdlayher): we don't recognize any flags yet.
    pub(crate) struct CommandFlags: u16 {}
}

impl<'a> Frame<'a> {
    /// Determines if enough data is available to parse a `Frame`, then returns
    /// the length of that
    pub(crate) fn check(src: &'a mut Cursor<&[u8]>) -> Result<()> {
        // Check for proper magic.
        if get_u32(src)? != NBD_REQUEST_MAGIC {
            return Err(Error::TransmitProtocol(FrameType::Request));
        }

        // Skip flags, check I/O type.
        skip(src, 2)?;
        let io_type = get_u16(src)?;

        // Skip handle, offset.
        skip(src, 16)?;

        // Length is applicable only to writes.
        let length = get_u32(src)? as usize;

        match FromPrimitive::from_u16(io_type) {
            // Nothing to do.
            Some(IoType::Disconnect) | Some(IoType::Flush) | Some(IoType::Read) => Ok(()),
            // Make sure we can consume a full write.
            Some(IoType::Write) => skip(src, length),
            None => Err(Error::TransmitProtocol(FrameType::Request)),
        }
    }

    /// Parses the next I/O operation `Frame` from `src`.
    pub(crate) fn parse(src: &'a mut Cursor<&[u8]>) -> Result<(Frame<'a>, Handle<'a>)> {
        if get_u32(src)? != NBD_REQUEST_MAGIC {
            return Err(Error::TransmitProtocol(FrameType::Request));
        }

        let flags = CommandFlags::from_bits(get_u16(src)?).unwrap_or_else(CommandFlags::empty);
        let io_type = get_u16(src)?;

        // Borrow opaque u64 Handle for use in reply, but do not decode it.
        let pos = src.position() as usize;
        let handle = &src.get_ref()[pos..pos + 8];
        src.advance(8);

        let offset = get_u64(src)?;
        let length = get_u32(src)? as usize;

        let header = Header {
            flags,
            offset,
            length,
        };

        let frame = match FromPrimitive::from_u16(io_type) {
            Some(IoType::Disconnect) => Frame::Disconnect,
            Some(IoType::Flush) => Frame::FlushRequest(header),
            Some(IoType::Read) => Frame::ReadRequest(header),
            Some(IoType::Write) => {
                // Write buffer lies beyond the end of the header, borrow it so
                // we can write it to the device.
                let pos = src.position() as usize;
                let buf = &src.get_ref()[pos..pos + length];

                Frame::WriteRequest(header, buf)
            }
            None => return Err(Error::TransmitProtocol(FrameType::Request)),
        };

        Ok((frame, handle))
    }

    /// Writes the current `Frame` out to `dst` as a response to the I/O
    /// operation requested by `handle`. It returns `Some(())` if any bytes were
    /// written to the stream or `None` if not.
    pub(crate) async fn write<S: AsyncWrite + Unpin>(
        self,
        handle: Handle<'a>,
        dst: &mut S,
        buf: &[u8],
    ) -> Result<Option<()>> {
        match self {
            Self::ErrorResponse(errno) => {
                // Note that this reply may or may not actually indicate an
                // error since Errno::None indicates an operation succeeded.
                dst.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
                dst.write_u32(errno as u32).await?;
                dst.write_all(handle).await?;

                Ok(Some(()))
            }
            Self::ReadResponse(length) => {
                dst.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
                dst.write_u32(NBD_OK).await?;
                dst.write_all(handle).await?;
                dst.write_all(&buf[..length]).await?;

                Ok(Some(()))
            }
            // Cannot handle writing other I/O responses yet.
            Self::Disconnect
            | Self::FlushRequest(..)
            | Self::ReadRequest(..)
            | Self::WriteRequest(..) => todo!(),
        }
    }
}
