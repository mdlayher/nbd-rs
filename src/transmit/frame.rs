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
}

/// An NBD transmission data frame sent between client and server. Note that the
/// frame types here do not necessarily correspond to the NBD specification, but
/// are used to chunk up logical operations in this library.
#[derive(Debug)]
pub(crate) enum Frame<'a> {
    // Control operations.
    Disconnect,

    // Read operations.
    ReadRequest(Header<'a>),
    ReadResponse(Handle<'a>, Errno, usize),

    // Write operations; WriteResponse is used for all requests.
    FlushRequest(Handle<'a>),
    WriteRequest(Header<'a>, &'a [u8]),
    WriteResponse(Handle<'a>, Errno),
}

/// An opaque value used by clients and server to denote matching requests and
/// responses.
type Handle<'a> = &'a [u8];

/// The header for each data transmission operation.
#[derive(Debug)]
pub(crate) struct Header<'a> {
    pub(crate) flags: CommandFlags,
    pub(crate) handle: Handle<'a>,
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
    pub(crate) fn parse(src: &'a mut Cursor<&[u8]>) -> Result<Frame<'a>> {
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
            handle,
            offset,
            length,
        };

        match FromPrimitive::from_u16(io_type) {
            Some(IoType::Disconnect) => Ok(Frame::Disconnect),
            Some(IoType::Flush) => {
                if offset != 0 || length != 0 {
                    // TODO(mdlayher): return error.
                }

                Ok(Frame::FlushRequest(handle))
            }
            Some(IoType::Read) => Ok(Frame::ReadRequest(header)),
            Some(IoType::Write) => {
                // Write buffer lies beyond the end of the header, borrow it so
                // we can write it to the device.
                let pos = src.position() as usize;
                let buf = &src.get_ref()[pos..pos + length];

                Ok(Frame::WriteRequest(header, buf))
            }
            None => Err(Error::TransmitProtocol(FrameType::Request)),
        }
    }

    /// Writes the current `Frame` out to `dst`. It returns `Some(())` if any
    /// bytes were written to the stream or `None` if not.
    pub(crate) async fn write<S: AsyncWrite + Unpin>(
        self,
        dst: &mut S,
        buf: &[u8],
    ) -> Result<Option<()>> {
        match self {
            Self::ReadResponse(handle, errno, length) => {
                dst.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
                dst.write_u32(errno as u32).await?;
                dst.write_all(handle).await?;
                dst.write_all(&buf[..length]).await?;

                Ok(Some(()))
            }
            Self::WriteResponse(handle, errno) => {
                dst.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
                dst.write_u32(errno as u32).await?;
                dst.write_all(handle).await?;

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
