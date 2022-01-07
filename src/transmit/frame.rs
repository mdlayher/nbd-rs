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
    ReadRequest(Header<'a>),
    ReadResponse(Handle<'a>, Errno, usize),
    Disconnect,
}

/// An opaque value used by clients and server to denote matching requests and
/// responses.
type Handle<'a> = &'a [u8];

/// The header for each data transmission operation.
#[derive(Debug)]
pub(crate) struct Header<'a> {
    // TODO(mdlayher): flags enum.
    pub(crate) flags: u16,
    pub(crate) handle: Handle<'a>,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

/// The type of an I/O operation as specified by the protocol.
#[repr(u16)]
#[derive(Debug, FromPrimitive)]
pub(crate) enum IoType {
    Disconnect = NBD_CMD_DISC,
    Read = NBD_CMD_READ,
}

impl<'a> Frame<'a> {
    /// Determines if enough data is available to parse a `Frame`, then returns
    /// the length of that
    pub(crate) fn check(src: &Cursor<&[u8]>) -> Result<()> {
        // TODO(mdlayher): this abuses the fact that reads and disconnects
        // contain a single header and no body data. This won't work for writes.
        if src.remaining() < 28 {
            Err(Error::Incomplete)
        } else {
            Ok(())
        }
    }

    /// Parses the next I/O operation `Frame` from `src`.
    pub(crate) fn parse(src: &'a mut Cursor<&[u8]>) -> Result<Frame<'a>> {
        // Callers use src.position() after this call returns to determine where
        // the next Frame begins. If early returns are added such as for
        // Disconnect (since we don't care about offset, length, etc. in that
        // case) then we have to update the cursor position as if we had read
        // the entire header anyway.

        if get_u32(src)? != NBD_REQUEST_MAGIC {
            return Err(Error::TransmitProtocol(FrameType::Request));
        }

        let flags = get_u16(src)?;
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
            Some(IoType::Read) => Ok(Frame::ReadRequest(header)),
            // Cannot handle writes or other I/O requests yet.
            _ => todo!(),
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
            // Cannot handle writing other I/O responses yet.
            Self::Disconnect | Self::ReadRequest(..) => todo!(),
        }
    }
}
