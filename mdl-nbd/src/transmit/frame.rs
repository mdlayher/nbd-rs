use bitflags::bitflags;
use bytes::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::io::{self, Cursor};
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
#[derive(Debug, FromPrimitive)]
pub(crate) enum Errno {
    None = NBD_OK,
    Permission = NBD_EPERM,
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

impl From<io::Error> for Errno {
    /// Converts `io::Error` into the equivalent `Errno` value.
    fn from(src: io::Error) -> Self {
        match src.kind() {
            io::ErrorKind::PermissionDenied => Errno::Permission,
            io::ErrorKind::Unsupported => Errno::NotSupported,
            _ => Errno::Invalid,
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
    TrimRequest(Header),
    WriteRequest(Header, Data<'a>),
}

/// An opaque value used by clients and server to denote matching requests and
/// responses.
type Handle<'a> = &'a [u8];

/// An opaque value used by clients and servers to carry data trailing a
/// `Header` in requests and responses.
type Data<'a> = &'a [u8];

/// The header for each data transmission operation.
#[derive(Debug)]
pub(crate) struct Header {
    pub(crate) flags: CommandFlags,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

/// The magic value of an I/O operation as specified by the protocol.
#[repr(u32)]
#[derive(Copy, Clone, Debug, FromPrimitive, PartialEq)]
pub(crate) enum MagicType {
    Request = NBD_REQUEST_MAGIC,
    SimpleReply = NBD_SIMPLE_REPLY_MAGIC,
}

/// The type of an I/O operation as specified by the protocol.
#[repr(u16)]
#[derive(Copy, Clone, Debug, FromPrimitive, PartialEq)]
pub(crate) enum IoType {
    Disconnect = NBD_CMD_DISC,
    Flush = NBD_CMD_FLUSH,
    Read = NBD_CMD_READ,
    Trim = NBD_CMD_TRIM,
    Write = NBD_CMD_WRITE,
}

bitflags! {
    /// An I/O command flag present in a `Header`.
    pub(crate) struct CommandFlags: u16 {
        const FUA = NBD_CMD_FLAG_FUA;
    }
}

impl<'a> Frame<'a> {
    /// Determines if enough data is available to parse a `Frame`, then returns
    /// the expected magic and I/O type values of that frame. `buf_length` must
    /// be `Some(n)` for parsing client read responses which contain a trailing
    /// byte buffer with no other length indicator.
    pub(crate) fn check(
        src: &'a mut Cursor<&[u8]>,
        buf_length: Option<usize>,
    ) -> Result<(MagicType, Option<IoType>)> {
        // Check for magic value.
        let mtype = FromPrimitive::from_u32(get_u32(src)?)
            .ok_or(Error::TransmitProtocol(FrameType::Request))?;

        match mtype {
            // Client request path.
            MagicType::Request => {
                // buf_length only applies to replies.
                assert!(buf_length.is_none());

                // Skip flags, check I/O type.
                skip(src, 2)?;
                let io_type = FromPrimitive::from_u16(get_u16(src)?)
                    .ok_or(Error::TransmitProtocol(FrameType::Request))?;

                // Skip handle, offset.
                skip(src, 16)?;

                // Length is applicable only to reads and writes.
                let length = get_u32(src)? as usize;

                if io_type == IoType::Write {
                    skip(src, length)?;
                }

                Ok((mtype, Some(io_type)))
            }
            // Server simple reply path.
            MagicType::SimpleReply => {
                // Skip errno, handle, and trailing body byte count stored in
                // buf_length.
                skip(src, 12)?;
                skip(src, buf_length.unwrap())?;

                Ok((mtype, None))
            }
        }
    }

    /// Parses the next I/O operation `Frame` from `src` using the expected
    /// message type and I/O type values to assert the cursor is in the correct
    /// position after frame check.
    pub(crate) fn parse(
        src: &'a mut Cursor<&[u8]>,
        mtype: MagicType,
        io_type: Option<IoType>,
    ) -> Result<(Frame<'a>, Handle<'a>)> {
        // Assert we're aligned properly by verifying matching magic from check.
        assert_eq!(get_u32(src)?, mtype as u32);

        match mtype {
            // Client request path.
            MagicType::Request => {
                let flags =
                    CommandFlags::from_bits(get_u16(src)?).unwrap_or_else(CommandFlags::empty);

                // Assert we're aligned properly by verifying matching I/O type
                // from check. This path must have Some(IoType).
                let io_type = io_type.unwrap();
                assert_eq!(get_u16(src)?, io_type as u16);

                // Borrow opaque u64 Handle for use in reply, but do not decode it.
                let handle = get_handle(src);

                let offset = get_u64(src)?;
                let length = get_u32(src)? as usize;

                let header = Header {
                    flags,
                    offset,
                    length,
                };

                let frame = match io_type {
                    IoType::Disconnect => Frame::Disconnect,
                    IoType::Flush => Frame::FlushRequest(header),
                    IoType::Read => Frame::ReadRequest(header),
                    IoType::Trim => Frame::TrimRequest(header),
                    IoType::Write => {
                        // Write buffer lies beyond the end of the header,
                        // borrow it so we can write it to the device.
                        let pos = src.position() as usize;
                        let buf = &src.get_ref()[pos..pos + length];

                        Frame::WriteRequest(header, buf)
                    }
                };

                Ok((frame, handle))
            }
            // Server simple reply path.
            MagicType::SimpleReply => {
                // io_type only applies to requests.
                assert!(io_type.is_none());

                // Treat unknown errors as EINVALs.
                let errno = FromPrimitive::from_u32(get_u32(src)?).unwrap_or(Errno::Invalid);
                let handle = get_handle(src);

                Ok((Frame::ErrorResponse(errno), handle))
            }
        }
    }

    /// Writes the current `Frame` out to `dst` as a response to the I/O
    /// operation requested by `handle`. It returns `Some(())` if any bytes were
    /// written to the stream or `None` if not.
    pub(crate) async fn write<S: AsyncWrite + Unpin>(
        self,
        dst: &mut S,
        handle: Handle<'a>,
        buf: &[u8],
    ) -> io::Result<Option<()>> {
        match self {
            Self::Disconnect => {
                dst.write_u32(NBD_REQUEST_MAGIC).await?;
                dst.write_u16(CommandFlags::empty().bits()).await?;
                dst.write_u16(IoType::Disconnect as u16).await?;
                dst.write_all(handle).await?;

                // Offset and length must be zero.
                dst.write_u64(0).await?;
                dst.write_u32(0).await?;

                Ok(Some(()))
            }
            Self::ErrorResponse(errno) => {
                // Note that this reply may or may not actually indicate an
                // error since Errno::None indicates an operation succeeded.
                dst.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
                dst.write_u32(errno as u32).await?;
                dst.write_all(handle).await?;

                Ok(Some(()))
            }
            Self::ReadRequest(header) => {
                dst.write_u32(NBD_REQUEST_MAGIC).await?;
                dst.write_u16(header.flags.bits()).await?;
                dst.write_u16(IoType::Read as u16).await?;
                dst.write_all(handle).await?;

                dst.write_u64(header.offset).await?;
                dst.write_u32(header.length as u32).await?;

                Ok(Some(()))
            }
            Self::WriteRequest(header, body) => {
                dst.write_u32(NBD_REQUEST_MAGIC).await?;
                dst.write_u16(header.flags.bits()).await?;
                dst.write_u16(IoType::Write as u16).await?;
                dst.write_all(handle).await?;

                dst.write_u64(header.offset).await?;
                dst.write_u32(header.length as u32).await?;

                dst.write_all(body).await?;

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
            Self::FlushRequest(..) | Self::TrimRequest(..) => todo!(),
        }
    }
}

// Returns a `Handle` from `src` and advances the `Cursor`.
fn get_handle<'a>(src: &mut Cursor<&'a [u8]>) -> Handle<'a> {
    // Borrow opaque u64 Handle for use in replies. We don't decode it because
    // the integer value is meaningless to the server since it'll just be
    // written directly back to the client.
    let pos = src.position() as usize;
    let handle = &src.get_ref()[pos..pos + 8];
    src.advance(8);
    handle
}
