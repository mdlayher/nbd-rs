use bytes::{Buf, BytesMut};
use std::io::{self, Cursor, SeekFrom};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::frame::{CommandFlags, Errno, Frame, Header};
use crate::frame::{Error, Result};
use crate::{Read, ReadWrite};

/// An abstraction over the capabilities of a given device, such as whether the
/// device is read-only or read/write.
pub(crate) enum Device<R, RW> {
    Read(R),
    ReadWrite(RW),
}

/// Wrappers for the variants of Device which may or may not be supported for a
/// given concrete type.
impl<R, RW> Device<R, RW>
where
    R: Read,
    RW: ReadWrite,
{
    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let length = match self {
            Self::Read(r) => Self::_read_at(r, buf, offset)?,
            Self::ReadWrite(rw) => Self::_read_at(rw, buf, offset)?,
        };

        Ok(length)
    }

    fn _read_at(r: &mut impl Read, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // Make use of fused operation where possible, or fall back to manual
        // read+seek.
        if let Some(res) = r.read_at(buf, offset) {
            res
        } else {
            r.seek(SeekFrom::Start(offset))?;
            r.read(buf)
        }
    }

    fn write_all_at(&mut self, req: &Header, buf: &[u8]) -> Result<()> {
        match self {
            Self::Read(..) => Err(Error::Unsupported),
            Self::ReadWrite(rw) => {
                // Make use of fused operation where possible, or fall back to
                // manual write+seek.
                if let Some(res) = rw.write_all_at(buf, req.offset) {
                    res?
                } else {
                    rw.seek(SeekFrom::Start(req.offset))?;
                    rw.write_all(buf)?
                };

                if req.flags.contains(CommandFlags::FUA) {
                    // Client wants us to flush the write buffer before replying
                    // to its request.
                    rw.flush()?;
                }

                Ok(())
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Self::ReadWrite(rw) => Ok(rw.flush()?),
            Self::Read(..) => Err(Error::Unsupported),
        }
    }

    fn trim(&mut self, req: &Header) -> Result<()> {
        match self {
            Self::Read(..) => Err(Error::Unsupported),
            Self::ReadWrite(rw) => {
                rw.trim(req.offset, req.length as u64)?;
                Ok(())
            }
        }
    }

    /// Handles a single I/O operation `req` with the device buffer `buf`.
    fn handle_io<'a>(&mut self, req: &'a Frame, buf: &mut [u8]) -> Option<Frame<'a>> {
        match req {
            // No reply.
            Frame::Disconnect => None,
            Frame::FlushRequest(req) => {
                // Offset and length are reserved and must be zero.
                //
                // Ignore flags for now. Some clients may set FUA or similar
                // it is only valid for writes.
                if req.offset != 0 || req.length != 0 {
                    return Some(Frame::ErrorResponse(Errno::Invalid));
                }

                let errno = self.flush().into();
                Some(Frame::ErrorResponse(errno))
            }
            Frame::ReadRequest(req) => {
                // Ignore flags for now. Some clients may set FUA or similar it
                // is only valid for writes.

                let res = match self.read_at(&mut buf[..req.length], req.offset) {
                    Ok(length) => Frame::ReadResponse(length),
                    Err(err) => Frame::ErrorResponse(err.into()),
                };

                Some(res)
            }
            Frame::TrimRequest(req) => {
                let errno = self.trim(req).into();
                Some(Frame::ErrorResponse(errno))
            }
            Frame::WriteRequest(req, buf) => {
                let errno = self.write_all_at(req, buf).into();
                Some(Frame::ErrorResponse(errno))
            }
            // Frames a client would handle.
            Frame::ErrorResponse(..) | Frame::ReadResponse(..) => todo!(),
        }
    }
}

/// A low level NBD connection type which handles data transmission operations
/// between a client stream and an exported device.
pub(crate) struct RawIoConnection<R, RW, S> {
    device: Device<R, RW>,
    device_buffer: Vec<u8>,
    stream: BufWriter<S>,
    stream_buffer: BytesMut,
}

impl<R, RW, S> RawIoConnection<R, RW, S>
where
    R: Read,
    RW: ReadWrite,
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Consumes the fields of a `ServerIoConnection` and allocates buffers to
    /// begin handling I/O for `device`.
    pub(crate) fn new(
        device: Device<R, RW>,
        stream: BufWriter<S>,
        stream_buffer: BytesMut,
    ) -> Self {
        Self {
            device,
            // TODO(mdlayher): Linux seems to perform 128KiB reads, good enough?
            device_buffer: vec![0u8; 256 * 1024],
            stream,
            stream_buffer,
        }
    }

    /// Serves I/O requests on the connection until the client disconnects or an
    /// error occurs.
    pub(crate) async fn serve(&mut self) -> crate::Result<()> {
        loop {
            if self.next_io().await?.is_some() {
                // Frame handled, try the next one.
                continue;
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket. 0 indicates "end of stream".
            if self.stream.read_buf(&mut self.stream_buffer).await? == 0 {
                // The remote closed the connection.
                if self.stream_buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Try to handle a single I/O operation but also terminate early with an
    /// incomplete error if we need to read more data from the stream.
    async fn next_io(&mut self) -> crate::Result<Option<()>> {
        let mut buf = Cursor::new(&self.stream_buffer[..]);
        match Frame::check(&mut buf) {
            Ok(_) => {
                // Found a complete Frame, parse it, handle any associated I/O
                // and client responses.
                let len = buf.position() as usize;
                buf.set_position(0);

                {
                    let (req, handle) = Frame::parse(&mut buf)?;
                    if let Some(res) = self.device.handle_io(&req, &mut self.device_buffer) {
                        // We have something to write, send it now and flush the
                        // stream. The response frame is aware of how much data
                        // is valid in the device buffer and will slice
                        // accordingly.
                        res.write(&mut self.stream, handle, &self.device_buffer)
                            .await?;
                        self.stream.flush().await?;
                    }
                }

                // Now advance the buffer beyond the current cursor for the next
                // operation.
                self.stream_buffer.advance(len);
                Ok(Some(()))
            }
            // Not enough data for an entire Frame.
            Err(Error::Incomplete) => Ok(None),
            // Failed to parse.
            Err(e) => Err(e.into()),
        }
    }
}
