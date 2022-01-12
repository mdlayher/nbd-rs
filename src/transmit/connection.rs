use bytes::{Buf, BytesMut};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::frame::{Errno, Frame};
use crate::frame::{Error, Result};

/// An abstraction over the capabilities of a given device, such as whether the
/// device is read-only or read/write.
pub(crate) enum Device<R: Read + Seek, RW: Read + Write + Seek> {
    Read(R),
    ReadWrite(RW),
}

/// Wrappers for the variants of Device which may or may not be supported for a
/// given concrete type.
impl<R, RW> Device<R, RW>
where
    R: Read + Seek,
    RW: Read + Write + Seek,
{
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let pos = SeekFrom::Start(offset);
        let length = match self {
            Self::Read(r) => {
                r.seek(pos)?;
                r.read(buf)?
            }
            Self::ReadWrite(rw) => {
                rw.seek(pos)?;
                rw.read(buf)?
            }
        };

        Ok(length)
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        match self {
            Self::Read(..) => Err(Error::Unsupported),
            Self::ReadWrite(rw) => {
                rw.seek(SeekFrom::Start(offset))?;
                rw.write_all(buf)?;
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

    /// Handles a single I/O operation `req` with the device buffer `buf`.
    fn handle_io<'a>(&mut self, req: &'a Frame, buf: &mut [u8]) -> Option<Frame<'a>> {
        match req {
            // No reply.
            Frame::Disconnect => None,
            Frame::FlushRequest => {
                let errno = self.flush().into();
                Some(Frame::WriteResponse(errno))
            }
            Frame::ReadRequest(req) => {
                if !req.flags.is_empty() {
                    // TODO(mdlayher): support flags.
                    return Some(Frame::ReadErrorResponse(Errno::Invalid));
                }

                let res = match self.read_at(req.offset, &mut buf[..req.length]) {
                    Ok(length) => Frame::ReadOkResponse(length),
                    Err(err) => Frame::ReadErrorResponse(err.into()),
                };

                Some(res)
            }
            Frame::WriteRequest(req, buf) => {
                if !req.flags.is_empty() {
                    // TODO(mdlayher): support flags.
                    return Some(Frame::WriteResponse(Errno::Invalid));
                }

                let errno = self.write_all_at(req.offset, buf).into();
                Some(Frame::WriteResponse(errno))
            }
            // Frames a client would handle.
            Frame::ReadErrorResponse(..) | Frame::ReadOkResponse(..) | Frame::WriteResponse(..) => {
                todo!()
            }
        }
    }
}

/// A low level NBD connection type which handles data transmission operations
/// between a client stream and an exported device.
pub(crate) struct RawIoConnection<R, RW, S>
where
    R: Read + Seek,
    RW: Read + Write + Seek,
{
    device: Device<R, RW>,
    device_buffer: Vec<u8>,
    stream: BufWriter<S>,
    stream_buffer: BytesMut,
}

impl<R, RW, S> RawIoConnection<R, RW, S>
where
    R: Read + Seek,
    RW: Read + Write + Seek,
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
                        res.write(handle, &mut self.stream, &self.device_buffer)
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
