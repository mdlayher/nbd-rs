use bytes::{Buf, BytesMut};
use std::io::{self, Cursor, Read, Seek, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::frame::{Errno, Frame};
use crate::frame::Error;

/// An NBD server connection which is ready to perform data transmission.
/// Calling the `transmit` method will consume the connection and block until
/// the NBD client disconnects.
pub struct ServerIoConnection<S> {
    stream: BufWriter<S>,
    buffer: BytesMut,
    readonly: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ServerIoConnection<S> {
    /// Creates a connection ready for I/O by consuming the stream and buffer
    /// from the handshake phase.
    pub(crate) fn new(stream: BufWriter<S>, buffer: BytesMut, readonly: bool) -> Self {
        Self {
            stream,
            buffer,
            readonly,
        }
    }

    /// Begins data transmission with a client using `device` as the export for
    /// I/O operations. This method blocks until the client disconnects or an
    /// unrecoverable error occurs.
    pub async fn transmit<D: Read + Write + Seek>(self, device: D) -> crate::Result<()> {
        RawIoConnection::new(device, self).serve().await
    }
}

/// A low level NBD connection type which handles data transmission operations
/// between a client stream and an exported device.
struct RawIoConnection<D, S> {
    device: D,
    device_buffer: Vec<u8>,
    stream: BufWriter<S>,
    stream_buffer: BytesMut,

    // TODO(mdlayher): disallow write requests using this value, once writes are
    // implemented.
    _readonly: bool,
}

impl<D, S> RawIoConnection<D, S>
where
    D: Read + Write + Seek,
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Consumes a `ServerIoConnection` and allocates buffers to begin handling
    /// I/O for `device`.
    fn new(device: D, conn: ServerIoConnection<S>) -> Self {
        Self {
            device,
            // TODO(mdlayher): Linux seems to perform 128KiB reads, good enough?
            device_buffer: vec![0u8; 256 * 1024],
            stream: conn.stream,
            stream_buffer: conn.buffer,
            _readonly: conn.readonly,
        }
    }

    /// Serves I/O requests on the connection until the client disconnects or an
    /// error occurs.
    async fn serve(&mut self) -> crate::Result<()> {
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
        match Frame::check(&buf) {
            Ok(_) => {
                // Found a complete Frame, parse it, handle any associated I/O
                // and client responses.
                let req = Frame::parse(&mut buf)?;
                let res = Self::handle_io(&req, &mut self.device, &mut self.device_buffer)?;
                if let Some(res) = res {
                    // We have something to write, send it now and flush the
                    // stream. The response frame is aware of how much data is
                    // valid in the device buffer and will slice accordingly.
                    res.write(&mut self.stream, &self.device_buffer).await?;
                    self.stream.flush().await?;
                }

                // Now advance the buffer beyond the current cursor for the next
                // operation.
                //
                // TODO(mdlayher): think about this carefully when we start
                // handling writes since those immediately follow the header.
                let pos = buf.position() as usize;
                self.stream_buffer.advance(pos);

                Ok(Some(()))
            }
            // Not enough data for an entire Frame.
            Err(Error::Incomplete) => Ok(None),
            // Failed to parse.
            Err(e) => Err(e.into()),
        }
    }

    /// Handles a single I/O operation `req` using `device` and its buffer.
    fn handle_io<'a>(
        req: &'a Frame,
        device: &mut D,
        device_buffer: &mut [u8],
    ) -> crate::Result<Option<Frame<'a>>> {
        match req {
            // No reply.
            Frame::Disconnect => Ok(None),
            Frame::ReadRequest(req) => {
                if req.flags != 0 {
                    // TODO(mdlayher): support flags.
                    return Ok(Some(Frame::ReadResponse(req.handle, Errno::Invalid, 0)));
                }

                device.seek(io::SeekFrom::Start(req.offset))?;
                let length = device.read(&mut device_buffer[..req.length])?;

                // Regardless of what the client requested, return the actual
                // number of bytes we read.
                Ok(Some(Frame::ReadResponse(req.handle, Errno::None, length)))
            }
            // Frames a client would handle.
            Frame::ReadResponse(..) => todo!(),
        }
    }
}