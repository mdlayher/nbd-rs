use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use super::device::Device;
use super::frame::Frame;
use crate::frame::Error;
use crate::{Read, ReadWrite, Stream};

/// A low level NBD connection type which handles data transmission operations
/// between a client stream and an exported device.
pub(crate) struct RawIoConnection<R, RW, S>
where
    R: Read,
    RW: ReadWrite,
{
    device: Device<R, RW>,
    device_buffer: Vec<u8>,
    stream: BufWriter<S>,
    stream_buffer: BytesMut,
}

impl<R, RW, S> RawIoConnection<R, RW, S>
where
    R: Read,
    RW: ReadWrite,
    S: Stream,
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

        // buf_length always None; no need for server to parse response bodies.
        match Frame::check(&mut buf, None) {
            Ok((mtype, io_type)) => {
                // Found a complete Frame, parse it, handle any associated I/O
                // and client responses.
                let len = buf.position() as usize;
                buf.set_position(0);

                {
                    let (req, handle) = Frame::parse(&mut buf, mtype, io_type)?;
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
