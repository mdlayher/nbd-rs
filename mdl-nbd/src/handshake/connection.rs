use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use crate::Stream;

use super::frame::*;

/// A low level NBD connection type which deals with reading and writing
/// `Frames` rather than high-level operations.
pub(crate) struct RawConnection<S> {
    pub(crate) stream: BufWriter<S>,
    pub(crate) buffer: BytesMut,
}

impl<S: Stream> RawConnection<S> {
    /// Creates an NBD server connection from `stream`.
    pub(crate) fn new(stream: S) -> Self {
        RawConnection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(32 * 1024),
        }
    }

    /// Reads a single `Frame` of the specified `FrameType` from the underlying
    /// stream.
    pub(crate) async fn read_frame(
        &mut self,
        frame_type: FrameType,
    ) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame(frame_type)? {
                // We read enough data to parse an entire frame, return it now.
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket. 0 indicates "end of stream".
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // The remote closed the connection.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    pub(crate) async fn write_frame(&mut self, frame: Frame) -> io::Result<()> {
        if frame.write(&mut self.stream).await?.is_some() {
            // Wrote a frame, flush it now.
            self.stream.flush().await
        } else {
            Ok(())
        }
    }

    /// Try to parse a single `Frame` but also terminate early with an
    /// incomplete error if we need to read more data from the stream.
    fn parse_frame(&mut self, frame_type: FrameType) -> crate::Result<Option<Frame>> {
        use crate::frame::Error::Incomplete;

        // Begin checking the data we have buffered and see if we can return an
        // entire Frame of the specified type.
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buf, frame_type) {
            Ok(_) => {
                // Found a frame, reset the cursor, parse the entire Frame, then
                // advance the cursor beyond this Frame again for the next read.
                let len = buf.position() as usize;

                buf.set_position(0);
                let frame = Frame::parse(&mut buf, frame_type)?;

                self.buffer.advance(len);
                Ok(Some(frame))
            }
            // Not enough data for an entire Frame.
            Err(Incomplete) => Ok(None),
            // Failed to parse.
            Err(e) => Err(e.into()),
        }
    }
}
