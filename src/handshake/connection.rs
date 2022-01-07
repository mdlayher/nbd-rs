use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::frame::*;
use crate::transmit::connection::ServerIoConnection;

// TODO(mdlayher): Server type to listen and produce ServerConnections.

/// An NBD server connection which can negotiate options with a client.
pub struct ServerConnection<S> {
    conn: RawConnection<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ServerConnection<S> {
    /// Creates an NBD server connection wrapping `stream` (typically an
    /// accepted client TCP connection) and allocates memory to begin serving
    /// the client's requests.
    pub fn new(stream: S) -> Self {
        Self {
            conn: RawConnection::new(stream),
        }
    }

    /// Initiates the NBD server handshake with a client by exposing the
    /// metadata from `exports`. It is possible for a client to query the server
    /// for options multiple times before initiating data transmission.
    ///
    /// If the client reads data from the server and disconnects without
    /// initiating the data transmission phase, `Ok(None)` will be returned.
    ///
    /// If the client is ready for data transmission, `Some((ServerIoConnection,
    /// Export))` will be returned so data transmission can begin using the
    /// client's chosen export.
    pub async fn handshake(
        mut self,
        exports: &Exports,
    ) -> crate::Result<Option<(ServerIoConnection<S>, Export)>> {
        // Send opening handshake, then negotiate options with client.
        self.conn
            .write_frame(Frame::ServerHandshake(
                HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES,
            ))
            .await?;

        loop {
            // Negotiate options with client.
            let client_options = match self.conn.read_frame(FrameType::ClientOptions).await {
                Ok(frame) => match frame {
                    Some(frame) => match frame {
                        Frame::ClientOptions(options) => options,
                        _ => return Err("client sent invalid client options frame".into()),
                    },
                    // No frame, client hung up.
                    None => return Ok(None),
                },
                // Client hung up, don't care.
                Err(_) => return Ok(None),
            };

            dbg!(&client_options);

            // We expect the client to match our hard-coded flags.
            if !client_options
                .flags
                .contains(ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES)
            {
                return Err("client cannot negotiate fixed newstyle and no zeroes flags".into());
            }

            // For every client known request option, generate the appropriate
            // response. Many of these responses will return information about
            // the current export.
            let response: Vec<OptionResponse> = client_options
                .known
                .into_iter()
                .map(|request| OptionResponse::from_request(request, exports))
                .collect();

            if response
                .iter()
                .any(|option| matches!(option, OptionResponse::Abort))
            {
                // Short-circuit; an abort means the server should abort
                // immediately and ignore any other client data, even if the
                // request might contain invalid options we would otherwise
                // reject.
                self.conn.write_frame(Frame::ServerOptionsAbort).await?;
                return Ok(None);
            }

            // May send nothing if the client didn't pass any unknown options.
            self.conn
                .write_frame(Frame::ServerUnsupportedOptions(client_options.unknown))
                .await?;

            // Data transmission requires that the client both sent a Go option
            // request and requested a valid export. These nested options handle
            // both of those conditions.
            let maybe_transmit: Option<Option<Export>> = response
                .iter()
                .map(|res| {
                    if let OptionResponse::Go(GoResponse::Ok { export, .. }) = res {
                        // Valid export.
                        Some(export.clone())
                    } else {
                        // Invalid export.
                        None
                    }
                })
                .next();

            dbg!(&response);

            // Respond to known options.
            self.conn
                .write_frame(ServerOptions::from_server(response))
                .await?;

            match maybe_transmit {
                Some(Some(export)) => {
                    // Client requested Go and a valid export, prepare for
                    // I/O.
                    return Ok(Some((
                        ServerIoConnection::new(
                            self.conn.stream,
                            self.conn.buffer,
                            export.readonly,
                        ),
                        export,
                    )));
                }
                // Client did not request Go or client did not request a valid
                // export.
                _ => continue,
            };
        }
    }
}

/// A low level NBD connection type which deals with reading and writing
/// `Frames` rather than high-level operations.
pub(crate) struct RawConnection<S> {
    stream: BufWriter<S>,
    buffer: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RawConnection<S> {
    /// Creates an NBD server connection from `stream`.
    pub(crate) fn new(stream: S) -> Self {
        RawConnection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(8 * 1024),
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
