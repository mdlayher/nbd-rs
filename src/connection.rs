use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::frame::{self, *};

/// A high-level Network Block Device (NBD) server connection.
pub struct Connection<S> {
    conn: RawConnection<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    /// Initiates the NBD server handshake with `stream` (typically a client TCP
    /// connection) and exposes metadata from `export`, creating a `Connection`
    /// which is ready to transmit data.
    ///
    /// If the client wishes to read data from the server without initiating the
    /// data transmission phase, `Ok(None)` will be returned.
    pub async fn handshake<'a>(stream: S, export: &'a Export<'a>) -> crate::Result<Option<Self>> {
        let mut conn = RawConnection::new(stream);

        // Send opening handshake, verify client flags.
        {
            conn.write_frame(Frame::ServerHandshake(
                HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES,
            ))
            .await?;

            let client_flags = match conn
                .read_frame(FrameType::ClientFlags)
                .await?
                .ok_or("client terminated while sending client flags")?
            {
                Frame::ClientFlags(flags) => flags,
                _ => return Err("client sent invalid client flags frame".into()),
            };

            dbg!(&client_flags);

            // We expect the client to match our hard-coded flags.
            if !client_flags.contains(ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES) {
                return Err("client cannot negotiate fixed newstyle and no zeroes flags".into());
            }
        }

        // Negotiate options with client.
        {
            let client_options = match conn
                .read_frame(FrameType::ClientOptions)
                .await?
                .ok_or("client terminated while sending client options")?
            {
                Frame::ClientOptions(options) => options,
                _ => return Err("client sent invalid client options frame".into()),
            };

            dbg!(&client_options);

            // For every client known request option, generate the appropriate
            // response. Many of these responses will return information about
            // the current export.
            let response: Vec<OptionResponse> = client_options
                .known
                .into_iter()
                .map(|request| OptionResponse::from_request(request, export))
                .collect();

            if response
                .iter()
                .any(|option| matches!(option, OptionResponse::Abort))
            {
                // Short-circuit; an abort means the server should abort
                // immediately and ignore any other client data, even if the
                // request might contain invalid options we would otherwise
                // reject.
                conn.write_frame(Frame::ServerOptionsAbort).await?;
                return Ok(None);
            }

            // May send nothing if the client didn't pass any unknown options.
            conn.write_frame(Frame::ServerUnsupportedOptions(client_options.unknown))
                .await?;

            let do_transmit = response.iter().any(OptionResponse::do_transmit);

            // Respond to known options.
            conn.write_frame(Frame::ServerOptions(response)).await?;

            if do_transmit {
                // Handshake complete, ready for transmission.
                Ok(Some(Self { conn }))
            } else {
                // No transmission needed, terminate the connection.
                Ok(None)
            }
        }
    }

    /// Begins the data transmission phase of the NBD protocol with a client
    /// which previously completed the NBD handshake.
    pub async fn transmit(&mut self) -> crate::Result<()> {
        // TODO(mdlayher): implement!

        let n = self.conn.stream.read_buf(&mut self.conn.buffer).await?;
        let mut _cursor = Cursor::new(&self.conn.buffer[..n]);

        dbg!(&self.conn.buffer[..n]);

        Ok(())
    }
}

/// A low level NBD connection type which deals with reading and writing
/// `Frames` rather than high-level operations.
struct RawConnection<S> {
    stream: BufWriter<S>,
    buffer: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RawConnection<S> {
    /// Creates an NBD server connection from `stream`.
    fn new(stream: S) -> Self {
        RawConnection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    /// Reads a single `Frame` of the specified `FrameType` from the underlying
    /// stream.
    async fn read_frame(&mut self, frame_type: FrameType) -> crate::Result<Option<Frame<'_>>> {
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
    async fn write_frame(&mut self, frame: Frame<'_>) -> io::Result<()> {
        if frame.write(&mut self.stream).await?.is_some() {
            // Wrote a frame, flush it now.
            self.stream.flush().await
        } else {
            Ok(())
        }
    }

    /// Try to parse a single `Frame` but also terminate early with an
    /// incomplete error if we need to read more data from the stream.
    fn parse_frame<'a>(&mut self, frame_type: FrameType) -> crate::Result<Option<Frame<'a>>> {
        use frame::Error::Incomplete;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::*;
    use tokio::net;

    #[tokio::test]
    async fn handshake() {
        // Start a locally bound TCP listener and connect to it via another
        // socket so we can perform client/server testing.
        let listener = net::TcpListener::bind("localhost:0")
            .await
            .expect("failed to listen");

        let mut stream = net::TcpStream::connect(
            listener
                .local_addr()
                .expect("failed to get listener address"),
        )
        .await
        .expect("failed to connect");

        let server = tokio::spawn(async move {
            let export = Export {
                name: "foo",
                description: "bar",
                size: 256 * MiB,
                block_size: 512,
                readonly: true,
            };

            let (socket, _) = listener.accept().await.expect("failed to accept");

            // TODO(mdlayher): make tests for data transmission phase later.
            let conn = Connection::handshake(socket, &export)
                .await
                .expect("failed to perform server handshake");

            assert!(
                conn.is_none(),
                "handshake should not have negotiated data transmission"
            );
        });

        let client = tokio::spawn(async move {
            // We don't have a Client type so these read/write interactions and
            // bytes are handled manually for now.
            //
            // TODO(mdlayher): replace with Client type and proper Frame support.

            // Drain server handshake.
            let mut buf = BytesMut::with_capacity(8 * 1024);
            let n = stream
                .read_buf(&mut buf)
                .await
                .expect("failed to drain server handshake");

            // Check the presence of server magic.
            assert_eq!(
                &buf[..8],
                NBDMAGIC_BUF,
                "unexpected server handshake magic value"
            );
            buf.advance(n);

            // Send client flags.
            stream
                .write_u32((ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES).bits())
                .await
                .expect("failed to send client flags");

            // Send client options. Notably this sends an unknown option 0 with
            // length 0 for the time being; we don't care about sending a known
            // option.
            stream
                .write_all(&[IHAVEOPT_BUF, &[0u8; 8]].concat())
                .await
                .expect("failed to send client options");

            stream.flush().await.expect("failed to flush stream");

            // Read server options.
            stream
                .read_buf(&mut buf)
                .await
                .expect("failed to read server options");

            // Since we don't support full frame parsing for frames that are
            // sent to clients, just assert for the server's reply magic value
            // which indicates it accepted our handshake and sent some kind of
            // options data back to us.
            assert_eq!(
                &buf[..8],
                REPLYMAGIC_BUF,
                "unexpected server reply magic value"
            );
        });

        client.await.expect("failed to run client");
        server.await.expect("failed to run server");
    }
}
