use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    net::{TcpStream, ToSocketAddrs},
};

use super::frame::*;
use crate::transmit::connection::ServerIoConnection;

/// An NBD client connection which can query information and perform I/O
/// transmission operations with a server export.
pub struct Client<S> {
    conn: RawConnection<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// Establishes a TCP connection with the NBD server at `addr` and
    /// immediately performs the client handshake operation. The resulting
    /// `Client` can then be used to query metadata or perform I/O transmission
    /// operations.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client<TcpStream>> {
        // Set TCP_NODELAY, per:
        // https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#protocol-phases.
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        Client::handshake(stream).await
    }

    /// Initiates the NBD client handshake with a server using `stream`
    /// (typically a TCP connection, but this is not required) to produce a
    /// `Client` which can then query metadata or perform I/O transmission
    /// operations.
    pub async fn handshake(stream: S) -> crate::Result<Self> {
        let mut conn = RawConnection::new(stream);

        // Expect the client and server to support the fixed newstyle handshake
        // with no zeroes flag enabled.
        let server_flags = match conn
            .read_frame(FrameType::ServerHandshake)
            .await?
            .ok_or("server terminated connection while reading server handshake")?
        {
            Frame::ServerHandshake(flags) => flags,
            _ => return Err("server sent invalid server handshake".into()),
        };

        if !server_flags.contains(HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES) {
            return Err("cannot negotiate fixed newstyle handshake with server".into());
        }

        Ok(Self { conn })
    }

    /// Performs an Info request to fetch `Export` metadata from the server. If
    /// `name` is `None`, the server's default export is fetched. If no export
    /// matching `name` could be found, `None` is returned.
    pub async fn info(&mut self, name: Option<&str>) -> crate::Result<Option<Export>> {
        // TODO(mdlayher): this feels awkward to get a String back from &str but
        // the mini-redis example does roughly the same.
        let name = name.map(|string| string.to_string());

        let options = self
            .options(OptionRequest::Info(GoRequest {
                name,
                info_requests: vec![
                    InfoType::Export,
                    InfoType::Name,
                    InfoType::Description,
                    InfoType::BlockSize,
                ],
            }))
            .await?;

        match &options[..] {
            [OptionResponse::Info(GoResponse::Ok { export, .. })] => Ok(Some(export.clone())),
            // TODO(mdlayher): display error strings? Add Error::NotFound?
            [OptionResponse::Info(GoResponse::Unknown(_))] => Ok(None),
            _ => Err("server did not send an info response server option".into()),
        }
    }

    /// Sends a raw `OptionRequest` for a known option and returns one or more
    /// raw `OptionResponse` values.
    async fn options(&mut self, option: OptionRequest) -> crate::Result<Vec<OptionResponse>> {
        self.conn
            .write_frame(Frame::ClientOptions(ClientOptions {
                flags: ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES,
                known: vec![option],
                unknown: Vec::new(),
            }))
            .await?;

        let server_options = match self
            .conn
            .read_frame(FrameType::ServerOptions)
            .await?
            .ok_or("server terminated connection while reading server options")?
        {
            Frame::ServerOptions(options) => options,
            _ => return Err("server sent invalid server options".into()),
        };

        if !server_options.unknown.is_empty() {
            return Err(format!(
                "server did not recognize options: {:?}",
                server_options.unknown,
            )
            .into());
        }

        Ok(server_options.known)
    }
}

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
    async fn read_frame(&mut self, frame_type: FrameType) -> crate::Result<Option<Frame>> {
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
    async fn write_frame(&mut self, frame: Frame) -> io::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::*;
    use std::sync::Arc;
    use tokio::net;

    #[tokio::test]
    async fn info() {
        // Start a locally bound TCP listener and connect to it via another
        // socket so we can perform client/server testing.
        let listener = net::TcpListener::bind("localhost:0")
            .await
            .expect("failed to listen");

        let client = Client::<TcpStream>::connect(
            listener
                .local_addr()
                .expect("failed to get listener address"),
        );

        let exports = Arc::new(Exports::single(Export {
            name: "foo".to_string(),
            description: "bar".to_string(),
            size: 256 * MiB,
            block_size: 512,
            readonly: true,
        }));

        let server_exports = exports.clone();
        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("failed to accept");

            // TODO(mdlayher): make tests for data transmission phase later.
            if ServerConnection::new(socket)
                .handshake(&server_exports)
                .await
                .expect("failed to perform server handshake")
                .is_some()
            {
                panic!("server should not have negotiated data transmission")
            }
        });

        let client_exports = exports.clone();
        let client_handle = tokio::spawn(async move {
            let mut client = client.await.expect("failed to complete client handshake");

            let export = client
                .info(None)
                .await
                .expect("failed to fetch default export")
                .expect("no default export was found");
            let got_exports = Exports::single(export);

            assert_eq!(
                *client_exports, got_exports,
                "unexpected export received by client"
            );
        });

        client_handle.await.expect("failed to run client");
        server_handle.await.expect("failed to run server");
    }
}
