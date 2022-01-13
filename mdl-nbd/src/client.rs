use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream, ToSocketAddrs};

// TODO(mdlayher): try to avoid usage of *.
use crate::handshake::frame::{Frame as HandshakeFrame, *};
use crate::handshake::RawConnection;
use crate::transmit::Frame as TransmitFrame;

/// An NBD client connection which can query information and perform I/O
/// transmission operations with a server export.
pub struct Client<S> {
    conn: RawConnection<S>,
}

impl Client<TcpStream> {
    /// Establishes a TCP connection with the NBD server at `addr` and
    /// immediately performs the client handshake operation. The resulting
    /// `Client` can then be used to query metadata or perform I/O transmission
    /// operations.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Self> {
        // Set TCP_NODELAY, per:
        // https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#protocol-phases.
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        Client::handshake(stream).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
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
            HandshakeFrame::ServerHandshake(flags) => flags,
            _ => return Err("server sent invalid server handshake".into()),
        };

        if !server_flags.contains(HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES) {
            return Err("cannot negotiate fixed newstyle handshake with server".into());
        }

        Ok(Self { conn })
    }

    /// Performs a Go request to fetch `Export` metadata from the server and
    /// initiate data transmission. If `name` is `None`, the server's default
    /// export is fetched. If no export matching `name` could be found, `None`
    /// is returned.
    ///
    /// If the server is ready for data transmission, `Some((ClientIoConnection,
    /// Export))` will be returned.
    pub async fn go(
        mut self,
        name: Option<&str>,
    ) -> crate::Result<Option<(ClientIoConnection<S>, Export)>> {
        let export = self
            .go_or_info(OptionRequest::Go(Self::go_request(name)))
            .await?;

        let export = match export {
            Some(export) => export,
            // Unknown export, do nothing.
            None => return Ok(None),
        };

        // Initiate data transmission with the server.
        Ok(Some((
            ClientIoConnection::new(self.conn.stream, self.conn.buffer, export.flags()),
            export,
        )))
    }

    /// Performs an Info request to fetch `Export` metadata from the server. If
    /// `name` is `None`, the server's default export is fetched. If no export
    /// matching `name` could be found, `None` is returned.
    pub async fn info(&mut self, name: Option<&str>) -> crate::Result<Option<Export>> {
        self.go_or_info(OptionRequest::Info(Self::go_request(name)))
            .await
    }

    /// Performs an Info request to fetch `Export` metadata from the server. If
    /// `name` is `None`, the server's default export is fetched. If no export
    /// matching `name` could be found, `None` is returned.
    async fn go_or_info(&mut self, option: OptionRequest) -> crate::Result<Option<Export>> {
        let options = self.options(option).await?;

        match &options[..] {
            // TODO(mdlayher): can these cases be simplified?
            [OptionResponse::Go(GoResponse::Ok { export, .. })]
            | [OptionResponse::Info(GoResponse::Ok { export, .. })] => Ok(Some(export.clone())),
            // TODO(mdlayher): display error strings? Add Error::NotFound?
            [OptionResponse::Go(GoResponse::Unknown(_))]
            | [OptionResponse::Info(GoResponse::Unknown(_))] => Ok(None),
            _ => Err("server did not send an info response server option".into()),
        }
    }

    /// Sends a raw `OptionRequest` for a known option and returns one or more
    /// raw `OptionResponse` values.
    async fn options(&mut self, option: OptionRequest) -> crate::Result<Vec<OptionResponse>> {
        self.conn
            .write_frame(HandshakeFrame::ClientOptions(ClientOptions {
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
            HandshakeFrame::ServerOptions(options) => options,
            _ => return Err("server sent invalid server options".into()),
        };

        if !server_options.unknown.is_empty() {
            let unknown = server_options.unknown;
            return Err(format!("server did not recognize options: {unknown:?}").into());
        }

        Ok(server_options.known)
    }

    /// Produces a GoRequest with optional `name` value for a non-default
    /// export.
    fn go_request(name: Option<&str>) -> GoRequest {
        // TODO(mdlayher): this feels awkward to get a String back from &str but
        // the mini-redis example does roughly the same.
        let name = name.map(|string| string.to_string());

        GoRequest {
            name,
            info_requests: vec![
                InfoType::Export,
                InfoType::Name,
                InfoType::Description,
                InfoType::BlockSize,
            ],
        }
    }
}

/// An NBD client connection which is ready to perform data transmission.
/// This type is constructed by using the [`Client`]'s `go` method.
pub struct ClientIoConnection<S> {
    stream: BufWriter<S>,
    _buffer: BytesMut,
    _flags: TransmissionFlags,
    handle: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ClientIoConnection<S> {
    /// Creates a connection ready for I/O by consuming the stream and buffer
    /// from the handshake phase.
    pub(crate) fn new(stream: BufWriter<S>, buffer: BytesMut, flags: TransmissionFlags) -> Self {
        Self {
            stream,
            _buffer: buffer,
            // TODO(mdlayher): pick apart and adjust capabilities such as for
            // read-only exports.
            _flags: flags,
            handle: 0,
        }
    }

    /// Initiates a client disconnect from the server and terminates the
    /// connection.
    pub async fn disconnect(mut self) -> crate::Result<()> {
        self.write_frame(TransmitFrame::Disconnect).await?;
        self.stream.shutdown().await?;
        Ok(())
    }

    /// Writes a single `TransmitFrame` value to the server.
    async fn write_frame(&mut self, frame: TransmitFrame<'_>) -> io::Result<()> {
        // For every write, increment the handle counter.
        //
        // TODO(mdlayher): start with a random value.
        self.handle += 1;
        let handle = &self.handle.to_be_bytes()[..];

        // TODO(mdlayher): read buffer.
        let buf = &[];
        if frame.write(&mut self.stream, handle, buf).await?.is_some() {
            // Wrote a frame, flush it now.
            self.stream.flush().await
        } else {
            Ok(())
        }
    }
}
