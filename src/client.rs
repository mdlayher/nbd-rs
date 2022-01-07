use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::handshake::connection::RawConnection;
use crate::handshake::frame::*;

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
