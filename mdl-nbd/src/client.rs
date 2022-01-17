use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream, ToSocketAddrs};

// TODO(mdlayher): try to avoid usage of *.
use crate::frame::Error;
use crate::handshake::frame::{Frame as HandshakeFrame, *};
use crate::handshake::RawConnection;
use crate::transmit::{CommandFlags, Frame as TransmitFrame, Header};
use crate::Stream;

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

impl<S: Stream> Client<S> {
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
    buffer: BytesMut,
    _flags: TransmissionFlags,
    handle: u64,
    offset: u64,
}

impl<S: Stream> ClientIoConnection<S> {
    /// Creates a connection ready for I/O by consuming the stream and buffer
    /// from the handshake phase.
    pub(crate) fn new(stream: BufWriter<S>, buffer: BytesMut, flags: TransmissionFlags) -> Self {
        Self {
            stream,
            buffer,
            // TODO(mdlayher): pick apart and adjust capabilities such as for
            // read-only exports.
            _flags: flags,
            handle: 0,
            offset: 0,
        }
    }

    /// Initiates a client disconnect from the server and terminates the
    /// connection.
    pub async fn disconnect(mut self) -> crate::Result<()> {
        self.write_frame(TransmitFrame::Disconnect).await?;
        self.stream.shutdown().await?;
        Ok(())
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> crate::Result<usize> {
        let n = self.read_at(self.offset, buf).await?;
        self.offset += n as u64;
        Ok(n)
    }

    // TODO(mdlayher): synchronous io::Read and io::Seek.

    pub async fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
        self.write_frame(TransmitFrame::ReadRequest(Header {
            flags: CommandFlags::empty(),
            offset,
            length: buf.len(),
        }))
        .await?;

        let size = self
            .read_frame_mut(buf, &|frame| {
                dbg!(frame);
            })
            .await?;

        Ok(size)
    }

    pub async fn write(&mut self, buf: &[u8]) -> crate::Result<usize> {
        let n = self.write_at(self.offset, buf).await?;
        self.offset += n as u64;
        Ok(n)
    }

    pub async fn write_at(&mut self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
        self.write_frame(TransmitFrame::WriteRequest(
            dbg!(Header {
                flags: CommandFlags::FUA,
                offset,
                length: buf.len(),
            }),
            buf,
        ))
        .await?;

        self.read_frame(&|frame| {
            dbg!(frame);
        })
        .await?;

        dbg!("write done");
        Ok(buf.len())
    }

    // TODO(mdlayher): move to transmit::ClientIoConnection or similar.

    /// Serves I/O requests on the connection until the client disconnects or an
    /// error occurs.
    async fn read_frame<F: Fn(&TransmitFrame)>(&mut self, on_frame: &F) -> crate::Result<()> {
        loop {
            if self.next_frame(on_frame).await?.is_some() {
                // Frame handled, try the next one.
                return Ok(());
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket. 0 indicates "end of stream".
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // The remote closed the connection.
                if self.buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Try to handle a single I/O operation but also terminate early with an
    /// incomplete error if we need to read more data from the stream.
    async fn next_frame<F: Fn(&TransmitFrame)>(
        &mut self,
        on_frame: &F,
    ) -> crate::Result<Option<()>> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match TransmitFrame::check(&mut buf, Some(0)) {
            Ok((mtype, io_type)) => {
                // TODO(mdlayher): we only expect simple replies. Verify this
                // magic type and consider erroring out.

                // Found a complete Frame, parse it, handle any associated I/O
                // and client responses.
                let len = buf.position() as usize;
                buf.set_position(0);

                {
                    let (req, _handle) = TransmitFrame::parse(&mut buf, mtype, io_type)?;
                    on_frame(&req);
                }

                // Now advance the buffer beyond the current cursor for the next
                // operation.
                self.buffer.advance(len);
                Ok(Some(()))
            }
            // Not enough data for an entire Frame.
            Err(Error::Incomplete) => Ok(None),
            // Failed to parse.
            Err(e) => Err(e.into()),
        }
    }

    /// Serves I/O requests on the connection until the client disconnects or an
    /// error occurs.
    async fn read_frame_mut<F: Fn(&TransmitFrame)>(
        &mut self,
        buf: &mut [u8],
        on_frame: &F,
    ) -> crate::Result<usize> {
        loop {
            if let Some(size) = self.next_frame_mut(buf, on_frame).await? {
                // Frame handled, try the next one.
                return Ok(size);
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket. 0 indicates "end of stream".
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // The remote closed the connection.
                if self.buffer.is_empty() {
                    return Ok(0);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Try to handle a single I/O operation but also terminate early with an
    /// incomplete error if we need to read more data from the stream.
    async fn next_frame_mut<F: Fn(&TransmitFrame)>(
        &mut self,
        read_buf: &mut [u8],
        on_frame: &F,
    ) -> crate::Result<Option<usize>> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match TransmitFrame::check(&mut buf, Some(read_buf.len())) {
            Ok((mtype, io_type)) => {
                // TODO(mdlayher): we only expect simple replies. Verify this
                // magic type and consider erroring out.

                // Found a complete Frame, parse it, handle any associated I/O
                // and client responses.
                let len = buf.position() as usize;
                buf.set_position(0);

                let (req, _handle) = TransmitFrame::parse(&mut buf, mtype, io_type)?;
                on_frame(&req);
                let size = buf.read_exact(read_buf).await?;

                // Now advance the buffer beyond the current cursor for the next
                // operation.
                self.buffer.advance(len);
                Ok(Some(size))
            }
            // Not enough data for an entire Frame.
            Err(Error::Incomplete) => Ok(None),
            // Failed to parse.
            Err(e) => Err(e.into()),
        }
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
