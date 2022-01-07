use bytes::BytesMut;
use std::io::{Read, Seek, Write};
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};

use crate::handshake::connection::RawConnection;
use crate::handshake::frame::*;
use crate::transmit::connection::RawIoConnection;

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
        RawIoConnection::new(device, self.stream, self.buffer, self.readonly)
            .serve()
            .await
    }
}
