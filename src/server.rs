use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::handshake::frame::*;
use crate::handshake::RawConnection;
use crate::transmit::RawIoConnection;

/// An NBD server which can accept incoming TCP connections and serve one or
/// more exported devices for client use.
pub struct Server {
    listener: TcpListener,
}

/// Export metadata and device handles which are managed by a [`Server`].
pub struct Devices {
    exports: Exports,
    devices: HashMap<String, File>,
}

impl Devices {
    /// Constructs a new `Devices` using `export` as the default export and
    /// `device` as the device for I/O transmission operations.
    pub fn new(export: Export, device: File) -> Self {
        let mut devices = HashMap::with_capacity(1);
        devices.insert(export.name.clone(), device);

        Self {
            exports: Exports::new(export),
            devices,
        }
    }

    /// Adds an additional named export and device handle which may be queried
    /// by name.
    pub fn add(&mut self, export: Export, device: File) -> &mut Self {
        let name = export.name.clone();
        self.exports.add(export);
        self.devices.insert(name, device);
        self
    }

    /// Returns the handle to an export by `name` for client use.
    async fn get(&self, name: &str) -> &File {
        // We control the list of exports so it should be impossible to fail to
        // look up an export by name when using the Server type.
        self.devices
            .get(name)
            .expect("invariant violation: name was never added to the devices map")
    }
}

impl Server {
    /// Binds a TCP listener for the NBD server at `addr` which is prepared to
    /// accept incoming connections.
    pub async fn bind<T: ToSocketAddrs>(addr: T) -> crate::Result<Self> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Server { listener })
    }

    /// Returns the local address that the `Server`'s TCP listener is bound to.
    pub fn local_addr(&self) -> crate::Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Continuously accepts and serves incoming NBD connections for `devices`.
    pub async fn run(self, devices: Devices) -> crate::Result<()> {
        let devices = Arc::new(devices);
        let locks = Arc::new(Mutex::new(HashSet::new()));

        loop {
            let (socket, addr) = self.listener.accept().await?;

            // Set TCP_NODELAY, per:
            // https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#protocol-phases.
            socket.set_nodelay(true)?;

            let devices = devices.clone();
            let locks = locks.clone();
            tokio::spawn(async move {
                let conn = ServerConnection::new(socket);

                if let Err(err) = Self::process(&devices, &locks, conn).await {
                    // TODO(mdlayher): error reporting through another channel.
                    println!("{:?}: error: {:?}", addr, err);
                }
            });
        }
    }

    /// Processes a single incoming NBD client connection.
    async fn process(
        devices: &Devices,
        locks: &Mutex<HashSet<String>>,
        conn: ServerConnection<TcpStream>,
    ) -> crate::Result<()> {
        let (conn, export) = {
            let mut locks = locks.lock().await;

            let (conn, export) = match conn.handshake(&devices.exports, &locks).await? {
                Some(conn) => conn,
                // Client didn't wish to initiate data transmission, do nothing.
                None => return Ok(()),
            };

            // Going to start transmission, lock this export.
            locks.insert(export.name.clone());
            (conn, export)
        };

        // Client wants to begin data transmission using this device. Once
        // transmission completes, drop the lock on the export.
        let mut file = devices.get(&export.name).await;
        let result = conn.transmit(&mut file).await;

        let mut locks = locks.lock().await;
        locks.remove(&export.name);
        result
    }
}

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
    /// Transmission will also be refused if the name of an export is already
    /// present in `locks`, due to the export being in use by another client.
    ///
    /// If the client is ready for data transmission, `Some((ServerIoConnection,
    /// Export))` will be returned so data transmission can begin using the
    /// client's chosen export.
    pub async fn handshake(
        mut self,
        exports: &Exports,
        locks: &HashSet<String>,
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
                .map(|request| OptionResponse::from_request(request, exports, locks))
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
/// This type is constructed by using the [`ServerConnection`]'s `handshake`
/// method.
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
