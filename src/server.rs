use bytes::BytesMut;
use log::debug;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::handshake::frame::*;
use crate::handshake::RawConnection;
use crate::transmit::{Device, RawIoConnection};

/// An NBD server which can accept incoming TCP connections and serve one or
/// more exported devices for client use.
pub struct Server {
    listener: TcpListener,
}

/// Export metadata and functions to open device handles which are managed by a
/// [`Server`].
pub struct Devices<D> {
    exports: Exports,
    devices: HashMap<String, DeviceFn<D>>,
}

/// A function which produces a handle to a device, given the name of the
/// export.
pub type DeviceFn<D> = Box<dyn Fn(&str) -> crate::Result<D> + Send + Sync>;

impl Devices<File> {
    /// Constructs a `Devices` using `path` to open a local file as the default
    /// export.
    pub fn file(path: &str) -> crate::Result<Self> {
        // TODO(mdlayher): the usual approach for file names is AsRef<Path> but
        // we need to use the string in multiple places. Look into this.

        // The file needs to exist and must be a regular file. Since the file
        // will be opened dynamically as needed later on to export to clients,
        // we'll have to repeat the same checks there.
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err("can only export regular files".into());
        }

        let readonly = metadata.permissions().readonly();

        let export = {
            // Construct the export using file metadata.
            //
            // TODO(mdlayher): block size?
            let export = Export::new(path, metadata.len()).description("file export");
            if !readonly {
                export
            } else {
                export.readonly()
            }
        };

        debug!("new export: {}", export);
        Ok(Self::new(export, Self::file_open(readonly)))
    }

    /// Produces a `DeviceFn<File>` which obeys the file's write restrictions.
    fn file_open(readonly: bool) -> DeviceFn<File> {
        Box::new(move |path| {
            // It might be possible for the file's read-only state to change
            // while the program is running, but since we've already created the
            // export and advertised it as read-only or read/write, we continue
            // to obey that decision in subsequent file opens.
            let file = OpenOptions::new().read(true).write(!readonly).open(path)?;

            let metadata = file.metadata()?;
            if metadata.is_file() {
                Ok(file)
            } else {
                Err("can only export regular files".into())
            }
        })
    }
}

impl<D: Read + Write + Seek> Devices<D> {
    /// Constructs a new `Devices` using `export` as the default export and
    /// `open` to open a device handle for I/O transmission operations.
    pub fn new(export: Export, open: DeviceFn<D>) -> Self {
        let mut devices = HashMap::with_capacity(1);
        devices.insert(export.name.clone(), open);

        Self {
            exports: Exports::new(export),
            devices,
        }
    }

    /// Adds an additional named export and `open` device handle function. The
    /// export can be queried by name.
    pub fn export(mut self, export: Export, open: DeviceFn<D>) -> Self {
        let name = export.name.clone();
        self.exports.export(export);
        self.devices.insert(name, open);
        self
    }

    /// Returns the device handle function for an export by `name` for client
    /// use.
    fn get(&self, name: &str) -> &DeviceFn<D> {
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
    pub async fn run<D: 'static + Read + Write + Seek + Send>(
        self,
        devices: Devices<D>,
    ) -> crate::Result<()> {
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
    async fn process<D: Read + Write + Seek>(
        devices: &Devices<D>,
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
        let result = {
            // The name of the export is passed as a hint.
            let mut device = devices.get(&export.name)(&export.name)?;
            conn.transmit(&mut device).await
        };
        {
            let mut locks = locks.lock().await;
            locks.remove(&export.name);
        }

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

            debug!("client: {:?}", client_options);

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

            debug!("server: {:?}", response);

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

            // Respond to known options.
            self.conn
                .write_frame(ServerOptions::from_server(response))
                .await?;

            match maybe_transmit {
                Some(Some(export)) => {
                    // Client requested Go and a valid export, prepare for
                    // I/O.
                    return Ok(Some((
                        ServerIoConnection::new(self.conn.stream, self.conn.buffer, export.flags()),
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
    flags: TransmissionFlags,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ServerIoConnection<S> {
    /// Creates a connection ready for I/O by consuming the stream and buffer
    /// from the handshake phase.
    pub(crate) fn new(stream: BufWriter<S>, buffer: BytesMut, flags: TransmissionFlags) -> Self {
        Self {
            stream,
            buffer,
            flags,
        }
    }

    /// Begins data transmission with a client using `device` as the export for
    /// I/O operations. This method blocks until the client disconnects or an
    /// unrecoverable error occurs.
    pub async fn transmit<D: Read + Write + Seek>(self, device: D) -> crate::Result<()> {
        // Infer the capabilities of the device and pass the appropriate Device
        // variant.
        let device = if self.flags.contains(TransmissionFlags::READ_ONLY) {
            Device::Read(device)
        } else {
            Device::ReadWrite(device)
        };

        RawIoConnection::new(device, self.stream, self.buffer)
            .serve()
            .await
    }
}
