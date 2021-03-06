use bitflags::bitflags;
use bytes::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::cmp::max;
use std::collections::HashSet;
use std::fmt;
use std::io::{self, Cursor, Write};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::consts::*;
use crate::frame::*;

/// Information about a Network Block Device export.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Export {
    pub name: String,
    pub description: Option<String>,
    pub size: u64,
    pub block_sizes: [u32; 3],
    _flags: TransmissionFlags,
}

impl Export {
    /// Creates a new read/write `Export` with the given name and device size.
    /// Use the `readonly` method to produce a read-only `Export`.
    pub fn new(name: &str, size: u64) -> Self {
        // The largest possible block size.
        const MAX_INFINITE: u32 = 0xffffffff;

        Self {
            name: name.to_string(),
            description: None,
            size,
            // Default values as recommended by the NBD protocol specification
            // for regular files. Notably for block devices, the minimum and
            // preferred values should likely be modified.
            block_sizes: [1, 4096, MAX_INFINITE],

            // Force the use of our default flags in this library to prevent
            // weird misconfigurations.
            _flags: TransmissionFlags::default(),
        }
    }

    /// Sets non-default block sizes for the `Export`. The array values are the
    /// minimum, preferred, and maximum block size, respectively.
    pub fn block_size(mut self, block_sizes: [u32; 3]) -> Self {
        // TODO(mdlayher): validation of sizes in relation to each other
        // according to the rules in the spec.
        self.block_sizes = block_sizes;
        self
    }

    /// Sets a human-readable description for the `Export`.
    pub fn description(mut self, description: &str) -> Self {
        // Noop for empty strings.
        if !description.is_empty() {
            self.description = Some(description.to_string());
        }

        self
    }

    /// Produces a read-only `Export`. Write commands are disabled during data
    /// transmission.
    pub fn readonly(mut self) -> Self {
        self._flags.set(TransmissionFlags::READ_ONLY, true);
        self
    }

    /// Enables the use of the Flush command on this `Export`. This option
    /// should be set when possible for read/write exports to enable durable
    /// sync of data to disks. However, if the export data is completely stored
    /// in memory, there is no need for this option.
    pub fn flush(mut self) -> Self {
        self._flags.set(
            TransmissionFlags::SEND_FLUSH | TransmissionFlags::SEND_FUA,
            true,
        );
        self
    }

    /// Indicates an `Export` is backed by rotational media, such as a
    /// conventional hard drive. This can be used as a hint for clients to
    /// optimize their access patterns.
    pub fn rotational(mut self) -> Self {
        self._flags.set(TransmissionFlags::ROTATIONAL, true);
        self
    }

    /// Enables the use of the TRIM command on this `Export`. This option should
    /// be set when possible for read/write exports backed by solid-state media,
    /// so unused bytes can be efficiently discarded by the disk.
    pub fn trim(mut self) -> Self {
        self._flags.set(TransmissionFlags::SEND_TRIM, true);
        self
    }

    /// Returns the set of `TransmissionFlags` applied to this `Export`.
    pub fn flags(&self) -> TransmissionFlags {
        self._flags
    }

    /// Produces a human-readable string of all of the `Export`'s metadata, with
    /// exception of its name.
    fn metadata_string(&self) -> String {
        // Apply prefix data for better metadata listing.
        let description = match &self.description {
            Some(s) => format!("{s} "),
            None => "".to_string(),
        };

        let readonly = if self._flags.contains(TransmissionFlags::READ_ONLY) {
            "[read-only] "
        } else {
            ""
        };

        // TODO(mdlayher): this bytes to MiB calculation is good enough
        // for now but probably not very robust.
        let size = self.size / MiB;

        let [min, pref, max] = self.block_sizes;

        format!("{description}{readonly}(size: {size}MiB, block sizes: {min}/{pref}/{max}B)")
    }
}

impl fmt::Display for Export {
    /// Produces a human-readable description of an `Export`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = &self.name;
        let meta = self.metadata_string();
        write!(f, "{name}: {meta}")
    }
}

/// One or more [`Export`]s which can be exposed via the NBD server handshake.
#[derive(Debug, PartialEq)]
pub(crate) struct Exports {
    // The default export.
    export: Export,
    // Other exports.
    exports: Vec<Export>,
}

impl Exports {
    /// Serves a single `Export` as the default export.
    pub(crate) fn new(export: Export) -> Self {
        Self {
            export,
            exports: vec![],
        }
    }

    /// Adds an additional non-default `Export` which may be queried by name.
    pub(crate) fn export(&mut self, export: Export) -> &mut Self {
        self.exports.push(export);
        self
    }
}

impl From<&Exports> for Vec<ListExport> {
    /// Flattens `Exports` into a format suitable for `Vec<ListExport>`.
    fn from(src: &Exports) -> Self {
        let mut list = Vec::with_capacity(1 + src.exports.len());
        list.push(src.export.clone().into());
        list.extend(src.exports.iter().map(|export| export.clone().into()));
        list
    }
}

// TODO(mdlayher): make private later if possible.

/// Denotes the expected type of a `Frame` without knowledge of its associated
/// data.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum FrameType {
    ClientOptions,
    ServerHandshake,
    ServerOptions,
    ServerUnsupportedOptions,
}

/// An NBD handshake data frame sent between client and server. Note that the
/// frame types here do not necessarily correspond to the NBD specification, but
/// are used to chunk up logical operations in this library.
#[derive(Debug, PartialEq)]
pub(crate) enum Frame {
    ClientOptions(ClientOptions),
    ServerHandshake(HandshakeFlags),
    ServerOptions(ServerOptions),
    ServerOptionsAbort,
    ServerUnsupportedOptions(Vec<u32>),
}

bitflags! {
    /// Valid bitflags for a server handshake.
    pub(crate) struct HandshakeFlags: u16 {
        const FIXED_NEWSTYLE = NBD_FLAG_FIXED_NEWSTYLE;
        const NO_ZEROES      = NBD_FLAG_NO_ZEROES;
    }

    /// Valid bitflags for a client handshake.
    pub(crate) struct ClientFlags: u32 {
        const FIXED_NEWSTYLE = NBD_FLAG_C_FIXED_NEWSTYLE;
        const NO_ZEROES      = NBD_FLAG_C_NO_ZEROES;
    }

    /// Valid bitflags for data transmission negotiation.
    pub struct TransmissionFlags: u16 {
        const HAS_FLAGS  = NBD_FLAG_HAS_FLAGS;
        const READ_ONLY  = NBD_FLAG_READ_ONLY;
        const SEND_FLUSH = NBD_FLAG_SEND_FLUSH;
        const SEND_FUA   = NBD_FLAG_SEND_FUA;
        const ROTATIONAL = NBD_FLAG_ROTATIONAL;
        const SEND_TRIM  = NBD_FLAG_SEND_TRIM;
    }
}

impl Default for TransmissionFlags {
    /// Returns the default transmission flags this server supports for all
    /// exports regardless of client configuration.
    fn default() -> Self {
        TransmissionFlags::HAS_FLAGS
    }
}

/// Options sent by the client which are parsed as either known or unknown
/// depending on the server's capabilities.
#[derive(Debug, PartialEq)]
pub(crate) struct ClientOptions {
    pub(crate) flags: ClientFlags,
    pub(crate) known: Vec<OptionRequest>,
    // Set only when parsing.
    pub(crate) unknown: Vec<u32>,
}

/// Options sent by the server which are parsed as either known or unknown
/// depending on the client's capabilities.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ServerOptions {
    pub(crate) known: Vec<OptionResponse>,
    // Set only when parsing.
    pub(crate) unknown: Vec<u32>,
}

impl ServerOptions {
    /// Produces a `Frame::ServerOptions` with the known options field set.
    pub(crate) fn from_server(known: Vec<OptionResponse>) -> Frame {
        Frame::ServerOptions(Self {
            known,
            unknown: Vec::new(),
        })
    }
}

/// Denotes the type of known options which can be handled by the server.
#[repr(u32)]
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub(crate) enum OptionCode {
    Abort = NBD_OPT_ABORT,
    Go = NBD_OPT_GO,
    Info = NBD_OPT_INFO,
    List = NBD_OPT_LIST,
}

/// The contents of known options which a client can send to a server.
#[derive(Debug, PartialEq)]
pub(crate) enum OptionRequest {
    Abort,
    // Go and Info ask for the same information, but Go causes data transmission
    // to immediately begin. The only other way to enter transmission after Info
    // would be to send NBD_OPT_EXPORT_NAME, which we may never support.
    Go(GoRequest),
    Info(GoRequest),
    List,
}

/// The contents of known options which a server can respond to on behalf of a
/// client.
#[derive(Debug, PartialEq)]
pub(crate) enum OptionResponse {
    Abort,
    Go(GoResponse),
    Info(GoResponse),
    List(ListResponse),
}

impl OptionRequest {
    /// Produces a `GoRequest` from `src` after the client option header has
    /// been consumed by `next_option` with the given `frame_type`. This can
    /// then be associated with `OptionRequest::Go` or `OptionRequest::Info` as
    /// necessary.
    fn go(src: &mut Cursor<&[u8]>, frame_type: FrameType) -> Result<GoRequest> {
        // Name may or may not be present.
        let name_length = get_u32(src)? as usize;
        let name = match name_length {
            0 => None,
            _ => {
                let mut name_buf = vec![0u8; name_length];
                get_exact(src, &mut name_buf)?;
                Some(
                    String::from_utf8(name_buf)
                        .map_err(|_err| Error::HandshakeProtocol(frame_type))?,
                )
            }
        };

        let num_infos = get_u16(src)? as usize;

        // Allocate enough space for the single export info (in the case of no
        // requested options) or enough for each requested option.
        let mut info_requests = Vec::with_capacity(max(1, num_infos));
        for _ in 0..num_infos {
            let raw = get_u16(src)?;
            let info = FromPrimitive::from_u16(raw).ok_or(Error::HandshakeProtocol(frame_type))?;

            info_requests.push(info);
        }

        if info_requests.is_empty() {
            // If clients don't request any options (nbd-client as of December
            // 2021), we always send export information anyway.
            info_requests.push(InfoType::Export);
        }

        // Never return empty info_requests.
        assert!(
            !info_requests.is_empty(),
            "info_requests must contain at least one element"
        );

        Ok(GoRequest {
            name,
            info_requests,
        })
    }

    /// Returns the associated `OptionCode` for `self`.
    fn code(&self) -> OptionCode {
        match self {
            Self::Abort => OptionCode::Abort,
            Self::Go(..) => OptionCode::Go,
            Self::Info(..) => OptionCode::Info,
            Self::List => OptionCode::List,
        }
    }
}

impl OptionResponse {
    /// Converts an `OptionRequest` into the matching `OptionResponse` while
    /// also associating additional data such as the `Exports` being served.
    pub(crate) fn from_request(
        src: OptionRequest,
        exports: &Exports,
        locks: &HashSet<String>,
    ) -> Self {
        match src {
            OptionRequest::Abort => Self::Abort,
            // TODO(mdlayher): tidy up locks.
            OptionRequest::Go(req) => Self::Go(Self::go(req, GoOrInfo::Go, exports, locks)),
            OptionRequest::Info(req) => Self::Info(Self::go(req, GoOrInfo::Info, exports, locks)),
            OptionRequest::List => Self::List(ListResponse(exports.into())),
        }
    }

    /// Produces the appropriate `GoResponse` for a `GoRequest` which may ask
    /// for a specified `Export`.
    fn go(
        req: GoRequest,
        code: GoOrInfo,
        exports: &Exports,
        locks: &HashSet<String>,
    ) -> GoResponse {
        let export = match req.name {
            Some(name) if name == exports.export.name => {
                // Name matches the default.
                &exports.export
            }
            // No name, use the default.
            None => &exports.export,
            Some(name) => {
                // Name must match one of the extra exports or we return unknown
                // export to the client.
                let matched: Vec<&Export> = exports
                    .exports
                    .iter()
                    .filter(|export| export.name == name)
                    .collect();

                match matched[..] {
                    [export] => export,
                    _ => return GoResponse::Unknown(format!("export not found: {name}")),
                }
            }
        };

        // If another client is already tranmsmitting with a given export and
        // the client sent a Go request, do not allow more connections.
        if matches!(code, GoOrInfo::Go) && locks.contains(&export.name) {
            let name = &export.name;
            GoResponse::Unknown(format!("export is locked by another client: {name}"))
        } else {
            GoResponse::Ok {
                info_requests: req.info_requests,
                export: export.clone(),
            }
        }
    }

    /// Returns the associated `OptionCode` for `self`.
    fn code(&self) -> OptionCode {
        match self {
            Self::Abort => OptionCode::Abort,
            Self::Go(..) => OptionCode::Go,
            Self::Info(..) => OptionCode::Info,
            Self::List(..) => OptionCode::List,
        }
    }
}

/// A Go option as sent by a client.
#[derive(Debug, PartialEq)]
pub(crate) struct GoRequest {
    pub(crate) name: Option<String>,
    pub(crate) info_requests: Vec<InfoType>,
}

/// A Go option as sent by a server in response to a client.
#[derive(Debug, PartialEq)]
pub(crate) enum GoResponse {
    Ok {
        info_requests: Vec<InfoType>,
        export: Export,
    },
    Unknown(String),
}

/// Denotes the type of an information request from a client.
#[repr(u16)]
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub(crate) enum InfoType {
    Export = NBD_INFO_EXPORT,
    Name = NBD_INFO_NAME,
    Description = NBD_INFO_DESCRIPTION,
    BlockSize = NBD_INFO_BLOCK_SIZE,
}

impl GoRequest {
    /// Writes the request bytes for a `GoRequest` to `dst`.
    fn write(&self, dst: &mut Vec<u8>) -> io::Result<()> {
        if let Some(name) = &self.name {
            // A name is present, write its length and the bytes if any exist.
            let length = name.len() as u32;
            Write::write_all(dst, &length.to_be_bytes())?;
            if length > 0 {
                Write::write_all(dst, name.as_bytes())?;
            }
        } else {
            Write::write_all(dst, &0u32.to_be_bytes())?;
        };

        Write::write_all(dst, &(self.info_requests.len() as u16).to_be_bytes())?;
        for info_request in &self.info_requests {
            Write::write_all(dst, &(*info_request as u16).to_be_bytes())?;
        }

        Ok(())
    }
}

/// Used to differentiate between Go and Info when necessary, since the two
/// carry identical frame data.
#[repr(u32)]
#[derive(Clone, Copy, Debug)]
enum GoOrInfo {
    Go = OptionCode::Go as u32,
    Info = OptionCode::Info as u32,
}

impl GoResponse {
    /// Writes the bytes for a `GoResponse` to `dst` with the specified `code`,
    async fn write<S: AsyncWrite + Unpin>(&self, dst: &mut S, code: GoOrInfo) -> io::Result<()> {
        match self {
            Self::Ok {
                info_requests,
                export,
            } => {
                for info in info_requests {
                    dst.write_u64(REPLYMAGIC).await?;
                    dst.write_u32(code as u32).await?;

                    match *info {
                        InfoType::Export => {
                            // Fixed size of 10 bytes for export info.
                            dst.write_u32(NBD_REP_INFO).await?;
                            dst.write_u32(2 + 8 + 2).await?;

                            // Export info type, export size, flags.
                            dst.write_u16(*info as u16).await?;
                            dst.write_u64(export.size).await?;
                            dst.write_u16(export._flags.bits()).await?;
                        }
                        InfoType::Name => Self::write_string(dst, *info, &export.name).await?,
                        InfoType::Description => {
                            // Write an empty description anyway rather than
                            // dealing with error logic.
                            let string = match &export.description {
                                Some(s) => s,
                                None => "",
                            };

                            Self::write_string(dst, *info, string).await?
                        }
                        InfoType::BlockSize => {
                            // Fixed size of 14 bytes for block size.
                            dst.write_u32(NBD_REP_INFO).await?;
                            dst.write_u32(2 + (4 * 3)).await?;
                            dst.write_u16(*info as u16).await?;

                            // TODO(mdlayher): break out
                            // minimum/preferred/maximum into export fields.
                            for bs in export.block_sizes {
                                dst.write_u32(bs).await?;
                            }
                        }
                    }
                }

                let option = match code {
                    GoOrInfo::Go => OptionCode::Go,
                    GoOrInfo::Info => OptionCode::Info,
                };

                Frame::ack(dst, option).await?;
            }
            Self::Unknown(message) => {
                dst.write_u64(REPLYMAGIC).await?;
                dst.write_u32(code as u32).await?;

                // Unknown export, return an error message.
                dst.write_u32(NBD_REP_ERR_UNKNOWN).await?;

                let length = message.len() as u32;

                dst.write_u32(length).await?;
                dst.write_all(message.as_bytes()).await?;
            }
        };

        Ok(())
    }

    /// Writes a string and its associated `InfoType` to `dst`.
    async fn write_string<S: AsyncWrite + Unpin>(
        dst: &mut S,
        info_type: InfoType,
        string: &str,
    ) -> io::Result<()> {
        dst.write_u32(NBD_REP_INFO).await?;

        // Add two bytes for length field.
        let length = string.len() as u32 + 2;

        dst.write_u32(length).await?;
        dst.write_u16(info_type as u16).await?;
        dst.write_all(string.as_bytes()).await?;

        Ok(())
    }
}

/// Information returned by the `ListResponse` type.
#[derive(Debug, PartialEq)]
pub(crate) struct ListExport {
    name: String,
    metadata: String,
}

impl From<Export> for ListExport {
    /// Converts an `Export` into a `ListExport` by packing fields in a
    /// structured way into metadata.
    fn from(src: Export) -> ListExport {
        let metadata = src.metadata_string();
        ListExport {
            name: src.name,
            metadata,
        }
    }
}

/// A List option as sent by a server in response to a client, containing data
/// about each `ListExport` this server can serve.
#[derive(Debug, PartialEq)]
pub(crate) struct ListResponse(pub(crate) Vec<ListExport>);

impl ListResponse {
    /// Writes the bytes describing the exports to `dst`.
    async fn write<S: AsyncWrite + Unpin>(&self, dst: &mut S) -> io::Result<()> {
        // Each export reply is prefixed with magic and the List code.
        for export in &self.0 {
            dst.write_u64(REPLYMAGIC).await?;
            dst.write_u32(OptionCode::List as u32).await?;

            dst.write_u32(NBD_REP_SERVER).await?;

            let name_length = export.name.len() as u32;
            let meta_length = export.metadata.len() as u32;

            // Extra bytes for the name string's length. Clients interpret bytes
            // beyond this length as metadata
            dst.write_u32(name_length + meta_length + 4).await?;
            dst.write_u32(name_length).await?;

            dst.write_all(export.name.as_bytes()).await?;
            dst.write_all(export.metadata.as_bytes()).await?;
        }

        Frame::ack(dst, OptionCode::List).await?;
        Ok(())
    }
}

impl Frame {
    /// Determines if enough data is available to parse a `Frame` of the given
    /// `FrameType` from `src`.
    pub(crate) fn check(src: &mut Cursor<&[u8]>, frame_type: FrameType) -> Result<()> {
        match frame_type {
            FrameType::ClientOptions => {
                // flags u32
                get_u32(src)?;

                while src.has_remaining() {
                    next_client_option(src, frame_type)?;
                }

                Ok(())
            }
            FrameType::ServerHandshake => {
                // NBDMAGIC u64 + IHAVEOPT u64 + flags u16
                get_u64(src)?;
                get_u64(src)?;
                get_u16(src)?;
                Ok(())
            }
            FrameType::ServerOptions => {
                if src.remaining() == 0 {
                    return Err(Error::Incomplete);
                }

                while src.has_remaining() {
                    next_server_option(src, frame_type)?;
                }

                Ok(())
            }
            FrameType::ServerUnsupportedOptions => {
                if src.remaining() == 0 {
                    return Err(Error::Incomplete);
                }

                while src.has_remaining() {
                    // REPLYMAGIC u64, option u32 + unsupported error u32
                    get_u64(src)?;
                    get_u64(src)?;

                    // Read error message length and then skip the text bytes.
                    let length = get_u32(src)? as usize;
                    skip(src, length)?;
                }

                Ok(())
            }
        }
    }

    /// Parses the next `Frame` according to the given `FrameType`.
    pub(crate) fn parse(src: &mut Cursor<&[u8]>, frame_type: FrameType) -> Result<Frame> {
        match frame_type {
            FrameType::ClientOptions => {
                let flags = ClientFlags::from_bits(get_u32(src)?)
                    .ok_or(Error::HandshakeProtocol(frame_type))?;

                let mut known = Vec::new();
                let mut unknown = Vec::new();
                while src.has_remaining() {
                    // Keep track of both known and unknown options so we can
                    // report errors to the client accordingly.
                    match next_client_option(src, frame_type)? {
                        ParsedRequest::Known(option) => known.push(option),
                        ParsedRequest::Unknown(code) => unknown.push(code),
                    }
                }

                Ok(Frame::ClientOptions(ClientOptions {
                    flags,
                    known,
                    unknown,
                }))
            }
            FrameType::ServerHandshake => {
                if get_u64(src)? != NBDMAGIC {
                    return Err(Error::HandshakeProtocol(frame_type));
                }
                if get_u64(src)? != IHAVEOPT {
                    return Err(Error::HandshakeProtocol(frame_type));
                }

                let flags = HandshakeFlags::from_bits(get_u16(src)?)
                    .ok_or(Error::HandshakeProtocol(frame_type))?;

                Ok(Frame::ServerHandshake(flags))
            }
            FrameType::ServerOptions => {
                let mut known = Vec::new();
                let mut unknown = Vec::new();
                while src.has_remaining() {
                    // Keep track of both known and unknown options so we can
                    // report errors to the client accordingly.
                    match next_server_option(src, frame_type)? {
                        ParsedResponse::Known(option) => known.push(option),
                        ParsedResponse::Unknown(codes) => unknown.extend(codes),
                    }
                }

                Ok(Frame::ServerOptions(ServerOptions { known, unknown }))
            }
            FrameType::ServerUnsupportedOptions => {
                let mut options = Vec::new();
                while src.has_remaining() {
                    if get_u64(src)? != REPLYMAGIC {
                        return Err(Error::HandshakeProtocol(frame_type));
                    }

                    options.push(get_u32(src)?);
                    if get_u32(src)? != NBD_REP_ERR_UNSUP {
                        return Err(Error::HandshakeProtocol(frame_type));
                    }

                    // Read error message length and then skip the textual
                    // bytes for now.
                    //
                    // TODO(mdlayher): figure out a way to expose this as part
                    // of the enum that makes sense for both client and server.
                    let length = get_u32(src)? as usize;
                    skip(src, length)?;
                }

                Ok(Frame::ServerUnsupportedOptions(options))
            }
        }
    }

    /// Writes the current `Frame` out to `dst`. It returns `Some(())` if any
    /// bytes were written to the stream or `None` if not.
    pub(crate) async fn write<S: AsyncWrite + Unpin>(&self, dst: &mut S) -> io::Result<Option<()>> {
        match self {
            Frame::ClientOptions(options) => {
                // When we write a Frame, it doesn't makes sense to provide
                // options. These are only set when parsing a Frame.
                //
                // TODO(mdlayher): use type system to enforce this invariant.
                assert!(
                    options.unknown.is_empty(),
                    "unknown ClientOptions must be empty for write"
                );

                dst.write_u32(options.flags.bits()).await?;

                // Write each option's header and code.
                let options = &options.known;
                for option in options {
                    dst.write_u64(IHAVEOPT).await?;
                    dst.write_u32(option.code() as u32).await?;

                    // Write each option to a vector first so we can compute its
                    // length and prepend that to the vector's bytes in the
                    // stream.
                    let mut buf = vec![];
                    match option {
                        OptionRequest::Go(req) | OptionRequest::Info(req) => req.write(&mut buf)?,
                        // Noop, already wrote the code and body is empty.
                        OptionRequest::Abort | OptionRequest::List => {}
                    };

                    let length = buf.len() as u32;
                    dst.write_u32(buf.len() as u32).await?;
                    if length > 0 {
                        dst.write_all(&buf).await?;
                    }
                }
            }
            Frame::ServerHandshake(flags) => {
                // Opening handshake and server flags.
                dst.write_u64(NBDMAGIC).await?;
                dst.write_u64(IHAVEOPT).await?;
                dst.write_u16(flags.bits()).await?;
            }
            Frame::ServerOptionsAbort => {
                // Abort writes a simple acknowledgement and nothing more.
                Self::ack(dst, OptionCode::Abort).await?;
            }
            Frame::ServerOptions(options) => {
                // When we write a Frame, it doesn't makes sense to provide
                // options. These are only set when parsing a Frame.
                //
                // TODO(mdlayher): use types to enforce this invariant.
                assert!(
                    options.unknown.is_empty(),
                    "unknown ServerOptions must be empty for write"
                );

                let options = &options.known;
                if options.is_empty() {
                    // Noop, nothing to write.
                    return Ok(None);
                }

                // Iterate through each option and write its bytes to the
                // stream.
                for option in options {
                    match option {
                        OptionResponse::Abort => {
                            // Noop, just acknowledge. It's unlikely this would
                            // be called due to the existence of
                            // Frame::ServerOptionsAbort.
                            Self::ack(dst, option.code()).await?;
                        }
                        OptionResponse::Go(res) => res.write(dst, GoOrInfo::Go).await?,
                        OptionResponse::Info(res) => res.write(dst, GoOrInfo::Info).await?,
                        OptionResponse::List(res) => res.write(dst).await?,
                    }
                }
            }
            Frame::ServerUnsupportedOptions(options) => {
                if options.is_empty() {
                    // Noop, nothing to write.
                    return Ok(None);
                }

                // These options are unsupported, return a textual error and the
                // unsupported error code.
                for option in options {
                    dst.write_u64(REPLYMAGIC).await?;
                    dst.write_u32(*option).await?;

                    let error = format!("unsupported option: {option}");

                    dst.write_u32(NBD_REP_ERR_UNSUP).await?;
                    dst.write_u32(error.len() as u32).await?;
                    dst.write_all(error.as_bytes()).await?;
                }
            }
        }

        // Wrote some data.
        Ok(Some(()))
    }

    /// Writes an acknowledgement for `code to `dst`.
    async fn ack<S: AsyncWrite + Unpin>(dst: &mut S, code: OptionCode) -> io::Result<()> {
        dst.write_u64(REPLYMAGIC).await?;
        dst.write_u32(code as u32).await?;
        dst.write_u32(NBD_REP_ACK).await?;
        dst.write_u32(0).await?;

        Ok(())
    }

    #[cfg(test)]
    /// Converts a Frame to its associated FrameType.
    fn to_type(&self) -> FrameType {
        match self {
            Self::ClientOptions(..) => FrameType::ClientOptions,
            Self::ServerHandshake(..) => FrameType::ServerHandshake,
            Self::ServerOptionsAbort | Self::ServerOptions(..) => FrameType::ServerOptions,
            Self::ServerUnsupportedOptions(..) => FrameType::ServerUnsupportedOptions,
        }
    }
}

/// Denotes known or unknown request options sent by a client.
#[derive(Debug)]
enum ParsedRequest {
    Known(OptionRequest),
    Unknown(u32),
}

/// Produces the next `ParseRequest` value from `src` by consuming the client
/// option header and inner data for a given `frame_type`.
fn next_client_option(src: &mut Cursor<&[u8]>, frame_type: FrameType) -> Result<ParsedRequest> {
    if get_u64(src)? != IHAVEOPT {
        return Err(Error::HandshakeProtocol(frame_type));
    }

    let option_code = get_u32(src)?;
    let length = get_u32(src)? as usize;

    Ok(match FromPrimitive::from_u32(option_code) {
        Some(option) => ParsedRequest::Known(match option {
            OptionCode::Go => OptionRequest::Go(OptionRequest::go(src, frame_type)?),
            OptionCode::Info => OptionRequest::Info(OptionRequest::go(src, frame_type)?),
            // These options have no body, no need to advance src.
            OptionCode::Abort => OptionRequest::Abort,
            OptionCode::List => OptionRequest::List,
        }),
        None => {
            // We aren't aware of this option, skip over it but note its code as
            // unknown for error reporting.
            skip(src, length)?;
            ParsedRequest::Unknown(option_code)
        }
    })
}

/// Denotes known or unknown request options sent by a server.
#[derive(Debug)]
enum ParsedResponse {
    Known(OptionResponse),
    Unknown(Vec<u32>),
}

/// Produces the next `ParseResponse` value from `src` by consuming the server
/// option header and inner data for a given `frame_type`.
fn next_server_option(src: &mut Cursor<&[u8]>, frame_type: FrameType) -> Result<ParsedResponse> {
    // Options are fragmented into smaller messages than the Frame API we
    // present. Parse the fragments and then assemble them after we are done.
    let mut fragments = Vec::new();
    let mut parser = OptionFragmentParser::new(src, frame_type);
    while let Some(option) = parser.next()? {
        fragments.push(option);
    }

    match &fragments[..] {
        [OptionFragment::Abort] => Ok(ParsedResponse::Known(OptionResponse::Abort)),
        [.., OptionFragment::ListDone] => {
            let mut exports = Vec::with_capacity(fragments.len() - 1);
            for fragment in fragments {
                match fragment {
                    OptionFragment::List(export) => exports.push(export),
                    OptionFragment::ListDone => {}
                    _ => return Err(Error::HandshakeProtocol(frame_type)),
                }
            }

            Ok(ParsedResponse::Known(OptionResponse::List(ListResponse(
                exports,
            ))))
        }
        // Go and Info share the same code but return differing final fragments.
        [.., OptionFragment::GoDone] => {
            Ok(ParsedResponse::go(GoOrInfo::Go, fragments, frame_type)?)
        }
        [.., OptionFragment::InfoDone] => {
            Ok(ParsedResponse::go(GoOrInfo::Info, fragments, frame_type)?)
        }
        // Go and Info, but error cases.
        [OptionFragment::GoError(name)] => Ok(ParsedResponse::Known(OptionResponse::Go(
            GoResponse::Unknown(name.to_string()),
        ))),
        [OptionFragment::InfoError(name)] => Ok(ParsedResponse::Known(OptionResponse::Info(
            GoResponse::Unknown(name.to_string()),
        ))),
        // One or more unknown fragments.
        [OptionFragment::Unknown(_)] | [.., OptionFragment::Unknown(_)] => {
            let mut codes = Vec::new();
            for fragment in fragments {
                if let OptionFragment::Unknown(code) = fragment {
                    codes.push(code);
                }
            }

            Ok(ParsedResponse::Unknown(codes))
        }
        // No idea, bail out.
        _ => Err(Error::HandshakeProtocol(frame_type)),
    }
}

impl ParsedResponse {
    /// Produces a `ParsedResponse` containing `Go` or `Info` from fragments
    /// depending on the input option.
    fn go(
        option: GoOrInfo,
        fragments: Vec<OptionFragment>,
        frame_type: FrameType,
    ) -> Result<ParsedResponse> {
        // Last fragment is just an end indicator, skip it.
        let mut info_requests = Vec::with_capacity(fragments.len() - 1);
        let mut export = Export::default();

        for fragment in fragments {
            match fragment {
                OptionFragment::Go(GoFragment::Export(size, flags)) => {
                    info_requests.push(InfoType::Export);
                    export.size = size;
                    export._flags = flags;
                }
                OptionFragment::Go(GoFragment::Name(name)) => {
                    info_requests.push(InfoType::Name);
                    export.name = name;
                }
                OptionFragment::Go(GoFragment::Description(description)) => {
                    info_requests.push(InfoType::Description);
                    export.description = description;
                }
                OptionFragment::Go(GoFragment::BlockSizes(block_sizes)) => {
                    // TODO(mdlayher): expose min/max as well.
                    info_requests.push(InfoType::BlockSize);
                    export.block_sizes = block_sizes;
                }
                OptionFragment::GoDone | OptionFragment::InfoDone => {}
                _ => return Err(Error::HandshakeProtocol(frame_type)),
            };
        }

        Ok(ParsedResponse::Known(match option {
            GoOrInfo::Go => OptionResponse::Go(GoResponse::Ok {
                info_requests,
                export,
            }),
            GoOrInfo::Info => OptionResponse::Info(GoResponse::Ok {
                info_requests,
                export,
            }),
        }))
    }
}

/// Parses OptionFragment values from a cursor until no more remain.
struct OptionFragmentParser<'a, 'b> {
    src: &'a mut Cursor<&'b [u8]>,
    frame_type: FrameType,

    done: bool,
}

/// A fragment of an option which can later be assembled into a `ParsedOption`.
#[derive(Debug)]
enum OptionFragment {
    Abort,
    Go(GoFragment),
    GoDone,
    GoError(String),
    InfoDone,
    InfoError(String),
    List(ListExport),
    ListDone,
    Unknown(u32),
}

/// A fragment of a `ParsedOption::Go` or `ParsedOption::Info`.
#[derive(Debug)]
enum GoFragment {
    Export(u64, TransmissionFlags),
    Name(String),
    Description(Option<String>),
    BlockSizes([u32; 3]),
}

impl<'a, 'b> OptionFragmentParser<'a, 'b> {
    /// Creates a new `OptionFragmentParser` ready for use.
    fn new(src: &'a mut Cursor<&'b [u8]>, frame_type: FrameType) -> Self {
        Self {
            src,
            frame_type,
            done: false,
        }
    }

    /// Iterates and produces `Some(OptionFragment)` values until no more
    /// remain, at which point `next` returns `None`.
    fn next(&mut self) -> Result<Option<OptionFragment>> {
        if self.done {
            // Previously ran into a final acknowledgement.
            return Ok(None);
        }

        // New server option.
        if get_u64(self.src)? != REPLYMAGIC {
            return Err(Error::HandshakeProtocol(self.frame_type));
        }

        let option_code = get_u32(self.src)?;
        let reply_code = get_u32(self.src)?;
        let length = get_u32(self.src)? as usize;

        if reply_code == NBD_REP_ACK && length == 0 {
            // After parsing this fragment, we'll be done.
            self.done = true;
        }

        Ok(Some(match FromPrimitive::from_u32(option_code) {
            Some(option) => self.option_fragment(option, reply_code, length)?,
            None => {
                // We aren't aware of this option, skip over it but note its
                // code as unknown for error reporting.
                skip(self.src, length)?;
                OptionFragment::Unknown(option_code)
            }
        }))
    }

    /// Produces the next `OptionFragment` based on the specified `code.`
    fn option_fragment(
        &mut self,
        option: OptionCode,
        reply_code: u32,
        length: usize,
    ) -> Result<OptionFragment> {
        match option {
            OptionCode::Abort => {
                if self.done {
                    Ok(OptionFragment::Abort)
                } else {
                    Err(Error::HandshakeProtocol(self.frame_type))
                }
            }
            OptionCode::Go => self.go(GoOrInfo::Go, reply_code, length),
            OptionCode::Info => self.go(GoOrInfo::Info, reply_code, length),
            OptionCode::List => {
                if self.done {
                    return Ok(OptionFragment::ListDone);
                }

                if reply_code != NBD_REP_SERVER {
                    return Err(Error::HandshakeProtocol(self.frame_type));
                }

                // Name length is followed by name, then any bytes after that
                // free-form metadata.
                let name_length = get_u32(self.src)? as usize;
                let meta_length = length - 4 - name_length;

                let name = self.read_string(name_length)?;
                let metadata = self.read_string(meta_length)?;

                Ok(OptionFragment::List(ListExport { name, metadata }))
            }
        }
    }

    /// Produces an `OptionFragment` related to a `Go` or `Info` option,
    /// depending on the value of `option`.
    fn go(&mut self, option: GoOrInfo, reply_code: u32, length: usize) -> Result<OptionFragment> {
        // TODO(mdlayher): more robust handling of error reply codes is
        // necessary, we have to check for various cases and return better enum
        // types.

        if reply_code == NBD_REP_ERR_UNKNOWN {
            // An unknown export error, read the literal string and note that we
            // are done parsing.
            self.done = true;
            let error = self.read_string(length)?;

            return match option {
                GoOrInfo::Go => Ok(OptionFragment::GoError(error)),
                GoOrInfo::Info => Ok(OptionFragment::InfoError(error)),
            };
        }

        if self.done {
            // Important: must return the correct type for parsing to work
            // later.
            match option {
                GoOrInfo::Go => return Ok(OptionFragment::GoDone),
                GoOrInfo::Info => return Ok(OptionFragment::InfoDone),
            }
        }

        match FromPrimitive::from_u16(get_u16(self.src)?) {
            Some(info_type) => {
                if reply_code != NBD_REP_INFO {
                    return Err(Error::HandshakeProtocol(self.frame_type));
                }

                match info_type {
                    InfoType::Export => {
                        // Fixed length.
                        if length != 12 {
                            return Err(Error::HandshakeProtocol(self.frame_type));
                        }

                        let size = get_u64(self.src)?;
                        let flags = TransmissionFlags::from_bits(get_u16(self.src)?)
                            .ok_or(Error::HandshakeProtocol(self.frame_type))?;

                        Ok(OptionFragment::Go(GoFragment::Export(size, flags)))
                    }
                    // length - 2 subtracts the space for info_type, leaving the
                    // string behind.
                    InfoType::Name => Ok(OptionFragment::Go(GoFragment::Name(
                        self.read_string(length - 2)?,
                    ))),
                    InfoType::Description => {
                        // Return None in place of empty string.
                        let string = self.read_string(length - 2)?;
                        let string = match string.len() {
                            0 => None,
                            _ => Some(string),
                        };

                        Ok(OptionFragment::Go(GoFragment::Description(string)))
                    }
                    InfoType::BlockSize => {
                        // Fixed length.
                        if length != 14 {
                            return Err(Error::HandshakeProtocol(self.frame_type));
                        }

                        let min = get_u32(self.src)?;
                        let pref = get_u32(self.src)?;
                        let max = get_u32(self.src)?;

                        Ok(OptionFragment::Go(GoFragment::BlockSizes([min, pref, max])))
                    }
                }
            }
            None => Err(Error::HandshakeProtocol(self.frame_type)),
        }
    }

    /// Reads and returns a string of size `length`.
    fn read_string(&mut self, length: usize) -> Result<String> {
        let mut name = vec![0u8; length];
        get_exact(self.src, &mut name)?;
        String::from_utf8(name).map_err(|_err| Error::HandshakeProtocol(self.frame_type))
    }
}

#[cfg(test)]
mod valid_tests {
    use super::*;

    /// A synthetic export reused throughout all of the tests.
    fn test_export() -> Export {
        Export::new("foo", 256 * MiB).description("bar").readonly()
    }

    /// Returns a byte containing the low bits of the test export's
    /// TransmissionFlags.
    fn transmission_flags_lo() -> u8 {
        // Defaults + read-only, don't bother setting every new flag.
        (TransmissionFlags::default() | TransmissionFlags::READ_ONLY)
            .bits()
            .try_into()
            .expect("transmission flags no longer fit into u8")
    }

    macro_rules! frame_read_tests {
        ($($name:ident: $type:path: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (buf, frame_type, want) = $value;
                let mut src = Cursor::new(&buf[..]);

                Frame::check(&mut src, frame_type).expect("failed to check frame");
                src.set_position(0);

                let got = match Frame::parse(&mut src, frame_type).expect("failed to parse frame") {
                    $type(v) => v,
                    frame => panic!("expected a {frame_type:?} frame, but got: {frame:?}"),
                };

                assert!(want.eq(&got), "unexpected {frame_type:?} frame contents:\nwant: {want:?}\n got: {got:?}");
            }
        )*
        }
    }

    frame_read_tests! {
        client_options_go_minimal: Frame::ClientOptions: (
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // Go length
                    0, 0, 0, 6,

                    // GoRequest
                    //
                    // Name length
                    0, 0, 0, 0,
                    // Number of info requests
                    0, 0,
                ],
            ].concat(),
            FrameType::ClientOptions,
            ClientOptions{
                flags: ClientFlags::all(),
                known: vec![OptionRequest::Go(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: Vec::new(),
            },
        ),
        client_options_go_full: Frame::ClientOptions: (
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // Go length
                    0, 0, 0, 18,

                    // GoRequest
                    //
                    // Name length + name
                    0, 0, 0, 4,
                    b't', b'e', b's', b't',
                    // Number of info requests
                    0, 4,
                    // Export
                    0, 0,
                    // Name
                    0, 1,
                    // Description
                    0, 2,
                    // Block size
                    0, 3,
                ],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Unknown
                    0, 0, 0, 0xff,
                    // Unknown length + bytes
                    0, 0, 0, 4,
                    0xff, 0xff, 0xff, 0xff,
                ],
            ].concat(),
            FrameType::ClientOptions,
            ClientOptions{
                      flags: ClientFlags::all(),
                known: vec![OptionRequest::Go(GoRequest{
                    name: Some("test".to_string()),
                    info_requests: vec![
                        InfoType::Export,
                        InfoType::Name,
                        InfoType::Description,
                        InfoType::BlockSize,
                    ],
                })],
                unknown: vec![0xff],
            },
        ),
        // Just test the bare minimum for Info since it shares almost all code
        // with Go.
        client_options_info_minimal: Frame::ClientOptions: (
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // Info length
                    0, 0, 0, 6,

                    // GoRequest
                    //
                    // Name length
                    0, 0, 0, 0,
                    // Number of info requests
                    0, 0,
                ],
            ].concat(),
            FrameType::ClientOptions,
            ClientOptions{
                      flags: ClientFlags::all(),
                known: vec![OptionRequest::Info(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: Vec::new(),
            },
        ),
        server_options_unknown: Frame::ServerOptions: (
            // One valid option, other unknown options.
            [
                // Abort acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Abort
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
                // Invalid option
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Invalid 1
                    0, 0, 0, 0xef,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length
                    0, 0, 0, 4,
                    // Junk data
                    0xff, 0xff, 0xff, 0xff,
                ],
                // Invalid acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Invalid 2
                    0, 0, 0, 0xff,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
            FrameType::ServerOptions, ServerOptions{
                known: vec![OptionResponse::Abort],
                unknown: vec![0xef, 0xff],
            },
        ),
    }

    macro_rules! frame_write_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[tokio::test]
            async fn $name() {
                let (frame, want) = $value;
                let frame_msg = format!("{frame:?}");

                let mut got = vec![];
                frame.write(&mut got).await.expect("failed to write frame");

                assert_eq!(
                    &want[..],
                    &got[..],
                    "unexpected frame bytes for {frame_msg}",
                );
            }
        )*
        }
    }

    frame_write_tests! {
        server_handshake_full: (
            Frame::ServerHandshake(HandshakeFlags::FIXED_NEWSTYLE | HandshakeFlags::NO_ZEROES),
            [NBDMAGIC_BUF, IHAVEOPT_BUF, &[0, 1 | 2]].concat(),
        ),
        server_options_abort_full: (
            Frame::ServerOptions(ServerOptions{
                known: vec![OptionResponse::Abort],
                unknown: Vec::new(),
            }),
            [
                // Abort acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Abort
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_options_abort_short: (
            Frame::ServerOptionsAbort,
            [
                // Abort acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    //Abort
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_options_go_full: (
            Frame::ServerOptions(ServerOptions {
                known: vec![OptionResponse::Go(GoResponse::Ok {
                    info_requests: vec![
                        InfoType::Export,
                        InfoType::Name,
                        InfoType::Description,
                        InfoType::BlockSize
                    ],
                    export: test_export(),
                })],
                unknown: Vec::new(),
            }),
            [
                // Export
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 12,
                    // Export info type
                    0, 0,
                    // Size
                    0, 0, 0, 0, 16, 0, 0, 0,
                    // Transmission flags
                    0, transmission_flags_lo(),
                ],
                // Name
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 5,
                    // Name info type
                    0, 1,
                    // Name
                    b'f', b'o', b'o',
                ],
                // Description
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 5,
                    // Description info type
                    0, 2,
                    // Description
                    b'b', b'a', b'r',
                ],
                // Block size
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 14,
                    // Block size info type
                    0, 3,
                    // Minimum size
                    0, 0, 0, 1,
                    // Preferred size
                    0, 0, 16, 0,
                    // Maximum size
                    0xff, 0xff, 0xff, 0xff,
                ],
                // Final acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        // Just test the bare minimum for Info since it shares almost all code
        // with Go.
        server_options_info_minimal: (
            Frame::ServerOptions(ServerOptions{
                known: vec![OptionResponse::Info(GoResponse::Ok {
                    info_requests: vec![InfoType::Export],
                    export: test_export(),
                })],
                unknown: Vec::new(),
            }),
            [
                // Export
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 12,
                    // Export info type
                    0, 0,
                    // Size
                    0, 0, 0, 0, 16, 0, 0, 0,
                    // Transmission flags
                    0, transmission_flags_lo(),
                ],
                // Final acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_options_list_full: (
            Frame::ServerOptions(ServerOptions {
                known: vec![OptionResponse::List(ListResponse(vec![test_export().into()]))],
                unknown: Vec::new(),
            }),
            [
                // List
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // List
                    0, 0, 0, 3,
                    // NBD_REP_SERVER
                    0, 0, 0, 2,
                    // Length
                    0, 0, 0, 70,
                    // Name length
                    0, 0, 0, 3,
                    // Name
                    b'f', b'o', b'o',
                ],
                // Metadata
                b"bar [read-only] (size: 256MiB, block sizes: 1/4096/4294967295B)",
                // Final acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // List
                    0, 0, 0, 3,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
    }

    macro_rules! frame_roundtrip_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[tokio::test]
            async fn $name() {
                let (frame, bytes) = $value;

                let mut buf = vec![];
                frame.write(&mut buf).await.expect("failed to write frame");

                assert_eq!(
                    &bytes[..],
                    &buf[..],
                    "unexpected frame bytes for {frame:?}",
                );

                let mut src = Cursor::new(&buf[..]);

                let frame_type = frame.to_type();
                Frame::check(&mut src, frame_type).expect("failed to check frame");
                src.set_position(0);

                let parsed = Frame::parse(&mut src, frame_type).expect("failed to parse frame");

                assert_eq!(frame, parsed, "unexpected frame after roundtrip");
            }
        )*
        }
    }

    frame_roundtrip_tests! {
        client_options_abort_roundtrip: (
            Frame::ClientOptions(ClientOptions{
                flags: ClientFlags::all(),
                known: vec![OptionRequest::Abort],
                unknown: Vec::new(),
            }),
            [
                                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Abort
                    0, 0, 0, 2,
                    // Abort length
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        client_options_go_roundtrip: (
            Frame::ClientOptions(ClientOptions{
                flags: ClientFlags::all(),
                known: vec![OptionRequest::Go(GoRequest{
                    name: Some("test".to_string()),
                    info_requests: vec![
                        InfoType::Export,
                        InfoType::Name,
                        InfoType::Description,
                        InfoType::BlockSize,
                    ],
                })],
                unknown: Vec::new(),
            }),
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // Go length
                    0, 0, 0, 18,

                    // GoRequest
                    //
                    // Name length + name
                    0, 0, 0, 4,
                    b't', b'e', b's', b't',
                    // Number of info requests
                    0, 4,
                    // Export
                    0, 0,
                    // Name
                    0, 1,
                    // Description
                    0, 2,
                    // Block size
                    0, 3,
                ],
            ].concat(),
        ),
        // Just test the bare minimum for Info since it shares almost all code
        // with Go.
        client_options_info_roundtrip: (
            Frame::ClientOptions(ClientOptions{
                flags: ClientFlags::all(),
                known: vec![OptionRequest::Info(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: Vec::new(),
            }),
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // Go length
                    0, 0, 0, 8,

                    // GoRequest
                    //
                    // Name length + name (empty)
                    0, 0, 0, 0,
                    // Number of info requests
                    0, 1,
                    // Export
                    0, 0,
                ],
            ].concat(),
        ),
        client_options_list_roundtrip: (
            Frame::ClientOptions(ClientOptions{
                flags: ClientFlags::all(),
                known: vec![OptionRequest::List],
                unknown: Vec::new(),
            }),
            [
                // Flags
                &[0, 0, 0, 1 | 2],
                // Magic
                IHAVEOPT_BUF,
                &[
                    // List
                    0, 0, 0, 3,
                    // List length
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_handshake_roundtrip: (
            Frame::ServerHandshake(HandshakeFlags::all()),
            [
                NBDMAGIC_BUF,
                IHAVEOPT_BUF,
                &[0, 1 | 2],
            ].concat(),
        ),
        server_options_abort_roundtrip: (
            Frame::ServerOptions(ServerOptions{
                known: vec![OptionResponse::Abort],
                unknown: Vec::new(),
            }),
            [
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Abort
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Abort length
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_options_roundtrip: (
            Frame::ServerOptions(ServerOptions{
                known: vec![
                    OptionResponse::Abort,
                    OptionResponse::Go(GoResponse::Ok {
                        info_requests: vec![
                            InfoType::Export,
                            InfoType::Name,
                            InfoType::Description,
                            InfoType::BlockSize
                        ],
                        export: test_export(),
                    }),
                    OptionResponse::Info(GoResponse::Ok {
                        info_requests: vec![InfoType::Export],
                        // TODO(mdlayher): this is not a realistic export,
                        // consider updating.
                        export: Export::new("", 256*MiB)
                            .block_size([0u32; 3])
                            .readonly()
                    }),
                    OptionResponse::List(ListResponse(vec![ListExport{
                        name: "foo".to_string(),
                        metadata: "bar (size: 256MiB, block sizes: 1/4096/4294967295B)".to_string(),
                    }])),
                    OptionResponse::Go(GoResponse::Unknown("not found".to_string())),
                    OptionResponse::Info(GoResponse::Unknown("not found".to_string())),
                ],
                // Unknown options cannot be set for roundtrip.
                unknown: vec![],
            }),
            // These bytes are not realistic, but instead represent the full
            // variety of server options which could be round-tripped.
            [
                // Abort acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Abort
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
                // Export
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 12,
                    // Export info type
                    0, 0,
                    // Size
                    0, 0, 0, 0, 16, 0, 0, 0,
                    // Transmission flags
                    0, transmission_flags_lo(),
                ],
                // Name
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 5,
                    // Name info type
                    0, 1,
                    // Name
                    b'f', b'o', b'o',
                ],
                // Description
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 5,
                    // Description info type
                    0, 2,
                    // Description
                    b'b', b'a', b'r',
                ],
                // Block size
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 14,
                    // Block size info type
                    0, 3,
                    // Minimum size
                    0, 0, 0, 1,
                    // Preferred size
                    0, 0, 16, 0,
                    // Maximum size
                    0xff, 0xff, 0xff, 0xff,
                ],
                // Go acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
                // Info
                //
                // Export
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // NBD_REP_INFO
                    0, 0, 0, 3,
                    // Length
                    0, 0, 0, 12,
                    // Export info type
                    0, 0,
                    // Size
                    0, 0, 0, 0, 16, 0, 0, 0,
                    // Transmission flags
                    0, transmission_flags_lo(),
                ],
                // Info acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
                // List
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // List
                    0, 0, 0, 3,
                    // NBD_REP_SERVER
                    0, 0, 0, 2,
                    // Length
                    0, 0, 0, 58,
                    // Name length
                    0, 0, 0, 3,
                    // Name
                    b'f', b'o', b'o',
                ],
                // Metadata
                b"bar (size: 256MiB, block sizes: 1/4096/4294967295B)",
                // List acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // List
                    0, 0, 0, 3,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
                // Go error
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // NBD_REP_ERR_UNKNOWN
                    0x80, 0, 0, 6,
                    // Length
                    0, 0, 0, 9,
                ],
                b"not found",
                // Info error
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Info
                    0, 0, 0, 6,
                    // NBD_REP_ERR_UNKNOWN
                    0x80, 0, 0, 6,
                    // Length
                    0, 0, 0, 9,
                ],
                b"not found",
            ].concat(),
        ),
        server_unsupported_options_roundtrip: (
            Frame::ServerUnsupportedOptions(vec![1, 2]),
            [
                // Option 1
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // Option code
                    0, 0, 0, 1,
                    // Error unsupported
                    0x80, 0, 0, 1,
                    // String length
                    0, 0, 0, 21,
                ],
                // Error string
                b"unsupported option: 1",

                // Option 2; see comments above.
                REPLYMAGIC_BUF,
                &[
                    0, 0, 0, 2,
                    0x80, 0, 0, 1,
                    0, 0, 0, 21,
                ],
                b"unsupported option: 2",
            ].concat(),
        ),
    }

    macro_rules! frame_write_none_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[tokio::test]
            async fn $name() {
                let frame = $value;
                let frame_msg = format!("{frame:?}");

                let mut got = vec![];
                let result = frame.write(&mut got).await.expect("failed to write frame");

                assert!(matches!(result, None), "expected None return from write");

                assert!(got.is_empty(), "expected empty frame bytes for {frame_msg}: {got:?}");
            }
        )*
        }
    }

    frame_write_none_tests! {
        server_options_none: Frame::ServerOptions(ServerOptions{known: Vec::new(), unknown: Vec::new()}),
        server_unsupported_options_none: Frame::ServerUnsupportedOptions(Vec::new()),
    }
}

#[cfg(test)]
mod invalid_tests {
    use super::*;

    macro_rules! frame_incomplete_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (frame_type, buf) = $value;
                let mut src = Cursor::new(&buf[..]);

                let err = Frame::check(&mut src, frame_type).expect_err("frame check succeeded");

                assert!(matches!(err, Error::Incomplete), "expected Error::Incomplete, but got: {err:?}");
            }
        )*
        }
    }

    frame_incomplete_tests! {
        client_options_short: (FrameType::ClientOptions, b"IHAVEOP"),
    }

    macro_rules! frame_protocol_error_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (frame_type, buf) = $value;
                let mut src = Cursor::new(&buf[..]);

                let err = Frame::parse(&mut src, frame_type).expect_err("frame parse succeeded");

                assert!(matches!(err, Error::HandshakeProtocol(_)), "expected Error::HandshakeProtocol, but got: {err:?}");
            }
        )*
        }
    }

    frame_protocol_error_tests! {
        client_options_magic: (FrameType::ClientOptions, b"deadbeef"),
        client_options_go_utf8: (
            FrameType::ClientOptions,
            [
                // ClientOptions
                //
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // Go length
                    0, 0, 0, 7,

                    // GoRequest
                    //
                    // Name length
                    0, 0, 0, 4,
                    // Name: bad UTF-8
                    b't', b'e', b's', 0xff,
                ],
            ].concat(),
        ),
        client_options_go_info_request: (
            FrameType::ClientOptions,
            [
                // ClientOptions
                //
                // Magic
                IHAVEOPT_BUF,
                &[
                    // Go
                    0, 0, 0, 7,
                    // Go length
                    0, 0, 0, 7,

                    // GoRequest
                    //
                    // Name length
                    0, 0, 0, 0,
                    // Number of info requests
                    0, 1,
                    // Invalid info request
                    0xff, 0xff,
                ],
            ].concat(),
        ),
    }
}
