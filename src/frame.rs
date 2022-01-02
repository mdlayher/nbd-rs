use bitflags::bitflags;
use bytes::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::cmp::max;
use std::fmt;
use std::io::{self, Read, Write};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::consts::*;

/// An NBD data frame sent between client and server. Note that the frame types
/// here do not necessarily correspond to the NBD specification, but are used to
/// chunk up logical operations in this library.
#[derive(Debug, PartialEq)]
pub enum Frame {
    ClientFlags(ClientFlags),
    ClientOptions(ClientOptions),
    ServerHandshake(HandshakeFlags),
    ServerOptions(Export, Vec<OptionResponse>),
    ServerOptionsAbort,
    ServerUnsupportedOptions(Vec<u32>),
}

/// Denotes the expected type of a Frame without knowledge of its associated
/// data.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum FrameType {
    ClientFlags,
    ClientOptions,
    ServerHandshake,
    ServerOptions,
    ServerUnsupportedOptions,
}

bitflags! {
    /// Valid bitflags for a server handshake.
    pub struct HandshakeFlags: u16 {
        const FIXED_NEWSTYLE = NBD_FLAG_FIXED_NEWSTYLE;
        const NO_ZEROES      = NBD_FLAG_NO_ZEROES;
    }

    /// Valid bitflags for a client handshake.
    pub struct ClientFlags: u32 {
        const FIXED_NEWSTYLE = NBD_FLAG_C_FIXED_NEWSTYLE;
        const NO_ZEROES      = NBD_FLAG_C_NO_ZEROES;
    }

    /// Valid bitflags for data transmission negotiation.
    pub struct TransmissionFlags: u16 {
        const HAS_FLAGS = NBD_FLAG_HAS_FLAGS;
        const READ_ONLY = NBD_FLAG_READ_ONLY;
    }
}

/// Options sent by the client which are parsed as either known or unknown
/// depending on the server's capabilities.
#[derive(Debug, Default, PartialEq)]
pub struct ClientOptions {
    pub known: Vec<OptionRequest>,
    pub unknown: Vec<u32>,
}

/// Information about the Network Block Device being served.
#[derive(Debug, Default, PartialEq)]
pub struct Export {
    pub name: String,
    pub description: String,
    pub size: u64,
    pub block_size: u32,
    pub readonly: bool,
}

/// Denotes the type of known options which can be handled by the server.
#[repr(u32)]
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum OptionRequestCode {
    Abort = NBD_OPT_ABORT,
    Go = NBD_OPT_GO,
    Info = NBD_OPT_INFO,
    List = NBD_OPT_LIST,
}

/// The contents of known options which a client can send to a server.
#[derive(Debug, PartialEq)]
pub enum OptionRequest {
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
pub enum OptionResponse {
    Abort,
    Go(GoResponse),
    Info(GoResponse),
    List(ListResponse),
}

impl From<OptionRequest> for OptionResponse {
    fn from(src: OptionRequest) -> OptionResponse {
        match src {
            OptionRequest::Abort => OptionResponse::Abort,
            OptionRequest::Go(req) => OptionResponse::Go(req.into()),
            OptionRequest::Info(req) => OptionResponse::Info(req.into()),
            OptionRequest::List => OptionResponse::List(ListResponse()),
        }
    }
}

impl OptionRequest {
    /// Produces a `GoRequest` from `src` after the client option header has
    /// been consumed by `next_option` with the given `frame_type`. This can
    /// then be associated with `OptionRequest::Go` or `OptionRequest::Info` as
    /// necessary.
    fn go(src: &mut io::Cursor<&[u8]>, frame_type: FrameType) -> Result<GoRequest> {
        // Name may or may not be present.
        let name_length = get_u32(src)? as usize;
        let name = match name_length {
            0 => None,
            _ => {
                let mut name_buf = vec![0u8; name_length];
                get_exact(src, &mut name_buf)?;
                Some(String::from_utf8(name_buf).map_err(|_err| Error::Protocol(frame_type))?)
            }
        };

        let num_infos = get_u16(src)? as usize;

        // Allocate enough space for the single export info (in the case of no
        // requested options) or enough for each requested option.
        let mut info_requests = Vec::with_capacity(max(1, num_infos));
        for _ in 0..num_infos {
            let raw = get_u16(src)?;
            let info = FromPrimitive::from_u16(raw).ok_or(Error::Protocol(frame_type))?;

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

    /// Returns the associated `OptionRequestCode` for `self`.
    fn code(&self) -> OptionRequestCode {
        match self {
            Self::Abort => OptionRequestCode::Abort,
            Self::Go(..) => OptionRequestCode::Go,
            Self::Info(..) => OptionRequestCode::Info,
            Self::List => OptionRequestCode::List,
        }
    }
}

impl OptionResponse {
    /// Reports whether NBD data transmission should begin after this option is processed.
    pub fn do_transmit(&self) -> bool {
        matches!(self, Self::Go(..))
    }

    /// Returns the associated `OptionRequestCode` for `self`.
    fn code(&self) -> OptionRequestCode {
        match self {
            Self::Abort => OptionRequestCode::Abort,
            Self::Go(..) => OptionRequestCode::Go,
            Self::Info(..) => OptionRequestCode::Info,
            Self::List(..) => OptionRequestCode::List,
        }
    }
}

/// A Go option as sent by a client.
#[derive(Debug, PartialEq)]
pub struct GoRequest {
    pub name: Option<String>,
    pub info_requests: Vec<InfoType>,
}

/// A Go option as sent by a server in response to a client.
#[derive(Debug, PartialEq)]
pub struct GoResponse {
    pub name: Option<String>,
    pub info_requests: Vec<InfoType>,
}

impl From<GoRequest> for GoResponse {
    fn from(src: GoRequest) -> GoResponse {
        GoResponse {
            name: src.name,
            info_requests: src.info_requests,
        }
    }
}

/// Denotes the type of an information request from a client.
#[repr(u16)]
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum InfoType {
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

/// A GoResponseCode selects the code that will be sent when calling
/// `GoResponse.write`.
#[repr(u32)]
#[derive(Clone, Copy)]
enum GoResponseCode {
    Go = OptionRequestCode::Go as u32,
    Info = OptionRequestCode::Info as u32,
}

impl GoResponse {
    /// Writes the bytes for a `GoResponse` to `dst` with the specified `code`,
    async fn write<S: AsyncWrite + Unpin>(
        &self,
        dst: &mut S,
        code: GoResponseCode,
        export: &Export,
    ) -> io::Result<()> {
        for info in &self.info_requests {
            // Each info request reply is prefixed with magic and the Go code.
            dst.write_u64(REPLYMAGIC).await?;
            dst.write_u32(code as u32).await?;

            match *info {
                InfoType::Export => {
                    // Fixed size of 10 bytes for export info.
                    dst.write_u32(NBD_REP_INFO).await?;
                    dst.write_u32(2 + 8 + 2).await?;

                    // Export info type, export size.
                    dst.write_u16(*info as u16).await?;
                    dst.write_u64(export.size).await?;

                    // Always set flags, optionally mark read-only.
                    let mut flags = TransmissionFlags::HAS_FLAGS;
                    if export.readonly {
                        flags |= TransmissionFlags::READ_ONLY;
                    }
                    dst.write_u16(flags.bits()).await?;
                }
                InfoType::Name => Self::write_string(dst, *info, &export.name).await?,
                InfoType::Description => {
                    Self::write_string(dst, *info, &export.description).await?
                }
                InfoType::BlockSize => {
                    // Fixed size of 14 bytes for block size.
                    dst.write_u32(NBD_REP_INFO).await?;
                    dst.write_u32(2 + (4 * 3)).await?;
                    dst.write_u16(*info as u16).await?;

                    // TODO(mdlayher): break out minimum/preferred/maximum into
                    // export fields? For now we specify the same value for
                    // each.
                    for _ in 0..3 {
                        dst.write_u32(export.block_size).await?;
                    }
                }
            }
        }

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

/// A List option as sent by a server in response to a client. This type
/// contains no data of its own but exists to hold methods for writing export
/// data lists to clients.
#[derive(Debug, PartialEq)]
pub struct ListResponse();

impl ListResponse {
    /// Writes the bytes describing `export` to `dst`.
    async fn write<S: AsyncWrite + Unpin>(&self, dst: &mut S, export: &Export) -> io::Result<()> {
        // Each export reply is prefixed with magic and the List code.
        //
        // TODO(mdlayher): support for passing in multiple exports.
        for export in [export] {
            dst.write_u64(REPLYMAGIC).await?;
            dst.write_u32(OptionRequestCode::List as u32).await?;

            dst.write_u32(NBD_REP_SERVER).await?;

            // Pack in the name and description strings along with an extra 4
            // for the name string length as the option's body. Clients will
            // interpret bytes up to name_length as the export name and bytes
            // beyond as a human-readable string, which we use to describe the
            // export's metadata.
            let metadata = format!(
                "{} (size: {}MiB, block size: {}B)",
                export.description,
                // TODO(mdlayher): this bytes to MiB calculation is good enough
                // for now but probably not very robust.
                export.size / (1 << 20),
                export.block_size
            );

            let name_length = export.name.len() as u32;
            let meta_length = metadata.len() as u32;

            dst.write_u32(name_length + meta_length + 4).await?;
            dst.write_u32(name_length).await?;

            dst.write_all(export.name.as_bytes()).await?;
            dst.write_all(metadata.as_bytes()).await?;
        }

        Ok(())
    }
}

impl Frame {
    /// Determines if enough data is available to parse a `Frame` of the given
    /// `FrameType` from `src`.
    pub fn check(src: &mut io::Cursor<&[u8]>, frame_type: FrameType) -> Result<()> {
        match frame_type {
            FrameType::ClientFlags => {
                // flags u32
                get_u32(src)?;
                Ok(())
            }
            FrameType::ClientOptions => {
                while src.has_remaining() {
                    next_option(src, frame_type)?;
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
            FrameType::ServerUnsupportedOptions => {
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
            // Frames the server will send instead of the client.
            //
            // TODO(mdlayher): implement client as well.
            FrameType::ServerOptions => Err(Error::Unsupported(frame_type)),
        }
    }

    /// Parses the next `Frame` according to the given `FrameType`.
    pub fn parse(src: &mut io::Cursor<&[u8]>, frame_type: FrameType) -> Result<Frame> {
        match frame_type {
            FrameType::ClientFlags => {
                let flags =
                    ClientFlags::from_bits(get_u32(src)?).ok_or(Error::Protocol(frame_type))?;

                Ok(Frame::ClientFlags(flags))
            }
            FrameType::ClientOptions => {
                let mut known = Vec::new();
                let mut unknown = Vec::new();
                while src.has_remaining() {
                    // Keep track of both known and unknown options so we can
                    // report errors to the client accordingly.
                    match next_option(src, frame_type)? {
                        ParsedOption::Known(option) => known.push(option),
                        ParsedOption::Unknown(code) => unknown.push(code),
                    }
                }

                Ok(Frame::ClientOptions(ClientOptions { known, unknown }))
            }
            FrameType::ServerHandshake => {
                if get_u64(src)? != NBDMAGIC {
                    return Err(Error::Protocol(frame_type));
                }
                if get_u64(src)? != IHAVEOPT {
                    return Err(Error::Protocol(frame_type));
                }

                let flags =
                    HandshakeFlags::from_bits(get_u16(src)?).ok_or(Error::Protocol(frame_type))?;

                Ok(Frame::ServerHandshake(flags))
            }
            FrameType::ServerUnsupportedOptions => {
                let mut options = Vec::new();
                while src.has_remaining() {
                    if get_u64(src)? != REPLYMAGIC {
                        return Err(Error::Protocol(frame_type));
                    }

                    options.push(get_u32(src)?);

                    if get_u32(src)? != NBD_REP_ERR_UNSUP {
                        return Err(Error::Protocol(frame_type));
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
            // Frames the server will send instead of the client.
            //
            // TODO(mdlayher): implement client as well.
            FrameType::ServerOptions => Err(Error::Unsupported(frame_type)),
        }
    }

    /// Writes the current `Frame` out to `dst`. It returns `Some(())` if any
    /// bytes were written to the stream or `None` if not.
    pub async fn write<S: AsyncWrite + Unpin>(&self, dst: &mut S) -> io::Result<Option<()>> {
        match self {
            Frame::ClientFlags(flags) => dst.write_u32(flags.bits()).await?,
            Frame::ClientOptions(options) => {
                // TODO(mdlayher): from a client perspective, it doesn't really
                // make sense to have known/unknown options. Only send known
                // options for now.
                assert!(options.unknown.is_empty(), "unknown options must be empty");

                let options = &options.known;
                if options.is_empty() {
                    // Noop, nothing to write.
                    return Ok(None);
                }

                // Write each option's header and code.
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
                Self::ack(dst, OptionRequestCode::Abort).await?;
            }
            Frame::ServerOptions(export, options) => {
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
                        }
                        OptionResponse::Go(res) => {
                            res.write(dst, GoResponseCode::Go, export).await?
                        }
                        OptionResponse::Info(res) => {
                            res.write(dst, GoResponseCode::Info, export).await?
                        }
                        // TODO(mdlayher): support for multiple exports.
                        OptionResponse::List(res) => res.write(dst, export).await?,
                    }

                    // Acknowledge the option was processed.
                    Self::ack(dst, option.code()).await?;
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

                    let error = format!("unsupported option: {}", option);

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
    async fn ack<S: AsyncWrite + Unpin>(dst: &mut S, code: OptionRequestCode) -> io::Result<()> {
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
            Self::ClientFlags(..) => FrameType::ClientFlags,
            Self::ClientOptions(..) => FrameType::ClientOptions,
            Self::ServerHandshake(..) => FrameType::ServerHandshake,
            Self::ServerOptionsAbort | Self::ServerOptions(..) => FrameType::ServerOptions,
            Self::ServerUnsupportedOptions(..) => FrameType::ServerUnsupportedOptions,
        }
    }
}

// Functions for consuming fixed amounts of data from `src` or returning
// `Error::Incomplete` when necessary.

fn get_u16(src: &mut io::Cursor<&[u8]>) -> Result<u16> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u16())
}

fn get_u32(src: &mut io::Cursor<&[u8]>) -> Result<u32> {
    if src.remaining() < 4 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u32())
}

fn get_u64(src: &mut io::Cursor<&[u8]>) -> Result<u64> {
    if src.remaining() < 8 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u64())
}

fn get_exact(src: &mut io::Cursor<&[u8]>, dst: &mut [u8]) -> Result<()> {
    if src.remaining() < dst.len() {
        return Err(Error::Incomplete);
    }

    Ok(src.read_exact(dst)?)
}

fn skip(src: &mut io::Cursor<&[u8]>, n: usize) -> Result<()> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Denotes known or unknown options sent by a client.
#[derive(Debug)]
enum ParsedOption {
    Known(OptionRequest),
    Unknown(u32),
}

// Produces the next `ParsedOption` value from `src` by consuming the client
// option header and inner data for a given `frame_type`.
fn next_option(src: &mut io::Cursor<&[u8]>, frame_type: FrameType) -> Result<ParsedOption> {
    if get_u64(src)? != IHAVEOPT {
        return Err(Error::Protocol(frame_type));
    }

    let option_code = get_u32(src)?;
    let length = get_u32(src)? as usize;

    Ok(match FromPrimitive::from_u32(option_code) {
        Some(option) => ParsedOption::Known(match option {
            OptionRequestCode::Go => OptionRequest::Go(OptionRequest::go(src, frame_type)?),
            OptionRequestCode::Info => OptionRequest::Info(OptionRequest::go(src, frame_type)?),
            // These options have no body, no need to advance src.
            OptionRequestCode::Abort => OptionRequest::Abort,
            OptionRequestCode::List => OptionRequest::List,
        }),
        None => {
            // We aren't aware of this option, skip over it but note its code as
            // unknown for error reporting.
            skip(src, length)?;
            ParsedOption::Unknown(option_code)
        }
    })
}

/// Contains error information encountered while dealing with Frames.
#[derive(Debug)]
pub enum Error {
    /// A sentinel which indicates more data must be read from a stream to parse
    /// an entire Frame.
    Incomplete,

    /// Indicates that parsing this frame type is unsupported.
    Unsupported(FrameType),

    /// Indicates that the frame is supported but cannot be parsed due to a
    /// protocol error, such as an incorrect magic number.
    Protocol(FrameType),

    Other(crate::Error),
}

/// A specialized result for returning Errors.
type Result<T> = std::result::Result<T, Error>;

// TODO(mdlayher): these From implementations are copied from tokio examples.
// Switch to another error crate to simplify things?

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<io::Error> for Error {
    fn from(src: io::Error) -> Error {
        Error::Other(src.into())
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Unsupported(ft) => write!(fmt, "frame type {:?} is not supported", ft),
            Error::Protocol(ft) => write!(
                fmt,
                "frame type {:?} could not be parsed due to protocol error",
                ft
            ),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
mod valid_tests {
    use super::*;

    macro_rules! frame_read_tests {
        ($($name:ident: $type:path: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (buf, frame_type, want) = $value;
                let mut src = io::Cursor::new(&buf[..]);

                Frame::check(&mut src, frame_type).expect("failed to check frame");
                src.set_position(0);

                let got = match Frame::parse(&mut src, frame_type).expect("failed to parse frame") {
                    $type(v) => v,
                    frame => panic!("expected a {:?} frame, but got: {:?}", frame_type, frame),
                };

                assert!(want.eq(&got), "unexpected {:?} frame contents:\nwant: {:?}\n got: {:?}", frame_type, want, got);
            }
        )*
        }
    }

    frame_read_tests! {
        client_flags_empty: Frame::ClientFlags: (
            [0u8; 4], FrameType::ClientFlags, ClientFlags::empty(),
        ),

        client_flags_all: Frame::ClientFlags: (
            [0, 0, 0, 1 | 2],
            FrameType::ClientFlags,
            ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES,
        ),

        client_options_go_minimal: Frame::ClientOptions: (
            [
                // ClientOptions
                //
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
                known: vec![OptionRequest::Go(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: vec![],
            },
        ),
        client_options_go_full: Frame::ClientOptions: (
            [
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
                // ClientOptions
                //
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
                known: vec![OptionRequest::Info(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: vec![],
            },
        ),
    }

    macro_rules! frame_write_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[tokio::test]
            async fn $name() {
                let (frame, want) = $value;
                let frame_msg = format!("{:?}", frame);

                let mut got = vec![];
                frame.write(&mut got).await.expect("failed to write frame");

                assert_eq!(
                    &want[..],
                    &got[..],
                    "unexpected frame bytes for {}",
                    frame_msg
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
            Frame::ServerOptions(Export{
                name: "foo".to_string(),
                description: "bar".to_string(),
                size: 256 * MiB,
                block_size: 512,
                readonly: true,
            }, vec![OptionResponse::Abort]),
            [
                // Abort acknowledgement
                //
                // Magic
                REPLYMAGIC_BUF,
                &[
                    // List
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
                    // List
                    0, 0, 0, 2,
                    // NBD_REP_ACK
                    0, 0, 0, 1,
                    // Length (empty)
                    0, 0, 0, 0,
                ],
            ].concat(),
        ),
        server_options_go_full: (
            Frame::ServerOptions(Export{
                name: "foo".to_string(),
                description: "bar".to_string(),
                size: 1024,
                block_size: 512,
                readonly: true,
            }, vec![OptionResponse::Go(GoResponse{
                name: Some("foo".to_string()),
                info_requests: vec![
                    InfoType::Export,
                    InfoType::Name,
                    InfoType::Description,
                    InfoType::BlockSize
                ],
            })]),
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
                    0, 0, 0, 0, 0, 0, 4, 0,
                    // Transmission flags
                    0, 1 | 2,
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
                    0, 0, 2, 0,
                    // Preferred size
                    0, 0, 2, 0,
                    // Maximum size
                    0, 0, 2, 0,
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
            Frame::ServerOptions(Export{
                name: "foo".to_string(),
                description: "bar".to_string(),
                size: 1024,
                block_size: 512,
                readonly: true,
            }, vec![OptionResponse::Info(GoResponse{
                name: Some("foo".to_string()),
                info_requests: vec![InfoType::Export],
            })]),
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
                    0, 0, 0, 0, 0, 0, 4, 0,
                    // Transmission flags
                    0, 1 | 2,
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
            Frame::ServerOptions(Export{
                name: "foo".to_string(),
                description: "bar".to_string(),
                size: 256 * MiB,
                block_size: 512,
                readonly: true,
            }, vec![OptionResponse::List(ListResponse())]),
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
                    0, 0, 0, 43,
                    // Name length
                    0, 0, 0, 3,
                    // Name
                    b'f', b'o', b'o',
                ],
                // Metadata
                b"bar (size: 256MiB, block size: 512B)",
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
                    "unexpected frame bytes for {:?}",
                    frame,
                );

                let mut src = io::Cursor::new(&buf[..]);

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
        client_flags_roundtrip: (
            Frame::ClientFlags(ClientFlags::all()),
            &[0, 0, 0, 1 | 2],
        ),

        client_options_abort_roundtrip: (
            Frame::ClientOptions(ClientOptions{
                known: vec![OptionRequest::Abort],
                unknown: Vec::new(),
            }),
            [
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
                known: vec![OptionRequest::Info(GoRequest{
                    name: None,
                    info_requests: vec![InfoType::Export],
                })],
                unknown: Vec::new(),
            }),
            [
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
                known: vec![OptionRequest::List],
                unknown: Vec::new(),
            }),
            [
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
                let frame_msg = format!("{:?}", frame);

                let mut got = vec![];
                let result = frame.write(&mut got).await.expect("failed to write frame");

                assert!(matches!(result, None), "expected None return from write");

                assert!(got.is_empty(), "expected empty frame bytes for {}: {:?}", frame_msg, got);
            }
        )*
        }
    }

    frame_write_none_tests! {
        client_options_none: Frame::ClientOptions(ClientOptions{known: Vec::new(), unknown: Vec::new()}),
        server_options_none: Frame::ServerOptions(Export::default(), Vec::new()),
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
                let mut src = io::Cursor::new(&buf[..]);

                let err = Frame::check(&mut src, frame_type).expect_err("frame check succeeded");

                assert!(matches!(err, Error::Incomplete), "expected Error::Incomplete, but got: {:?}", err);
            }
        )*
        }
    }

    frame_incomplete_tests! {
        client_flags_short: (FrameType::ClientFlags, [0u8; 3]),
        client_options_short: (FrameType::ClientOptions, b"IHAVEOP"),
    }

    macro_rules! frame_protocol_error_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (frame_type, buf) = $value;
                let mut src = io::Cursor::new(&buf[..]);

                let err = Frame::parse(&mut src, frame_type).expect_err("frame parse succeeded");

                assert!(matches!(err, Error::Protocol(_)), "expected Error::Protocol, but got: {:?}", err);
            }
        )*
        }
    }

    frame_protocol_error_tests! {
        client_flags_all: (FrameType::ClientFlags, [0xff, 0xff, 0xff, 0xff]),
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

    #[test]
    fn frames_unsupported() {
        let types = [FrameType::ServerOptions];

        for ft in types {
            let err =
                Frame::check(&mut io::Cursor::default(), ft).expect_err("cursor should be empty");

            assert!(
                matches!(err, Error::Unsupported(_)),
                "expected unsupported frame type from Frame::check for {:?}",
                ft,
            );

            let err =
                Frame::parse(&mut io::Cursor::default(), ft).expect_err("cursor should be empty");

            assert!(
                matches!(err, Error::Unsupported(_)),
                "expected unsupported frame type from Frame::parse for {:?}",
                ft,
            );
        }
    }
}
