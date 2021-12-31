use bitflags::bitflags;
use bytes::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::cmp::max;
use std::fmt;
use std::io::{self, Read};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::consts::*;

/// An NBD data frame sent between client and server. Note that the frame types
/// here do not necessarily correspond to the NBD specification, but are used to
/// chunk up logical operations in this library.
#[derive(Debug)]
pub enum Frame {
    ClientFlags(ClientFlags),
    ClientOptions(ClientOptions),
    ServerHandshake(HandshakeFlags),
    ServerOptions(Export, Vec<(OptionRequestCode, OptionRequest)>),
    ServerUnsupportedOptions(Vec<u32>),
}

/// Denotes the expected type of a Frame without knowledge of its associated
/// data.
#[allow(dead_code)]
#[derive(Debug)]
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
    pub known: Vec<(OptionRequestCode, OptionRequest)>,
    pub unknown: Vec<u32>,
}

/// Information about the Network Block Device being served.
#[derive(Debug)]
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
    Go = NBD_OPT_GO,
}

/// The contents of known options which can be handled by the server.
#[derive(Debug, PartialEq)]
pub enum OptionRequest {
    Go(GoRequest),
}

impl OptionRequest {
    /// Produces an `OptionRequest(GoRequest)` from `src` after the client
    /// option header has been consumed by `next_option`.
    pub fn go(src: &mut io::Cursor<&[u8]>) -> Result<OptionRequest> {
        // Name may or may not be present.
        let name_length = get_u32(src)? as usize;
        let name = match name_length {
            0 => None,
            _ => {
                let mut name_buf = vec![0u8; name_length];
                get_exact(src, &mut name_buf)?;
                Some(String::from_utf8(name_buf)?)
            }
        };

        let num_infos = get_u16(src)? as usize;

        // Allocate enough space for the single export info (in the case of no
        // requested options) or enough for each requested option.
        let mut info_requests = Vec::with_capacity(max(1, num_infos));
        for _ in 0..num_infos {
            let raw = get_u16(src)?;
            let info = FromPrimitive::from_u16(raw)
                .ok_or(format!("unrecognized info request value: {}", raw))?;

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

        Ok(Self::Go(GoRequest {
            name,
            info_requests,
        }))
    }
}

/// Data parsed from a Go option.
#[derive(Debug, PartialEq)]
pub struct GoRequest {
    pub name: Option<String>,
    pub info_requests: Vec<InfoType>,
}

/// Denotes the type of an information request from a client.
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, FromPrimitive)]
pub enum InfoType {
    Export = NBD_INFO_EXPORT,
    Name = NBD_INFO_NAME,
    Description = NBD_INFO_DESCRIPTION,
    BlockSize = NBD_INFO_BLOCK_SIZE,
}

impl GoRequest {
    /// Writes the reply to a `GoRequest` to `s`.
    ///
    /// # Panics
    /// At least one value must be present in `info_requests`.
    pub async fn reply<W: AsyncWrite + Unpin>(
        &self,
        s: &mut BufWriter<W>,
        export: &Export,
    ) -> io::Result<()> {
        // We always send export info at a minimum.
        assert!(
            !self.info_requests.is_empty(),
            "no info_requests were set for GoRequest reply"
        );

        for info in &self.info_requests {
            // Each info request reply is prefixed with magic and the Go code.
            s.write_u64(REPLYMAGIC).await?;
            s.write_u32(OptionRequestCode::Go as u32).await?;

            match *info {
                InfoType::Export => {
                    // Fixed size of 10 bytes for export info.
                    s.write_u32(NBD_REP_INFO).await?;
                    s.write_u32(2 + 8 + 2).await?;

                    // Export info type, export size.
                    s.write_u16(*info as u16).await?;
                    s.write_u64(export.size).await?;

                    // Always set flags, optionally mark read-only.
                    let mut flags = TransmissionFlags::HAS_FLAGS;
                    if export.readonly {
                        flags |= TransmissionFlags::READ_ONLY;
                    }
                    s.write_u16(flags.bits()).await?;
                }
                InfoType::Name => Self::reply_string(s, *info, &export.name).await?,
                InfoType::Description => Self::reply_string(s, *info, &export.description).await?,
                InfoType::BlockSize => {
                    // Fixed size of 14 bytes for block size.
                    s.write_u32(NBD_REP_INFO).await?;
                    s.write_u32(2 + (4 * 3)).await?;
                    s.write_u16(*info as u16).await?;

                    // TODO(mdlayher): break out minimum/preferred/maximum into
                    // export fields? For now we specify the same value for
                    // each.
                    for _ in 0..3 {
                        s.write_u32(export.block_size).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Writes a string and its associated `InfoType` to `s`.
    async fn reply_string<W: AsyncWrite + Unpin>(
        s: &mut BufWriter<W>,
        info_type: InfoType,
        string: &str,
    ) -> io::Result<()> {
        s.write_u32(NBD_REP_INFO).await?;

        // Add two bytes for length field.
        let length = string.len() as u32 + 2;

        s.write_u32(length).await?;
        s.write_u16(info_type as u16).await?;
        s.write_all(string.as_bytes()).await?;

        Ok(())
    }
}

impl Frame {
    /// Determines if enough data is available to parse a `Frame` of the given
    /// `FrameType` from `src`.
    pub fn check(src: &mut io::Cursor<&[u8]>, frame_type: &FrameType) -> Result<()> {
        match frame_type {
            FrameType::ClientFlags => {
                get_u32(src)?;
                Ok(())
            }
            FrameType::ClientOptions => {
                while src.has_remaining() {
                    next_option(src)?;
                }

                Ok(())
            }
            // Frames the server will send instead of the client.
            //
            // TODO(mdlayher): implement client as well.
            FrameType::ServerHandshake
            | FrameType::ServerOptions
            | FrameType::ServerUnsupportedOptions => Err(format!(
                "protocol error; check: unhandled frame type {:?}",
                frame_type
            )
            .into()),
        }
    }

    /// Parses the next `Frame` according to the given `FrameType`.
    pub fn parse(src: &mut io::Cursor<&[u8]>, frame_type: &FrameType) -> Result<Frame> {
        match frame_type {
            FrameType::ClientFlags => {
                let flags =
                    ClientFlags::from_bits(get_u32(src)?).ok_or("client sent invalid flags")?;

                Ok(Frame::ClientFlags(flags))
            }
            FrameType::ClientOptions => {
                let mut known = Vec::new();
                let mut unknown = Vec::new();
                while src.has_remaining() {
                    // Keep track of both known and unknown options so we can
                    // report errors to the client accordingly.
                    match next_option(src)? {
                        ParsedOption::Known(option) => known.push(option),
                        ParsedOption::Unknown(code) => unknown.push(code),
                    }
                }

                Ok(Frame::ClientOptions(ClientOptions { known, unknown }))
            }
            // Frames the server will send instead of the client.
            //
            // TODO(mdlayher): implement client as well.
            FrameType::ServerHandshake
            | FrameType::ServerOptions
            | FrameType::ServerUnsupportedOptions => Err(format!(
                "protocol error; parse: unhandled frame type {:?}",
                frame_type
            )
            .into()),
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
    Known((OptionRequestCode, OptionRequest)),
    Unknown(u32),
}

// Produces the next `ParsedOption` value from `src` by consuming the client
// option header and inner data.
fn next_option(src: &mut io::Cursor<&[u8]>) -> Result<ParsedOption> {
    if get_u64(src)? != IHAVEOPT {
        return Err("client failed to send option magic".into());
    }

    let option_code = get_u32(src)?;
    let length = get_u32(src)? as usize;

    let option: OptionRequestCode = match FromPrimitive::from_u32(option_code) {
        Some(o) => o,
        None => {
            // We aren't aware of this option, skip over it but note its code as
            // unknown for error reporting.
            skip(src, length)?;
            return Ok(ParsedOption::Unknown(option_code));
        }
    };

    let opt = match option {
        OptionRequestCode::Go => OptionRequest::go(src)?,
    };

    Ok(ParsedOption::Known((option, opt)))
}

/// Contains error information encountered while dealing with Frames.
#[derive(Debug)]
pub enum Error {
    /// A sentinel which indicates more data must be read from a stream to parse
    /// an entire Frame.
    Incomplete,

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

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! frame_tests {
        ($($name:ident: $type:path: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (buf, frame_type, want) = $value;
                let mut src = io::Cursor::new(&buf[..]);

                Frame::check(&mut src, &frame_type).expect("failed to check frame");
                src.set_position(0);

                let got = match Frame::parse(&mut src, &frame_type).expect("failed to parse frame") {
                    $type(v) => v,
                    frame => panic!("expected a {:?} frame, but got: {:?}", frame_type, frame),
                };

                assert!(want.eq(&got), "unexpected {:?} frame contents:\nwant: {:?}\n got: {:?}", frame_type, want, got);
            }
        )*
        }
    }

    frame_tests! {
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
                b'I', b'H', b'A', b'V', b'E', b'O', b'P', b'T',
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
            FrameType::ClientOptions,
            ClientOptions{
                known: vec![(
                    OptionRequestCode::Go,
                    OptionRequest::Go(GoRequest{
                        name: None,
                        info_requests: vec![InfoType::Export],
                    }),
                )],
                unknown: vec![],
            },
        ),

        client_options_go_full: Frame::ClientOptions: (
            [
                // Magic
                b'I', b'H', b'A', b'V', b'E', b'O', b'P', b'T',
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

                // Magic
                b'I', b'H', b'A', b'V', b'E', b'O', b'P', b'T',
                // Unknown
                0, 0, 0, 0xff,
                // Unknown length + bytes
                0, 0, 0, 4,
                0xff, 0xff, 0xff, 0xff,

            ],
            FrameType::ClientOptions,
            ClientOptions{
                known: vec![(
                    OptionRequestCode::Go,
                    OptionRequest::Go(GoRequest{
                        name: Some("test".to_string()),
                        info_requests: vec![
                            InfoType::Export,
                            InfoType::Name,
                            InfoType::Description,
                            InfoType::BlockSize,
                        ],
                    }),
                )],
                unknown: vec![0xff],
            },
        ),
    }
}
