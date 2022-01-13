//! NBD protocol constants for use in other crates.
//! Reference: <https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md>.

#![allow(dead_code)]

/// A symbolic constant for 1 MiB.
#[allow(non_upper_case_globals)]
pub const MiB: u64 = 1 << 20;

// Handshake magic.
pub const NBDMAGIC: u64 = 0x4e42444d41474943;
pub const IHAVEOPT: u64 = 0x49484156454F5054;
pub const REPLYMAGIC: u64 = 0x3e889045565a9;

pub const NBDMAGIC_BUF: &[u8] = b"NBDMAGIC";
pub const IHAVEOPT_BUF: &[u8] = b"IHAVEOPT";
pub const REPLYMAGIC_BUF: &[u8] = &[0x00, 0x03, 0xe8, 0x89, 0x04, 0x55, 0x65, 0xa9];

// Handshake option requests.
pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;
pub const NBD_OPT_STARTTLS: u32 = 5;
pub const NBD_OPT_INFO: u32 = 6;
pub const NBD_OPT_GO: u32 = 7;
pub const NBD_OPT_STRUCTURED_REPLY: u32 = 8;

// Handshake option success responses.
pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_INFO: u32 = 3;

// Handshake option error responses.
pub const NBD_REP_ERR_UNSUP: u32 = 1 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_POLICY: u32 = 2 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_INVALID: u32 = 3 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_PLATFORM: u32 = 4 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_TLS_REQD: u32 = 5 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_UNKNOWN: u32 = 6 | NBD_REP_FLAG_ERROR;
pub const NBD_REP_ERR_BLOCK_SIZE_REQD: u32 = 8 | NBD_REP_FLAG_ERROR;
const NBD_REP_FLAG_ERROR: u32 = 1 << 31;

// Handshake info types.
pub const NBD_INFO_EXPORT: u16 = 0;
pub const NBD_INFO_NAME: u16 = 1;
pub const NBD_INFO_DESCRIPTION: u16 = 2;
pub const NBD_INFO_BLOCK_SIZE: u16 = 3;

// Server handshake flags.
pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client handshake flags.
pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = NBD_FLAG_FIXED_NEWSTYLE as u32;
pub const NBD_FLAG_C_NO_ZEROES: u32 = NBD_FLAG_NO_ZEROES as u32;

// Transmission magic.
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;
pub const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

// Transmission commands.
pub const NBD_CMD_READ: u16 = 0;
pub const NBD_CMD_WRITE: u16 = 1;
pub const NBD_CMD_DISC: u16 = 2;
pub const NBD_CMD_FLUSH: u16 = 3;
pub const NBD_CMD_TRIM: u16 = 4;
pub const NBD_CMD_WRITE_ZEROES: u16 = 6;
pub const NBD_CMD_RESIZE: u16 = 8;

// Transmission negotiation flags.
pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
pub const NBD_FLAG_ROTATIONAL: u16 = 1 << 4;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
pub const NBD_FLAG_CAN_MULTI_CONN: u16 = 1 << 8;
pub const NBD_FLAG_SEND_RESIZE: u16 = 1 << 9;

// Transmission command flags.
pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;

// Transmission error numbers.
pub const NBD_OK: u32 = 0;
pub const NBD_EPERM: u32 = 1;
pub const NBD_EIO: u32 = 5;
pub const NBD_ENOMEM: u32 = 12;
pub const NBD_EINVAL: u32 = 22;
pub const NBD_ENOSPC: u32 = 28;
pub const NBD_EOVERFLOW: u32 = 75;
pub const NBD_ENOTSUP: u32 = 95;
pub const NBD_ESHUTDOWN: u32 = 108;
