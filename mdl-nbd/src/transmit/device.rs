use std::io::{self, SeekFrom};

use super::frame::{CommandFlags, Errno, Frame, Header};
use crate::frame::{Error, Result};
use crate::{Read, ReadWrite};

/// An abstraction over the capabilities of a given device, such as whether the
/// device is read-only or read/write.
pub(crate) enum Device<R, RW>
where
    R: Read,
    RW: ReadWrite,
{
    Read(R),
    ReadWrite(RW),
}

/// Wrappers for the variants of Device which may or may not be supported for a
/// given concrete type.
impl<R, RW> Device<R, RW>
where
    R: Read,
    RW: ReadWrite,
{
    /// Handles a single I/O operation `req` with the device buffer `buf`.
    pub(crate) fn handle_io<'a>(&mut self, req: &'a Frame, buf: &mut [u8]) -> Option<Frame<'a>> {
        match req {
            // No reply.
            Frame::Disconnect => None,
            Frame::FlushRequest(req) => {
                // Offset and length are reserved and must be zero.
                //
                // Ignore flags for now. Some clients may set FUA or similar
                // it is only valid for writes.
                if req.offset != 0 || req.length != 0 {
                    return Some(Frame::ErrorResponse(Errno::Invalid));
                }

                let errno = self.flush().into();
                Some(Frame::ErrorResponse(errno))
            }
            Frame::ReadRequest(req) => {
                // Ignore flags for now. Some clients may set FUA or similar it
                // is only valid for writes.

                let res = match self.read_at(&mut buf[..req.length], req.offset) {
                    Ok(length) => Frame::ReadResponse(length),
                    Err(err) => Frame::ErrorResponse(err.into()),
                };

                Some(res)
            }
            Frame::TrimRequest(req) => {
                let errno = self.trim(req).into();
                Some(Frame::ErrorResponse(errno))
            }
            Frame::WriteRequest(req, buf) => {
                let errno = self.write_all_at(req, buf).into();
                Some(Frame::ErrorResponse(errno))
            }
            // Frames a client would handle.
            Frame::ErrorResponse(..) | Frame::ReadResponse(..) => todo!(),
        }
    }

    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let length = match self {
            Self::Read(r) => Self::_read_at(r, buf, offset)?,
            Self::ReadWrite(rw) => Self::_read_at(rw, buf, offset)?,
        };

        Ok(length)
    }

    fn _read_at(r: &mut impl Read, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // Make use of fused operation where possible, or fall back to manual
        // read+seek.
        if let Some(res) = r.read_at(buf, offset) {
            res
        } else {
            r.seek(SeekFrom::Start(offset))?;
            r.read(buf)
        }
    }

    fn write_all_at(&mut self, req: &Header, buf: &[u8]) -> Result<()> {
        match self {
            Self::Read(..) => Err(Error::Unsupported),
            Self::ReadWrite(rw) => {
                // Make use of fused operation where possible, or fall back to
                // manual write+seek.
                if let Some(res) = rw.write_all_at(buf, req.offset) {
                    res?
                } else {
                    rw.seek(SeekFrom::Start(req.offset))?;
                    rw.write_all(buf)?
                };

                if req.flags.contains(CommandFlags::FUA) {
                    // Client wants us to flush the write buffer before replying
                    // to its request.
                    rw.flush()?;
                }

                Ok(())
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Self::ReadWrite(rw) => Ok(rw.sync()?),
            Self::Read(..) => Err(Error::Unsupported),
        }
    }

    fn trim(&mut self, req: &Header) -> Result<()> {
        match self {
            Self::Read(..) => Err(Error::Unsupported),
            Self::ReadWrite(rw) => {
                rw.trim(req.offset, req.length as u64)?;
                Ok(())
            }
        }
    }
}
