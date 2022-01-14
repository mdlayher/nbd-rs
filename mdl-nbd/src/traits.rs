use crate::Result;
use std::fs::File;
use std::io::{self, Cursor};

/// A type which may be used as a read-only device.
pub trait Read: io::Read + io::Seek {}

/// A type which may be used as a read-write device.
pub trait ReadWrite: Read + io::Write {
    fn trim(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// Optional extensions for a read-write device which may or may not be
/// implemented depending on the concrete type.
pub trait ReadWriteTrimExt: ReadWrite {
    fn trim(&mut self) -> Result<usize>;
}

// Trait implementations for types commonly used with this library. Types copied
// from io::Read and io::Write implementations for these types.

impl Read for File {}
impl Read for &File {}
impl ReadWrite for File {}
impl ReadWrite for &File {}

impl<T> Read for Cursor<T> where T: AsRef<[u8]> {}
impl ReadWrite for Cursor<&mut [u8]> {}
impl ReadWrite for Cursor<&mut Vec<u8>> {}
impl ReadWrite for Cursor<Vec<u8>> {}
impl ReadWrite for Cursor<Box<[u8]>> {}
