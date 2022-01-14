use std::io::{self, Cursor};

/// A type which may be used as a read-only device.
pub trait Read: io::Read + io::Seek {
    fn read_at(&self, _buf: &mut [u8], _offset: u64) -> Option<io::Result<usize>> {
        // No-op by default.
        None
    }
}

/// A type which may be used as a read-write device.
pub trait ReadWrite: Read + io::Write {
    fn write_all_at(&self, _buf: &[u8], _offset: u64) -> Option<io::Result<()>> {
        // No-op by default.
        None
    }
}

// Trait implementations for types commonly used with this library. Types copied
// from io::Read and io::Write implementations for these types.

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

    use crate::{Read, ReadWrite};

    // Use fused read/write+seek system calls on UNIX platforms.

    impl Read for File {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Option<io::Result<usize>> {
            Some(FileExt::read_at(self, buf, offset))
        }
    }

    impl Read for &File {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Option<io::Result<usize>> {
            Some(FileExt::read_at(*self, buf, offset))
        }
    }

    impl ReadWrite for File {
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Option<io::Result<()>> {
            Some(FileExt::write_all_at(self, buf, offset))
        }
    }

    impl ReadWrite for &File {
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Option<io::Result<()>> {
            Some(FileExt::write_all_at(*self, buf, offset))
        }
    }
}

#[cfg(not(unix))]
mod unix {
    use std::fs::File;

    use crate::{Read, ReadWrite};

    // Fall back to read/write then seek system calls on non-UNIX platforms.

    impl Read for File {}
    impl Read for &File {}
    impl ReadWrite for File {}
    impl ReadWrite for &File {}
}

impl<T> Read for Cursor<T> where T: AsRef<[u8]> {}

impl ReadWrite for Cursor<&mut [u8]> {}
impl ReadWrite for Cursor<&mut Vec<u8>> {}
impl ReadWrite for Cursor<Vec<u8>> {}
impl ReadWrite for Cursor<Box<[u8]>> {}
