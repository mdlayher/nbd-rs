use std::io::{self, Cursor};
use tokio::io::{AsyncRead, AsyncWrite};

/// A type which contains a stream of request and response data, typically
/// backed by a network connection.
pub trait Stream: AsyncRead + AsyncWrite + Unpin {}

// Blanket implementation for Stream.
impl<T: AsyncRead + AsyncWrite + Unpin> Stream for T {}

/// A type which may be used as a read-only device.
pub trait Read: io::Read + io::Seek {
    /// Devices may optionally support a fused read and seek operation. By
    /// default, this method returns `None`, causing the device to fall back to
    /// separate read and seek operations.
    fn read_at(&self, _buf: &mut [u8], _offset: u64) -> Option<io::Result<usize>> {
        None
    }
}

/// A type which may be used as a read-write device.
pub trait ReadWrite: Read + io::Write {
    /// Devices may optionally support a fused write and seek operation. By
    /// default, this method returns `None`, causing the device to fall back to
    /// separate write and seek operations.
    fn write_all_at(&self, _buf: &[u8], _offset: u64) -> Option<io::Result<()>> {
        // No-op by default.
        None
    }

    /// Devices may optionally support a durable sync operation for Flush, such
    /// as `fsync` on Linux files. By default, this method returns an
    /// unsupported operation error.
    fn sync(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    /// Devices may optionally support the TRIM operation. By default, this
    /// method is a no-op because the NBD specification specifies that a server
    /// MAY choose to discard bytes at its discretion.
    fn trim(&self, _offset: u64, _length: u64) -> io::Result<u64> {
        // No-op by default, though server may choose to discard bytes.
        Ok(0)
    }
}

// Trait implementations for types commonly used with this library. Types copied
// from io::Read and io::Write implementations for these types.

/// Implements the `ReadWrite` trait's `sync` method for a `File` or `&File`.
macro_rules! impl_file_sync {
    () => {
        /// Durably sync file data using `File::sync_all(self)`.
        fn sync(&mut self) -> io::Result<()> {
            io::Write::flush(self)?;
            File::sync_all(self)
        }
    };
}

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

    use crate::{Read, ReadWrite};

    // Use fused read/write+seek system calls on UNIX platforms.

    impl Read for File {
        /// Performs a fused read and seek system call operation.
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Option<io::Result<usize>> {
            Some(FileExt::read_at(self, buf, offset))
        }
    }

    impl Read for &File {
        /// Performs a fused read and seek system call operation.
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Option<io::Result<usize>> {
            Some(FileExt::read_at(*self, buf, offset))
        }
    }

    impl ReadWrite for File {
        /// Performs a fused write and seek system call operation.
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Option<io::Result<()>> {
            Some(FileExt::write_all_at(self, buf, offset))
        }

        impl_file_sync!();
    }

    impl ReadWrite for &File {
        /// Performs a fused write and seek system call operation.
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Option<io::Result<()>> {
            Some(FileExt::write_all_at(*self, buf, offset))
        }

        impl_file_sync!();
    }

    // Linux only: enable TRIM support.

    #[cfg(target_os = "linux")]
    mod linux {
        // TODO(mdlayher): it would appear FITRIM isn't the right choice. The
        // nbd-server source uses fallocate for files and ioctl BLKDISCARD for
        // devices. For now, do nothing.

        /// The raw Rust equivalent to a C `fstrim_range` struct.
        #[allow(non_camel_case_types)]
        #[derive(Debug)]
        pub struct _fstrim_range {
            pub start: u64,
            pub len: u64,
            pub minlen: u64,
        }

        // Equivalent to Linux's FITRIM ioctl:
        // #define FITRIM _IOWR('X', 121, struct fstrim_range)
        nix::ioctl_write_ptr!(_ioctl_fitrim, 'X', 121, _fstrim_range);

        // #define BLKDISCARD _IO(0x12,119)
        nix::ioctl_write_ptr_bad!(
            _ioctl_blkdiscard,
            nix::request_code_none!(0x12, 119),
            [u64; 2]
        );
    }
}

#[cfg(not(unix))]
mod unix {
    use std::fs::File;

    use crate::{Read, ReadWrite};

    // Fall back to read/write then seek system calls on non-UNIX platforms.

    impl Read for File {}
    impl Read for &File {}

    impl ReadWrite for File {
        impl_file_sync!();
    }

    impl ReadWrite for &File {
        impl_file_sync!();
    }
}

// TODO(mdlayher): it seems like the following blanket implementations for our
// Read and ReadWrite traits would be useful, but we can't specialize for File
// on UNIX/Linux as we do above if we declare blanket implementations. Find a
// solution to this problem. In the meantime, we just implement the traits for
// File and Cursor since those are most likely to be used in practice.
//
// impl<T: io::Read + io::Seek> Read for T {}
// impl<T: Read + io::Write> ReadWrite for T {}

impl<T> Read for Cursor<T> where T: AsRef<[u8]> {}

impl<T> ReadWrite for Cursor<T>
where
    T: AsRef<[u8]>,
    Cursor<T>: io::Write,
{
}
