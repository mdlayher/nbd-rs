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

        /// Performs a TRIM operation using the `FITRIM` `ioctl`.
        #[cfg(target_os = "linux")]
        fn trim(&self, offset: u64, length: u64) -> io::Result<u64> {
            use std::os::unix::io::AsRawFd;
            linux::ioctl_fitrim(self.as_raw_fd(), offset, length)
        }
    }

    impl ReadWrite for &File {
        /// Performs a fused write and seek system call operation.
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Option<io::Result<()>> {
            Some(FileExt::write_all_at(*self, buf, offset))
        }

        /// Performs a TRIM operation using the `FITRIM` `ioctl`.
        #[cfg(target_os = "linux")]
        fn trim(&self, offset: u64, length: u64) -> io::Result<u64> {
            use std::os::unix::io::AsRawFd;
            linux::ioctl_fitrim(self.as_raw_fd(), offset, length)
        }
    }

    // Linux only: enable TRIM support.

    #[cfg(target_os = "linux")]
    mod linux {
        use log::debug;
        use nix::errno::Errno;
        use std::io;

        /// Invokes the FITRIM ioctl on `fd` starting at `offset` for `length`
        /// bytes. The `minlen` field on `fstrim_range` is always set to 0.
        pub fn ioctl_fitrim(fd: i32, offset: u64, length: u64) -> io::Result<u64> {
            // "Any extent less than minlen bytes will be ignored in this
            // process. The operation can be run over the entire device by
            // setting start to zero and len to ULLONG_MAX."
            //
            // Reference: https://lwn.net/Articles/417809/.
            let data = &[_fstrim_range {
                start: offset,
                len: length,
                // Trim everything.
                minlen: 0,
            }];

            // TODO(mdlayher): test with a real SSD to confirm these return and
            // error values.

            debug!("trim  in: {data:?}");
            let res = unsafe { _ioctl_fitrim(fd, data) };
            debug!("trim out: {res:?} {data:?}");

            match res {
                // The number of bytes trimmed.
                Ok(_) => Ok(data[0].len),
                // Permission denied.
                Err(Errno::EPERM) => Err(io::ErrorKind::PermissionDenied.into()),
                // This device, filesystem, or mounted filesystem configuration
                // does not support TRIM.
                Err(Errno::ENOTTY) | Err(Errno::EOPNOTSUPP) | Err(Errno::EROFS) => {
                    Err(io::ErrorKind::Unsupported.into())
                }
                // Fallback.
                Err(_) => Err(io::ErrorKind::InvalidInput.into()),
            }
        }

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
        nix::ioctl_write_buf!(_ioctl_fitrim, 'X', 121, _fstrim_range);
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
