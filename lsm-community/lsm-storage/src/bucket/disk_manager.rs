use std::fs::File;
use std::io::Result;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// Configuration options for disk I/O
#[derive(Debug, Clone, Copy)]
pub struct DiskManagerOptions {
    /// Use direct I/O to bypass OS cache
    pub use_direct_io: bool,
    /// Queue depth for async I/O (Linux io_uring)
    pub queue_depth: u32,
}

impl Default for DiskManagerOptions {
    fn default() -> Self {
        #[cfg(not(target_os = "windows"))]
        {
            return Self {
                use_direct_io: true,
                queue_depth: 256,
            };
        }

        #[cfg(target_os = "windows")]
        Self {
            use_direct_io: false,
            queue_depth: 256,
        }
    }
}

/// Platform-agnostic disk manager
/// Automatically uses the best I/O backend for each platform:
/// - Linux: io_uring for async batched I/O
/// - macOS: fcntl optimizations + pread
/// - Windows: standard I/O with optional FILE_FLAG_NO_BUFFERING
#[derive(Debug)]
pub struct BktDiskManager {
    file: File,
    size: u64,
    #[cfg(target_os = "linux")]
    io_uring: Option<std::sync::Arc<std::sync::Mutex<IoUring>>>,
}

impl BktDiskManager {
    /// Create a new disk manager with default options
    pub fn new(path: &Path) -> Result<Self> {
        Self::with_options(path, DiskManagerOptions::default())
    }

    /// Create a new disk manager with custom options
    pub fn with_options(path: &Path, options: DiskManagerOptions) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::new_linux(path, options)
        }

        #[cfg(target_os = "macos")]
        {
            Self::new_macos(path, options)
        }

        #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
        {
            Self::new_unix(path, options)
        }

        #[cfg(windows)]
        {
            Self::new_windows(path, options)
        }
    }

    /// Create a new file and return a disk manager
    pub fn create(path: &Path, data: &[u8]) -> Result<Self> {
        Self::create_with_options(path, data, DiskManagerOptions::default())
    }

    /// Create a new file with custom options
    pub fn create_with_options(
        path: &Path,
        data: &[u8],
        options: DiskManagerOptions,
    ) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::create_linux(path, data, options)
        }

        #[cfg(target_os = "macos")]
        {
            Self::create_macos(path, data, options)
        }

        #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
        {
            Self::create_unix(path, data, options)
        }

        #[cfg(windows)]
        {
            Self::create_windows(path, data, options)
        }
    }

    /// Read data from file at given offset
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        #[cfg(target_os = "linux")]
        {
            if self.io_uring.is_some() {
                return self.read_uring_single(offset, len);
            }
        }

        // Default synchronous read for all platforms
        self.read_sync(offset, len)
    }

    /// Write data to file at given offset
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            if self.io_uring.is_some() {
                return self.write_uring_single(offset, data);
            }
        }

        // Default synchronous write
        self.write_sync(offset, data)
    }

    /// Batch read multiple regions (optimized on Linux with io_uring)
    pub fn read_batch(&self, requests: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        #[cfg(target_os = "linux")]
        {
            if self.io_uring.is_some() {
                return self.read_batch_uring(requests);
            }
        }

        // Fallback: sequential reads for non-Linux or non-io_uring systems
        requests
            .iter()
            .map(|(offset, len)| self.read(*offset, *len))
            .collect()
    }

    /// Batch write multiple regions (optimized on Linux with io_uring)
    pub fn write_batch(&self, requests: &[(u64, Vec<u8>)]) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            if self.io_uring.is_some() {
                return self.write_batch_uring(requests);
            }
        }

        // Fallback: sequential writes
        for (offset, data) in requests {
            self.write(*offset, data)?;
        }
        Ok(())
    }

    /// Get file size
    pub fn size(&self) -> u64 {
        self.size
    }

    #[cfg(target_os = "linux")]
    fn new_linux(path: &Path, options: DiskManagerOptions) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        if options.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let size = file.metadata()?.len();

        // Initialize io_uring for async I/O
        let io_uring = Some(std::sync::Arc::new(std::sync::Mutex::new(IoUring::new(
            options.queue_depth,
        )?)));

        // Advise kernel for random access (typical for LSM-Tree)
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, size as i64, libc::POSIX_FADV_RANDOM);
        }

        Ok(Self {
            file,
            size,
            io_uring,
        })
    }

    #[cfg(target_os = "linux")]
    fn create_linux(path: &Path, data: &[u8], options: DiskManagerOptions) -> Result<Self> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut write_opts = OpenOptions::new();
        write_opts.write(true).create(true).truncate(true);

        if options.use_direct_io {
            write_opts.custom_flags(libc::O_DIRECT | libc::O_SYNC);
        }

        let mut write_file = write_opts.open(path)?;
        write_file.write_all(data)?;
        write_file.sync_all()?;
        drop(write_file);

        Self::new_linux(path, options)
    }

    #[cfg(target_os = "linux")]
    fn read_uring_single(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        let fd = types::Fd(self.file.as_raw_fd());

        let ring = self.io_uring.as_ref().unwrap();
        let mut ring = ring.lock().unwrap();

        let read_op = opcode::Read::new(fd, data.as_mut_ptr(), len as u32).offset(offset);

        unsafe {
            ring.submission()
                .push(&read_op.build().user_data(0))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No completion"))?;

        let result = cqe.result();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(-result));
        }

        if result as usize != len as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Short read",
            ));
        }

        Ok(data)
    }

    #[cfg(target_os = "linux")]
    fn write_uring_single(&self, offset: u64, data: &[u8]) -> Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let ring = self.io_uring.as_ref().unwrap();
        let mut ring = ring.lock().unwrap();

        let write_op = opcode::Write::new(fd, data.as_ptr(), data.len() as u32).offset(offset);

        unsafe {
            ring.submission()
                .push(&write_op.build().user_data(0))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }

        ring.submit_and_wait(1)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No completion"))?;

        let result = cqe.result();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(-result));
        }

        if result as usize != data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "Short write",
            ));
        }

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn read_batch_uring(&self, requests: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        let fd = types::Fd(self.file.as_raw_fd());
        let ring = self.io_uring.as_ref().unwrap();
        let mut ring = ring.lock().unwrap();

        let mut buffers: Vec<Vec<u8>> = requests
            .iter()
            .map(|(_, len)| vec![0u8; *len as usize])
            .collect();

        // Submit all read operations
        unsafe {
            let mut sq = ring.submission();
            for (i, ((offset, len), buf)) in requests.iter().zip(buffers.iter_mut()).enumerate() {
                let read_op = opcode::Read::new(fd, buf.as_mut_ptr(), *len as u32).offset(*offset);

                sq.push(&read_op.build().user_data(i as u64))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }
        }

        ring.submit_and_wait(requests.len())?;

        // Collect results
        let mut results = vec![None; requests.len()];
        for _ in 0..requests.len() {
            if let Some(cqe) = ring.completion().next() {
                let idx = cqe.user_data() as usize;
                let result = cqe.result();

                if result < 0 {
                    return Err(std::io::Error::from_raw_os_error(-result));
                }

                results[idx] = Some(result);
            }
        }

        // Verify all completed
        for (i, result) in results.iter().enumerate() {
            match result {
                Some(n) if *n as usize == requests[i].1 as usize => {}
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Read failed for request {}", i),
                    ));
                }
            }
        }

        Ok(buffers)
    }

    #[cfg(target_os = "linux")]
    fn write_batch_uring(&self, requests: &[(u64, Vec<u8>)]) -> Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let ring = self.io_uring.as_ref().unwrap();
        let mut ring = ring.lock().unwrap();

        // Submit all write operations
        unsafe {
            let mut sq = ring.submission();
            for (i, (offset, data)) in requests.iter().enumerate() {
                let write_op =
                    opcode::Write::new(fd, data.as_ptr(), data.len() as u32).offset(*offset);

                sq.push(&write_op.build().user_data(i as u64))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }
        }

        ring.submit_and_wait(requests.len())?;

        // Collect results
        for _ in 0..requests.len() {
            if let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result < 0 {
                    return Err(std::io::Error::from_raw_os_error(-result));
                }
            }
        }

        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn new_macos(path: &Path, options: DiskManagerOptions) -> Result<Self> {
        let file = File::options().read(true).write(true).open(path)?;

        if options.use_direct_io {
            unsafe {
                let fd = file.as_raw_fd();
                // F_NOCACHE: disable caching
                libc::fcntl(fd, 48, 1);
                // F_RDAHEAD: disable read-ahead for random access
                libc::fcntl(fd, 45, 0);
            }
        }

        let size = file.metadata()?.len();

        Ok(Self { file, size })
    }

    #[cfg(target_os = "macos")]
    fn create_macos(path: &Path, data: &[u8], options: DiskManagerOptions) -> Result<Self> {
        use std::io::Write;

        let mut write_file = File::create(path)?;

        if options.use_direct_io {
            unsafe {
                let fd = write_file.as_raw_fd();
                libc::fcntl(fd, 48, 1); // F_NOCACHE
            }
        }

        write_file.write_all(data)?;
        write_file.sync_all()?;
        drop(write_file);

        Self::new_macos(path, options)
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    fn new_unix(path: &Path, options: DiskManagerOptions) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        if options.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let size = file.metadata()?.len();

        Ok(Self { file, size })
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    fn create_unix(path: &Path, data: &[u8], options: DiskManagerOptions) -> Result<Self> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut write_opts = OpenOptions::new();
        write_opts.write(true).create(true).truncate(true);

        if options.use_direct_io {
            write_opts.custom_flags(libc::O_DIRECT | libc::O_SYNC);
        }

        let mut write_file = write_opts.open(path)?;
        write_file.write_all(data)?;
        write_file.sync_all()?;
        drop(write_file);

        Self::new_unix(path, options)
    }

    #[cfg(windows)]
    fn new_windows(path: &Path, options: DiskManagerOptions) -> Result<Self> {
        use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};

        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        if options.use_direct_io {
            use winapi::um::winbase::FILE_FLAG_NO_BUFFERING;
            opts.custom_flags(FILE_FLAG_NO_BUFFERING);
        }

        let file = opts.open(path)?;
        let size = file.metadata()?.len();

        Ok(Self { file, size })
    }

    #[cfg(windows)]
    fn create_windows(path: &Path, data: &[u8], options: DiskManagerOptions) -> Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::{fs::OpenOptions, io::Write};
        use winapi::um::winbase::{FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH};

        let mut write_opts = OpenOptions::new();
        write_opts.write(true).create(true).truncate(true);

        if options.use_direct_io {
            write_opts.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
        }

        let mut write_file = write_opts.open(path)?;
        write_file.write_all(data)?;
        write_file.sync_all()?;
        drop(write_file);

        Self::new_windows(path, options)
    }

    fn read_sync(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];

        #[cfg(unix)]
        {
            self.file.read_exact_at(&mut data, offset)?;
        }

        #[cfg(windows)]
        {
            self.file.seek_read(&mut data, offset)?;
        }

        Ok(data)
    }

    fn write_sync(&self, offset: u64, data: &[u8]) -> Result<()> {
        #[cfg(unix)]
        {
            self.file.write_all_at(data, offset)?;
        }

        #[cfg(windows)]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
        }

        Ok(())
    }
}

/// Pool for managing multiple files with shared I/O resources
pub struct DiskManagerPool {
    managers: Vec<BktDiskManager>,
    #[cfg(target_os = "linux")]
    shared_ring: Option<std::sync::Arc<std::sync::Mutex<IoUring>>>,
}

impl DiskManagerPool {
    /// Create a pool of disk managers for multiple files
    pub fn new(paths: Vec<&Path>) -> Result<Self> {
        Self::with_options(paths, DiskManagerOptions::default())
    }

    /// Create a pool with custom options
    pub fn with_options(paths: Vec<&Path>, options: DiskManagerOptions) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let shared_ring = Some(std::sync::Arc::new(std::sync::Mutex::new(IoUring::new(
                options.queue_depth,
            )?)));

            let managers = paths
                .iter()
                .map(|path| {
                    let file = File::options().read(true).write(true).open(path)?;
                    let size = file.metadata()?.len();

                    Ok(BktDiskManager {
                        file,
                        size,
                        io_uring: shared_ring.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Self {
                managers,
                shared_ring,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let managers = paths
                .iter()
                .map(|path| BktDiskManager::with_options(path, options))
                .collect::<Result<Vec<_>>>()?;

            Ok(Self { managers })
        }
    }

    /// Read from multiple files in batch
    /// requests: (file_index, offset, length)
    pub fn read_batch_multi(&self, requests: &[(usize, u64, u64)]) -> Result<Vec<Vec<u8>>> {
        #[cfg(target_os = "linux")]
        {
            if let Some(ring) = &self.shared_ring {
                return self.read_batch_multi_uring(requests, ring);
            }
        }

        // Fallback: sequential reads
        requests
            .iter()
            .map(|(file_idx, offset, len)| self.managers[*file_idx].read(*offset, *len))
            .collect()
    }

    #[cfg(target_os = "linux")]
    fn read_batch_multi_uring(
        &self,
        requests: &[(usize, u64, u64)],
        ring: &std::sync::Arc<std::sync::Mutex<IoUring>>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut ring = ring.lock().unwrap();
        let mut buffers: Vec<Vec<u8>> = requests
            .iter()
            .map(|(_, _, len)| vec![0u8; *len as usize])
            .collect();

        // Submit all operations
        unsafe {
            let mut sq = ring.submission();
            for (i, ((file_idx, offset, len), buf)) in
                requests.iter().zip(buffers.iter_mut()).enumerate()
            {
                let fd = types::Fd(self.managers[*file_idx].file.as_raw_fd());
                let read_op = opcode::Read::new(fd, buf.as_mut_ptr(), *len as u32).offset(*offset);

                sq.push(&read_op.build().user_data(i as u64))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }
        }

        ring.submit_and_wait(requests.len())?;

        // Collect completions
        for _ in 0..requests.len() {
            if let Some(cqe) = ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }
        }

        Ok(buffers)
    }

    /// Get a reference to a specific disk manager
    pub fn get(&self, index: usize) -> Option<&BktDiskManager> {
        self.managers.get(index)
    }

    /// Get the number of files in the pool
    pub fn len(&self) -> usize {
        self.managers.len()
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.managers.is_empty()
    }
}
