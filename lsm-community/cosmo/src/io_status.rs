// io_stats.rs - IO Statistics Toolkit
use std::fmt;
use std::time::{Duration, Instant};

/// Represents Input/Output statistics collected from system monitoring.
///
/// This structure contains comprehensive IO metrics including both the amount of data
/// transferred and the number of system calls made for read and write operations.
/// It provides methods to calculate differences, totals, and determine if any IO
/// operations have occurred during measurement periods.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IOStats {
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_syscalls: u64,
    pub write_syscalls: u64,
}

impl IOStats {
    /// Creates a new instance of `IOStats` with all fields initialized to zero.
    ///
    /// This constructor function initializes an empty IO statistics structure that
    /// can be used as a baseline for measurements or as a starting point for
    /// accumulating IO metrics.
    ///
    /// # Returns
    /// A new `IOStats` instance with all counters set to zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculates the difference between two IO statistics instances.
    ///
    /// This method computes the delta between the current instance and another
    /// `IOStats` instance by subtracting the other's values from this instance's values.
    /// It uses saturating subtraction to prevent underflow, ensuring all results
    /// are non-negative.
    ///
    /// # Arguments
    /// - `other`: A reference to another `IOStats` instance to subtract from this one.
    ///
    /// # Returns
    /// A new `IOStats` instance containing the computed differences.
    pub fn diff(&self, other: &IOStats) -> IOStats {
        IOStats {
            read_bytes: self.read_bytes.saturating_sub(other.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(other.write_bytes),
            read_syscalls: self.read_syscalls.saturating_sub(other.read_syscalls),
            write_syscalls: self.write_syscalls.saturating_sub(other.write_syscalls),
        }
    }

    /// Calculates the total number of bytes transferred in both read and write operations.
    ///
    /// This method provides a convenient way to get the aggregate data transfer
    /// amount by summing the read_bytes and write_bytes fields.
    ///
    /// # Returns
    /// The total number of bytes transferred as a `u64` value.
    pub fn total_bytes(&self) -> u64 {
        self.read_bytes + self.write_bytes
    }

    /// Calculates the total number of system calls made for both read and write operations.
    ///
    /// This method provides the aggregate count of all IO-related system calls
    /// by summing the read_syscalls and write_syscalls fields.
    ///
    /// # Returns
    /// The total number of system calls as a `u64` value.
    pub fn total_syscalls(&self) -> u64 {
        self.read_syscalls + self.write_syscalls
    }

    /// Determines whether any IO operations have been recorded in this instance.
    ///
    /// This method checks if either bytes were transferred or system calls were made,
    /// providing a quick way to determine if any IO activity occurred during the
    /// measurement period.
    ///
    /// # Returns
    /// `true` if any IO activity was detected, `false` otherwise.
    pub fn has_io(&self) -> bool {
        self.total_bytes() > 0 || self.total_syscalls() > 0
    }
}

impl fmt::Display for IOStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
               "IO Statistics: Read {} bytes ({} syscalls), Write {} bytes ({} syscalls), Total {} bytes",
               self.read_bytes, self.read_syscalls,
               self.write_bytes, self.write_syscalls,
               self.total_bytes()
        )
    }
}

/// Represents the complete execution result including return value, duration, and IO statistics.
///
/// This structure encapsulates all information about a measured function execution,
/// including the original return value, the time taken to execute, and detailed
/// IO statistics collected during the execution period.
///
/// # Type Parameters
/// - `T`: The type of the return value from the measured function.
pub struct ExecutionResult<T> {
    pub result: T,
    pub duration: Duration,
    pub io_stats: IOStats,
}

impl<T> ExecutionResult<T> {
    /// Prints a detailed execution report including timing, IO statistics, and throughput metrics.
    ///
    /// This method outputs a comprehensive report that includes execution duration,
    /// detailed IO statistics, calculated throughput (if IO occurred), and the
    /// function's return value. The report is formatted for easy readability.
    ///
    /// # Arguments
    /// - `operation_name`: A string slice representing the name of the measured operation.
    ///
    /// # Type Constraints
    /// - `T`: Must implement `fmt::Debug` to display the return value.
    pub fn print_report(&self, operation_name: &str)
    where
        T: fmt::Debug,
    {
        println!("=== {} Execution Report ===", operation_name);
        println!(
            "Execution Time: {:?} ({} us)",
            self.duration,
            self.duration.as_micros()
        );
        println!("{}", self.io_stats);
        if self.io_stats.has_io() {
            let throughput = if self.duration.as_secs_f64() > 0.0 {
                self.io_stats.total_bytes() as f64 / self.duration.as_secs_f64() / 1024.0 / 1024.0
            } else {
                0.0
            };
            println!("IO Throughput: {:.2} MB/s", throughput);
        }
        println!("Return Value: {:?}", self.result);
        println!();
    }

    /// Prints a concise execution summary with essential timing and IO information.
    ///
    /// This method provides a single-line summary containing the operation name,
    /// execution duration, and total IO bytes transferred, suitable for quick
    /// monitoring or logging purposes.
    ///
    /// # Arguments
    /// - `operation_name`: A string slice representing the name of the measured operation.
    pub fn print_summary(&self, operation_name: &str) {
        println!(
            "{}: Time {:?}, IO {} bytes",
            operation_name,
            self.duration,
            self.io_stats.total_bytes()
        );
    }
}

/// Provides functionality for collecting and measuring IO statistics during function execution.
///
/// This collector serves as the primary interface for measuring IO operations performed
/// by functions or code blocks. It interfaces with the operating system to gather
/// accurate IO metrics and provides convenient methods for function measurement
/// and reporting.
pub struct IOStatsCollector;

impl IOStatsCollector {
    /// Retrieves the current process's IO statistics from the Linux proc filesystem.
    ///
    /// This method reads IO metrics from `/proc/self/io` which provides detailed
    /// information about the current process's IO operations including bytes
    /// transferred and system call counts. This implementation is specific to
    /// Linux systems that support the proc filesystem.
    ///
    /// # Returns
    /// A `Result` containing `IOStats` if successful, or an `std::io::Error` if
    /// the proc filesystem cannot be accessed or parsed.
    #[cfg(target_os = "linux")]
    fn get_current_io_stats() -> std::io::Result<IOStats> {
        use std::fs;

        let io_content = fs::read_to_string("/proc/self/io")?;
        let mut stats = IOStats::new();

        for line in io_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                if let Ok(value) = parts[1].parse::<u64>() {
                    match parts[0] {
                        "read_bytes:" => stats.read_bytes = value,
                        "write_bytes:" => stats.write_bytes = value,
                        "syscr:" => stats.read_syscalls = value,
                        "syscw:" => stats.write_syscalls = value,
                        _ => {}
                    }
                }
            }
        }

        Ok(stats)
    }

    /// Returns empty IO statistics for non-Linux systems where detailed IO monitoring is not available.
    ///
    /// This method provides a fallback implementation for operating systems that
    /// do not support detailed process IO statistics monitoring. It returns an
    /// empty `IOStats` instance to maintain API consistency across platforms.
    ///
    /// # Returns
    /// A `Result` containing an empty `IOStats` instance.
    #[cfg(not(target_os = "linux"))]
    fn get_current_io_stats() -> std::io::Result<IOStats> {
        Ok(IOStats::new())
    }

    /// Measures the execution time and IO statistics of a given function.
    ///
    /// This method captures IO statistics before and after function execution,
    /// calculates the difference to determine the IO operations performed by
    /// the function, and measures the total execution time. It provides a
    /// comprehensive view of both performance and IO behavior.
    ///
    /// # Arguments
    /// - `f`: A closure or function to be measured that takes no parameters.
    ///
    /// # Type Parameters
    /// - `F`: The type of the function/closure, must implement `FnOnce() -> R`.
    /// - `R`: The return type of the measured function.
    ///
    /// # Returns
    /// An `ExecutionResult<R>` containing the function's return value, execution
    /// duration, and IO statistics collected during execution.
    pub fn measure<F, R>(f: F) -> ExecutionResult<R>
    where
        F: FnOnce() -> R,
    {
        let start_io = Self::get_current_io_stats().unwrap_or_default();
        let start_time = Instant::now();

        let result = f();

        let duration = start_time.elapsed();
        let end_io = Self::get_current_io_stats().unwrap_or_default();

        let io_stats = end_io.diff(&start_io);

        ExecutionResult {
            result,
            duration,
            io_stats,
        }
    }

    /// Measures function execution and automatically prints a concise summary report.
    ///
    /// This convenience method combines measurement with immediate reporting,
    /// providing a quick way to measure and display basic performance metrics
    /// for a function without manual report generation.
    ///
    /// # Arguments
    /// - `operation_name`: A string slice describing the operation being measured.
    /// - `f`: A closure or function to be measured that takes no parameters.
    ///
    /// # Type Parameters
    /// - `F`: The type of the function/closure, must implement `FnOnce() -> R`.
    /// - `R`: The return type of the measured function.
    ///
    /// # Returns
    /// The return value of the measured function.
    pub fn measure_and_print<F, R>(operation_name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let execution_result = Self::measure(f);
        execution_result.print_summary(operation_name);
        execution_result.result
    }

    /// Measures function execution and automatically prints a detailed report including return values.
    ///
    /// This method provides comprehensive measurement and reporting functionality,
    /// including detailed IO statistics, timing information, and the function's
    /// return value. It requires the return type to implement Debug for value display.
    ///
    /// # Arguments
    /// - `operation_name`: A string slice describing the operation being measured.
    /// - `f`: A closure or function to be measured that takes no parameters.
    ///
    /// # Type Parameters
    /// - `F`: The type of the function/closure, must implement `FnOnce() -> R`.
    /// - `R`: The return type of the measured function, must implement `fmt::Debug`.
    ///
    /// # Returns
    /// The return value of the measured function.
    pub fn measure_and_print_detailed<F, R>(operation_name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
        R: fmt::Debug,
    {
        let execution_result = Self::measure(f);
        execution_result.print_report(operation_name);
        execution_result.result
    }
}

/// Convenience macro for measuring IO statistics with automatic summary reporting.
///
/// This macro provides a simple way to wrap code blocks and automatically
/// measure their IO statistics and execution time. It prints a concise summary
/// upon completion and returns the result of the measured code block.
///
/// # Arguments
/// - `$name`: An expression that evaluates to a string slice for the operation name.
/// - `$code`: A code block to be measured and executed.
///
/// # Returns
/// The return value of the executed code block.
#[macro_export]
macro_rules! measure_io {
    ($name:expr, $code:block) => {{
        IOStatsCollector::measure_and_print($name, || $code)
    }};
}

/// Convenience macro for measuring IO statistics with detailed reporting including return values.
///
/// This macro provides comprehensive measurement and reporting functionality,
/// automatically measuring execution time and IO statistics while displaying
/// detailed information including the return value. The return type must
/// implement the Debug trait for value display.
///
/// # Arguments
/// - `$name`: An expression that evaluates to a string slice for the operation name.
/// - `$code`: A code block to be measured and executed.
///
/// # Returns
/// The return value of the executed code block.
#[macro_export]
macro_rules! measure_io_detailed {
    ($name:expr, $code:block) => {{
        IOStatsCollector::measure_and_print_detailed($name, || $code)
    }};
}

/// Checks and reports the current system's support for IO statistics collection.
///
/// This function examines the runtime environment to determine whether accurate
/// IO statistics can be collected. On Linux systems, it verifies access to the
/// proc filesystem. On other systems, it reports limited functionality.
/// The function prints diagnostic information to help users understand the
/// capabilities and limitations of IO measurement on their platform.
pub fn check_io_support() {
    #[cfg(target_os = "linux")]
    {
        match std::fs::metadata("/proc/self/io") {
            Ok(_) => println!("✓ Precise IO statistics supported (Linux /proc/self/io)"),
            Err(_) => println!("⚠ /proc/self/io not accessible, IO statistics may be inaccurate"),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        println!("ℹ Current system does not support precise IO statistics, only execution time will be shown");
        println!("  For complete IO statistics, please run on a Linux system");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_io_stats_diff() {
        let stats1 = IOStats {
            read_bytes: 100,
            write_bytes: 200,
            read_syscalls: 10,
            write_syscalls: 20,
        };

        let stats2 = IOStats {
            read_bytes: 150,
            write_bytes: 300,
            read_syscalls: 15,
            write_syscalls: 25,
        };

        let diff = stats2.diff(&stats1);
        assert_eq!(diff.read_bytes, 50);
        assert_eq!(diff.write_bytes, 100);
        assert_eq!(diff.total_bytes(), 150);
    }

    #[test]
    fn test_measure_function() {
        let result = IOStatsCollector::measure(|| {
            // Simulate some work
            std::thread::sleep(Duration::from_millis(1));
            42
        });

        assert_eq!(result.result, 42);
        assert!(result.duration.as_millis() >= 1);
    }

    #[test]
    fn test_measure_with_file_io() {
        let result = IOStatsCollector::measure(|| {
            let mut file = File::create("test_io.txt").unwrap();
            file.write_all(b"Hello, World!").unwrap();
            file.sync_all().unwrap();
            std::fs::remove_file("test_io.txt").unwrap();
            "file_operation_done"
        });

        println!("File IO test: {}", result.io_stats);
        // On Linux systems, should detect bytes written
        #[cfg(target_os = "linux")]
        assert!(result.io_stats.write_bytes > 0 || result.io_stats.write_syscalls > 0);
    }
}
