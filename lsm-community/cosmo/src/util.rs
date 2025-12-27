use std::time::{SystemTime, UNIX_EPOCH};

/// Generates a microsecond-precision timestamp representing the current system time.
///
/// Returns the number of microseconds elapsed since the Unix epoch (January 1, 1970 00:00:00 UTC).
/// This function provides a high-precision timestamp suitable for performance measurements,
/// event sequencing, or other time-sensitive operations.
///
/// # Returns
/// * `u64` - The current timestamp in microseconds
///
/// # Panics
/// * Panics with "Time went backwards" if the system clock is set to a time before the Unix epoch,
///   which should not occur under normal operating conditions.
///
/// # Example
///
/// let timestamp = generate_timestamp_us();
/// println!("Current time in microseconds: {}", timestamp);
///
#[allow(dead_code)]
pub fn generate_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_micros() as u64
}
