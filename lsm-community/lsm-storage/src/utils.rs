use std::time::{SystemTime, UNIX_EPOCH};

/// Generate a 64-bit timestamp representing the current time in milliseconds since Unix epoch.
///
/// # Returns
/// * `u64` - Current timestamp in milliseconds since January 1, 1970 00:00:00 UTC
///
/// # Panics
/// * Panics if the system time is before the Unix epoch (should never happen in practice)
pub fn generate_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_millis() as u64
}

/// Generate a 64-bit timestamp representing the current time in microseconds since Unix epoch.
///
/// # Returns
/// * `u64` - Current timestamp in microseconds since January 1, 1970 00:00:00 UTC
///
/// # Panics
/// * Panics if the system time is before the Unix epoch (should never happen in practice)
pub fn generate_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_micros() as u64
}

/// Generate a 64-bit timestamp representing the current time in seconds since Unix epoch.
///
/// # Returns
/// * `u64` - Current timestamp in seconds since January 1, 1970 00:00:00 UTC
///
/// # Panics
/// * Panics if the system time is before the Unix epoch (should never happen in practice)
pub fn generate_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_secs()
}
