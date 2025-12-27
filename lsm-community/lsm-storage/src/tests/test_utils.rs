#[cfg(test)]
mod test_utils {
    use crate::utils::{generate_timestamp, generate_timestamp_micros, generate_timestamp_secs};

    #[test]
    fn test_generate_timestamp() {
        let ts1 = generate_timestamp();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts2 = generate_timestamp();

        assert!(ts2 > ts1);
        assert!(ts2 - ts1 >= 10);
    }

    #[test]
    fn test_generate_timestamp_micros() {
        let ts1 = generate_timestamp_micros();
        std::thread::sleep(std::time::Duration::from_micros(100));
        let ts2 = generate_timestamp_micros();

        assert!(ts2 > ts1);
    }

    #[test]
    fn test_generate_timestamp_secs() {
        let ts = generate_timestamp_secs();
        // Should be a reasonable timestamp (after year 2020)
        assert!(ts > 1577836800); // Jan 1, 2020
    }
}
