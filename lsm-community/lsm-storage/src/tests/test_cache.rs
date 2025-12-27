#[cfg(test)]
mod test_cache {
    use crate::cache::CacheKey;

    #[test]
    fn test_cache_key_roundtrip() {
        let virtual_comm_id = 12345u16;
        let page_id = 987654321u32;

        let key = CacheKey::new(virtual_comm_id, page_id);

        assert_eq!(key.virtual_comm_id(), virtual_comm_id);
        assert_eq!(key.page_id(), page_id);
    }

    #[test]
    fn test_cache_key_edge_cases() {
        // Test maximum values
        let key_max = CacheKey::new(u16::MAX, u32::MAX);
        assert_eq!(key_max.virtual_comm_id(), u16::MAX);
        assert_eq!(key_max.page_id(), u32::MAX);

        // Test minimum values
        let key_min = CacheKey::new(0, 0);
        assert_eq!(key_min.virtual_comm_id(), 0);
        assert_eq!(key_min.page_id(), 0);
    }

    #[test]
    fn test_cache_key_uniqueness() {
        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 100);
        let key3 = CacheKey::new(1, 101);
        let key4 = CacheKey::new(2, 100);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
        assert_ne!(key2, key3);
    }
}
