#[cfg(test)]
mod test_mapper {
    use crate::mapper::{EdgeIdMapper, VertexIdMapper};

    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_vertex_id_mapper_basic_operations() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");

        // Create a new mapper
        let mapper = VertexIdMapper::new(&log_path)?;

        // Test insert
        mapper.insert(b"user:123", 0)?;
        mapper.insert(b"user:456", 1)?;
        mapper.insert(b"user:789", 2)?;

        // Test get
        assert_eq!(mapper.get_inner_id(b"user:123"), Some(0));
        assert_eq!(mapper.get_inner_id(b"user:456"), Some(1));
        assert_eq!(mapper.get_inner_id(b"user:789"), Some(2));
        assert_eq!(mapper.get_inner_id(b"user:999"), None);

        // Test remove
        let removed = mapper.remove(b"user:456")?;
        assert_eq!(removed, Some(1));
        assert_eq!(mapper.get_inner_id(b"user:456"), None);

        // Verify other mappings still exist
        assert_eq!(mapper.get_inner_id(b"user:123"), Some(0));
        assert_eq!(mapper.get_inner_id(b"user:789"), Some(2));

        Ok(())
    }

    #[test]
    fn test_vertex_id_mapper_persistence() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");

        // Create mapper and insert data
        {
            let mapper = VertexIdMapper::new(&log_path)?;
            mapper.insert(b"user:111", 0)?;
            mapper.insert(b"user:222", 1)?;
            mapper.insert(b"user:333", 2)?;
            mapper.remove(b"user:222")?;
        }

        // Reopen mapper and verify data persisted
        {
            let mapper = VertexIdMapper::new(&log_path)?;
            assert_eq!(mapper.get_inner_id(b"user:111"), Some(0));
            assert_eq!(mapper.get_inner_id(b"user:222"), None); // Was removed
            assert_eq!(mapper.get_inner_id(b"user:333"), Some(2));
        }

        Ok(())
    }

    #[test]
    fn test_edge_id_mapper_basic_operations() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("edge_mapper.log");

        // Create a new mapper
        let mapper = EdgeIdMapper::new(&log_path)?;

        // Create edge handles
        let handle1 = EdgeIdMapper::pack_edge_handle(0, 1); // 0 -> 1
        let handle2 = EdgeIdMapper::pack_edge_handle(1, 2); // 1 -> 2
        let handle3 = EdgeIdMapper::pack_edge_handle(2, 0); // 2 -> 0

        // Test insert
        mapper.insert(b"edge:a", handle1)?;
        mapper.insert(b"edge:b", handle2)?;
        mapper.insert(b"edge:c", handle3)?;

        // Test get
        assert_eq!(mapper.get_inner_id(b"edge:a"), Some(handle1));
        assert_eq!(mapper.get_inner_id(b"edge:b"), Some(handle2));
        assert_eq!(mapper.get_inner_id(b"edge:c"), Some(handle3));
        assert_eq!(mapper.get_inner_id(b"edge:d"), None);

        // Test unpack
        let (src, dst) = EdgeIdMapper::unpack_edge_handle(handle1);
        assert_eq!(src, 0);
        assert_eq!(dst, 1);

        // Test remove
        let removed = mapper.remove(b"edge:b")?;
        assert_eq!(removed, Some(handle2));
        assert_eq!(mapper.get_inner_id(b"edge:b"), None);

        // Verify other mappings still exist
        assert_eq!(mapper.get_inner_id(b"edge:a"), Some(handle1));
        assert_eq!(mapper.get_inner_id(b"edge:c"), Some(handle3));

        Ok(())
    }

    #[test]
    fn test_edge_id_mapper_persistence() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("edge_mapper.log");

        let handle1 = EdgeIdMapper::pack_edge_handle(0, 1);
        let handle2 = EdgeIdMapper::pack_edge_handle(1, 2);
        let handle3 = EdgeIdMapper::pack_edge_handle(2, 3);

        // Create mapper and insert data
        {
            let mapper = EdgeIdMapper::new(&log_path)?;
            mapper.insert(b"edge:x", handle1)?;
            mapper.insert(b"edge:y", handle2)?;
            mapper.insert(b"edge:z", handle3)?;
            mapper.remove(b"edge:y")?;
        }

        // Reopen mapper and verify data persisted
        {
            let mapper = EdgeIdMapper::new(&log_path)?;
            assert_eq!(mapper.get_inner_id(b"edge:x"), Some(handle1));
            assert_eq!(mapper.get_inner_id(b"edge:y"), None); // Was removed
            assert_eq!(mapper.get_inner_id(b"edge:z"), Some(handle3));
        }

        Ok(())
    }

    #[test]
    fn test_edge_handle_packing_unpacking() {
        // Test various vertex ID pairs
        let test_cases = vec![
            (0, 0),
            (0, 1),
            (1, 0),
            (u32::MAX, u32::MAX),
            (12345, 67890),
            (0xFFFF, 0xAAAA),
        ];

        for (src, dst) in test_cases {
            let handle = EdgeIdMapper::pack_edge_handle(src, dst);
            let (unpacked_src, unpacked_dst) = EdgeIdMapper::unpack_edge_handle(handle);

            assert_eq!(src, unpacked_src, "Source mismatch for ({}, {})", src, dst);
            assert_eq!(
                dst, unpacked_dst,
                "Destination mismatch for ({}, {})",
                src, dst
            );
        }
    }

    #[test]
    fn test_vertex_id_mapper_concurrent_operations() -> anyhow::Result<()> {
        use std::thread;

        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");
        let mapper = Arc::new(VertexIdMapper::new(&log_path)?);

        // Spawn multiple threads to insert concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let mapper_clone = Arc::clone(&mapper);
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    let key = format!("user:{}:{}", i, j);
                    let inner_id = (i * 10 + j) as u32;
                    mapper_clone.insert(key.as_bytes(), inner_id).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all insertions
        for i in 0..10 {
            for j in 0..10 {
                let key = format!("user:{}:{}", i, j);
                let expected_id = (i * 10 + j) as u32;
                assert_eq!(mapper.get_inner_id(key.as_bytes()), Some(expected_id));
            }
        }

        Ok(())
    }

    #[test]
    fn test_vertex_id_mapper_remove_nonexistent() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");
        let mapper = VertexIdMapper::new(&log_path)?;

        // Try to remove a key that doesn't exist
        let removed = mapper.remove(b"nonexistent")?;
        assert_eq!(removed, None);

        Ok(())
    }

    #[test]
    fn test_vertex_id_mapper_overwrite() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");
        let mapper = VertexIdMapper::new(&log_path)?;

        // Insert initial mapping
        mapper.insert(b"user:123", 0)?;
        assert_eq!(mapper.get_inner_id(b"user:123"), Some(0));

        // Overwrite with new value
        mapper.insert(b"user:123", 99)?;
        assert_eq!(mapper.get_inner_id(b"user:123"), Some(99));

        Ok(())
    }

    #[test]
    fn test_empty_mapper() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");
        let mapper = VertexIdMapper::new(&log_path)?;

        // Query on empty mapper
        assert_eq!(mapper.get_inner_id(b"anything"), None);

        // Remove on empty mapper
        let removed = mapper.remove(b"anything")?;
        assert_eq!(removed, None);

        Ok(())
    }

    #[test]
    fn test_large_keys_and_values() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let log_path = temp_dir.path().join("vertex_mapper.log");
        let mapper = VertexIdMapper::new(&log_path)?;

        // Test with large keys
        let large_key = vec![b'x'; 1000];
        mapper.insert(&large_key, 42)?;
        assert_eq!(mapper.get_inner_id(&large_key), Some(42));
        println!("Large key 1 test passed");

        Ok(())
    }
}
