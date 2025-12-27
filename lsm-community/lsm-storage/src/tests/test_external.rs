#[cfg(test)]
mod tests {
    use crate::{
        config::LsmCommunityStorageOptions,
        delta::{DeltaOpType, DeltaOperation},
        external::ExternalStorage,
        types::VId,
    };
    use rand::RngCore;

    #[test]
    fn test_giant_vertices_write_and_read() {
        let graph_name = "test_giant_combined";

        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        options.work_space_dir = "./workspace".to_owned();

        // Phase 1: Write
        {
            let storage =
                ExternalStorage::new(options.clone()).expect("Failed to create external storage");

            let mut rng = rand::rng();

            for vertex_id in 0..100 {
                let neighbors: Vec<VId> = (0..10_000).map(|_| rng.next_u32()).collect();

                storage
                    .put_giant_vertex(vertex_id, neighbors.into_iter())
                    .expect(&format!("Failed to put giant vertex {}", vertex_id));
            }

            println!("Write phase completed");
        }

        // Phase 2: Read
        {
            let storage = ExternalStorage::new(options).expect("Failed to open external storage");

            for vertex_id in 0..100 {
                let neighbors = storage
                    .get_giant_vertex(vertex_id)
                    .expect(&format!("Failed to get giant vertex {}", vertex_id));
                assert_eq!(neighbors.len(), 10_000);
            }

            println!("Read phase completed");
        }
    }

    /// Bonus test: Verify compression effectiveness
    #[test]
    fn test_compression_stats() {
        let graph_name = "test_compression";

        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();

        let storage = ExternalStorage::new(options).expect("Failed to create external storage");

        // Create a vertex with sorted neighbors (highly compressible)
        let sorted_neighbors: Vec<VId> = (0..10_000).collect();
        let serialized_size = bincode::serialize(&sorted_neighbors).unwrap().len();

        storage
            .put_giant_vertex(1000, sorted_neighbors.into_iter())
            .expect("Failed to write sorted vertex");

        // Create a vertex with random neighbors (less compressible)
        let mut rng = rand::rng();
        let random_neighbors: Vec<VId> = (0..10_000).map(|_| rng.next_u32()).collect();

        storage
            .put_giant_vertex(1001, random_neighbors.into_iter())
            .expect("Failed to write random vertex");

        println!("Original serialized size: {} bytes", serialized_size);
        println!(
            "Approximately {} KB per 100K neighbors",
            serialized_size / 1024
        );
        println!("Compression test completed - check RocksDB stats for actual compressed sizes");

        // Verify both can be read back
        let sorted = storage.get_giant_vertex(1000).unwrap();
        let random = storage.get_giant_vertex(1001).unwrap();

        assert_eq!(sorted.len(), 10_000);
        assert_eq!(random.len(), 10_000);

        // Verify sorted property
        for i in 0..10_000 {
            assert_eq!(sorted[i], i as VId, "Sorted neighbors should be sequential");
        }
    }

    use tempfile::TempDir;

    #[test]
    fn test_delta_merge_without_base() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;

        let mut options = LsmCommunityStorageOptions::default();
        options.work_space_dir = temp_dir.path().to_str().unwrap().to_string();
        options.graph_name = "hello".to_string();
        options.giant_cache_capacity = 100;

        let storage = ExternalStorage::new(options)?;

        // Append multiple deltas without any base value
        storage.append_delta(1, DeltaOperation::new(100, DeltaOpType::AddNeighbor, 10))?;
        storage.append_delta(1, DeltaOperation::new(200, DeltaOpType::AddNeighbor, 20))?;
        storage.append_delta(1, DeltaOperation::new(150, DeltaOpType::RemoveNeighbor, 10))?;

        // Read back - should trigger full merge with None as base
        let log = storage.read_delta_log(1)?.expect("Should have delta log");

        // Verify: neighbor 10 should have ts=150, neighbor 20 should have ts=200
        assert_eq!(log.len(), 2);

        let op10 = log.ops().iter().find(|op| op.neighbor == 10).unwrap();
        assert_eq!(op10.timestamp, 150);
        assert_eq!(op10.get_op_type(), Some(DeltaOpType::RemoveNeighbor));

        let op20 = log.ops().iter().find(|op| op.neighbor == 20).unwrap();
        assert_eq!(op20.timestamp, 200);

        Ok(())
    }

    fn create_test_storage() -> anyhow::Result<(ExternalStorage, TempDir)> {
        let temp_dir = TempDir::new()?;
        let mut options = LsmCommunityStorageOptions::default();
        options.work_space_dir = temp_dir.path().to_str().unwrap().to_string();
        options.graph_name = "hello".to_string();
        options.giant_cache_capacity = 100;
        let storage = ExternalStorage::new(options)?;
        Ok((storage, temp_dir))
    }

    #[test]
    fn test_vertex_property_put_get() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let vertex_id = 123;
        let property_name = "age".to_string();
        let value = b"30";

        // Put property
        storage.put_vertex_property(vertex_id, property_name.clone(), value)?;

        // Get property
        let retrieved = storage.get_vertex_property(vertex_id, property_name)?;
        assert_eq!(retrieved, Some(value.to_vec()));

        Ok(())
    }

    #[test]
    fn test_vertex_property_remove() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let vertex_id = 456;
        let property_name = "name".to_string();
        let value = b"Alice";

        // Put and verify
        storage.put_vertex_property(vertex_id, property_name.clone(), value)?;
        assert!(
            storage
                .get_vertex_property(vertex_id, property_name.clone())?
                .is_some()
        );

        // Remove and verify
        storage.remove_vertex_property(vertex_id, property_name.clone())?;
        assert!(
            storage
                .get_vertex_property(vertex_id, property_name)?
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn test_get_all_vertex_properties() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let vertex_id = 789;

        // Put multiple properties
        storage.put_vertex_property(vertex_id, "age".to_string(), b"25")?;
        storage.put_vertex_property(vertex_id, "name".to_string(), b"Bob")?;
        storage.put_vertex_property(vertex_id, "city".to_string(), b"NYC")?;

        // Get all properties
        let properties = storage.get_all_vertex_properties(vertex_id)?;
        assert_eq!(properties.len(), 3);

        // Verify properties exist (order may vary)
        let prop_map: std::collections::HashMap<String, Vec<u8>> = properties.into_iter().collect();

        assert_eq!(prop_map.get("age"), Some(&b"25".to_vec()));
        assert_eq!(prop_map.get("name"), Some(&b"Bob".to_vec()));
        assert_eq!(prop_map.get("city"), Some(&b"NYC".to_vec()));

        Ok(())
    }

    #[test]
    fn test_edge_property_put_get() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let src = 100;
        let dst = 200;
        let property_name = "weight".to_string();
        let value = b"0.85";

        // Put property
        storage.put_edge_property(src, dst, property_name.clone(), value)?;

        // Get property
        let retrieved = storage.get_edge_property(src, dst, property_name)?;
        assert_eq!(retrieved, Some(value.to_vec()));

        Ok(())
    }

    #[test]
    fn test_edge_property_remove() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let src = 300;
        let dst = 400;
        let property_name = "label".to_string();
        let value = b"friend";

        // Put and verify
        storage.put_edge_property(src, dst, property_name.clone(), value)?;
        assert!(
            storage
                .get_edge_property(src, dst, property_name.clone())?
                .is_some()
        );

        // Remove and verify
        storage.remove_edge_property(src, dst, property_name.clone())?;
        assert!(
            storage
                .get_edge_property(src, dst, property_name)?
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn test_get_all_edge_properties() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        let src = 500;
        let dst = 600;

        // Put multiple properties
        storage.put_edge_property(src, dst, "weight".to_string(), b"0.5")?;
        storage.put_edge_property(src, dst, "label".to_string(), b"knows")?;
        storage.put_edge_property(src, dst, "since".to_string(), b"2020")?;

        // Get all properties
        let properties = storage.get_all_edge_properties(src, dst)?;
        assert_eq!(properties.len(), 3);

        // Verify properties exist (order may vary)
        let prop_map: std::collections::HashMap<String, Vec<u8>> = properties.into_iter().collect();

        assert_eq!(prop_map.get("weight"), Some(&b"0.5".to_vec()));
        assert_eq!(prop_map.get("label"), Some(&b"knows".to_vec()));
        assert_eq!(prop_map.get("since"), Some(&b"2020".to_vec()));

        Ok(())
    }

    #[test]
    fn test_edge_properties_different_edges() -> anyhow::Result<()> {
        let (storage, _temp_dir) = create_test_storage()?;

        // Put properties for different edges
        storage.put_edge_property(1, 2, "weight".to_string(), b"0.1")?;
        storage.put_edge_property(1, 3, "weight".to_string(), b"0.2")?;
        storage.put_edge_property(2, 3, "weight".to_string(), b"0.3")?;

        // Verify each edge has its own property
        assert_eq!(
            storage.get_edge_property(1, 2, "weight".to_string())?,
            Some(b"0.1".to_vec())
        );
        assert_eq!(
            storage.get_edge_property(1, 3, "weight".to_string())?,
            Some(b"0.2".to_vec())
        );
        assert_eq!(
            storage.get_edge_property(2, 3, "weight".to_string())?,
            Some(b"0.3".to_vec())
        );

        Ok(())
    }
}
