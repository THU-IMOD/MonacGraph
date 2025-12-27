#[cfg(test)]
mod test_bucket_disk_manager {
    use crate::bucket::disk_manager::{BktDiskManager, DiskManagerOptions};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Helper function to create a temporary test directory
    fn setup_test_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp directory")
    }

    /// Helper function to get a test file path
    fn test_file_path(dir: &TempDir, name: &str) -> PathBuf {
        dir.path().join(name)
    }

    #[test]
    fn test_create_and_read_basic() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_basic.db");

        // Create file with initial data
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let manager = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");

        // Verify file size
        assert_eq!(manager.size(), 8);

        // Read back the data
        let read_data = manager.read(0, 8).expect("Failed to read data");
        assert_eq!(data, read_data);
    }

    #[test]
    fn test_read_at_offset() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_offset.db");

        // Create file with pattern data
        let data: Vec<u8> = (0..100).collect();
        let manager = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");

        // Read from middle
        let read_data = manager.read(10, 20).expect("Failed to read at offset");
        let expected: Vec<u8> = (10..30).collect();
        assert_eq!(expected, read_data);

        // Read from start
        let read_start = manager.read(0, 10).expect("Failed to read from start");
        let expected_start: Vec<u8> = (0..10).collect();
        assert_eq!(expected_start, read_start);

        // Read from end
        let read_end = manager.read(90, 10).expect("Failed to read from end");
        let expected_end: Vec<u8> = (90..100).collect();
        assert_eq!(expected_end, read_end);
    }

    #[test]
    fn test_write_and_read() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_write.db");

        // Create file with initial data
        let initial_data = vec![0u8; 100];
        let manager =
            BktDiskManager::create(&path, &initial_data).expect("Failed to create disk manager");

        // Write some data
        let write_data = vec![1, 2, 3, 4, 5];
        manager
            .write(10, &write_data)
            .expect("Failed to write data");

        // Read back and verify
        let read_data = manager.read(10, 5).expect("Failed to read data");
        assert_eq!(write_data, read_data);

        // Verify surrounding data is unchanged
        let before = manager.read(5, 5).expect("Failed to read before");
        assert_eq!(vec![0u8; 5], before);

        let after = manager.read(15, 5).expect("Failed to read after");
        assert_eq!(vec![0u8; 5], after);
    }

    #[test]
    fn test_write_overwrite() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_overwrite.db");

        // Create file
        let data = vec![1u8; 50];
        let manager = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");

        // First write
        let first_write = vec![2, 2, 2, 2, 2];
        manager
            .write(10, &first_write)
            .expect("Failed to first write");

        // Overwrite
        let second_write = vec![3, 3, 3, 3, 3];
        manager
            .write(10, &second_write)
            .expect("Failed to second write");

        // Verify overwrite succeeded
        let read_data = manager.read(10, 5).expect("Failed to read");
        assert_eq!(second_write, read_data);
    }

    #[test]
    fn test_read_batch() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_batch_read.db");

        // Create file with pattern data
        let data: Vec<u8> = (0..100).collect();
        let manager = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");

        // Batch read multiple regions
        let requests = vec![
            (0, 10),  // Read first 10 bytes
            (20, 10), // Read 10 bytes starting at offset 20
            (50, 10), // Read 10 bytes starting at offset 50
            (90, 10), // Read last 10 bytes
        ];

        let results = manager.read_batch(&requests).expect("Failed to batch read");

        // Verify results
        assert_eq!(results.len(), 4);

        let expected_0: Vec<u8> = (0..10).collect();
        assert_eq!(results[0], expected_0);

        let expected_1: Vec<u8> = (20..30).collect();
        assert_eq!(results[1], expected_1);

        let expected_2: Vec<u8> = (50..60).collect();
        assert_eq!(results[2], expected_2);

        let expected_3: Vec<u8> = (90..100).collect();
        assert_eq!(results[3], expected_3);
    }

    #[test]
    fn test_write_batch() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_batch_write.db");

        // Create file with zeros
        let data = vec![0u8; 100];
        let manager = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");

        // Batch write multiple regions
        let write_requests = vec![
            (0, vec![1, 1, 1, 1, 1]),
            (10, vec![2, 2, 2, 2, 2]),
            (20, vec![3, 3, 3, 3, 3]),
            (30, vec![4, 4, 4, 4, 4]),
        ];

        manager
            .write_batch(&write_requests)
            .expect("Failed to batch write");

        // Verify each write
        let read_0 = manager.read(0, 5).expect("Failed to read region 0");
        assert_eq!(read_0, vec![1, 1, 1, 1, 1]);

        let read_1 = manager.read(10, 5).expect("Failed to read region 1");
        assert_eq!(read_1, vec![2, 2, 2, 2, 2]);

        let read_2 = manager.read(20, 5).expect("Failed to read region 2");
        assert_eq!(read_2, vec![3, 3, 3, 3, 3]);

        let read_3 = manager.read(30, 5).expect("Failed to read region 3");
        assert_eq!(read_3, vec![4, 4, 4, 4, 4]);

        // Verify gaps are still zeros
        let gap = manager.read(5, 5).expect("Failed to read gap");
        assert_eq!(gap, vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_open_existing_file() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_existing.db");

        // Create file first
        let data = vec![5u8; 50];
        let manager1 = BktDiskManager::create(&path, &data).expect("Failed to create disk manager");
        drop(manager1);

        // Open existing file
        let manager2 = BktDiskManager::new(&path).expect("Failed to open existing file");

        // Verify size
        assert_eq!(manager2.size(), 50);

        // Verify content
        let read_data = manager2.read(0, 50).expect("Failed to read");
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_with_direct_io_option() {
        let temp_dir = setup_test_dir();
        let path = test_file_path(&temp_dir, "test_direct_io.db");

        // Create with Direct I/O enabled
        let options = DiskManagerOptions {
            use_direct_io: true,
            queue_depth: 256,
        };

        let data = vec![7u8; 4096]; // Use page-aligned size
        let manager = BktDiskManager::create_with_options(&path, &data, options)
            .expect("Failed to create with Direct I/O");

        // Read and verify
        let read_data = manager
            .read(0, 4096)
            .expect("Failed to read with Direct I/O");
        assert_eq!(read_data, data);

        // Write and verify
        let write_data = vec![8u8; 4096];
        manager
            .write(0, &write_data)
            .expect("Failed to write with Direct I/O");

        let verify_data = manager.read(0, 4096).expect("Failed to verify write");
        assert_eq!(verify_data, write_data);
    }
}

#[cfg(test)]
mod test_bucket {
    use crate::bucket::Bucket;
    use crate::bucket::builder::BucketBuilder;
    use crate::bucket::disk_manager::BktDiskManager;
    use crate::graph::CsrGraph;
    use crate::types::VId;
    use anyhow::Result;
    use rand::seq::IndexedRandom;
    use tempfile::TempDir;

    #[test]
    fn test_simple_bucket_build() -> anyhow::Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Create a temporary directory for output
        let temp_dir = TempDir::new()?;
        let bucket_path = temp_dir.path().join("simple_bucket.bkt");

        // Create bucket builder
        let mut builder = BucketBuilder::new(4096);

        // Add all vertices from the graph
        for vertex_id in 0..graph.num_vertices() as VId {
            let neighbors = graph.get_neighbor_iter(vertex_id);
            builder.add(vertex_id, neighbors);
        }

        // Build and save to file
        let bucket = builder.build(0, &bucket_path)?;

        // Verify basic properties
        assert_eq!(bucket.vertex_metas.len(), 13);
        assert!(bucket.edge_bloom.is_some());
        assert!(bucket_path.exists());

        println!(
            " Bucket built successfully with {} vertices and {} edges",
            graph.num_vertices(),
            graph.num_edges()
        );

        Ok(())
    }

    #[test]
    fn test_bucket_build_and_open() -> Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Create a temporary directory for output
        let temp_dir = TempDir::new()?;
        let bucket_path = temp_dir.path().join("test_bucket.bkt");

        // Step 1: Build and save the bucket
        {
            let mut builder = BucketBuilder::new(4096);

            // Add all vertices from the graph
            for vertex_id in 0..graph.num_vertices() as VId {
                let v_start = graph.offsets()[vertex_id as usize];
                let v_end = graph.offsets()[vertex_id as usize + 1];
                let neighbors = graph.neighbors()[v_start..v_end].iter().copied();

                builder.add(vertex_id, neighbors);
            }

            // Build and save
            let bucket = builder.build(0, &bucket_path)?;

            println!(" Bucket built:");
            println!("  - Vertices: {}", bucket.vertex_metas.len());
            println!("  - Block size: {}", bucket.get_block_size());
        }

        // Step 2: Open the bucket from file
        let file = BktDiskManager::new(&bucket_path)?;
        let opened_bucket = Bucket::open(0, file, true)?;

        // Step 3: Verify the opened bucket
        assert_eq!(
            opened_bucket.get_vritual_community_id(),
            0,
            "Virtual comm ID mismatch"
        );
        assert_eq!(opened_bucket.get_block_size(), 4096, "Block size mismatch");
        assert_eq!(
            opened_bucket.vertex_metas.len(),
            13,
            "Vertex count mismatch"
        );
        assert!(opened_bucket.edge_bloom.is_some(), "Bloom filter missing");

        println!("Bucket opened successfully:");
        println!("  - Vertices: {}", opened_bucket.vertex_metas.len());
        println!("  - Block size: {}", opened_bucket.get_block_size());

        // Step 4: Verify vertex metas match
        // Check a few specific vertices
        let vertex_0_meta = opened_bucket
            .vertex_metas
            .iter()
            .find(|m| m.vertex_id == 0)
            .expect("Vertex 0 should exist");
        println!(
            "  - Vertex 0: page_id={}, offset={}",
            vertex_0_meta.page_id, vertex_0_meta.offset_inner
        );

        let vertex_3_meta = opened_bucket
            .vertex_metas
            .iter()
            .find(|m| m.vertex_id == 3)
            .expect("Vertex 3 should exist");
        println!(
            "  - Vertex 3: page_id={}, offset={}",
            vertex_3_meta.page_id, vertex_3_meta.offset_inner
        );

        println!(" All verifications passed!");

        Ok(())
    }

    #[test]
    fn test_bucket_degrees_only() -> anyhow::Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Create a temporary directory for output
        let temp_dir = TempDir::new()?;
        let bucket_path = temp_dir.path().join("test_degrees.bkt");

        // Build bucket
        let mut builder = BucketBuilder::new(4096);
        for vertex_id in 0..graph.num_vertices() as VId {
            let neighbors = graph.get_neighbor_iter(vertex_id);
            builder.add(vertex_id, neighbors);
        }
        let mut bucket = builder.build(0, &bucket_path)?;

        println!("Verifying degrees:");
        let num_vertices = graph.num_vertices() as usize;
        let sample_size = 100.min(num_vertices);
        let mut rng = rand::rng();

        let all_vertices: Vec<VId> = (0..num_vertices as VId).collect();

        let sampled_vertices: Vec<VId> = all_vertices
            .choose_multiple(&mut rng, sample_size)
            .copied()
            .collect();

        for vertex_id in sampled_vertices {
            let v_start = graph.offsets()[vertex_id as usize];
            let v_end = graph.offsets()[vertex_id as usize + 1];
            let expected_degree = v_end - v_start;

            let neighbors = bucket.get_neighbors_for_test(vertex_id)?;
            let actual_degree = neighbors.len();

            assert_eq!(
                actual_degree, expected_degree,
                "Vertex {}: degree mismatch",
                vertex_id
            );

            println!("Vertex {}: degree = {}", vertex_id, actual_degree);
        }

        // Total edge count check
        let total_edges: usize = (0..graph.num_vertices() as VId)
            .map(|v| bucket.get_neighbors_for_test(v).unwrap().len())
            .sum();

        assert_eq!(total_edges, graph.num_edges(), "Total edge count mismatch");

        println!("All {} vertices verified!", graph.num_vertices());
        println!("Total edges: {}", total_edges);

        Ok(())
    }

    #[test]
    fn test_bucket_degrees_only_large_graph() -> Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Create a temporary directory for output
        let temp_dir = TempDir::new()?;
        let bucket_path = temp_dir.path().join("test_degrees_rn.bkt");

        // Build bucket
        let mut builder = BucketBuilder::new(4096 * 4);
        for vertex_id in 0..graph.num_vertices() as VId {
            let neighbors = graph.get_neighbor_iter(vertex_id);
            builder.add(vertex_id, neighbors);
        }
        let mut bucket = builder.build(0, &bucket_path)?;

        println!("Verifying degrees:");

        // Test all vertices
        for vertex_id in 0..graph.num_vertices() as VId {
            // Expected degree from graph
            let v_start = graph.offsets()[vertex_id as usize];
            let v_end = graph.offsets()[vertex_id as usize + 1];
            let expected_degree = v_end - v_start;

            // Actual degree from bucket
            let neighbors = bucket.get_neighbors_for_test(vertex_id)?;
            let actual_degree = neighbors.len();

            assert_eq!(
                actual_degree, expected_degree,
                "Vertex {}: degree mismatch",
                vertex_id
            );

            println!("Vertex {}: degree = {}", vertex_id, actual_degree);
        }

        println!("{} vertices verified!", graph.num_vertices());

        Ok(())
    }
}
