#[cfg(test)]
mod test_mem_graph {
    use std::sync::Arc;

    use crate::{mem_graph::MemGraph, types::VId};

    #[test]
    fn test_new_memgraph() {
        let mem_graph = MemGraph::new(42);

        assert_eq!(mem_graph.virtual_id(), 42);
        assert_eq!(mem_graph.approximate_size(), 0);
    }

    #[test]
    fn test_put_vertex() {
        let mem_graph = MemGraph::new(0);

        // Insert first vertex
        mem_graph.put_vertex(1).unwrap();
        assert!(mem_graph.map.contains_key(&1));
        assert_eq!(mem_graph.map.len(), 1);

        // Size should include VId overhead
        let expected_size = std::mem::size_of::<VId>();
        assert_eq!(mem_graph.approximate_size(), expected_size);

        // Insert same vertex again - should do nothing
        let size_before = mem_graph.approximate_size();
        mem_graph.put_vertex(1).unwrap();
        assert_eq!(mem_graph.approximate_size(), size_before);
        assert_eq!(mem_graph.map.len(), 1);
    }

    #[test]
    fn test_put_edge_new_vertex() {
        let mem_graph = MemGraph::new(0);

        // Add edge from non-existent vertex
        mem_graph.put_edge(1, 2).unwrap();

        assert!(mem_graph.map.contains_key(&1));
        assert_eq!(mem_graph.map.len(), 1);

        // Check the edge list
        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![2]);

        // Size should include: VId (key) + VId (neighbor)
        let expected_size = std::mem::size_of::<VId>() * 2;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_put_edge_existing_vertex() {
        let mem_graph = MemGraph::new(0);

        // Create vertex first
        mem_graph.put_vertex(1).unwrap();
        let size_after_vertex = mem_graph.approximate_size();

        // Add edge to existing vertex
        mem_graph.put_edge(1, 2).unwrap();
        mem_graph.put_edge(1, 3).unwrap();

        // Check neighbors
        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![2, 3]);

        // Size should increase by 2 VIds
        let expected_size = size_after_vertex + std::mem::size_of::<VId>() * 2;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_put_edge_batch_single_source() {
        let mem_graph = MemGraph::new(0);

        // Add multiple edges from same source
        let edges = vec![(1, 2), (1, 3), (1, 4)];
        mem_graph.put_edge_batch(edges).unwrap();

        assert_eq!(mem_graph.map.len(), 1);

        // Check neighbors
        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![2, 3, 4]);

        // Size: 1 VId (key) + 3 VIds (neighbors)
        let expected_size = std::mem::size_of::<VId>() * 4;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_put_edge_batch_multiple_sources() {
        let mem_graph = MemGraph::new(0);

        // Add edges from multiple sources
        let edges = vec![(1, 2), (2, 3), (1, 4), (3, 5), (2, 6)];
        mem_graph.put_edge_batch(edges).unwrap();

        assert_eq!(mem_graph.map.len(), 3);

        // Check vertex 1's neighbors
        let entry1 = mem_graph.map.get(&1).unwrap();
        let neighbors1 = entry1.value().read().unwrap();
        assert_eq!(neighbors1.len(), 2);
        assert!(neighbors1.contains(&2));
        assert!(neighbors1.contains(&4));

        // Check vertex 2's neighbors
        let entry2 = mem_graph.map.get(&2).unwrap();
        let neighbors2 = entry2.value().read().unwrap();
        assert_eq!(neighbors2.len(), 2);
        assert!(neighbors2.contains(&3));
        assert!(neighbors2.contains(&6));

        // Check vertex 3's neighbors
        let entry3 = mem_graph.map.get(&3).unwrap();
        let neighbors3 = entry3.value().read().unwrap();
        assert_eq!(*neighbors3, vec![5]);

        // Size: 3 VIds (keys) + 5 VIds (neighbors)
        let expected_size = std::mem::size_of::<VId>() * 8;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_put_edge_batch_empty() {
        let mem_graph = MemGraph::new(0);

        // Empty edge list should do nothing
        mem_graph.put_edge_batch(vec![]).unwrap();

        assert_eq!(mem_graph.map.len(), 0);
        assert_eq!(mem_graph.approximate_size(), 0);
    }

    #[test]
    fn test_put_edge_batch_with_existing_vertices() {
        let mem_graph = MemGraph::new(0);

        // Create some vertices first
        mem_graph.put_vertex(1).unwrap();
        mem_graph.put_vertex(2).unwrap();
        let size_after_vertices = mem_graph.approximate_size();

        // Add edges to existing vertices
        let edges = vec![(1, 3), (1, 4), (2, 5)];
        mem_graph.put_edge_batch(edges).unwrap();

        // Check vertex 1's neighbors
        let entry1 = mem_graph.map.get(&1).unwrap();
        let neighbors1 = entry1.value().read().unwrap();
        assert_eq!(*neighbors1, vec![3, 4]);

        // Check vertex 2's neighbors
        let entry2 = mem_graph.map.get(&2).unwrap();
        let neighbors2 = entry2.value().read().unwrap();
        assert_eq!(*neighbors2, vec![5]);

        // Size should increase by 3 VIds (the 3 neighbors)
        let expected_size = size_after_vertices + std::mem::size_of::<VId>() * 3;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_mixed_operations() {
        let mem_graph = MemGraph::new(5);

        // Mix of put_vertex, put_edge, and put_edge_batch
        mem_graph.put_vertex(1).unwrap();
        mem_graph.put_edge(1, 2).unwrap();
        mem_graph.put_edge(2, 3).unwrap();
        mem_graph
            .put_edge_batch(vec![(1, 4), (3, 5), (3, 6)])
            .unwrap();

        assert_eq!(mem_graph.map.len(), 3);
        assert_eq!(mem_graph.virtual_id(), 5);

        // Check vertex 1: should have [2, 4]
        let entry1 = mem_graph.map.get(&1).unwrap();
        let neighbors1 = entry1.value().read().unwrap();
        assert_eq!(*neighbors1, vec![2, 4]);

        // Check vertex 2: should have [3]
        let entry2 = mem_graph.map.get(&2).unwrap();
        let neighbors2 = entry2.value().read().unwrap();
        assert_eq!(*neighbors2, vec![3]);

        // Check vertex 3: should have [5, 6]
        let entry3 = mem_graph.map.get(&3).unwrap();
        let neighbors3 = entry3.value().read().unwrap();
        assert_eq!(*neighbors3, vec![5, 6]);
    }

    #[test]
    fn test_duplicate_edges() {
        let mem_graph = MemGraph::new(0);

        // Add same edge multiple times (no deduplication)
        mem_graph.put_edge(1, 2).unwrap();
        mem_graph.put_edge(1, 2).unwrap();
        mem_graph.put_edge(1, 2).unwrap();

        // Should have 3 duplicate edges
        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![2, 2, 2]);

        // Size should reflect all 3 edges
        let expected_size = std::mem::size_of::<VId>() * 4; // 1 key + 3 neighbors
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_large_vertex_ids() {
        let mem_graph = MemGraph::new(0);

        // Test with large vertex IDs (near u32::MAX)
        let large_id = u32::MAX - 10;
        mem_graph.put_vertex(large_id).unwrap();
        mem_graph.put_edge(large_id, u32::MAX).unwrap();

        assert!(mem_graph.map.contains_key(&large_id));
        let entry = mem_graph.map.get(&large_id).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![u32::MAX]);
    }

    #[test]
    fn test_self_loop() {
        let mem_graph = MemGraph::new(0);

        // Add self-loop edge
        mem_graph.put_edge(1, 1).unwrap();

        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(*neighbors, vec![1]);
    }

    #[test]
    fn test_vertex_with_many_neighbors() {
        let mem_graph = MemGraph::new(0);

        // Create vertex with 1000 neighbors
        let edges: Vec<(VId, VId)> = (0..1000).map(|i| (1, i)).collect();
        mem_graph.put_edge_batch(edges).unwrap();

        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(neighbors.len(), 1000);

        // Verify approximate size
        let expected_size = std::mem::size_of::<VId>() * 1001; // 1 key + 1000 neighbors
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }

    #[test]
    fn test_concurrent_reads() {
        use std::thread;

        let mem_graph = Arc::new(MemGraph::new(0));

        // Populate graph
        mem_graph.put_edge(1, 2).unwrap();
        mem_graph.put_edge(1, 3).unwrap();
        mem_graph.put_edge(2, 4).unwrap();

        let mem_graph_clone1 = mem_graph.clone();
        let mem_graph_clone2 = mem_graph.clone();

        // Spawn multiple threads to read concurrently
        let handle1 = thread::spawn(move || {
            for _ in 0..100 {
                if let Some(entry) = mem_graph_clone1.map.get(&1) {
                    let neighbors = entry.value().read().unwrap();
                    assert!(neighbors.contains(&2) && neighbors.contains(&3));
                }
            }
        });

        let handle2 = thread::spawn(move || {
            for _ in 0..100 {
                if let Some(entry) = mem_graph_clone2.map.get(&2) {
                    let neighbors = entry.value().read().unwrap();
                    assert_eq!(*neighbors, vec![4]);
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_concurrent_writes() {
        use std::thread;

        let mem_graph = Arc::new(MemGraph::new(0));
        mem_graph.put_vertex(1).unwrap();

        let mem_graph_clone1 = mem_graph.clone();
        let mem_graph_clone2 = mem_graph.clone();

        // Two threads adding edges to same vertex concurrently
        let handle1 = thread::spawn(move || {
            for i in 0..50 {
                mem_graph_clone1.put_edge(1, i * 2).unwrap();
            }
        });

        let handle2 = thread::spawn(move || {
            for i in 0..50 {
                mem_graph_clone2.put_edge(1, i * 2 + 1).unwrap();
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Check that all 100 edges were added
        let entry = mem_graph.map.get(&1).unwrap();
        let neighbors = entry.value().read().unwrap();
        assert_eq!(neighbors.len(), 100);

        // Verify approximate size (1 key + 100 neighbors)
        let expected_size = std::mem::size_of::<VId>() * 101;
        assert_eq!(mem_graph.approximate_size(), expected_size);
    }
}
