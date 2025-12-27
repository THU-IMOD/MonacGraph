#[cfg(test)]
mod test_lsm_comm_state {
    use anyhow::Ok;
    use serial_test::serial;

    use crate::{
        comm_io::LsmCommunityStorageState, config::LsmCommunityStorageOptions, graph::CsrGraph,
    };

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_lsm_state_create() -> anyhow::Result<()> {
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        // Load the graph
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        // Create storage state
        let (state, _, vertex_index) =
            LsmCommunityStorageState::create_with_graph_file(graph, options);
        let vc_list = vertex_index.get_virtual_community_list_for_test();
        println!("Virtual Community Count in vertex index: {}", vc_list.len());
        println!("Bucket Count in state: {}", state.buckets.len());
        Ok(())
    }
}

#[cfg(test)]
mod test_lsm_community_inner {
    use serial_test::serial;

    use crate::{
        comm_io::LsmCommunityStorageInner, config::LsmCommunityStorageOptions, types::VId,
    };

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_lsm_inner_create() -> anyhow::Result<()> {
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let (inner, _, vertex_index) =
            LsmCommunityStorageInner::open(false, None, None, options.clone())?;
        let neighbor_iter = inner.get_neighbor_iter(0, &vertex_index);
        let neighbors: Vec<VId> = neighbor_iter.collect();
        println!("Neighbors: {:?}", neighbors);
        assert_eq!(inner.options.block_size, options.block_size);
        // Create storage state
        Ok(())
    }
}

#[cfg(test)]
mod test_lsm_comm {
    use crate::{
        comm_io::LsmCommunity, config::LsmCommunityStorageOptions, delta::DeltaOpType,
        graph::CsrGraph, types::VId,
    };
    use rand::{Rng, seq::IndexedRandom};
    use serial_test::serial;

    #[test]
    #[serial(lsm_community_example)]
    fn test_lsm_comm_insert_and_remove_edges_with_delta() -> anyhow::Result<()> {
        let graph_name = "dyn";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();

        let db_path = format!("./{}/{}", options.work_space_dir, options.graph_name);
        let _ = std::fs::remove_dir_all(&db_path);

        let lsm_community = LsmCommunity::open(options)?;

        println!("Testing edge insertion and removal with delta operations:");

        // Test Case 1: Add new edges
        println!("\n=== Test Case 1: Adding new edges ===");

        // Add edge 0 -> 1 (creates a new connection in community 0)
        lsm_community.insert_edge(0, 1)?;
        let neighbors_0 = lsm_community.read_out_neighbor_clone(0)?;
        assert!(
            neighbors_0.contains(&1),
            "Vertex 0 should have neighbor 1 after insertion"
        );
        println!("Added edge 0 -> 1, vertex 0 neighbors: {:?}", neighbors_0);

        // Add edge 2 -> 1 (connects existing vertices)
        lsm_community.insert_edge(2, 1)?;
        let neighbors_2 = lsm_community.read_out_neighbor_clone(2)?;
        assert!(
            neighbors_2.contains(&1),
            "Vertex 2 should have neighbor 1 after insertion"
        );
        println!("Added edge 2 -> 1, vertex 2 neighbors: {:?}", neighbors_2);

        // Add edge 5 -> 6 (creates a cycle in community 1: 4->6->5->4)
        lsm_community.insert_edge(5, 6)?;
        let neighbors_5 = lsm_community.read_out_neighbor_clone(5)?;
        assert!(
            neighbors_5.contains(&6),
            "Vertex 5 should have neighbor 6 after insertion"
        );
        println!("Added edge 5 -> 6, vertex 5 neighbors: {:?}", neighbors_5);

        // Add edge 8 -> 7 (creates bidirectional connection in community 2)
        lsm_community.insert_edge(8, 7)?;
        let neighbors_8 = lsm_community.read_out_neighbor_clone(8)?;
        assert!(
            neighbors_8.contains(&7),
            "Vertex 8 should have neighbor 7 after insertion"
        );
        println!("Added edge 8 -> 7, vertex 8 neighbors: {:?}", neighbors_8);

        // Test Case 2: Remove existing edges
        println!("\n=== Test Case 2: Removing existing edges ===");

        // Remove edge 1 -> 0 (original edge from graph)
        lsm_community.remove_edge(1, 0)?;
        let neighbors_1_after_remove = lsm_community.read_out_neighbor_clone(1)?;
        assert!(
            !neighbors_1_after_remove.contains(&0),
            "Vertex 1 should not have neighbor 0 after removal"
        );
        println!(
            "Removed edge 1 -> 0, vertex 1 neighbors: {:?}",
            neighbors_1_after_remove
        );

        // Remove edge 7 -> 8 (original edge from graph)
        lsm_community.remove_edge(7, 8)?;
        let neighbors_7_after_remove = lsm_community.read_out_neighbor_clone(7)?;
        assert!(
            !neighbors_7_after_remove.contains(&8),
            "Vertex 7 should not have neighbor 8 after removal"
        );
        println!(
            "Removed edge 7 -> 8, vertex 7 neighbors: {:?}",
            neighbors_7_after_remove
        );

        // Test Case 3: Add and then remove the same edge
        println!("\n=== Test Case 3: Add and remove same edge ===");

        // Add edge 11 -> 3 (connects two communities)
        lsm_community.insert_edge(11, 3)?;
        let neighbors_11_added = lsm_community.read_out_neighbor_clone(11)?;
        assert!(
            neighbors_11_added.contains(&3),
            "Vertex 11 should have neighbor 3 after insertion"
        );
        println!(
            "Added edge 11 -> 3, vertex 11 neighbors: {:?}",
            neighbors_11_added
        );

        // Remove the edge we just added
        lsm_community.remove_edge(11, 3)?;
        let neighbors_11_removed = lsm_community.read_out_neighbor_clone(11)?;
        assert!(
            !neighbors_11_removed.contains(&3),
            "Vertex 11 should not have neighbor 3 after removal"
        );
        println!(
            "Removed edge 11 -> 3, vertex 11 neighbors: {:?}",
            neighbors_11_removed
        );

        // Test Case 4: Multiple operations on same vertex
        println!("\n=== Test Case 4: Multiple operations on same vertex ===");

        let vertex_3_original = graph.get_neighbor_iter(3).collect::<Vec<_>>();
        println!("Vertex 3 original neighbors: {:?}", vertex_3_original);

        // Add multiple edges to vertex 3
        lsm_community.insert_edge(3, 5)?; // Connect to community 1
        lsm_community.insert_edge(3, 8)?; // Connect to community 2
        lsm_community.insert_edge(3, 12)?; // Connect to community 3

        let neighbors_3_after_adds = lsm_community.read_out_neighbor_clone(3)?;
        assert!(neighbors_3_after_adds.contains(&5), "Should contain 5");
        assert!(neighbors_3_after_adds.contains(&8), "Should contain 8");
        assert!(neighbors_3_after_adds.contains(&12), "Should contain 12");
        println!(
            "Added edges 3->{{5,8,12}}, vertex 3 neighbors: {:?}",
            neighbors_3_after_adds
        );

        // Remove some original and newly added edges
        lsm_community.remove_edge(3, 0)?; // Remove original
        lsm_community.remove_edge(3, 5)?; // Remove newly added

        let neighbors_3_final = lsm_community.read_out_neighbor_clone(3)?;
        assert!(!neighbors_3_final.contains(&0), "Should not contain 0");
        assert!(!neighbors_3_final.contains(&5), "Should not contain 5");
        assert!(neighbors_3_final.contains(&8), "Should still contain 8");
        assert!(neighbors_3_final.contains(&12), "Should still contain 12");
        println!(
            "Removed edges 3->{{0,5}}, vertex 3 neighbors: {:?}",
            neighbors_3_final
        );

        // Verify degree changes
        let original_degree = vertex_3_original.len();
        let final_degree = neighbors_3_final.len();
        println!("Vertex 3 degree: {} -> {}", original_degree, final_degree);

        println!("\nAll edge insertion and removal tests passed!");
        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_lsm_comm_read_all_edges() -> anyhow::Result<()> {
        let graph_name = "example";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;

        println!("Testing read_all_edges:");

        // Step 1: Get all edges from LSM Community
        let mut actual_edges = lsm_community.read_all_edges()?;
        actual_edges.sort_unstable();

        // Step 2: Get all edges from original graph
        let mut expected_edges = Vec::<(VId, VId)>::new();
        let num_vertices = graph.num_vertices() as usize;
        for vertex_id in 0..num_vertices as VId {
            let neighbors = graph.get_neighbor_iter(vertex_id);
            for neighbor in neighbors {
                expected_edges.push((vertex_id, neighbor));
            }
        }
        expected_edges.sort_unstable();

        // Step 3: Verify edge count
        println!(
            "Expected {} edges, got {} edges",
            expected_edges.len(),
            actual_edges.len()
        );
        assert_eq!(
            actual_edges.len(),
            expected_edges.len(),
            "Edge count mismatch"
        );

        // Step 4: Verify each edge matches
        for (i, (expected, actual)) in expected_edges.iter().zip(actual_edges.iter()).enumerate() {
            assert_eq!(
                expected, actual,
                "Edge mismatch at index {}: expected {:?}, got {:?}",
                i, expected, actual
            );
        }

        println!("All {} edges verified successfully!", actual_edges.len());

        // Step 5: Print some sample edges for visual verification
        println!("\nSample edges (first 10):");
        for (i, edge) in actual_edges.iter().take(10).enumerate() {
            println!("  Edge {}: {} -> {}", i, edge.0, edge.1);
        }

        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_lsm_comm_read_in_neighbor_clone() -> anyhow::Result<()> {
        let graph_name = "example";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;

        println!("Testing read_in_neighbor_clone:");

        // Build expected in-neighbors from original graph
        let num_vertices = graph.num_vertices() as usize;
        let mut expected_in_neighbors: Vec<Vec<VId>> = vec![Vec::new(); num_vertices];

        for vertex_id in 0..num_vertices as VId {
            let neighbors = graph.get_neighbor_iter(vertex_id);
            for neighbor in neighbors {
                expected_in_neighbors[neighbor as usize].push(vertex_id);
            }
        }

        // Sort for comparison
        for in_neighbors in &mut expected_in_neighbors {
            in_neighbors.sort_unstable();
        }

        // Test a sample of vertices
        let sample_size = num_vertices.min(50);
        let mut rng = rand::rng();
        let all_vertices: Vec<VId> = (0..num_vertices as VId).collect();
        let sampled_vertices: Vec<VId> = all_vertices
            .choose_multiple(&mut rng, sample_size)
            .copied()
            .collect();

        println!("\nTesting {} sampled vertices:", sample_size);

        for vertex_id in sampled_vertices {
            // Get in-neighbors using read_in_neighbor_clone
            let mut actual_in_neighbors = lsm_community.read_in_neighbor_clone(vertex_id)?;
            actual_in_neighbors.sort_unstable();

            let expected = &expected_in_neighbors[vertex_id as usize];

            // Verify in-degree matches
            assert_eq!(
                actual_in_neighbors.len(),
                expected.len(),
                "Vertex {}: in-degree mismatch (expected {}, got {})",
                vertex_id,
                expected.len(),
                actual_in_neighbors.len()
            );

            // Verify all in-neighbors match
            assert_eq!(
                actual_in_neighbors, *expected,
                "Vertex {}: in-neighbor list mismatch",
                vertex_id
            );

            println!(
                "Vertex {}: in-degree = {}, in-neighbors = {:?}",
                vertex_id,
                actual_in_neighbors.len(),
                actual_in_neighbors
            );
        }

        println!("\nAll sampled vertices passed in-neighbor verification!");

        // Test specific vertices with known in-neighbors from the graph
        println!("\nTesting specific vertices with known structure:");

        // Vertex 0: should have in-neighbors [1, 3]
        let in_neighbors_0 = lsm_community.read_in_neighbor_clone(0)?;
        println!(
            "Vertex 0 in-neighbors: {:?} (expected: vertices that point to 0)",
            in_neighbors_0
        );

        // Vertex 3: should have in-neighbors [1, 2, 7]
        let in_neighbors_3 = lsm_community.read_in_neighbor_clone(3)?;
        println!(
            "Vertex 3 in-neighbors: {:?} (expected: vertices that point to 3)",
            in_neighbors_3
        );

        // Vertex 9: should have in-neighbors [7, 8, 10]
        let in_neighbors_9 = lsm_community.read_in_neighbor_clone(9)?;
        println!(
            "Vertex 9 in-neighbors: {:?} (expected: vertices that point to 9)",
            in_neighbors_9
        );

        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_lsm_comm_delta_applying() -> anyhow::Result<()> {
        // Replace when the large graph is ready
        let graph_name = "example";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;
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
            let expected_degree = graph.get_neighbor_iter(vertex_id).collect::<Vec<_>>().len();
            let res = lsm_community.read_neighbor(vertex_id, true).unwrap();
            let inner_neighbor = res.0.unwrap();
            let actual_degree = inner_neighbor.collect::<Vec<_>>().len();

            assert_eq!(
                actual_degree, expected_degree,
                "Vertex {}: degree mismatch",
                vertex_id
            );

            println!("Vertex {}: degree = {}", vertex_id, actual_degree);
        }

        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_edge_insertion_with_delta() -> anyhow::Result<()> {
        // Setup
        let graph_name = "dyn";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();

        // Remove database file;
        let db_path = format!("./{}/{}", options.work_space_dir, options.graph_name);
        let _ = std::fs::remove_dir_all(&db_path);

        let lsm_community = LsmCommunity::open(options)?;

        let num_vertices = graph.num_vertices() as VId;
        let mut rng = rand::rng();

        // Generate 10 random edges
        let mut test_edges = Vec::new();
        for _ in 0..10 {
            let src = rng.random_range(0..num_vertices);
            let dst = rng.random_range(0..num_vertices);
            test_edges.push((src, dst));
        }

        println!("Testing edge insertions:");
        println!("========================");

        // Insert edges and verify
        for (src, dst) in test_edges {
            println!("\nInserting edge: {} -> {}", src, dst);

            // Insert the edge
            lsm_community.insert_edge(src, dst)?;

            // Read neighbor with delta=true to get delta operations
            let res = lsm_community.read_neighbor(src, true)?;

            // Check if we got delta operations
            if let Some(deltas) = res.1 {
                println!("Delta operations found:");
                let mut found_matching_delta = false;

                for delta in deltas.ops() {
                    println!(
                        "  Delta: op_type={:?}, target={}",
                        delta.op_type, delta.neighbor
                    );

                    // Check if this delta matches our insertion
                    if matches!(
                        DeltaOpType::from_u32(delta.op_type),
                        Some(DeltaOpType::AddNeighbor)
                    ) && delta.neighbor == dst
                    {
                        found_matching_delta = true;
                        println!("Found matching delta for {} -> {}", src, dst);
                    }
                }

                assert!(
                    found_matching_delta,
                    "Did not find matching delta operation for edge {} -> {}",
                    src, dst
                );
            } else {
                panic!("Expected delta operations for vertex {} but got None", src);
            }
        }

        println!("\nAll edge insertions verified successfully!");
        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_edge_removal_with_delta() -> anyhow::Result<()> {
        // Setup
        let graph_name = "dyn";
        let graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;

        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();

        let db_path = format!("./{}/{}", options.work_space_dir, options.graph_name);
        let _ = std::fs::remove_dir_all(&db_path);

        let lsm_community = LsmCommunity::open(options)?;

        let num_vertices = graph.num_vertices() as VId;
        let mut rng = rand::rng();

        // Generate 10 random edges to remove
        let mut test_edges = Vec::new();
        for _ in 0..10 {
            let src = rng.random_range(0..num_vertices);
            let dst = rng.random_range(0..num_vertices);
            test_edges.push((src, dst));
        }

        println!("Testing edge removals:");
        println!("======================");

        // Remove edges and verify
        for (src, dst) in test_edges {
            println!("\nRemoving edge: {} -> {}", src, dst);

            // Remove the edge
            lsm_community.remove_edge(src, dst)?;

            // Read neighbor with delta=true to get delta operations
            let res = lsm_community.read_neighbor(src, true)?;

            // Check if we got delta operations
            if let Some(deltas) = res.1 {
                println!("Delta operations found:");
                let mut found_matching_delta = false;

                for delta in deltas.ops() {
                    println!(
                        "  Delta: op_type={:?}, target={}",
                        delta.op_type, delta.neighbor
                    );

                    // Check if this delta matches our removal
                    if matches!(
                        DeltaOpType::from_u32(delta.op_type),
                        Some(DeltaOpType::RemoveNeighbor)
                    ) && delta.neighbor == dst
                    {
                        found_matching_delta = true;
                        println!("Found matching delta for {} -> {}", src, dst);
                    }
                }

                assert!(
                    found_matching_delta,
                    "Did not find matching delta operation for edge removal {} -> {}",
                    src, dst
                );
            } else {
                panic!("Expected delta operations for vertex {} but got None", src);
            }
        }

        println!("\nAll edge removals verified successfully!");
        Ok(())
    }
}

#[cfg(test)]
mod test_apply_delta {
    use crate::{
        LsmCommunity,
        delta::{DeltaLog, DeltaOperation},
    };

    #[test]
    fn test_apply_delta_add_operations() {
        let mut neighbors = vec![1, 2, 3];
        let delta = DeltaLog {
            ops: vec![
                DeltaOperation {
                    timestamp: 1,
                    neighbor: 4,
                    op_type: 0,
                },
                DeltaOperation {
                    timestamp: 2,
                    neighbor: 5,
                    op_type: 0,
                },
            ],
        };
        LsmCommunity::apply_delta_to_neighbors(&mut neighbors, &delta);
        neighbors.sort_unstable();

        assert_eq!(neighbors, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_apply_delta_remove_operations() {
        let mut neighbors = vec![1, 2, 3, 4, 5];
        let delta = DeltaLog {
            ops: vec![
                DeltaOperation {
                    timestamp: 1,
                    neighbor: 2,
                    op_type: 1,
                },
                DeltaOperation {
                    timestamp: 2,
                    neighbor: 4,
                    op_type: 1,
                },
            ],
        };

        LsmCommunity::apply_delta_to_neighbors(&mut neighbors, &delta);
        neighbors.sort_unstable();

        assert_eq!(neighbors, vec![1, 3, 5]);
    }

    #[test]
    fn test_apply_delta_mixed_operations() {
        let mut neighbors = vec![1, 2, 3];
        let delta = DeltaLog {
            ops: vec![
                DeltaOperation {
                    timestamp: 1,
                    neighbor: 2,
                    op_type: 1,
                }, // Remove 2
                DeltaOperation {
                    timestamp: 2,
                    neighbor: 4,
                    op_type: 0,
                }, // Add 4
                DeltaOperation {
                    timestamp: 3,
                    neighbor: 2,
                    op_type: 0,
                }, // Add 2 back
            ],
        };

        LsmCommunity::apply_delta_to_neighbors(&mut neighbors, &delta);
        neighbors.sort_unstable();

        assert_eq!(neighbors, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_apply_delta_override_same_neighbor() {
        let mut neighbors = vec![1, 2, 3];
        let delta = DeltaLog {
            ops: vec![
                DeltaOperation {
                    timestamp: 1,
                    neighbor: 4,
                    op_type: 0,
                }, // Add 4
                DeltaOperation {
                    timestamp: 2,
                    neighbor: 4,
                    op_type: 1,
                }, // Remove 4
                DeltaOperation {
                    timestamp: 3,
                    neighbor: 4,
                    op_type: 0,
                }, // Add 4 again
            ],
        };

        LsmCommunity::apply_delta_to_neighbors(&mut neighbors, &delta);
        neighbors.sort_unstable();

        // Final state: 4 should exist (last operation wins)
        assert_eq!(neighbors, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_apply_delta_empty() {
        let mut neighbors = vec![1, 2, 3];
        let delta = DeltaLog { ops: vec![] };

        LsmCommunity::apply_delta_to_neighbors(&mut neighbors, &delta);

        assert_eq!(neighbors, vec![1, 2, 3]);
    }
}
