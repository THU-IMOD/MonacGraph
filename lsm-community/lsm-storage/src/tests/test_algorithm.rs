#[cfg(test)]
mod test_algorithm_bfs {

    use crate::{comm_io::LsmCommunity, config::LsmCommunityStorageOptions, types::VId};
    use rustc_hash::FxHashMap;
    use serial_test::serial;

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_bfs() -> anyhow::Result<()> {
        // Replace when the large graph is ready
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;
        // Perform BFS;
        let start_vertex: VId = 0; // Start BFS from vertex 0
        let bfs_result = lsm_community.bfs(start_vertex);
        // Verify BFS result
        // Expected BFS result from vertex 0
        let mut expected: FxHashMap<VId, u32> = FxHashMap::default();
        expected.insert(0, 0);
        expected.insert(2, 1);
        expected.insert(3, 2);
        expected.insert(4, 3);
        expected.insert(11, 3);
        expected.insert(6, 4);
        expected.insert(7, 4);
        expected.insert(12, 4);
        expected.insert(5, 5);
        expected.insert(8, 5);
        expected.insert(9, 5);
        expected.insert(10, 6);

        // Verify the BFS result
        assert_eq!(
            bfs_result.len(),
            expected.len(),
            "BFS result count mismatch: expected {}, got {}",
            expected.len(),
            bfs_result.len()
        );

        for (vertex, distance) in bfs_result {
            println!("Vertex: {}, Distance: {}", vertex, distance);

            assert!(
                expected.contains_key(&vertex),
                "Unexpected vertex {} in BFS result",
                vertex
            );

            assert_eq!(
                expected[&vertex], distance,
                "Distance mismatch for vertex {}: expected {}, got {}",
                vertex, expected[&vertex], distance
            );
        }

        println!("BFS test passed: all vertices and distances are correct!");

        Ok(())
    }
}

#[cfg(test)]
mod test_algorithm_wcc {
    use crate::{comm_io::LsmCommunity, config::LsmCommunityStorageOptions};
    use serial_test::serial;

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_wcc() -> anyhow::Result<()> {
        // Replace when the large graph is ready
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;
        // Perform WCC;
        let wcc_result = lsm_community.wcc();
        // Verify WCC result
        // Assert that WCC result is not trivial (all zeros)
        assert!(
            wcc_result.iter().all(|&comp_id| comp_id == 0),
            "WCC result is Wrong"
        );
        Ok(())
    }
}

#[cfg(test)]
mod test_algorithm_community {
    use crate::{comm_io::LsmCommunity, config::LsmCommunityStorageOptions};
    use serial_test::serial;

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_community_detection() -> anyhow::Result<()> {
        // Replace when the large graph is ready
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;
        // Perform community detection;
        let community_result = lsm_community.community_detection();
        // Verify community detection result
        // Assert that community detection result is not trivial (all zeros)
        assert_eq!(community_result.len(), 4);
        Ok(())
    }

    #[test]
    #[serial(lsm_community_example)]
    fn test_simple_community_search() -> anyhow::Result<()> {
        // Replace when the large graph is ready
        let graph_name = "example";
        // Setup storage options
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();
        let lsm_community = LsmCommunity::open(options)?;
        // Perform community detection;
        let community_result = lsm_community.community_search(0).unwrap();
        // Verify community detection result
        // Assert that community detection result is not trivial (all zeros)
        assert_eq!(community_result.len(), 4);
        Ok(())
    }
}
