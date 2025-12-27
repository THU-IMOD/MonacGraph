#[cfg(test)]
mod test_graph {
    use crate::{graph::CsrGraph, types::VId};

    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_graph_from_temp_file() -> std::io::Result<()> {
        // Create a temporary file with test graph data
        let mut temp_file = NamedTempFile::new()?;

        let graph_data = "\
t 13 20
v 0 0 0
v 1 0 0
v 2 0 0
v 3 0 0
v 4 0 1
v 5 0 1
v 6 0 1
v 7 0 2
v 8 0 2
v 9 0 2
v 10 0 2
v 11 0 3
v 12 0 3
e 0 2
e 1 0
e 1 2
e 1 3
e 2 3
e 3 0
e 3 4
e 3 11
e 4 6
e 4 7
e 5 4
e 6 5
e 7 3
e 7 8
e 7 9
e 8 9
e 8 10
e 10 7
e 10 9
e 11 12
";

        temp_file.write_all(graph_data.as_bytes())?;
        temp_file.flush()?;

        // Load the graph
        let graph = CsrGraph::from_file(temp_file.path())?;

        // Verify basic properties
        assert_eq!(graph.num_vertices(), 13, "Incorrect number of vertices");
        assert_eq!(graph.num_edges(), 20, "Incorrect number of edges");

        // Verify communities
        assert_eq!(graph.communities()[0], 0);
        assert_eq!(graph.communities()[4], 1);
        assert_eq!(graph.communities()[7], 2);
        assert_eq!(graph.communities()[11], 3);

        // Verify CSR structure - check vertex 3's neighbors
        let v3_start = graph.offsets()[3];
        let v3_end = graph.offsets()[4];
        let v3_neighbors: Vec<VId> = graph.neighbors()[v3_start..v3_end].to_vec();

        assert_eq!(
            v3_neighbors,
            vec![0, 4, 11],
            "Vertex 3 should have neighbors [0, 4, 11]"
        );

        // Verify vertex 1's neighbors
        let v1_start = graph.offsets()[1];
        let v1_end = graph.offsets()[2];
        let v1_neighbors: Vec<VId> = graph.neighbors()[v1_start..v1_end].to_vec();

        assert_eq!(
            v1_neighbors,
            vec![0, 2, 3],
            "Vertex 1 should have neighbors [0, 2, 3]"
        );

        Ok(())
    }

    #[test]
    fn test_load_graph_from_file() -> std::io::Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Verify basic properties
        assert_eq!(graph.num_vertices(), 13, "Incorrect number of vertices");
        assert_eq!(graph.num_edges(), 20, "Incorrect number of edges");

        // Verify communities
        assert_eq!(graph.communities()[0], 0);
        assert_eq!(graph.communities()[4], 1);
        assert_eq!(graph.communities()[7], 2);
        assert_eq!(graph.communities()[11], 3);

        // Verify CSR structure - check vertex 3's neighbors
        let v3_start = graph.offsets()[3];
        let v3_end = graph.offsets()[4];
        let v3_neighbors: Vec<VId> = graph.neighbors()[v3_start..v3_end].to_vec();

        assert_eq!(
            v3_neighbors,
            vec![0, 4, 11],
            "Vertex 3 should have neighbors [0, 4, 11]"
        );

        // Verify vertex 1's neighbors
        let v1_start = graph.offsets()[1];
        let v1_end = graph.offsets()[2];
        let v1_neighbors: Vec<VId> = graph.neighbors()[v1_start..v1_end].to_vec();

        assert_eq!(
            v1_neighbors,
            vec![0, 2, 3],
            "Vertex 1 should have neighbors [0, 2, 3]"
        );

        Ok(())
    }

    #[test]
    fn test_induced_graph() -> std::io::Result<()> {
        // Load the graph
        let graph = CsrGraph::from_file("../data/example.graph")?;

        // Compute induced subgraph for vertices [0, 1, 2, 3]
        let vertex_ids = vec![0, 1, 2, 3];
        let induced_iters = graph.induced_graph(&vertex_ids);

        // Collect neighbors for each vertex
        let induced_neighbors: Vec<Vec<VId>> = induced_iters
            .into_iter()
            .map(|iter| iter.collect())
            .collect();

        // Verify the induced subgraph
        // Vertex 0's neighbors: [2] (all neighbors are in the subgraph)
        assert_eq!(
            induced_neighbors[0],
            vec![2],
            "Vertex 0 should have neighbor [2]"
        );

        // Vertex 1's neighbors: [0, 2, 3] (all neighbors are in the subgraph)
        assert_eq!(
            induced_neighbors[1],
            vec![0, 2, 3],
            "Vertex 1 should have neighbors [0, 2, 3]"
        );

        // Vertex 2's neighbors: [3] (all neighbors are in the subgraph)
        assert_eq!(
            induced_neighbors[2],
            vec![3],
            "Vertex 2 should have neighbor [3]"
        );

        // Vertex 3's neighbors: [0, 4, 11] but only [0] is in the subgraph
        assert_eq!(
            induced_neighbors[3],
            vec![0, 4, 11],
            "Vertex 3 should have all neighbors [0, 4, 11] (no filtering for storage)"
        );

        Ok(())
    }

    #[test]
    fn test_compute_community_list() -> std::io::Result<()> {
        // Load the graph
        let mut graph = CsrGraph::from_file("../data/example.graph")?;

        // Compute community structure
        let communities = graph.get_community_structure();

        // Verify number of communities (based on the test data, max_comm_id should be 3)
        assert_eq!(communities.len(), 4, "Should have 4 communities (0-3)");

        // Verify community 0: vertices [0, 1, 2, 3]
        assert_eq!(
            communities[0],
            vec![0, 1, 2, 3],
            "Community 0 should contain vertices [0, 1, 2, 3]"
        );

        // Verify community 1: vertices [4, 5, 6]
        assert_eq!(
            communities[1],
            vec![4, 5, 6],
            "Community 1 should contain vertices [4, 5, 6]"
        );

        // Verify community 2: vertices [7, 8, 9, 10]
        assert_eq!(
            communities[2],
            vec![7, 8, 9, 10],
            "Community 2 should contain vertices [7, 8, 9, 10]"
        );

        // Verify community 3: vertices [11, 12]
        assert_eq!(
            communities[3],
            vec![11, 12],
            "Community 3 should contain vertices [11, 12]"
        );

        // Verify all vertices are accounted for
        let total_vertices: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(
            total_vertices, 13,
            "Total vertices across all communities should be 13"
        );

        Ok(())
    }
}
