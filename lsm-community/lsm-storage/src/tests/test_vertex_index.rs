#[cfg(test)]
mod test_vertex_index {

    use crate::{
        graph::CsrGraph,
        vertex_index::{VertexIndex, VertexIndexItem},
    };

    #[test]
    fn test_build_large_vertex_index() -> std::io::Result<()> {
        // Load the graph
        // Switch to rn.graph when you want to test the large dataset.
        let mut graph = CsrGraph::from_file("../data/example.graph")?;
        // Build vertex index
        // giant_vertex_boundary = 10 (no vertex has degree >= 10 in this test)
        // giant_community_boundary = 1024 (1KB, all communities are small)
        let (vertex_index, giant_vertices) = VertexIndex::build_from_graph(
            &mut graph, 50,   // giant_vertex_boundary
            1024, // giant_community_boundary (1KB)
        );

        let virtual_comm_list = vertex_index.get_virtual_community_list_for_test();

        println!("Virtual Community Count: {}", virtual_comm_list.len());

        // Verify no giant vertices (all degrees < 10)
        assert_eq!(giant_vertices.len(), 0, "Should have no giant vertices");
        Ok(())
    }

    #[test]
    fn test_build_vertex_index() -> std::io::Result<()> {
        // Create a temporary file with test graph data

        // Load the graph
        let mut graph = CsrGraph::from_file("../data/example.graph")?;

        // Build vertex index
        // giant_vertex_boundary = 10 (no vertex has degree >= 10 in this test)
        // giant_community_boundary = 1024 (1KB, all communities are small)
        let (vertex_index, giant_vertices) = VertexIndex::build_from_graph(
            &mut graph, 10,   // giant_vertex_boundary
            1024, // giant_community_boundary (1KB)
        );

        // Verify no giant vertices (all degrees < 10)
        assert_eq!(giant_vertices.len(), 0, "Should have no giant vertices");

        // Verify all vertices are Normal (not Giant)
        for vid in 0..13 {
            assert!(
                vertex_index.vertex_array[vid].is_normal(),
                "Vertex {} should be Normal",
                vid
            );
        }

        // Verify vertex degrees
        assert_eq!(vertex_index.vertex_degree[0], 1); // v0 has 1 edge
        assert_eq!(vertex_index.vertex_degree[1], 3); // v1 has 3 edges
        assert_eq!(vertex_index.vertex_degree[3], 3); // v3 has 3 edges
        assert_eq!(vertex_index.vertex_degree[11], 1); // v11 has 1 edge

        // Verify community map was moved correctly
        assert_eq!(vertex_index.community_map.len(), 13);
        assert_eq!(vertex_index.community_map[0], 0);
        assert_eq!(vertex_index.community_map[4], 1);
        assert_eq!(vertex_index.community_map[7], 2);
        assert_eq!(vertex_index.community_map[11], 3);

        // Verify community list was moved correctly
        assert_eq!(vertex_index.community_list.len(), 4);
        assert_eq!(vertex_index.community_list[0], vec![0, 1, 2, 3]);
        assert_eq!(vertex_index.community_list[1], vec![4, 5, 6]);
        assert_eq!(vertex_index.community_list[2], vec![7, 8, 9, 10]);
        assert_eq!(vertex_index.community_list[3], vec![11, 12]);

        // Verify virtual community IDs are assigned
        // Since all communities are small, they should be grouped
        let v0_virtual_id = vertex_index.vertex_array[0].virtual_comm_id();
        let v4_virtual_id = vertex_index.vertex_array[4].virtual_comm_id();

        println!("Vertex 0 virtual_comm_id: {}", v0_virtual_id);
        println!("Vertex 4 virtual_comm_id: {}", v4_virtual_id);

        // All vertices should have valid virtual community IDs
        for vid in 0..13 {
            let item = &vertex_index.vertex_array[vid];
            assert!(item.is_normal());
            let virtual_id = item.virtual_comm_id();
            println!("Vertex {} -> virtual_comm_id: {}", vid, virtual_id);
        }

        Ok(())
    }

    #[test]
    fn test_build_vertex_index_with_giant_vertices() -> std::io::Result<()> {
        let mut graph = CsrGraph::from_file("../data/example.graph")?;

        // Set giant_vertex_boundary = 3, so vertices with degree >= 3 are giant
        let (vertex_index, giant_vertices) = VertexIndex::build_from_graph(
            &mut graph, 3,    // giant_vertex_boundary
            1024, // giant_community_boundary
        );

        // Verify giant vertices: v1 (degree=3), v3 (degree=3), v7 (degree=3)
        println!("Giant vertices: {:?}", giant_vertices);
        assert!(
            giant_vertices.len() >= 3,
            "Should have at least 3 giant vertices"
        );
        assert!(giant_vertices.contains(&1), "Vertex 1 should be giant");
        assert!(giant_vertices.contains(&3), "Vertex 3 should be giant");
        assert!(giant_vertices.contains(&7), "Vertex 7 should be giant");

        // Verify giant vertices in vertex_array
        assert!(
            vertex_index.vertex_array[1].is_giant(),
            "Vertex 1 should be Giant"
        );
        assert!(
            vertex_index.vertex_array[3].is_giant(),
            "Vertex 3 should be Giant"
        );
        assert!(
            vertex_index.vertex_array[7].is_giant(),
            "Vertex 7 should be Giant"
        );

        // Verify normal vertices
        assert!(
            vertex_index.vertex_array[0].is_normal(),
            "Vertex 0 should be Normal"
        );
        assert!(
            vertex_index.vertex_array[11].is_normal(),
            "Vertex 11 should be Normal"
        );

        Ok(())
    }

    #[test]
    fn test_set_normal_components() {
        let mut item = VertexIndexItem::normal(100, 50000, 2048);

        // Set all at once
        item.set_normal(200, 60000, 3096);
        assert_eq!(item.virtual_comm_id(), 200);
        assert_eq!(item.page_id(), 60000);
        assert_eq!(item.offset(), 3096);
    }

    #[test]
    fn test_set_individual_fields() {
        let mut item = VertexIndexItem::normal(100, 50000, 2048);

        // Set virtual_comm_id
        item.set_virtual_comm_id(999);
        assert_eq!(item.virtual_comm_id(), 999);
        assert_eq!(item.page_id(), 50000); // unchanged
        assert_eq!(item.offset(), 2048); // unchanged

        // Set page_id
        item.set_page_id(123456);
        assert_eq!(item.virtual_comm_id(), 999); // unchanged
        assert_eq!(item.page_id(), 123456);
        assert_eq!(item.offset(), 2048); // unchanged

        // Set offset
        item.set_offset(4095);
        assert_eq!(item.virtual_comm_id(), 999); // unchanged
        assert_eq!(item.page_id(), 123456); // unchanged
        assert_eq!(item.offset(), 4095);
    }

    #[test]
    #[should_panic(expected = "Cannot set components on Giant vertex")]
    fn test_set_normal_on_giant_panics() {
        let mut item = VertexIndexItem::giant();
        item.set_normal(100, 50000, 2048);
    }

    #[test]
    #[should_panic(expected = "Cannot set virtual_comm_id on Giant vertex")]
    fn test_set_virtual_comm_id_on_giant_panics() {
        let mut item = VertexIndexItem::giant();
        item.set_virtual_comm_id(100);
    }

    #[test]
    fn test_vertex_index_serialization() -> std::io::Result<()> {
        let graph_name = "example";
        let mut graph = CsrGraph::from_file(format!("../data/{}.graph", graph_name))?;
        // Build vertex index
        // giant_vertex_boundary = 10 (no vertex has degree >= 10 in this test)
        // giant_community_boundary = 1024 (1KB, all communities are small)
        let (vertex_index, _) = VertexIndex::build_from_graph(
            &mut graph, 50,   // giant_vertex_boundary
            1024, // giant_community_boundary (1KB)
        );

        // Serialize vertex index.
        vertex_index.serialize_to_file(
            format!("./workspace/test_{}/vertex_index.bin.zst", graph_name),
            3,
        )?;
        let loaded = VertexIndex::deserialize_from_file(format!(
            "./workspace/test_{}/vertex_index.bin.zst",
            graph_name
        ))
        .unwrap();
        assert_eq!(
            vertex_index.giant_vertex_boundary,
            loaded.giant_vertex_boundary
        );
        Ok(())
    }
}
