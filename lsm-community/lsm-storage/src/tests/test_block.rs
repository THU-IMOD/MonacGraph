#[cfg(test)]
mod tests_block_builder {
    use crate::block::builder::BlockBuilder;

    #[test]
    fn test_block_builder_basic() {
        let mut builder = BlockBuilder::new(1024);

        assert!(builder.is_empty());
        assert_eq!(builder.vertex_count(), 0);
        assert_eq!(builder.edge_count(), 0);

        // Add first vertex with neighbors
        assert!(builder.add_vertex(1, &[2, 3, 4]));
        assert_eq!(builder.vertex_count(), 1);
        assert_eq!(builder.edge_count(), 3);

        // Add second vertex
        assert!(builder.add_vertex(2, &[5, 6]));
        assert_eq!(builder.vertex_count(), 2);
        assert_eq!(builder.edge_count(), 5);

        // Build and verify
        let (block, _) = builder.build();
        assert_eq!(block.vertex_count, 2);
        assert_eq!(block.edge_count, 5);
    }

    #[test]
    fn test_block_builder_size_limit() {
        // Small block size to test limits
        let mut builder = BlockBuilder::new(50);

        // First vertex should always be added
        assert!(builder.add_vertex(1, &[2, 3, 4, 5, 6]));

        // Second vertex might not fit
        let res = builder.add_vertex(2, &[7, 8, 9, 10]);
        println!("Result: {}", res);
        // Depending on exact size, this might succeed or fail
        // The important thing is it doesn't panic
    }

    #[test]
    fn test_block_builder_isolated_vertex() {
        let mut builder = BlockBuilder::new(1024);

        assert!(builder.add_isolated_vertex(1));
        assert!(builder.add_vertex(2, &[3, 4]));
        assert!(builder.add_isolated_vertex(5));

        let (block, _) = builder.build();
        assert_eq!(block.vertex_count, 3);
        assert_eq!(block.edge_count, 2);
    }

    #[test]
    #[should_panic(expected = "block should not be empty")]
    fn test_block_builder_empty_build() {
        let builder = BlockBuilder::new(1024);
        builder.build();
    }

    #[test]
    fn test_block_builder_clear() {
        let mut builder = BlockBuilder::new(1024);

        let res = builder.add_vertex(1, &[2, 3]);
        println!("Result: {}", res);
        assert!(!builder.is_empty());

        builder.clear();
        assert!(builder.is_empty());
        assert_eq!(builder.vertex_count(), 0);
        assert_eq!(builder.edge_count(), 0);
    }

    #[test]
    fn test_add_vertex_or_build() {
        let mut builder = BlockBuilder::new(100); // Small size

        // First addition should succeed
        let result = builder.add_vertex_or_build(1, &[2, 3, 4]);
        assert!(result.is_none());

        // Keep adding until we trigger a build
        let mut blocks = Vec::new();
        for i in 2..10 {
            if let Some(block) = builder.add_vertex_or_build(i, &[i + 1, i + 2, i + 3]) {
                blocks.push(block);
            }
        }

        // Should have created at least one block
        assert!(!blocks.is_empty());
    }
}

#[cfg(test)]
mod test_block_iterator {
    use crate::{block::Block, types::VId};

    #[test]
    fn test_vertex_iterator() {
        let vertex_list = vec![(10, 0), (20, 3), (30, 5)];
        let edge_list = vec![20, 30, 40, 10, 30, 10, 20];

        let block = Block::new(vertex_list.clone(), edge_list, 4096);

        let vertices: Vec<_> = block.get_vertex_iter().collect();
        assert_eq!(vertices, vertex_list);
        assert_eq!(block.get_vertex_iter().len(), 3);
    }

    #[test]
    fn test_neighbor_iterator() {
        let vertex_list = vec![(0, 0), (1, 2), (2, 5)];
        let edge_list = vec![1, 2, 0, 2, 3, 0, 1];

        let block = Block::new(vertex_list, edge_list, 4096);

        // Vertex 0: neighbors [1, 2] (offset 0-2)
        let neighbors: Vec<_> = block.get_neighbor_iter(0).unwrap().collect();
        assert_eq!(neighbors, vec![1, 2]);
        assert_eq!(block.get_neighbor_iter(0).unwrap().len(), 2);

        // Vertex 1: neighbors [0, 2, 3] (offset 2-5)
        let neighbors: Vec<_> = block.get_neighbor_iter(1).unwrap().collect();
        assert_eq!(neighbors, vec![0, 2, 3]);
        assert_eq!(block.get_neighbor_iter(1).unwrap().len(), 3);

        // Vertex 2: neighbors [0, 1] (offset 5-7)
        let neighbors: Vec<_> = block.get_neighbor_iter(2).unwrap().collect();
        assert_eq!(neighbors, vec![0, 1]);
        assert_eq!(block.get_neighbor_iter(2).unwrap().len(), 2);
    }

    #[test]
    fn test_iterator_exhaustion() {
        let vertex_list = vec![(1, 0)];
        let edge_list = vec![2];

        let block = Block::new(vertex_list, edge_list, 4096);

        let mut iter = block.get_vertex_iter();
        assert_eq!(iter.next(), Some((1, 0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None); // Should stay None
    }

    #[test]
    fn test_empty_neighbors() {
        let vertex_list = vec![(0, 0), (1, 0), (2, 0)];
        let edge_list = vec![];

        let block = Block::new(vertex_list, edge_list, 4096);

        // All vertices have no neighbors
        for i in 0..3 {
            let neighbors: Vec<_> = block.get_neighbor_iter(i).unwrap().collect();
            assert_eq!(neighbors, Vec::<VId>::new());
        }
    }
}

#[cfg(test)]
mod test_block {
    use crate::{
        block::Block,
        types::{VId, VIdList, VertexList},
    };

    /// Helper function to create the test graph block
    /// Graph: 13 vertices, 20 edges
    /// Based on the graph data provided
    fn create_test_graph_block() -> Block {
        // Build adjacency list from edge data
        // Edges: (0,2), (1,0), (1,2), (1,3), (2,3), (3,0), (3,4), (3,11),
        //        (4,6), (4,7), (5,4), (6,5), (7,3), (7,8), (7,9), (8,9),
        //        (8,10), (10,7), (10,9), (11,12)

        // Build CSR format manually
        // For each vertex, we need to know where its neighbors start in edge_list
        let vertex_list: VertexList = vec![
            (0, 0),   // vertex 0: 1 edge  -> offset 0
            (1, 1),   // vertex 1: 3 edges -> offset 1
            (2, 4),   // vertex 2: 1 edge  -> offset 4
            (3, 5),   // vertex 3: 3 edges -> offset 5
            (4, 8),   // vertex 4: 2 edges -> offset 8
            (5, 10),  // vertex 5: 1 edge  -> offset 10
            (6, 11),  // vertex 6: 1 edge  -> offset 11
            (7, 12),  // vertex 7: 3 edges -> offset 12
            (8, 15),  // vertex 8: 2 edges -> offset 15
            (9, 17),  // vertex 9: 0 edges -> offset 17
            (10, 17), // vertex 10: 2 edges -> offset 17
            (11, 19), // vertex 11: 1 edge -> offset 19
            (12, 20), // vertex 12: 0 edges -> offset 20
        ];

        // All neighbors in CSR order
        let edge_list: VIdList = vec![
            2, // neighbors of vertex 0
            0, 2, 3, // neighbors of vertex 1
            3, // neighbors of vertex 2
            0, 4, 11, // neighbors of vertex 3
            6, 7, // neighbors of vertex 4
            4, // neighbors of vertex 5
            5, // neighbors of vertex 6
            3, 8, 9, // neighbors of vertex 7
            9, 10, // neighbors of vertex 8
            // vertex 9 has no outgoing edges
            7, 9, // neighbors of vertex 10
            12, // neighbors of vertex 11
               // vertex 12 has no outgoing edges
        ];

        Block::new(vertex_list, edge_list, 4096)
    }

    #[test]
    fn test_block_basic() {
        let block = create_test_graph_block();

        // Test basic counts
        assert_eq!(block.vertex_count, 13);
        assert_eq!(block.edge_count, 20);

        // Test data size
        let expected_size = 4096; // header + vertices + edges
        assert_eq!(block.data.len(), expected_size);
    }

    #[test]
    fn test_encode_decode() {
        let block = create_test_graph_block();

        // Encode
        let encoded = block.encode();
        assert_eq!(encoded.len(), 4096);

        // Decode
        let decoded = Block::decode(encoded.to_vec());

        assert_eq!(decoded.vertex_count, 13);
        assert_eq!(decoded.edge_count, 20);
        assert_eq!(decoded.data.len(), block.data.len());
    }

    #[test]
    fn test_vertex_iterator() {
        let block = create_test_graph_block();

        // Collect all vertices
        let vertices: Vec<_> = block.get_vertex_iter().collect();

        assert_eq!(vertices.len(), 13);
        assert_eq!(block.get_vertex_iter().len(), 13);

        // Check first few vertices
        assert_eq!(vertices[0], (0, 0));
        assert_eq!(vertices[1], (1, 1));
        assert_eq!(vertices[2], (2, 4));
        assert_eq!(vertices[3], (3, 5));

        // Check last vertex
        assert_eq!(vertices[12], (12, 20));
    }

    #[test]
    fn test_neighbor_iterator_vertex_0() {
        let block = create_test_graph_block();

        // Vertex 0: neighbors [2]
        let neighbors: Vec<_> = block.get_neighbor_iter(0).unwrap().collect();
        assert_eq!(neighbors, vec![2]);
        assert_eq!(block.get_neighbor_iter(0).unwrap().len(), 1);
    }

    #[test]
    fn test_neighbor_iterator_vertex_1() {
        let block = create_test_graph_block();

        // Vertex 1: neighbors [0, 2, 3]
        let neighbors: Vec<_> = block.get_neighbor_iter(1).unwrap().collect();
        assert_eq!(neighbors, vec![0, 2, 3]);
        assert_eq!(block.get_neighbor_iter(1).unwrap().len(), 3);
    }

    #[test]
    fn test_neighbor_iterator_vertex_3() {
        let block = create_test_graph_block();

        // Vertex 3: neighbors [0, 4, 11]
        let neighbors: Vec<_> = block.get_neighbor_iter(3).unwrap().collect();
        assert_eq!(neighbors, vec![0, 4, 11]);
        assert_eq!(block.get_neighbor_iter(3).unwrap().len(), 3);
    }

    #[test]
    fn test_neighbor_iterator_vertex_7() {
        let block = create_test_graph_block();

        // Vertex 7: neighbors [3, 8, 9]
        let neighbors: Vec<_> = block.get_neighbor_iter(7).unwrap().collect();
        assert_eq!(neighbors, vec![3, 8, 9]);
        assert_eq!(block.get_neighbor_iter(7).unwrap().len(), 3);
    }

    #[test]
    fn test_neighbor_iterator_empty_neighbors() {
        let block = create_test_graph_block();

        // Vertex 9: no outgoing edges
        let neighbors: Vec<_> = block.get_neighbor_iter(9).unwrap().collect();
        assert_eq!(neighbors, Vec::<u32>::new());
        assert_eq!(block.get_neighbor_iter(9).unwrap().len(), 0);

        // Vertex 12: no outgoing edges
        let neighbors: Vec<_> = block.get_neighbor_iter(12).unwrap().collect();
        assert_eq!(neighbors, Vec::<u32>::new());
        assert_eq!(block.get_neighbor_iter(12).unwrap().len(), 0);
    }

    #[test]
    fn test_neighbor_iterator_all_vertices() {
        let block = create_test_graph_block();

        // Expected neighbors for each vertex
        let expected_neighbors = vec![
            vec![2],        // v0
            vec![0, 2, 3],  // v1
            vec![3],        // v2
            vec![0, 4, 11], // v3
            vec![6, 7],     // v4
            vec![4],        // v5
            vec![5],        // v6
            vec![3, 8, 9],  // v7
            vec![9, 10],    // v8
            vec![],         // v9
            vec![7, 9],     // v10
            vec![12],       // v11
            vec![],         // v12
        ];

        for (vid, expected) in expected_neighbors.iter().enumerate() {
            let neighbors: Vec<_> = block.get_neighbor_iter(vid).unwrap().collect();
            assert_eq!(neighbors, *expected, "Vertex {} neighbors mismatch", vid);
        }
    }

    #[test]
    fn test_neighbor_iterator_out_of_bounds() {
        let block = create_test_graph_block();

        // Try to get neighbors of non-existent vertex
        assert!(block.get_neighbor_iter(13).is_none());
        assert!(block.get_neighbor_iter(100).is_none());
    }

    #[test]
    fn test_empty_block() {
        let vertex_list: VertexList = vec![];
        let edge_list: VIdList = vec![];

        let block = Block::new(vertex_list, edge_list, 4096);

        assert_eq!(block.vertex_count, 0);
        assert_eq!(block.edge_count, 0);
        assert_eq!(block.get_vertex_iter().len(), 0);
        assert_eq!(block.get_edge_iter().len(), 0);
    }

    #[test]
    fn test_vertices_no_edges() {
        let vertex_list: VertexList = vec![(0, 0), (1, 0), (2, 0)];
        let edge_list: VIdList = vec![];

        let block = Block::new(vertex_list, edge_list, 4096);

        assert_eq!(block.vertex_count, 3);
        assert_eq!(block.edge_count, 0);

        // All vertices should have no neighbors
        for i in 0..3 {
            let neighbors: Vec<_> = block.get_neighbor_iter(i).unwrap().collect();
            assert_eq!(neighbors, Vec::<u32>::new());
        }
    }

    #[test]
    fn test_roundtrip_with_test_graph() {
        let block1 = create_test_graph_block();

        // Encode
        let encoded = block1.encode();

        // Decode
        let block2 = Block::decode(encoded.to_vec());

        // Verify counts
        assert_eq!(block2.vertex_count, block1.vertex_count);
        assert_eq!(block2.edge_count, block1.edge_count);

        // Verify all vertices
        let vertices1: Vec<_> = block1.get_vertex_iter().collect();
        let vertices2: Vec<_> = block2.get_vertex_iter().collect();
        assert_eq!(vertices1, vertices2);

        // Verify all edges
        let edges1: Vec<_> = block1.get_edge_iter().collect();
        let edges2: Vec<_> = block2.get_edge_iter().collect();
        assert_eq!(edges1, edges2);

        // Verify neighbors for all vertices
        for vid in 0..13 {
            let neighbors1: Vec<_> = block1.get_neighbor_iter(vid).unwrap().collect();
            let neighbors2: Vec<_> = block2.get_neighbor_iter(vid).unwrap().collect();
            assert_eq!(neighbors1, neighbors2, "Vertex {} neighbors mismatch", vid);
        }
    }

    #[test]
    fn test_iterator_exhaustion() {
        let block = create_test_graph_block();

        let mut vertex_iter = block.get_vertex_iter();

        // Consume all vertices
        for _ in 0..13 {
            assert!(vertex_iter.next().is_some());
        }

        // Should be exhausted
        assert!(vertex_iter.next().is_none());
        assert!(vertex_iter.next().is_none());
    }

    #[test]
    fn test_get_neighbor_clone() {
        let block = create_test_graph_block();

        // Vertex 1: neighbors [0, 2, 3]
        let neighbors = block.get_neighbor_clone(1).unwrap();
        assert_eq!(neighbors, vec![0, 2, 3]);

        // Vertex 7: neighbors [3, 8, 9]
        let neighbors = block.get_neighbor_clone(7).unwrap();
        assert_eq!(neighbors, vec![3, 8, 9]);

        // Vertex 9: no neighbors
        let neighbors = block.get_neighbor_clone(9).unwrap();
        assert_eq!(neighbors, Vec::<u32>::new());

        // Out of bounds
        assert!(block.get_neighbor_clone(13).is_none());
    }

    #[test]
    fn test_neighbor_clone_vs_iterator() {
        let block = create_test_graph_block();

        // Verify that clone and iterator produce same results
        for vid in 0..13 {
            let from_clone = block.get_neighbor_clone(vid).unwrap();
            let from_iter: Vec<_> = block.get_neighbor_iter(vid).unwrap().collect();
            assert_eq!(from_clone, from_iter, "Vertex {} mismatch", vid);
        }
    }

    #[test]
    fn test_block_edge_iterator() {
        let vertex_list = vec![(10, 0), (20, 3), (30, 5)];
        let edge_list = vec![20, 30, 40, 10, 30, 10, 20];

        let block = Block::new(vertex_list, edge_list, 4096);

        let edges: Vec<_> = block.get_edge_iter().collect();
        assert_eq!(
            edges,
            vec![
                (10, 20),
                (10, 30),
                (10, 40), // vertex 10's neighbors
                (20, 10),
                (20, 30), // vertex 20's neighbors
                (30, 10),
                (30, 20), // vertex 30's neighbors
            ]
        );
        assert_eq!(block.get_edge_iter().len(), 7);
    }

    #[test]
    fn test_block_edge_iterator_with_empty_vertices() {
        // Some vertices have no neighbors
        let vertex_list = vec![(0, 0), (1, 0), (2, 0)];
        let edge_list = vec![3, 4];

        let block = Block::new(vertex_list, edge_list, 4096);

        let edges: Vec<_> = block.get_edge_iter().collect();
        assert_eq!(
            edges,
            vec![
                (2, 3),
                (2, 4), // only vertex 2 has neighbors
            ]
        );
        assert_eq!(block.get_edge_iter().len(), 2);
    }

    #[test]
    fn test_block_edge_iterator_empty() {
        let vertex_list = vec![(0, 0), (1, 0)];
        let edge_list = vec![];

        let block = Block::new(vertex_list, edge_list, 4096);

        let edges: Vec<_> = block.get_edge_iter().collect();
        assert_eq!(edges, Vec::<(VId, VId)>::new());
        assert_eq!(block.get_edge_iter().len(), 0);
    }

    #[test]
    fn test_block_edge_iterator_single_edge() {
        let vertex_list = vec![(5, 0), (10, 1)];
        let edge_list = vec![10];

        let block = Block::new(vertex_list, edge_list, 4096);

        let edges: Vec<_> = block.get_edge_iter().collect();
        assert_eq!(edges, vec![(5, 10)]);
        assert_eq!(block.get_edge_iter().len(), 1);
    }
}
