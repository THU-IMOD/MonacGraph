pub mod builder;
pub mod iterator;

use crate::types::{VIdList, VIdListView, VertexList, VertexListView};
use iterator::{BlockEdgeIterator, NeighborIterator, VertexIterator};

/// A block is the smallest unit of read and caching in LSM-Community-Storage.
/// It is a Paged CSR of subgraph.
#[allow(dead_code)]
pub struct Block {
    pub vertex_count: u16,
    pub edge_count: u16,
    pub vertex_list_view: VertexListView,
    pub edge_list_view: VIdListView,
    pub data: Vec<u8>,
}

#[allow(dead_code)]
impl Block {
    /// Create a new block from vertex list and edge list
    pub fn new(vertex_list: VertexList, edge_list: VIdList, block_size: usize) -> Self {
        let vertex_count = vertex_list.len() as u16;
        let edge_count = edge_list.len() as u16;

        // Calculate sizes
        let header_size = 4; // vertex_count (2B) + edge_count (2B)
        let vertex_list_size = (vertex_count as usize) * 8;
        let edge_list_size = (edge_count as usize) * 4;
        let total_size = header_size + vertex_list_size + edge_list_size;

        assert!(block_size >= total_size, "Too large for graph data.");

        let mut data = Vec::with_capacity(total_size);

        // Write header
        data.extend_from_slice(&vertex_count.to_be_bytes());
        data.extend_from_slice(&edge_count.to_be_bytes());

        // Write vertex list
        for (vid, offset) in &vertex_list {
            data.extend_from_slice(&vid.to_be_bytes());
            data.extend_from_slice(&offset.to_be_bytes());
        }

        // Write edge list
        for edge in &edge_list {
            data.extend_from_slice(&edge.to_be_bytes());
        }

        data.resize(block_size, 0);

        // Create views
        let vertex_list_view = VertexListView {
            offset: header_size,
            len: vertex_list_size,
        };

        let edge_list_view = VIdListView {
            offset: header_size + vertex_list_size,
            len: edge_list_size,
        };

        Block {
            vertex_count,
            edge_count,
            vertex_list_view,
            edge_list_view,
            data,
        }
    }

    /// Encode to bytes (returns reference to internal data)
    pub fn encode(&self) -> &[u8] {
        &self.data
    }

    /// Decode from raw bytes with partial parsing
    pub fn decode(data: Vec<u8>) -> Self {
        assert!(data.len() >= 4, "Data too short for header");

        // Parse header (only counts)
        let vertex_count = u16::from_be_bytes([data[0], data[1]]);
        let edge_count = u16::from_be_bytes([data[2], data[3]]);

        // Calculate view metadata
        let header_size = 4;
        let vertex_list_size = (vertex_count as usize) * 8;
        let edge_list_size = (edge_count as usize) * 4;

        // Validate data size
        // let expected_size = header_size + vertex_list_size + edge_list_size;
        // assert_eq!(data.len(), expected_size, "Data size mismatch");

        // Create views (no parsing, just metadata)
        let vertex_list_view = VertexListView {
            offset: header_size,
            len: vertex_list_size,
        };

        let edge_list_view = VIdListView {
            offset: header_size + vertex_list_size,
            len: edge_list_size,
        };

        Block {
            vertex_count,
            edge_count,
            vertex_list_view,
            edge_list_view,
            data,
        }
    }

    /// Create an iterator over all vertices
    pub fn get_vertex_iter(&self) -> VertexIterator<'_> {
        VertexIterator::new(self)
    }

    /// Create an iterator over neighbors of a specific vertex
    pub fn get_neighbor_iter(&self, vertex_index: usize) -> Option<NeighborIterator<'_>> {
        NeighborIterator::new(self, vertex_index)
    }

    /// Create an iterator over all edges (source, destination pairs)
    pub fn get_edge_iter(&self) -> BlockEdgeIterator<'_> {
        BlockEdgeIterator::new(self)
    }

    /// Get a cloned list of all neighbors for a vertex
    /// This performs a copy of the neighbor data
    pub fn get_neighbor_clone(&self, vertex_index: usize) -> Option<VIdList> {
        self.get_neighbor_iter(vertex_index)
            .map(|iter| iter.collect())
    }
}
