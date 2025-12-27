use super::Block;
use rustc_hash::FxHashMap;

/// Builds a block for graph storage.
pub struct BlockBuilder {
    /// Vertex list being built: (vertex_id, edge_offset)
    vertices: Vec<(u32, u32)>,
    /// Edge list being built
    edges: Vec<u32>,
    /// The expected block size in bytes
    block_size: usize,
    /// Current edge offset (number of edges added so far)
    current_edge_offset: u32,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
            block_size,
            current_edge_offset: 0,
        }
    }

    /// Estimate the current size of the block in bytes
    fn estimated_size(&self) -> usize {
        let header_size = 4; // vertex_count (2B) + edge_count (2B)
        let vertex_list_size = self.vertices.len() * 8; // (u32 + u32) per vertex
        let edge_list_size = self.edges.len() * 4; // u32 per edge
        header_size + vertex_list_size + edge_list_size
    }

    /// Adds a vertex with its neighbors to the block.
    /// Returns false when the block is full and cannot accommodate this vertex.
    ///
    /// # Arguments
    /// * `vertex_id` - The ID of the vertex
    /// * `neighbors` - Slice of neighbor vertex IDs
    #[must_use]
    pub fn add_vertex(&mut self, vertex_id: u32, neighbors: &[u32]) -> bool {
        // Calculate the size needed for this addition
        let vertex_entry_size = 8; // vertex_id (4B) + offset (4B)
        let edges_size = neighbors.len() * 4; // each edge is 4B
        let new_size = self.estimated_size() + vertex_entry_size + edges_size;

        // Check if adding this vertex would exceed block size
        // Allow the first vertex even if it exceeds block_size (like MiniLSM)
        if new_size > self.block_size && !self.is_empty() {
            return false;
        }

        // Add vertex entry with current edge offset
        self.vertices.push((vertex_id, self.current_edge_offset));

        // Add all neighbor edges
        self.edges.extend_from_slice(neighbors);
        self.current_edge_offset += neighbors.len() as u32;

        true
    }

    /// Adds a vertex with no neighbors to the block.
    /// This is a convenience method for isolated vertices.
    #[must_use]
    pub fn add_isolated_vertex(&mut self, vertex_id: u32) -> bool {
        self.add_vertex(vertex_id, &[])
    }

    /// Check if there are no vertices in the block.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty()
    }

    /// Get the number of vertices currently in the builder
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Get the number of edges currently in the builder
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Finalize the block and consume the builder.
    /// Returns the built block and a mapping from vertex_id to its index in the vertex list.
    ///
    /// # Returns
    /// A tuple of (Block, FxHashMap<u32, u16>) where:
    /// - Block: the constructed block
    /// - FxHashMap: maps vertex_id -> vertex_index (0-based position in vertex list)
    ///
    /// # Panics
    /// Panics if the block is empty.
    pub fn build(self) -> (Block, FxHashMap<u32, u16>) {
        if self.is_empty() {
            panic!("block should not be empty");
        }

        // Build the vertex_id to index mapping
        // Pre-allocate with exact capacity to avoid resizing
        let mut vertex_index_map =
            FxHashMap::with_capacity_and_hasher(self.vertices.len(), Default::default());

        for (index, &(vertex_id, _offset)) in self.vertices.iter().enumerate() {
            vertex_index_map.insert(vertex_id, index as u16);
        }

        let block = Block::new(self.vertices, self.edges, self.block_size);

        (block, vertex_index_map)
    }

    /// Clear the builder for reuse
    pub fn clear(&mut self) {
        self.vertices.clear();
        self.edges.clear();
        self.current_edge_offset = 0;
    }

    /// Try to add a vertex, and if it fails, build the current block and start a new one.
    /// Returns Some(Block) if a block was built, None if the vertex was successfully added.
    pub fn add_vertex_or_build(&mut self, vertex_id: u32, neighbors: &[u32]) -> Option<Block> {
        if !self.add_vertex(vertex_id, neighbors) {
            // Build current block
            let vertices = std::mem::take(&mut self.vertices);
            let edges = std::mem::take(&mut self.edges);
            self.current_edge_offset = 0;

            let block = Block::new(vertices, edges, self.block_size);

            // Add the vertex to the new block
            let added = self.add_vertex(vertex_id, neighbors);
            assert!(added, "vertex should fit in empty block");

            Some(block)
        } else {
            None
        }
    }
}
