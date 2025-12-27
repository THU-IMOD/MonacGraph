use crate::types::graph_query::GraphQuery;
use crate::types::graph_serialize::{
    Offset, TopologyDecode, TopologyEncode, VertexId, VertexLength,
};
use crate::types::CSRGraph;
use crate::types::Vertex;
use std::collections::{BTreeMap, HashMap};

/// A block contains a CSR (Compressed Sparse Row) format representation of a part of a graph.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CSRCommBlock<T, L, O> {
    /// Total number of vertices in this CSR block.
    pub vertex_count: L,

    /// List of vertices with their corresponding offsets.
    /// Each tuple contains a vertex and its offset in the neighbor list.
    pub vertex_list: Vec<(Vertex<T>, O)>,

    /// Flattened list of all neighbors for all vertices.
    /// The offsets in vertex_list determine which neighbors belong to which vertex.
    pub neighbor_list: Vec<Vertex<T>>,

    /// HashMap for efficient vertex lookup.
    /// Maps vertex data to its index in the vertex_list for quick retrieval.
    pub(crate) vertex_index: HashMap<T, usize>,
}

#[allow(dead_code)]
impl CSRCommBlock<u64, u64, u64> {
    /// Converts this CSR block into a complete CSR graph structure.
    /// This method creates a new CSR graph containing only the internal connections
    /// between vertices present in this block.
    pub fn generate_csr_graph(&self) -> CSRGraph<u64, u64, u64> {
        // Vector to store the offset of each vertex in the new edge list
        let mut inner_offset = Vec::new();
        // Vector to store the flattened list of neighboring vertices
        let mut inner_edge_list = Vec::new();
        // Tracks the current offset position as we build the edge list
        let mut current_offset = 0u64;

        // Process each vertex in this block
        for (vertex, _) in &self.vertex_list {
            let global_vertex_id = vertex.vertex_id;

            // Filter neighbors to include only those present in this block
            // and convert them to their local indices
            let mut inner_neighbors = self
                .read_neighbor(&global_vertex_id)
                .iter()
                .filter_map(|n| {
                    if self.vertex_index.contains_key(&n.vertex_id) {
                        Some(self.vertex_index.get(&n.vertex_id).unwrap().clone() as u64)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            // Record the offset for this vertex
            inner_offset.push(current_offset);
            // Update the current offset for the next vertex
            current_offset += inner_neighbors.len() as u64;
            // Add this vertex's neighbors to the edge list
            inner_edge_list.append(&mut inner_neighbors);
        }

        // Use the same vertex count as the block
        let new_vertex_count = self.vertex_count;

        // Construct and return the new CSR graph
        CSRGraph {
            vertex_count: new_vertex_count,
            offsets: inner_offset,
            neighbor_list: inner_edge_list,
            community_index: Default::default(),
        }
    }
}

#[allow(dead_code)]
impl<T, L, O> TopologyEncode for CSRCommBlock<T, L, O>
where
    T: VertexId,     // Type T must implement the VertexId trait
    L: VertexLength, // Type L must implement the VertexLength trait
    O: Offset,       // Type O must implement the Offset trait
{
    /// Implements the TopologyEncode trait for CSRCommBlock.
    /// This method serializes the graph structure into a byte vector.
    /// Note: This only encodes the topology information and omits the vertex_index HashMap.
    fn encode_topology(&self) -> Vec<u8> {
        let mut encoded_bytes = Vec::<u8>::new();

        // First encode the total number of vertices
        let vertex_count_bytes = &self.vertex_count.to_bytes();
        encoded_bytes.extend_from_slice(vertex_count_bytes);

        // Then encode each vertex and its corresponding offset in the neighbor list
        for (vertex, offset) in &self.vertex_list {
            encoded_bytes.extend_from_slice(&vertex.encode_topology());
            encoded_bytes.extend_from_slice(&offset.to_bytes());
        }

        // Finally encode all neighbors in the neighbor list
        for neighbor in &self.neighbor_list {
            encoded_bytes.extend_from_slice(&neighbor.encode_topology());
        }
        // The vertex_index HashMap is not encoded as it can be reconstructed from vertex_list

        encoded_bytes
    }
}

#[allow(dead_code)]
impl<T, L, O> TopologyDecode for CSRCommBlock<T, L, O>
where
    T: VertexId,     // Type T must implement the VertexId trait
    L: VertexLength, // Type L must implement the VertexLength trait
    O: Offset,       // Type O must implement the Offset trait
{
    /// Implements the TopologyDecode trait for CSRCommBlock.
    /// This method deserializes a byte array back into a CSRCommBlock structure.
    /// Returns None if the byte array is invalid or lacks sufficient data.
    fn from_bytes_topology(bytes: &[u8]) -> Option<Self> {
        let mut decode_offset = 0usize;

        // Ensure there are enough bytes to read the vertex count
        if decode_offset + L::byte_size() > bytes.len() {
            // Insufficient data to decode vertex count
            return None;
        }

        // First decode the total number of vertices
        let vertex_count =
            L::from_bytes(&bytes[decode_offset..decode_offset + L::byte_size()]).unwrap();
        decode_offset += L::byte_size();

        // Initialize containers for decoded data
        let mut vertex_list = Vec::<(Vertex<T>, O)>::new();
        let mut vertex_index = HashMap::<T, usize>::new();

        // Convert vertex_count to usize for iteration
        let vertex_count_usize = match vertex_count.try_into() {
            Ok(count) => count,
            Err(_) => return None,
        };

        // Decode each vertex and its offset
        for vertex_idx in 0..vertex_count_usize {
            // Decode the vertex
            let decoded_vertex_opt = Vertex::<T>::from_bytes_topology(
                &bytes[decode_offset..decode_offset + Vertex::<T>::byte_size()],
            );
            match decoded_vertex_opt {
                None => {
                    // Failed to decode vertex
                    return None;
                }
                Some(decoded_vertex) => {
                    // Add vertex to index map for quick lookups
                    vertex_index.insert(decoded_vertex.vertex_id, vertex_idx);

                    // Decode the vertex's offset
                    decode_offset += Vertex::<T>::byte_size();
                    let offset_type_size = O::byte_size();
                    let mut offset_bytes = Vec::new();
                    offset_bytes.resize(O::byte_size(), 0u8);
                    offset_bytes
                        .copy_from_slice(&bytes[decode_offset..decode_offset + offset_type_size]);
                    let decoded_vertex_offset = O::from_bytes(&offset_bytes).unwrap();
                    vertex_list.push((decoded_vertex, decoded_vertex_offset));
                    decode_offset += O::byte_size();
                }
            }
        }

        // Decode the neighbor list until we reach the end of the byte array
        let mut neighbor_list = Vec::<Vertex<T>>::new();
        loop {
            // Check if we've reached the end of the byte array
            if decode_offset >= bytes.len() {
                break;
            }

            // Check if there are enough bytes left to decode a vertex
            let decoded_end = decode_offset + Vertex::<T>::byte_size();
            if decoded_end > bytes.len() {
                // Partial data encountered - cannot decode complete vertex
                return None;
            }

            // Decode the neighbor vertex
            let neighbor_opt = Vertex::<T>::from_bytes_topology(&bytes[decode_offset..decoded_end]);
            match neighbor_opt {
                None => {
                    // Failed to decode neighbor
                    return None;
                }
                Some(neighbor) => {
                    neighbor_list.push(neighbor);
                    decode_offset += Vertex::<T>::byte_size();
                }
            }
        }

        // Construct and return the complete CSRCommBlock
        Some(CSRCommBlock {
            vertex_count,
            vertex_list,
            neighbor_list,
            vertex_index,
        })
    }
}

impl<T, L, O> GraphQuery<T, Vertex<T>> for CSRCommBlock<T, L, O>
where
    T: VertexId,     // Type T must implement the VertexId trait
    L: VertexLength, // Type L must implement the VertexLength trait
    O: Offset,       // Type O must implement the Offset trait
{
    /// Retrieves all neighbors of the specified vertex.
    /// Returns an empty vector if the vertex doesn't exist or is marked as tombstone.
    fn read_neighbor(&self, vertex_id: &T) -> Vec<Vertex<T>> {
        // Step 1: Locate the vertex in the index
        let vertex_idx_opt = self.vertex_index.get(vertex_id);

        // Check if the vertex exists in this block
        let vertex_list_idx = match vertex_idx_opt {
            None => {
                // Vertex not found in the index
                // For performance reasons, we don't do a full scan and just return empty
                return vec![];
            }
            Some(vertex_idx) => *vertex_idx,
        };

        // If the vertex is marked as deleted (tomb), return empty
        if self.vertex_list[vertex_list_idx].0.tomb == 1 {
            return vec![];
        }

        // Step 2: Determine the range of neighbors in neighbor_list using offsets
        // Get the start offset for this vertex's neighbors
        let neighbors_start = match self.vertex_list[vertex_list_idx].1.try_into() {
            Ok(offset_usize) => offset_usize,
            Err(_) => {
                return vec![];
            }
        };

        // Convert vertex_count to usize for comparison
        let vertex_count_usize = match self.vertex_count.try_into() {
            Ok(count) => count,
            Err(_) => {
                return vec![];
            }
        };

        // Determine the end offset:
        // - For the last vertex, it's the end of neighbor_list
        // - For other vertices, it's the start offset of the next vertex
        let neighbors_end = if vertex_list_idx + 1 == vertex_count_usize {
            self.neighbor_list.len()
        } else {
            match self.vertex_list[vertex_list_idx + 1].1.try_into() {
                Ok(offset_usize) => offset_usize,
                Err(_) => {
                    return vec![];
                }
            }
        };

        // Step 3: Extract the neighbors and filter out tombstone vertices
        self.neighbor_list[neighbors_start..neighbors_end]
            .to_vec()
            .into_iter()
            .filter(|vertex| vertex.tomb == 0) // Keep only non-tombstone neighbors
            .collect::<Vec<_>>()
    }

    /// Checks if a vertex exists in the graph and is not marked as deleted.
    /// Returns true if the vertex exists and is active, false otherwise.
    fn has_vertex(&self, vertex_id: &T) -> bool {
        // Check if the vertex exists in the index
        if !self.vertex_index.contains_key(vertex_id) {
            return false;
        }

        // Check if the vertex is marked as deleted (tomb)
        let vertex_array_idx = self.vertex_index.get(vertex_id).unwrap();
        let vertex = self.vertex_list[*vertex_array_idx].0;
        if vertex.tomb != 0 {
            return false;
        }

        true
    }

    /// Checks if there is an active edge from src_id to dst_id.
    /// Returns true if the edge exists and is active, false otherwise.
    fn has_edge(&self, src_id: &T, dst_id: &T) -> bool {
        self.read_neighbor(src_id)
            .iter()
            .any(|vertex| vertex.vertex_id == *dst_id && vertex.tomb == 0)
    }

    /// Returns a list of all vertices in the graph.
    fn vertex_list(&self) -> Vec<Vertex<T>> {
        self.vertex_list
            .iter()
            .map(|vertex| vertex.0.clone())
            .collect::<Vec<_>>()
    }

    /// Returns a complete representation of the graph as a map.
    /// Each entry maps a vertex ID to its vertex object and list of neighbors.
    fn all(&self) -> BTreeMap<T, (Vertex<T>, Vec<Vertex<T>>)> {
        // Initialize the result map
        let mut graph_map = BTreeMap::<T, (Vertex<T>, Vec<Vertex<T>>)>::new();

        // Process each vertex in the index
        for (vertex_id, vertex_array_idx) in self.vertex_index.iter() {
            // Step 1: Get the vertex object
            let vertex = self.vertex_list[*vertex_array_idx].0.clone();

            // Step 2: Determine the range of neighbors in neighbor_list using offsets
            // Get the start offset for this vertex's neighbors
            let neighbors_start = match self.vertex_list[*vertex_array_idx].1.try_into() {
                Ok(offset_usize) => offset_usize,
                Err(_) => {
                    panic!("Cast Error.")
                }
            };

            // Convert vertex_count to usize for comparison
            let vertex_count_usize = match self.vertex_count.try_into() {
                Ok(count) => count,
                Err(_) => panic!("Usize cast error."),
            };

            // Determine the end offset
            let neighbors_end = if *vertex_array_idx + 1 == vertex_count_usize {
                self.neighbor_list.len()
            } else {
                match self.vertex_list[*vertex_array_idx + 1].1.try_into() {
                    Ok(offset_usize) => offset_usize,
                    Err(_) => {
                        panic!("Cast Error.");
                    }
                }
            };

            // Step 3: Extract all neighbors for this vertex
            let neighbor_list = self.neighbor_list[neighbors_start..neighbors_end].to_vec();

            // Add this vertex and its neighbors to the map
            graph_map.insert(*vertex_id, (vertex, neighbor_list));
        }

        graph_map
    }
}
