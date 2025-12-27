use std::collections::{BTreeMap, HashMap};

use crate::types::graph_query::GraphQuery;
use crate::types::graph_serialize::{ByteEncodable, TopologyDecode, TopologyEncode};

/// A simple implementation of CSR block, by replacing the V(u64) with u64.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CSRSimpleCommBlock {
    /// Total number of vertices in this CSR block.
    pub vertex_count: u64,

    /// List of vertices with their corresponding offsets.
    /// Each tuple contains a vertex and its offset in the neighbor list.
    pub vertex_list: Vec<(u64, u64)>,

    /// Flattened list of all neighbors for all vertices.
    /// The offsets in vertex_list determine which neighbors belong to which vertex.
    pub neighbor_list: Vec<u64>,

    /// HashMap for efficient vertex lookup.
    /// Maps vertex data to its index in the vertex_list for quick retrieval.
    pub(crate) vertex_index: HashMap<u64, usize>,
}

impl GraphQuery<u64, u64> for CSRSimpleCommBlock {
    /// Retrieves all neighbors of the specified vertex.
    /// Returns an empty vector if the vertex doesn't exist or is marked as tombstone.
    fn read_neighbor(&self, vertex_id: &u64) -> Vec<u64> {
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
            .into_iter() // Keep only non-tombstone neighbors
            .collect::<Vec<_>>()
    }

    /// Checks if a vertex exists in the graph and is not marked as deleted.
    /// Returns true if the vertex exists and is active, false otherwise.
    fn has_vertex(&self, vertex_id: &u64) -> bool {
        // Check if the vertex exists in the index
        self.vertex_index.contains_key(vertex_id)
    }

    /// Checks if there is an active edge from src_id to dst_id.
    /// Returns true if the edge exists and is active, false otherwise.
    fn has_edge(&self, src_id: &u64, dst_id: &u64) -> bool {
        self.read_neighbor(src_id)
            .iter()
            .any(|vertex| *vertex == *dst_id)
    }

    /// Returns a list of all vertices in the graph.
    fn vertex_list(&self) -> Vec<u64> {
        self.vertex_list
            .iter()
            .map(|vertex| vertex.0.clone())
            .collect::<Vec<_>>()
    }

    /// Returns a complete representation of the graph as a map.
    /// Each entry maps a vertex ID to its vertex object and list of neighbors.
    fn all(&self) -> BTreeMap<u64, (u64, Vec<u64>)> {
        // Initialize the result map
        let mut graph_map = BTreeMap::<u64, (u64, Vec<u64>)>::new();

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

#[allow(dead_code)]
impl TopologyDecode for CSRSimpleCommBlock {
    /// Implements the TopologyDecode trait for CSRSimpleCommBlock.
    /// This method deserializes a byte array back into a CSRCommBlock structure.
    /// Returns None if the byte array is invalid or lacks sufficient data.
    fn from_bytes_topology(bytes: &[u8]) -> Option<Self> {
        let mut decode_offset = 0usize;

        // Ensure there are enough bytes to read the vertex count
        if decode_offset + u64::byte_size() > bytes.len() {
            // Insufficient data to decode vertex count
            return None;
        }

        // First decode the total number of vertices
        let vertex_count =
            u64::from_bytes(&bytes[decode_offset..decode_offset + u64::byte_size()]).unwrap();
        decode_offset += u64::byte_size();

        // Initialize containers for decoded data
        let mut vertex_list = Vec::<(u64, u64)>::new();
        let mut vertex_index = HashMap::<u64, usize>::new();

        // Convert vertex_count to usize for iteration
        let vertex_count_usize = match vertex_count.try_into() {
            Ok(count) => count,
            Err(_) => return None,
        };

        // Decode each vertex and its offset
        for vertex_idx in 0..vertex_count_usize {
            // Decode the vertex ID.
            let vertex_id =
                u64::from_bytes(&bytes[decode_offset..decode_offset + u64::byte_size()]).unwrap();
            decode_offset += u64::byte_size();
            // Add vertex to index map for quick lookups
            vertex_index.insert(vertex_id, vertex_idx);

            let offset_type_size = u64::byte_size();
            let mut offset_bytes = Vec::new();
            offset_bytes.resize(u64::byte_size(), 0u8);
            offset_bytes.copy_from_slice(&bytes[decode_offset..decode_offset + offset_type_size]);
            let decoded_vertex_offset = u64::from_bytes(&offset_bytes).unwrap();
            vertex_list.push((vertex_id, decoded_vertex_offset));
            decode_offset += offset_type_size;
        }

        // Decode the neighbor list until we reach the end of the byte array
        let mut neighbor_list = Vec::<u64>::new();
        loop {
            // Check if we've reached the end of the byte array
            if decode_offset >= bytes.len() {
                break;
            }

            // Check if there are enough bytes left to decode a vertex
            let decoded_end = decode_offset + u64::byte_size();
            if decoded_end > bytes.len() {
                // Partial data encountered - cannot decode complete vertex
                return None;
            }

            // Decode the neighbor vertex
            let neighbor_opt = u64::from_bytes(&bytes[decode_offset..decoded_end]);
            match neighbor_opt {
                None => {
                    // Failed to decode neighbor
                    return None;
                }
                Some(neighbor) => {
                    neighbor_list.push(neighbor);
                    decode_offset += u64::byte_size();
                }
            }
        }

        // Construct and return the complete CSRCommBlock
        Some(CSRSimpleCommBlock {
            vertex_count,
            vertex_list,
            neighbor_list,
            vertex_index,
        })
    }
}

#[allow(dead_code)]
impl TopologyEncode for CSRSimpleCommBlock {
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
            encoded_bytes.extend_from_slice(&vertex.to_bytes());
            encoded_bytes.extend_from_slice(&offset.to_bytes());
        }

        // Finally encode all neighbors in the neighbor list
        for neighbor in &self.neighbor_list {
            encoded_bytes.extend_from_slice(&neighbor.to_bytes());
        }
        // The vertex_index HashMap is not encoded as it can be reconstructed from vertex_list

        encoded_bytes
    }
}
