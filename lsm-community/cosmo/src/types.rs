use crate::config::READ_BUFFER_SIZE;
use crate::types::graph_query::GraphQuery;
use crate::types::graph_serialize::{
    Offset, TopologyDecode, TopologyEncode, VertexId, VertexLength,
};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

pub(crate) mod graph_query;
pub(crate) mod graph_serialize;

// Define the Vertex struct (10 Bytes each, but 12 Bytes when applying size_of).
#[derive(Debug, Clone, Copy)]
pub struct Vertex<T> {
    /// Unique identifier for this vertex in the system, typically u32
    pub vertex_id: T,
    /// Timestamp when the vertex was created, defaults to current time
    pub(crate) timestamp: u64,
    /// Deletion marker: 0 = exists, 1 = deleted, 2 = force deleted
    pub(crate) tomb: u8,
}

#[allow(dead_code)]
impl<T> Vertex<T>
where
    T: VertexId,
{
    /// Returns the size in bytes that this vertex type occupies in memory
    /// Calculated as size of generic type T plus 9 bytes (8 for timestamp, 1 for tomb)
    pub fn byte_size() -> usize {
        T::byte_size() + 9
    }
}

impl<T> TopologyEncode for Vertex<T>
where
    T: VertexId,
{
    /// Serializes the vertex data into a byte vector
    /// Returns a vector containing the binary representation of this vertex
    fn encode_topology(&self) -> Vec<u8> {
        let mut result = Vec::new();
        // Serialize the vertex_id field using its implementation
        result.extend_from_slice(&self.vertex_id.to_bytes());
        // Serialize the timestamp as little-endian bytes
        result.extend_from_slice(&self.timestamp.to_le_bytes());
        // Serialize the tomb field as a single byte
        result.push(self.tomb);
        result
    }
}

impl<T> TopologyDecode for Vertex<T>
where
    T: VertexId,
{
    /// Deserializes vertex data from a byte slice
    /// Returns Some(Vertex) if deserialization succeeds, None otherwise
    fn from_bytes_topology(bytes: &[u8]) -> Option<Self> {
        // Calculate required byte length and verify we have enough data
        let required_size = T::byte_size() + 8 + 1; // vertex_id + timestamp + tomb
        if bytes.len() < required_size {
            return None;
        }

        // Deserialize vertex_id from the first section of bytes
        let vertex_id = T::from_bytes(&bytes[0..T::byte_size()])?;

        // Deserialize timestamp (8 bytes) from the middle section
        let timestamp_start = T::byte_size();
        let timestamp_end = timestamp_start + 8;
        let mut timestamp_bytes = [0u8; 8];
        timestamp_bytes.copy_from_slice(&bytes[timestamp_start..timestamp_end]);
        let timestamp = u64::from_le_bytes(timestamp_bytes);

        // Deserialize tomb (1 byte) from the last section
        let tomb = bytes[timestamp_end];

        // Construct and return the vertex
        Some(Vertex {
            vertex_id,
            timestamp,
            tomb,
        })
    }
}

/// A CSR (Compressed Sparse Row) implementation of a graph, storing complete graph structure.
/// Optimized for high-performance by omitting timestamp and tombstone management.
/// Assumes vertex IDs are continuous integers without gaps.
///
/// # Type Parameters
/// - `T`: Type for vertex IDs (typically u32 or similar)
/// - `L`: Type for vertex count (typically a numeric type)
/// - `O`: Type for offset values in the CSR structure
///
/// # Performance Notes
/// This implementation prioritizes memory efficiency and traversal speed over
/// flexibility for modifications.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CSRGraph<T, L, O> {
    /// Total number of vertices in the graph
    pub vertex_count: L,

    /// Offset array that indicates where each vertex's adjacency list begins
    /// For each vertex i, its neighbors are stored in neighbor_list[offsets[i] to offsets[i+1]]
    pub offsets: Vec<O>,

    /// Flattened adjacency list containing all neighbors of all vertices
    /// Segmented according to the offsets array
    pub neighbor_list: Vec<T>,

    /// Maps vertex IDs to their community assignments
    /// Each vertex belongs to a community identified by u32
    pub community_index: BTreeMap<T, u32>,
}

/// Implementation of the GraphQuery trait for CSRGraph
/// Provides methods to query graph structure efficiently
impl<T, L, O> GraphQuery<T, T> for CSRGraph<T, L, O>
where
    T: VertexId,     // Type representing vertex IDs
    L: VertexLength, // Type representing count of vertices
    O: Offset,       // Type representing offsets in the CSR structure
{
    /// Retrieves all neighbors of a specified vertex
    ///
    /// # Arguments
    /// * `vertex_id` - Reference to the ID of the vertex whose neighbors we want
    ///
    /// # Returns
    /// * `Vec<T>` - A vector containing the IDs of all neighboring vertices
    ///              Returns empty vector if vertex doesn't exist or has no neighbors
    fn read_neighbor(&self, vertex_id: &T) -> Vec<T> {
        // Convert vertex_id to usize for array indexing
        let vertex_id_usize: usize = match TryInto::<usize>::try_into(*vertex_id) {
            Ok(id) => id,
            Err(_) => return vec![], // Return empty vector if conversion fails
        };

        // Convert vertex_count to usize for comparison
        let vertex_count_usize: usize = match TryInto::<usize>::try_into(self.vertex_count) {
            Ok(count) => count,
            Err(_) => return vec![], // Return empty vector if conversion fails
        };

        // Check if vertex_id is within valid range
        if vertex_id_usize >= vertex_count_usize {
            vec![] // Return empty vector for out-of-range vertex IDs
        } else {
            // Get starting position in neighbor_list for this vertex
            let start_offset_usize: usize =
                match TryInto::<usize>::try_into(self.offsets[vertex_id_usize]) {
                    Ok(offset) => offset,
                    Err(_) => return vec![], // Return empty vector if conversion fails
                };

            // Get ending position in neighbor_list for this vertex
            let end_offset_usize: usize = if vertex_id_usize + 1 < self.offsets.len() {
                match TryInto::<usize>::try_into(self.offsets[vertex_id_usize + 1]) {
                    Ok(offset) => offset,
                    Err(_) => return vec![], // Return empty vector if conversion fails
                }
            } else {
                // If this is the last vertex, use the end of neighbor_list
                self.neighbor_list.len()
            };

            // Early return if there are no neighbors (equal offsets)
            if start_offset_usize == end_offset_usize {
                return vec![];
            }

            // Extract and return the slice of neighbors for this vertex
            self.neighbor_list[start_offset_usize..end_offset_usize].to_vec()
        }
    }

    /// Checks if a vertex exists in the graph
    ///
    /// # Arguments
    /// * `vertex_id` - Reference to the ID of the vertex to check
    ///
    /// # Returns
    /// * `bool` - True if the vertex exists, false otherwise
    fn has_vertex(&self, vertex_id: &T) -> bool {
        // Convert vertex_id to usize for array indexing
        let vertex_id_usize: usize = match TryInto::<usize>::try_into(*vertex_id) {
            Ok(id) => id,
            Err(_) => return false, // Return false if conversion fails
        };

        // Convert vertex_count to usize for comparison
        let vertex_count_usize: usize = match TryInto::<usize>::try_into(self.vertex_count) {
            Ok(count) => count,
            Err(_) => return false, // Return false if conversion fails
        };

        // Check if vertex_id is within valid range
        vertex_id_usize < vertex_count_usize // Note: this is the correct logic (< not >=)
    }

    /// Checks if an edge exists between two vertices
    ///
    /// # Arguments
    /// * `src_id` - Reference to the ID of the source vertex
    /// * `dst_id` - Reference to the ID of the destination vertex
    ///
    /// # Returns
    /// * `bool` - True if the edge exists, false otherwise
    fn has_edge(&self, src_id: &T, dst_id: &T) -> bool {
        if self.has_vertex(src_id) {
            // Check if dst_id appears in the neighbor list of src_id
            self.read_neighbor(src_id)
                .iter()
                .any(|&vertex_id| vertex_id == *dst_id)
        } else {
            false // Source vertex doesn't exist, so edge can't exist
        }
    }

    /// Returns a list of all vertex IDs in the graph
    /// This method is marked as "not commonly used" and may be less optimized
    ///
    /// # Returns
    /// * `Vec<T>` - A vector containing all vertex IDs
    fn vertex_list(&self) -> Vec<T> {
        let vertex_count_usize: usize = match TryInto::<usize>::try_into(self.vertex_count) {
            Ok(count) => count,
            Err(_) => return vec![], // Return empty vector if conversion fails
        };

        // Generate sequential vertex IDs from 0 to vertex_count-1
        // Filtering out any ID that can't be converted to type T
        (0..vertex_count_usize)
            .filter_map(|i| T::try_from(i).ok())
            .collect()
    }

    /// Returns a complete representation of the graph as a map of vertices to their neighbors
    ///
    /// This method:
    /// 1. Retrieves all vertices in the graph
    /// 2. For each vertex, obtains its neighbor list
    /// 3. Builds a map that associates each vertex with itself and its neighbors
    ///
    /// # Returns
    /// * `BTreeMap<T, (T, Vec<T>)>` - A map where:
    ///   - The key is the vertex ID
    ///   - The value is a tuple containing:
    ///     - The vertex ID again (for convenience)
    ///     - A vector of all neighboring vertex IDs
    ///
    /// This provides a complete view of the graph structure that can be
    /// easily iterated over or used for graph analysis algorithms.
    fn all(&self) -> BTreeMap<T, (T, Vec<T>)> {
        // Get the list of all vertices in the graph
        let vertex_list = self.vertex_list();

        // Create a new map to store the complete graph information
        let mut all_graph_info = BTreeMap::<T, (T, Vec<T>)>::new();

        // For each vertex, retrieve its neighbors and add to the map
        for vertex in vertex_list.into_iter() {
            let neighbor_list = self.read_neighbor(&vertex);
            all_graph_info.insert(vertex, (vertex, neighbor_list));
        }

        // Return the complete graph structure
        all_graph_info
    }
}

#[allow(dead_code)]
impl CSRGraph<u64, u64, u64> {
    /// Loads a graph from a graph file in a specific format
    ///
    /// # Format
    /// The file should have the following structure:
    /// - First line: Contains metadata with format "? [vertex_count] [edge_count]"
    /// - Vertex lines: Start with "v", followed by vertex ID and optional community ID
    ///   Format: "v [vertex_id] ... [community_id]" (community_id at position 3)
    /// - Edge lines: Start with "e", followed by source and destination vertex IDs
    ///   Format: "e [source_id] [destination_id]"
    ///
    /// # Arguments
    /// * `file_path` - Path to the graph file
    ///
    /// # Returns
    /// * `CSRGraph<u64, u64, u64>` - A CSR representation of the graph
    ///
    /// # Panics
    /// * If the file cannot be opened or read
    /// * If the file format is incorrect
    /// * If parsing of numeric values fails
    pub fn from_graph_file(file_path: &str) -> CSRGraph<u64, u64, u64> {
        // Open the graph file with a buffered reader for efficient reading
        let graph_file = File::open(file_path).unwrap();
        let mut graph_reader = BufReader::with_capacity(READ_BUFFER_SIZE, graph_file);
        let mut first_line = String::new();
        graph_reader.read_line(&mut first_line).unwrap();

        // Parse the first line to extract vertex and edge counts
        let first_line_tokens: Vec<&str> = first_line.split_whitespace().collect();
        assert_eq!(first_line_tokens.len(), 3);
        let vertex_count = first_line_tokens[1].parse::<usize>().unwrap();
        let edge_count = first_line_tokens[2].parse::<usize>().unwrap();

        // Pre-allocate space for the neighbor list and vertex degrees
        let mut neighbor_list = Vec::with_capacity(edge_count);
        let mut degrees = vec![0u64; vertex_count];

        // Map to store community assignments for vertices
        let mut community_index = BTreeMap::<u64, u32>::new();

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new((vertex_count + edge_count) as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("Graph Loading.");

        // Process each line in the file
        for line in graph_reader.lines() {
            if let Ok(line) = line {
                // Split the line into tokens
                let tokens: Vec<&str> = line.split_whitespace().collect();

                if tokens[0] == "v" {
                    // Process vertex line
                    let parsed_vid = tokens[1].parse::<u64>().ok().expect("File format error.");

                    // If community information exists (at index 3), store it
                    if tokens.len() >= 4 {
                        let community_id =
                            tokens[3].parse::<u32>().ok().expect("File format error.");
                        community_index.insert(parsed_vid, community_id);
                    }
                } else if tokens[0] == "e" {
                    // Process edge line
                    let src = tokens[1].parse::<u64>().ok().expect("File format error.");
                    let dst = tokens[2].parse::<u64>().ok().expect("File format error.");

                    // Add destination to neighbor list and increment source vertex degree
                    neighbor_list.push(dst);
                    degrees[src as usize] += 1;
                }
                pb.inc(1);
            }
        }

        // Compute CSR offset array from vertex degrees
        // Each offset indicates where the adjacency list of a vertex begins
        let mut offsets = vec![0u64; vertex_count];
        for v in 0..vertex_count - 1 {
            offsets[v + 1] = offsets[v] + degrees[v];
        }

        // Create and return the CSRGraph instance
        let vertex_count_u64 = vertex_count as u64;
        Self {
            vertex_count: vertex_count_u64,
            offsets,
            neighbor_list,
            community_index,
        }
    }
}

/// A subgraph in-memory representation for graph computing.
/// It will be used in building SCC-DAG (Strongly Connected Components Directed Acyclic Graph).
#[allow(dead_code)]
#[derive(Debug)]
pub struct CSRSubGraph<T, L, O> {
    /// Total number of vertices in the graph
    pub vertex_count: L,

    /// List of vertices with their associated data
    /// Each entry is a tuple containing the vertex ID (T) and its metadata (O)
    pub vertex_list: Vec<(T, O)>,

    /// Flattened adjacency list containing all neighbors of all vertices
    /// Segmented according to the offsets stored in vertex_index
    /// This implements the Compressed Sparse Row (CSR) format for efficient storage
    pub neighbor_list: Vec<T>,

    /// Maps vertex IDs to their offsets in the neighbor_list
    /// Used to quickly locate the neighbors of a specific vertex
    pub vertex_index: HashMap<T, usize>,
}

#[allow(dead_code)]
impl GraphQuery<u64, u64> for CSRSubGraph<u64, u64, u64> {
    /// Retrieves all neighbors (connected vertices) for a given vertex ID
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the graph instance
    /// * `vertex_id` - Reference to the ID of the vertex whose neighbors we want to find
    ///
    /// # Returns
    ///
    /// * `Vec<u64>` - A vector containing the IDs of all neighboring vertices
    ///
    fn read_neighbor(&self, vertex_id: &u64) -> Vec<u64> {
        // Step 1: Locate the vertex in the index to get its position in the vertex list
        let vertex_opt = self.vertex_index.get(vertex_id);

        // Step 2: Check if the vertex exists in this graph
        let vertex_list_idx = match vertex_opt {
            None => {
                // Vertex not found in the index
                // For performance reasons, we don't perform a full scan and simply return an empty vector
                return vec![];
            }
            Some(vertex_idx) => *vertex_idx,
        };

        // Step 3: Determine the range of neighbors in neighbor_list using offsets
        // Get the starting position for this vertex's neighbors in the neighbor list
        let neighbors_start = self.vertex_list[vertex_list_idx].1 as usize;

        // Determine the ending position:
        // - For the last vertex in the list, use the end of neighbor_list
        // - For other vertices, use the start position of the next vertex
        let neighbors_end = if vertex_list_idx + 1 == self.vertex_count as usize {
            self.neighbor_list.len()
        } else {
            self.vertex_list[vertex_list_idx + 1].1 as usize
        };

        // Step 4: Extract the neighbors within the determined range
        // Returns a vector containing all neighboring vertex IDs
        self.neighbor_list[neighbors_start..neighbors_end].to_vec()
    }

    /// Checks whether a vertex with the given ID exists in the graph
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the graph instance
    /// * `vertex_id` - Reference to the vertex ID to check
    ///
    /// # Returns
    ///
    /// * `bool` - Returns true if the vertex exists, false otherwise
    ///
    fn has_vertex(&self, vertex_id: &u64) -> bool {
        // Simply check if the vertex_id exists as a key in the vertex_index hashmap
        // This provides O(1) constant time lookup performance
        self.vertex_index.contains_key(vertex_id)
    }

    /// Checks whether an edge exists between two vertices in the graph
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the graph instance
    /// * `src_id` - Reference to the source vertex ID
    /// * `dst_id` - Reference to the destination vertex ID
    ///
    /// # Returns
    ///
    /// * `bool` - Returns true if an edge exists from src_id to dst_id, false otherwise
    ///
    fn has_edge(&self, src_id: &u64, dst_id: &u64) -> bool {
        // First retrieve all neighbors of the source vertex
        // Then check if the destination vertex ID exists in the neighbor list
        // Using the any() iterator adapter for efficient short-circuit evaluation
        self.read_neighbor(src_id)
            .iter()
            .any(|vertex| *vertex == *dst_id)
    }

    /// Returns a vector containing all vertex IDs in the graph
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the graph instance
    ///
    /// # Returns
    ///
    /// * `Vec<u64>` - A vector containing all vertex IDs in the graph
    ///
    fn vertex_list(&self) -> Vec<u64> {
        // Iterate through the vertex_list, extracting only the vertex IDs (the first element of each tuple)
        // Transform each tuple (vertex_id, offset) into just the vertex_id using map()
        // Finally, collect the results into a new Vec<u64>
        self.vertex_list
            .iter()
            .map(|vertex| vertex.0.clone())
            .collect::<Vec<_>>()
    }

    /// Returns a complete representation of the graph as a BTreeMap
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the graph instance
    ///
    /// # Returns
    ///
    /// * `BTreeMap<u64, (u64, Vec<u64>)>` - A map where:
    ///   - The key is the vertex ID
    ///   - The value is a tuple containing:
    ///     - The vertex ID (duplicated from the key)
    ///     - A vector of all neighboring vertex IDs
    ///
    fn all(&self) -> BTreeMap<u64, (u64, Vec<u64>)> {
        // Initialize an empty BTreeMap to store the complete graph structure
        // Using BTreeMap for ordered traversal based on vertex IDs
        let mut graph_map = BTreeMap::<u64, (u64, Vec<u64>)>::new();

        // Iterate through all vertices in the index
        for (&vertex_id, &vertex_list_idx) in &self.vertex_index {
            // Step 1: Determine the range of neighbors in neighbor_list using offsets
            // Get the start offset for this vertex's neighbors
            let neighbors_start = self.vertex_list[vertex_list_idx].1 as usize;

            // Determine the end offset:
            // - If this is the last vertex, use the end of neighbor_list
            // - Otherwise, use the start offset of the next vertex
            let neighbors_end = if vertex_list_idx + 1 == self.vertex_count as usize {
                self.neighbor_list.len()
            } else {
                self.vertex_list[vertex_list_idx + 1].1 as usize
            };

            // Step 2: Extract all neighbors for this vertex into a new vector
            let neighbor_list = self.neighbor_list[neighbors_start..neighbors_end].to_vec();

            // Step 3: Add this vertex and its neighbors to the map
            // Format: vertex_id => (vertex_id, [neighbor_ids])
            graph_map.insert(vertex_id, (vertex_id, neighbor_list));
        }

        // Return the completed graph map
        graph_map
    }
}

#[allow(dead_code)]
impl CSRGraph<u64, u64, u64> {
    /// Induces a subgraph from the original graph based on a list of vertices.
    ///
    /// This function creates a new subgraph containing only the vertices specified in
    /// the input list and the edges between them from the original graph.
    ///
    /// # Arguments
    ///
    /// * `vertex_list` - A vector of vertex IDs to include in the subgraph
    ///
    /// # Returns
    ///
    /// A `CSRSubGraph` structure representing the induced subgraph in CSR (Compressed Sparse Row) format
    ///
    /// # Implementation Details
    ///
    /// 1. Creates a HashSet from the input vertex list for efficient lookups
    /// 2. Initializes data structures to store the new graph representation:
    ///    - vertex_list: Stores each vertex and its offset in the neighbor list
    ///    - neighbor_list: Stores all neighbors in a flattened array
    ///    - vertex_index: Maps original vertex IDs to their positions in the new graph
    /// 3. For each vertex in the input set:
    ///    - Filters its neighbors to include only those present in the input set
    ///    - Adds the vertex to the vertex list with its current offset
    ///    - Updates the vertex index with the vertex's position
    ///    - Appends the filtered neighbors to the neighbor list
    ///
    /// The resulting subgraph maintains the same connectivity as the original graph
    /// but contains only the specified vertices and the edges between them.
    pub fn induce_subgraph(&self, vertex_list: &Vec<u64>) -> CSRSubGraph<u64, u64, u64> {
        // Create a set from the vertex list for efficient membership testing
        let vertex_set: HashSet<u64> = HashSet::from_iter(vertex_list.iter().cloned());
        let vertex_count = vertex_set.len() as u64;

        // Initialize data structures for the subgraph
        let mut vertex_list = Vec::<(u64, u64)>::new();
        let mut neighbor_list = Vec::<u64>::new();
        let mut vertex_index = HashMap::<u64, usize>::new();
        let mut current_offset = 0u64;

        // Process each vertex to build the subgraph structure
        for (vertex_list_id, vertex) in vertex_set.iter().enumerate() {
            // Get neighbors of the current vertex that are also in the vertex set
            let mut neighbors = self
                .read_neighbor(vertex)
                .into_iter()
                .filter(|n| vertex_set.contains(n))
                .collect::<Vec<_>>();

            // Add vertex with its offset in the neighbor list
            vertex_list.push((*vertex, current_offset));

            // Map the original vertex ID to its position in the new subgraph
            vertex_index.insert(*vertex, vertex_list_id);

            // Update the current offset for the next vertex
            current_offset += neighbors.len() as u64;

            // Add the filtered neighbors to the neighbor list
            neighbor_list.append(&mut neighbors);
        }

        // Construct and return the subgraph
        CSRSubGraph {
            vertex_count,
            vertex_list,
            neighbor_list,
            vertex_index,
        }
    }

    /// Creates a CSR subgraph that contains all vertices from the original graph.
    ///
    /// This function builds a subgraph that is effectively a copy of the original graph
    /// but in CSRSubGraph format. Unlike selective sub-graphs, this includes all vertices.
    ///
    /// # Returns
    /// * `CSRSubGraph<u64, u64, u64>` - A subgraph containing all vertices from the original graph
    pub fn induce_graph(&self) -> CSRSubGraph<u64, u64, u64> {
        // Get the total number of vertices in the original graph
        let vertex_count = self.vertex_count;

        // Create the vertex list by pairing each vertex ID with its offset in the neighbor list
        // This builds the foundation of the CSR representation
        let vertex_list = self
            .vertex_list()
            .iter()
            .cloned()
            .zip(self.offsets.iter().cloned())
            .collect::<Vec<(_, _)>>();

        // Copy the neighbor list directly from the original graph
        // This preserves all edge connections
        let neighbor_list = self.neighbor_list.clone();

        // Build a lookup map from vertex IDs to their positions in the vertex list
        // This enables efficient vertex lookups during graph operations
        let mut vertex_index = HashMap::<u64, usize>::new();
        for (vertex_list_id, (vertex_id, _)) in vertex_list.iter().enumerate() {
            vertex_index.insert(*vertex_id, vertex_list_id);
        }

        // Construct and return the complete subgraph with all components
        CSRSubGraph {
            vertex_count,  // Total number of vertices
            vertex_list,   // List of vertex ID and offset pairs
            neighbor_list, // List of all neighboring vertices (edges)
            vertex_index,  // Lookup map for efficient vertex access
        }
    }
}
