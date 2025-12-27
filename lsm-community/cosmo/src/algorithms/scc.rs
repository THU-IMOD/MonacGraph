use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::ThreadPoolBuilder;

use crate::comm_io::comm_idx::BoundaryGraph;
use crate::comm_io::sim_csr_block::CSRSimpleCommBlock;
use crate::comm_io::{CommunityItemRef, CommunityStorage};
use crate::types::graph_query::GraphQuery;
use crate::types::CSRGraph;

/// Configuration for Strongly Connected Components (SCC) algorithm
///
/// Holds parameters that control the execution of the SCC algorithm.
///
/// # Fields
///
/// * `thread_num` - Number of threads to use for parallel computation
#[derive(Clone, Default)]
pub struct SCCConfig {
    pub thread_num: usize,
}

/// Trait for computing Strongly Connected Components (SCC) in a graph
///
/// Implementors of this trait provide functionality to find all strongly connected
/// components within a graph structure. A strongly connected component is a subgraph
/// where every vertex is reachable from every other vertex.
///
/// # Type Parameters
///
/// * `T` - The type of node identifiers in the graph
///
/// # Methods
///
/// * `scc` - Computes all strongly connected components using the provided configuration
pub trait SCC<T> {
    /// Computes strongly connected components in the graph
    ///
    /// # Arguments
    ///
    /// * `wcc_config` - Configuration options for the SCC algorithm, including thread count
    ///
    /// # Returns
    ///
    /// * `Vec<Vec<T>>` - A vector of strongly connected components, where each component
    ///                   is represented as a vector of node identifiers
    fn scc(&self, wcc_config: SCCConfig) -> Vec<Vec<T>>;
}

/// Controller for Strongly Connected Components (SCC) algorithm operations
///
/// This struct serves as a controller for SCC-related operations, providing
/// a centralized way to manage and execute the SCC algorithm while maintaining
/// access to the community storage.
///
/// # Fields
///
/// * `storage_engine` - Thread-safe reference (Arc) to the community storage
///                      that contains the graph data to be analyzed
pub struct SCCController {
    storage_engine: Arc<CommunityStorage>,
}

#[allow(dead_code)]
impl CSRSimpleCommBlock {
    /// Computes the Strongly Connected Components (SCCs) within a community using Tarjan's algorithm
    ///
    /// This method identifies all strongly connected components in the graph represented by the
    /// CSR (Compressed Sparse Row) block structure. A strongly connected component is a maximal
    /// subgraph where every vertex is reachable from every other vertex.
    ///
    /// # Algorithm
    ///
    /// Implements Tarjan's algorithm with the following steps:
    /// 1. Performs depth-first search traversal of the graph
    /// 2. Assigns each vertex an index and keeps track of the lowest reachable vertex
    /// 3. Maintains a stack of visited vertices
    /// 4. Identifies components when a root node of a component is found
    ///
    /// # Returns
    ///
    /// * `Vec<Vec<u64>>` - A vector of strongly connected components, where each component
    ///                    is represented as a vector of vertex IDs
    ///
    /// # Time Complexity
    ///
    /// * O(V + E) where V is the number of vertices and E is the number of edges
    ///
    /// # Space Complexity
    ///
    /// * O(V) for storing indices, low links, stack, and components
    pub fn scc(&self) -> Vec<Vec<u64>> {
        // Initialize data structures for Tarjan's algorithm
        let mut index = 0; // Global index counter for DFS
        let mut indices: HashMap<u64, usize> = HashMap::new(); // Maps vertex ID to its discovery time
        let mut low_links: HashMap<u64, usize> = HashMap::new(); // Maps vertex ID to the lowest ancestor reachable
        let mut on_stack: HashSet<u64> = HashSet::new(); // Tracks vertices currently on the stack
        let mut stack: Vec<u64> = Vec::new(); // Maintains vertices in DFS order
        let mut components: Vec<Vec<u64>> = Vec::new(); // Stores the resulting SCCs

        /// Recursive DFS function that identifies strongly connected components
        ///
        /// # Arguments
        ///
        /// * `vertex_id` - The current vertex being processed
        /// * `index` - Global index counter for numbering vertices in DFS order
        /// * `indices` - Maps vertex ID to its DFS discovery index
        /// * `low_links` - Maps vertex ID to lowest indexed vertex reachable
        /// * `on_stack` - Tracks which vertices are currently on the stack
        /// * `stack` - Stack of vertices in DFS visitation order
        /// * `components` - Collection of identified SCCs
        /// * `csr_block` - Reference to the CSR graph structure
        fn strong_connect(
            vertex_id: u64,
            index: &mut usize,
            indices: &mut HashMap<u64, usize>,
            low_links: &mut HashMap<u64, usize>,
            on_stack: &mut HashSet<u64>,
            stack: &mut Vec<u64>,
            components: &mut Vec<Vec<u64>>,
            csr_block: &CSRSimpleCommBlock,
        ) {
            // Set the depth index for vertex_id and initialize low-link value
            indices.insert(vertex_id, *index);
            low_links.insert(vertex_id, *index);
            *index += 1;

            // Add vertex to stack and mark as on stack
            stack.push(vertex_id);
            on_stack.insert(vertex_id);

            // Get the position in vertex_list to access neighbors
            if let Some(&vertex_pos) = csr_block.vertex_index.get(&vertex_id) {
                let (_, offset) = csr_block.vertex_list[vertex_pos];

                // Determine the range of neighbors by finding the next vertex's offset or end of list
                let next_offset = if vertex_pos + 1 < csr_block.vertex_list.len() {
                    csr_block.vertex_list[vertex_pos + 1].1 // Next vertex offset
                } else {
                    csr_block.neighbor_list.len() as u64 // End of neighbor list
                };

                // Process all adjacent neighbors
                for neighbor_idx in offset..next_offset {
                    let neighbor_id = csr_block.neighbor_list[neighbor_idx as usize];

                    // Only consider neighbors that are part of this community (exist in vertex_index)
                    if csr_block.vertex_index.contains_key(&neighbor_id) {
                        if !indices.contains_key(&neighbor_id) {
                            // Case 1: Neighbor has not yet been visited; perform recursive DFS
                            strong_connect(
                                neighbor_id,
                                index,
                                indices,
                                low_links,
                                on_stack,
                                stack,
                                components,
                                csr_block,
                            );

                            // Update low-link value - check if the subtree rooted at neighbor
                            // has a connection to an ancestor of current vertex
                            if let (Some(&neighbor_low), Some(&vertex_low)) =
                                (low_links.get(&neighbor_id), low_links.get(&vertex_id))
                            {
                                low_links
                                    .insert(vertex_id, std::cmp::min(vertex_low, neighbor_low));
                            }
                        } else if on_stack.contains(&neighbor_id) {
                            // Case 2: Neighbor is on stack, so it's a back-edge in the DFS tree
                            // Update low-link value to consider this back-edge
                            if let (Some(&neighbor_idx), Some(&vertex_low)) =
                                (indices.get(&neighbor_id), low_links.get(&vertex_id))
                            {
                                low_links
                                    .insert(vertex_id, std::cmp::min(vertex_low, neighbor_idx));
                            }
                        }
                        // Case 3 (implicit): Neighbor has been visited but is not on stack,
                        // so it belongs to a different SCC - nothing to do
                    }
                }

                // Check if this vertex is the root of an SCC
                if let (Some(&vertex_idx), Some(&vertex_low)) =
                    (indices.get(&vertex_id), low_links.get(&vertex_id))
                {
                    if vertex_idx == vertex_low {
                        // Found a root node of an SCC - collect all vertices in this component
                        let mut component = Vec::new();

                        // Pop vertices from stack until we reach the current vertex (root of SCC)
                        loop {
                            let w = stack.pop().unwrap(); // Pop from stack
                            on_stack.remove(&w); // Mark as no longer on stack
                            component.push(w); // Add to current component

                            if w == vertex_id {
                                // If reached current vertex, SCC is complete
                                break;
                            }
                        }

                        // Store the identified component in the results
                        components.push(component);
                    }
                }
            }
        }

        // Main loop: Process all vertices in this community to find all SCCs
        // This ensures we handle disconnected components in the graph
        for (vertex, _) in &self.vertex_list {
            let vertex_id = *vertex;

            // Only process unvisited vertices
            if !indices.contains_key(&vertex_id) {
                strong_connect(
                    vertex_id,
                    &mut index,
                    &mut indices,
                    &mut low_links,
                    &mut on_stack,
                    &mut stack,
                    &mut components,
                    self,
                );
            }
        }

        // Return all identified strongly connected components
        components
    }
}

#[allow(dead_code)]
impl CSRGraph<u64, u64, u64> {
    /// Computes strongly connected components (SCCs) using Tarjan's algorithm.
    ///
    /// This implementation uses a non-recursive depth-first search approach to avoid
    /// stack overflow issues on large graphs. The algorithm identifies all strongly
    /// connected components in the directed graph represented by this CSR structure.
    ///
    /// # Algorithm Overview
    /// Tarjan's algorithm is a linear-time algorithm for finding strongly connected
    /// components in a directed graph. It performs a single depth-first search and
    /// uses a stack to keep track of the current path. When a vertex is found to be
    /// the root of an SCC (i.e., its index equals its low-link value), all vertices
    /// in the SCC are popped from the stack.
    ///
    /// # Time Complexity
    /// O(V + E) where V is the number of vertices and E is the number of edges.
    ///
    /// # Space Complexity
    /// O(V) for the auxiliary data structures (indices, low_links, stack, etc.).
    ///
    /// # Returns
    /// A vector of vectors, where each inner vector contains the vertex IDs that
    /// form a strongly connected component. The vertex IDs are the original global
    /// identifiers from the graph, not the internal CSR indices.
    ///
    ///
    /// # Implementation Details
    /// - Uses explicit DFS stack to avoid recursion and potential stack overflow
    /// - Leverages the CSR graph's `read_neighbor` method for safe adjacency access
    /// - Handles disconnected components by iterating through all unvisited vertices
    /// - Maps between global vertex IDs and internal CSR indices for efficiency
    pub fn tarjan(&self) -> Vec<Vec<u64>> {
        // Return early if the graph has no vertices
        if self.vertex_count == 0 {
            return Vec::new();
        }

        let n = self.vertex_count as usize;

        // Get all vertex IDs from community_index keys and sort them
        let mut vertex_ids: Vec<u64> = self.community_index.keys().cloned().collect();
        vertex_ids.sort_unstable();

        // Build mapping from global ID to CSR index for O(1) lookup
        let global_to_csr: HashMap<u64, usize> = vertex_ids
            .iter()
            .enumerate()
            .map(|(idx, &id)| (id, idx))
            .collect();

        println!("CSR Graph Tarjan: {} vertices.", n);

        // ==================================================================================
        // Run Tarjan's algorithm with non-recursive DFS
        // ==================================================================================

        // Algorithm state using arrays instead of HashMaps for better performance
        let mut index = 0; // Global index counter
        let mut indices = vec![usize::MAX; n]; // Discovery time for each vertex
        let mut low_links = vec![usize::MAX; n]; // Lowest reachable vertex from subtree
        let mut on_stack = vec![false; n]; // Track vertices currently on DFS stack
        let mut stack = Vec::new(); // Stack for current path
        let mut components = Vec::new(); // Result: collected SCCs

        // Define DFS state for non-recursive implementation
        #[derive(Clone, Copy)]
        enum State {
            Start,                  // Initialize vertex processing
            ProcessNeighbor(usize), // Process neighbor at given index
            Finish,                 // Finalize vertex processing
        }

        // Process all vertices (handles disconnected components)
        for start_idx in 0..n {
            if indices[start_idx] != usize::MAX {
                continue; // Skip already visited vertices
            }

            // Non-recursive DFS using explicit stack to avoid stack overflow
            let mut dfs_stack = Vec::new();
            dfs_stack.push((start_idx, State::Start));

            while let Some((v_idx, state)) = dfs_stack.pop() {
                match state {
                    State::Start => {
                        // Initialize vertex for Tarjan's algorithm
                        indices[v_idx] = index;
                        low_links[v_idx] = index;
                        index += 1;

                        // Add to DFS path stack
                        stack.push(v_idx);
                        on_stack[v_idx] = true;

                        // Begin processing neighbors from index 0
                        dfs_stack.push((v_idx, State::ProcessNeighbor(0)));
                    }

                    State::ProcessNeighbor(neighbor_pos) => {
                        // Get the global vertex ID for current vertex
                        let vertex_global_id = vertex_ids[v_idx];

                        // Use read_neighbor function to get all neighbors
                        let neighbors = self.read_neighbor(&vertex_global_id);

                        if neighbor_pos < neighbors.len() {
                            let neighbor_global_id = neighbors[neighbor_pos];

                            // Convert neighbor global ID to CSR index
                            if let Some(&n_idx) = global_to_csr.get(&neighbor_global_id) {
                                // Schedule processing of next neighbor
                                dfs_stack.push((v_idx, State::ProcessNeighbor(neighbor_pos + 1)));

                                if indices[n_idx] == usize::MAX {
                                    // Unvisited neighbor: perform DFS recursion
                                    dfs_stack.push((n_idx, State::Start));
                                } else if on_stack[n_idx] {
                                    // Back edge found: update low_link value
                                    low_links[v_idx] = low_links[v_idx].min(indices[n_idx]);
                                }
                                // Forward/cross edges are ignored in Tarjan's algorithm
                            } else {
                                // Neighbor not found in mapping, skip to next neighbor
                                dfs_stack.push((v_idx, State::ProcessNeighbor(neighbor_pos + 1)));
                            }
                        } else {
                            // All neighbors processed: move to finalization
                            dfs_stack.push((v_idx, State::Finish));
                        }
                    }

                    State::Finish => {
                        // Get the global vertex ID for current vertex
                        let vertex_global_id = vertex_ids[v_idx];

                        // Use read_neighbor function to get all neighbors
                        let neighbors = self.read_neighbor(&vertex_global_id);

                        // Update low_link values based on descendants
                        for &neighbor_global_id in &neighbors {
                            if let Some(&n_idx) = global_to_csr.get(&neighbor_global_id) {
                                if on_stack[n_idx] {
                                    low_links[v_idx] = low_links[v_idx].min(low_links[n_idx]);
                                }
                            }
                        }

                        // Check if current vertex is root of an SCC
                        if indices[v_idx] == low_links[v_idx] {
                            // Collect all vertices in this SCC by popping from stack
                            let mut component = Vec::new();

                            loop {
                                let w_idx = stack.pop().unwrap();
                                on_stack[w_idx] = false;
                                component.push(w_idx);

                                // Stop when we reach the SCC root
                                if w_idx == v_idx {
                                    break;
                                }
                            }

                            components.push(component);
                        }
                    }
                }
            }
        }

        // ==================================================================================
        // Convert internal CSR indices back to global vertex IDs
        // ==================================================================================
        components
            .into_iter()
            .map(|component| {
                component
                    .into_iter()
                    .map(|idx| vertex_ids[idx]) // Map CSR index back to global ID
                    .collect()
            })
            .collect()
    }
}

#[allow(dead_code)]
impl BoundaryGraph {
    /// Finds strongly connected components (SCCs) in the boundary graph using an optimized
    /// implementation of Tarjan's algorithm with CSR (Compressed Sparse Row) format.
    ///
    /// This function performs three main steps:
    /// 1. Converts the adjacency map to CSR format for better cache locality
    /// 2. Runs a non-recursive version of Tarjan's algorithm on the CSR structure
    /// 3. Converts internal vertex IDs back to global IDs
    ///
    /// # Returns
    /// A vector of vectors, where each inner vector represents one strongly connected component
    /// containing the global vertex IDs.
    ///
    /// # Performance Notes
    /// - Uses CSR format to improve memory access patterns
    /// - Uses arrays instead of HashMaps for algorithm state to boost performance
    /// - Implements non-recursive DFS to avoid stack overflow on large graphs
    pub fn boundary_scc_opt(&self) -> Vec<Vec<u64>> {
        // Return early if the graph has no vertices
        if self.boundary_csr.boundary_count == 0 {
            return Vec::new();
        }

        let n = self.boundary_csr.boundary_count as usize;

        // ==================================================================================
        // Step 1: Run Tarjan's algorithm on CSR structure with non-recursive DFS
        // ==================================================================================

        // Algorithm state using arrays instead of HashMaps for better performance
        let mut index = 0; // Global index counter
        let mut indices = vec![usize::MAX; n]; // Discovery time for each vertex
        let mut low_links = vec![usize::MAX; n]; // Lowest reachable vertex from subtree
        let mut on_stack = vec![false; n]; // Track vertices currently on DFS stack
        let mut stack = Vec::new(); // Stack for current path
        let mut components = Vec::new(); // Result: collected SCCs

        // Define DFS state for non-recursive implementation
        #[derive(Clone, Copy)]
        enum State {
            Start,                  // Initialize vertex processing
            ProcessNeighbor(usize), // Process neighbor at given index
            Finish,                 // Finalize vertex processing
        }

        // Process all vertices (handles disconnected components)
        for start_idx in 0..n {
            if indices[start_idx] != usize::MAX {
                continue; // Skip already visited vertices
            }

            // Non-recursive DFS using explicit stack to avoid stack overflow
            let mut dfs_stack = Vec::new();
            dfs_stack.push((start_idx, State::Start));

            while let Some((v_idx, state)) = dfs_stack.pop() {
                match state {
                    State::Start => {
                        // Initialize vertex for Tarjan's algorithm
                        indices[v_idx] = index;
                        low_links[v_idx] = index;
                        index += 1;

                        // Add to DFS path stack
                        stack.push(v_idx);
                        on_stack[v_idx] = true;

                        // Begin processing neighbors from index 0
                        dfs_stack.push((v_idx, State::ProcessNeighbor(0)));
                    }

                    State::ProcessNeighbor(neighbor_pos) => {
                        // Get adjacency list bounds for current vertex from CSR
                        let start = self.boundary_csr.offset_list[v_idx] as usize;
                        let end = self.boundary_csr.offset_list[v_idx + 1] as usize;
                        let degree = end - start;

                        if neighbor_pos < degree {
                            let n_idx = self.boundary_csr.edge_list[start + neighbor_pos] as usize;

                            // Schedule processing of next neighbor
                            dfs_stack.push((v_idx, State::ProcessNeighbor(neighbor_pos + 1)));

                            if indices[n_idx] == usize::MAX {
                                // Unvisited neighbor: perform DFS recursion
                                dfs_stack.push((n_idx, State::Start));
                            } else if on_stack[n_idx] {
                                // Back edge found: update low_link value
                                low_links[v_idx] = low_links[v_idx].min(indices[n_idx]);
                            }
                            // Forward/cross edges are ignored in Tarjan's algorithm
                        } else {
                            // All neighbors processed: move to finalization
                            dfs_stack.push((v_idx, State::Finish));
                        }
                    }

                    State::Finish => {
                        // Update low_link values based on descendants
                        let start = self.boundary_csr.offset_list[v_idx] as usize;
                        let end = self.boundary_csr.offset_list[v_idx + 1] as usize;

                        for i in start..end {
                            let n_idx = self.boundary_csr.edge_list[i] as usize;
                            if on_stack[n_idx] {
                                low_links[v_idx] = low_links[v_idx].min(low_links[n_idx]);
                            }
                        }

                        // Check if current vertex is root of an SCC
                        if indices[v_idx] == low_links[v_idx] {
                            // Collect all vertices in this SCC by popping from stack
                            let mut component = Vec::new();

                            loop {
                                let w_idx = stack.pop().unwrap();
                                on_stack[w_idx] = false;
                                component.push(w_idx);

                                // Stop when we reach the SCC root
                                if w_idx == v_idx {
                                    break;
                                }
                            }

                            components.push(component);
                        }
                    }
                }
            }
        }

        // ==================================================================================
        // Step 2: Convert internal CSR indices back to global vertex IDs
        // ==================================================================================
        components
            .into_iter()
            .map(|component| {
                component
                    .into_iter()
                    .map(|idx| self.boundary_csr.vertex_mapper[idx]) // Map CSR index back to global ID
                    .collect()
            })
            .collect()
    }

    /// Computes the Strongly Connected Components (SCCs) in the boundary graph
    /// using a non-recursive implementation of Tarjan's algorithm.
    ///
    /// This algorithm identifies groups of vertices where each vertex is reachable
    /// from any other vertex in the same group. It's particularly useful for analyzing
    /// connectivity patterns in directed graphs.
    ///
    /// # Returns
    ///
    /// A vector of SCCs, where each SCC is represented as a vector of node IDs (u64).
    /// Only SCCs containing at least one node are included in the result.
    ///
    /// # Algorithm Details
    ///
    /// This implementation uses a non-recursive version of Tarjan's algorithm to avoid
    /// stack overflows when processing large graphs. It maintains an explicit stack and
    /// state machine to track the algorithm's progress.
    ///
    /// The algorithm uses the following key data structures:
    /// - indices: Maps each vertex to its discovery time (index)
    /// - low_links: Tracks the lowest index reachable from each vertex
    /// - stack: Maintains vertices that may form an SCC
    /// - on_stack: Efficiently tracks which vertices are currently on the stack
    pub fn boundary_scc(&self) -> Vec<Vec<u64>> {
        // Return early if the graph has no vertices
        if self.boundary_adj_map.is_empty() {
            return Vec::new();
        }

        // Initialize data structures for Tarjan's algorithm
        let mut index = 0;
        let mut indices: HashMap<u64, usize> = HashMap::new(); // Maps vertex to its discovery time
        let mut low_links: HashMap<u64, usize> = HashMap::new(); // Lowest index reachable from vertex
        let mut on_stack: HashSet<u64> = HashSet::new(); // Tracks vertices on the stack
        let mut stack: Vec<u64> = Vec::new(); // Stack of vertices being processed
        let mut components: Vec<Vec<u64>> = Vec::new(); // Collection of found SCCs

        // Define states for the non-recursive implementation
        #[derive(Clone, Copy)]
        enum State {
            Start,     // Initial state when visiting a vertex
            Neighbors, // State for processing neighbors of a vertex
            Finish,    // Final state for completing vertex processing
        }

        // Process each vertex in the graph that hasn't been visited yet
        for &vertex_id in self.boundary_adj_map.keys() {
            if indices.contains_key(&vertex_id) {
                continue; // Skip already visited vertices
            }

            // Initialize stack for depth-first search traversal
            // Each entry contains: (vertex_id, current_state, neighbor_index)
            let mut dfs_stack = Vec::new();
            dfs_stack.push((vertex_id, State::Start, 0));

            // Process vertices until the DFS stack is empty
            while let Some((current_id, state, neighbor_idx)) = dfs_stack.pop() {
                match state {
                    State::Start => {
                        // Initialize vertex with a unique index
                        indices.insert(current_id, index);
                        low_links.insert(current_id, index);
                        index += 1;

                        // Add vertex to stack and mark it as on-stack
                        stack.push(current_id);
                        on_stack.insert(current_id);

                        // Move to processing neighbors state
                        dfs_stack.push((current_id, State::Neighbors, 0));
                    }

                    State::Neighbors => {
                        // Get all neighbors of the current vertex
                        let neighbors = self
                            .boundary_adj_map
                            .get(&current_id)
                            .map(|s| s.iter().cloned().collect::<Vec<_>>())
                            .unwrap_or_else(Vec::new);

                        if neighbor_idx >= neighbors.len() {
                            // All neighbors processed, move to finish state
                            dfs_stack.push((current_id, State::Finish, 0));
                        } else {
                            let neighbor_id = neighbors[neighbor_idx];

                            // Schedule processing of next neighbor
                            dfs_stack.push((current_id, State::Neighbors, neighbor_idx + 1));

                            if !indices.contains_key(&neighbor_id) {
                                // Neighbor not visited yet, process it first
                                dfs_stack.push((neighbor_id, State::Start, 0));
                            } else if on_stack.contains(&neighbor_id) {
                                // Update low_link value if neighbor is on the stack
                                // (This indicates a back-edge in the DFS tree)
                                if let (Some(&neighbor_idx), Some(&vertex_low)) =
                                    (indices.get(&neighbor_id), low_links.get(&current_id))
                                {
                                    low_links.insert(
                                        current_id,
                                        std::cmp::min(vertex_low, neighbor_idx),
                                    );
                                }
                            }
                        }
                    }

                    State::Finish => {
                        // Update low_link values based on fully processed neighbors
                        if let Some(neighbors) = self.boundary_adj_map.get(&current_id) {
                            for &neighbor_id in neighbors {
                                if on_stack.contains(&neighbor_id) {
                                    if let (Some(&neighbor_low), Some(&vertex_low)) =
                                        (low_links.get(&neighbor_id), low_links.get(&current_id))
                                    {
                                        low_links.insert(
                                            current_id,
                                            std::cmp::min(vertex_low, neighbor_low),
                                        );
                                    }
                                }
                            }
                        }

                        // Check if current vertex is the root of an SCC
                        if let (Some(&vertex_idx), Some(&vertex_low)) =
                            (indices.get(&current_id), low_links.get(&current_id))
                        {
                            if vertex_idx == vertex_low {
                                // Current vertex is the root of an SCC
                                // Collect all vertices in this SCC
                                let mut temp_component = Vec::new();

                                // Pop vertices from stack until the current vertex is reached
                                loop {
                                    let w = stack.pop().unwrap();
                                    on_stack.remove(&w);
                                    temp_component.push(w);

                                    if w == current_id {
                                        break;
                                    }
                                }

                                // Add the identified SCC to the result
                                components.push(temp_component);
                            }
                        }
                    }
                }
            }
        }

        // Return all identified SCCs
        components
    }
}

impl SCCController {
    /// Creates a new instance of the SCC processor.
    ///
    /// This builder function initializes an SCC processor with the provided
    /// community storage. The community storage contains all graph data
    /// including vertices, edges, and community partitioning information
    /// needed for SCC computation.
    ///
    /// # Parameters
    ///
    /// * `storage_engine` - An Arc-wrapped CommunityStorage containing the graph data
    ///   and community information. Using Arc enables thread-safe shared ownership,
    ///   which is essential for parallel processing of communities.
    ///
    /// # Returns
    ///
    /// A new instance of the SCC processor configured with the provided storage.
    pub fn new(storage_engine: Arc<CommunityStorage>) -> Self {
        Self { storage_engine }
    }

    /// Computes all Strongly Connected Components (SCCs) across the entire graph.
    ///
    /// This function performs SCC analysis in three main steps:
    /// 1. Computes SCCs for each community in parallel
    /// 2. Computes SCCs for the boundary graph (connections between communities)
    /// 3. Merges SCCs that are connected through the boundary graph
    ///
    /// This approach leverages the community structure of the graph to parallelize
    /// computation and improve performance.
    ///
    /// # Parameters
    ///
    /// * `scc_config` - Configuration for SCC computation, including thread count
    ///
    /// # Returns
    ///
    /// A vector of SCCs, where each SCC is represented as a vector of node IDs (u64).
    /// Only non-empty SCCs are included in the result.
    pub fn scc(&self, scc_config: SCCConfig) -> Vec<Vec<u64>> {
        // Step 1: Compute SCCs for each community in parallel
        // Each community's SCCs are computed independently and then collected
        let community_count = self.storage_engine.community_index.community_map.len();

        // Set up thread pool for parallel processing
        let thread_num = scc_config.thread_num;
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_num)
            .build()
            .unwrap();

        // Shared storage for SCCs from all communities, protected by mutex
        let local_scc_list = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));

        // Process all communities in parallel
        pool.install(|| {
            (0..community_count as u32)
                .into_par_iter()
                .for_each(|community_id| {
                    // Load the community data
                    let community_item_ref_opt =
                        self.storage_engine.load_community_ref(&community_id);
                    match community_item_ref_opt {
                        None => {
                            // Skip communities that don't exist
                        }
                        Some(community_item_ref) => {
                            // Compute SCCs based on community type
                            let mut local_scc = match community_item_ref {
                                CommunityItemRef::Normal(community_csr) => {
                                    // For normal communities, compute SCCs on the fly
                                    community_csr.scc()
                                }
                                CommunityItemRef::Giant(giant_community_index) => {
                                    // For giant communities, use precomputed SCCs
                                    giant_community_index.scc_meta.scc_list.clone()
                                }
                            };

                            // Store the computed SCCs in the shared list
                            {
                                let mut local_scc_list_guard = local_scc_list.lock().unwrap();
                                local_scc_list_guard.append(&mut local_scc);
                            }
                        }
                    }
                });
        });

        // Create a mapping from vertices to their SCC IDs
        let mut local_scc_list_guard = local_scc_list.lock().unwrap();
        let mut vertex_scc_map = Vec::<usize>::new();
        vertex_scc_map.resize(self.storage_engine.vertex_count as usize, 0);

        // Assign each vertex to its community-level SCC
        for (scc_id, local_scc) in local_scc_list_guard.iter().enumerate() {
            for v in local_scc {
                vertex_scc_map[*v as usize] = scc_id;
            }
        }

        // Step 2: Compute SCCs for the boundary graph
        // The boundary graph represents connections between communities
        let merged_scc_list = self
            .storage_engine
            .community_index
            .boundary_graph
            .boundary_scc_opt();

        // Step 3: Merge SCCs that are connected through the boundary graph
        // If vertices from different community-level SCCs belong to the same boundary SCC,
        // they should be merged into a single SCC
        for boundary_scc in merged_scc_list {
            if boundary_scc.len() > 1 {
                // Use the first vertex's SCC as the target for merging
                let first_idx = vertex_scc_map[boundary_scc[0] as usize];
                let mut merged_elements = Vec::new();

                // Process all other vertices in this boundary SCC
                for merged_boundary in 1..boundary_scc.len() {
                    let idx = vertex_scc_map[boundary_scc[merged_boundary] as usize];
                    if idx != first_idx {
                        // Take the SCC that needs to be merged (emptying it)
                        let merged_scc = std::mem::take(&mut local_scc_list_guard[idx]);
                        merged_elements.extend(merged_scc);
                    }
                }

                // Add all merged vertices to the target SCC
                local_scc_list_guard[first_idx].extend(merged_elements);
            }
        }

        // Return only non-empty SCCs.
        // SCCs can be empty if they were merged into other SCCs.
        local_scc_list_guard
            .clone()
            .into_iter()
            .filter(|scc| scc.len() > 0)
            .collect::<Vec<_>>()
    }
}
