use crate::types::graph_query::GraphQuery;
use crate::types::CSRSubGraph;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// SCCIndex is a data structure used to organize strongly connected components (SCCs) in a graph.
/// It maintains relationships between vertices and their corresponding SCCs, as well as the hierarchical
/// organization of SCCs into levels.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SCCMeta {
    /// A list of SCCs where each SCC is represented as a vector of vertex IDs.
    /// Each inner vector contains all vertices belonging to a single strongly connected component.
    pub scc_list: Vec<Vec<u64>>,

    /// Maps an SCC ID to its level in the hierarchical organization.
    /// This helps in understanding the topological ordering of SCCs.
    pub scc_level: HashMap<u64, u64>,

    /// A list of SCCs organized by levels, where each inner vector contains
    /// all SCC IDs that belong to a specific level in the hierarchy.
    pub level_list: Vec<Vec<u64>>,

    /// Maps a vertex ID to its corresponding SCC ID.
    /// Allows for efficient lookup of which SCC a particular vertex belongs to.
    pub vertex_scc: HashMap<u64, u64>,

    /// Maps a scc ID to its neighbors.
    /// Represents a scc-dag sketch.
    pub scc_dag: HashMap<u64, Vec<u64>>,
}

/// A data structure that stores the offsets for each Strongly Connected Component (SCC) part.
///
/// This struct is a simple wrapper around a vector of 64-bit unsigned integers that
/// represents the starting offsets of each SCC in a graph. These offsets enable efficient
/// navigation and indexing into the graph's SCC structure.
///
/// # Structure
/// * The inner vector contains the offset position of each SCC part
/// * Each offset (u64) indicates where a specific SCC begins
/// * The difference between consecutive offsets determines the size of each SCC
///
/// This indexing approach allows for constant-time access to the beginning of any SCC
/// while maintaining a compact memory representation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SCCIndex(
    /// Vector containing the starting offset of each SCC part
    pub Vec<u64>,
);

#[allow(dead_code)]
impl SCCMeta {
    /// Performs a breadth-first search on the SCC DAG to find all SCCs reachable from a starting vertex.
    ///
    /// # Arguments
    /// * `start_vertex_id` - The ID of the vertex from which to start the search
    ///
    /// # Returns
    /// A vector containing all SCC IDs reachable from the starting vertex's SCC
    pub fn bfs_scc_dag(&self, start_vertex_id: &u64) -> Vec<u64> {
        // Get the SCC ID containing the start vertex
        let src_scc_id = self.vertex_scc.get(start_vertex_id).unwrap();

        // Initialize BFS queue and visited set for tracking SCCs
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();

        // Add the source SCC to both the visited set and queue
        visited.insert(*src_scc_id);
        queue.push_back(*src_scc_id);

        while let Some(current_scc) = queue.pop_front() {
            // If the current SCC is not in the SCC DAG, skip it
            if let Some(neighbors) = self.scc_dag.get(&current_scc) {
                // Process all neighboring SCCs of the current SCC
                for &neighbor in neighbors {
                    // Note: There appears to be a commented line here that was intended to check
                    // if a destination SCC was found (removed or incomplete implementation)

                    // If we haven't visited this neighbor yet, add it to the queue for processing
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // Convert the visited set to a vector and return all reachable SCCs
        visited.into_iter().collect::<Vec<_>>()
    }

    /// Finds a path of SCC IDs from the SCC containing src_id to the SCC containing dst_id
    ///
    /// # Arguments
    /// - `src_id`: The source vertex ID
    /// - `dst_id`: The destination vertex ID
    ///
    /// # Returns
    /// A `Vec<u64>` containing the SCC IDs in the path from source SCC to destination SCC
    pub fn path_scc_dag(&self, src_id: &u64, dst_id: &u64) -> Vec<u64> {
        // First, find which SCCs contain the source and destination vertices
        let src_scc = match self.find_vertex_scc(src_id) {
            Some(scc_id) => scc_id,
            None => return vec![], // Source vertex not found
        };

        let dst_scc = match self.find_vertex_scc(dst_id) {
            Some(scc_id) => scc_id,
            None => return vec![], // Destination vertex not found
        };

        // If source and destination are in the same SCC, return that SCC
        if src_scc == dst_scc {
            return vec![src_scc];
        }

        // Perform BFS on the SCC DAG to find path from src_scc to dst_scc
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut predecessor = HashMap::new();

        // Start BFS from the source SCC
        queue.push_back(src_scc);
        visited.insert(src_scc);

        // Perform BFS
        while let Some(current_scc) = queue.pop_front() {
            // If the destination SCC is found, reconstruct and return the path
            if current_scc == dst_scc {
                return self.reconstruct_scc_path(&predecessor, &src_scc, &dst_scc);
            }

            // Explore neighbor SCCs of the current SCC
            if let Some(neighbors) = self.scc_dag.get(&current_scc) {
                for &neighbor_scc in neighbors {
                    // If the neighbor SCC has not been visited, visit it
                    if !visited.contains(&neighbor_scc) {
                        visited.insert(neighbor_scc);
                        predecessor.insert(neighbor_scc, current_scc); // Track the predecessor
                        queue.push_back(neighbor_scc); // Add to the queue for further exploration
                    }
                }
            }
        }

        // If no path is found, return an empty vector
        vec![]
    }

    /// Reconstructs the SCC path from the source SCC to the destination SCC using the predecessor map.
    ///
    /// This function traces back from the destination SCC to the source SCC using the `predecessor` map,
    /// and then reverses the path to return it in the correct order (from source SCC to destination SCC).
    ///
    /// # Arguments
    /// - `predecessor`: A map containing each SCC's predecessor in the BFS search.
    /// - `src_scc`: The ID of the source SCC.
    /// - `dst_scc`: The ID of the destination SCC.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the SCCs in the path from source SCC to destination SCC, in order.
    fn reconstruct_scc_path(
        &self,
        predecessor: &HashMap<u64, u64>,
        src_scc: &u64,
        dst_scc: &u64,
    ) -> Vec<u64> {
        // Vector to store the reconstructed path
        let mut path = Vec::new();
        // Start from the destination SCC
        let mut current = *dst_scc;

        // Add the destination SCC to the path
        path.push(current);

        // Trace the path back to the source SCC using the predecessor map
        while current != *src_scc {
            match predecessor.get(&current) {
                // If a predecessor is found, move to the predecessor and add to the path
                Some(&pred) => {
                    current = pred;
                    path.push(current);
                }
                // If no predecessor is found (this would mean no path exists), break the loop
                None => break,
            }
        }

        // Reverse the path to get it from source SCC to destination SCC
        path.reverse();
        path
    }

    /// Performs a breadth-first search on the SCC DAG to find all reachable SCCs from a source SCC.
    ///
    /// # Arguments
    /// * `community_id` - The ID of the community being processed
    /// * `src_scc_id` - The source SCC ID from which to start the search
    /// * `global_visited_scc` - A map tracking which SCCs have been visited in each community
    ///
    /// # Returns
    /// A vector containing all SCC IDs reachable from the source SCC
    pub fn bfs_scc_id_dag(
        &self,
        community_id: &u32,
        src_scc_id: &u64,
        global_visited_scc: &mut HashMap<u32, HashSet<u64>>,
    ) -> Vec<u64> {
        /// Checks if a specific SCC has been visited in a given community.
        ///
        /// # Arguments
        /// * `community_id` - The ID of the community to check
        /// * `src_scc_id` - The SCC ID to check for visitation
        /// * `global_visited_scc` - Map of visited SCCs per community
        ///
        /// # Returns
        /// `true` if the SCC has been visited in the specified community, `false` otherwise
        fn is_scc_visited(
            community_id: &u32,
            src_scc_id: &u64,
            global_visited_scc: &mut HashMap<u32, HashSet<u64>>,
        ) -> bool {
            if global_visited_scc.contains_key(community_id) {
                let visited_scc_ref = global_visited_scc.get(community_id).unwrap();
                visited_scc_ref.contains(src_scc_id)
            } else {
                false
            }
        }

        /// Marks an SCC as visited in a specific community.
        ///
        /// # Arguments
        /// * `community_id` - The ID of the community
        /// * `src_scc_id` - The SCC ID to mark as visited
        /// * `global_visited_scc` - Map tracking visited SCCs per community
        fn mark_scc_visit(
            community_id: &u32,
            src_scc_id: &u64,
            global_visited_scc: &mut HashMap<u32, HashSet<u64>>,
        ) {
            if !global_visited_scc.contains_key(community_id) {
                global_visited_scc.insert(*community_id, HashSet::new());
            }
            let visited_scc_ref = global_visited_scc.get_mut(community_id).unwrap();
            visited_scc_ref.insert(*src_scc_id);
        }

        // Initialize BFS queue and visited set
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();

        // Add the source SCC to both the visited set and queue
        visited.insert(*src_scc_id);
        queue.push_back(*src_scc_id);

        while let Some(current_scc) = queue.pop_front() {
            // If the current SCC is not in the SCC DAG, skip it
            if let Some(neighbors) = self.scc_dag.get(&current_scc) {
                // Process all neighbors of the current SCC
                for &neighbor in neighbors {
                    // If we haven't visited this neighbor yet, add it to the queue
                    if !visited.contains(&neighbor)
                        && is_scc_visited(community_id, src_scc_id, global_visited_scc)
                    {
                        // Mark the neighbor as visited in the global tracking structure
                        mark_scc_visit(community_id, src_scc_id, global_visited_scc);
                        // Add to local visited set and queue for further processing
                        visited.insert(neighbor);
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // Convert the visited set to a vector and return it
        visited.into_iter().collect::<Vec<_>>()
    }

    /// Finds the Strongly Connected Component (SCC) ID for a given vertex.
    ///
    /// This function retrieves the SCC ID associated with a vertex from the internal mapping.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - A reference to the unique identifier of the vertex
    ///
    /// # Returns
    ///
    /// * `Some(scc_id)` - The SCC ID if the vertex exists in the graph
    /// * `None` - If the vertex is not found
    pub fn find_vertex_scc(&self, vertex_id: &u64) -> Option<u64> {
        // Look up the SCC ID from the vertex_scc hashmap
        match self.vertex_scc.get(vertex_id) {
            None => None, // Return None if vertex is not found
            Some(&scc_id) => {
                Some(scc_id) // Return the SCC ID if vertex exists
            }
        }
    }

    /// Retrieves all vertices belonging to a specific Strongly Connected Component (SCC).
    ///
    /// This function returns a vector of vertex IDs that are part of the requested SCC.
    ///
    /// # Arguments
    ///
    /// * `scc_id` - A reference to the unique identifier of the SCC
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u64>)` - A vector containing all vertex IDs in the specified SCC
    /// * `None` - If the SCC ID is out of bounds (doesn't exist in the graph)
    pub fn find_scc_vertices(&self, scc_id: &u64) -> Option<Vec<u64>> {
        // Check if the SCC ID is within the valid range
        if *scc_id >= self.scc_list.len() as u64 {
            None // Return None if SCC ID is out of bounds
        } else {
            // Return a clone of the vector containing all vertices in this SCC
            Some(self.scc_list[*scc_id as usize].clone())
        }
    }

    /// Retrieves the level of a specific Strongly Connected Component (SCC).
    ///
    /// This function looks up the hierarchical level assigned to the requested SCC.
    ///
    /// # Arguments
    ///
    /// * `scc_id` - A reference to the unique identifier of the SCC
    ///
    /// # Returns
    ///
    /// * `Some(level_id)` - The level ID if the SCC exists in the graph
    /// * `None` - If the SCC is not found
    pub fn find_scc_level(&self, scc_id: &u64) -> Option<u64> {
        // Look up the level ID from the scc_level hashmap
        match self.scc_level.get(scc_id) {
            None => None, // Return None if SCC is not found
            Some(&level_id) => {
                Some(level_id) // Return the level ID if SCC exists
            }
        }
    }

    /// Retrieves all Strongly Connected Components (SCCs) at a specific level.
    ///
    /// This function returns a vector of SCC IDs that belong to the requested hierarchical level.
    ///
    /// # Arguments
    ///
    /// * `level_id` - A reference to the unique identifier of the level
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u64>)` - A vector containing all SCC IDs at the specified level
    /// * `None` - If the level ID is out of bounds (doesn't exist in the graph)
    pub fn find_level_sccs(&self, level_id: &u64) -> Option<Vec<u64>> {
        // Check if the level ID is within the valid range
        if *level_id >= self.level_list.len() as u64 {
            None // Return None if level ID is out of bounds
        } else {
            // Return a clone of the vector containing all SCCs at this level
            Some(self.level_list[*level_id as usize].clone())
        }
    }

    /// Finds the hierarchical level for a given vertex.
    ///
    /// This function determines the level of a vertex by first finding its SCC,
    /// then looking up the level of that SCC.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - A reference to the unique identifier of the vertex
    ///
    /// # Returns
    ///
    /// * `Some(level_id)` - The level ID if both the vertex and its SCC exist
    /// * `None` - If either the vertex or its SCC is not found
    pub fn find_vertex_level(&self, vertex_id: &u64) -> Option<u64> {
        // First, find the SCC that contains this vertex
        match self.find_vertex_scc(vertex_id) {
            None => {
                None // Return None if vertex is not found
            }
            Some(scc_id) => {
                // Then, find the level of the identified SCC
                self.find_scc_level(&scc_id)
            }
        }
    }

    /// Determines if there is a path from one vertex to another in the graph.
    ///
    /// This function checks reachability by evaluating topological constraints and SCC membership.
    /// It first compares the hierarchical levels of the vertices, then checks if they belong to the
    /// same SCC, and finally delegates to SCC-level reachability if necessary.
    ///
    /// # Arguments
    ///
    /// * `src_id` - A reference to the unique identifier of the source vertex
    /// * `dst_id` - A reference to the unique identifier of the destination vertex
    ///
    /// # Returns
    ///
    /// * `true` - If the destination vertex is reachable from the source vertex
    /// * `false` - If there is no path from the source to the destination
    pub fn is_reachable(&self, src_id: &u64, dst_id: &u64) -> bool {
        // Check if traversal is possible based on topological levels
        // If source vertex is at a higher level than destination, it can never reach it
        if self.find_vertex_level(src_id).unwrap() > self.find_vertex_level(dst_id).unwrap() {
            false
        } else {
            // Get the strongly connected component IDs for both vertices
            let src_scc_id = self.find_vertex_scc(src_id).unwrap();
            let dst_scc_id = self.find_vertex_scc(dst_id).unwrap();

            // If both vertices belong to the same SCC, they are trivially reachable
            if src_scc_id == dst_scc_id {
                true
            } else {
                // Delegate to SCC-level reachability check for vertices in different components
                self.is_scc_reachable(&src_scc_id, &dst_scc_id)
            }
        }
    }

    /// Computes the set of reachable SCCs from a starting SCC with path-based caching optimization.
    ///
    /// This function performs a breadth-first search (BFS) on the SCC DAG to find all SCCs reachable
    /// from the starting SCC, with aggressive caching to avoid redundant computations. The key optimization
    /// is that during BFS traversal, when encountering an SCC that already has cached reachability data,
    /// the function leverages transitivity to update all SCCs along the current path.
    ///
    /// # Arguments
    ///
    /// * `start_scc` - The starting SCC ID to compute reachability from
    /// * `target_sccs` - Set of target SCC IDs we're interested in (used for filtering results)
    /// * `cache` - Thread-safe cache mapping SCC IDs to their complete reachable SCC sets
    ///
    /// # Returns
    ///
    /// A set containing all target SCCs that are reachable from the starting SCC
    ///
    /// # Algorithm
    ///
    /// 1. **Cache Check**: First checks if reachability from start_scc is already cached
    /// 2. **BFS with Path Tracking**: Performs BFS while maintaining the path from start to current node
    /// 3. **Transitive Cache Utilization**: When encountering a cached SCC during BFS:
    ///    - Uses its cached reachability to update all SCCs in the current path
    ///    - Skips further exploration from that cached SCC (optimization)
    /// 4. **Path-based Caching**: Records reachability for every SCC encountered in any path
    /// 5. **Batch Cache Update**: Updates the cache with reachability data for all path SCCs
    ///
    /// # Performance Optimizations
    ///
    /// - **Transitive Closure**: If path A→B→C exists and C's reachability is cached as {D,E,F},
    ///   then both A and B are updated to reach {C,D,E,F}
    /// - **Early Termination**: Stops exploring from SCCs that already have cached data
    /// - **Batch Operations**: Updates cache in batch to reduce lock contention
    /// - **Filtered Results**: Only returns SCCs that are in the target set to reduce memory usage
    /// # Thread Safety
    ///
    /// Function is thread-safe through DashMap's concurrent operations, enabling parallel
    /// execution across different starting SCCs without data races.
    pub(crate) fn compute_reachable_sccs_with_cache(
        &self,
        start_scc: u64,
        target_sccs: &FxHashSet<u64>,
        cache: &mut FxHashMap<u64, Arc<FxHashSet<u64>>>,
    ) -> FxHashSet<u64> {
        // Check if reachability from start_scc is already cached
        if let Some(cached) = cache.get(&start_scc) {
            return cached
                .iter()
                .filter(|&&scc| target_sccs.contains(&scc))
                .cloned()
                .collect();
        }

        // Initialize BFS data structures with path tracking
        let mut visited = FxHashSet::default();
        let mut queue = VecDeque::new();

        // Maps each SCC to the set of SCCs reachable from it (discovered during BFS)
        let mut path_cache: FxHashMap<u64, FxHashSet<u64>> = FxHashMap::default();

        // Start BFS from the initial SCC, tracking the path from start
        queue.push_back((start_scc, vec![start_scc]));
        visited.insert(start_scc);
        path_cache.insert(start_scc, FxHashSet::default());

        while let Some((current_scc, path)) = queue.pop_front() {
            // Optimization: leverage existing cache data for transitive closure
            if let Some(cached_reachable) = cache.get(&current_scc) {
                // Update reachability for all SCCs in the current path using cached data
                for &path_scc in &path {
                    path_cache
                        .entry(path_scc)
                        .or_insert_with(FxHashSet::default)
                        .extend(cached_reachable.iter());
                }
                // Skip further exploration from this cached SCC since its reachability is known
                continue;
            }

            // Standard BFS traversal on the SCC DAG
            if let Some(neighbors) = self.scc_dag.get(&current_scc) {
                for &neighbor_scc in neighbors {
                    if !visited.contains(&neighbor_scc) {
                        visited.insert(neighbor_scc);

                        // Update reachability for all SCCs in the current path
                        // Each SCC in the path can reach this newly discovered neighbor
                        for &path_scc in &path {
                            path_cache
                                .entry(path_scc)
                                .or_insert_with(FxHashSet::default)
                                .insert(neighbor_scc);
                        }

                        // Extend the path and continue BFS
                        let mut new_path = path.clone();
                        new_path.push(neighbor_scc);
                        queue.push_back((neighbor_scc, new_path));
                    }
                }
            }
        }

        // Batch update the shared cache with all discovered reachability data
        for (scc_id, reachable) in path_cache {
            if !reachable.is_empty() {
                cache.insert(scc_id, Arc::new(reachable.clone()));
            }
        }

        // Return only the target SCCs that are reachable from start_scc
        cache
            .get(&start_scc)
            .map(|reachable| {
                reachable
                    .iter()
                    .filter(|&&scc| target_sccs.contains(&scc))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Determines if there is a path from one SCC to another in the SCC-level DAG.
    ///
    /// This function performs a breadth-first search (BFS) traversal of the SCC DAG
    /// to check if the destination SCC is reachable from the source SCC.
    ///
    /// # Arguments
    ///
    /// * `src_scc_id` - A reference to the unique identifier of the source SCC
    /// * `dst_scc_id` - A reference to the unique identifier of the destination SCC
    ///
    /// # Returns
    ///
    /// * `true` - If the destination SCC is reachable from the source SCC
    /// * `false` - If there is no path from the source to the destination
    fn is_scc_reachable(&self, src_scc_id: &u64, dst_scc_id: &u64) -> bool {
        // If source and destination are the same, they are trivially reachable
        if src_scc_id == dst_scc_id {
            return true;
        }

        // Use BFS to find if dst_scc_id is reachable from src_scc_id
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();

        // Start BFS from the source SCC
        queue.push_back(*src_scc_id);
        visited.insert(*src_scc_id);

        while let Some(current_scc) = queue.pop_front() {
            // If the current SCC is not in the SCC DAG, skip it
            if let Some(neighbors) = self.scc_dag.get(&current_scc) {
                // Check all neighbors of the current SCC
                for &neighbor in neighbors {
                    // If we found the destination SCC, return true
                    if neighbor == *dst_scc_id {
                        return true;
                    }

                    // If we haven't visited this neighbor yet, add it to the queue
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // If we've exhausted all reachable SCCs and haven't found dst_scc_id, return false
        false
    }

    /// Main function: Builds all strongly connected components (SCCs) of a graph
    /// using Tarjan's algorithm.
    ///
    /// Returns a tuple containing:
    /// - A mapping from vertex IDs to their SCC IDs
    /// - A vector of all SCCs, where each SCC is a vector of vertex IDs
    pub fn build_scc(g: &CSRSubGraph<u64, u64, u64>) -> (HashMap<u64, u64>, Vec<Vec<u64>>) {
        // Collect all vertex IDs into a HashSet
        let all_vertices: HashSet<u64> = g.vertex_list().iter().cloned().collect();

        // Initialize data structures for Tarjan's algorithm
        let mut dfn: HashMap<u64, u64> = HashMap::new(); // Discovery time for each vertex
        let mut low: HashMap<u64, u64> = HashMap::new(); // Lowest reachable vertex time
        let mut in_stack: HashMap<u64, bool> = HashMap::new(); // Tracks vertices in the stack
        let mut scc_index: u64 = 0; // Current discovery time
        let mut scc_cnt: u64 = 0; // SCC counter

        // Result containers
        let mut scc_map: HashMap<u64, u64> = HashMap::new(); // Maps vertices to their SCC IDs
        let mut components: Vec<Vec<u64>> = Vec::new(); // Lists of vertices in each SCC

        // Apply Tarjan's algorithm to each unvisited vertex
        for &vi in all_vertices.iter() {
            if !dfn.contains_key(&vi) {
                Self::tarjan(
                    g,
                    vi,
                    &mut dfn,
                    &mut low,
                    &mut in_stack,
                    &mut scc_index,
                    &mut scc_cnt,
                    &mut scc_map,
                    &mut components,
                );
            }
        }

        (scc_map, components)
    }

    /// Tarjan function: Implements a non-recursive version of Tarjan's algorithm for a vertex
    ///
    /// This function identifies strongly connected components (SCCs) starting from a given vertex
    /// using an iterative approach instead of recursion to avoid stack overflow for large graphs.
    ///
    /// # Parameters
    /// * `g` - The graph in CSR format
    /// * `start_id` - ID of the vertex to start the traversal from
    /// * `dfn` - Discovery time map for each vertex
    /// * `low` - Lowest reachable vertex time map
    /// * `in_stack` - Tracks whether a vertex is currently in the stack
    /// * `scc_index` - Current discovery time counter
    /// * `scc_cnt` - Counter for the number of SCCs found
    /// * `scc_map` - Output mapping from vertex IDs to their SCC IDs
    /// * `components` - Output list of SCCs, where each SCC is a vector of vertex IDs
    fn tarjan(
        g: &CSRSubGraph<u64, u64, u64>,
        start_id: u64,
        dfn: &mut HashMap<u64, u64>,
        low: &mut HashMap<u64, u64>,
        in_stack: &mut HashMap<u64, bool>,
        scc_index: &mut u64,
        scc_cnt: &mut u64,
        scc_map: &mut HashMap<u64, u64>,
        components: &mut Vec<Vec<u64>>,
    ) {
        // Structure to track progress through a vertex's neighbors
        struct VertexState {
            id: u64,
            out_idx: isize,
            neighbors: Vec<u64>,
        }

        // Stack for SCC identification and buffer for backtracking
        let mut scc_stk: Vec<u64> = Vec::new();
        let mut scc_buff: Vec<(u64, isize)> = Vec::new(); // Store ID and out_idx

        // Initialize the starting vertex
        let neighbors = g.read_neighbor(&start_id);
        let mut u = VertexState {
            id: start_id,
            out_idx: neighbors.len() as isize - 1,
            neighbors,
        };

        // Set initial values for the starting vertex
        *scc_index += 1;
        dfn.insert(u.id, *scc_index);
        low.insert(u.id, *scc_index);
        scc_stk.push(u.id);
        in_stack.insert(u.id, true);

        // Non-recursive DFS implementation
        loop {
            while u.out_idx >= 0 {
                let v_id = u.neighbors[u.out_idx as usize];

                if !dfn.contains_key(&v_id) {
                    // Unvisited neighbor, continue DFS
                    *scc_index += 1;
                    dfn.insert(v_id, *scc_index);
                    low.insert(v_id, *scc_index);
                    scc_stk.push(v_id);
                    in_stack.insert(v_id, true);

                    // Save current state for backtracking (including current out_idx)
                    scc_buff.push((u.id, u.out_idx));

                    // Prepare to process neighbor v
                    let v_neighbors = g.read_neighbor(&v_id);
                    u = VertexState {
                        id: v_id,
                        out_idx: v_neighbors.len() as isize - 1,
                        neighbors: v_neighbors,
                    };
                } else {
                    // Already visited neighbor
                    if *in_stack.get(&v_id).unwrap_or(&false) {
                        let current_low = *low.get_mut(&u.id).unwrap();
                        let v_dfn = *dfn.get(&v_id).unwrap();
                        *low.get_mut(&u.id).unwrap() = current_low.min(v_dfn);
                    }
                    u.out_idx -= 1;
                }
            }

            // Check if current vertex is the root of an SCC
            if dfn[&u.id] == low[&u.id] {
                // Create a new SCC
                let mut component = Vec::new();

                // Pop all vertices of this SCC from the stack
                loop {
                    let v_id = scc_stk.pop().unwrap();
                    in_stack.insert(v_id, false);
                    component.push(v_id);
                    scc_map.insert(v_id, *scc_cnt);

                    if v_id == u.id {
                        break;
                    }
                }

                components.push(component);
                *scc_cnt += 1;
            }

            // Backtrack to parent vertex
            if let Some((parent_id, parent_out_idx)) = scc_buff.pop() {
                // Get parent's neighbor list
                let parent_neighbors = g.read_neighbor(&parent_id);

                // Update parent's low value
                let parent_low = *low.get(&parent_id).unwrap();
                let current_low = *low.get(&u.id).unwrap();
                *low.get_mut(&parent_id).unwrap() = parent_low.min(current_low);

                // Continue processing the parent with the saved out_idx - 1
                u = VertexState {
                    id: parent_id,
                    out_idx: parent_out_idx - 1, // Move to next neighbor after the one we just processed
                    neighbors: parent_neighbors,
                };
            } else {
                // Processing of current connected component complete
                break;
            }
        }
    }

    /// Build the SCC-DAG structure for organizing the giant communities.
    ///
    /// This function constructs a Directed Acyclic Graph (DAG) where each node is a Strongly Connected Component (SCC).
    /// The DAG is organized in levels based on topological sorting, which helps visualize the hierarchical structure
    /// of the graph's communities.
    ///
    /// # Parameters
    /// * `g` - The original graph in CSR format
    /// * `scc_list` - List of SCCs, where each SCC is represented as a vector of vertex IDs
    /// * `vertex_scc` - Mapping from vertex IDs to their corresponding SCC IDs
    ///
    /// # Returns
    /// A tuple containing:
    /// * HashMap mapping SCC IDs to their level in the DAG hierarchy
    /// * Vector of levels, where each level contains a list of SCC IDs at that level
    /// * HashMap mapping SCC IDs to their neighbor SCC IDs (outgoing edges in the DAG)
    pub fn build_scc_dag(
        g: &CSRSubGraph<u64, u64, u64>,
        scc_list: &Vec<Vec<u64>>,
        vertex_scc: &HashMap<u64, u64>,
    ) -> (
        HashMap<u64, u64>,      // SCC ID -> level
        Vec<Vec<u64>>,          // List of SCC IDs at each level
        HashMap<u64, Vec<u64>>, // SCC ID -> list of neighbor SCC IDs
    ) {
        let scc_count = scc_list.len();

        let mut scc_out_edges: HashMap<u64, HashSet<u64>> = HashMap::new();
        let mut scc_in_degree: HashMap<u64, u64> = HashMap::new();

        for scc_id in 0..scc_count as u64 {
            scc_out_edges.insert(scc_id, HashSet::new());
            scc_in_degree.insert(scc_id, 0);
        }

        for (source_scc_id, vertices) in scc_list.iter().enumerate() {
            let source_scc_id = source_scc_id as u64;

            for &vertex in vertices {
                let neighbors = g.read_neighbor(&vertex);

                for &neighbor in &neighbors {
                    if let Some(&target_scc_id) = vertex_scc.get(&neighbor) {
                        if source_scc_id != target_scc_id {
                            if scc_out_edges
                                .get_mut(&source_scc_id)
                                .unwrap()
                                .insert(target_scc_id)
                            {
                                *scc_in_degree.get_mut(&target_scc_id).unwrap() += 1;
                            }
                        }
                    }
                }
            }
        }

        let mut scc_level: HashMap<u64, u64> = HashMap::new();
        let mut level_sccs: Vec<Vec<u64>> = Vec::new();

        let mut queue: VecDeque<u64> = VecDeque::new();

        // Add all SCCs with in-degree 0 to the queue (level 0)
        for scc_id in 0..scc_count as u64 {
            if *scc_in_degree.get(&scc_id).unwrap() == 0 {
                queue.push_back(scc_id);
                scc_level.insert(scc_id, 0);
            }
        }

        level_sccs.push(queue.iter().cloned().collect());

        while !queue.is_empty() {
            let scc_id = queue.pop_front().unwrap();
            let current_level = *scc_level.get(&scc_id).unwrap();

            for &next_scc_id in scc_out_edges.get(&scc_id).unwrap() {
                let in_degree = scc_in_degree.get_mut(&next_scc_id).unwrap();
                *in_degree -= 1;

                if *in_degree == 0 {
                    let next_level = current_level + 1;
                    scc_level.insert(next_scc_id, next_level);
                    queue.push_back(next_scc_id);

                    while level_sccs.len() <= next_level as usize {
                        level_sccs.push(Vec::new());
                    }

                    level_sccs[next_level as usize].push(next_scc_id);
                }
            }
        }

        // Debug: Check if any SCCs are missing a level
        let mut un_assigned_scc_list = vec![];
        let mut un_assigned_scc_in_degree = vec![];
        for scc_id in 0..scc_count as u64 {
            if !scc_level.contains_key(&scc_id) {
                un_assigned_scc_list.push(scc_id);
                un_assigned_scc_in_degree.push(scc_in_degree.get(&scc_id).unwrap().clone());
            }
        }

        let mut scc_neighbors: HashMap<u64, Vec<u64>> = HashMap::new();
        for (scc_id, neighbors) in scc_out_edges {
            scc_neighbors.insert(scc_id, neighbors.into_iter().collect());
        }

        (scc_level, level_sccs, scc_neighbors)
    }

    /// Prints a visualization of the SCC-DAG structure for better understanding of the community organization.
    ///
    /// This function creates a textual representation of the Strongly Connected Components (SCCs)
    /// organized as a Directed Acyclic Graph (DAG), showing hierarchical levels, connections between SCCs,
    /// and various statistics about the graph structure.
    ///
    /// # Parameters
    /// * `scc_list` - List of SCCs, where each SCC is represented as a vector of vertex IDs
    /// * `scc_level` - Mapping from SCC IDs to their level in the DAG hierarchy
    /// * `level_list` - Vector of levels, where each level contains a list of SCC IDs at that level
    /// * `scc_neighbors` - Mapping from SCC IDs to their neighbor SCC IDs (outgoing edges in the DAG)
    pub fn print_scc_dag(
        scc_list: &Vec<Vec<u64>>,
        scc_level: &HashMap<u64, u64>,
        level_list: &Vec<Vec<u64>>,
        scc_neighbors: &HashMap<u64, Vec<u64>>,
    ) {
        println!("\n=== SCC-DAG Visualization ===");

        // Print level structure
        println!("\nLevel Structure:");
        for (level, sccs) in level_list.iter().enumerate() {
            println!("Level {}: {:?}", level, sccs);
        }

        // Print detailed information for each SCC
        println!("\nSCC Details:");
        for (scc_id, vertices) in scc_list.iter().enumerate() {
            let scc_id = scc_id as u64;
            let level = scc_level.get(&scc_id).unwrap_or(&0);

            // Get outgoing edges
            let neighbors = match scc_neighbors.get(&scc_id) {
                None => &vec![],
                Some(neighbors) => neighbors,
            };

            println!(
                "SCC {:2} (Level: {:2}): Contains {:4} vertices, connects to: {:?}",
                scc_id,
                level,
                vertices.len(),
                neighbors
            );

            // Print vertices in the SCC (maximum 5 displayed)
            let vertex_display = if vertices.len() <= 5 {
                format!("{:?}", vertices)
            } else {
                format!("[{:?} ... +{} more]", &vertices[0..5], vertices.len() - 5)
            };

            println!("    Vertices: {}", vertex_display);
        }

        // Print ASCII tree diagram showing relationships between SCCs
        println!("\nSCC Relationship Diagram (max 8 SCCs per level):");

        for (level, sccs) in level_list.iter().enumerate() {
            // Display level label
            println!("Level {}:", level);

            // Maximum 8 SCCs per level
            let display_sccs = if sccs.len() <= 8 {
                sccs.clone()
            } else {
                let mut limited = sccs[0..7].to_vec();
                limited.push(sccs[sccs.len() - 1]);
                limited
            };

            // Print SCC boxes
            for &scc_id in &display_sccs {
                print!("+-SCC {:2}-+", scc_id);
                if scc_id != *display_sccs.last().unwrap() {
                    print!("   ");
                }
            }
            println!();

            // If not the last level, show connection lines
            if level < level_list.len() - 1 {
                // Display outgoing edges
                for &scc_id in &display_sccs {
                    let neighbors = match scc_neighbors.get(&scc_id) {
                        None => &vec![],
                        Some(neighbors) => neighbors,
                    };

                    if !neighbors.is_empty() {
                        print!("|        |   ");
                    } else {
                        print!("|        |   ");
                    }
                }
                println!();

                for &scc_id in &display_sccs {
                    let neighbors = match scc_neighbors.get(&scc_id) {
                        None => &vec![],
                        Some(neighbors) => neighbors,
                    };

                    if !neighbors.is_empty() {
                        print!("+---+----+   ");
                    } else {
                        print!("|        |   ");
                    }
                }
                println!();

                for &scc_id in &display_sccs {
                    let neighbors = match scc_neighbors.get(&scc_id) {
                        None => &vec![],
                        Some(neighbors) => neighbors,
                    };

                    if !neighbors.is_empty() {
                        print!("    |        ");
                    } else {
                        print!("             ");
                    }
                }
                println!();

                for &scc_id in &display_sccs {
                    let neighbors = match scc_neighbors.get(&scc_id) {
                        None => &vec![],
                        Some(neighbors) => neighbors,
                    };

                    if !neighbors.is_empty() {
                        print!("    v        ");
                    } else {
                        print!("             ");
                    }
                }
                println!();
            }
        }

        // Print statistics
        println!("\nStatistics:");
        println!("Total SCCs: {}", scc_list.len());
        println!("Total Levels: {}", level_list.len());

        // Calculate number of edges
        let total_edges: usize = scc_neighbors
            .values()
            .map(|neighbors| neighbors.len())
            .sum();
        println!("Edges between SCCs: {}", total_edges);

        // Calculate maximum out-degree and in-degree
        let max_out_degree = scc_neighbors
            .values()
            .map(|neighbors| neighbors.len())
            .max()
            .unwrap_or(0);

        // Calculate in-degrees
        let mut in_degrees: HashMap<u64, usize> = HashMap::new();
        for neighbors in scc_neighbors.values() {
            for &neighbor in neighbors {
                *in_degrees.entry(neighbor).or_insert(0) += 1;
            }
        }

        let max_in_degree = in_degrees.values().cloned().max().unwrap_or(0);

        println!("Maximum Out-degree: {}", max_out_degree);
        println!("Maximum In-degree: {}", max_in_degree);
    }

    /// Builds a Strongly Connected Components (SCC) index from the provided subgraph.
    ///
    /// This function constructs an SCC index by:
    /// 1. Identifying all strongly connected components in the graph
    /// 2. Building a directed acyclic graph (DAG) of these components
    /// 3. Organizing the components into hierarchical levels
    ///
    /// # Arguments
    /// * `g` - Reference to a CSR (Compressed Sparse Row) subgraph
    ///
    /// # Returns
    /// * `Self` - A new SCC index containing the component structure
    pub fn build_from_subgraph(g: &CSRSubGraph<u64, u64, u64>) -> Self {
        // Step 1: Identify strongly connected components in the graph
        // Apply Tarjan's algorithm to find all SCCs
        let (vertex_scc, scc_list) = SCCMeta::build_scc(&g);

        // Step 2: Construct the SCC-DAG (Directed Acyclic Graph)
        // Build a higher-level graph where each node represents an SCC
        // and organize components into hierarchical levels
        let (scc_level, level_list, scc_dag) = SCCMeta::build_scc_dag(&g, &scc_list, &vertex_scc);

        // Return a new SCC index containing all computed structures
        Self {
            scc_list,   // List of all strongly connected components
            scc_level,  // Level assigned to each SCC in the hierarchy
            level_list, // Lists of SCCs grouped by level
            vertex_scc, // Mapping from original vertices to their SCC
            scc_dag,    // Directed acyclic graph of SCCs
        }
    }
}
