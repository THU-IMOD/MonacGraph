use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::comm_io::comm_idx::{BoundaryGraph, CommunityIndexItem};
use crate::comm_io::sim_csr_block::CSRSimpleCommBlock;
use crate::comm_io::{CommunityItemRef, CommunityStorage};
use crate::types::graph_query::GraphQuery;
use dashmap::DashSet;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rustc_hash::{FxHashMap, FxHashSet};

/// Configuration for the BFS (Breadth-First Search) algorithm.
///
/// # Type Parameters
/// * `'a` - Lifetime parameter for the referenced global visit set
/// * `K` - Key type for the elements in the visit set
///
/// # Fields
/// * `thread_num` - Number of threads to use for parallel execution
/// * `global_visit` - Optional shared visit set to track globally visited nodes across threads
pub struct BFSConfig<'a, K> {
    pub thread_num: usize,
    pub global_visit: Option<&'a DashSet<K>>,
}

/// Trait for implementing the Breadth-First Search algorithm.
///
/// # Type Parameters
/// * `K` - Key type for vertex identifiers
/// * `T` - Result type returned by the BFS algorithm
///
/// This trait allows different graph implementations to provide their own
/// BFS traversal method while maintaining a consistent interface.
#[allow(dead_code)]
pub trait BFS<K, T> {
    /// Performs a breadth-first search starting from the specified vertex.
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    /// * `bfs_config` - Configuration parameters for the BFS algorithm
    ///
    /// # Returns
    /// A vector containing the result elements of type `T` from the BFS traversal
    fn bfs(&self, start_vertex_id: &K, bfs_config: BFSConfig<K>) -> Vec<T>;
}

#[allow(dead_code)]
impl BFS<u64, u64> for BoundaryGraph {
    /// Performs a breadth-first search on a boundary graph starting from the specified vertex.
    ///
    /// This implementation requires a global visit set to track vertices that have been
    /// visited across different BFS calls or across thread boundaries.
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    /// * `bfs_config` - Configuration parameters for the BFS algorithm, including the global visit set
    ///
    /// # Returns
    /// A vector containing all vertex IDs visited during the BFS traversal
    ///
    /// # Panics    ///  if no global visit set is provided in the configuration
    fn bfs(&self, start_vertex_id: &u64, bfs_config: BFSConfig<u64>) -> Vec<u64> {
        match bfs_config.global_visit {
            None => {
                panic!("Boundary graph needs a global visit list.");
            }
            Some(global_visit_set) => {
                // Convert start vertex global ID to local ID
                let start_local_id =
                    match self.boundary_csr.vertex_mapper_reverse.get(start_vertex_id) {
                        Some(&local_id) => local_id,
                        None => {
                            // Start vertex not in boundary graph, return empty result
                            return Vec::new();
                        }
                    };

                // Initialize local data structures for BFS traversal
                let mut visited = HashSet::new();
                let mut queue = VecDeque::new();
                let mut result = Vec::new();

                // Add the starting vertex to the queue (using local ID)
                queue.push_back(start_local_id);

                // Mark the starting vertex as visited in the local set (using global ID)
                visited.insert(*start_vertex_id);

                // Standard BFS loop
                while let Some(current_local) = queue.pop_front() {
                    // Convert local ID back to global ID for result
                    let current_global = self.boundary_csr.vertex_mapper[current_local as usize];
                    result.push(current_global);

                    // Get the edge range for current vertex from CSR
                    let start_offset = self.boundary_csr.offset_list[current_local as usize];
                    let end_offset = self.boundary_csr.offset_list[current_local as usize + 1];

                    // Process all neighbors of the current vertex
                    for edge_idx in start_offset..end_offset {
                        let neighbor_local = self.boundary_csr.edge_list[edge_idx as usize];
                        let neighbor_global =
                            self.boundary_csr.vertex_mapper[neighbor_local as usize];

                        // Only process neighbors that haven't been visited locally or globally
                        if !visited.contains(&neighbor_global)
                            && !global_visit_set.contains(&neighbor_global)
                        {
                            // Mark the neighbor as visited in both global and local sets
                            global_visit_set.insert(neighbor_global);
                            visited.insert(neighbor_global);
                            // Queue the neighbor for processing (using local ID)
                            queue.push_back(neighbor_local);
                        }
                    }
                }

                // Return the list of visited vertices (all in global IDs)
                result
            }
        }
    }
}

impl CSRSimpleCommBlock {
    pub fn inner_bfs(&self, start_vertex_id: &u64) -> Vec<u64> {
        // Initialize local data structures for BFS traversal
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        // Add the starting vertex to the queue
        queue.push_back(*start_vertex_id);

        // Mark the starting vertex as visited in the local set
        visited.insert(*start_vertex_id);

        // Standard BFS loop
        while let Some(current) = queue.pop_front() {
            // Add current vertex to the result
            result.push(current);

            // Read neighbors from the CSR structure
            let neighbors = self.read_neighbor(&current);
            for neighbor in neighbors {
                // Only process neighbors that haven't been visited locally or globally
                if !visited.contains(&neighbor) && self.vertex_index.contains_key(&neighbor) {
                    // Mark the neighbor as visited in the local set
                    visited.insert(neighbor);
                    // Queue the neighbor for processing
                    queue.push_back(neighbor);
                }
            }
        }

        // Return the list of visited vertices
        result
    }

    /// Perform BFS on the reverse graph starting from the given vertex
    /// Returns all vertices that can reach the start vertex
    pub fn reverse_bfs(&self, start: &u64) -> Vec<u64> {
        // Check if the start vertex exists in this block
        if !self.vertex_index.contains_key(start) {
            return vec![];
        }

        // Step 1: Build the reverse adjacency list
        // Only include edges where both vertices are in this block
        let mut reverse_adj: FxHashMap<u64, Vec<u64>> = FxHashMap::default();

        // Iterate through all vertices and their edges
        for i in 0..self.vertex_list.len() {
            let (src_vertex, offset) = self.vertex_list[i];

            // Determine the range of neighbors for this vertex
            let end_offset = if i + 1 < self.vertex_list.len() {
                self.vertex_list[i + 1].1
            } else {
                self.neighbor_list.len() as u64
            };

            // Add reverse edges only if the destination vertex is also in this block
            for j in offset..end_offset {
                let dst_vertex = self.neighbor_list[j as usize];

                // Only add reverse edge if dst_vertex exists in this block
                if self.vertex_index.contains_key(&dst_vertex) {
                    reverse_adj.entry(dst_vertex).or_default().push(src_vertex);
                }
            }
        }

        // Step 2: Perform BFS on the reverse graph
        let mut visited = FxHashSet::default();
        let mut queue = VecDeque::new();
        let mut reachable = Vec::new();

        queue.push_back(*start);
        visited.insert(*start);
        reachable.push(*start);

        while let Some(current) = queue.pop_front() {
            if let Some(predecessors) = reverse_adj.get(&current) {
                for &pred in predecessors {
                    // Double check that predecessor is in this block (should always be true)
                    if self.vertex_index.contains_key(&pred) && visited.insert(pred) {
                        queue.push_back(pred);
                        reachable.push(pred);
                    }
                }
            }
        }
        reachable
    }
}

impl BFS<u64, u64> for CSRSimpleCommBlock {
    /// Performs a breadth-first search on a CSR (Compressed Sparse Row) graph structure
    /// starting from the specified vertex.
    ///
    /// This implementation requires a global visit set to track vertices that have been
    /// visited across different BFS calls or across thread boundaries.
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    /// * `bfs_config` - Configuration parameters for the BFS algorithm, including the global visit set
    ///
    /// # Returns
    /// A vector containing all vertex IDs visited during the BFS traversal
    ///
    /// # Panics    ///  if no global visit set is provided in the configuration
    fn bfs(&self, start_vertex_id: &u64, bfs_config: BFSConfig<u64>) -> Vec<u64> {
        match bfs_config.global_visit {
            None => {
                panic!("CSR block needs a global visit list.");
            }
            Some(visited_set) => {
                // Initialize local data structures for BFS traversal
                let mut visited = HashSet::new();
                let mut queue = VecDeque::new();
                let mut result = Vec::new();

                // Add the starting vertex to the queue
                queue.push_back(*start_vertex_id);

                // Mark the starting vertex as visited in the local set
                visited.insert(*start_vertex_id);

                // Standard BFS loop
                while let Some(current) = queue.pop_front() {
                    // Add current vertex to the result
                    result.push(current);

                    // Read neighbors from the CSR structure
                    let neighbors = self.read_neighbor(&current);
                    for neighbor in neighbors {
                        // Only process neighbors that haven't been visited locally or globally
                        if !visited.contains(&neighbor) && !visited_set.contains(&neighbor) {
                            // Mark the neighbor as visited in the local set
                            visited.insert(neighbor);
                            // Queue the neighbor for processing
                            queue.push_back(neighbor);
                        }
                    }
                }

                // Return the list of visited vertices
                result
            }
        }
    }
}

/// Represents different types of visit tasks in the graph traversal system.
///
/// This enum is used to specify the target type for a traversal operation,
/// which can be either a vertex-based traversal within a community or
/// an SCC-based (Strongly Connected Component) traversal.
#[allow(dead_code)]
#[derive(Hash, Eq, PartialEq, Debug)]
pub enum VisitTask {
    /// Represents a normal traversal starting from a specific vertex within a community.
    ///
    /// # Fields
    /// * `community_id` - The ID of the community in which the traversal will occur
    /// * `start_vertex_id` - The ID of the starting vertex for the traversal
    NormalTarget {
        community_id: u32,
        start_vertex_id: u64,
    },

    /// Represents a traversal targeting a specific SCC (Strongly Connected Component).
    ///
    /// # Fields
    /// * `community_id` - The ID of the community containing the SCC
    /// * `scc_id` - The ID of the SCC to be traversed
    SCCTarget { community_id: u32, scc_id: u64 },
}

/// Controller for managing breadth-first search operations across a community-based graph.
///
/// This struct coordinates BFS traversals by maintaining a reference to the storage
/// engine containing graph data and a global set for tracking visited vertices across
/// multiple traversals.
#[allow(dead_code)]
pub struct BFSController {
    /// Reference to the community storage engine containing graph data
    storage_engine: Arc<CommunityStorage>,

    /// Thread-safe concurrent set for tracking globally visited vertices across different BFS operations
    visited_vertex_set: DashSet<u64>,
}

#[allow(dead_code)]
impl BFSController {
    /// Creates a new BFS controller with the provided storage engine.
    ///
    /// Initializes an empty DashSet for tracking visited vertices.
    ///
    /// # Arguments
    /// * `storage_engine` - Arc-wrapped reference to the community storage containing graph data
    ///
    /// # Returns
    /// A new instance of BFSController
    pub fn new(storage_engine: Arc<CommunityStorage>) -> Self {
        Self {
            storage_engine,
            visited_vertex_set: DashSet::new(),
        }
    }

    /// Explores vertices within a specified community starting from a given vertex.
    ///
    /// This method performs a breadth-first search within a community, identifying boundary
    /// vertices that are reachable from the starting vertex. It handles different community
    /// types (normal and giant) with appropriate traversal strategies.
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    /// * `community_id` - Reference to the ID of the community to explore
    ///
    /// # Returns
    /// A HashSet containing all boundary vertex IDs reachable from the starting vertex
    fn explore_in_community(&self, start_vertex_id: &u64, community_id: &u32) -> HashSet<u64> {
        // Attempt to load the community from storage
        let community_item_opt = self.storage_engine.load_community_ref(community_id);
        match community_item_opt {
            None => {
                // If community doesn't exist, return an empty result
                HashSet::new()
            }
            Some(comm_item_ref) => {
                match comm_item_ref {
                    // Handle normal community using direct BFS
                    CommunityItemRef::Normal(community_csr_block) => {
                        let mut queue = VecDeque::new();
                        let mut result = HashSet::new();

                        // Add starting vertex to the queue
                        queue.push_back(*start_vertex_id);

                        // Mark starting vertex as globally visited
                        self.visited_vertex_set.insert(*start_vertex_id);

                        // Standard BFS loop
                        while let Some(current) = queue.pop_front() {
                            // If current vertex is a boundary vertex, add it to results
                            if self.storage_engine.community_index.is_boundary(&current) {
                                result.insert(current);
                            }

                            // Get neighbors from the CSR block
                            let neighbors = community_csr_block.read_neighbor(&current);

                            for neighbor in neighbors {
                                // Only process valid neighbors that haven't been visited yet
                                if community_csr_block.vertex_index.contains_key(&neighbor)
                                    && !self.visited_vertex_set.contains(&neighbor)
                                {
                                    // Mark neighbor as globally visited
                                    self.visited_vertex_set.insert(neighbor);
                                    // Queue neighbor for processing
                                    queue.push_back(neighbor);
                                }
                            }
                        }

                        result
                    }
                    // Handle giant community using SCC-based traversal
                    CommunityItemRef::Giant(giant_comm_idx) => {
                        // Perform BFS on SCC DAG to find all reachable SCCs
                        let scc_id_list = giant_comm_idx.scc_meta.bfs_scc_dag(start_vertex_id);

                        // Collect all vertices from the reachable SCCs
                        let visited_vertex_list =
                            scc_id_list
                                .into_iter()
                                .fold(Vec::<u64>::new(), |mut acc, scc_id| {
                                    let mut vertex_list =
                                        giant_comm_idx.scc_meta.scc_list[scc_id as usize].clone();
                                    acc.append(&mut vertex_list);
                                    acc
                                });

                        // Filter vertices to only include boundary vertices and mark all as visited
                        visited_vertex_list
                            .into_iter()
                            .filter(|vertex_id| {
                                self.visited_vertex_set.insert(*vertex_id);
                                self.storage_engine.community_index.is_boundary(vertex_id)
                            })
                            .collect::<HashSet<_>>()
                    }
                }
            }
        }
    }

    /// Generates visit tasks for graph traversal starting from a specified vertex.
    ///
    /// This method performs a multi-stage exploration process:
    /// 1. First explores within the community of the starting vertex
    /// 2. Then explores the boundary graph connecting different communities
    /// 3. Finally, generates appropriate visit tasks based on the exploration results
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    ///
    /// # Returns
    /// A vector of VisitTask objects representing traversal tasks to be executed
    ///
    /// # Panics    ///  if the starting vertex is not found in any community
    pub fn generate_visit_task(&self, start_vertex_id: &u64) -> Vec<VisitTask> {
        // Step 1. Find the community containing the start vertex
        let community_id = match self
            .storage_engine
            .community_index
            .get_community_id(start_vertex_id)
        {
            None => {
                panic!("Community graph needs a community graph.");
            }
            Some(community_id) => community_id,
        };

        // Initialize a concurrent set to track visited boundary vertices
        let global_visited_boundary = DashSet::<u64>::new();

        // Explore the start vertex's community to find initial boundary vertices
        let init_boundary_list = self.explore_in_community(start_vertex_id, &community_id);

        // For each boundary vertex found, explore the boundary graph
        for init_boundary in &init_boundary_list {
            let _ = self.storage_engine.community_index.boundary_graph.bfs(
                init_boundary,
                BFSConfig {
                    thread_num: 0,
                    global_visit: Some(&global_visited_boundary),
                },
            );
        }

        // Remove initial boundaries from the global visited set
        for init in &init_boundary_list {
            global_visited_boundary.remove(init);
        }

        // Convert the concurrent set to a vector
        let boundary_list = global_visited_boundary.into_iter().collect::<Vec<_>>();

        // Initialize structures to track visit tasks and visited SCCs
        let mut visit_task_list = HashSet::<VisitTask>::new();
        let mut global_visited_scc = HashMap::<u32, HashSet<u64>>::new();

        // Step 2. Generate visit tasks based on discovered boundary vertices
        for boundary in boundary_list {
            // Step 2.1. Find the community containing this boundary vertex
            let community_location_opt = self
                .storage_engine
                .community_index
                .get_vertex_community(&boundary);
            match community_location_opt {
                None => {
                    // Skip if community doesn't exist
                    continue;
                }
                Some((community_id, community_index_item)) => {
                    match community_index_item {
                        // Handle normal communities with direct vertex targeting
                        CommunityIndexItem::Normal { .. } => {
                            visit_task_list.insert(VisitTask::NormalTarget {
                                community_id,
                                start_vertex_id: boundary,
                            });
                        }
                        // Handle giant communities with SCC-based targeting
                        CommunityIndexItem::Giant { .. } => {
                            // Find the SCC containing this boundary vertex
                            let giant_community_idx_opt =
                                self.storage_engine.giant_community_map.get(&community_id);
                            let scc_id_list = match giant_community_idx_opt {
                                None => {
                                    println!("Community ID Error:{}", community_id);
                                    continue;
                                }
                                Some(giant_comm_idx) => {
                                    match giant_comm_idx.scc_meta.vertex_scc.get(&boundary) {
                                        None => {
                                            println!("V-S Error:{}", boundary);
                                            println!(
                                                "V-S Map: {:?}",
                                                giant_comm_idx.scc_meta.vertex_scc
                                            );
                                            continue;
                                        }
                                        Some(scc_id) => {
                                            // Perform BFS on the SCC DAG
                                            giant_comm_idx.scc_meta.bfs_scc_id_dag(
                                                &community_id,
                                                scc_id,
                                                &mut global_visited_scc,
                                            )
                                        }
                                    }
                                }
                            };

                            // Create SCC target tasks for each reachable SCC
                            for scc_id in scc_id_list {
                                visit_task_list.insert(VisitTask::SCCTarget {
                                    community_id,
                                    scc_id,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Convert the HashSet of tasks to a Vector and return
        visit_task_list.into_iter().collect::<Vec<_>>()
    }

    /// Executes a list of visit tasks in parallel to traverse the graph.
    ///
    /// This method processes the generated visit tasks using a thread pool,
    /// handling different task types appropriately to mark visited vertices.
    ///
    /// # Arguments
    /// * `visit_task_list` - Vector of VisitTask objects to be executed
    /// * `thread_num` - Number of threads to use for parallel execution
    ///
    /// # Returns
    /// A vector containing all vertex IDs visited during the execution of tasks
    pub fn execute_visit_task(
        &self,
        visit_task_list: Vec<VisitTask>,
        thread_num: usize,
    ) -> Vec<u64> {
        // Create a thread pool with the specified number of threads
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_num)
            .build()
            .unwrap();

        // Use the thread pool to process tasks in parallel
        pool.install(|| {
            visit_task_list.par_iter().for_each(|visit_task| {
                match visit_task {
                    // Handle normal target tasks (vertex-based in normal communities)
                    VisitTask::NormalTarget {
                        community_id,
                        start_vertex_id,
                    } => {
                        // Load the community CSR
                        let community_item_ref = self
                            .storage_engine
                            .load_community_ref(community_id)
                            .unwrap();
                        match community_item_ref {
                            CommunityItemRef::Normal(csr_comm_block) => {
                                // Perform BFS within the CSR block
                                let vertex_id_list = csr_comm_block.bfs(
                                    start_vertex_id,
                                    BFSConfig {
                                        thread_num: 0,
                                        global_visit: Some(&self.visited_vertex_set),
                                    },
                                );
                                // Mark all discovered vertices as visited
                                for vertex_id in vertex_id_list {
                                    self.visited_vertex_set.insert(vertex_id);
                                }
                            }
                            CommunityItemRef::Giant(_) => {
                                // Skip if community type doesn't match expected type
                            }
                        }
                    }
                    // Handle SCC target tasks (SCC-based in giant communities)
                    VisitTask::SCCTarget {
                        community_id,
                        scc_id,
                    } => {
                        // Load the community reference
                        let community_item_ref_opt =
                            self.storage_engine.load_community_ref(community_id);
                        match community_item_ref_opt {
                            None => {
                                // Skip if community doesn't exist
                            }
                            Some(community_item_ref) => {
                                match community_item_ref {
                                    CommunityItemRef::Normal(_) => {
                                        // Skip if community type doesn't match expected type
                                    }
                                    CommunityItemRef::Giant(_) => {
                                        // Load all vertices in the specified SCC
                                        let vertex_id_list = self
                                            .storage_engine
                                            .load_scc_vertex_list(community_id, scc_id)
                                            .unwrap();
                                        // Mark all vertices in the SCC as visited
                                        for vertex_id in vertex_id_list {
                                            self.visited_vertex_set.insert(vertex_id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        });

        // Convert the concurrent set of visited vertices to a vector and return
        self.visited_vertex_set
            .iter()
            .map(|vertex_id| vertex_id.clone())
            .collect::<Vec<_>>()
    }
}

impl BFS<u64, u64> for BFSController {
    /// Implements the BFS trait for the controller, coordinating multi-community traversal.
    ///
    /// This high-level BFS implementation orchestrates traversal across multiple
    /// communities by generating and executing visit tasks.
    ///
    /// # Arguments
    /// * `start_vertex_id` - Reference to the ID of the starting vertex
    /// * `bfs_config` - Configuration parameters for the BFS algorithm
    ///
    /// # Returns
    /// A vector containing all vertex IDs visited during the traversal
    ///
    /// # Panics /// Panics if a global visit set is provided, as this implementation manages its own global tracking
    fn bfs(&self, start_vertex_id: &u64, bfs_config: BFSConfig<u64>) -> Vec<u64> {
        match bfs_config.global_visit {
            None => {
                // Step 1. Generate visit tasks based on the starting vertex
                let bfs_visit_task_list = self.generate_visit_task(&start_vertex_id);

                // Step 2. Execute the generated tasks in parallel
                self.execute_visit_task(bfs_visit_task_list, bfs_config.thread_num)
            }
            Some(_) => {
                // This implementation manages its own global visit tracking and doesn't accept external tracking
                panic!("No need for global visit function");
            }
        }
    }
}
