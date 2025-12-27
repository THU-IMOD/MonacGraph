use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use dashmap::DashSet;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use rayon::ThreadPoolBuilder;

use crate::comm_io::comm_idx::CommunityIndex;
use crate::comm_io::{CommunityItemRef, CommunityStorage};
use crate::types::graph_query::GraphQuery;

/// Configuration for Weakly Connected Components (WCC) algorithm.
///
/// # Fields
///
/// * `thread_num` - The number of threads to use for parallel computation.
#[derive(Clone, Default)]
pub struct WCCConfig {
    pub thread_num: usize,
}

/// Trait for computing Weakly Connected Components (WCC) in a graph.
///
/// This trait provides methods to count and identify weakly connected components
/// in a graph structure. A weakly connected component is a maximal subgraph where
/// there exists a path between any two vertices, ignoring edge directions.
///
/// # Type Parameters
///
/// * `T` - The type used for node/vertex identifiers and the count of components.
///
/// # Methods
///
/// * `count_wcc` - Returns the total number of weakly connected components.
/// * `wcc` - Returns all weakly connected components as vectors of node identifiers,
///           with configurable parallelism options.
pub trait WCC<T> {
    fn count_wcc(&self) -> T;
    fn wcc(&self, wcc_config: WCCConfig) -> Vec<Vec<T>>;
}

/// Controller for Weakly Connected Components (WCC) operations.
///
/// This struct serves as the main interface for executing WCC algorithms
/// on graph data stored in the community storage engine.
///
/// # Fields
///
/// * `storage_engine` - Thread-safe reference-counted pointer to the storage
///   backend that contains the graph data for community detection.
pub struct WCCController {
    storage_engine: Arc<CommunityStorage>,
}

impl CommunityIndex {
    /// Computes weakly connected components (WCC) in the community graph.
    ///
    /// This method identifies groups of communities that are connected to each other,
    /// regardless of edge direction. The implementation uses the following approach:
    ///
    /// # Algorithm
    ///
    /// 1. Constructs an undirected community graph from the boundary graph
    /// 2. Performs breadth-first search (BFS) to identify all connected components
    /// 3. Returns sorted components for consistent results
    ///
    /// # Returns
    ///
    /// A vector of vectors, where each inner vector represents one weakly connected
    /// component containing the community IDs that belong to that component.
    /// Communities within each component are sorted in ascending order.
    ///
    /// # Complexity
    ///
    /// - Time Complexity: O(E + V), where E is the number of edges between communities
    ///   and V is the number of communities
    /// - Space Complexity: O(V) for the visited set and queue
    pub fn community_wcc(&self) -> Vec<Vec<u32>> {
        // Step 1. Build the community graph.
        let mut community_graph = HashMap::<u32, HashSet<u32>>::new();
        for (src_comm_id, dst_comm_id) in self.boundary_graph.community_boundary.keys() {
            if community_graph.contains_key(src_comm_id) {
                let src_comm_neighbors = community_graph.get_mut(src_comm_id).unwrap();
                src_comm_neighbors.insert(*dst_comm_id);
            } else {
                let mut src_comm_neighbors = HashSet::<u32>::new();
                src_comm_neighbors.insert(*dst_comm_id);
                community_graph.insert(*src_comm_id, src_comm_neighbors);
            }

            if community_graph.contains_key(dst_comm_id) {
                let dst_comm_neighbors = community_graph.get_mut(dst_comm_id).unwrap();
                dst_comm_neighbors.insert(*src_comm_id);
            } else {
                let mut dst_comm_neighbors = HashSet::<u32>::new();
                dst_comm_neighbors.insert(*src_comm_id);
                community_graph.insert(*dst_comm_id, dst_comm_neighbors);
            }
        }

        // Step 2. Find weakly connected components (WCC) using BFS.
        let mut wcc_result = Vec::<Vec<u32>>::new();

        // Track visited communities
        let mut visited = HashSet::<u32>::new();

        // Process each community as a potential starting point
        for &community_id in community_graph.keys() {
            // Skip if this community has already been assigned to a component
            if visited.contains(&community_id) {
                continue;
            }

            // Create a new component
            let mut component = Vec::<u32>::new();

            // Initialize BFS queue with the current community
            let mut queue = VecDeque::<u32>::new();
            queue.push_back(community_id);
            visited.insert(community_id);

            // BFS traversal to find all communities in this component
            while !queue.is_empty() {
                let current_community = queue.pop_front().unwrap();
                component.push(current_community);

                // Check all neighbors of the current community
                if let Some(neighbors) = community_graph.get(&current_community) {
                    for &neighbor in neighbors {
                        // Process unvisited neighbors
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back(neighbor);
                        }
                    }
                }
            }

            // Sort communities in this component for consistent output
            component.sort();

            // Add this component to the results
            wcc_result.push(component);
        }

        // Return all identified weakly connected components
        wcc_result
    }
}

impl WCCController {
    /// Creates a new WCC controller with the specified storage engine.
    ///
    /// # Parameters
    ///
    /// * `storage_engine` - Thread-safe reference-counted pointer to the community storage
    ///   that contains the graph data for WCC operations.
    ///
    /// # Returns
    ///
    /// A new `WCCController` instance.
    pub fn new(storage_engine: Arc<CommunityStorage>) -> Self {
        Self { storage_engine }
    }
}

impl WCC<u64> for WCCController {
    /// Returns the total count of weakly connected components in the community graph.
    ///
    /// This method retrieves the pre-computed WCC results from the storage engine
    /// and returns the count as a 64-bit unsigned integer.
    ///
    /// # Returns
    ///
    /// The number of weakly connected components in the community graph.
    fn count_wcc(&self) -> u64 {
        self.storage_engine.community_index.community_wcc().len() as u64
    }

    /// Retrieves all weakly connected components as vectors of vertex IDs with parallel processing.
    ///
    /// This method extracts vertices from each community in the weakly connected components,
    /// using the configured thread pool for parallel processing.
    ///
    /// # Parameters
    ///
    /// * `wcc_config` - Configuration settings for WCC processing, including the number of threads.
    ///
    /// # Returns
    ///
    /// A vector of vectors, where each inner vector contains the vertex IDs belonging to
    /// one weakly connected component.
    ///
    /// # Implementation Details
    ///
    /// - Uses a thread pool with the specified number of threads for parallel processing
    /// - Processes each community in parallel to extract vertex IDs
    /// - Handles both normal and giant communities differently
    /// - Uses a thread-safe `DashSet` to collect results from multiple threads
    fn wcc(&self, wcc_config: WCCConfig) -> Vec<Vec<u64>> {
        let wcc_vertex_set = DashSet::<Vec<u64>>::new();
        let wcc_community_list = self.storage_engine.community_index.community_wcc();

        // Create a thread pool with the specified number of threads
        let thread_num = wcc_config.thread_num;
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_num)
            .build()
            .unwrap();
        pool.install(|| {
            wcc_community_list.par_iter().for_each(|wcc_item| {
                let mut wcc_vertex_list = vec![];
                for community_id in wcc_item {
                    let community_item_ref = self
                        .storage_engine
                        .load_community_ref(community_id)
                        .unwrap();
                    match community_item_ref {
                        CommunityItemRef::Normal(small_community_ref) => {
                            let mut vertex_list = small_community_ref
                                .vertex_list()
                                .into_iter()
                                .map(|vertex| vertex)
                                .collect::<Vec<_>>();
                            wcc_vertex_list.append(&mut vertex_list);
                        }
                        CommunityItemRef::Giant(giant_community_idx) => {
                            let mut vertex_list = giant_community_idx
                                .scc_meta
                                .vertex_scc
                                .keys()
                                .cloned()
                                .collect::<Vec<_>>();
                            wcc_vertex_list.append(&mut vertex_list);
                        }
                    }
                }
                wcc_vertex_set.insert(wcc_vertex_list);
            });
        });
        wcc_vertex_set.into_iter().collect::<Vec<_>>()
    }
}
