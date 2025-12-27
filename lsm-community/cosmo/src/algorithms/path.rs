use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::algorithms::path::PathFuture::{PathSplicing, PathUnit};
use crate::comm_io::comm_idx::{BoundaryGraph, CommunityIndexItem};
use crate::comm_io::sim_csr_block::CSRSimpleCommBlock;
use crate::comm_io::{CommunityItemRef, CommunityStorage, GiantCommunityIndex};
use crate::types::graph_query::GraphQuery;
use crate::types::CSRSubGraph;
use dashmap::DashMap;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rustc_hash::{FxHashMap, FxHashSet};

/// `PathController` is responsible for managing path-related operations.
///
/// It holds a reference to the `CommunityStorage` through an `Arc`, allowing for safe
/// shared access across multiple threads. This struct likely serves as a controller
/// for interacting with storage in the context of managing paths.
#[allow(dead_code)]
pub struct PathController {
    /// The storage engine responsible for persisting community data.
    /// Wrapped in an `Arc` to allow shared ownership across threads.
    storage_engine: Arc<CommunityStorage>,
}

/// `PathConfig` holds the configuration settings for path-related operations.
///
/// Specifically, it stores the number of threads (`thread_num`) to be used for concurrent
/// tasks. This struct can be used to configure how many threads are allocated for
/// operations involving paths.
#[allow(dead_code)]
pub struct PathConfig {
    /// The number of threads to use for concurrent path-related operations.
    pub thread_num: usize,
}

/// `EnrichedBoundaryGraph` represents a graph with enriched boundary information.
///
/// It contains vertices, neighbors, and a set of source vertices for the destination
/// vertex, along with a reference to a `BoundaryGraph`. This struct is used to
/// store additional boundary-related data for graph traversal or analysis.
#[derive(Debug)]
#[allow(dead_code)]
pub struct EnrichedBoundaryGraph<'a> {
    /// The source vertex in the enriched boundary graph.
    source_vertex: u64,
    /// The neighbors of the source vertex.
    source_neighbors: Vec<u64>,
    /// The destination vertex in the enriched boundary graph.
    dest_vertex: u64,
    /// A set of source vertices that are connected to the destination vertex.
    dest_source: HashSet<u64>,
    /// A reference to the original `BoundaryGraph` associated with this enriched graph.
    boundary_graph: &'a BoundaryGraph,
}

/// `CommunitySplicingTask` represents a task for splicing two community nodes.
///
/// It stores the community ID and the source and destination vertices involved
/// in the splicing operation. This struct is used to define and track a task
/// for connecting or merging two nodes in a community graph.
#[derive(Debug)]
#[allow(dead_code)]
pub struct CommunitySplicingTask {
    /// The unique identifier of the community.
    community_id: u32,
    /// The source vertex in the splicing task.
    src: u64,
    /// The destination vertex in the splicing task.
    dst: u64,
}

/// `PathFuture` represents the future state of a path operation.
///
/// It can either be a single path unit identified by a unique ID or a community
/// splicing task that involves merging two nodes in a community graph. This enum
/// is used to represent different types of path-related tasks or results.
#[derive(Debug)]
#[allow(dead_code)]
pub enum PathFuture {
    /// A single unit of the path identified by its unique ID.
    PathUnit(u64),
    /// A community splicing task that involves merging source and destination nodes.
    PathSplicing(CommunitySplicingTask),
}

#[allow(dead_code)]
impl CSRSimpleCommBlock {
    /// Finds the shortest path between the source and destination nodes using Breadth-First Search (BFS).
    ///
    /// This function performs a BFS starting from the `src_id` and explores neighbors until it reaches
    /// the `dst_id`. It keeps track of visited nodes and their predecessors to reconstruct the path if found.
    ///
    /// # Arguments
    /// - `src_id`: The ID of the source vertex.
    /// - `dst_id`: The ID of the destination vertex.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the nodes in the shortest path from source to destination.
    /// If no path exists, it returns an empty vector.

    pub fn path(&self, src_id: &u64, dst_id: &u64) -> Vec<u64> {
        // Queue to hold vertices to explore in BFS order
        let mut queue = VecDeque::new();
        // Set to track visited vertices to prevent revisiting
        let mut visited = HashSet::new();
        // Map to store the predecessor of each vertex for path reconstruction
        let mut predecessor = HashMap::new();

        // Start BFS from the source vertex
        queue.push_back(*src_id);
        visited.insert(*src_id);

        // Perform BFS
        while let Some(current) = queue.pop_front() {
            // If the destination vertex is found, reconstruct and return the path
            if current == *dst_id {
                return self.reconstruct_path(&predecessor, src_id, dst_id);
            }

            // Explore neighbors of the current vertex
            for neighbor in self.read_neighbor(&current) {
                // If the neighbor has not been visited, visit it
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    predecessor.insert(neighbor, current); // Track the predecessor
                    queue.push_back(neighbor); // Add to the queue for further exploration
                }
            }
        }

        // If no path is found, return an empty vector
        vec![]
    }

    /// Reconstructs the path from the source node to the destination node using the predecessor map.
    ///
    /// This function traces back from the destination node to the source node using the `predecessor` map,
    /// and then reverses the path to return it in the correct order (from source to destination).
    ///
    /// # Arguments
    /// - `predecessor`: A map containing each node's predecessor in the BFS search.
    /// - `src_id`: The ID of the source vertex.
    /// - `dst_id`: The ID of the destination vertex.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the nodes in the path from source to destination, in order.
    fn reconstruct_path(
        &self,
        predecessor: &HashMap<u64, u64>,
        src_id: &u64,
        dst_id: &u64,
    ) -> Vec<u64> {
        // Vector to store the reconstructed path
        let mut path = Vec::new();
        // Start from the destination node
        let mut current = *dst_id;

        // Add the destination node to the path
        path.push(current);

        // Trace the path back to the source node using the predecessor map
        while current != *src_id {
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
        // Reverse the path to get it from source to destination
        path.reverse();
        path
    }
}

#[allow(dead_code)]
impl<'a> EnrichedBoundaryGraph<'a> {
    /// Creates a new instance of `EnrichedBoundaryGraph` with the provided data.
    ///
    /// This constructor function initializes an `EnrichedBoundaryGraph` by setting the source vertex,
    /// destination vertex, source boundary, destination boundary, and a reference to the boundary graph.
    ///
    /// # Arguments
    /// - `src_vertex`: The ID of the source vertex in the graph.
    /// - `dst_vertex`: The ID of the destination vertex in the graph.
    /// - `src_reachable_boundary`: A vector containing the IDs of vertices reachable from the source vertex.
    /// - `dst_source_boundary`: A set containing the IDs of source vertices for the destination vertex.
    /// - `boundary_graph`: A reference to the original `BoundaryGraph` associated with this enriched graph.
    ///
    /// # Returns
    /// A new `EnrichedBoundaryGraph` instance initialized with the provided data.
    pub fn new(
        src_vertex: u64,
        dst_vertex: u64,
        src_reachable_boundary: Vec<u64>,
        dst_source_boundary: HashSet<u64>,
        boundary_graph: &'a BoundaryGraph,
    ) -> Self {
        // Initialize and return a new EnrichedBoundaryGraph with the provided values
        EnrichedBoundaryGraph {
            source_vertex: src_vertex,
            source_neighbors: src_reachable_boundary,
            dest_vertex: dst_vertex,
            dest_source: dst_source_boundary,
            boundary_graph,
        }
    }

    /// Returns the neighbors of a given vertex in the boundary graph.
    ///
    /// The function checks if the vertex has an entry in the boundary graph's adjacency map (`boundary_adj_map`).
    /// If it does, it returns its neighbors accordingly, considering whether the vertex is part of the destination
    /// source set or not. If the vertex is not found in the adjacency map, it checks if the vertex is the source
    /// or destination vertex and returns the appropriate neighbors or an empty vector.
    ///
    /// # Arguments
    /// - `vertex_id`: The ID of the vertex whose neighbors are to be retrieved.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the neighboring vertices.

    pub fn get_neighbors(&self, vertex_id: &u64) -> Vec<u64> {
        // Check if the vertex exists in the boundary graph's adjacency map
        if self.boundary_graph.boundary_adj_map.contains_key(vertex_id) {
            // If the vertex is part of the destination source, add the destination vertex to the neighbors
            if self.dest_source.contains(vertex_id) {
                let mut temp = self
                    .boundary_graph
                    .boundary_adj_map
                    .get(vertex_id)
                    .unwrap()
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                temp.push(self.dest_vertex); // Add destination vertex to the list
                temp
            } else {
                // Otherwise, return the list of neighbors from the adjacency map
                self.boundary_graph
                    .boundary_adj_map
                    .get(vertex_id)
                    .unwrap()
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            }
        } else {
            // If the vertex is not in the adjacency map, check for special cases
            if *vertex_id == self.source_vertex {
                // If it's the source vertex, return its neighbors
                self.source_neighbors.clone()
            } else if *vertex_id == self.dest_vertex {
                // If it's the destination vertex, return an empty list of neighbors
                vec![]
            } else {
                // For other vertices, return an empty list of neighbors
                vec![]
            }
        }
    }

    /// Finds the shortest path between the source and destination vertices using Breadth-First Search (BFS).
    ///
    /// This function performs a BFS starting from the source vertex and explores neighbors until it reaches
    /// the destination vertex. It keeps track of visited nodes and their predecessors to reconstruct the path if found.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the nodes in the shortest path from source to destination.
    /// If no path exists, it returns an empty vector.
    pub fn boundary_path(&self) -> Vec<u64> {
        // If the source and destination vertices are the same, return the source vertex as the path
        if self.source_vertex == self.dest_vertex {
            return vec![self.source_vertex];
        }

        // Queue to hold vertices to explore in BFS order
        let mut queue = VecDeque::new();
        // Set to track visited vertices to prevent revisiting
        let mut visited = HashSet::new();
        // Map to store the predecessor of each vertex for path reconstruction
        let mut predecessor = HashMap::new();

        // Start BFS from the source vertex
        queue.push_back(self.source_vertex);
        visited.insert(self.source_vertex);

        // Perform BFS
        while let Some(current) = queue.pop_front() {
            // If the destination vertex is found, reconstruct and return the path
            if current == self.dest_vertex {
                return self.reconstruct_path(&predecessor);
            }

            // Explore neighbors of the current vertex
            for &neighbor in &self.get_neighbors(&current) {
                // If the neighbor has not been visited, visit it
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    predecessor.insert(neighbor, current); // Track the predecessor
                    queue.push_back(neighbor); // Add to the queue for further exploration
                }
            }
        }

        // If no path is found, return an empty vector
        vec![]
    }

    /// Reconstructs the path from the source vertex to the destination vertex using the predecessor map.
    ///
    /// This function traces the path from the destination vertex to the source vertex by following the
    /// predecessor information stored during the BFS search. It then reverses the path to return it in
    /// the correct order (from source to destination).
    ///
    /// # Arguments
    /// - `predecessor`: A map containing each node's predecessor during the BFS search.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the nodes in the reconstructed path from source to destination.
    fn reconstruct_path(&self, predecessor: &HashMap<u64, u64>) -> Vec<u64> {
        // Vector to store the reconstructed path
        let mut path = Vec::new();
        // Start from the destination vertex
        let mut current = self.dest_vertex;

        // Add the destination vertex to the path
        path.push(current);

        // Trace the path back to the source vertex using the predecessor map
        while current != self.source_vertex {
            match predecessor.get(&current) {
                // If a predecessor exists, move to the predecessor and add it to the path
                Some(&pred) => {
                    current = pred;
                    path.push(current);
                }
                // If no predecessor is found, the path cannot be reconstructed (break the loop)
                None => break,
            }
        }

        // Reverse the path to ensure it's from source to destination
        path.reverse();
        path
    }
}

impl CSRSubGraph<u64, u64, u64> {
    /// Finds a path from source vertex to destination vertex using BFS
    ///
    /// # Arguments
    /// - `src_id`: The source vertex ID
    /// - `dst_id`: The destination vertex ID
    ///
    /// # Returns
    /// A `Vec<u64>` containing the vertex IDs in the path from source to destination
    /// Returns empty vector if no path exists or if vertices don't exist in the graph
    pub fn path(&self, src_id: &u64, dst_id: &u64) -> Vec<u64> {
        // Check if both source and destination vertices exist in this graph
        if !self.has_vertex(src_id) || !self.has_vertex(dst_id) {
            return vec![];
        }

        // If source and destination are the same, return single vertex path
        if src_id == dst_id {
            return vec![*src_id];
        }

        // Queue to hold vertices to explore in BFS order
        let mut queue = VecDeque::new();
        // Set to track visited vertices to prevent revisiting
        let mut visited = HashSet::new();
        // Map to store the predecessor of each vertex for path reconstruction
        let mut predecessor = HashMap::new();

        // Start BFS from the source vertex
        queue.push_back(*src_id);
        visited.insert(*src_id);

        // Perform BFS
        while let Some(current) = queue.pop_front() {
            // If the destination vertex is found, reconstruct and return the path
            if current == *dst_id {
                return self.reconstruct_path(&predecessor, src_id, dst_id);
            }

            // Get neighbors of the current vertex using the GraphQuery trait
            let neighbors = self.read_neighbor(&current);

            // Explore neighbors of the current vertex
            for neighbor in neighbors {
                // Extract the vertex ID from the neighbor
                // Assuming the neighbor type implements some way to get vertex_id
                // Since we don't know the exact structure of V, we'll need to handle this
                let neighbor_id = neighbor; // This assumes V is the same type as T (u64)

                // Check if the neighbor exists in this graph (filter out external vertices)
                if self.has_vertex(&neighbor_id) && !visited.contains(&neighbor_id) {
                    visited.insert(neighbor_id);
                    predecessor.insert(neighbor_id, current); // Track the predecessor
                    queue.push_back(neighbor_id); // Add to the queue for further exploration
                }
            }
        }

        // If no path is found, return an empty vector
        vec![]
    }

    /// Reconstructs the path from the source vertex to the destination vertex using the predecessor map.
    ///
    /// This function traces back from the destination vertex to the source vertex using the `predecessor` map,
    /// and then reverses the path to return it in the correct order (from source to destination).
    ///
    /// # Arguments
    /// - `predecessor`: A map containing each vertex's predecessor in the BFS search.
    /// - `src_id`: The ID of the source vertex.
    /// - `dst_id`: The ID of the destination vertex.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the vertices in the path from source to destination, in order.
    fn reconstruct_path(
        &self,
        predecessor: &HashMap<u64, u64>,
        src_id: &u64,
        dst_id: &u64,
    ) -> Vec<u64> {
        // Vector to store the reconstructed path
        let mut path = Vec::new();
        // Start from the destination vertex
        let mut current = *dst_id;

        // Add the destination vertex to the path
        path.push(current);

        // Trace the path back to the source vertex using the predecessor map
        while current != *src_id {
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

        // Reverse the path to get it from source to destination
        path.reverse();
        path
    }
}

#[allow(dead_code)]
impl PathController {
    /// Finds the reachable boundary vertices for a given vertex in the community graph.
    ///
    /// This function locates the community for a given vertex, loads the boundary vertices for that community,
    /// and filters the boundary vertices to find those that are reachable from the input vertex.
    ///
    /// # Arguments
    /// - `vertex_id`: The ID of the vertex whose reachable boundary vertices are to be found.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the reachable boundary vertices for the given vertex.
    fn find_reachable_boundary(&self, vertex_id: &u64) -> Vec<u64> {
        // Step 1: Locate the community ID for the given vertex.
        let community_id_opt = self
            .storage_engine
            .community_index
            .get_community_id(vertex_id);
        let community_id = match community_id_opt {
            None => {
                panic!("Vertex {} not arranged community.", vertex_id); // If no community is found, panic with an error message.
            }
            Some(community_id) => {
                community_id // If community ID is found, store it.
            }
        };

        // Step 2: Load all boundary vertices for the identified community.
        let boundary_list_all_opt = self
            .storage_engine
            .community_index
            .boundary_graph
            .community_boundary_list
            .get(&community_id); // Unwrap is used to get the boundary list for this community, assuming it's always present.

        let boundary_list_all = match boundary_list_all_opt {
            None => return vec![],
            Some(boundary_list) => boundary_list,
        };
        // Step 3: Load the reference to the community (either normal or giant community).
        let community_ref = self
            .storage_engine
            .load_community_ref(&community_id)
            .unwrap();

        // Step 4: Filter out unreachable boundary vertices based on community reference type.
        match community_ref {
            // If the community is a normal one, perform a BFS to find reachable vertices.
            CommunityItemRef::Normal(comm_csr_block) => {
                let all_reachable_vertex_list = comm_csr_block.inner_bfs(vertex_id); // Perform BFS for reachable vertices.
                all_reachable_vertex_list
                    .into_iter()
                    .filter(|vertex| {
                        boundary_list_all.contains(vertex) // Only keep vertices that are part of the boundary list.
                    })
                    .collect::<Vec<_>>() // Collect the filtered reachable boundary vertices.
            }
            // If the community is a giant one, check for reachability via SCC (Strongly Connected Component) metadata.
            CommunityItemRef::Giant(giant_community_index) => {
                boundary_list_all
                    .iter()
                    .filter(|&boundary| {
                        giant_community_index
                            .scc_meta
                            .is_reachable(vertex_id, boundary) // Check if the boundary is reachable using SCC metadata.
                    })
                    .cloned() // Clone the values since we're working with references.
                    .collect::<Vec<_>>() // Collect the reachable boundaries into a vector.
            }
        }
    }

    /// Finds the boundary vertices that are reachable from the given vertex in its community.
    ///
    /// This function locates the community for the given vertex, loads the boundary vertices for that community,
    /// and filters out the ones that are reachable from the input vertex. The method differs depending on whether
    /// the community is normal or giant, using BFS for normal communities and SCC metadata for giant communities.
    ///
    /// # Arguments
    /// - `vertex_id`: The ID of the vertex whose reachable boundary vertices are to be found.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the IDs of the boundary vertices that are reachable from the given vertex.
    fn find_source_boundary_old(&self, vertex_id: &u64) -> Vec<u64> {
        // Step 1: Locate the community ID for the given vertex.
        let community_id_opt = self
            .storage_engine
            .community_index
            .get_community_id(vertex_id);
        let community_id = match community_id_opt {
            None => {
                panic!("Vertex {} not arranged community.", vertex_id); // Panic if the vertex does not belong to any community.
            }
            Some(community_id) => {
                community_id // If community ID is found, store it.
            }
        };

        // Step 2: Load all boundary vertices for the identified community.
        let boundary_list_all = self
            .storage_engine
            .community_index
            .boundary_graph
            .community_boundary_list
            .get(&community_id)
            .unwrap(); // Unwrap is used assuming the boundary list is always available.

        // Step 3: Load the reference to the community (either normal or giant).
        let community_ref = self
            .storage_engine
            .load_community_ref(&community_id)
            .unwrap();

        // Step 4: Depending on the type of community, filter the reachable boundary vertices.
        match community_ref {
            // If the community is normal, use BFS to check which boundary vertices are reachable from the given vertex.
            CommunityItemRef::Normal(csr_comm_block) => {
                boundary_list_all
                    .iter()
                    .filter(|&boundary| {
                        // Perform BFS for each boundary and check if the vertex is reachable from it.
                        let reachable_list = csr_comm_block
                            .inner_bfs(boundary)
                            .into_iter()
                            .collect::<HashSet<_>>();
                        reachable_list.contains(vertex_id) // Keep the boundary if it's reachable from the vertex.
                    })
                    .cloned() // Clone the references to return owned values.
                    .collect::<Vec<_>>() // Collect the filtered boundaries into a vector.
            }
            // If the community is giant, use SCC (Strongly Connected Component) metadata to check reachability.
            CommunityItemRef::Giant(giant_comm_index) => {
                boundary_list_all
                    .iter()
                    .filter(|&boundary| {
                        // Use SCC metadata to check if the boundary is reachable from the given vertex.
                        giant_comm_index.scc_meta.is_reachable(boundary, vertex_id)
                    })
                    .cloned() // Clone the references to return owned values.
                    .collect::<Vec<_>>() // Collect the reachable boundary vertices into a vector.
            }
        }
    }

    /// Finds all boundary vertices within a community that can reach the specified target vertex.
    ///
    /// This method identifies source boundary vertices that have a path to the given vertex,
    /// using different optimization strategies based on the community type (normal vs giant).
    ///
    /// # Arguments
    /// * `vertex_id` - The target vertex ID to find reachable boundary vertices for
    ///
    /// # Returns
    /// * `Vec<u64>` - A vector of boundary vertex IDs that can reach the target vertex
    ///
    /// # Panics
    /// * If the vertex is not found in any community
    /// * If the community boundary list is not available
    /// * If the community cannot be loaded
    fn find_source_boundary(&self, vertex_id: &u64) -> Vec<u64> {
        // Step 1: Locate the community ID for the given vertex.
        // Each vertex belongs to exactly one community in the partitioned graph.
        let community_id = self
            .storage_engine
            .community_index
            .get_community_id(vertex_id)
            .expect(&format!("Vertex {} not arranged community.", vertex_id));

        // Step 2: Load all boundary vertices for the identified community.
        // Boundary vertices are those that have connections to other communities.
        let boundary_list_all = self
            .storage_engine
            .community_index
            .boundary_graph
            .community_boundary_list
            .get(&community_id)
            .expect("Boundary list should always be available");

        // Step 3: Load the reference to the community (either normal or giant).
        // Communities are stored differently based on their size and structure.
        let community_ref = self
            .storage_engine
            .load_community_ref(&community_id)
            .expect("Community should be loadable");

        // Step 4: Depending on the type of community, filter the reachable boundary vertices.
        // Different algorithms are used for optimal performance based on community characteristics.
        match community_ref {
            // For normal communities, use reverse BFS from the target vertex
            // This approach is efficient for smaller, less complex communities.
            CommunityItemRef::Normal(csr_comm_block) => {
                // Build reverse graph and perform BFS from the target vertex to find all vertices
                // that can reach it (by traversing edges in reverse direction).
                let reverse_reachable = csr_comm_block.reverse_bfs(vertex_id);

                // Convert the result to a hash set for O(1) lookup performance.
                let reverse_reachable_set: FxHashSet<u64> = reverse_reachable.into_iter().collect();

                // Filter boundary vertices that can reach the target vertex.
                // Only include boundaries that appear in the reverse-reachable set.
                boundary_list_all
                    .iter()
                    .filter(|&boundary| reverse_reachable_set.contains(boundary))
                    .cloned()
                    .collect()
            }

            // For giant communities, use SCC-based optimization
            // Large communities benefit from strongly connected component analysis
            // to reduce computational complexity.
            CommunityItemRef::Giant(giant_comm_index) => {
                let scc_meta = &giant_comm_index.scc_meta;

                // 1. Find the SCC (Strongly Connected Component) of the target vertex.
                // Vertices in the same SCC can all reach each other.
                let target_scc = scc_meta
                    .find_vertex_scc(vertex_id)
                    .expect("Vertex should belong to an SCC");

                // 2. Build a mapping from each boundary vertex to its SCC ID.
                // This allows us to work at the SCC level rather than individual vertex level.
                let boundary_to_scc: FxHashMap<u64, u64> = boundary_list_all
                    .par_iter() // Use parallel iteration for performance
                    .map(|&vertex| (vertex, scc_meta.find_vertex_scc(&vertex).unwrap()))
                    .collect();

                // 3. Build reverse mapping from SCC ID to all boundary vertices in that SCC.
                // This groups boundary vertices by their strongly connected component.
                let mut scc_to_boundaries: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
                for (&vertex, &scc_id) in &boundary_to_scc {
                    scc_to_boundaries.entry(scc_id).or_default().push(vertex);
                }

                // 4. Get unique SCCs that contain boundary vertices.
                // We only need to check reachability for SCCs that actually have boundaries.
                let unique_boundary_sccs: FxHashSet<u64> =
                    boundary_to_scc.values().cloned().collect();

                // 5. Set up cache for SCC reachability computations.
                // Caching prevents redundant reachability calculations for the same SCC pairs.
                let mut scc_reachable_cache = FxHashMap::<u64, Arc<FxHashSet<u64>>>::default();

                // 6. Find all boundary vertices that can reach the target vertex.
                let mut source_boundaries = Vec::new();

                // Create a set containing only the target SCC for reachability queries.
                let mut target_scc_set: FxHashSet<u64> = FxHashSet::default();
                target_scc_set.insert(target_scc);

                // Process each SCC that contains boundary vertices.
                for &src_scc in &unique_boundary_sccs {
                    // Use the existing cached method to compute which SCCs are reachable
                    // from the current source SCC. This leverages SCC-level reachability
                    // which is much more efficient than vertex-level for large graphs.
                    let reachable_sccs = scc_meta.compute_reachable_sccs_with_cache(
                        src_scc,
                        &target_scc_set,
                        &mut scc_reachable_cache,
                    );

                    // Check if this source SCC can reach the target SCC.
                    if reachable_sccs.contains(&target_scc) {
                        // Add all boundary vertices from this reachable SCC to the result.
                        // Since the SCC can reach the target, all vertices in it can too.
                        if let Some(boundaries) = scc_to_boundaries.get(&src_scc) {
                            source_boundaries.extend(boundaries.iter().cloned());
                        }
                    }
                }

                source_boundaries
            }
        }
    }

    /// Builds an enriched view of the boundary graph for a given source and destination vertex.
    ///
    /// This function finds the reachable boundaries for the source and destination vertices within their respective
    /// communities and then constructs an `EnrichedBoundaryGraph` containing the relevant information for the path analysis.
    ///
    /// # Arguments
    /// - `source_vertex`: The ID of the source vertex.
    /// - `destination_vertex`: The ID of the destination vertex.
    ///
    /// # Returns
    /// An `EnrichedBoundaryGraph` that contains the enriched boundary graph data for path analysis.
    fn build_path_community_view(
        &self,
        source_vertex: &u64,
        destination_vertex: &u64,
    ) -> EnrichedBoundaryGraph<'_> {
        // Step 1: Find the reachable boundary vertices for the source and destination communities.
        // For the source vertex, find the boundary vertices reachable from it.
        let src_reachable_boundary_list = self.find_reachable_boundary(source_vertex);
        // For the destination vertex, find the boundary vertices that can reach it.
        let dst_reachable_boundary_list = self.find_source_boundary(destination_vertex);

        // Step 2: Create and return a new EnrichedBoundaryGraph using the found data.
        // The graph is enriched with source and destination boundary vertices along with other relevant information.
        EnrichedBoundaryGraph::new(
            *source_vertex,              // Source vertex
            *destination_vertex,         // Destination vertex
            src_reachable_boundary_list, // Reachable boundaries for the source
            dst_reachable_boundary_list
                .into_iter()
                .collect::<HashSet<_>>(), // Reachable boundaries for the destination (converted to HashSet)
            &self.storage_engine.community_index.boundary_graph, // Reference to the boundary graph
        )
    }

    /// Generates a sequence of path-related tasks for splicing operations along a community boundary path.
    ///
    /// This function creates a list of `PathFuture` tasks based on a given community boundary path. The tasks are either
    /// `PathUnit` representing individual boundary units or `PathSplicing` representing splicing operations between
    /// consecutive boundary vertices that belong to the same community.
    ///
    /// # Arguments
    /// - `community_boundary_path`: A vector of boundary vertex IDs representing the path along the community boundaries.
    ///
    /// # Returns
    /// A `Vec<PathFuture>` containing a sequence of tasks (either `PathUnit` or `PathSplicing`) based on the community boundary path.
    fn generate_splicing_task(&self, community_boundary_path: &Vec<u64>) -> Vec<PathFuture> {
        let mut res = vec![]; // The result vector to store the generated tasks.

        // Step 1: Retrieve the community ID of the first boundary vertex in the path.
        let mut current_community_id = self
            .storage_engine
            .community_index
            .get_community_id(&community_boundary_path[0])
            .unwrap();

        // Add the first boundary vertex as a PathUnit task to the result vector.
        res.push(PathUnit(community_boundary_path[0]));

        // Step 2: Iterate through the remaining boundary vertices in the path.
        for i in 1..community_boundary_path.len() {
            // Retrieve the community ID of the next boundary vertex in the path.
            let next_community_id = self
                .storage_engine
                .community_index
                .get_community_id(&community_boundary_path[i])
                .unwrap();

            // If the current and next boundary vertex belong to the same community, create a splicing task.
            if current_community_id == next_community_id {
                res.push(PathSplicing(CommunitySplicingTask {
                    community_id: current_community_id,
                    src: community_boundary_path[i - 1], // The source vertex for splicing.
                    dst: community_boundary_path[i],     // The destination vertex for splicing.
                }));
            } else {
                // If the community changes, update the current community ID.
                current_community_id = next_community_id;
            }

            // Add the next boundary vertex as a PathUnit task to the result vector.
            res.push(PathUnit(community_boundary_path[i]));
        }

        // Return the sequence of tasks (PathUnit and PathSplicing) generated from the boundary path.
        res
    }

    /// Fills the path between two vertices (source and destination) in a community graph.
    ///
    /// This function retrieves the appropriate community reference based on the community ID from the `community_splicing_task`.
    /// Depending on whether the community is normal or giant, it calculates the path between the source and destination vertices.
    /// It then returns the path, excluding the source and destination vertices.
    ///
    /// # Arguments
    /// - `community_splicing_task`: The task containing the community ID and the source and destination vertices.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the vertices in the path between the source and destination, excluding the source and destination.
    fn fill_path(&self, community_splicing_task: &CommunitySplicingTask) -> Vec<u64> {
        // Load the community reference based on the community ID from the splicing task.
        let community_ref = self
            .storage_engine
            .load_community_ref(&community_splicing_task.community_id)
            .unwrap();

        // Match on the type of the community reference (Normal or Giant).
        match community_ref {
            // If the community is normal, find the path using the normal community block.
            CommunityItemRef::Normal(csr_comm_block) => {
                // Get the path from the source to the destination vertex.
                let path =
                    csr_comm_block.path(&community_splicing_task.src, &community_splicing_task.dst);
                // Return the path excluding the first and last vertices (source and destination).
                path[1..path.len() - 1].to_vec()
            }
            // If the community is giant, find the path using the giant community's method.
            CommunityItemRef::Giant(giant_comm_index) => {
                // Get the path for the giant community between source and destination.
                let path = self.fill_path_giant_scc(
                    &community_splicing_task.community_id,
                    giant_comm_index,
                    &community_splicing_task.src,
                    &community_splicing_task.dst,
                );
                // Return the path excluding the first and last vertices (source and destination).
                path[1..path.len() - 1].to_vec()
            }
        }
    }

    /// Finds a path between two vertices within a giant community using SCC-based pathfinding
    ///
    /// This method handles pathfinding in large communities by leveraging the Strongly Connected
    /// Component (SCC) decomposition. It first finds a path through the SCC DAG, then constructs
    /// a subgraph containing only the relevant SCCs to perform the actual vertex-level pathfinding.
    ///
    /// # Arguments
    ///
    /// * `community_id` - Identifier of the giant community containing the vertices
    /// * `giant_community_index` - Shared reference to the giant community's SCC metadata
    /// * `src_id` - Source vertex identifier for pathfinding
    /// * `dst_id` - Destination vertex identifier for pathfinding
    ///
    /// # Returns
    ///
    /// * `Vec<u64>` - Path as a sequence of vertex IDs from source to destination
    /// * Empty vector if no path exists or if the community is not giant
    ///
    /// # Algorithm
    ///
    /// 1. Validates that the specified community is indeed a giant community
    /// 2. Finds a path through the SCC DAG using condensed graph representation
    /// 3. Loads relevant SCCs from storage (with caching for performance)
    /// 4. Constructs a temporary CSR subgraph containing vertices from path SCCs
    /// 5. Performs standard pathfinding on the constructed subgraph
    ///
    /// # Performance Optimizations
    ///
    /// - Uses SCC-based decomposition to reduce search space
    /// - Employs LRU caching for frequently accessed SCC blocks
    /// - Constructs minimal subgraph containing only path-relevant vertices
    pub fn fill_path_giant_scc(
        &self,
        community_id: &u32,
        giant_community_index: Arc<GiantCommunityIndex>,
        src_id: &u64,
        dst_id: &u64,
    ) -> Vec<u64> {
        // Validate community exists and retrieve giant community metadata
        let community_index_item_opt = self
            .storage_engine
            .community_index
            .get_community_location(community_id);
        let (length, scc_index_offset) = match community_index_item_opt {
            None => return vec![], // Community not found
            Some(community_index_item) => {
                match community_index_item {
                    CommunityIndexItem::Normal { .. } => {
                        // Incorrect community type - expected giant community
                        return vec![];
                    }
                    CommunityIndexItem::Giant {
                        length,
                        scc_index_offset,
                    } => (length, scc_index_offset),
                }
            }
        };

        // Find path through SCC DAG using condensed graph representation
        let scc_path = giant_community_index.scc_meta.path_scc_dag(src_id, dst_id);

        if scc_path.is_empty() {
            return vec![]; // No path exists between source and destination SCCs
        }

        // Initialize data structures for constructing SCC path subgraph
        let mut scc_path_vertex_count = 0u64;
        let mut scc_path_edge_count = 0u64;
        let mut scc_path_vertex_list = vec![];
        let mut scc_path_edge_list = vec![];
        let mut scc_path_vertex_index = HashMap::<u64, usize>::new();

        // Load and process each SCC on the path
        scc_path.into_iter().for_each(|scc_id| {
            // Calculate SCC data location and size within giant community storage
            let scc_start = giant_community_index.scc_index.0[scc_id as usize] as usize;
            let scc_length = if scc_id + 1 == giant_community_index.scc_meta.scc_list.len() as u64 {
                length - scc_start // Last SCC extends to end of community data
            } else {
                giant_community_index.scc_index.0[(scc_id + 1) as usize] as usize - scc_start
            };

            // Attempt to load SCC from cache, read from storage if cache miss
            let target_scc_csr = match self.storage_engine.scc_cache.get(&(*community_id, scc_id)) {
                None => {
                    // Cache miss: load SCC data from giant community file
                    let csr_comm_block = self.storage_engine.read_csr_from_giant(
                        community_id,
                        scc_index_offset + scc_start,
                        scc_length,
                    );
                    let csr_comm_block_arc = Arc::new(csr_comm_block);
                    self.storage_engine
                        .scc_cache
                        .insert((*community_id, scc_id), csr_comm_block_arc.clone());
                    csr_comm_block_arc
                }
                // Cache hit: use cached SCC data
                Some(csr_scc_block_arc) => csr_scc_block_arc.clone(),
            };

            // Add all vertices and edges from this SCC to the path subgraph
            for (vertex, _) in &target_scc_csr.vertex_list {
                let neighbor_list = target_scc_csr
                    .read_neighbor(&vertex)
                    .into_iter()
                    .map(|v| v)
                    .collect::<Vec<u64>>();

                // Record vertex index mapping for CSR construction
                let offset = scc_path_edge_count;
                scc_path_vertex_index.insert(*vertex, scc_path_vertex_count as usize);
                scc_path_vertex_count += 1;

                // Add vertex with its edge offset to vertex list
                scc_path_vertex_list.push((*vertex, offset));

                // Add all neighbors to edge list and update edge count
                scc_path_edge_count += neighbor_list.len() as u64;
                scc_path_edge_list.extend_from_slice(&neighbor_list);
            }
        });

        // Construct CSR subgraph containing only vertices from SCCs on the path
        let scc_path_csr = CSRSubGraph {
            vertex_count: scc_path_vertex_count,
            vertex_list: scc_path_vertex_list,
            neighbor_list: scc_path_edge_list,
            vertex_index: scc_path_vertex_index,
        };

        // Perform standard pathfinding on the constructed subgraph
        scc_path_csr.path(src_id, dst_id)
    }

    /// Fills the path between two vertices (source and destination) in a giant community graph using BFS.
    ///
    /// This function performs a Breadth-First Search (BFS) to find the shortest path between the source vertex (`src_id`)
    /// and the destination vertex (`dst_id`) in a giant community. It tracks predecessors during the search to reconstruct
    /// the path once the destination is found.
    ///
    /// # Arguments
    /// - `src_id`: The ID of the source vertex.
    /// - `dst_id`: The ID of the destination vertex.
    ///
    /// # Returns
    /// A `Vec<u64>` containing the vertices in the path from the source to the destination.
    /// If no path is found, it returns an empty vector.
    fn fill_path_giant(&self, src_id: &u64, dst_id: &u64) -> Vec<u64> {
        // Helper function to reconstruct the path from the source to the destination.
        // This function uses the predecessor map to backtrack from the destination to the source.
        fn reconstruct_path(
            predecessor: &HashMap<u64, u64>,
            src_id: &u64,
            dst_id: &u64,
        ) -> Vec<u64> {
            let mut path = Vec::new();
            let mut current = *dst_id;

            // Start from the destination and backtrack to the source
            path.push(current);

            // Backtrack using the predecessor map until the source is reached
            while current != *src_id {
                match predecessor.get(&current) {
                    Some(&pred) => {
                        current = pred;
                        path.push(current);
                    }
                    None => break, // No path exists if there is no predecessor
                }
            }
            // Reverse the path so that it goes from source to destination
            path.reverse();
            path
        }

        // Initialize BFS structures
        let mut queue = VecDeque::new(); // Queue for BFS exploration
        let mut visited = HashSet::new(); // Set to track visited vertices
        let mut predecessor = HashMap::new(); // Map to track the predecessor of each vertex

        // Start BFS from the source vertex
        queue.push_back(*src_id);
        visited.insert(*src_id);

        // Perform BFS to find the shortest path to the destination vertex
        while let Some(current) = queue.pop_front() {
            // If the destination vertex is found, reconstruct and return the path
            if current == *dst_id {
                return reconstruct_path(&predecessor, src_id, dst_id);
            }

            // Explore neighbors of the current vertex
            for neighbor in self.storage_engine.read_neighbor(&current) {
                // If the neighbor has not been visited, visit it
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    predecessor.insert(neighbor, current); // Track the predecessor for path reconstruction
                    queue.push_back(neighbor); // Add the neighbor to the queue for further exploration
                }
            }
        }

        // If no path is found, return an empty vector
        vec![]
    }

    /// Finds the path between two vertices (source and destination) through community splicing.
    ///
    /// This function constructs a path from a source vertex to a destination vertex by first building an enriched community
    /// view, then generating a community boundary path, followed by splicing tasks for the community. Finally, it performs
    /// parallelized splicing tasks to generate the final path.
    ///
    /// # Arguments
    /// - `source_vertex`: The ID of the source vertex.
    /// - `destination_vertex`: The ID of the destination vertex.
    /// - `path_config`: Configuration settings for the path generation, including the number of threads to use.
    ///
    /// # Returns
    /// A `Vec<u64>` representing the path from the source vertex to the destination vertex.
    pub fn path(
        &self,
        source_vertex: &u64,
        destination_vertex: &u64,
        path_config: PathConfig,
    ) -> Vec<u64> {
        // Step 1: Acquire the enriched community view for the source and destination vertices.
        // This builds an enriched boundary graph view that includes necessary data for path generation.
        let enriched_community_view =
            self.build_path_community_view(source_vertex, destination_vertex);

        // Step 2: Acquire the community path (boundary path).
        // The boundary path represents a sequence of boundary vertices that connect the source and destination.
        let community_boundary_path = enriched_community_view.boundary_path();

        if community_boundary_path.len() == 0 {
            return vec![];
        }

        // Step 3: Generate the splicing task for the community path.
        // This step generates a list of splicing tasks based on the boundary path.
        let path_future_list = self.generate_splicing_task(&community_boundary_path);

        // Step 4: Perform the splicing tasks in parallel using a thread pool.
        // Set the number of threads based on the path configuration (`path_config.thread_num`).
        let thread_num = path_config.thread_num;
        let path_map = DashMap::<u64, Vec<u64>>::new(); // Map to store the filled paths for each community boundary.

        // Create a thread pool to handle splicing tasks in parallel.
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_num)
            .build()
            .unwrap();

        // Execute splicing tasks in parallel using the thread pool.
        pool.install(|| {
            path_future_list.par_iter().for_each(|path_future| {
                match path_future {
                    // If it's a PathUnit (a single boundary vertex), do nothing (just a placeholder).
                    PathUnit(_) => {
                        // Do nothing.
                    }
                    // If it's a PathSplicing task, fill the path for the community splicing task.
                    PathSplicing(community_splicing_task) => {
                        let filled_path = self.fill_path(community_splicing_task);
                        path_map.insert(community_splicing_task.src, filled_path);
                        // Store the filled path in the map.
                    }
                }
            })
        });

        // Step 5: Generate the full path by combining all the spliced parts.
        // Iterate through the generated path tasks and combine the results into a complete path.
        let mut res = vec![];
        path_future_list.iter().for_each(|path_future| {
            match path_future {
                // For PathUnit, just add the vertex to the result path.
                PathUnit(src) => {
                    res.push(*src);
                }
                // For PathSplicing, extend the result path with the filled path.
                PathSplicing(comm_splicing_task) => {
                    let filled_path_ref = path_map.get(&comm_splicing_task.src).unwrap();
                    res.extend_from_slice(filled_path_ref.value());
                }
            }
        });

        // Return the complete path.
        res
    }

    /// Creates a new instance of the struct, initializing it with the given storage engine.
    ///
    /// This constructor function takes an `Arc<CommunityStorage>` (which allows for thread-safe,
    /// shared ownership of the `CommunityStorage` instance) and initializes the struct with it.
    ///
    /// # Arguments
    /// - `storage_engine`: An `Arc<CommunityStorage>` that provides access to community data.
    ///
    /// # Returns
    /// A new instance of the struct with the provided `storage_engine`.
    pub fn new(storage_engine: Arc<CommunityStorage>) -> Self {
        // Initialize the struct with the given storage engine
        Self { storage_engine }
    }
}
