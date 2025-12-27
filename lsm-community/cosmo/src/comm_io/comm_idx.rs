use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CommunityIndexItem {
    /// Represents a normal-sized community in the graph
    ///
    /// Contains information about where to find this community's data
    /// in the underlying storage structure.
    Normal {
        offset: usize, // The starting position of this community in the data array
        length: usize, // The number of elements in this community
    },
    /// Represents a giant (exceptionally large) community in the graph
    ///
    /// Giant communities are stored with a special indexing structure
    /// for more efficient access and processing.
    Giant {
        scc_index_offset: usize, // The starting position of the strongly connected component index
        length: usize,           // The total number of elements in this giant community
    },
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct BoundaryCSR {
    pub(crate) boundary_count: u64,
    pub(crate) offset_list: Vec<u64>,
    pub(crate) edge_list: Vec<u64>,
    pub(crate) vertex_mapper: Vec<u64>,
    pub(crate) vertex_mapper_reverse: FxHashMap<u64, u64>,
}

#[allow(dead_code)]
impl BoundaryCSR {
    pub fn build_from_boundary_adj(boundary_adj: &FxHashMap<u64, FxHashSet<u64>>) -> Self {
        // Collect all unique vertex IDs (both as sources and targets)
        let mut all_vertices = FxHashSet::default();
        for (src, neighbor_list) in boundary_adj {
            all_vertices.insert(*src);
            for dest in neighbor_list {
                all_vertices.insert(*dest);
            }
        }

        // Convert to sorted vector for consistent ordering
        let mut vertex_mapper: Vec<u64> = all_vertices.into_iter().collect();
        vertex_mapper.sort_unstable();

        // Build reverse mapping from global ID to local ID
        let mut vertex_mapper_reverse = FxHashMap::default();
        for (local_id, &global_id) in vertex_mapper.iter().enumerate() {
            vertex_mapper_reverse.insert(global_id, local_id as u64);
        }

        let boundary_count = vertex_mapper.len() as u64;

        // Initialize offset_list with n+1 elements
        let mut offset_list = vec![0; boundary_count as usize + 1];
        let mut edge_list = Vec::new();

        // Build CSR structure
        for (local_id, &global_id) in vertex_mapper.iter().enumerate() {
            // Get neighbors for this vertex (if it exists in the adjacency map)
            if let Some(neighbors) = boundary_adj.get(&global_id) {
                // Convert neighbor global IDs to local IDs and sort them
                let mut local_neighbors: Vec<u64> = neighbors
                    .iter()
                    .map(|&neighbor_global_id| {
                        *vertex_mapper_reverse
                            .get(&neighbor_global_id)
                            .expect("Neighbor should exist in vertex_mapper_reverse")
                    })
                    .collect();
                local_neighbors.sort_unstable();

                // Add edges to edge_list
                edge_list.extend(local_neighbors);
            }

            // Update offset for next vertex
            offset_list[local_id + 1] = edge_list.len() as u64;
        }

        Self {
            boundary_count,
            offset_list,
            edge_list,
            vertex_mapper,
            vertex_mapper_reverse,
        }
    }
}

/// The graph constructed from boundary vertices between communities.
/// Used to accelerate query processing by optimizing cross-community traversals.
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct BoundaryGraph {
    /// Maps each vertex to its community identifier.
    /// Each vertex belongs to exactly one community.
    pub vertex_community_map: Vec<u32>,

    /// Adjacency map that contains only boundary vertices.
    /// Maps a vertex ID to the set of adjacent boundary vertices.
    /// This reduces the graph size for faster traversal between communities.
    pub boundary_adj_map: FxHashMap<u64, FxHashSet<u64>>,

    /// The CSR format of the boundary graph for fast computing.
    pub boundary_csr: BoundaryCSR,

    /// Records all boundary edges between each pair of communities.
    /// Keys are (community_id1, community_id2) pairs.
    /// Values are sets of (vertex_id1, vertex_id2) pairs representing boundary edges.
    pub community_boundary: FxHashMap<(u32, u32), FxHashSet<(u64, u64)>>,

    /// Record the boundary list of each community.
    /// Keys are community_id.
    /// Values are sets of vertex id representing each boundary.
    pub community_boundary_list: FxHashMap<u32, FxHashSet<u64>>,
}

/// A comprehensive index structure for community-based graph partitioning.
/// Provides efficient access to both community data and boundary information
/// for optimized graph traversal and query processing.
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommunityIndex {
    /// Maps community identifiers to their corresponding index entries.
    /// Each entry contains location and size information for the community,
    /// with different handling for normal and giant communities.
    pub community_map: FxHashMap<u32, CommunityIndexItem>,

    /// Contains the boundary information between communities.
    /// Enables efficient cross-community traversal by focusing only on
    /// boundary vertices and edges rather than the entire graph.
    pub boundary_graph: BoundaryGraph,
}

#[allow(dead_code)]
impl CommunityIndex {
    /// Retrieves the location information for a specific community.
    ///
    /// Given a community identifier, this function looks up and returns
    /// the corresponding index item that contains storage information
    /// for that community's data.
    ///
    /// # Arguments
    ///
    /// * `community_id` - The identifier of the community to locate
    ///
    /// # Returns
    ///
    /// * `Some(CommunityIndexItem)` - If the community exists, returns its location information
    /// * `None` - If the community does not exist in the index
    pub fn get_community_location(&self, community_id: &u32) -> Option<CommunityIndexItem> {
        match self.community_map.get(&community_id) {
            None => None,
            Some(&community_index_item) => Some(community_index_item.clone()),
        }
    }

    /// Determines if a vertex is a boundary vertex.
    ///
    /// Boundary vertices are those that connect different communities.
    /// This function checks if the given vertex ID exists in the boundary
    /// adjacency map, which only stores vertices that are at community boundaries.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The identifier of the vertex to check
    ///
    /// # Returns
    ///
    /// * `true` - If the vertex is a boundary vertex (connects different communities)
    /// * `false` - If the vertex is internal to a community (not a boundary vertex)
    pub fn is_boundary(&self, vertex_id: &u64) -> bool {
        self.boundary_graph.boundary_adj_map.contains_key(vertex_id)
    }

    /// Retrieves all neighboring boundary vertices for a given boundary vertex.
    ///
    /// For a specified boundary vertex, this function returns all other boundary
    /// vertices that are directly connected to it. This information is useful
    /// for traversing between communities through their boundary connections.
    ///
    /// # Arguments
    ///
    /// * `boundary_id` - The identifier of the boundary vertex to find neighbors for
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u64>)` - If the vertex is a boundary vertex, returns a vector of
    ///                      all neighboring boundary vertex identifiers
    /// * `None` - If the vertex is not a boundary vertex or does not exist
    pub fn get_neighbor_boundaries(&self, boundary_id: &u64) -> Option<Vec<u64>> {
        match self.boundary_graph.boundary_adj_map.get(boundary_id) {
            None => None,
            Some(neighbor_boundary_set) => {
                Some(neighbor_boundary_set.iter().cloned().collect::<Vec<_>>())
            }
        }
    }

    /// Retrieves the community identifier for a given vertex.
    ///
    /// Maps a vertex to its containing community using the vertex-to-community mapping.
    /// Performs bounds checking to ensure the vertex ID is valid within the graph's range.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The identifier of the vertex to lookup
    ///
    /// # Returns
    ///
    /// * `Some(u32)` - If the vertex exists, returns the identifier of its containing community
    /// * `None` - If the vertex ID is out of range or otherwise invalid
    pub fn get_community_id(&self, vertex_id: &u64) -> Option<u32> {
        if *vertex_id > self.boundary_graph.vertex_community_map.len() as u64 {
            None
        } else {
            Some(self.boundary_graph.vertex_community_map[*vertex_id as usize])
        }
    }

    /// Retrieves the community index item for a given vertex.
    ///
    /// Maps a vertex to its corresponding community item by looking up its community ID in the vertex-to-community mapping,
    /// and then retrieving the community details from the community map. This function handles the case where no community
    /// item is found for the given vertex, logging a message and returning `None`.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The identifier of the vertex to look up
    ///
    /// # Returns
    ///
    /// * `Some(CommunityIndexItem)` - If a valid community item is found for the vertex, returns the corresponding community index item
    /// * `None` - If the vertex ID does not map to a valid community item or the community item is not found in the community map
    pub fn get_vertex_community(&self, vertex_id: &u64) -> Option<(u32, CommunityIndexItem)> {
        let community_id = self.boundary_graph.vertex_community_map[*vertex_id as usize];
        let community_item_opt = self.community_map.get(&community_id);
        match community_item_opt {
            None => {
                println!("No community item found for {}", vertex_id);
                None
            }
            Some(community_item) => Some((community_id, community_item.clone())),
        }
    }
}
