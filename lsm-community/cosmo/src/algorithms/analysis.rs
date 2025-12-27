use crate::comm_io::CommunityStorage;

/// Calculates the number of boundary nodes in a community graph
///
/// This function analyzes the community boundary structure by counting
/// the number of entries in the boundary adjacency map.
///
/// # Arguments
///
/// * `storage_engine` - A reference to the CommunityStorage containing the community index data
///
/// # Returns
///
/// * `u64` - The number of boundary nodes in the community graph
pub fn boundary_analysis(storage_engine: &CommunityStorage) -> u64 {
    storage_engine
        .community_index
        .boundary_graph
        .boundary_adj_map
        .len() as u64
}
