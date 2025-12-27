use crate::{LsmCommunity, types::VId};

impl LsmCommunity {
    /// Returns the pre-computed community structure of the graph.
    ///
    /// Communities are groups of vertices that were detected during graph loading,
    /// typically based on the community IDs present in the input graph file.
    /// This method provides access to the static community partitioning.
    ///
    /// # Returns
    ///
    /// A vector of communities, where:
    /// - Each inner `Vec<VId>` represents one community
    /// - Contains all vertex IDs that belong to that community
    /// - Communities are indexed by their position in the outer vector
    pub fn community_detection(&self) -> Vec<Vec<VId>> {
        let vertex_index_state = self.vertex_index.read();
        vertex_index_state.community_list.clone()
    }

    /// Finds the community that a given vertex belongs to.
    ///
    /// This method looks up the community membership of a vertex and returns
    /// all vertices in that same community. The community structure is based
    /// on the pre-computed partitioning from graph loading.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID to search for
    ///
    /// # Returns
    ///
    /// - `Some(Vec<VId>)`: A vector containing all vertex IDs in the same community,
    ///   including the query vertex itself
    /// - `None`: If the vertex_id is invalid (>= vertex count)
    ///
    /// # Performance
    ///
    /// - Time: O(1) for lookup + O(C) for cloning the community vector,
    ///   where C is the size of the community
    /// - Space: O(C) for the returned vector
    pub fn community_search(&self, vertex_id: VId) -> Option<Vec<VId>> {
        if vertex_id >= self.vertex_count() {
            None
        } else {
            let vertex_index_state = self.vertex_index.read();
            // Determine the community ID
            let comm_id = vertex_index_state.community_map[vertex_id as usize];
            Some(
                vertex_index_state
                    .community_list
                    .get(comm_id as usize)
                    .cloned()
                    .unwrap(),
            )
        }
    }
}
