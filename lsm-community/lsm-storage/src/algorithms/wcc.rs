use rayon::prelude::*;

use crate::{LsmCommunity, types::VId};

impl LsmCommunity {
    /// Compute Weakly Connected Components using Union-Find.
    ///
    /// In a directed graph, WCC treats all edges as undirected.
    /// This implementation uses parallel edge collection followed by
    /// sequential Union-Find algorithm.
    ///
    /// # Returns
    ///
    /// Returns a vector where `result[vertex_id]` contains the component ID
    /// that the vertex belongs to. Vertices with the same component ID are
    /// in the same weakly connected component.
    ///
    /// # Performance
    ///
    /// - Time: O(V + E * α(V)) where α is the inverse Ackermann function
    /// - Space: O(V + E)
    pub fn wcc(&self) -> Vec<VId> {
        let vertex_count = self.vertex_count();
        let vertex_index_state = self.vertex_index.read();
        let all_edges: Vec<(VId, VId)> = (0..vertex_count as VId)
            .into_par_iter()
            .flat_map(|vid| {
                let mut edges = Vec::new();

                if let Ok((Some(neighbors), _)) =
                    self.read_neighbor_hold_index_vertex(vid, false, &vertex_index_state)
                {
                    for neighbor in neighbors {
                        // Add both directions for undirected treatment
                        edges.push((vid, neighbor));
                        edges.push((neighbor, vid));
                    }
                }

                edges
            })
            .collect();

        let mut parent: Vec<VId> = (0..vertex_count as VId).collect();
        let mut rank = vec![0u32; vertex_count as usize];

        // Find with path compression
        fn find(parent: &mut [VId], mut x: VId) -> VId {
            while parent[x as usize] != x {
                let next = parent[x as usize];
                parent[x as usize] = parent[next as usize];
                x = next;
            }
            x
        }

        // Union by rank
        fn union(parent: &mut [VId], rank: &mut [u32], x: VId, y: VId) {
            let root_x = find(parent, x);
            let root_y = find(parent, y);

            if root_x != root_y {
                if rank[root_x as usize] < rank[root_y as usize] {
                    parent[root_x as usize] = root_y;
                } else if rank[root_x as usize] > rank[root_y as usize] {
                    parent[root_y as usize] = root_x;
                } else {
                    parent[root_y as usize] = root_x;
                    rank[root_x as usize] += 1;
                }
            }
        }

        // Execute union for all edges
        for (u, v) in all_edges {
            union(&mut parent, &mut rank, u, v);
        }

        // Step 3: Build final result with path compression
        let mut result = vec![0; vertex_count as usize];
        for vid in 0..vertex_count as VId {
            result[vid as usize] = find(&mut parent, vid);
        }
        result
    }
}
