use crate::types::{EdgeList, VId, VIdList, VirtualCommId};
use anyhow::{Ok, Result};
use crossbeam_skiplist::SkipMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};

/// In-memory graph structure using skip list for concurrent access.
///
/// MemGraph stores graph vertices and their edges in memory, similar to a MemTable
/// in LSM-Tree. It uses a skip list for efficient concurrent reads and writes, with
/// values wrapped in Arc<RwLock<>> to allow modification of existing entries.
pub struct MemGraph {
    /// Skip list mapping vertex IDs to their neighbors
    pub map: Arc<SkipMap<VId, Arc<RwLock<VIdList>>>>,
    /// Approximate size of the graph in bytes
    approximate_size: Arc<AtomicUsize>,
    /// Virtual community ID this MemGraph belongs to
    virtual_id: VirtualCommId,
}

impl MemGraph {
    /// Creates a new empty MemGraph for the given virtual community ID.
    ///
    /// # Arguments
    ///
    /// * `virtual_id` - The virtual community ID this MemGraph belongs to
    pub fn new(virtual_id: VirtualCommId) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
            virtual_id,
        }
    }

    /// Returns the virtual community ID of this MemGraph.
    pub fn virtual_id(&self) -> VirtualCommId {
        self.virtual_id
    }

    // Get neighbor for MemGraph.
    pub fn get_neighbor_iter(&self, vertex_id: VId) -> impl Iterator<Item = VId> {
        self.map
            .get(&vertex_id)
            .map(|entry| {
                let neighbors = entry.value().read().unwrap();
                neighbors.clone()
            })
            .unwrap_or_else(Vec::new)
            .into_iter()
    }

    /// Returns the approximate size of the MemGraph in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Put a vertex into this MemGraph.
    ///
    /// Creates a new vertex with an empty edge list if it doesn't exist.
    /// If the vertex already exists, this operation does nothing.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID to insert
    pub fn put_vertex(&self, vertex_id: VId) -> Result<()> {
        // Check if vertex already exists
        if self.map.contains_key(&vertex_id) {
            return Ok(());
        }

        // Create new vertex with empty edge list
        let edge_list = Vec::new();
        self.map.insert(vertex_id, Arc::new(RwLock::new(edge_list)));

        // Update approximate size
        // Size includes: VId + Arc + RwLock + Vec overhead
        let entry_overhead = std::mem::size_of::<VId>();

        self.approximate_size
            .fetch_add(entry_overhead, Ordering::Relaxed);

        Ok(())
    }

    /// Put an edge into this MemGraph.
    ///
    /// Adds a directed edge from src_id to dst_id. If src_id doesn't exist,
    /// it will be created automatically. The edge is appended to the source
    /// vertex's edge list (duplicates are not checked).
    ///
    /// # Arguments
    ///
    /// * `src_id` - Source vertex ID
    /// * `dst_id` - Destination vertex ID
    pub fn put_edge(&self, src_id: VId, dst_id: VId) -> Result<()> {
        if let Some(entry) = self.map.get(&src_id) {
            // Source vertex exists, append the edge
            let mut edge_list = entry.value().write().unwrap();
            edge_list.push(dst_id);

            // Update approximate size (one VId added)
            self.approximate_size
                .fetch_add(std::mem::size_of::<VId>(), Ordering::Relaxed);
        } else {
            // Source vertex doesn't exist, create it with this edge
            let edge_list = vec![dst_id];
            self.map.insert(src_id, Arc::new(RwLock::new(edge_list)));

            // Update approximate size (entry overhead + one VId)
            let entry_overhead = std::mem::size_of::<VId>() + std::mem::size_of::<VId>();

            self.approximate_size
                .fetch_add(entry_overhead, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Put a batch of edges into this MemGraph.
    ///
    /// The edge_list should contain (source, destination) pairs.
    /// Each pair represents a directed edge. Edges are grouped by source vertex
    /// for efficient batch insertion.
    ///
    /// # Arguments
    ///
    /// * `edge_list` - A list of (source_vid, destination_vid) pairs
    pub fn put_edge_batch(&self, edge_list: EdgeList) -> Result<()> {
        if edge_list.is_empty() {
            return Ok(());
        }

        // Group edges by source vertex for efficient batch insertion
        use std::collections::HashMap;
        let mut edges_by_src: HashMap<VId, Vec<VId>> = HashMap::new();

        // Group destinations by source
        for (src_id, dst_id) in edge_list {
            edges_by_src
                .entry(src_id)
                .or_insert_with(Vec::new)
                .push(dst_id);
        }

        // Insert edges for each source vertex
        for (src_id, destinations) in edges_by_src {
            let num_edges = destinations.len();

            if let Some(entry) = self.map.get(&src_id) {
                // Source vertex exists, extend its neighbor list
                let mut neighbor_list = entry.value().write().unwrap();
                neighbor_list.extend(destinations);

                // Update approximate size (num_edges * VId)
                self.approximate_size
                    .fetch_add(num_edges * std::mem::size_of::<VId>(), Ordering::Relaxed);
            } else {
                // Source vertex doesn't exist, create it with these neighbors
                self.map.insert(src_id, Arc::new(RwLock::new(destinations)));

                // Update approximate size (entry overhead + edges)
                let entry_overhead =
                    std::mem::size_of::<VId>() + num_edges * std::mem::size_of::<VId>();

                self.approximate_size
                    .fetch_add(entry_overhead, Ordering::Relaxed);
            }
        }

        Ok(())
    }
}
