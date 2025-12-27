use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    cache::CacheKey,
    graph::CsrGraph,
    types::{CommId, VId},
};

/// Compact vertex type using bitpacking.
///
/// Layout in u64 (compatible with CacheKey):
/// - Bit 63: discriminant (0 = Normal, 1 = Giant)
/// - Bits 62-48: virtual_comm_id (15 bits)
/// - Bits 47-16: page_id (32 bits)
/// - Bits 15-0: offset (16 bits)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VertexIndexItem(u64);

impl VertexIndexItem {
    const GIANT_FLAG: u64 = 1u64 << 63;

    /// Create a normal vertex type.
    #[inline]
    pub fn normal(virtual_comm_id: u16, page_id: u32, offset: u16) -> Self {
        let packed = ((virtual_comm_id as u64) << 48) | ((page_id as u64) << 16) | (offset as u64);
        Self(packed)
    }

    /// Create a giant vertex type.
    #[inline]
    pub fn giant() -> Self {
        Self(Self::GIANT_FLAG)
    }

    /// Check if this is a normal vertex.
    #[inline]
    pub fn is_normal(&self) -> bool {
        self.0 & Self::GIANT_FLAG == 0
    }

    /// Check if this is a giant vertex.
    #[inline]
    pub fn is_giant(&self) -> bool {
        self.0 & Self::GIANT_FLAG != 0
    }

    /// Set all components for a normal vertex (panics if Giant).
    #[inline]
    pub fn set_normal(&mut self, virtual_comm_id: u16, page_id: u32, offset: u16) {
        debug_assert!(self.is_normal(), "Cannot set components on Giant vertex");
        self.0 = ((virtual_comm_id as u64) << 48) | ((page_id as u64) << 16) | (offset as u64);
    }

    /// Set virtual_comm_id (panics if Giant).
    #[inline]
    pub fn set_virtual_comm_id(&mut self, virtual_comm_id: u16) {
        debug_assert!(
            self.is_normal(),
            "Cannot set virtual_comm_id on Giant vertex"
        );
        // Clear old virtual_comm_id bits and set new ones
        self.0 = (self.0 & 0x0000_FFFF_FFFF_FFFF) | ((virtual_comm_id as u64) << 48);
    }

    /// Set page_id (panics if Giant).
    #[inline]
    pub fn set_page_id(&mut self, page_id: u32) {
        debug_assert!(self.is_normal(), "Cannot set page_id on Giant vertex");
        // Clear old page_id bits and set new ones
        self.0 = (self.0 & 0xFFFF_0000_0000_FFFF) | ((page_id as u64) << 16);
    }

    /// Set offset (panics if Giant).
    #[inline]
    pub fn set_offset(&mut self, offset: u16) {
        debug_assert!(self.is_normal(), "Cannot set offset on Giant vertex");
        // Clear old offset bits and set new ones
        self.0 = (self.0 & 0xFFFF_FFFF_FFFF_0000) | (offset as u64);
    }

    /// Convert to CacheKey (only valid for Normal vertices).
    /// Returns None if this is a Giant vertex.
    #[inline]
    pub fn to_cache_key(&self) -> Option<CacheKey> {
        if self.is_normal() {
            let virtual_comm_id = self.virtual_comm_id();
            let page_id = self.page_id();
            Some(CacheKey::new(virtual_comm_id, page_id))
        } else {
            None
        }
    }

    /// Extract components for normal vertices.
    /// Returns None if this is a giant vertex.
    #[inline]
    pub fn as_normal(&self) -> Option<(u16, u32, u16)> {
        if self.is_normal() {
            let virtual_comm_id = ((self.0 >> 48) & 0x7FFF) as u16;
            let page_id = ((self.0 >> 16) & 0xFFFFFFFF) as u32;
            let offset = (self.0 & 0xFFFF) as u16;
            Some((virtual_comm_id, page_id, offset))
        } else {
            None
        }
    }

    /// Get virtual_comm_id (panics if Giant).
    #[inline]
    pub fn virtual_comm_id(&self) -> u16 {
        debug_assert!(self.is_normal());
        ((self.0 >> 48) & 0x7FFF) as u16
    }

    /// Get page_id (panics if Giant).
    #[inline]
    pub fn page_id(&self) -> u32 {
        debug_assert!(self.is_normal());
        ((self.0 >> 16) & 0xFFFFFFFF) as u32
    }

    /// Get offset (panics if Giant).
    #[inline]
    pub fn offset(&self) -> u16 {
        debug_assert!(self.is_normal());
        (self.0 & 0xFFFF) as u16
    }
}

/// Vertex index that maps vertex IDs to their storage locations.
///
/// This structure manages the mapping between logical vertex IDs and their physical
/// storage locations in the graph storage system. It handles both normal vertices
/// (stored in paged format) and giant vertices (stored separately in RocksDB).
///
/// The index also manages the partitioning of communities into virtual communities,
/// where small communities are grouped together into buckets for efficient storage,
/// and large communities get dedicated storage buckets.
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct VertexIndex {
    /// Degree threshold for identifying giant vertices.
    /// Vertices with degree >= this value are considered giant and stored separately.
    pub giant_vertex_boundary: usize,

    /// Size threshold in bytes for identifying giant communities.
    /// Communities with total size >= this value get dedicated virtual community IDs.
    /// Typically set to 256MB.
    pub giant_community_boundary: usize,

    /// Array mapping each vertex ID to its storage location.
    /// For normal vertices: contains virtual_comm_id, page_id, and offset.
    /// For giant vertices: marked as Giant type.
    pub vertex_array: Vec<VertexIndexItem>,

    /// Maps each vertex ID to its original community ID.
    /// Length equals the number of vertices.
    pub community_map: Vec<CommId>,

    /// List of vertices belonging to each community.
    /// `community_list[comm_id]` contains all vertex IDs in that community.
    pub community_list: Vec<Vec<VId>>,

    /// Degree (number of neighbors) for each vertex.
    /// `vertex_degree[vid]` gives the degree of vertex `vid`.
    pub vertex_degree: Vec<u32>,
}

impl Default for VertexIndex {
    fn default() -> Self {
        Self {
            vertex_array: vec![],
            giant_community_boundary: 0,
            giant_vertex_boundary: 0,
            community_map: vec![],
            community_list: vec![],
            vertex_degree: vec![],
        }
    }
}

impl VertexIndex {
    /// Get the virtual community structure.
    ///
    /// Returns a list of communities grouped by their virtual community IDs.
    /// Each inner vector contains all vertex IDs that belong to that virtual community.
    ///
    /// This is useful for understanding how the original communities have been
    /// partitioned into virtual communities (buckets) for storage purposes.
    ///
    /// # Returns
    ///
    /// A vector where `result[virtual_comm_id]` contains all vertex IDs assigned
    /// to that virtual community. Only includes normal (non-giant) vertices.
    pub fn get_virtual_community_list_for_test(&self) -> Vec<Vec<VId>> {
        // Find the maximum virtual community ID
        let max_virtual_comm_id = self
            .vertex_array
            .iter()
            .filter_map(|item| {
                if item.is_normal() {
                    Some(item.virtual_comm_id())
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0);

        // Initialize result vector
        let mut virtual_communities = vec![Vec::new(); (max_virtual_comm_id + 1) as usize];

        // Group vertices by their virtual community ID
        for (vid, item) in self.vertex_array.iter().enumerate() {
            if item.is_normal() {
                let virtual_id = item.virtual_comm_id() as usize;
                virtual_communities[virtual_id].push(vid as VId);
            }
        }

        virtual_communities
    }

    /// Add a new vertex to the index.
    /// Will be improved later.
    pub fn add_giant_vertex(&mut self) -> anyhow::Result<VId> {
        // Implementation for adding a giant vertex
        self.vertex_array.push(VertexIndexItem::giant());
        let new_vertex_id = self.vertex_array.len() as VId - 1;
        // Maintain the community list;
        self.community_list.push(vec![new_vertex_id]);
        self.community_map.push(self.community_list.len() as CommId);
        Ok(new_vertex_id)
    }

    /// Build vertex index from a CSR graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - The CSR graph to build index from (consumed)
    /// * `giant_vertex_boundary` - Vertices with degree >= this are considered giant
    /// * `giant_community_boundary` - Size threshold in bytes for giant communities (typically 256MB)
    ///
    /// # Returns
    ///
    /// Returns the VertexIndex and a vector of giant vertex IDs that should be stored in RocksDB
    pub fn build_from_graph(
        graph: &mut CsrGraph,
        giant_vertex_boundary: usize,
        giant_community_boundary: usize,
    ) -> (Self, Vec<VId>) {
        const U32_SIZE: usize = std::mem::size_of::<u32>();

        let num_vertices = graph.num_vertices();

        // Ensure community list is computed
        let community_list = graph.get_community_structure();
        let num_communities = community_list.len();

        let mut vertex_degree = Vec::with_capacity(num_vertices);
        let mut vertex_array = Vec::with_capacity(num_vertices);
        let mut giant_vertices = Vec::new();
        let mut community_sizes = vec![0usize; num_communities];

        for vid in 0..num_vertices {
            let degree = graph.get_degree(vid as VId);
            vertex_degree.push(degree);

            let comm_id = graph.community_map[vid] as usize;

            if degree as usize >= giant_vertex_boundary {
                // Giant vertex
                giant_vertices.push(vid as VId);
                vertex_array.push(VertexIndexItem::giant());
            } else {
                // Normal vertex
                let size = (degree as usize + 1) * U32_SIZE;
                community_sizes[comm_id] += size;

                // Placeholder - will set correct virtual_comm_id later
                vertex_array.push(VertexIndexItem::normal(0, 0, 0));
            }
        }

        let mut virtual_comm_id: u16 = 0;
        let mut community_to_virtual = vec![0u16; num_communities];

        // Separate giant and small communities
        let mut giant_communities = Vec::new();
        let mut small_communities = Vec::new();

        for (comm_id, &size) in community_sizes.iter().enumerate() {
            if size >= giant_community_boundary {
                giant_communities.push(comm_id);
            } else if size > 0 {
                // Only include non-empty communities
                small_communities.push(comm_id);
            }
        }

        // Assign virtual IDs to giant communities
        for &comm_id in &giant_communities {
            community_to_virtual[comm_id] = virtual_comm_id;
            virtual_comm_id += 1;
        }

        // Group small communities into buckets
        let mut current_bucket_size = 0;
        let mut current_bucket_communities = Vec::new();

        for &comm_id in &small_communities {
            let comm_size = community_sizes[comm_id];

            if current_bucket_size + comm_size > giant_community_boundary
                && !current_bucket_communities.is_empty()
            {
                for &bucket_comm_id in &current_bucket_communities {
                    community_to_virtual[bucket_comm_id] = virtual_comm_id;
                }
                virtual_comm_id += 1;
                current_bucket_size = 0;
                current_bucket_communities.clear();
            }

            current_bucket_communities.push(comm_id);
            current_bucket_size += comm_size;
        }

        // Finalize last bucket
        if !current_bucket_communities.is_empty() {
            for &bucket_comm_id in &current_bucket_communities {
                community_to_virtual[bucket_comm_id] = virtual_comm_id;
            }
        }

        // ========== Update normal vertices with correct virtual_comm_id ==========
        for vid in 0..num_vertices {
            if vertex_array[vid].is_normal() {
                let comm_id = graph.community_map[vid] as usize;
                let virtual_id = community_to_virtual[comm_id];
                vertex_array[vid].set_virtual_comm_id(virtual_id);
            }
        }

        // Get fields from graph
        let community_map = graph.community_map.clone();
        let community_list = graph
            .community_list
            .clone()
            .expect("Community list not computed");

        let index = Self {
            giant_vertex_boundary,
            giant_community_boundary,
            vertex_array,
            community_map,
            community_list,
            vertex_degree,
        };

        (index, giant_vertices)
    }

    /// Check the state of the vertex;
    pub fn is_giant(&self, vertex_id: VId) -> Option<bool> {
        if vertex_id >= self.vertex_array.len() as u32 {
            return None;
        }
        Some(self.vertex_array[vertex_id as usize].is_giant())
    }

    /// Serialize the VertexIndex to a file with compression
    ///
    /// # Arguments
    /// * `path` - File path to write to
    /// * `compression_level` - Zstd compression level (1-22, recommended: 3-6 for balance)
    ///
    /// # Returns
    /// Result indicating success or error
    pub fn serialize_to_file<P: AsRef<Path>>(
        &self,
        path: P,
        compression_level: i32,
    ) -> std::io::Result<()> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        let mut encoder = zstd::stream::write::Encoder::new(buf_writer, compression_level)?;

        bincode::serialize_into(&mut encoder, self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        encoder.finish()?;
        Ok(())
    }

    /// Deserialize the VertexIndex from a file
    ///
    /// # Arguments
    /// * `path` - File path to read from
    ///
    /// # Returns
    /// Result containing the deserialized VertexIndex or error
    pub fn deserialize_from_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let buf_reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let decoder = zstd::stream::read::Decoder::new(buf_reader)?;

        bincode::deserialize_from(decoder)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Serialize without compression (faster but larger files)
    pub fn serialize_to_file_uncompressed<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let mut buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        bincode::serialize_into(&mut buf_writer, self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        buf_writer.flush()?;
        Ok(())
    }

    /// Deserialize without compression
    pub fn deserialize_from_file_uncompressed<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let buf_reader = BufReader::with_capacity(8 * 1024 * 1024, file);

        let vertex_index = bincode::deserialize_from(buf_reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(vertex_index)
    }
}
