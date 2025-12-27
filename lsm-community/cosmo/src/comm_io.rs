use crate::comm_io::comm_idx::CommunityIndexItem::{Giant, Normal};
use crate::comm_io::comm_idx::{BoundaryCSR, BoundaryGraph, CommunityIndex, CommunityIndexItem};
use crate::comm_io::scc_idx::{SCCIndex, SCCMeta};
use crate::comm_io::sim_csr_block::CSRSimpleCommBlock;
use crate::types::graph_query::GraphQuery;
use crate::types::graph_serialize::{ByteEncodable, TopologyDecode, TopologyEncode};
use crate::types::{CSRGraph, CSRSubGraph};
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressStyle};
use memmap2::Mmap;
use moka::sync::Cache;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub mod comm_idx;
pub mod csr_block;
mod scc_idx;
pub mod sim_csr_block;

/// Enum representing different variants of a CSR key, used for indexing communities.
///
/// # Variants
/// * `Normal(u32)` - Represents a normal CSR key with a single 32-bit identifier.
/// * `Giant(u32, u64)` - Represents a giant CSR key with a combination of a 32-bit identifier and a 64-bit offset.
///
/// The `CSRKey` enum is used to distinguish between normal and giant community keys,
/// where normal keys are simpler and giant keys may require additional offset information for efficient indexing.
#[allow(dead_code)]
pub enum CSRKey {
    /// Represents a normal CSR key, typically used for smaller community indices.
    Normal(u32),

    /// Represents a giant CSR key, which includes a base identifier and a larger offset.
    /// This allows handling larger and more complex community structures efficiently.
    Giant(u32, u64),
}

/// Implements the `PartialEq` trait for the `CSRKey` enum, allowing comparison between two `CSRKey` values.
///
/// This implementation defines the logic for equality between two `CSRKey` values, taking into account the different variants of the enum.
///
/// # Comparison Logic
/// * `CSRKey::Normal(a) == CSRKey::Normal(b)` - Compares the two `Normal` keys by their `u32` values.
/// * `CSRKey::Giant(a1, b1) == CSRKey::Giant(a2, b2)` - Compares the two `Giant` keys by both their `u32` and `u64` values.
/// * All other combinations of `CSRKey` variants return `false`.
///
/// This ensures that `PartialEq` works correctly, comparing both the variant and the values contained within each variant.
#[allow(dead_code)]
impl PartialEq for CSRKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Compare Normal variant keys
            (CSRKey::Normal(a1), CSRKey::Normal(a2)) => a1 == a2,
            // Compare Giant variant keys
            (CSRKey::Giant(a1, b1), CSRKey::Giant(a2, b2)) => a1 == a2 && b1 == b2,
            // Return false for mismatched variants
            _ => false,
        }
    }
}

/// Implements the `Hash` trait for the `CSRKey` enum, allowing `CSRKey` values to be hashed for use in hash-based collections.
///
/// This implementation defines how to compute a hash for a `CSRKey` value, based on its variant and contained data.
///
/// # Hashing Logic
/// * `CSRKey::Normal(value)` - Hashes the `u32` value contained in the `Normal` variant.
/// * `CSRKey::Giant(value1, value2)` - Hashes both the `u32` and `u64` values contained in the `Giant` variant.
///
/// This ensures that `CSRKey` can be used as a key in hash maps or other hash-based collections by appropriately hashing the values of each variant.
#[allow(dead_code)]
impl Hash for CSRKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            // Hash Normal variant value
            CSRKey::Normal(value1) => {
                value1.hash(state);
            }
            // Hash both Giant variant values
            CSRKey::Giant(value1, value2) => {
                value1.hash(state);
                value2.hash(state);
            }
        }
    }
}

/// Implements the `PartialOrd` trait for the `CSRKey` enum, allowing comparisons between `CSRKey` values with partial ordering.
///
/// This implementation defines how to compare two `CSRKey` values for ordering, using the `cmp` function to perform the actual comparison.
///
/// # Comparison Logic
/// * `partial_cmp` calls the `cmp` method, which is expected to return an `Ordering` for the two `CSRKey` values.
/// * This approach provides a way to order `CSRKey` values, but as this is a `PartialOrd` implementation, it can return `None` in cases where ordering is undefined (though this implementation always returns `Some(Ordering)`).
///
/// The `partial_cmp` method is typically used when a total ordering might not be possible, but here we ensure that a comparison can always be made by relying on `cmp`.
#[allow(dead_code)]
impl PartialOrd for CSRKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Use the cmp method to compare the two CSRKey values
        Some(self.cmp(other))
    }
}

/// Implements the `Ord` trait for the `CSRKey` enum, allowing total ordering of `CSRKey` values.
///
/// This implementation defines how to compare two `CSRKey` values to establish a total ordering, which is necessary for sorting and using `CSRKey` in ordered collections.
///
/// # Comparison Logic
/// * `CSRKey::Normal(a)` vs `CSRKey::Normal(b)` - Compares the `u32` values contained in the `Normal` variants.
/// * `CSRKey::Normal(a)` vs `CSRKey::Giant(b1, _)` - Compares the `u32` value of the `Normal` variant with the `u32` value of the `Giant` variant. `Normal` is considered smaller than `Giant`.
/// * `CSRKey::Giant(a1, _)` vs `CSRKey::Normal(b)` - Compares the `u32` value of the `Giant` variant with the `u32` value of the `Normal` variant. `Giant` is considered larger than `Normal`.
/// * `CSRKey::Giant(a1, b1)` vs `CSRKey::Giant(a2, b2)` - First compares the `u32` values of the `Giant` variants, and if they are equal, it compares the `u64` values for a more granular comparison.
///
/// This implementation ensures that `CSRKey` values can be ordered in a consistent, total manner, allowing them to be used in sorted collections and algorithms.
#[allow(dead_code)]
impl Ord for CSRKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Compare Normal variants
            (CSRKey::Normal(a1), CSRKey::Normal(a2)) => a1.cmp(a2),
            // Compare Normal with Giant, Normal is considered smaller
            (CSRKey::Normal(a1), CSRKey::Giant(a2, _)) => a1.cmp(a2),
            // Compare Giant with Normal, Giant is considered larger
            (CSRKey::Giant(a1, _), CSRKey::Normal(a2)) => a1.cmp(a2),
            // Compare two Giant variants
            (CSRKey::Giant(a1, b1), CSRKey::Giant(a2, b2)) => {
                // First compare the u32 values
                let cmp_u32 = a1.cmp(a2);
                // If u32 values are equal, compare the u64 values
                if cmp_u32 == Ordering::Equal {
                    b1.cmp(b2)
                } else {
                    cmp_u32
                }
            }
        }
    }
}

/// Implements the `Eq` trait for the `CSRKey` enum, indicating that `CSRKey` values can be compared for equality.
///
/// This implementation enables the use of `CSRKey` in data structures that require equality comparisons, such as `HashSet` or `HashMap`.
///
/// The `Eq` trait is automatically derived from the `PartialEq` implementation, as it does not require any additional logic beyond what is defined in `PartialEq`.
/// Therefore, if two `CSRKey` values are considered equal according to the `PartialEq` implementation, they are also considered equal by the `Eq` trait.
///
/// The `Eq` trait represents total equality and doesn't allow for the possibility of `None` being returned, as is the case with `PartialEq`.
#[allow(dead_code)]
impl Eq for CSRKey {}

/// An indexing structure specifically designed for managing giant communities in graph analysis.
///
/// This struct provides specialized indexing for large communities that require special handling
/// due to their size. It combines metadata about Strongly Connected Components (SCCs) with
/// an index of their offsets to enable efficient navigation and retrieval of giant community data.
///
/// # Fields
/// * `scc_meta` - Metadata about the Strongly Connected Components within the giant community
/// * `scc_index` - Index containing offset information for efficient access to each SCC
///
/// Giant communities are typically split into multiple SCCs to improve processing efficiency
/// and memory management. This index structure facilitates operations on these subdivided
/// giant communities.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GiantCommunityIndex {
    /// Metadata about the Strongly Connected Components (SCCs) within the giant community
    /// Includes information such as counts, sizes, and other properties of the SCCs
    pub scc_meta: SCCMeta,

    /// Index containing the offset positions of each SCC within the giant community
    /// Enables efficient lookup and traversal of the community's internal structure
    pub scc_index: SCCIndex,
}

/// A storage structure for managing community data in graph analysis.
///
/// This struct provides centralized storage and caching for community information
/// within a graph. It maintains the total vertex count, graph identifier, and uses
/// specialized indexing and caching mechanisms to efficiently retrieve community data.
/// It also incorporates memory-mapping for fast access to community data and giant community
/// structures.
///
/// # Fields
/// * `vertex_count` - The total number of vertices in the graph
/// * `graph_name` - A string identifier for the graph
/// * `community_index` - Internal index structure for mapping vertices to communities
/// * `community_cache` - Caching system for frequently accessed regular communities
/// * `giant_community_map` - Index mapping for giant communities that require special handling
/// * `scc_cache` - Cache for Strongly Connected Components (SCCs) within communities
/// * `normal_mem_map` - Memory map for efficient access to the normal community data
/// * `giant_mem_map_manager` - Manages memory mappings for giant communities to improve access times
///
/// The caching system is designed to optimize memory usage and performance by using different
/// strategies for regular communities and giant communities. SCCs within communities are
/// also cached separately to improve access times for hierarchical community structures.
///
/// The memory mapping system ensures fast access to both normal and giant communities while
/// reducing the need for repeated disk I/O operations, leveraging memory-mapped files for
/// efficient data retrieval.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CommunityStorage {
    /// Total number of vertices in the graph
    pub vertex_count: u64,

    /// String identifier for the graph
    pub graph_name: String,

    /// Workspace.
    pub workspace: String,

    /// Internal mapping structure from vertices to their communities
    pub community_index: CommunityIndex,

    /// Cache for regular community data to improve retrieval performance
    /// Uses u32 keys (community IDs) and stores atomic references to
    /// CSR (Compressed Sparse Row) formatted community blocks
    community_cache: Cache<u32, Arc<CSRSimpleCommBlock>>,

    /// Index mapping for giant communities that require special handling due to their size
    /// Maps community IDs to specialized index structures for giant communities
    pub giant_community_map: DashMap<u32, Arc<GiantCommunityIndex>>,

    /// Cache for Strongly Connected Components (SCCs) within communities
    /// The key is a tuple of (community_id, scc_id) for efficient lookup
    /// Stores atomic references to CSR blocks representing the SCC substructure
    pub(crate) scc_cache: Cache<(u32, u64), Arc<CSRSimpleCommBlock>>,

    /// The memory map used to quickly access the normal community.
    /// This memory map is used to improve access times to the data for regular communities,
    /// reducing the overhead of repeatedly reading the data from storage.
    normal_mem_map: Arc<Mmap>,

    /// A DashMap used for managing memory mappings of giant communities.
    /// This stores memory-mapped data for giant communities that require special handling,
    /// enabling fast access to large community structures directly from memory.
    giant_mem_map_manager: DashMap<u32, Mmap>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ProcessedCommunityResult {
    community_id: u32,
    community_boundary_list: Vec<u64>,
    comm_csr_block: Arc<CSRSimpleCommBlock>,
    is_giant: bool,
    boundary_updates: Vec<(u64, u64, u32, u32)>,
    community: Arc<BTreeSet<u64>>,
    giant_comm_index: Option<Arc<GiantCommunityIndex>>,
    giant_csr_byte_group: Option<Arc<Vec<Vec<u8>>>>,
}

/// Enum representing a community item that can either be a normal community or a giant community.
///
/// # Variants
/// * `Normal` - Represents a normal community, stored as an `Arc<CSRSimpleCommBlock>`.
/// * `Giant` - Represents a giant community, stored as an `Arc<GiantCommunityIndex>`.
///
/// The `CommunityItemRef` enum is used to differentiate between normal and giant communities, where giant
/// communities are handled with special indexing and metadata due to their size and complexity.
#[allow(dead_code)]
#[derive(Debug)]
pub enum CommunityItemRef {
    /// Represents a normal community, typically a smaller group of vertices or nodes in the graph.
    /// This community is represented by a `CSRSimpleCommBlock`, which efficiently stores the community data.
    Normal(Arc<CSRSimpleCommBlock>),

    /// Represents a giant community, which is typically large and divided into smaller SCCs.
    /// The `GiantCommunityIndex` stores the metadata and offsets necessary to handle the community efficiently.
    Giant(Arc<GiantCommunityIndex>),
}

/// Enum representing a community item instance, which can either be a normal community or a giant community.
///
/// This enum is used to represent different types of community instances in a graph analysis context. A community can
/// either be a normal community or a giant community. Giant communities are typically large and split into smaller
/// sub-communities (or Strongly Connected Components, SCCs), requiring special handling for efficient processing.
///
/// # Variants
///
/// * `Normal` - Represents a normal community, stored as a `CSRSimpleCommBlock`. This variant is used for
///   smaller communities that can be handled without additional complexity.
///
/// * `Giant` - Represents a giant community, stored as an `Arc<GiantCommunityIndex>` along with a vector of
///   `CSRSimpleCommBlock` items. The `Arc<GiantCommunityIndex>` contains metadata and an index for the giant
///   community, while the `Vec<CSRSimpleCommBlock>` holds the actual sub-communities or sub-graphs (e.g., SCCs).
///   This variant is used for large communities that require subdivision for efficient processing.
///
/// # Usage
///
/// The `CommunityItemInstance` enum allows for efficient handling of both normal and giant communities. It provides
/// flexibility for different types of graph communities, ensuring that giant communities, which might require special
/// memory and performance management, are handled appropriately through their metadata and indices.
#[allow(dead_code)]
#[derive(Debug)]
pub enum CommunityItemInstance {
    /// Represents a normal community, typically small and stored directly in a CSRSimpleCommBlock.
    Normal(CSRSimpleCommBlock),

    /// Represents a giant community, stored as an Arc<GiantCommunityIndex> and accompanied by a vector of CSRSimpleCommBlocks
    /// representing sub-communities or sub-graphs (e.g., SCCs).
    Giant(Arc<GiantCommunityIndex>, Vec<CSRSimpleCommBlock>),
}

#[allow(dead_code)]
impl CommunityStorage {
    /// Builds a community storage structure from index files.
    ///
    /// This function initializes the community storage by reading data from pre-existing
    /// index files. It loads both regular communities and giant communities (SCCs),
    /// setting up memory maps for efficient data access.
    ///
    /// # Arguments
    ///
    /// * `graph_name` - The base name of the graph used to construct file paths
    ///
    /// # Returns
    ///
    /// * A fully initialized instance of the community storage structure
    ///
    /// # Panics
    ///
    /// * If the "cosmo.db" directory doesn't exist
    /// * If required index or storage files don't exist
    pub fn build_from_index_file(graph_name: &str) -> Option<Self> {
        // Prepare the storage file paths.
        let dir_path = Path::new("cosmo.db");
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Verify that required directory and files exist
        if !dir_path.exists() {
            return None;
        }
        for path in &[&file_path, &index_path] {
            if !path.exists() {
                return None;
            }
        }

        // Initialize memory mapping for regular communities
        // Open the storage file for small communities with read/write access
        let small_comm_store_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        // Create memory map for efficient access to community storage
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&small_comm_store_file).unwrap() });

        // Read and deserialize the community index file
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&index_path)
            .unwrap();
        let mut community_index_bytes = Vec::new();
        community_index_file
            .read_to_end(&mut community_index_bytes)
            .unwrap();
        let community_index: CommunityIndex = bincode::deserialize(&community_index_bytes).unwrap();

        // Initialize collections for giant communities (SCCs)
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let giant_mem_map_manager = DashMap::<u32, Mmap>::new();

        // Process giant communities from the index
        // For each giant community, create a memory map and load its index
        for (community_id, community_index_item) in &community_index.community_map {
            match community_index_item {
                Giant {
                    scc_index_offset, ..
                } => {
                    // Construct the path to the giant community storage file
                    let dir_path = Path::new("cosmo.db");
                    let giant_comm_name =
                        format!("{graph_name}_giant_comm{}_storage.bin", community_id);
                    let giant_comm_path = dir_path.join(&giant_comm_name);

                    // Open or create the giant community file
                    let giant_comm_file = fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&giant_comm_path)
                        .unwrap();

                    // Create memory mapping for this giant community
                    let giant_mem_map = unsafe { Mmap::map(&giant_comm_file).unwrap() };
                    giant_mem_map_manager.insert(*community_id, giant_mem_map);

                    // Get a reference to the memory map and deserialize the SCC index
                    let giant_mem_map_ref = giant_mem_map_manager.get(&community_id).unwrap();
                    let scc_index_item_bytes =
                        giant_mem_map_ref.value()[0..*scc_index_offset].to_vec();
                    let scc_index_item: GiantCommunityIndex =
                        bincode::deserialize(&scc_index_item_bytes).unwrap();

                    // Store the deserialized SCC index in the map
                    giant_community_map.insert(*community_id, Arc::new(scc_index_item));
                }
                _ => {} // Skip non-giant community entries
            }
        }

        // Initialize LRU caches for better performance
        // Cache for regular communities
        let community_cache = Cache::new(1000000);
        // Cache for Strongly Connected Components (SCCs)
        let scc_cache = Cache::new(1000000);

        // Return the fully constructed community storage structure
        Some(Self {
            vertex_count: community_index.boundary_graph.vertex_community_map.len() as u64,
            graph_name: graph_name.to_owned(),
            workspace: "cosmo.db".to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager,
        })
    }

    /// Builds a community storage structure from index files.
    ///
    /// This function initializes the community storage by reading data from pre-existing
    /// index files. It loads both regular communities and giant communities (SCCs),
    /// setting up memory maps for efficient data access.
    ///
    /// # Arguments
    ///
    /// * `graph_name` - The base name of the graph used to construct file paths
    ///
    /// # Returns
    ///
    /// * A fully initialized instance of the community storage structure
    ///
    /// # Panics
    ///
    /// * If the "cosmo.db" directory doesn't exist
    /// * If required index or storage files don't exist
    pub fn build_from_index_file_for_ablation(graph_name: &str, giant_theta: f64) -> Option<Self> {
        // Prepare the storage file paths.
        let working_space = format!("cosmo{}.db", giant_theta);
        let dir_path = Path::new(&working_space);
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Verify that required directory and files exist
        if !dir_path.exists() {
            return None;
        }
        for path in &[&file_path, &index_path] {
            if !path.exists() {
                return None;
            }
        }

        // Initialize memory mapping for regular communities
        // Open the storage file for small communities with read/write access
        let small_comm_store_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        // Create memory map for efficient access to community storage
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&small_comm_store_file).unwrap() });

        // Read and deserialize the community index file
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&index_path)
            .unwrap();
        let mut community_index_bytes = Vec::new();
        community_index_file
            .read_to_end(&mut community_index_bytes)
            .unwrap();
        let community_index: CommunityIndex = bincode::deserialize(&community_index_bytes).unwrap();

        // Initialize collections for giant communities (SCCs)
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let giant_mem_map_manager = DashMap::<u32, Mmap>::new();

        // Process giant communities from the index
        // For each giant community, create a memory map and load its index
        for (community_id, community_index_item) in &community_index.community_map {
            match community_index_item {
                Giant {
                    scc_index_offset, ..
                } => {
                    let workspace = format!("cosmo{}.db", giant_theta);
                    // Construct the path to the giant community storage file
                    let dir_path = Path::new(&workspace);
                    let giant_comm_name =
                        format!("{graph_name}_giant_comm{}_storage.bin", community_id);
                    let giant_comm_path = dir_path.join(&giant_comm_name);

                    // Open or create the giant community file
                    let giant_comm_file = fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&giant_comm_path)
                        .unwrap();

                    // Create memory mapping for this giant community
                    let giant_mem_map = unsafe { Mmap::map(&giant_comm_file).unwrap() };
                    giant_mem_map_manager.insert(*community_id, giant_mem_map);

                    // Get a reference to the memory map and deserialize the SCC index
                    let giant_mem_map_ref = giant_mem_map_manager.get(&community_id).unwrap();
                    let scc_index_item_bytes =
                        giant_mem_map_ref.value()[0..*scc_index_offset].to_vec();
                    let scc_index_item: GiantCommunityIndex =
                        bincode::deserialize(&scc_index_item_bytes).unwrap();

                    // Store the deserialized SCC index in the map
                    giant_community_map.insert(*community_id, Arc::new(scc_index_item));
                }
                _ => {} // Skip non-giant community entries
            }
        }

        // Initialize LRU caches for better performance
        // Cache for regular communities
        let community_cache = Cache::new(1000000);
        // Cache for Strongly Connected Components (SCCs)
        let scc_cache = Cache::new(1000000);

        // Return the fully constructed community storage structure
        Some(Self {
            vertex_count: community_index.boundary_graph.vertex_community_map.len() as u64,
            graph_name: graph_name.to_owned(),
            workspace: format!("cosmo{}.db", giant_theta).to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager,
        })
    }

    /// Optimized version of community storage construction function
    ///
    /// Builds a community storage structure from a graph file with enhanced performance
    /// through pre-computation, batch processing, and optimized data structures.
    ///
    /// # Arguments
    ///
    /// * `graph_file` - Path to the input graph file
    /// * `graph_name` - Name identifier for the graph (used in output file naming)
    /// * `giant_theta` - Threshold ratio for determining giant communities (0.0-1.0)
    ///
    /// # Returns
    ///
    /// A fully constructed `Self` instance with optimized community storage
    ///
    /// # Performance Optimizations
    ///
    /// 1. Pre-computed community structures using Vec instead of BTreeMap
    /// 2. Batch construction of vertex-community mapping
    /// 3. Pre-allocated data structure capacities
    /// 4. FxHashMap usage for better performance
    /// 5. Bulk boundary edge detection
    /// 6. Single timestamp generation to avoid system calls
    /// 7. Efficient memory mapping for storage
    pub fn build_from_graph_file_opt(graph_file: &str, graph_name: &str, giant_theta: f64) -> Self {
        // Load CSR graph from file
        let csr_memory = CSRGraph::from_graph_file(graph_file);

        // Calculate threshold for identifying giant communities
        let giant_vertex_count = (csr_memory.vertex_count as f64 * giant_theta).ceil() as u64;

        // Initialize LRU caches for community and SCC data
        let community_cache = Cache::new(1000000);
        let scc_cache = Cache::new(1000000);

        // Optimization 1: Pre-compute community structure using Vec for fast lookups
        let mut community_structure = BTreeMap::<u32, BTreeSet<u64>>::new();
        let mut vc_map = Vec::<u32>::new();
        vc_map.resize(csr_memory.vertex_count as usize, 0u32); // Pre-allocate vertex-community mapping

        // Optimization 2: Batch build vertex-community map to reduce redundant access
        for (vertex_id, community_id) in &csr_memory.community_index {
            vc_map[*vertex_id as usize] = *community_id; // Direct vector access for O(1) lookup
            community_structure
                .entry(*community_id)
                .or_insert_with(BTreeSet::new)
                .insert(*vertex_id);
        }

        // Prepare storage file paths
        let dir_path = Path::new("cosmo.db");
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Create directory if it doesn't exist
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
            println!("Created directory: cosmo.db");
        }

        // Remove existing files to ensure clean state
        for path in &[&file_path, &index_path] {
            if path.exists() {
                let _ = fs::remove_file(path);
                println!("Removed existing file: {}", path.display());
            }
        }

        // Create storage file for small communities
        let small_comm_store_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();
        println!("Created new file: {}", file_path.display());

        // Optimization 3: Pre-allocate capacity for all data structures
        let total_communities = community_structure.len();
        let mut community_offset_index = Vec::<u64>::with_capacity(total_communities);
        let mut current_community_offset = 0u64;

        // Optimization 4: Use FxHashMap for better performance than default HashMap
        let mut boundary_adj_map = FxHashMap::<u64, FxHashSet<u64>>::default();
        let mut community_boundary = FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let mut community_map = FxHashMap::<u32, CommunityIndexItem>::default();
        let mut community_boundary_list_manager = FxHashMap::<u32, FxHashSet<u64>>::default();

        // Optimization 5: Pre-calculate estimated sizes for capacity pre-allocation
        let avg_degree = csr_memory.neighbor_list.len() / csr_memory.vertex_count as usize;

        // Setup progress bar for user feedback
        let pb = ProgressBar::new(community_structure.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Storage Building.");

        // Optimization 7: Batch process boundary detection to reduce redundant computation
        let mut all_boundary_edges = FxHashMap::<u32, Vec<(u64, u64, u32)>>::default();

        // Pre-compute all boundary edges for each community
        for (community_id, community) in &community_structure {
            let mut boundary_edges = Vec::new();

            for vertex in community {
                let neighbors = csr_memory.read_neighbor(vertex);
                for neighbor in &neighbors {
                    if !community.contains(neighbor) {
                        // Use pre-computed vc_map for fast community lookup
                        let next_community = vc_map[*neighbor as usize];
                        boundary_edges.push((*vertex, *neighbor, next_community));
                    }
                }
            }

            if !boundary_edges.is_empty() {
                all_boundary_edges.insert(*community_id, boundary_edges);
            }
        }

        // Main community processing loop
        for (community_id, community) in community_structure.into_iter() {
            let community_size = community.len();

            // Optimization 8: Pre-allocate capacity based on community size
            let estimated_edges = community_size * avg_degree;
            let mut comm_vertex_list = Vec::<(u64, u64)>::with_capacity(community_size);
            let mut comm_neighbor_list = Vec::<u64>::with_capacity(estimated_edges);
            let mut comm_vertex_index = FxHashMap::<u64, usize>::with_capacity_and_hasher(
                community_size,
                Default::default(),
            );
            let mut comm_offset = 0u64;
            let mut community_boundary_list = Vec::with_capacity(community_size / 4); // Estimate 25% boundary vertices

            // Optimization 9: Batch process boundary information
            let boundary_edges = all_boundary_edges
                .get(&community_id)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let mut boundary_vertices = FxHashSet::<u64>::default();

            // Process all boundary edges for this community
            for (vertex, neighbor, next_community) in boundary_edges {
                boundary_vertices.insert(*vertex); // Mark as boundary vertex

                // Batch update boundary mappings
                boundary_adj_map
                    .entry(*vertex)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);

                if !boundary_adj_map.contains_key(neighbor) {
                    boundary_adj_map.insert(*neighbor, FxHashSet::default());
                }

                // Update inter-community boundary mapping
                community_boundary
                    .entry((community_id, *next_community))
                    .or_insert_with(FxHashSet::default)
                    .insert((*vertex, *neighbor));

                // Update boundary vertex lists for both communities
                community_boundary_list_manager
                    .entry(community_id)
                    .or_insert_with(FxHashSet::default)
                    .insert(*vertex);

                community_boundary_list_manager
                    .entry(*next_community)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);
            }

            // Build community vertex list with optimized timestamp generation
            for (vertex_inner_idx, vertex) in community.iter().enumerate() {
                let neighbors = csr_memory.read_neighbor(vertex); // Get vertex neighbors

                // Check if vertex is on community boundary
                if boundary_vertices.contains(vertex) {
                    community_boundary_list.push(*vertex);
                }

                // Optimization 10: Use fixed timestamp + increment to avoid system calls
                comm_vertex_list.push((*vertex, comm_offset));

                comm_vertex_index.insert(*vertex, vertex_inner_idx); // Build index mapping
                comm_offset += neighbors.len() as u64; // Update offset for next vertex

                // Batch convert neighbors to Vertex objects
                comm_neighbor_list.extend(neighbors);
            }

            // Build CSR block for this community
            let comm_vertex_count = community.len() as u64;
            let comm_csr_block = CSRSimpleCommBlock {
                vertex_count: comm_vertex_count,
                vertex_list: comm_vertex_list,
                neighbor_list: comm_neighbor_list,
                vertex_index: comm_vertex_index.into_iter().collect::<HashMap<_, _>>(),
            };

            // Process community based on size (small vs giant)
            if community.len() < giant_vertex_count as usize {
                // Handle normal (small) community
                Self::enrich_boundary_graph_normal(
                    community_boundary_list,
                    &community,
                    &csr_memory,
                    &mut boundary_adj_map,
                );

                let comm_csr_bytes = comm_csr_block.encode_topology(); // Serialize community data
                community_offset_index.push(current_community_offset);
                community_map.insert(
                    community_id,
                    Normal {
                        offset: current_community_offset as usize,
                        length: comm_csr_bytes.len(),
                    },
                );

                // Write community data to storage file (platform-specific)
                #[cfg(windows)]
                {
                    small_comm_store_file
                        .seek_write(&comm_csr_bytes, current_community_offset)
                        .unwrap();
                }
                #[cfg(unix)]
                {
                    small_comm_store_file
                        .write_at(&comm_csr_bytes, current_community_offset)
                        .unwrap();
                }
                current_community_offset += comm_csr_bytes.len() as u64;
            } else {
                // Handle giant community with SCC decomposition
                let g_induced_comm =
                    csr_memory.induce_subgraph(&community.iter().cloned().collect::<Vec<_>>());
                let (giant_comm_index, giant_csr_byte_group) =
                    Self::process_giant_community(&g_induced_comm, &comm_csr_block);

                Self::enrich_boundary_graph_giant(
                    community_boundary_list,
                    &giant_comm_index.scc_meta,
                    &mut boundary_adj_map,
                );

                // Serialize and store giant community index
                let giant_comm_index_bytes = bincode::serialize(&giant_comm_index).unwrap();
                let giant_comm_index_length = giant_comm_index_bytes.len();
                let giant_comm_length =
                    giant_csr_byte_group.iter().fold(0usize, |mut acc, item| {
                        acc += item.len();
                        acc
                    });

                community_map.insert(
                    community_id,
                    Giant {
                        scc_index_offset: giant_comm_index_length,
                        length: giant_comm_length,
                    },
                );
                giant_community_map.insert(community_id, Arc::new(giant_comm_index));

                // Create separate storage file for giant community
                let giant_comm_name =
                    format!("{graph_name}_giant_comm{}_storage.bin", community_id);
                let giant_comm_path = dir_path.join(&giant_comm_name);
                let giant_comm_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&giant_comm_path)
                    .unwrap();

                // Write giant community data sequentially
                let mut current_write_ptr = 0u64;
                #[cfg(windows)]
                {
                    giant_comm_file
                        .seek_write(&giant_comm_index_bytes, 0)
                        .unwrap();
                }
                #[cfg(unix)]
                {
                    giant_comm_file
                        .write_at(&giant_comm_index_bytes, 0)
                        .unwrap();
                }
                current_write_ptr += giant_comm_index_length as u64;

                // Write each SCC data block
                for scc_csr_b in giant_csr_byte_group.into_iter() {
                    #[cfg(windows)]
                    {
                        giant_comm_file
                            .seek_write(&scc_csr_b, current_write_ptr)
                            .unwrap();
                    }
                    #[cfg(unix)]
                    {
                        giant_comm_file
                            .write_at(&scc_csr_b, current_write_ptr)
                            .unwrap();
                    }
                    current_write_ptr += scc_csr_b.len() as u64;
                }
            }
            pb.inc(1); // Update progress bar
        }

        let boundary_adj_csr = BoundaryCSR::build_from_boundary_adj(&boundary_adj_map);

        // Build final community index structure
        let community_index = CommunityIndex {
            community_map: community_map.into_iter().collect(),
            boundary_graph: BoundaryGraph {
                vertex_community_map: vc_map,
                boundary_adj_map: boundary_adj_map.into_iter().collect(),
                boundary_csr: boundary_adj_csr,
                community_boundary: community_boundary.into_iter().collect(),
                community_boundary_list: community_boundary_list_manager.into_iter().collect(),
            },
        };

        // Create memory map for efficient access to small community storage
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&small_comm_store_file).unwrap() });

        // Serialize and save community index
        let community_index_bytes = bincode::serialize(&community_index).unwrap();
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)
            .unwrap();
        community_index_file
            .write_all(&community_index_bytes)
            .unwrap();

        // Return fully constructed storage instance
        Self {
            vertex_count: csr_memory.vertex_count,
            graph_name: graph_name.to_owned(),
            workspace: "cosmo.db".to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager: DashMap::<u32, Mmap>::new(),
        }
    }

    pub fn build_from_graph_file_opt_par_high_performance(
        graph_file: &str,
        graph_name: &str,
        giant_theta: f64,
    ) -> Self {
        // Load CSR graph from file and wrap in Arc for thread-safe sharing
        let csr_memory = Arc::new(CSRGraph::from_graph_file(graph_file));

        // Calculate threshold for identifying giant communities
        let giant_vertex_count = (csr_memory.vertex_count as f64 * giant_theta).ceil() as u64;

        // Initialize LRU caches for community and SCC data
        let community_cache = Cache::new(1000000);
        let scc_cache = Cache::new(1000000);

        // Optimization 1: Pre-compute community structure using Vec for fast lookups
        let mut community_structure = BTreeMap::<u32, BTreeSet<u64>>::new();
        let mut vc_map = Vec::<u32>::new();
        vc_map.resize(csr_memory.vertex_count as usize, 0u32);

        // Optimization 2: Batch build vertex-community map to reduce redundant access
        for (vertex_id, community_id) in &csr_memory.community_index {
            vc_map[*vertex_id as usize] = *community_id;
            community_structure
                .entry(*community_id)
                .or_insert_with(BTreeSet::new)
                .insert(*vertex_id);
        }

        // Wrap read-only data in Arc for thread-safe sharing
        let vc_map = Arc::new(vc_map);

        // Prepare storage file paths
        let dir_path = Path::new("cosmo.db");
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Create directory if it doesn't exist
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
            println!("Created directory: cosmo.db");
        }

        // Remove existing files to ensure clean state
        for path in &[&file_path, &index_path] {
            if path.exists() {
                let _ = fs::remove_file(path);
                println!("Removed existing file: {}", path.display());
            }
        }

        // Create storage file for small communities and wrap in Arc for thread sharing
        let small_comm_store_file = Arc::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap(),
        );
        println!("Created new file: {}", file_path.display());

        // Optimization 3: Pre-allocate capacity for all data structures
        let total_communities = community_structure.len();

        // Use DashMap for lock-free concurrent access to boundary_adj_map
        let boundary_adj_map = Arc::new(DashMap::<u64, FxHashSet<u64>>::new());
        let mut community_boundary = FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let mut community_map = FxHashMap::<u32, CommunityIndexItem>::default();
        let mut community_boundary_list_manager = FxHashMap::<u32, FxHashSet<u64>>::default();
        let mut community_offset_index = Vec::<u64>::with_capacity(total_communities);

        // Optimization 5: Pre-calculate estimated sizes for capacity pre-allocation
        let avg_degree = csr_memory.neighbor_list.len() / csr_memory.vertex_count as usize;

        // Optimization 7: Batch process boundary detection to reduce redundant computation
        let mut all_boundary_edges = FxHashMap::<u32, Vec<(u64, u64, u32)>>::default();

        // Pre-compute all boundary edges for each community
        for (community_id, community) in &community_structure {
            let mut boundary_edges = Vec::new();

            for vertex in community {
                let neighbors = csr_memory.read_neighbor(vertex);
                for neighbor in &neighbors {
                    if !community.contains(neighbor) {
                        // Use pre-computed vc_map for fast community lookup
                        let next_community = vc_map[*neighbor as usize];
                        boundary_edges.push((*vertex, *neighbor, next_community));
                    }
                }
            }

            if !boundary_edges.is_empty() {
                all_boundary_edges.insert(*community_id, boundary_edges);
            }
        }

        // Wrap boundary edges in Arc for thread-safe sharing
        let all_boundary_edges = Arc::new(all_boundary_edges);

        // Thread-safe collection for parallel processing results
        let processed_communities = Arc::new(Mutex::new(Vec::<ProcessedCommunityResult>::new()));

        // Convert community structure to vector for parallel iteration
        let communities: Vec<_> = community_structure.into_iter().collect();

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(communities.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Index Computing.");

        // Main parallel community processing loop using rayon
        communities
            .into_par_iter()
            .for_each(|(community_id, community)| {
                let community_size = community.len();

                // Optimization 8: Pre-allocate capacity based on community size
                let estimated_edges = community_size * avg_degree;
                let mut comm_vertex_list = Vec::<(u64, u64)>::with_capacity(community_size);
                let mut comm_neighbor_list = Vec::<u64>::with_capacity(estimated_edges);
                let mut comm_vertex_index = FxHashMap::<u64, usize>::with_capacity_and_hasher(
                    community_size,
                    Default::default(),
                );
                let mut comm_offset = 0u64;
                let mut community_boundary_list = Vec::with_capacity(community_size / 4);

                // Optimization 9: Batch process boundary information locally
                let boundary_edges = all_boundary_edges
                    .get(&community_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let mut boundary_vertices = FxHashSet::<u64>::default();
                let mut local_boundary_updates = Vec::new();

                // Process boundary edges and collect updates for later batch processing
                for (vertex, neighbor, next_community) in boundary_edges {
                    boundary_vertices.insert(*vertex);
                    local_boundary_updates.push((
                        *vertex,
                        *neighbor,
                        community_id,
                        *next_community,
                    ));
                }

                // Build community vertex list with optimized timestamp generation
                for (vertex_inner_idx, vertex) in community.iter().enumerate() {
                    let neighbors = csr_memory.read_neighbor(vertex);

                    // Check if vertex is on community boundary
                    if boundary_vertices.contains(vertex) {
                        community_boundary_list.push(*vertex);
                    }

                    // Optimization 10: Use fixed timestamp + increment to avoid system calls
                    comm_vertex_list.push((*vertex, comm_offset));

                    comm_vertex_index.insert(*vertex, vertex_inner_idx);
                    comm_offset += neighbors.len() as u64;

                    // Batch convert neighbors to Vertex objects
                    comm_neighbor_list.extend(neighbors);
                }

                // Build CSR block for this community
                let comm_vertex_count = community.len() as u64;
                let comm_csr_block = CSRSimpleCommBlock {
                    vertex_count: comm_vertex_count,
                    vertex_list: comm_vertex_list,
                    neighbor_list: comm_neighbor_list,
                    vertex_index: comm_vertex_index.into_iter().collect::<HashMap<_, _>>(),
                };

                // Process giant communities in parallel
                let (giant_comm_index, giant_csr_byte_group) =
                    if community.len() >= giant_vertex_count as usize {
                        let g_induced_comm = csr_memory
                            .induce_subgraph(&community.iter().cloned().collect::<Vec<_>>());
                        let (giant_index, giant_bytes) =
                            Self::process_giant_community(&g_induced_comm, &comm_csr_block);
                        (Some(Arc::new(giant_index)), Some(Arc::new(giant_bytes)))
                    } else {
                        (None, None)
                    };

                // Create processing result
                let result = ProcessedCommunityResult {
                    community_id,
                    community_boundary_list,
                    comm_csr_block: Arc::new(comm_csr_block),
                    is_giant: community.len() >= giant_vertex_count as usize,
                    boundary_updates: local_boundary_updates,
                    community: Arc::new(community),
                    giant_comm_index,
                    giant_csr_byte_group,
                };

                // Store result in thread-safe collection
                processed_communities.lock().unwrap().push(result);

                // Update thread-safe progress bar
                pb.inc(1);
            });

        // Serial processing phase for file I/O and shared state updates
        let processed_communities = match Arc::try_unwrap(processed_communities) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(_) => {
                panic!("Cannot unwrap Arc, multiple references exist");
            }
        };

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(processed_communities.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Index Preparing.");

        // ===== OPTIMIZATION: Batch processing for boundary updates and small communities =====

        // Step 1: Collect all boundary updates for batch processing
        let mut boundary_adj_updates = FxHashMap::<u64, FxHashSet<u64>>::default();
        let mut community_boundary_updates =
            FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let mut community_boundary_list_updates = FxHashMap::<u32, FxHashSet<u64>>::default();

        // Step 2: Collect all small community data for batch file writing
        let mut small_community_write_buffer = Vec::new();
        let mut small_community_metadata = Vec::<(u32, usize, usize)>::new(); // (community_id, offset, length)
        let mut current_write_offset = 0usize;

        // Step 3: Process giant communities separately (they need individual files)
        let mut giant_communities_to_process = Vec::new();

        // Collect updates and prepare batch operations
        for result in processed_communities.into_iter() {
            // Collect boundary updates
            for (vertex, neighbor, community_id, next_community) in &result.boundary_updates {
                // Collect boundary adjacency updates
                boundary_adj_updates
                    .entry(*vertex)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);
                boundary_adj_updates
                    .entry(*neighbor)
                    .or_insert_with(FxHashSet::default);

                // Collect inter-community boundary updates
                community_boundary_updates
                    .entry((*community_id, *next_community))
                    .or_insert_with(FxHashSet::default)
                    .insert((*vertex, *neighbor));

                // Collect boundary vertex list updates
                community_boundary_list_updates
                    .entry(*community_id)
                    .or_insert_with(FxHashSet::default)
                    .insert(*vertex);
                community_boundary_list_updates
                    .entry(*next_community)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);
            }

            // Process community based on size
            if !result.is_giant {
                // Collect small community data for batch writing
                let comm_csr_bytes = result.comm_csr_block.encode_topology();
                let length = comm_csr_bytes.len();

                small_community_metadata.push((result.community_id, current_write_offset, length));
                small_community_write_buffer.extend_from_slice(&comm_csr_bytes);
                current_write_offset += length;
            } else {
                // Collect giant communities for separate processing
                giant_communities_to_process.push(result);
            }
            pb.inc(1);
        }

        // ===== BATCH APPLY BOUNDARY UPDATES =====

        // Batch apply boundary adjacency map updates
        for (vertex, neighbors) in boundary_adj_updates {
            if neighbors.is_empty() {
                boundary_adj_map.insert(vertex, FxHashSet::default());
            } else {
                boundary_adj_map.insert(vertex, neighbors);
            }
        }

        // Batch apply community boundary updates
        for ((community_id, next_community), edges) in community_boundary_updates {
            community_boundary
                .entry((community_id, next_community))
                .or_insert_with(FxHashSet::default)
                .extend(edges);
        }

        // Batch apply community boundary list updates
        for (community_id, vertices) in community_boundary_list_updates {
            community_boundary_list_manager
                .entry(community_id)
                .or_insert_with(FxHashSet::default)
                .extend(vertices);
        }

        // ===== BATCH WRITE SMALL COMMUNITIES =====

        // Write all small community data in one operation
        println!("Writing Small File");
        if !small_community_write_buffer.is_empty() {
            #[cfg(windows)]
            {
                small_comm_store_file
                    .seek_write(&small_community_write_buffer, 0)
                    .unwrap();
            }
            #[cfg(unix)]
            {
                small_comm_store_file
                    .write_at(&small_community_write_buffer, 0)
                    .unwrap();
            }

            // Update community map and offset index based on batch write results
            for (community_id, offset, length) in small_community_metadata {
                community_offset_index.push(offset as u64);
                community_map.insert(community_id, Normal { offset, length });
            }
        }

        // ===== PROCESS GIANT COMMUNITIES INDIVIDUALLY =====

        // Process giant communities (these still need individual files)
        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(giant_communities_to_process.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("Flushing Giant Files.");
        for result in giant_communities_to_process.into_iter() {
            if let (Some(giant_comm_index), Some(giant_csr_byte_group)) =
                (&result.giant_comm_index, result.giant_csr_byte_group)
            {
                // Serialize and store giant community index
                let giant_comm_index_bytes = bincode::serialize(giant_comm_index.as_ref()).unwrap();
                let giant_comm_index_length = giant_comm_index_bytes.len();
                let giant_comm_length =
                    giant_csr_byte_group.iter().fold(0usize, |mut acc, item| {
                        acc += item.len();
                        acc
                    });

                community_map.insert(
                    result.community_id,
                    Giant {
                        scc_index_offset: giant_comm_index_length,
                        length: giant_comm_length,
                    },
                );
                giant_community_map.insert(result.community_id, giant_comm_index.clone());

                // Create separate storage file for giant community
                let giant_comm_name =
                    format!("{graph_name}_giant_comm{}_storage.bin", result.community_id);
                let giant_comm_path = dir_path.join(&giant_comm_name);
                let giant_comm_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&giant_comm_path)
                    .unwrap();

                // Prepare all data for batch writing
                let mut giant_write_buffer =
                    Vec::with_capacity(giant_comm_index_length + giant_comm_length);
                giant_write_buffer.extend_from_slice(&giant_comm_index_bytes);

                // Append all SCC data blocks
                for scc_csr_b in giant_csr_byte_group.iter() {
                    giant_write_buffer.extend_from_slice(scc_csr_b);
                }

                // Write all giant community data in one operation
                #[cfg(windows)]
                {
                    giant_comm_file.seek_write(&giant_write_buffer, 0).unwrap();
                }
                #[cfg(unix)]
                {
                    giant_comm_file.write_at(&giant_write_buffer, 0).unwrap();
                }
            }
            pb.inc(1);
        }

        // Update progress bar for processed small communities

        // Extract final results from Arc<Mutex<T>> wrappers
        let final_vc_map = Arc::try_unwrap(vc_map).unwrap();

        // Convert DashMap to regular HashMap for final result
        let final_boundary_adj_map: FxHashMap<u64, FxHashSet<u64>> = boundary_adj_map
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        let boundary_adj_csr = BoundaryCSR::build_from_boundary_adj(&final_boundary_adj_map);

        // Build final community index structure
        let community_index = CommunityIndex {
            community_map: community_map.into_iter().collect(),
            boundary_graph: BoundaryGraph {
                vertex_community_map: final_vc_map,
                boundary_adj_map: final_boundary_adj_map,
                boundary_csr: boundary_adj_csr,
                community_boundary: community_boundary.into_iter().collect(),
                community_boundary_list: community_boundary_list_manager.into_iter().collect(),
            },
        };

        // Extract file handle and create memory map for efficient access
        let final_small_comm_store_file = Arc::try_unwrap(small_comm_store_file).unwrap();
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&final_small_comm_store_file).unwrap() });

        // Serialize and save community index
        let community_index_bytes = bincode::serialize(&community_index).unwrap();
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)
            .unwrap();
        community_index_file
            .write_all(&community_index_bytes)
            .unwrap();

        // Return fully constructed storage instance
        Self {
            vertex_count: csr_memory.vertex_count,
            graph_name: graph_name.to_owned(),
            workspace: "cosmo.db".to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager: DashMap::<u32, Mmap>::new(),
        }
    }

    /// Parallel optimized version of community storage construction function
    ///
    /// Builds a community storage structure from a graph file using parallel processing
    /// to significantly improve performance on multicore systems. Combines optimizations
    /// from the sequential version with parallel community processing.
    ///
    /// # Arguments
    ///
    /// * `graph_file` - Path to the input graph file
    /// * `graph_name` - Name identifier for the graph (used in output file naming)
    /// * `giant_theta` - Threshold ratio for determining giant communities (0.0-1.0)
    ///
    /// # Returns
    ///
    /// A fully constructed `Self` instance with optimized community storage
    ///
    /// # Parallel Optimizations
    ///
    /// 1. Thread-safe data structures using Arc<Mutex<T>> and DashMap
    /// 2. Parallel community processing using rayon's par_iter()
    /// 3. Separate parallel computation and serial I/O phases
    /// 4. Lock contention minimization through local processing
    /// 5. Batch boundary updates to reduce synchronization overhead
    /// 6. Memory-efficient parallel result collection
    ///
    /// # Performance Benefits
    ///
    /// - Scales with available CPU cores for community processing
    /// - Maintains all sequential optimizations (pre-allocation, FxHashMap, etc.)
    /// - Reduces total processing time for large graphs with many communities
    pub fn build_from_graph_file_opt_par(
        graph_file: &str,
        graph_name: &str,
        giant_theta: f64,
    ) -> Self {
        // Load CSR graph from file and wrap in Arc for thread-safe sharing
        let csr_memory = Arc::new(CSRGraph::from_graph_file(graph_file));

        // Calculate threshold for identifying giant communities
        let giant_vertex_count = (csr_memory.vertex_count as f64 * giant_theta).ceil() as u64;

        // Initialize LRU caches for community and SCC data
        let community_cache = Cache::new(1000000);
        let scc_cache = Cache::new(1000000);

        // Optimization 1: Pre-compute community structure using Vec for fast lookups
        let mut community_structure = BTreeMap::<u32, BTreeSet<u64>>::new();
        let mut vc_map = Vec::<u32>::new();
        vc_map.resize(csr_memory.vertex_count as usize, 0u32);

        // Optimization 2: Batch build vertex-community map to reduce redundant access
        for (vertex_id, community_id) in &csr_memory.community_index {
            vc_map[*vertex_id as usize] = *community_id;
            community_structure
                .entry(*community_id)
                .or_insert_with(BTreeSet::new)
                .insert(*vertex_id);
        }

        // Wrap read-only data in Arc for thread-safe sharing
        let vc_map = Arc::new(vc_map);

        // Prepare storage file paths
        let dir_path = Path::new("cosmo.db");
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Create directory if it doesn't exist
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
            println!("Created directory: cosmo.db");
        }

        // Remove existing files to ensure clean state
        for path in &[&file_path, &index_path] {
            if path.exists() {
                let _ = fs::remove_file(path);
                println!("Removed existing file: {}", path.display());
            }
        }

        // Create storage file for small communities and wrap in Arc for thread sharing
        let small_comm_store_file = Arc::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap(),
        );
        println!("Created new file: {}", file_path.display());

        // Optimization 3: Pre-allocate capacity for all data structures
        let total_communities = community_structure.len();

        // Use DashMap for lock-free concurrent access to boundary_adj_map
        let boundary_adj_map = Arc::new(DashMap::<u64, FxHashSet<u64>>::new());
        let mut community_boundary = FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let mut community_map = FxHashMap::<u32, CommunityIndexItem>::default();
        let mut community_boundary_list_manager = FxHashMap::<u32, FxHashSet<u64>>::default();
        let mut community_offset_index = Vec::<u64>::with_capacity(total_communities);

        // Optimization 5: Pre-calculate estimated sizes for capacity pre-allocation
        let avg_degree = csr_memory.neighbor_list.len() / csr_memory.vertex_count as usize;

        // Optimization 7: Batch process boundary detection to reduce redundant computation
        let mut all_boundary_edges = FxHashMap::<u32, Vec<(u64, u64, u32)>>::default();

        // Pre-compute all boundary edges for each community
        for (community_id, community) in &community_structure {
            let mut boundary_edges = Vec::new();

            for vertex in community {
                let neighbors = csr_memory.read_neighbor(vertex);
                for neighbor in &neighbors {
                    if !community.contains(neighbor) {
                        // Use pre-computed vc_map for fast community lookup
                        let next_community = vc_map[*neighbor as usize];
                        boundary_edges.push((*vertex, *neighbor, next_community));
                    }
                }
            }

            if !boundary_edges.is_empty() {
                all_boundary_edges.insert(*community_id, boundary_edges);
            }
        }

        // Wrap boundary edges in Arc for thread-safe sharing
        let all_boundary_edges = Arc::new(all_boundary_edges);

        // Thread-safe collection for parallel processing results
        let processed_communities = Arc::new(Mutex::new(Vec::<ProcessedCommunityResult>::new()));

        // Convert community structure to vector for parallel iteration
        let communities: Vec<_> = community_structure.into_iter().collect();

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(communities.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Index Computing.");

        // Main parallel community processing loop using rayon
        communities
            .into_par_iter()
            .for_each(|(community_id, community)| {
                let community_size = community.len();

                // Optimization 8: Pre-allocate capacity based on community size
                let estimated_edges = community_size * avg_degree;
                let mut comm_vertex_list = Vec::<(u64, u64)>::with_capacity(community_size);
                let mut comm_neighbor_list = Vec::<u64>::with_capacity(estimated_edges);
                let mut comm_vertex_index = FxHashMap::<u64, usize>::with_capacity_and_hasher(
                    community_size,
                    Default::default(),
                );
                let mut comm_offset = 0u64;
                let mut community_boundary_list = Vec::with_capacity(community_size / 4);

                // Optimization 9: Batch process boundary information locally
                let boundary_edges = all_boundary_edges
                    .get(&community_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let mut boundary_vertices = FxHashSet::<u64>::default();
                let mut local_boundary_updates = Vec::new();

                // Process boundary edges and collect updates for later batch processing
                for (vertex, neighbor, next_community) in boundary_edges {
                    boundary_vertices.insert(*vertex);
                    local_boundary_updates.push((
                        *vertex,
                        *neighbor,
                        community_id,
                        *next_community,
                    ));
                }

                // Build community vertex list with optimized timestamp generation
                for (vertex_inner_idx, vertex) in community.iter().enumerate() {
                    let neighbors = csr_memory.read_neighbor(vertex);

                    // Check if vertex is on community boundary
                    if boundary_vertices.contains(vertex) {
                        community_boundary_list.push(*vertex);
                    }

                    // Optimization 10: Use fixed timestamp + increment to avoid system calls
                    comm_vertex_list.push((*vertex, comm_offset));

                    comm_vertex_index.insert(*vertex, vertex_inner_idx);
                    comm_offset += neighbors.len() as u64;

                    // Batch convert neighbors to Vertex objects
                    comm_neighbor_list.extend(neighbors);
                }

                // Build CSR block for this community
                let comm_vertex_count = community.len() as u64;
                let comm_csr_block = CSRSimpleCommBlock {
                    vertex_count: comm_vertex_count,
                    vertex_list: comm_vertex_list,
                    neighbor_list: comm_neighbor_list,
                    vertex_index: comm_vertex_index.into_iter().collect::<HashMap<_, _>>(),
                };

                // Process giant communities in parallel
                let (giant_comm_index, giant_csr_byte_group) =
                    if community.len() >= giant_vertex_count as usize {
                        let g_induced_comm = csr_memory
                            .induce_subgraph(&community.iter().cloned().collect::<Vec<_>>());
                        let (giant_index, giant_bytes) =
                            Self::process_giant_community(&g_induced_comm, &comm_csr_block);
                        (Some(Arc::new(giant_index)), Some(Arc::new(giant_bytes)))
                    } else {
                        (None, None)
                    };

                // Perform boundary enrichment in parallel using DashMap
                if community.len() < giant_vertex_count as usize {
                    // Normal community enrichment
                    Self::enrich_boundary_graph_normal_parallel(
                        &community_boundary_list,
                        &community,
                        &csr_memory,
                        &boundary_adj_map,
                    );
                } else if let Some(ref giant_index) = giant_comm_index {
                    // Giant community enrichment
                    // println!("Perform Giant Enr.");
                    Self::enrich_boundary_graph_giant_parallel(
                        &community_boundary_list,
                        &giant_index.scc_meta,
                        &boundary_adj_map,
                    );
                }

                // Create processing result
                let result = ProcessedCommunityResult {
                    community_id,
                    community_boundary_list,
                    comm_csr_block: Arc::new(comm_csr_block),
                    is_giant: community.len() >= giant_vertex_count as usize,
                    boundary_updates: local_boundary_updates,
                    community: Arc::new(community),
                    giant_comm_index,
                    giant_csr_byte_group,
                };

                // Store result in thread-safe collection
                processed_communities.lock().unwrap().push(result);

                // Update thread-safe progress bar
                pb.inc(1);
            });

        // Serial processing phase for file I/O and shared state updates
        let processed_communities = match Arc::try_unwrap(processed_communities) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(_) => {
                panic!("Cannot unwrap Arc, multiple references exist");
            }
        };

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(processed_communities.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Index Preparing.");

        // ===== OPTIMIZATION: Batch processing for boundary updates and small communities =====

        // Step 1: Collect all boundary updates for batch processing
        let mut boundary_adj_updates = FxHashMap::<u64, FxHashSet<u64>>::default();
        let mut community_boundary_updates =
            FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let mut community_boundary_list_updates = FxHashMap::<u32, FxHashSet<u64>>::default();

        // Step 2: Collect all small community data for batch file writing
        let mut small_community_write_buffer = Vec::new();
        let mut small_community_metadata = Vec::<(u32, usize, usize)>::new(); // (community_id, offset, length)
        let mut current_write_offset = 0usize;

        // Step 3: Process giant communities separately (they need individual files)
        let mut giant_communities_to_process = Vec::new();

        // Collect updates and prepare batch operations
        for result in processed_communities.into_iter() {
            // Collect boundary updates
            for (vertex, neighbor, community_id, next_community) in &result.boundary_updates {
                // Collect boundary adjacency updates
                boundary_adj_updates
                    .entry(*vertex)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);
                boundary_adj_updates
                    .entry(*neighbor)
                    .or_insert_with(FxHashSet::default);

                // Collect inter-community boundary updates
                community_boundary_updates
                    .entry((*community_id, *next_community))
                    .or_insert_with(FxHashSet::default)
                    .insert((*vertex, *neighbor));

                // Collect boundary vertex list updates
                community_boundary_list_updates
                    .entry(*community_id)
                    .or_insert_with(FxHashSet::default)
                    .insert(*vertex);
                community_boundary_list_updates
                    .entry(*next_community)
                    .or_insert_with(FxHashSet::default)
                    .insert(*neighbor);
            }

            // Process community based on size
            if !result.is_giant {
                // Collect small community data for batch writing
                let comm_csr_bytes = result.comm_csr_block.encode_topology();
                let length = comm_csr_bytes.len();

                small_community_metadata.push((result.community_id, current_write_offset, length));
                small_community_write_buffer.extend_from_slice(&comm_csr_bytes);
                current_write_offset += length;
            } else {
                // Collect giant communities for separate processing
                giant_communities_to_process.push(result);
            }
            pb.inc(1);
        }

        // ===== BATCH APPLY BOUNDARY UPDATES =====

        // Batch apply boundary adjacency map updates
        for (vertex, neighbors) in boundary_adj_updates {
            if neighbors.is_empty() {
                boundary_adj_map.insert(vertex, FxHashSet::default());
            } else {
                boundary_adj_map.insert(vertex, neighbors);
            }
        }

        // Batch apply community boundary updates
        for ((community_id, next_community), edges) in community_boundary_updates {
            community_boundary
                .entry((community_id, next_community))
                .or_insert_with(FxHashSet::default)
                .extend(edges);
        }

        // Batch apply community boundary list updates
        for (community_id, vertices) in community_boundary_list_updates {
            community_boundary_list_manager
                .entry(community_id)
                .or_insert_with(FxHashSet::default)
                .extend(vertices);
        }

        // ===== BATCH WRITE SMALL COMMUNITIES =====

        // Write all small community data in one operation
        println!("Writing Small File");
        if !small_community_write_buffer.is_empty() {
            #[cfg(windows)]
            {
                small_comm_store_file
                    .seek_write(&small_community_write_buffer, 0)
                    .unwrap();
            }
            #[cfg(unix)]
            {
                small_comm_store_file
                    .write_at(&small_community_write_buffer, 0)
                    .unwrap();
            }

            // Update community map and offset index based on batch write results
            for (community_id, offset, length) in small_community_metadata {
                community_offset_index.push(offset as u64);
                community_map.insert(community_id, Normal { offset, length });
            }
        }

        // ===== PROCESS GIANT COMMUNITIES INDIVIDUALLY =====

        // Process giant communities (these still need individual files)
        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(giant_communities_to_process.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("Flushing Giant Files.");
        for result in giant_communities_to_process.into_iter() {
            if let (Some(giant_comm_index), Some(giant_csr_byte_group)) =
                (&result.giant_comm_index, result.giant_csr_byte_group)
            {
                // Serialize and store giant community index
                let giant_comm_index_bytes = bincode::serialize(giant_comm_index.as_ref()).unwrap();
                let giant_comm_index_length = giant_comm_index_bytes.len();
                let giant_comm_length =
                    giant_csr_byte_group.iter().fold(0usize, |mut acc, item| {
                        acc += item.len();
                        acc
                    });

                community_map.insert(
                    result.community_id,
                    Giant {
                        scc_index_offset: giant_comm_index_length,
                        length: giant_comm_length,
                    },
                );
                giant_community_map.insert(result.community_id, giant_comm_index.clone());

                // Create separate storage file for giant community
                let giant_comm_name =
                    format!("{graph_name}_giant_comm{}_storage.bin", result.community_id);
                let giant_comm_path = dir_path.join(&giant_comm_name);
                let giant_comm_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&giant_comm_path)
                    .unwrap();

                // Prepare all data for batch writing
                let mut giant_write_buffer =
                    Vec::with_capacity(giant_comm_index_length + giant_comm_length);
                giant_write_buffer.extend_from_slice(&giant_comm_index_bytes);

                // Append all SCC data blocks
                for scc_csr_b in giant_csr_byte_group.iter() {
                    giant_write_buffer.extend_from_slice(scc_csr_b);
                }

                // Write all giant community data in one operation
                #[cfg(windows)]
                {
                    giant_comm_file.seek_write(&giant_write_buffer, 0).unwrap();
                }
                #[cfg(unix)]
                {
                    giant_comm_file.write_at(&giant_write_buffer, 0).unwrap();
                }
            }
            pb.inc(1);
        }

        // Update progress bar for processed small communities

        // Extract final results from Arc<Mutex<T>> wrappers
        let final_vc_map = Arc::try_unwrap(vc_map).unwrap();

        // Convert DashMap to regular HashMap for final result
        let final_boundary_adj_map: FxHashMap<u64, FxHashSet<u64>> = boundary_adj_map
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        let boundary_adj_csr = BoundaryCSR::build_from_boundary_adj(&final_boundary_adj_map);

        // Build final community index structure
        let community_index = CommunityIndex {
            community_map: community_map.into_iter().collect(),
            boundary_graph: BoundaryGraph {
                vertex_community_map: final_vc_map,
                boundary_adj_map: final_boundary_adj_map,
                boundary_csr: boundary_adj_csr,
                community_boundary: community_boundary.into_iter().collect(),
                community_boundary_list: community_boundary_list_manager.into_iter().collect(),
            },
        };

        // Extract file handle and create memory map for efficient access
        let final_small_comm_store_file = Arc::try_unwrap(small_comm_store_file).unwrap();
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&final_small_comm_store_file).unwrap() });

        // Serialize and save community index
        let community_index_bytes = bincode::serialize(&community_index).unwrap();
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)
            .unwrap();
        community_index_file
            .write_all(&community_index_bytes)
            .unwrap();

        // Return fully constructed storage instance
        Self {
            vertex_count: csr_memory.vertex_count,
            graph_name: graph_name.to_owned(),
            workspace: "cosmo.db".to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager: DashMap::<u32, Mmap>::new(),
        }
    }

    /// Parallel optimized version of community storage construction function
    ///
    /// Builds a community storage structure from a graph file using parallel processing
    /// to significantly improve performance on multicore systems. Combines optimizations
    /// from the sequential version with parallel community processing.
    ///
    /// # Arguments
    ///
    /// * `graph_file` - Path to the input graph file
    /// * `graph_name` - Name identifier for the graph (used in output file naming)
    /// * `giant_theta` - Threshold ratio for determining giant communities (0.0-1.0)
    ///
    /// # Returns
    ///
    /// A fully constructed `Self` instance with optimized community storage
    ///
    /// # Parallel Optimizations
    ///
    /// 1. Thread-safe data structures using Arc<Mutex<T>> and DashMap
    /// 2. Parallel community processing using rayon's par_iter()
    /// 3. Separate parallel computation and serial I/O phases
    /// 4. Lock contention minimization through local processing
    /// 5. Batch boundary updates to reduce synchronization overhead
    /// 6. Memory-efficient parallel result collection
    ///
    /// # Performance Benefits
    ///
    /// - Scales with available CPU cores for community processing
    /// - Maintains all sequential optimizations (pre-allocation, FxHashMap, etc.)
    /// - Reduces total processing time for large graphs with many communities
    pub fn build_from_graph_file_opt_par_ablation(
        graph_file: &str,
        graph_name: &str,
        giant_theta: f64,
    ) -> Self {
        // Load CSR graph from file and wrap in Arc for thread-safe sharing
        let csr_memory = Arc::new(CSRGraph::from_graph_file(graph_file));

        // Calculate threshold for identifying giant communities
        let giant_vertex_count = (csr_memory.vertex_count as f64 * giant_theta).ceil() as u64;

        // Initialize LRU caches for community and SCC data
        let community_cache = Cache::new(1000000);
        let scc_cache = Cache::new(1000000);

        // Optimization 1: Pre-compute community structure using Vec for fast lookups
        let mut community_structure = BTreeMap::<u32, BTreeSet<u64>>::new();
        let mut vc_map = Vec::<u32>::new();
        vc_map.resize(csr_memory.vertex_count as usize, 0u32);

        // Optimization 2: Batch build vertex-community map to reduce redundant access
        for (vertex_id, community_id) in &csr_memory.community_index {
            vc_map[*vertex_id as usize] = *community_id;
            community_structure
                .entry(*community_id)
                .or_insert_with(BTreeSet::new)
                .insert(*vertex_id);
        }

        // Wrap read-only data in Arc for thread-safe sharing
        let vc_map = Arc::new(vc_map);
        let working_dir = format!("cosmo{}.db", giant_theta);

        // Prepare storage file paths
        let dir_path = Path::new(&working_dir);
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Create directory if it doesn't exist
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
            println!("Created directory: cosmo.db");
        }

        // Remove existing files to ensure clean state
        for path in &[&file_path, &index_path] {
            if path.exists() {
                let _ = fs::remove_file(path);
                println!("Removed existing file: {}", path.display());
            }
        }

        // Create storage file for small communities and wrap in Arc for thread sharing
        let small_comm_store_file = Arc::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap(),
        );
        println!("Created new file: {}", file_path.display());

        // Optimization 3: Pre-allocate capacity for all data structures
        let total_communities = community_structure.len();

        // Use DashMap for lock-free concurrent access to boundary_adj_map
        let boundary_adj_map = Arc::new(DashMap::<u64, FxHashSet<u64>>::new());
        let community_boundary = Arc::new(Mutex::new(
            FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default(),
        ));
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let community_map = Arc::new(Mutex::new(FxHashMap::<u32, CommunityIndexItem>::default()));
        let community_boundary_list_manager =
            Arc::new(Mutex::new(FxHashMap::<u32, FxHashSet<u64>>::default()));
        let current_community_offset = Arc::new(Mutex::new(0u64));
        let community_offset_index =
            Arc::new(Mutex::new(Vec::<u64>::with_capacity(total_communities)));

        // Optimization 5: Pre-calculate estimated sizes for capacity pre-allocation
        let avg_degree = csr_memory.neighbor_list.len() / csr_memory.vertex_count as usize;

        // Optimization 7: Batch process boundary detection to reduce redundant computation
        let mut all_boundary_edges = FxHashMap::<u32, Vec<(u64, u64, u32)>>::default();

        // Pre-compute all boundary edges for each community
        for (community_id, community) in &community_structure {
            let mut boundary_edges = Vec::new();

            for vertex in community {
                let neighbors = csr_memory.read_neighbor(vertex);
                for neighbor in &neighbors {
                    if !community.contains(neighbor) {
                        // Use pre-computed vc_map for fast community lookup
                        let next_community = vc_map[*neighbor as usize];
                        boundary_edges.push((*vertex, *neighbor, next_community));
                    }
                }
            }

            if !boundary_edges.is_empty() {
                all_boundary_edges.insert(*community_id, boundary_edges);
            }
        }

        // Wrap boundary edges in Arc for thread-safe sharing
        let all_boundary_edges = Arc::new(all_boundary_edges);

        // Thread-safe collection for parallel processing results
        let processed_communities = Arc::new(Mutex::new(Vec::<ProcessedCommunityResult>::new()));

        // Convert community structure to vector for parallel iteration
        let communities: Vec<_> = community_structure.into_iter().collect();

        // Setup thread-safe progress bar for user feedback
        let pb = Arc::new(ProgressBar::new(communities.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Index Computing.");

        // Main parallel community processing loop using rayon
        communities
            .into_par_iter()
            .for_each(|(community_id, community)| {
                let community_size = community.len();

                // Optimization 8: Pre-allocate capacity based on community size
                let estimated_edges = community_size * avg_degree;
                let mut comm_vertex_list = Vec::<(u64, u64)>::with_capacity(community_size);
                let mut comm_neighbor_list = Vec::<u64>::with_capacity(estimated_edges);
                let mut comm_vertex_index = FxHashMap::<u64, usize>::with_capacity_and_hasher(
                    community_size,
                    Default::default(),
                );
                let mut comm_offset = 0u64;
                let mut community_boundary_list = Vec::with_capacity(community_size / 4);

                // Optimization 9: Batch process boundary information locally
                let boundary_edges = all_boundary_edges
                    .get(&community_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let mut boundary_vertices = FxHashSet::<u64>::default();
                let mut local_boundary_updates = Vec::new();

                // Process boundary edges and collect updates for later batch processing
                for (vertex, neighbor, next_community) in boundary_edges {
                    boundary_vertices.insert(*vertex);
                    local_boundary_updates.push((
                        *vertex,
                        *neighbor,
                        community_id,
                        *next_community,
                    ));
                }

                // Build community vertex list with optimized timestamp generation
                for (vertex_inner_idx, vertex) in community.iter().enumerate() {
                    let neighbors = csr_memory.read_neighbor(vertex);

                    // Check if vertex is on community boundary
                    if boundary_vertices.contains(vertex) {
                        community_boundary_list.push(*vertex);
                    }

                    // Optimization 10: Use fixed timestamp + increment to avoid system calls
                    comm_vertex_list.push((*vertex, comm_offset));

                    comm_vertex_index.insert(*vertex, vertex_inner_idx);
                    comm_offset += neighbors.len() as u64;

                    // Batch convert neighbors to Vertex objects
                    comm_neighbor_list.extend(neighbors);
                }

                // Build CSR block for this community
                let comm_vertex_count = community.len() as u64;
                let comm_csr_block = CSRSimpleCommBlock {
                    vertex_count: comm_vertex_count,
                    vertex_list: comm_vertex_list,
                    neighbor_list: comm_neighbor_list,
                    vertex_index: comm_vertex_index.into_iter().collect::<HashMap<_, _>>(),
                };

                // Process giant communities in parallel
                let (giant_comm_index, giant_csr_byte_group) =
                    if community.len() >= giant_vertex_count as usize {
                        let g_induced_comm = csr_memory
                            .induce_subgraph(&community.iter().cloned().collect::<Vec<_>>());
                        let (giant_index, giant_bytes) =
                            Self::process_giant_community(&g_induced_comm, &comm_csr_block);
                        (Some(Arc::new(giant_index)), Some(Arc::new(giant_bytes)))
                    } else {
                        (None, None)
                    };

                // Perform boundary enrichment in parallel using DashMap
                if community.len() < giant_vertex_count as usize {
                    // Normal community enrichment
                    Self::enrich_boundary_graph_normal_parallel(
                        &community_boundary_list,
                        &community,
                        &csr_memory,
                        &boundary_adj_map,
                    );
                } else if let Some(ref giant_index) = giant_comm_index {
                    // Giant community enrichment
                    // println!("Perform Giant Enr.");
                    Self::enrich_boundary_graph_giant_parallel(
                        &community_boundary_list,
                        &giant_index.scc_meta,
                        &boundary_adj_map,
                    );
                }

                // Create processing result
                let result = ProcessedCommunityResult {
                    community_id,
                    community_boundary_list,
                    comm_csr_block: Arc::new(comm_csr_block),
                    is_giant: community.len() >= giant_vertex_count as usize,
                    boundary_updates: local_boundary_updates,
                    community: Arc::new(community),
                    giant_comm_index,
                    giant_csr_byte_group,
                };

                // Store result in thread-safe collection
                processed_communities.lock().unwrap().push(result);

                // Update thread-safe progress bar
                pb.inc(1);
            });

        // Serial processing phase for file I/O and shared state updates
        let processed_communities = processed_communities.lock().unwrap();

        // Process results sequentially to avoid I/O contention
        for result in processed_communities.iter() {
            // Update shared boundary mappings with batch operations
            {
                let mut community_boundary_guard = community_boundary.lock().unwrap();
                let mut community_boundary_list_guard =
                    community_boundary_list_manager.lock().unwrap();

                // Apply all boundary updates for this community in batch
                for (vertex, neighbor, community_id, next_community) in &result.boundary_updates {
                    // Update boundary adj_map.
                    boundary_adj_map
                        .entry(*vertex)
                        .or_insert_with(FxHashSet::default)
                        .insert(*neighbor);
                    boundary_adj_map
                        .entry(*neighbor)
                        .or_insert_with(FxHashSet::default);

                    // Update inter-community boundary mapping
                    community_boundary_guard
                        .entry((*community_id, *next_community))
                        .or_insert_with(FxHashSet::default)
                        .insert((*vertex, *neighbor));

                    // Update boundary vertex lists for both communities
                    community_boundary_list_guard
                        .entry(*community_id)
                        .or_insert_with(FxHashSet::default)
                        .insert(*vertex);

                    community_boundary_list_guard
                        .entry(*next_community)
                        .or_insert_with(FxHashSet::default)
                        .insert(*neighbor);
                }
            }

            // Process community based on size (small vs giant)
            if !result.is_giant {
                // Handle normal (small) community
                let comm_csr_bytes = result.comm_csr_block.encode_topology();

                // Synchronized file writing with offset management
                let mut offset_guard = current_community_offset.lock().unwrap();
                let mut index_guard = community_offset_index.lock().unwrap();
                let mut map_guard = community_map.lock().unwrap();

                index_guard.push(*offset_guard);
                map_guard.insert(
                    result.community_id,
                    Normal {
                        offset: *offset_guard as usize,
                        length: comm_csr_bytes.len(),
                    },
                );

                // Write community data to storage file
                #[cfg(windows)]
                {
                    small_comm_store_file
                        .seek_write(&comm_csr_bytes, *offset_guard)
                        .unwrap();
                }
                #[cfg(unix)]
                {
                    small_comm_store_file
                        .write_at(&comm_csr_bytes, *offset_guard)
                        .unwrap();
                }
                *offset_guard += comm_csr_bytes.len() as u64;
            } else {
                // Handle giant community with pre-computed data
                if let (Some(giant_comm_index), Some(giant_csr_byte_group)) =
                    (&result.giant_comm_index, &result.giant_csr_byte_group)
                {
                    // Serialize and store giant community index
                    let giant_comm_index_bytes =
                        bincode::serialize(giant_comm_index.as_ref()).unwrap();
                    let giant_comm_index_length = giant_comm_index_bytes.len();
                    let giant_comm_length =
                        giant_csr_byte_group.iter().fold(0usize, |mut acc, item| {
                            acc += item.len();
                            acc
                        });

                    community_map.lock().unwrap().insert(
                        result.community_id,
                        Giant {
                            scc_index_offset: giant_comm_index_length,
                            length: giant_comm_length,
                        },
                    );
                    giant_community_map.insert(result.community_id, giant_comm_index.clone());

                    // Create separate storage file for giant community
                    let giant_comm_name =
                        format!("{graph_name}_giant_comm{}_storage.bin", result.community_id);
                    let giant_comm_path = dir_path.join(&giant_comm_name);
                    let giant_comm_file = fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&giant_comm_path)
                        .unwrap();

                    // Write giant community data sequentially
                    let mut current_write_ptr = 0u64;
                    #[cfg(windows)]
                    {
                        giant_comm_file
                            .seek_write(&giant_comm_index_bytes, 0)
                            .unwrap();
                    }
                    #[cfg(unix)]
                    {
                        giant_comm_file
                            .write_at(&giant_comm_index_bytes, 0)
                            .unwrap();
                    }
                    current_write_ptr += giant_comm_index_length as u64;

                    // Write each SCC data block
                    for scc_csr_b in giant_csr_byte_group.iter() {
                        #[cfg(windows)]
                        {
                            giant_comm_file
                                .seek_write(scc_csr_b, current_write_ptr)
                                .unwrap();
                        }
                        #[cfg(unix)]
                        {
                            giant_comm_file
                                .write_at(scc_csr_b, current_write_ptr)
                                .unwrap();
                        }
                        current_write_ptr += scc_csr_b.len() as u64;
                    }
                }
            }
        }

        // Extract final results from Arc<Mutex<T>> wrappers
        let final_community_map = Arc::try_unwrap(community_map)
            .unwrap()
            .into_inner()
            .unwrap();
        let final_community_boundary = Arc::try_unwrap(community_boundary)
            .unwrap()
            .into_inner()
            .unwrap();
        let final_community_boundary_list = Arc::try_unwrap(community_boundary_list_manager)
            .unwrap()
            .into_inner()
            .unwrap();
        let final_vc_map = Arc::try_unwrap(vc_map).unwrap();

        // Convert DashMap to regular HashMap for final result
        let final_boundary_adj_map: FxHashMap<u64, FxHashSet<u64>> = boundary_adj_map
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        let boundary_adj_csr = BoundaryCSR::build_from_boundary_adj(&final_boundary_adj_map);
        // Build final community index structure
        let community_index = CommunityIndex {
            community_map: final_community_map.into_iter().collect(),
            boundary_graph: BoundaryGraph {
                vertex_community_map: final_vc_map,
                boundary_adj_map: final_boundary_adj_map,
                boundary_csr: boundary_adj_csr,
                community_boundary: final_community_boundary.into_iter().collect(),
                community_boundary_list: final_community_boundary_list.into_iter().collect(),
            },
        };

        // Extract file handle and create memory map for efficient access
        let final_small_comm_store_file = Arc::try_unwrap(small_comm_store_file).unwrap();
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&final_small_comm_store_file).unwrap() });

        // Serialize and save community index
        let community_index_bytes = bincode::serialize(&community_index).unwrap();
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)
            .unwrap();
        community_index_file
            .write_all(&community_index_bytes)
            .unwrap();

        // Return fully constructed storage instance
        Self {
            vertex_count: csr_memory.vertex_count,
            graph_name: graph_name.to_owned(),
            workspace: format!("cosmo{}.db", giant_theta).to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager: DashMap::<u32, Mmap>::new(),
        }
    }

    /// Constructs a `CommunityStorage` object from a graph file and performs community structure processing.
    ///
    /// This function reads a graph from a file, processes the community structure, and builds the internal
    /// storage for the graph's community data. It handles both small and giant communities by using different
    /// methods for encoding, storing, and indexing. It also manages Strongly Connected Components (SCCs) within
    /// the communities and efficiently stores the data using memory-mapping and caching mechanisms.
    ///
    /// # Arguments
    ///
    /// * `graph_file` - The path to the graph file.
    /// * `graph_name` - A string identifier for the graph.
    /// * `giant_theta` - A threshold value used to determine whether a community is considered "giant".
    ///
    /// # Returns
    ///
    /// * `Self` - The constructed `CommunityStorage` object.
    pub fn build_from_graph_file(graph_file: &str, graph_name: &str, giant_theta: f64) -> Self {
        // Load a csr graph from file.
        let csr_memory = CSRGraph::from_graph_file(graph_file);

        // Calculate the threshold for giant communities.
        let giant_vertex_count = (csr_memory.vertex_count as f64 * giant_theta).ceil() as u64;

        // Initialize caches for regular communities and SCCs.
        let community_cache = Cache::new(1000000);
        let scc_cache = Cache::new(1000000);

        // Process the community structure.
        let mut community_structure = BTreeMap::<u32, BTreeSet<u64>>::new();
        let mut vc_map = Vec::<u32>::new();
        vc_map.resize(csr_memory.vertex_count as usize, 0u32);

        // Populate the community structure and map vertices to their community IDs.
        for (vertex_id, community_id) in &csr_memory.community_index {
            vc_map[*vertex_id as usize] = *community_id;
            community_structure
                .entry(*community_id)
                .or_insert_with(BTreeSet::new)
                .insert(*vertex_id);
        }

        // Prepare the storage file paths.
        let dir_path = Path::new("cosmo.db");
        let file_name = format!("{graph_name}_comm_small_storage.bin");
        let comm_index_name = format!("{graph_name}_comm_index.bin");

        let file_path = dir_path.join(&file_name);
        let index_path = dir_path.join(&comm_index_name);

        // Create necessary directories and remove existing files if they exist.
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
            println!("Created directory: cosmo.db");
        }
        for path in &[&file_path, &index_path] {
            if path.exists() {
                let _ = fs::remove_file(path);
                println!("Removed existing file: {}", path.display());
            }
        }

        // Create the storage file for small communities.
        let small_comm_store_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();
        println!("Created new file: {}", file_path.display());

        // Initialize some basic data structures for community processing.
        let mut community_offset_index = Vec::<u64>::new();
        let mut current_community_offset = 0u64;
        let mut boundary_adj_map = FxHashMap::<u64, FxHashSet<u64>>::default();
        let mut community_boundary = FxHashMap::<(u32, u32), FxHashSet<(u64, u64)>>::default();
        let giant_community_map = DashMap::<u32, Arc<GiantCommunityIndex>>::new();
        let mut community_map = FxHashMap::<u32, CommunityIndexItem>::default();
        let mut community_boundary_list_manager = FxHashMap::<u32, FxHashSet<u64>>::default();

        // Create a progress bar for monitoring the community processing.
        let pb = ProgressBar::new(community_structure.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        pb.set_message("COSMO Storage Building.");

        // Iterate over all the communities and process each one.
        for (community_id, community) in community_structure.into_iter() {
            let mut comm_vertex_list = Vec::<(u64, u64)>::new();
            let mut comm_neighbor_list = Vec::<u64>::new();
            let mut comm_vertex_index = HashMap::<u64, usize>::new();
            let mut comm_offset = 0u64;
            let mut community_boundary_list = vec![];

            // Process each vertex in the community.
            for (vertex_inner_idx, vertex) in community.iter().enumerate() {
                // Retrieve all the neighbors of the vertex and process boundary edges.
                let mut is_boundary = false;
                let neighbors = csr_memory.read_neighbor(vertex);
                neighbors.iter().for_each(|n| {
                    if !community.contains(n) {
                        // This neighbor is a boundary, this edge is a boundary edge.
                        is_boundary = true;
                        let next_community = *csr_memory.community_index.get(n).unwrap();
                        boundary_adj_map
                            .entry(*vertex)
                            .or_insert_with(FxHashSet::default)
                            .insert(*n);
                        if !boundary_adj_map.contains_key(n) {
                            boundary_adj_map.insert(*n, FxHashSet::default());
                        }
                        community_boundary
                            .entry((community_id, next_community))
                            .or_insert_with(FxHashSet::default)
                            .insert((*vertex, *n));
                        if !community_boundary_list_manager.contains_key(&community_id) {
                            community_boundary_list_manager
                                .insert(community_id, FxHashSet::<u64>::default());
                        }
                        community_boundary_list_manager
                            .get_mut(&community_id)
                            .unwrap()
                            .insert(*vertex);
                        if !community_boundary_list_manager.contains_key(&next_community) {
                            community_boundary_list_manager
                                .insert(next_community, FxHashSet::<u64>::default());
                        }
                        community_boundary_list_manager
                            .get_mut(&next_community)
                            .unwrap()
                            .insert(*n);
                    }
                });

                // If it is a boundary.
                if is_boundary {
                    community_boundary_list.push(*vertex);
                }

                // Build the community vertex list and the index.
                comm_vertex_list.push((*vertex, comm_offset));

                comm_vertex_index.insert(*vertex, vertex_inner_idx);
                comm_offset += neighbors.len() as u64;
                comm_neighbor_list.append(&mut neighbors.clone())
            }

            // Process the community into a CSR block.
            let comm_vertex_count = community.len() as u64;
            let comm_csr_block = CSRSimpleCommBlock {
                vertex_count: comm_vertex_count,
                vertex_list: comm_vertex_list,
                neighbor_list: comm_neighbor_list,
                vertex_index: comm_vertex_index,
            };

            // Process the small community.
            if community.len() < giant_vertex_count as usize {
                Self::enrich_boundary_graph_normal(
                    community_boundary_list,
                    &community,
                    &csr_memory,
                    &mut boundary_adj_map,
                );
                // Encode and save the small community data.
                let comm_csr_bytes = comm_csr_block.encode_topology();
                community_offset_index.push(current_community_offset);
                community_map.insert(
                    community_id,
                    Normal {
                        offset: current_community_offset as usize,
                        length: comm_csr_bytes.len(),
                    },
                );
                #[cfg(windows)]
                {
                    small_comm_store_file
                        .seek_write(&comm_csr_bytes, current_community_offset)
                        .unwrap();
                }
                #[cfg(unix)]
                {
                    small_comm_store_file
                        .write_at(&comm_csr_bytes, current_community_offset)
                        .unwrap();
                }
                current_community_offset += comm_csr_bytes.len() as u64;
            } else {
                // Process the giant community.
                let g_induced_comm =
                    csr_memory.induce_subgraph(&community.iter().cloned().collect::<Vec<_>>());
                let (giant_comm_index, giant_csr_byte_group) =
                    Self::process_giant_community(&g_induced_comm, &comm_csr_block);

                Self::enrich_boundary_graph_giant(
                    community_boundary_list,
                    &giant_comm_index.scc_meta,
                    &mut boundary_adj_map,
                );

                // Serialize the giant community index and save the data.
                let giant_comm_index_bytes = bincode::serialize(&giant_comm_index).unwrap();
                let giant_comm_index_length = giant_comm_index_bytes.len();
                let giant_comm_length =
                    giant_csr_byte_group.iter().fold(0usize, |mut acc, item| {
                        acc += item.len();
                        acc
                    });
                community_map.insert(
                    community_id,
                    Giant {
                        scc_index_offset: giant_comm_index_length,
                        length: giant_comm_length,
                    },
                );
                giant_community_map.insert(community_id, Arc::new(giant_comm_index));

                // Save the giant community data to a new file.
                let giant_comm_name =
                    format!("{graph_name}_giant_comm{}_storage.bin", community_id);
                let giant_comm_path = dir_path.join(&giant_comm_name);
                let giant_comm_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&giant_comm_path)
                    .unwrap();

                // Write the giant community data to the file.
                let mut current_write_ptr = 0u64;
                #[cfg(windows)]
                {
                    giant_comm_file
                        .seek_write(&giant_comm_index_bytes, 0)
                        .unwrap();
                }
                #[cfg(unix)]
                {
                    giant_comm_file
                        .write_at(&giant_comm_index_bytes, 0)
                        .unwrap();
                }
                current_write_ptr += giant_comm_index_length as u64;

                // Write the SCC data.
                for scc_csr_b in giant_csr_byte_group.into_iter() {
                    #[cfg(windows)]
                    {
                        giant_comm_file
                            .seek_write(&scc_csr_b, current_write_ptr)
                            .unwrap();
                    }
                    #[cfg(unix)]
                    {
                        giant_comm_file
                            .write_at(&scc_csr_b, current_write_ptr)
                            .unwrap();
                    }
                    current_write_ptr += scc_csr_b.len() as u64;
                }
            }
            pb.inc(1);
        }

        let boundary_csr = BoundaryCSR::build_from_boundary_adj(&boundary_adj_map);
        println!("Build boundary csr: {:?}", boundary_csr);

        // Finalize community index and memory map.
        let community_index = CommunityIndex {
            community_map,
            boundary_graph: BoundaryGraph {
                vertex_community_map: vc_map,
                boundary_adj_map,
                boundary_csr,
                community_boundary,
                community_boundary_list: community_boundary_list_manager,
            },
        };

        // Create memory map for the normal community storage.
        let normal_mem_map = Arc::new(unsafe { Mmap::map(&small_comm_store_file).unwrap() });

        // Save the community index to a file.
        let community_index_bytes = bincode::serialize(&community_index).unwrap();
        let mut community_index_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)
            .unwrap();
        community_index_file
            .write_all(&community_index_bytes)
            .unwrap();

        // Return the constructed `CommunityStorage`.
        Self {
            vertex_count: csr_memory.vertex_count,
            graph_name: graph_name.to_owned(),
            workspace: "cosmo.db".to_owned(),
            community_index,
            community_cache,
            giant_community_map,
            scc_cache,
            normal_mem_map,
            giant_mem_map_manager: DashMap::<u32, Mmap>::new(),
        }
    }

    /// Processes a giant community by generating its Strongly Connected Components (SCC),
    /// creating CSR blocks for each SCC, and returning the serialized data for storage.
    ///
    /// This function operates on a given community subgraph and creates the corresponding
    /// CSR blocks for each SCC. It returns both the community index (including SCC metadata)
    /// and the serialized data for all SCCs, which will be used for storage in the community system.
    ///
    /// # Arguments
    ///
    /// * `g_comm` - A reference to a `CSRSubGraph` representing the giant community. It holds
    ///   the structure of the graph and the necessary data for processing the community.
    /// * `g_block` - A `CSRSimpleCommBlock` containing the community's compressed sparse row (CSR)
    ///   block, used to access the graph's vertex and neighbor information.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `GiantCommunityIndex` - Contains metadata about the SCCs and the offsets for each SCC's CSR block.
    /// * `Vec<Vec<u8>>` - A vector of byte vectors, where each byte vector represents the serialized
    ///   CSR block for each SCC.
    pub fn process_giant_community(
        g_comm: &CSRSubGraph<u64, u64, u64>,
        g_block: &CSRSimpleCommBlock,
    ) -> (GiantCommunityIndex, Vec<Vec<u8>>) {
        // Generate SCC metadata from the community subgraph
        let scc_meta = SCCMeta::build_from_subgraph(g_comm);

        let mut current_scc_offset = 0u64; // Keep track of the current offset for each SCC
        let mut decoded_bytes_list = Vec::<Vec<u8>>::new(); // List to store the serialized byte data for each SCC
        let mut scc_offset_map = BTreeMap::<u64, u64>::new(); // Map to track the offsets of each SCC

        // Iterate through each SCC group (each SCC contains a set of vertices)
        for (scc_id, scc_group) in scc_meta.scc_list.iter().enumerate() {
            // Prepare lists to store the data for this SCC
            let mut scc_vertex_list = Vec::<(u64, u64)>::new(); // List of vertices with their offsets
            let mut scc_neighbor_list = Vec::<u64>::new(); // List of neighboring vertices
            let mut scc_vertex_index = HashMap::<u64, usize>::new(); // Index mapping vertices to their position in the list
            let mut scc_offset = 0u64; // Offset for this SCC's CSR block

            // Process each vertex in the current SCC
            for (vertex_inner_idx, vertex) in scc_group.iter().enumerate() {
                // Retrieve neighbors of the current vertex from the community block
                let neighbors = g_block.read_neighbor(vertex);

                // Add the vertex and its offset to the vertex list
                scc_vertex_list.push((*vertex, scc_offset));

                // Add the vertex to the vertex index
                scc_vertex_index.insert(*vertex, vertex_inner_idx);

                // Update the offset based on the number of neighbors
                scc_offset += neighbors.len() as u64;

                // Add each neighbor to the neighbor list, with a timestamp
                scc_neighbor_list.append(&mut neighbors.clone());
            }

            // After processing all the vertices, we create the CSR block for this SCC
            let scc_vertex_count = scc_group.len() as u64;
            let scc_csr_block = CSRSimpleCommBlock {
                vertex_count: scc_vertex_count,
                vertex_list: scc_vertex_list,
                neighbor_list: scc_neighbor_list,
                vertex_index: scc_vertex_index,
            };

            // Encode the SCC CSR block into bytes
            let scc_csr_bytes = scc_csr_block.encode_topology();
            let scc_csr_bytes_length = scc_csr_bytes.len() as u64;

            // Store the serialized bytes for this SCC
            decoded_bytes_list.push(scc_csr_bytes);

            // Update the offset map with the current SCC's offset
            scc_offset_map.insert(scc_id as u64, current_scc_offset);
            current_scc_offset += scc_csr_bytes_length;
        }

        // Convert the offset map into a vector of offsets for the SCC index
        let scc_index = scc_offset_map.values().cloned().collect::<Vec<_>>();

        // Return the processed data:
        // 1. The GiantCommunityIndex containing the SCC metadata and the index for SCCs.
        // 2. The list of byte vectors representing the serialized CSR blocks of each SCC.
        (
            GiantCommunityIndex {
                scc_meta,
                scc_index: SCCIndex(scc_index),
            },
            decoded_bytes_list,
        )
    }

    /// Enriches the boundary graph for a given community by processing the subgraph and identifying
    /// the strongly connected components (SCCs) within the boundary vertices. The function updates the
    /// provided `boundary_adj_map` with the enriched boundary adjacency information.
    ///
    /// This function first induces a subgraph using the given community vertices (`comm_vertex_list`)
    /// and then builds the SCC metadata from this induced subgraph. It then proceeds to enrich the boundary
    /// graph using the helper function `enrich_boundary_graph_giant`.
    ///
    /// # Arguments
    ///
    /// * `boundary_list` - A vector containing the list of boundary vertices. These vertices represent
    ///   the boundary of the community, and the function will enrich their adjacency information.
    /// * `comm_vertex_list` - A reference to a BTreeSet of vertices that are part of the community. This
    ///   set is used to induce the subgraph that represents the community's structure.
    /// * `whole_graph` - A reference to the entire graph (`CSRGraph`) that holds the entire network. The
    ///   community's subgraph will be induced from this graph based on the vertices in `comm_vertex_list`.
    /// * `boundary_adj_map` - A mutable reference to a `HashMap` that holds the adjacency information for
    ///   the boundary vertices. This map will be updated with the enriched adjacency data for the boundary.
    ///
    /// # Returns
    ///
    /// This function does not return a value. It updates the `boundary_adj_map` in place with the enriched
    /// boundary adjacency data.
    ///
    /// # Example
    ///
    /// rust
    /// let boundary_vertices = vec![1, 2, 3];
    /// let comm_vertices: BTreeSet<u64> = vec![4, 5, 6].into_iter().collect();
    /// let whole_graph = CSRGraph::new(...); // Assume CSRGraph is properly initialized
    /// let mut boundary_adj_map = HashMap::new();
    ///
    /// enrich_boundary_graph_normal(boundary_vertices, &comm_vertices, &whole_graph, &mut boundary_adj_map);
    ///
    ///
    /// # See Also
    ///
    /// * `enrich_boundary_graph_giant` - A helper function used for enriching the boundary graph.
    /// * `CSRGraph::induce_subgraph` - Used to induce the subgraph from the full graph.
    /// * `SCCMeta::build_from_subgraph` - Used to generate SCC metadata from the induced subgraph.
    pub fn enrich_boundary_graph_normal(
        boundary_list: Vec<u64>,
        comm_vertex_list: &BTreeSet<u64>,
        whole_graph: &CSRGraph<u64, u64, u64>,
        boundary_adj_map: &mut FxHashMap<u64, FxHashSet<u64>>,
    ) {
        // Induce a subgraph from the given community vertices
        let g_comm =
            whole_graph.induce_subgraph(&comm_vertex_list.iter().cloned().collect::<Vec<_>>());

        // Build SCC metadata from the induced community subgraph
        let scc_meta = SCCMeta::build_from_subgraph(&g_comm);

        // Enrich the boundary graph using the generated SCC metadata
        Self::enrich_boundary_graph_giant(boundary_list, &scc_meta, boundary_adj_map);
    }

    /// Enriches the boundary graph by updating the adjacency information for boundary vertices.
    ///
    /// This function processes each pair of boundary vertices (`src`, `dst`) and checks whether there
    /// is a reachability relationship between them based on the strongly connected component (SCC) metadata.
    /// If the vertices are reachable, and they are not already adjacent, the adjacency map is updated to
    /// reflect this relationship.
    ///
    /// # Arguments
    ///
    /// * `boundary_list` - A vector containing the list of boundary vertices. The function will examine
    ///   each pair of vertices from this list to determine if they should be marked as adjacent based on reachability.
    /// * `scc_meta` - A reference to the `SCCMeta` instance that holds the SCC metadata. This metadata is
    ///   used to check if there is a reachability relationship between vertices.
    /// * `boundary_adj_map` - A mutable reference to a `HashMap` that maps a boundary vertex to a `HashSet`
    ///   of adjacent vertices. This map is updated in place to reflect the reachability information between
    ///   boundary vertices.
    ///
    /// # Returns
    ///
    /// This function does not return a value. It modifies the `boundary_adj_map` in place, updating it with
    /// adjacency information based on the reachability of boundary vertices.
    ///
    /// # Example
    ///
    /// rust
    /// let boundary_vertices = vec![1, 2, 3];
    /// let scc_meta = SCCMeta::new(...); // Assume SCCMeta is properly initialized
    /// let mut boundary_adj_map = HashMap::new();
    ///
    /// enrich_boundary_graph_giant(boundary_vertices, &scc_meta, &mut boundary_adj_map);
    ///
    ///
    /// # See Also
    ///
    /// * `SCCMeta::is_reachable` - Used to check the reachability between two vertices based on the SCC metadata.
    pub fn enrich_boundary_graph_giant(
        boundary_list: Vec<u64>,
        scc_meta: &SCCMeta,
        boundary_adj_map: &mut FxHashMap<u64, FxHashSet<u64>>,
    ) {
        // Iterate through all pairs of boundary vertices (src, dst)
        for src in &boundary_list {
            for dst in &boundary_list {
                // Skip the pair if src == dst
                if src != dst {
                    // If src and dst are not already adjacent, check if they are reachable
                    if !boundary_adj_map.get_mut(src).unwrap().contains(dst) {
                        // Check reachability using the SCC metadata
                        if scc_meta.is_reachable(src, dst) {
                            // If reachable, add dst to the adjacency list of src
                            boundary_adj_map.get_mut(src).unwrap().insert(*dst);
                        }
                    }
                }
            }
        }
    }

    /// Enriches boundary graph for normal-sized communities using parallel processing
    ///
    /// This function enhances the boundary adjacency information for communities that don't
    /// exceed the giant community threshold. It uses SCC decomposition and parallel processing
    /// to efficiently analyze boundary connectivity patterns within the community.
    ///
    /// # Arguments
    ///
    /// * `boundary_list` - Array of vertex IDs that lie on the community boundary
    /// * `comm_vertex_list` - Complete set of vertices belonging to this community
    /// * `whole_graph` - Reference to the full graph for subgraph induction
    /// * `boundary_adj_map` - Thread-safe map storing boundary adjacency relationships
    ///
    /// # Algorithm
    ///
    /// 1. Induces a subgraph containing only the community's vertices
    /// 2. Builds SCC (Strongly Connected Component) metadata from the subgraph
    /// 3. Delegates to parallel giant community boundary enrichment with SCC data
    ///
    /// # Performance Notes
    ///
    /// Despite being for "normal" communities, this function uses the same SCC-based
    /// approach as giant communities to maintain consistency and leverage parallel
    /// processing capabilities for improved performance.
    fn enrich_boundary_graph_normal_parallel(
        boundary_list: &[u64],
        comm_vertex_list: &BTreeSet<u64>,
        whole_graph: &CSRGraph<u64, u64, u64>,
        boundary_adj_map: &DashMap<u64, FxHashSet<u64>>,
    ) {
        // Create focused subgraph containing only community vertices
        let g_comm =
            whole_graph.induce_subgraph(&comm_vertex_list.iter().cloned().collect::<Vec<_>>());

        // Compute SCC decomposition for boundary analysis
        let scc_meta = SCCMeta::build_from_subgraph(&g_comm);

        // Apply parallel boundary enrichment using SCC-based approach
        Self::enrich_boundary_graph_giant_parallel(boundary_list, &scc_meta, boundary_adj_map);
    }

    /// Enriches boundary graph for giant communities using parallel SCC-based reachability
    ///
    /// This function computes transitive closure of boundary vertex connectivity within
    /// a community using SCC (Strongly Connected Component) metadata for efficient
    /// reachability queries. It builds a complete adjacency map for boundary vertices
    /// based on internal community connectivity.
    ///
    /// # Arguments
    ///
    /// * `boundary_list` - Array of vertex IDs that lie on the community boundary
    /// * `scc_meta` - SCC metadata providing efficient reachability queries
    /// * `boundary_adj_map` - Thread-safe concurrent map for storing adjacency relationships
    ///
    /// # Algorithm
    ///
    /// 1. Pre-initializes empty adjacency sets for all boundary vertices
    /// 2. Tests all boundary vertex pairs for reachability using SCC metadata
    /// 3. Adds reachable pairs to the boundary adjacency map
    /// 4. Skips pairs that are already directly adjacent or identical
    ///
    /// # Performance Optimizations
    ///
    /// - Uses DashMap for lock-free concurrent access during parallel processing
    /// - Leverages SCC metadata for O(1) reachability queries vs. expensive path searches
    /// - Pre-allocates adjacency sets to reduce allocation overhead
    /// - Skips redundant checks for existing adjacency
    ///
    /// # Thread Safety
    ///
    /// Function is thread-safe through DashMap's lock-free concurrent operations,
    /// enabling safe parallel execution across multiple boundary vertex subsets.
    fn enrich_boundary_graph_giant_parallel(
        boundary_list: &[u64],
        scc_meta: &SCCMeta,
        boundary_adj_map: &DashMap<u64, FxHashSet<u64>>,
    ) {
        // 1. Build a map from boundary vertex -> SCC ID
        let boundary_to_scc: FxHashMap<u64, u64> = boundary_list
            .iter()
            .map(|&vertex| (vertex, scc_meta.find_vertex_scc(&vertex).unwrap()))
            .collect();

        // 2. Build SCC ID -> boundary vertices reverse map.
        let mut scc_to_boundaries: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
        for (&vertex, &scc_id) in &boundary_to_scc {
            scc_to_boundaries
                .entry(scc_id)
                .or_insert_with(Vec::new)
                .push(vertex);
        }

        // 3. Grab the unique scc ids.
        let unique_boundary_sccs: FxHashSet<u64> = boundary_to_scc.values().cloned().collect();

        // 4. Set the reachable cache of SCC level.
        let mut scc_reachable_cache = FxHashMap::<u64, Arc<FxHashSet<u64>>>::default();

        // 5. Compute the reachable of SCCs.
        unique_boundary_sccs.iter().for_each(|&src_scc| {
            let reachable_sccs = scc_meta.compute_reachable_sccs_with_cache(
                src_scc,
                &unique_boundary_sccs,
                &mut scc_reachable_cache,
            );

            // 6. Compute boundary reachable.
            if let Some(src_vertices) = scc_to_boundaries.get(&src_scc) {
                for &src_vertex in src_vertices {
                    let mut reachable_vertices = FxHashSet::default();

                    // Update the boundary vertices in each SCC.
                    for &reachable_scc in &reachable_sccs {
                        if let Some(dst_vertices) = scc_to_boundaries.get(&reachable_scc) {
                            for &dst_vertex in dst_vertices {
                                if src_vertex != dst_vertex {
                                    reachable_vertices.insert(dst_vertex);
                                }
                            }
                        }
                    }

                    if !reachable_vertices.is_empty() {
                        boundary_adj_map.insert(src_vertex, reachable_vertices);
                    }
                }
            }
        });

        drop(scc_reachable_cache);
    }

    /// Reads a segment of the memory-mapped file and converts it into a CSRSimpleCommBlock.
    ///
    /// This function extracts a portion of the memory-mapped file `normal_mem_map` starting at
    /// the given `offset` and extending `length` bytes. The byte slice is then passed to the
    /// `CSRSimpleCommBlock::from_bytes_topology` method to convert the raw bytes into a structured CSR
    /// block representing the community's graph data.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting position in the memory-mapped file where the data should be read from.
    /// * `length` - The number of bytes to read starting from the given `offset`.
    ///
    /// # Returns
    ///
    /// * `CSRSimpleCommBlock` - The deserialized CSRSimpleCommBlock, which represents the graph's
    ///   compressed sparse row data for the community.
    ///
    /// # Panics
    /// This function uses `unwrap()` on the result of `CSRSimpleCommBlock::from_bytes_topology`, which
    /// will panic if the deserialization fails. In a production environment, this should be replaced
    /// with proper error handling.
    pub fn read_csr_from_normal(&self, offset: usize, length: usize) -> CSRSimpleCommBlock {
        // Extract the byte slice from the memory-mapped file.
        let csr_bytes = self.normal_mem_map[offset..offset + length].to_vec();

        // Convert the byte slice into a CSRSimpleCommBlock using the from_bytes_topology method.
        // This method is expected to deserialize the byte data into the CSRSimpleCommBlock structure.
        CSRSimpleCommBlock::from_bytes_topology(&csr_bytes).unwrap() // This will panic if the deserialization fails.
    }

    /// Reads a segment of the memory-mapped file for a giant community and returns a deserialized CSRSimpleCommBlock.
    ///
    /// This function first checks if the memory map for the given community ID is already loaded. If not, it
    /// loads the memory map from a file corresponding to the community, inserts it into the map manager for future
    /// access, and then reads the requested segment from the memory map. The byte slice is deserialized into a
    /// `CSRSimpleCommBlock` to represent the community's graph structure.
    ///
    /// # Arguments
    ///
    /// * `community_id` - The unique identifier for the giant community.
    /// * `offset` - The starting position in the memory-mapped file where the data should be read from.
    /// * `length` - The number of bytes to read starting from the given `offset`.
    ///
    /// # Returns
    ///
    /// * `CSRSimpleCommBlock` - The deserialized CSRSimpleCommBlock, which represents the graph's compressed
    ///   sparse row data for the community.
    ///
    /// # Panics
    /// This function uses `unwrap()` in several places, which can panic if the memory mapping fails or if
    /// deserialization fails. In a production environment, you should handle errors more gracefully.
    pub fn read_csr_from_giant(
        &self,
        community_id: &u32,
        offset: usize,
        length: usize,
    ) -> CSRSimpleCommBlock {
        // Check if the memory map for this giant community is already loaded.
        if !self.giant_mem_map_manager.contains_key(community_id) {
            // If the memory map is not present, load it from a file.
            let graph_name = self.graph_name.clone();
            let dir_path = Path::new(&self.workspace);

            // Construct the file name and path for the giant community.
            let giant_comm_name = format!("{graph_name}_giant_comm{}_storage.bin", community_id);
            let giant_comm_path = dir_path.join(&giant_comm_name);

            // Open the file for reading and writing. If the file does not exist, it will be created.
            let giant_comm_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&giant_comm_path)
                .unwrap();

            // Memory-map the file into memory.
            let giant_mem_map = unsafe { Mmap::map(&giant_comm_file).unwrap() };

            // Insert the memory map for this community into the giant community map manager.
            self.giant_mem_map_manager
                .insert(*community_id, giant_mem_map);
        }

        // Retrieve the memory map for the community.
        let giant_comm_mem_map_ref = self.giant_mem_map_manager.get(community_id).unwrap();

        // Slice the memory map based on the offset and length, then convert the slice to a Vec<u8>.
        let csr_bytes = giant_comm_mem_map_ref[offset..offset + length].to_vec();

        // Deserialize the byte slice into a CSRSimpleCommBlock representing the community's graph structure.
        CSRSimpleCommBlock::from_bytes_topology(&csr_bytes).unwrap()
    }

    pub fn read_vertex_list_from_giant(&self, community_id: &u32, offset: usize) -> Vec<u64> {
        // Check if the memory map for this giant community is already loaded.
        if !self.giant_mem_map_manager.contains_key(community_id) {
            // If the memory map is not present, load it from a file.
            let graph_name = self.graph_name.clone();
            let dir_path = Path::new(&self.workspace);

            // Construct the file name and path for the giant community.
            let giant_comm_name = format!("{graph_name}_giant_comm{}_storage.bin", community_id);
            let giant_comm_path = dir_path.join(&giant_comm_name);

            // Open the file for reading and writing. If the file does not exist, it will be created.
            let giant_comm_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&giant_comm_path)
                .unwrap();

            // Memory-map the file into memory.
            let giant_mem_map = unsafe { Mmap::map(&giant_comm_file).unwrap() };

            // Insert the memory map for this community into the giant community map manager.
            self.giant_mem_map_manager
                .insert(*community_id, giant_mem_map);
        }

        // Retrieve the memory map for the community.
        let giant_comm_mem_map_ref = self.giant_mem_map_manager.get(community_id).unwrap();

        // Slice the memory map based on the offset and length, then convert the slice to a Vec<u8>.
        let vertex_count_bytes = giant_comm_mem_map_ref[offset..offset + u64::byte_size()].to_vec();
        let vertex_count = u64::from_le_bytes(vertex_count_bytes.try_into().unwrap());
        let vertex_array_length = (u64::byte_size() + u64::byte_size()) * vertex_count as usize;
        let vertex_array_bytes = giant_comm_mem_map_ref
            [offset + u64::byte_size()..offset + u64::byte_size() + vertex_array_length]
            .to_vec();
        // Parse the vertex list.
        let mut vertex_array = Vec::new();
        let middle_pointer = u64::byte_size();
        let offset_length = u64::byte_size();
        for v in 0..vertex_count as usize {
            let vertex_parsed: u64 = u64::from_bytes(
                &vertex_array_bytes[v * (middle_pointer + offset_length)
                    ..v * (middle_pointer + offset_length) + middle_pointer],
            )
            .unwrap();
            vertex_array.push(vertex_parsed);
        }
        vertex_array
    }

    /// Retrieves the neighbors of a given vertex in the community's graph.
    ///
    /// This function reads the neighbors of a vertex, first checking if the vertex exists within
    /// the community index. It handles both normal communities (by loading CSR blocks from cache)
    /// and giant communities (by reading from a memory-mapped file and caching the data). The function
    /// supports caching to avoid unnecessary reloading and deserialization of community data.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The unique identifier of the vertex whose neighbors need to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Vec<u64>` - A list of vertices that are neighbors to the given vertex.
    ///
    /// # Panics
    /// This function uses `unwrap()` in several places, which will panic if the community or SCC data
    /// is not found. This should be handled more gracefully in production code with proper error handling.
    pub fn read_neighbor(&self, vertex_id: &u64) -> Vec<u64> {
        // If the vertex_id is out of bounds (i.e., greater than the total vertex count), return an empty vector.
        if *vertex_id >= self.vertex_count {
            // Vertex index out of range.
            return vec![];
        }

        // Retrieve the community that the vertex belongs to from the community index.
        let community_item_opt = self.community_index.get_vertex_community(vertex_id);

        let result = match community_item_opt {
            // If the community is not found, return an empty vector (no neighbors).
            None => {
                // Vertex or Community not exist.
                vec![]
            }
            // If the community is found, we proceed with reading the neighbors from the community.
            Some((community_id, community_item)) => {
                match community_item {
                    // Normal community: Load the CSR block from cache or read it if not cached.
                    Normal { offset, length } => {
                        // Try to load the CSR block from the community cache.
                        let target_comm_csr = match self.community_cache.get(&community_id) {
                            None => {
                                // Cache miss: Read the CSR block from the normal community file.
                                let csr_comm_block = self.read_csr_from_normal(offset, length);
                                let csr_comm_block_arc = Arc::new(csr_comm_block);
                                self.community_cache
                                    .insert(community_id, csr_comm_block_arc.clone());
                                csr_comm_block_arc
                            }
                            // Cache hit: Directly return the cached CSR block.
                            Some(csr_comm_block_arc) => csr_comm_block_arc.clone(),
                        };
                        // Perform the actual neighbor reading for the vertex.
                        target_comm_csr.read_neighbor(vertex_id)
                    }
                    // Giant community: Handle it differently by loading SCCs and reading from giant memory map.
                    Giant {
                        scc_index_offset,
                        length,
                    } => {
                        // Attempt to load the SCC index for the giant community from the giant community map.
                        let scc_index_entry = match self.giant_community_map.get(&community_id) {
                            None => {
                                // Cache miss: Load the giant community from disk and create a memory map.
                                if !self.giant_mem_map_manager.contains_key(&community_id) {
                                    let graph_name = self.graph_name.clone();
                                    let dir_path = Path::new(&self.workspace);
                                    let giant_comm_name = format!(
                                        "{graph_name}_giant_comm{}_storage.bin",
                                        community_id
                                    );
                                    let giant_comm_path = dir_path.join(&giant_comm_name);

                                    // Open the giant community file.
                                    let giant_comm_file = fs::OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(&giant_comm_path)
                                        .unwrap();

                                    // Memory-map the giant community file.
                                    let giant_mem_map =
                                        unsafe { Mmap::map(&giant_comm_file).unwrap() };

                                    // Insert the memory-mapped file into the manager.
                                    self.giant_mem_map_manager
                                        .insert(community_id, giant_mem_map);
                                }

                                // Retrieve the memory-mapped data.
                                let giant_mem_map_ref =
                                    self.giant_mem_map_manager.get(&community_id).unwrap();
                                // Load and deserialize the SCC index from the memory map.
                                let scc_index_item_bytes =
                                    giant_mem_map_ref.value()[0..scc_index_offset].to_vec();
                                let scc_index_item: GiantCommunityIndex =
                                    bincode::deserialize(&scc_index_item_bytes).unwrap();
                                self.giant_community_map
                                    .insert(community_id, Arc::new(scc_index_item));

                                let scc_index_entry =
                                    self.giant_community_map.get(&community_id).unwrap();
                                scc_index_entry
                            }
                            // Cache hit: Retrieve the cached SCC index entry.
                            Some(scc_index_entry) => scc_index_entry,
                        };

                        // Find the SCC ID for the given vertex.
                        let scc_id = *scc_index_entry.scc_meta.vertex_scc.get(vertex_id).unwrap();

                        // Get the start index for this SCC and compute its length.
                        let scc_start = scc_index_entry.scc_index.0[scc_id as usize] as usize;
                        let scc_length = if scc_id + 1
                            == scc_index_entry.scc_meta.scc_list.len() as u64
                        {
                            length - scc_start
                        } else {
                            scc_index_entry.scc_index.0[(scc_id + 1) as usize] as usize - scc_start
                        };

                        // Try to load the SCC CSR block from cache.
                        let target_scc_csr = match self.scc_cache.get(&(community_id, scc_id)) {
                            None => {
                                // Cache miss: Read the SCC CSR block from the giant community file.
                                let csr_comm_block = self.read_csr_from_giant(
                                    &community_id,
                                    scc_index_offset + scc_start,
                                    scc_length,
                                );
                                let csr_comm_block_arc = Arc::new(csr_comm_block);
                                self.scc_cache
                                    .insert((community_id, scc_id), csr_comm_block_arc.clone());
                                csr_comm_block_arc
                            }
                            // Cache hit: Return the cached SCC CSR block.
                            Some(csr_scc_block_arc) => csr_scc_block_arc.clone(),
                        };

                        // Perform the actual neighbor reading for the vertex in the giant community.
                        target_scc_csr.read_neighbor(vertex_id)
                    }
                }
            }
        };
        result
    }

    /// Loads a reference to a community item based on the provided `community_id`.
    ///
    /// This function first attempts to find the location of the community (normal or giant) in the community index.
    /// If the community is found, it will either load the community's data from a cache or fetch it from disk.
    ///
    /// For normal communities, it will load the CSR block from the cache, or from the file if not cached.
    /// For giant communities, it will handle loading and memory-mapping of the associated SCCs (Strongly Connected Components).
    ///
    /// # Parameters
    /// - `community_id`: The ID of the community to load.
    ///
    /// # Returns
    /// - `Option<CommunityItemRef>`: A reference to the community item if found, otherwise `None`.
    pub fn load_community_ref(&self, community_id: &u32) -> Option<CommunityItemRef> {
        // Step 1: Get the community location (normal or giant) from the community index.
        let community_index_item_opt = self.community_index.get_community_location(community_id);

        let result = match community_index_item_opt {
            None => {
                // If the community is not found in the index, return None.
                // This means the target community does not exist.
                None
            }
            Some(community_index_item) => {
                match community_index_item {
                    // Case 1: Normal community
                    // For a normal community, we attempt to load the CSR block either from the cache or from the file.
                    Normal { offset, length } => {
                        // Step 2: Try to load the CSR block from the community cache.
                        let target_comm_csr = match self.community_cache.get(community_id) {
                            None => {
                                // Cache miss: If the CSR block is not in the cache, read it from the normal community file.
                                let csr_comm_block = self.read_csr_from_normal(offset, length);
                                let csr_comm_block_arc = Arc::new(csr_comm_block);

                                // Step 3: Cache the CSR block for future use.
                                self.community_cache
                                    .insert(*community_id, csr_comm_block_arc.clone());

                                // Return the CSR block wrapped in an Arc.
                                csr_comm_block_arc
                            }
                            // Cache hit: Return the cached CSR block directly.
                            Some(csr_comm_block_arc) => csr_comm_block_arc.clone(),
                        };
                        // Step 4: Wrap the result in a `CommunityItemRef::Normal` and return it.
                        Some(CommunityItemRef::Normal(target_comm_csr))
                    }
                    // Case 2: Giant community
                    // For a giant community, we handle it by loading SCCs and reading from a memory-mapped file.
                    Giant {
                        scc_index_offset, ..
                    } => {
                        // Step 5: Attempt to load the SCC index for the giant community from the giant community map.
                        let scc_index_entry = match self.giant_community_map.get(&community_id) {
                            None => {
                                // Cache miss: If the giant community is not in the cache, load it from disk.
                                // If the memory-mapped file is not yet available, create it.
                                if !self.giant_mem_map_manager.contains_key(&community_id) {
                                    let graph_name = self.graph_name.clone();
                                    let dir_path = Path::new(&self.workspace);
                                    let giant_comm_name = format!(
                                        "{graph_name}_giant_comm{}_storage.bin",
                                        community_id
                                    );
                                    let giant_comm_path = dir_path.join(&giant_comm_name);

                                    // Step 6: Open the giant community file.
                                    let giant_comm_file = fs::OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(&giant_comm_path)
                                        .unwrap();

                                    // Step 7: Memory-map the giant community file.
                                    let giant_mem_map =
                                        unsafe { Mmap::map(&giant_comm_file).unwrap() };

                                    // Step 8: Insert the memory-mapped file into the memory map manager.
                                    self.giant_mem_map_manager
                                        .insert(*community_id, giant_mem_map);
                                }

                                // Step 9: Retrieve the memory-mapped data for the giant community.
                                let giant_mem_map_ref =
                                    self.giant_mem_map_manager.get(&community_id).unwrap();
                                // Step 10: Deserialize the SCC index from the memory-mapped file.
                                let scc_index_item_bytes =
                                    giant_mem_map_ref.value()[0..scc_index_offset].to_vec();
                                let scc_index_item: GiantCommunityIndex =
                                    bincode::deserialize(&scc_index_item_bytes).unwrap();

                                // Step 11: Insert the deserialized SCC index into the giant community map.
                                self.giant_community_map
                                    .insert(*community_id, Arc::new(scc_index_item));

                                // Step 12: Retrieve the SCC index entry from the map and return it.
                                let scc_index_entry =
                                    self.giant_community_map.get(&community_id).unwrap();
                                scc_index_entry
                            }
                            // Cache hit: Retrieve the cached SCC index entry.
                            Some(scc_index_entry) => scc_index_entry,
                        };
                        // Step 13: Return the giant community wrapped in `CommunityItemRef::Giant`.
                        Some(CommunityItemRef::Giant(Arc::clone(scc_index_entry.value())))
                    }
                }
            }
        };
        result
    }

    /// Loads a community instance based on the provided `community_id`.
    ///
    /// This function first tries to load the community reference (either normal or giant) using the provided `community_id`.
    /// Depending on whether the community is normal or giant, it will either clone the CSR block or load and cache the sub-community blocks (SCCs).
    /// For normal communities, the CSR block is cloned. For giant communities, it retrieves the SCC index and loads the SCC blocks, caching them as needed.
    ///
    /// # Parameters
    /// - `community_id`: The ID of the community to load.
    ///
    /// # Returns
    /// - `Option<CommunityItemInstance>`: Returns an instance of `CommunityItemInstance` if found, otherwise `None`.
    pub fn load_community_instance(&self, community_id: &u32) -> Option<CommunityItemInstance> {
        // Step 1: Attempt to load the community reference (Normal or Giant).
        match self.load_community_ref(community_id) {
            None => {
                // If the community reference is not found, return None.
                None
            }
            Some(community_item) => {
                match community_item {
                    // Case 1: Normal community
                    // For normal communities, clone the CSR block and return it wrapped in CommunityItemInstance::Normal.
                    CommunityItemRef::Normal(normal_community_csr) => Some(
                        CommunityItemInstance::Normal((*normal_community_csr).clone()),
                    ),
                    // Case 2: Giant community
                    // For giant communities, load the SCC index and retrieve each SCC CSR block.
                    CommunityItemRef::Giant(scc_index_entry) => {
                        // Step 2: Retrieve the SCC index offset and length from the community index.
                        let (scc_index_offset, length) = match self
                            .community_index
                            .community_map
                            .get(&community_id)
                            .unwrap()
                        {
                            // An error occurs if the community is not giant.
                            Normal { .. } => {
                                return None; // This should not happen for a giant community.
                            }
                            Giant {
                                scc_index_offset,
                                length,
                            } => (*scc_index_offset, *length),
                        };

                        // Step 3: Iterate through each SCC in the giant community and load the corresponding CSR block.
                        let mut scc_csr_list = vec![];
                        for (scc_id, scc_start) in scc_index_entry.scc_index.0.iter().enumerate() {
                            // Step 4: Calculate the length of the current SCC.
                            let scc_length =
                                if scc_id + 1 == scc_index_entry.scc_meta.scc_list.len() {
                                    length - *scc_start as usize // Last SCC in the community.
                                } else {
                                    scc_index_entry.scc_index.0[scc_id + 1] as usize
                                        - *scc_start as usize // Calculate next SCC length.
                                };

                            // Step 5: Try to load the SCC CSR block from cache.
                            let target_scc_csr =
                                match self.scc_cache.get(&(*community_id, scc_id as u64)) {
                                    None => {
                                        // Cache miss: Read the SCC CSR block from the giant community file.
                                        let csr_comm_block = self.read_csr_from_giant(
                                            &community_id,
                                            scc_index_offset + *scc_start as usize,
                                            scc_length,
                                        );
                                        let csr_comm_block_arc = Arc::new(csr_comm_block);

                                        // Step 6: Cache the SCC CSR block for future use.
                                        self.scc_cache.insert(
                                            (*community_id, scc_id as u64),
                                            csr_comm_block_arc.clone(),
                                        );

                                        // Return the CSR block wrapped in an Arc.
                                        csr_comm_block_arc
                                    }
                                    // Cache hit: Return the cached SCC CSR block.
                                    Some(csr_scc_block_arc) => csr_scc_block_arc.clone(),
                                };

                            // Add the cloned SCC CSR block to the list.
                            scc_csr_list.push((*target_scc_csr).clone());
                        }

                        // Step 7: Return the giant community instance wrapped in CommunityItemInstance::Giant.
                        Some(CommunityItemInstance::Giant(scc_index_entry, scc_csr_list))
                    }
                }
            }
        }
    }

    pub fn load_scc_vertex_list(&self, community_id: &u32, scc_id: &u64) -> Option<Vec<u64>> {
        // Retrieve the community that the vertex belongs to from the community index.
        let community_item_opt = self.community_index.get_community_location(community_id);

        let result = match community_item_opt {
            // If the community is not found, return an empty vector (no neighbors).
            None => {
                // Vertex or Community not exist.
                return None;
            }
            // If the community is found, we proceed with reading the neighbors from the community.
            Some(community_item) => {
                match community_item {
                    // Normal community: Load the CSR block from cache or read it if not cached.
                    Normal { .. } => None,
                    // Giant community: Handle it differently by loading SCCs and reading from giant memory map.
                    Giant {
                        scc_index_offset,
                        length: _length,
                    } => {
                        // Attempt to load the SCC index for the giant community from the giant community map.
                        let scc_index_entry = match self.giant_community_map.get(&community_id) {
                            None => {
                                // Cache miss: Load the giant community from disk and create a memory map.
                                if !self.giant_mem_map_manager.contains_key(&community_id) {
                                    let graph_name = self.graph_name.clone();
                                    let dir_path = Path::new(&self.workspace);
                                    let giant_comm_name = format!(
                                        "{graph_name}_giant_comm{}_storage.bin",
                                        community_id
                                    );
                                    let giant_comm_path = dir_path.join(&giant_comm_name);

                                    // Open the giant community file.
                                    let giant_comm_file = fs::OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(&giant_comm_path)
                                        .unwrap();

                                    // Memory-map the giant community file.
                                    let giant_mem_map =
                                        unsafe { Mmap::map(&giant_comm_file).unwrap() };

                                    // Insert the memory-mapped file into the manager.
                                    self.giant_mem_map_manager
                                        .insert(*community_id, giant_mem_map);
                                }

                                // Retrieve the memory-mapped data.
                                let giant_mem_map_ref =
                                    self.giant_mem_map_manager.get(&community_id).unwrap();
                                // Load and deserialize the SCC index from the memory map.
                                let scc_index_item_bytes =
                                    giant_mem_map_ref.value()[0..scc_index_offset].to_vec();
                                let scc_index_item: GiantCommunityIndex =
                                    bincode::deserialize(&scc_index_item_bytes).unwrap();
                                self.giant_community_map
                                    .insert(*community_id, Arc::new(scc_index_item));

                                let scc_index_entry =
                                    self.giant_community_map.get(&community_id).unwrap();
                                scc_index_entry
                            }
                            // Cache hit: Retrieve the cached SCC index entry.
                            Some(scc_index_entry) => scc_index_entry,
                        };

                        // Get the start index for this SCC and compute its length.
                        let scc_start = scc_index_entry.scc_index.0[*scc_id as usize] as usize;

                        // Try to load the SCC CSR block from cache.
                        match self.scc_cache.get(&(*community_id, *scc_id)) {
                            None => {
                                // Cache Miss, Try load from storage engine.
                                Some(
                                    self.read_vertex_list_from_giant(
                                        community_id,
                                        scc_start + scc_index_offset,
                                    )
                                    .into_iter()
                                    .map(|vertex| vertex)
                                    .collect::<Vec<_>>(),
                                )
                            }
                            // Cache hit: Return the cached SCC CSR block.
                            Some(target_scc_csr_block) => Some(
                                target_scc_csr_block
                                    .vertex_list
                                    .iter()
                                    .map(|(vertex, _)| *vertex)
                                    .collect::<Vec<_>>(),
                            ),
                        }
                    }
                }
            }
        };
        result
    }
}
