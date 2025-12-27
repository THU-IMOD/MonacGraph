use std::sync::Arc;

use crate::cache::CacheKey;
use crate::config::LsmCommunityStorageOptions;
use crate::delta::{DeltaLog, DeltaOpType, DeltaOperation};
use crate::external::ExternalStorage;
use crate::iterator::{GlobalNeighborIterator, UnifiedNeighborIterator};
use crate::types::{EdgeList, PageId, VIdList};
use crate::utils::generate_timestamp_micros;
use crate::{
    bucket::{Bucket, builder::BucketBuilder, disk_manager::BktDiskManager},
    cache::BlockCache,
    graph::CsrGraph,
    mem_graph::MemGraph,
    types::{VId, VirtualCommId},
    vertex_index::VertexIndex,
};
use anyhow::{Ok, Result};
use moka::sync::Cache;
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::{Path, PathBuf};

/// The storage state of LSMCommunity.
pub struct LsmCommunityStorageState {
    // The in-memory structure to record deltas
    pub mem_graph: Arc<MemGraph>,

    // The in-memory immutable structure to record deltas.
    pub imm_mem_graphs: Vec<Arc<MemGraph>>,

    // The bucket structures.
    pub buckets: FxHashMap<VirtualCommId, Arc<Bucket>>,
}

impl LsmCommunityStorageState {
    /// Create a new lsm storage state, i.e., a snapshot.
    pub fn create_with_graph_file(
        mut graph: CsrGraph,
        lsm_community_storage_option: LsmCommunityStorageOptions,
    ) -> (Self, FxHashMap<VId, VIdList>, VertexIndex) {
        // Step 1 - Build vertex index to compute the virtual community id.
        println!("Building Vertex Index");
        let (mut vertex_index, _) = VertexIndex::build_from_graph(
            &mut graph,
            lsm_community_storage_option.giant_vertex_boundary,
            lsm_community_storage_option.min_bucket_size,
        );
        println!("Building Vertex Index - [OK]");
        let mut giant_vertex_map = FxHashMap::<VId, VIdList>::default();
        // Step 2 - Get the vertex group.
        let mut vertex_groups = FxHashMap::<VirtualCommId, Vec<VId>>::default();

        for (vertex_id, vertex_index_item) in vertex_index.vertex_array.iter().enumerate() {
            // Check whether giant;
            if vertex_index_item.is_normal() {
                let virtual_comm_id = vertex_index_item.virtual_comm_id();
                // println!("VId: {}", virtual_comm_id);
                vertex_groups
                    .entry(virtual_comm_id)
                    .or_default()
                    .push(vertex_id as VId);
            } else {
                // Giant vertex.
                giant_vertex_map.insert(
                    vertex_id as VId,
                    graph.get_neighbor_iter(vertex_id as VId).collect(),
                );
            }
        }

        // Step 3 - For each group, build bucket
        // create a dir named ./{lsm_community_storage_option.work_space_dir}/{lsm_community_storage_option.graph_name}
        let bucket_dir = PathBuf::from(&lsm_community_storage_option.work_space_dir)
            .join(&lsm_community_storage_option.graph_name);

        // Create directory if not exists
        std::fs::create_dir_all(&bucket_dir).unwrap();
        println!("Building Buckets");
        let mut buckets = FxHashMap::<VirtualCommId, Arc<Bucket>>::default();

        #[cfg(not(test))]
        let progress_bar = {
            use indicatif::{ProgressBar, ProgressStyle};
            let total_vertices: usize = vertex_groups.values().map(|v| v.len()).sum();
            let pb = ProgressBar::new(total_vertices as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}",
                    )
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_message("Loading vertices");
            pb
        };

        for (virtual_comm_id, vertex_list) in vertex_groups.iter() {
            let mut bucket_builder = BucketBuilder::new(lsm_community_storage_option.block_size);
            // Insert vertex and neighbors in this group to this builder
            for vertex_id in vertex_list {
                let neighbor_iter = graph.get_neighbor_iter(*vertex_id);
                bucket_builder.add(*vertex_id, neighbor_iter);
                #[cfg(not(test))]
                progress_bar.inc(1);
            }
            // The file full path is `./{work_space_dir}/{graph_name}/bucket_{virtual_comm_id}.bkt`
            let bucket_path = bucket_dir.join(format!("bucket_{}.bkt", virtual_comm_id));
            let bucket_build = bucket_builder.build(*virtual_comm_id, bucket_path).unwrap();

            for vertex_meta in &bucket_build.vertex_metas {
                // Update the vertex index.
                vertex_index.vertex_array[vertex_meta.vertex_id as usize]
                    .set_page_id(vertex_meta.page_id);
                vertex_index.vertex_array[vertex_meta.vertex_id as usize]
                    .set_offset(vertex_meta.offset_inner);
            }

            // Fill the built bucket into hashmap;
            buckets.insert(*virtual_comm_id, Arc::new(bucket_build));
        }
        #[cfg(not(test))]
        progress_bar.finish_with_message("Vertices loaded");
        println!("Building Buckets - [OK]");
        // Save vertex index to file;
        let vertex_index_path = bucket_dir.join("vertex_index.bin.zst");
        println!("Saving Vertex Index");
        vertex_index
            .serialize_to_file(vertex_index_path, 3)
            .unwrap();
        println!("Saving Vertex Index - [OK]");
        (
            Self {
                mem_graph: Arc::new(MemGraph::new(0)),
                imm_mem_graphs: vec![],
                buckets,
            },
            giant_vertex_map,
            vertex_index,
        )
    }
}

#[allow(dead_code)]
pub struct LsmCommunityStorageInner {
    pub state: Arc<RwLock<Arc<LsmCommunityStorageState>>>,
    pub state_lock: Mutex<()>,
    block_cache: Arc<BlockCache>,
    pub options: Arc<LsmCommunityStorageOptions>,
}

#[allow(dead_code)]
impl LsmCommunityStorageInner {
    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(
        is_recover: bool,
        vertex_index_opt: Option<PathBuf>,
        bucket_path_opt: Option<Vec<PathBuf>>,
        options: LsmCommunityStorageOptions,
    ) -> Result<(Self, FxHashMap<VId, VIdList>, VertexIndex)> {
        if is_recover {
            println!("Perform Recovering");
            // Recover from files.
            // Step 1 - Recover the buckets.
            let mut buckets = FxHashMap::<VirtualCommId, Arc<Bucket>>::default();
            let bucket_path_list = bucket_path_opt.unwrap();
            // Travel each bucket.
            for bucket_path in bucket_path_list {
                // Step 2: Open the bucket from file
                let file = BktDiskManager::new(&bucket_path)?;
                let virtual_comm_id = Self::extract_community_id(&bucket_path).unwrap();
                let bucket = Bucket::open(virtual_comm_id, file, true)?;
                // Push into buckets.
                buckets.insert(virtual_comm_id, Arc::new(bucket));
            }

            // Step 3 - Recover the vertex index.
            let vertex_index_path = vertex_index_opt.unwrap();
            let vertex_index = VertexIndex::deserialize_from_file(&vertex_index_path)?;

            // Build lsm storage state.
            let state = LsmCommunityStorageState {
                mem_graph: Arc::new(MemGraph::new(0)),
                imm_mem_graphs: Vec::new(),
                buckets,
            };
            println!("Perform Recovering - [OK]");

            Ok((
                Self {
                    state: Arc::new(RwLock::new(Arc::new(state))),
                    state_lock: Mutex::new(()),
                    block_cache: Arc::new(Cache::new(options.block_cache_capacity)),
                    options: Arc::new(options),
                },
                FxHashMap::<VId, VIdList>::default(),
                vertex_index,
            ))
        } else {
            // Build from scratch.
            Self::build_from_csr_graph(options)
        }
    }

    /// Get the neighbor of a vertex.
    pub fn get_neighbor_iter(
        &self,
        vertex_id: VId,
        vertex_index: &VertexIndex,
    ) -> GlobalNeighborIterator {
        // Step 1 - Get mem neighbors
        let state = self.state.read();
        let mem_neighbors = state
            .mem_graph
            .map
            .get(&vertex_id)
            .map(|entry| entry.value().read().unwrap().clone())
            .unwrap_or_else(Vec::new);

        // Step 2 - Get the neighbor in bucket.
        let vertex_item = vertex_index.vertex_array[vertex_id as usize];

        let (block_arc, vertex_offset) = if let Some(cache_key) = vertex_item.to_cache_key() {
            let block = if let Some(cached_block) = self.block_cache.get(&cache_key) {
                // Cache hit
                cached_block
            } else {
                // Cache miss - load from bucket
                let virtual_comm_id = vertex_item.virtual_comm_id();
                let target_bucket = state.buckets.get(&virtual_comm_id).unwrap();
                // Load block
                let block_loaded_res = target_bucket.read_block(vertex_item.page_id()).unwrap();
                // Push it to block cache
                self.block_cache.insert(cache_key, block_loaded_res.clone());
                block_loaded_res
            };
            (Some(block), Some(vertex_item.offset() as usize))
        } else {
            (None, None)
        };

        // Step 3 - Create and return combined iterator
        GlobalNeighborIterator::new(mem_neighbors, block_arc, vertex_offset)
    }

    /// Extract the virtual community id from the bucket path.
    fn extract_community_id(bucket_path: &Path) -> Option<VirtualCommId> {
        let filename = bucket_path.file_name()?.to_string_lossy();

        // filename format: "bucket_0.bkt"
        // Extract the number between "bucket_" and ".bkt"
        if let Some(stripped) = filename.strip_prefix("bucket_") {
            if let Some(id_str) = stripped.split('.').next() {
                return id_str.parse::<VirtualCommId>().ok();
            }
        }

        None
    }

    // Build from beginning.
    fn build_from_csr_graph(
        options: LsmCommunityStorageOptions,
    ) -> Result<(Self, FxHashMap<VId, VIdList>, VertexIndex)> {
        println!("Build from Scratch");
        // Two cases: Build from scratch, or recover from files.
        // Step 1 - Build lsm storage state;
        let block_cache_capacity = options.block_cache_capacity;
        // println!("Before creating graph from file.");
        // Create the graph from the graph file.
        #[cfg(test)]
        let graph = CsrGraph::from_file(format!("../data/{}.graph", options.graph_name))?;

        #[cfg(not(test))]
        let graph = CsrGraph::from_file(format!("./data/{}.graph", options.graph_name))?;
        let (state, giant_vertex_map, vertex_index) =
            LsmCommunityStorageState::create_with_graph_file(graph, options.clone());

        // Step 2 - Build block cache;
        let block_cache = BlockCache::new(block_cache_capacity);

        // Step 4 - Return Inner;
        Ok((
            Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                block_cache: Arc::new(block_cache),
                options: Arc::new(options),
            },
            giant_vertex_map,
            vertex_index,
        ))
    }

    /// Warm up the cache.
    pub fn warm_up(&self, vertex_index: &VertexIndex) -> Vec<VId> {
        let state = self.state.read();
        // Travel each vertex, gather bucket and page id.
        let mut giant_vertex_ids = Vec::<VId>::new();
        let mut location_boundary = FxHashMap::<VirtualCommId, PageId>::default();

        vertex_index
            .vertex_array
            .iter()
            .enumerate()
            .for_each(|(vertex_id, vertex_index_item)| {
                if vertex_index_item.is_normal() {
                    let virtual_comm_id = vertex_index_item.virtual_comm_id();
                    let page_id = vertex_index_item.page_id();

                    // Insert or update with the maximum page_id for each virtual_comm_id
                    location_boundary
                        .entry(virtual_comm_id)
                        .and_modify(|existing_page_id| {
                            *existing_page_id = (*existing_page_id).max(page_id);
                        })
                        .or_insert(page_id);
                } else {
                    giant_vertex_ids.push(vertex_id as VId);
                }
            });

        #[cfg(not(test))]
        let progress_bar = {
            use indicatif::{ProgressBar, ProgressStyle};
            let total_pages: PageId = location_boundary.values().sum();
            let pb = ProgressBar::new(total_pages as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}",
                    )
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_message("Loading pages");
            pb
        };

        location_boundary
            .into_par_iter()
            .for_each(|(virtual_comm_id, max_page_id)| {
                let bucket = state.buckets.get(&virtual_comm_id).unwrap();
                for page_id in 0..=max_page_id {
                    if let anyhow::Result::Ok(load_page) = bucket.read_block(page_id) {
                        // Insert into block cache (cache is thread-safe)
                        let cache_key = CacheKey::new(virtual_comm_id, page_id);
                        self.block_cache.insert(cache_key, load_page);
                        #[cfg(not(test))]
                        progress_bar.inc(1);
                    }
                }
            });
        #[cfg(not(test))]
        progress_bar.finish_with_message("Loading Pages - [OK]");

        giant_vertex_ids
    }

    // Check can recover from files.
    fn check_recover(options: LsmCommunityStorageOptions) -> Option<(PathBuf, Vec<PathBuf>)> {
        let graph_name = &options.graph_name;
        let work_space_dir = &options.work_space_dir;

        // Construct the graph directory path: ./{work_space_dir}/{graph_name}/
        let graph_dir = PathBuf::from(work_space_dir).join(graph_name);

        // Check if the graph directory exists
        if !graph_dir.exists() || !graph_dir.is_dir() {
            return None;
        }

        // Find vertex_index.* file
        let vertex_index_path = std::fs::read_dir(&graph_dir)
            .ok()?
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("vertex_index.")
            })
            .map(|entry| entry.path())?;

        // Find all bucket_*.bkt* files
        let bucket_paths: Vec<PathBuf> = std::fs::read_dir(&graph_dir)
            .ok()?
            .filter_map(Result::ok)
            .filter(|entry| {
                let filename = entry.file_name().to_string_lossy().to_string();
                filename.starts_with("bucket_") && filename.contains(".bkt")
            })
            .map(|entry| entry.path())
            .collect();

        // Check if we found at least one bucket file
        if bucket_paths.is_empty() {
            return None;
        }

        // Return the vertex index path and all bucket paths
        Some((vertex_index_path, bucket_paths))
    }
}

#[allow(dead_code)]
pub struct LsmCommunity {
    /// Inner storage.
    pub(crate) inner: Arc<LsmCommunityStorageInner>,
    /// External DB to store giant vertex,
    pub(crate) external_db: Arc<ExternalStorage>,
    /// Vertex Index.
    pub(crate) vertex_index: Arc<RwLock<VertexIndex>>,
    /// Notifies the L0 flush thread to stop working.
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread.
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl LsmCommunity {
    pub fn open(options: LsmCommunityStorageOptions) -> anyhow::Result<Arc<Self>> {
        // Create external storage
        let external_db = Arc::new(ExternalStorage::new(options.clone())?);

        // Check if we can recover from files
        let giant_vertex_map: FxHashMap<VId, VIdList>;
        let inner: LsmCommunityStorageInner;
        let vertex_index: VertexIndex;
        if let Some((vertex_index_path, bucket_paths)) =
            LsmCommunityStorageInner::check_recover(options.clone())
        {
            // Create state
            (inner, giant_vertex_map, vertex_index) = LsmCommunityStorageInner::open(
                true,
                Some(vertex_index_path.clone()),
                Some(bucket_paths.clone()),
                options,
            )?;
        } else {
            // Create state
            (inner, giant_vertex_map, vertex_index) =
                LsmCommunityStorageInner::open(false, None, None, options)?;
        }

        #[cfg(test)]
        println!("Handle Giant Vertex {}", giant_vertex_map.len());

        // Push the giant vertices into external DB;
        for (giant_vertex_id, neighbors) in giant_vertex_map.into_iter() {
            external_db.put_giant_vertex(giant_vertex_id, neighbors.into_iter())?;
        }
        // Create flush notifier
        let (tx, _) = crossbeam_channel::unbounded();

        Ok(Arc::new(Self {
            inner: Arc::new(inner),
            external_db,
            vertex_index: Arc::new(RwLock::new(vertex_index)),
            flush_notifier: tx,
            flush_thread: Mutex::new(None),
        }))
    }

    /// Get the vertex count in this storage engine
    pub fn vertex_count(&self) -> u32 {
        let vertex_index_state = self.vertex_index.read();
        vertex_index_state.vertex_array.len() as u32
    }

    /// Check the state of a vertex.
    /// - `true` if the vertex is giant.
    /// - `false` if the vertex is normal.
    pub fn check_vertex_state(&self, vertex_id: VId) -> Option<bool> {
        let vertex_index_state = self.vertex_index.read();
        Some(vertex_index_state.vertex_array[vertex_id as usize].is_giant())
    }

    /// Get all the vertices in this graph.
    pub fn get_all_vertex_id(&self) -> Vec<VId> {
        (0..self.vertex_count())
            .map(|vertex_id| vertex_id as VId)
            .collect::<Vec<VId>>()
    }

    /// Insert an edge from src_vertex to dst_vertex.
    pub fn insert_edge(&self, src_vertex: VId, dst_vertex: VId) -> anyhow::Result<()> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(src_vertex) {
            if let Some(_) = self.check_vertex_state(dst_vertex) {
                self.external_db.append_delta(
                    src_vertex,
                    DeltaOperation::new(
                        generate_timestamp_micros(),
                        DeltaOpType::AddNeighbor,
                        dst_vertex,
                    ),
                )
            } else {
                // If the vertex not exists, return error.
                Err(anyhow::anyhow!("Vertex not exists"))
            }
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Vertex not exists"))
        }
    }

    /// Remove an edge from src_vertex to dst_vertex.
    pub fn remove_edge(&self, src_vertex: VId, dst_vertex: VId) -> anyhow::Result<()> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(src_vertex) {
            if let Some(_) = self.check_vertex_state(dst_vertex) {
                self.external_db.append_delta(
                    src_vertex,
                    DeltaOperation::new(
                        generate_timestamp_micros(),
                        DeltaOpType::RemoveNeighbor,
                        dst_vertex,
                    ),
                )
            } else {
                // If the vertex not exists, return error.
                Err(anyhow::anyhow!("Vertex not exists"))
            }
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Vertex not exists"))
        }
    }

    /// Put the vertex property, through all field.
    pub fn put_vertex_property(&self, vertex_id: VId, property_bytes: &[u8]) -> anyhow::Result<()> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(vertex_id) {
            self.external_db
                .put_vertex_property(vertex_id, "all".to_owned(), property_bytes)
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Vertex not exists"))
        }
    }

    /// Put the vertex property, through all field.
    pub fn get_vertex_property(&self, vertex_id: VId) -> anyhow::Result<Option<Vec<u8>>> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(vertex_id) {
            self.external_db
                .get_vertex_property(vertex_id, "all".to_owned())
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Vertex not exists"))
        }
    }

    /// Get the edge property through all field.
    pub fn put_edge_property(
        &self,
        src_vertex: VId,
        dst_vertex: VId,
        property_bytes: &[u8],
    ) -> anyhow::Result<()> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(src_vertex) {
            if let Some(_) = self.check_vertex_state(dst_vertex) {
                self.external_db.put_edge_property(
                    src_vertex,
                    dst_vertex,
                    "all".to_owned(),
                    property_bytes,
                )
            } else {
                // If the vertex not exists, return error.
                Err(anyhow::anyhow!("Vertex not exists"))
            }
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Vertex not exists"))
        }
    }

    /// Get the edge property through all field.
    pub fn get_edge_property(
        &self,
        src_vertex: VId,
        dst_vertex: VId,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        // Check the type of source vertex.
        if let Some(_) = self.check_vertex_state(src_vertex) {
            if let Some(_) = self.check_vertex_state(dst_vertex) {
                self.external_db
                    .get_edge_property(src_vertex, dst_vertex, "all".to_owned())
            } else {
                // If the vertex not exists, return error.
                Err(anyhow::anyhow!("Dst Vertex not exists"))
            }
        } else {
            // If the vertex not exists, return error.
            Err(anyhow::anyhow!("Src Vertex not exists"))
        }
    }

    /// Insert a new vertex in async manner.
    pub fn insert_vertex_async(&self, vertex_id: VId) -> anyhow::Result<VId> {
        // Check if the vertex already exists
        if let Some(_) = self.check_vertex_state(vertex_id) {
            return Err(anyhow::anyhow!("Vertex already exists"));
        }

        let mut vertex_index_state = self.vertex_index.write();
        let result_vertex_id = vertex_index_state.add_giant_vertex()?;

        // Drop write lock immediately after modification
        drop(vertex_index_state);

        // Clone Arc to vertex_index (this is cheap - just incrementing refcount)
        let vertex_index_arc = Arc::clone(&self.vertex_index);

        // Construct the vertex index file path
        let vertex_index_path = PathBuf::from(&self.inner.options.work_space_dir)
            .join(&self.inner.options.graph_name)
            .join("vertex_index.bin.zst");

        std::thread::spawn(move || {
            // Try to acquire read lock with timeout (if you use parking_lot's RwLock)
            // or just use regular read() if you don't care about timeout
            let vertex_index_state = vertex_index_arc.read();

            if let Err(e) = vertex_index_state.serialize_to_file(&vertex_index_path, 3) {
                eprintln!("Failed to save vertex index: {}", e);
            }
        });

        Ok(result_vertex_id)
    }

    /// Insert a new vertex.
    pub fn insert_vertex(&self) -> anyhow::Result<VId> {
        let mut vertex_index_state = self.vertex_index.write();
        let result_vertex_id = vertex_index_state.add_giant_vertex()?;

        // Construct the vertex index file path
        let vertex_index_path = PathBuf::from(&self.inner.options.work_space_dir)
            .join(&self.inner.options.graph_name)
            .join("vertex_index.bin.zst");

        // Save immediately while holding the write lock
        vertex_index_state.serialize_to_file(&vertex_index_path, 3)?;

        Ok(result_vertex_id)
    }

    /// Warm up the cache.
    pub fn warm_up(&self) -> anyhow::Result<()> {
        let vertex_index_state = self.vertex_index.read();
        let giant_vertex_ids = self.inner.warm_up(&vertex_index_state);
        println!("Loading Giant Vertices");
        #[cfg(not(test))]
        let progress_bar = {
            use indicatif::{ProgressBar, ProgressStyle};
            let total_vertices: usize = giant_vertex_ids.len();
            let pb = ProgressBar::new(total_vertices as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}",
                    )
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_message("Loading giant vertices");
            pb
        };
        // Warm up the external storage
        giant_vertex_ids
            .into_par_iter()
            .for_each(|giant_vertex_id| {
                self.external_db.get_giant_vertex(giant_vertex_id);
                #[cfg(not(test))]
                progress_bar.inc(1);
            });
        #[cfg(not(test))]
        progress_bar.finish_with_message("Giant Vertices loaded");
        println!("Loading Giant Vertices - [OK]");
        Ok(())
    }

    /// Read all edges in this graph.
    pub fn read_all_edges(&self) -> anyhow::Result<EdgeList> {
        // Step 1: For each vertices;
        let mut all_edges = Vec::<(VId, VId)>::new();
        for vertex_id in self.get_all_vertex_id() {
            let neighbor_list = self.read_out_neighbor_clone(vertex_id)?;
            all_edges.extend(neighbor_list.into_iter().map(|dst| (vertex_id, dst)));
        }
        Ok(all_edges)
    }

    /// Read the in neighbors of a vertex.
    pub fn read_in_neighbor_clone(&self, vertex_id: VId) -> anyhow::Result<VIdList> {
        let all_edges = self.read_all_edges()?;
        let in_neighbors = all_edges
            .iter()
            .filter_map(|(src, dst)| if *dst == vertex_id { Some(*src) } else { None })
            .collect::<Vec<_>>();
        Ok(in_neighbors)
    }

    /// Read the out neighbors of a vertex.
    pub fn read_out_neighbor_clone(&self, vertex_id: VId) -> anyhow::Result<VIdList> {
        let (iter, delta_opt) = self.read_neighbor(vertex_id, true)?;

        // Collect base neighbors
        let mut base_neighbors = match iter {
            Some(iter) => iter.collect::<Vec<_>>(),
            None => Vec::new(),
        };

        // If no delta, return base directly
        let Some(delta) = delta_opt else {
            return Ok(base_neighbors);
        };

        // Apply delta operations efficiently
        #[cfg(test)]
        println!("Applying delta to neighbors");

        Self::apply_delta_to_neighbors(&mut base_neighbors, &delta);

        Ok(base_neighbors)
    }

    /// Apply delta operations to the neighbor list in-place
    ///
    /// Performance characteristics:
    /// - Time: O(n + m) where n=base_neighbors, m=delta.ops
    /// - Space: O(k) where k=unique neighbors in delta
    pub fn apply_delta_to_neighbors(base_neighbors: &mut Vec<VId>, delta: &DeltaLog) {
        if delta.ops().is_empty() {
            return;
        }

        // Build the final state from delta operations
        // Use FxHashSet for integer keys (faster than std HashMap for VId)
        let mut neighbor_state: FxHashSet<VId> = FxHashSet::default();

        // Start with base neighbors
        neighbor_state.extend(base_neighbors.iter().copied());

        // Apply each delta operation in order (already sorted by timestamp)
        for op in delta.ops() {
            match op.op_type {
                0 => {
                    // AddNeighbor
                    neighbor_state.insert(op.neighbor);
                }
                1 => {
                    // RemoveNeighbor
                    neighbor_state.remove(&op.neighbor);
                }
                _ => {
                    // Reserved for future use, ignore unknown operations
                }
            }
        }

        // Rebuild the neighbor list from final state
        base_neighbors.clear();
        base_neighbors.extend(neighbor_state.into_iter());

        // Optional: sort for deterministic output
        // Remove this if order doesn't matter for your use case
        base_neighbors.sort_unstable();
    }

    /// Read neighbors and optionally delta log for a vertex.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID to query
    /// * `with_delta` - Whether to fetch the delta log from external storage
    ///
    /// # Returns
    ///
    /// Returns a tuple of:
    /// - `Option<UnifiedNeighborIterator>`: Iterator over neighbors (None if vertex doesn't exist)
    /// - `Option<DeltaLog>`: Delta log if with_delta is true and deltas exist (None otherwise)
    ///
    /// # Behavior
    ///
    /// - If vertex doesn't exist: returns (None, None)
    /// - If vertex exists but has no neighbors: returns (Some(empty_iter), delta_option)
    /// - If vertex exists and has neighbors:
    ///   - First checks external_db for giant vertices
    ///   - Otherwise uses internal LSM-Community storage
    /// - Delta log is only queried if with_delta is true
    pub fn read_neighbor(
        &self,
        vertex_id: VId,
        with_delta: bool,
    ) -> anyhow::Result<(Option<UnifiedNeighborIterator>, Option<DeltaLog>)> {
        let vertex_index_state = self.vertex_index.read();
        // Step 1: Check if vertex exists by checking vertex index
        if let Some(is_giant) = self.check_vertex_state(vertex_id) {
            if is_giant {
                // If giant;
                if let Some(giant_neighbors) = self.external_db.get_giant_vertex(vertex_id) {
                    // Found in external storage as giant vertex
                    let iter = UnifiedNeighborIterator::from_external(giant_neighbors);

                    // Step 3: Get delta log if requested
                    let delta_log = if with_delta {
                        self.external_db.read_delta_log(vertex_id)?
                    } else {
                        None
                    };
                    return Ok((Some(iter), delta_log));
                } else {
                    // Step 3: Get delta log if requested
                    let delta_log = if with_delta {
                        self.external_db.read_delta_log(vertex_id)?
                    } else {
                        None
                    };
                    return Ok((None, delta_log));
                }
            } else {
                // If normal;
                let global_iter = self.inner.get_neighbor_iter(vertex_id, &vertex_index_state);
                let iter = UnifiedNeighborIterator::from_internal(global_iter);

                // Step 3: Get delta log if requested
                let delta_log = if with_delta {
                    self.external_db.read_delta_log(vertex_id)?
                } else {
                    None
                };
                return Ok((Some(iter), delta_log));
            }

            // If with delta;
        } else {
            println!("Invalid Vertex");
            return Ok((None, None));
        }
    }

    /// Read neighbors and optionally delta log for a vertex.
    /// Used for graph analytic algorithms that hold the vertex index in memory.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID to query
    /// * `with_delta` - Whether to fetch the delta log from external storage
    ///
    /// # Returns
    ///
    /// Returns a tuple of:
    /// - `Option<UnifiedNeighborIterator>`: Iterator over neighbors (None if vertex doesn't exist)
    /// - `Option<DeltaLog>`: Delta log if with_delta is true and deltas exist (None otherwise)
    ///
    /// # Behavior
    ///
    /// - If vertex doesn't exist: returns (None, None)
    /// - If vertex exists but has no neighbors: returns (Some(empty_iter), delta_option)
    /// - If vertex exists and has neighbors:
    ///   - First checks external_db for giant vertices
    ///   - Otherwise uses internal LSM-Community storage
    /// - Delta log is only queried if with_delta is true
    pub fn read_neighbor_hold_index_vertex(
        &self,
        vertex_id: VId,
        with_delta: bool,
        vertex_index: &VertexIndex,
    ) -> anyhow::Result<(Option<UnifiedNeighborIterator>, Option<DeltaLog>)> {
        // Step 1: Check if vertex exists by checking vertex index
        if let Some(is_giant) = self.check_vertex_state(vertex_id) {
            if is_giant {
                // If giant;
                if let Some(giant_neighbors) = self.external_db.get_giant_vertex(vertex_id) {
                    // Found in external storage as giant vertex
                    let iter = UnifiedNeighborIterator::from_external(giant_neighbors);

                    // Step 3: Get delta log if requested
                    let delta_log = if with_delta {
                        self.external_db.read_delta_log(vertex_id)?
                    } else {
                        None
                    };
                    return Ok((Some(iter), delta_log));
                } else {
                    // Step 3: Get delta log if requested
                    let delta_log = if with_delta {
                        self.external_db.read_delta_log(vertex_id)?
                    } else {
                        None
                    };
                    return Ok((None, delta_log));
                }
            } else {
                // If normal;
                let global_iter = self.inner.get_neighbor_iter(vertex_id, &vertex_index);
                let iter = UnifiedNeighborIterator::from_internal(global_iter);

                // Step 3: Get delta log if requested
                let delta_log = if with_delta {
                    self.external_db.read_delta_log(vertex_id)?
                } else {
                    None
                };
                return Ok((Some(iter), delta_log));
            }

            // If with delta;
        } else {
            println!("Invalid Vertex");
            return Ok((None, None));
        }
    }
}
