use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, MergeOperands, Options};
use std::{path::Path, sync::Arc};

use crate::{
    config::LsmCommunityStorageOptions,
    delta::{DeltaLog, DeltaOperation},
    property::{EdgePropertyKey, VertexPropertyKey},
    types::{VId, VIdList},
};

pub type GiantVertexCache = moka::sync::Cache<VId, Arc<VIdList>>;

/// Represents a delta operation on a vertex's neighbor set
#[derive(Debug, Clone)]
pub enum DeltaOp {
    /// Add a neighbor vertex
    AddNeighbor(VId),
    /// Remove a neighbor vertex
    RemoveNeighbor(VId),
}

/// External storage engine for storing large-scale graph data that doesn't fit
/// in the main LSM-Community storage structure.
///
/// This storage manages three types of data using RocksDB column families:
///
/// 1. **Giant Vertices**: Vertices with extremely high degree (exceeding threshold)
///    - Key: vertex_id (u64, 8 bytes)
///    - Value: adjacency list (compressed neighbor list)
///
/// 2. **Delta Updates**: Short-term incremental edge modifications
///    - Key: vertex_id (u64, 8 bytes)
///    - Value: delta operations (e.g., +v2 means add neighbor v2, -v4 means remove v4)
///
/// 3. **Properties**: Vertex and edge attributes
///    - Key (Vertex): vertex_id | property_name (u64 + string)
///    - Key (Edge): src_id | dst_id | property_name (u64 + u64 + string)
///    - Value: property value (serialized binary)
#[allow(dead_code)]
pub struct ExternalStorage {
    /// RocksDB instance with three column families
    db: Arc<DB>,

    /// The giant vertex cache.
    giant_cache: GiantVertexCache,
}

impl ExternalStorage {
    /// Column family name for giant vertices
    const CF_GIANT_VERTICES: &'static str = "giant_vertices";
    /// Column family name for delta updates
    const CF_DELTAS: &'static str = "deltas";
    /// Column family name for vertex properties
    const CF_VERTEX_PROPERTIES: &'static str = "vertex_properties";
    /// Column family name for edge properties
    const CF_EDGE_PROPERTIES: &'static str = "edge_properties";

    /// Creates a new ExternalStorage instance with custom giant vertex cache capacity.
    ///
    /// # Arguments
    ///
    /// * `options` - Configuration options for the storage system
    ///
    /// # Returns
    ///
    /// Returns a new `ExternalStorage` instance or an error if initialization fails.
    pub fn new(options: LsmCommunityStorageOptions) -> anyhow::Result<Self> {
        let db_path = Path::new(&options.work_space_dir)
            .join(&options.graph_name)
            .join("external_db");

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&db_path)?;

        // Configure options for the main database
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Configure column family for giant vertices (optimized for read-heavy workload)
        let mut giant_cf_opts = Options::default();
        giant_cf_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        giant_cf_opts.set_max_write_buffer_number(3);
        giant_cf_opts.set_target_file_size_base(128 * 1024 * 1024); // 128MB SST files

        // Set up block-based table with large block cache for read performance
        let mut giant_block_opts = BlockBasedOptions::default();
        giant_block_opts.set_block_cache(&Cache::new_lru_cache(256 * 1024 * 1024)); // 256MB
        giant_cf_opts.set_block_based_table_factory(&giant_block_opts);

        // Configure column family for deltas (optimized for write-heavy workload with merge operator)
        let mut delta_cf_opts = Options::default();
        delta_cf_opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB write buffer
        delta_cf_opts.set_max_write_buffer_number(5); // More write buffers for high write throughput
        delta_cf_opts.set_level_zero_file_num_compaction_trigger(8);
        delta_cf_opts.set_level_zero_slowdown_writes_trigger(20);

        // Set merge operator for efficient delta accumulation
        delta_cf_opts.set_merge_operator(
            "DeltaLogMergeOperator",
            // Full merge: merge base value (existing DeltaLog) with operands (raw operations)
            |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
                let operand_slices: Vec<&[u8]> = operands.iter().collect();
                DeltaLog::merge_for_rocksdb(existing_value, &operand_slices)
            },
            // Partial merge: merge multiple operands without base value
            |_key: &[u8], _left_operand: Option<&[u8]>, operands: &MergeOperands| {
                let operand_slices: Vec<&[u8]> = operands.iter().collect();
                DeltaLog::partial_merge_for_rocksdb(&operand_slices)
            },
        );

        // Configure column family for vertex properties (balanced configuration)
        let mut vertex_prop_cf_opts = Options::default();
        vertex_prop_cf_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        vertex_prop_cf_opts.set_max_write_buffer_number(3);
        vertex_prop_cf_opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB SST files
        // Enable bloom filter for faster point lookups
        let mut vertex_prop_block_opts = BlockBasedOptions::default();
        vertex_prop_block_opts.set_bloom_filter(10.0, false); // 10 bits per key
        vertex_prop_block_opts.set_block_cache(&Cache::new_lru_cache(128 * 1024 * 1024)); // 128MB
        vertex_prop_cf_opts.set_block_based_table_factory(&vertex_prop_block_opts);

        // Configure column family for edge properties (optimized for potential high volume)
        let mut edge_prop_cf_opts = Options::default();
        edge_prop_cf_opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB write buffer (edges are more numerous)
        edge_prop_cf_opts.set_max_write_buffer_number(4);
        edge_prop_cf_opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB SST files
        edge_prop_cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4); // Fast compression for edge properties
        // Enable bloom filter for faster point lookups
        let mut edge_prop_block_opts = BlockBasedOptions::default();
        edge_prop_block_opts.set_bloom_filter(10.0, false); // 10 bits per key
        edge_prop_block_opts.set_block_cache(&Cache::new_lru_cache(128 * 1024 * 1024)); // 128MB
        edge_prop_cf_opts.set_block_based_table_factory(&edge_prop_block_opts);

        // Create column family descriptors
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(Self::CF_GIANT_VERTICES, giant_cf_opts),
            ColumnFamilyDescriptor::new(Self::CF_DELTAS, delta_cf_opts),
            ColumnFamilyDescriptor::new(Self::CF_VERTEX_PROPERTIES, vertex_prop_cf_opts),
            ColumnFamilyDescriptor::new(Self::CF_EDGE_PROPERTIES, edge_prop_cf_opts),
        ];

        // Open database with column families
        let db = DB::open_cf_descriptors(&db_opts, &db_path, cf_descriptors)?;
        let db = Arc::new(db);

        // Initialize giant vertex cache with LRU eviction policy
        let giant_cache = moka::sync::Cache::builder()
            .max_capacity(options.giant_cache_capacity)
            .build();

        Ok(Self { db, giant_cache })
    }

    /// Append a single delta operation to a vertex's delta log.
    ///
    /// Uses RocksDB's merge operator for efficient append without read-modify-write.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    /// * `op` - The delta operation to append
    pub fn append_delta(&self, vertex_id: VId, op: DeltaOperation) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_DELTAS)
            .ok_or_else(|| anyhow::anyhow!("Delta CF not found"))?;

        let key = vertex_id.to_be_bytes();
        let value = op.encode();

        // Use merge instead of put - this will be accumulated by the merge operator
        self.db.merge_cf(&cf, &key, &value)?;
        Ok(())
    }

    /// Append multiple delta operations to a vertex's delta log in a single merge.
    ///
    /// More efficient than calling append_delta multiple times.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    /// * `ops` - Slice of delta operations to append
    pub fn append_deltas_batch(
        &self,
        vertex_id: VId,
        ops: &[DeltaOperation],
    ) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_DELTAS)
            .ok_or_else(|| anyhow::anyhow!("Delta CF not found"))?;

        let key = vertex_id.to_be_bytes();
        let value = DeltaOperation::encode_batch(ops);

        self.db.merge_cf(&cf, &key, &value)?;
        Ok(())
    }

    /// Read the delta log for a vertex.
    ///
    /// RocksDB will automatically execute the merge operator to combine all
    /// delta operations into a single DeltaLog.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    ///
    /// # Returns
    ///
    /// Returns Some(DeltaLog) if the vertex has delta operations, None otherwise.
    pub fn read_delta_log(&self, vertex_id: VId) -> anyhow::Result<Option<DeltaLog>> {
        let cf = self
            .db
            .cf_handle(Self::CF_DELTAS)
            .ok_or_else(|| anyhow::anyhow!("Delta CF not found"))?;

        let key = vertex_id.to_be_bytes();

        match self.db.get_cf(&cf, &key)? {
            Some(bytes) => {
                let log = DeltaLog::decode(&bytes)?;
                Ok(Some(log))
            }
            None => Ok(None),
        }
    }

    /// Delete the delta log for a vertex.
    ///
    /// This is useful after applying deltas to the base graph or during cleanup.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    pub fn delete_delta_log(&self, vertex_id: VId) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_DELTAS)
            .ok_or_else(|| anyhow::anyhow!("Delta CF not found"))?;

        let key = vertex_id.to_be_bytes();
        self.db.delete_cf(&cf, &key)?;
        Ok(())
    }

    /// Put giant vertex into external DB.
    ///
    /// This method stores a giant vertex's adjacency list both in RocksDB (compressed)
    /// and in the in-memory cache (uncompressed for fast access).
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The ID of the giant vertex
    /// * `neighbor_iter` - Iterator over the neighbor vertex IDs
    ///
    /// # Compression
    ///
    /// The adjacency list is compressed using LZ4 before writing to RocksDB to save space,
    /// but the cache stores uncompressed data for performance.
    pub fn put_giant_vertex(
        &self,
        vertex_id: VId,
        neighbor_iter: impl Iterator<Item = VId>,
    ) -> anyhow::Result<()> {
        // Collect neighbors into a VIdList
        let neighbors: VIdList = neighbor_iter.collect();
        let neighbors_arc = Arc::new(neighbors.clone());

        // Serialize the neighbor list using bincode
        let serialized = bincode::serialize(&neighbors)?;

        // Compress the serialized data using LZ4
        let compressed = lz4::block::compress(&serialized, None, true)?;

        // Encode vertex_id as key (4 bytes for u32, big-endian for proper ordering)
        let key = vertex_id.to_be_bytes();

        // Get CF handle and write to RocksDB
        let cf = self
            .db
            .cf_handle(Self::CF_GIANT_VERTICES)
            .expect("Giant vertices CF should exist");
        self.db.put_cf(&cf, &key, &compressed)?;

        // Insert into cache (uncompressed)
        self.giant_cache.insert(vertex_id, neighbors_arc);

        Ok(())
    }

    /// Get the giant vertex iter.
    ///
    /// This method retrieves a giant vertex's adjacency list. It first checks the cache,
    /// and if not found, reads from RocksDB, decompresses, and populates the cache.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The ID of the giant vertex
    ///
    /// # Returns
    ///
    /// Returns `Some(iterator)` if the vertex exists (even if it has no neighbors),
    /// or `None` if the vertex is not found in storage.
    ///
    /// # Zero-copy Design
    ///
    /// The iterator returns references to cached data without copying, achieved by
    /// returning an iterator over the Arc-wrapped VIdList in the cache.
    /// Get the giant vertex neighbors as an Arc.
    ///
    /// Returns the Arc-wrapped neighbor list if the vertex exists.
    /// Caller can iterate over it without any copying.
    pub fn get_giant_vertex(&self, vertex_id: VId) -> Option<Arc<VIdList>> {
        // Check cache first
        if let Some(neighbors_arc) = self.giant_cache.get(&vertex_id) {
            return Some(neighbors_arc);
        }

        // Cache miss: read from RocksDB
        let key = vertex_id.to_be_bytes();
        let cf = self
            .db
            .cf_handle(Self::CF_GIANT_VERTICES)
            .expect("Giant vertices CF should exist");

        let compressed = self.db.get_cf(&cf, &key).ok()??;
        let decompressed = lz4::block::decompress(&compressed, None).ok()?;
        let neighbors: VIdList = bincode::deserialize(&decompressed).ok()?;

        // Insert into cache
        let neighbors_arc = Arc::new(neighbors);
        self.giant_cache.insert(vertex_id, neighbors_arc.clone());

        Some(neighbors_arc)
    }

    /// Put a vertex property into storage.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    /// * `property_name` - Name of the property
    /// * `value` - Property value as bytes
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if successful, or an error if the operation fails.
    pub fn put_vertex_property(
        &self,
        vertex_id: VId,
        property_name: String,
        value: &[u8],
    ) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_VERTEX_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Vertex properties CF not found"))?;

        let key = VertexPropertyKey::new(vertex_id, property_name);
        let encoded_key = key.encode();

        self.db.put_cf(&cf, &encoded_key, value)?;
        Ok(())
    }

    /// Remove a vertex property from storage.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    /// * `property_name` - Name of the property to remove
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if successful, or an error if the operation fails.
    pub fn remove_vertex_property(
        &self,
        vertex_id: VId,
        property_name: String,
    ) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_VERTEX_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Vertex properties CF not found"))?;

        let key = VertexPropertyKey::new(vertex_id, property_name);
        let encoded_key = key.encode();

        self.db.delete_cf(&cf, &encoded_key)?;
        Ok(())
    }

    /// Get a vertex property from storage.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    /// * `property_name` - Name of the property
    ///
    /// # Returns
    ///
    /// Returns Some(Vec<u8>) if the property exists, None otherwise.
    pub fn get_vertex_property(
        &self,
        vertex_id: VId,
        property_name: String,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(Self::CF_VERTEX_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Vertex properties CF not found"))?;

        let key = VertexPropertyKey::new(vertex_id, property_name);
        let encoded_key = key.encode();

        Ok(self.db.get_cf(&cf, &encoded_key)?)
    }

    /// Get all properties for a vertex.
    ///
    /// # Arguments
    ///
    /// * `vertex_id` - The vertex ID
    ///
    /// # Returns
    ///
    /// Returns a vector of (property_name, property_value) tuples.
    pub fn get_all_vertex_properties(
        &self,
        vertex_id: VId,
    ) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
        let cf = self
            .db
            .cf_handle(Self::CF_VERTEX_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Vertex properties CF not found"))?;

        let prefix = VertexPropertyKey::prefix(vertex_id);
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(&prefix);

        let mut properties = Vec::new();

        while iter.valid() {
            let key = iter.key().unwrap();

            // Check if key still matches the prefix
            if !key.starts_with(&prefix) {
                break;
            }

            let property_key = VertexPropertyKey::decode(key)?;

            // Double check vertex_id matches (should always be true)
            if property_key.vertex_id != vertex_id {
                break;
            }

            let value = iter.value().unwrap().to_vec();
            properties.push((property_key.property_name, value));

            iter.next();
        }

        Ok(properties)
    }

    /// Put an edge property into storage.
    ///
    /// # Arguments
    ///
    /// * `source_id` - The source vertex ID
    /// * `destination_id` - The destination vertex ID
    /// * `property_name` - Name of the property
    /// * `value` - Property value as bytes
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if successful, or an error if the operation fails.
    pub fn put_edge_property(
        &self,
        source_id: VId,
        destination_id: VId,
        property_name: String,
        value: &[u8],
    ) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_EDGE_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Edge properties CF not found"))?;

        let key = EdgePropertyKey::new(source_id, destination_id, property_name);
        let encoded_key = key.encode();

        self.db.put_cf(&cf, &encoded_key, value)?;
        Ok(())
    }

    /// Remove an edge property from storage.
    ///
    /// # Arguments
    ///
    /// * `source_id` - The source vertex ID
    /// * `destination_id` - The destination vertex ID
    /// * `property_name` - Name of the property to remove
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if successful, or an error if the operation fails.
    pub fn remove_edge_property(
        &self,
        source_id: VId,
        destination_id: VId,
        property_name: String,
    ) -> anyhow::Result<()> {
        let cf = self
            .db
            .cf_handle(Self::CF_EDGE_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Edge properties CF not found"))?;

        let key = EdgePropertyKey::new(source_id, destination_id, property_name);
        let encoded_key = key.encode();

        self.db.delete_cf(&cf, &encoded_key)?;
        Ok(())
    }

    /// Get an edge property from storage.
    ///
    /// # Arguments
    ///
    /// * `source_id` - The source vertex ID
    /// * `destination_id` - The destination vertex ID
    /// * `property_name` - Name of the property
    ///
    /// # Returns
    ///
    /// Returns Some(Vec<u8>) if the property exists, None otherwise.
    pub fn get_edge_property(
        &self,
        source_id: VId,
        destination_id: VId,
        property_name: String,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(Self::CF_EDGE_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Edge properties CF not found"))?;

        let key = EdgePropertyKey::new(source_id, destination_id, property_name);
        let encoded_key = key.encode();

        Ok(self.db.get_cf(&cf, &encoded_key)?)
    }

    /// Get all properties for an edge.
    ///
    /// # Arguments
    ///
    /// * `source_id` - The source vertex ID
    /// * `destination_id` - The destination vertex ID
    ///
    /// # Returns
    ///
    /// Returns a vector of (property_name, property_value) tuples.
    pub fn get_all_edge_properties(
        &self,
        source_id: VId,
        destination_id: VId,
    ) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
        let cf = self
            .db
            .cf_handle(Self::CF_EDGE_PROPERTIES)
            .ok_or_else(|| anyhow::anyhow!("Edge properties CF not found"))?;

        let prefix = EdgePropertyKey::prefix(source_id, destination_id);
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(&prefix);

        let mut properties = Vec::new();

        while iter.valid() {
            let key = iter.key().unwrap();

            // Check if key still matches the prefix
            if !key.starts_with(&prefix) {
                break;
            }

            let property_key = EdgePropertyKey::decode(key)?;

            // Double check IDs match (should always be true)
            if property_key.source_id != source_id || property_key.destination_id != destination_id
            {
                break;
            }

            let value = iter.value().unwrap().to_vec();
            properties.push((property_key.property_name, value));

            iter.next();
        }

        Ok(properties)
    }
}
