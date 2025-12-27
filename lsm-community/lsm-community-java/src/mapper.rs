use dashmap::DashMap;
use rocksdb::{DB, Options};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// Vertex ID mapper: external bytes ↔ internal u32 (consecutive from 0)
pub struct VertexIdMapper {
    /// External ID (bytes) -> Internal ID (u32)
    /// In-memory for fast lookup
    outer_to_inner: DashMap<Vec<u8>, u32>,

    /// Internal ID (u32) -> External ID (bytes)
    /// Using Vec since inner IDs are consecutive: 0, 1, 2, ..., n-1
    /// Vec index IS the inner_id, providing O(1) lookup
    inner_to_outer: RwLock<Vec<Vec<u8>>>,

    /// Next available internal vertex ID (also equals current vertex count)
    next_inner_id: AtomicU32,

    /// RocksDB instance for persistence
    /// Key: outer_id (Vec<u8>), Value: inner_id (u32 as bytes)
    db: Arc<DB>,
}

impl VertexIdMapper {
    pub fn new(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();

        // Optimize RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // optimize for point lookups if needed, though we scan on startup

        let db = DB::open(&opts, &path)?;

        let outer_to_inner = DashMap::new();
        let inner_to_outer = RwLock::new(Vec::new());
        let next_inner_id = AtomicU32::new(0);

        let mapper = Self {
            outer_to_inner,
            inner_to_outer,
            next_inner_id,
            db: Arc::new(db),
        };

        // Recover state from RocksDB to Memory
        mapper.recover_from_db()?;

        Ok(mapper)
    }

    /// Recover in-memory state by scanning the entire RocksDB
    fn recover_from_db(&self) -> anyhow::Result<()> {
        let mut max_inner_id = 0;
        let mut count = 0;

        // Iterate over all items in RocksDB
        // Since RocksDB keys are sorted by byte content (outer_id),
        // the inner_ids will appear in random order.
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        // Temporary vector to hold data before bulk inserting into RwLock Vec
        // We use a BTreeMap or similar intermediate if we strictly needed ordering,
        // but here we just need to resize the Vec correctly.
        // Direct insertion into RwLock is fine, but let's be careful about resizing.
        let mut recovered_vec: Vec<Vec<u8>> = Vec::new();

        for item in iter {
            let (key, value) = item?; // key is outer_id, value is inner_id bytes

            // Parse inner_id (u32) from bytes
            let inner_id = if value.len() == 4 {
                u32::from_be_bytes(value[0..4].try_into().unwrap())
            } else {
                continue; // Should not happen if data is clean
            };

            // Update Max ID for next_inner_id calculation
            if inner_id >= max_inner_id {
                max_inner_id = inner_id;
            }
            count += 1;

            // 1. Restore DashMap
            let outer_id = key.to_vec();
            self.outer_to_inner.insert(outer_id.clone(), inner_id);

            // 2. Prepare for Vec
            // Resize if necessary
            if inner_id as usize >= recovered_vec.len() {
                recovered_vec.resize(inner_id as usize + 1, Vec::new());
            }
            recovered_vec[inner_id as usize] = outer_id;
        }

        // Apply recovered vector
        let mut inner_to_outer_lock = self.inner_to_outer.write().unwrap();
        *inner_to_outer_lock = recovered_vec;

        // Update atomic counter
        // If count > 0, next id is max_id + 1. If 0, it is 0.
        // Note: This logic assumes IDs are strictly reused or we just append.
        // If we strictly follow the log logic, next_id should be max + 1.
        if count > 0 {
            self.next_inner_id
                .store(max_inner_id + 1, Ordering::Relaxed);
        } else {
            self.next_inner_id.store(0, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Insert a mapping from external vertex ID to internal ID
    pub fn insert(&self, outer_id: &[u8], inner_id: u32) -> anyhow::Result<()> {
        // 1. Persist to RocksDB first (WAL ensures durability)
        // Store inner_id as Big Endian bytes
        self.db.put(outer_id, inner_id.to_be_bytes())?;

        // 2. Update In-Memory DashMap
        self.outer_to_inner.insert(outer_id.to_vec(), inner_id);

        // 3. Update In-Memory Vec
        let mut inner_to_outer = self.inner_to_outer.write().unwrap();
        if inner_id as usize >= inner_to_outer.len() {
            inner_to_outer.resize(inner_id as usize + 1, Vec::new());
        }
        inner_to_outer[inner_id as usize] = outer_id.to_vec();
        drop(inner_to_outer);

        // 4. Update next_inner_id if necessary
        // Using fetch_max usually works, or simple CAS loop, or just store if larger.
        let mut current = self.next_inner_id.load(Ordering::Relaxed);
        loop {
            if inner_id < current {
                break;
            }
            let next_candidate = inner_id + 1;
            match self.next_inner_id.compare_exchange_weak(
                current,
                next_candidate,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(val) => current = val,
            }
        }

        Ok(())
    }

    /// Remove a mapping from external vertex ID to internal ID
    pub fn remove(&self, outer_id: &[u8]) -> anyhow::Result<Option<u32>> {
        // Check if exists in memory first
        let removed_inner_id = if let Some((_, v)) = self.outer_to_inner.remove(outer_id) {
            Some(v)
        } else {
            None
        };

        if let Some(inner_id) = removed_inner_id {
            // 1. Remove from RocksDB
            self.db.delete(outer_id)?;

            // 2. Clear inner_to_outer entry (set to empty vec, do not shrink vec to keep indices valid)
            let mut inner_to_outer = self.inner_to_outer.write().unwrap();
            if (inner_id as usize) < inner_to_outer.len() {
                inner_to_outer[inner_id as usize] = Vec::new();
            }
        }

        Ok(removed_inner_id)
    }

    pub fn get_inner_id(&self, outer_id: &[u8]) -> Option<u32> {
        self.outer_to_inner
            .get(outer_id)
            .map(|entry| *entry.value())
    }

    pub fn get_outer_id(&self, inner_id: u32) -> Option<Vec<u8>> {
        let inner_to_outer = self.inner_to_outer.read().unwrap();
        if (inner_id as usize) < inner_to_outer.len() {
            let outer = &inner_to_outer[inner_id as usize];
            if outer.is_empty() {
                None
            } else {
                Some(outer.clone())
            }
        } else {
            None
        }
    }
}

/// Edge ID mapper: external bytes ↔ internal edge handle (i64)
pub struct EdgeIdMapper {
    /// External edge ID (bytes) -> Internal edge handle (i64)
    outer_to_inner: DashMap<Vec<u8>, i64>,

    /// Internal edge handle (i64) -> External edge ID (bytes)
    inner_to_outer: DashMap<i64, Vec<u8>>,

    /// RocksDB instance for persistence
    /// Key: outer_id (Vec<u8>), Value: edge_handle (i64 as bytes)
    db: Arc<DB>,
}

impl EdgeIdMapper {
    pub fn new(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();

        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, &path)?;

        let mapper = Self {
            outer_to_inner: DashMap::new(),
            inner_to_outer: DashMap::new(),
            db: Arc::new(db),
        };

        mapper.recover_from_db()?;

        Ok(mapper)
    }

    fn recover_from_db(&self) -> anyhow::Result<()> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item?; // key: outer, value: i64 bytes

            let edge_handle = if value.len() == 8 {
                i64::from_be_bytes(value[0..8].try_into().unwrap())
            } else {
                continue;
            };

            let outer_id = key.to_vec();

            // Populate Memory
            self.outer_to_inner.insert(outer_id.clone(), edge_handle);
            self.inner_to_outer.insert(edge_handle, outer_id);
        }

        Ok(())
    }

    pub fn insert(&self, outer_id: &[u8], edge_handle: i64) -> anyhow::Result<()> {
        // 1. Persist to RocksDB
        self.db.put(outer_id, edge_handle.to_be_bytes())?;

        // 2. Update Memory
        self.outer_to_inner.insert(outer_id.to_vec(), edge_handle);
        self.inner_to_outer.insert(edge_handle, outer_id.to_vec());

        Ok(())
    }

    pub fn remove(&self, outer_id: &[u8]) -> anyhow::Result<Option<i64>> {
        let removed_handle = self.outer_to_inner.remove(outer_id).map(|(_, v)| v);

        if let Some(handle) = removed_handle {
            // 1. Remove from RocksDB
            self.db.delete(outer_id)?;

            // 2. Remove from second map
            self.inner_to_outer.remove(&handle);
        }

        Ok(removed_handle)
    }

    pub fn get_inner_id(&self, outer_id: &[u8]) -> Option<i64> {
        self.outer_to_inner.get(outer_id).map(|e| *e.value())
    }

    pub fn get_outer_id(&self, edge_handle: i64) -> Option<Vec<u8>> {
        self.inner_to_outer
            .get(&edge_handle)
            .map(|e| e.value().clone())
    }

    /// Pack two u32 vertex IDs into a single i64 edge handle
    #[inline]
    pub fn pack_edge_handle(src: u32, dst: u32) -> i64 {
        let packed = ((src as u64) << 32) | (dst as u64);
        packed as i64
    }

    /// Unpack i64 edge handle back to two u32 vertex IDs
    #[inline]
    pub fn unpack_edge_handle(edge_handle: i64) -> (u32, u32) {
        let packed = edge_handle as u64;
        let src = (packed >> 32) as u32;
        let dst = (packed & 0xFFFF_FFFF) as u32;
        (src, dst)
    }
}
