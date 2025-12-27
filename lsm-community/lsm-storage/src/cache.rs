use std::sync::Arc;

use crate::{
    block::Block,
    types::{PageId, VirtualCommId},
};

/// Cache key packed into a single u64 for efficient hashing and storage.
/// Layout: [virtual_comm_id: 32bits][page_id: 32bits]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey(pub u64);

impl CacheKey {
    /// Create a new cache key from components.
    #[inline]
    pub fn new(virtual_comm_id: VirtualCommId, page_id: PageId) -> Self {
        let key = ((virtual_comm_id as u64) << 32) | (page_id as u64);
        Self(key)
    }

    /// Extract the virtual community ID.
    #[inline]
    pub fn virtual_comm_id(&self) -> VirtualCommId {
        (self.0 >> 32) as VirtualCommId
    }

    /// Extract the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        (self.0 & 0xFFFFFFFF) as PageId
    }

    /// Get the raw u64 value (useful for debugging).
    #[inline]
    pub fn raw(&self) -> u64 {
        self.0
    }
}

/// Block cache type using the packed cache key.
pub type BlockCache = moka::sync::Cache<CacheKey, Arc<Block>>;
