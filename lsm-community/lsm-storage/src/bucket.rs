use std::sync::Arc;

use crate::{
    block::Block,
    bucket::disk_manager::BktDiskManager,
    types::{PageId, VId, VirtualCommId},
};
use anyhow::Result;
use anyhow::bail;
use bloom::Bloom;
use bytes::{Buf, BufMut};
use rustc_hash::FxHashMap;

mod bloom;
pub mod builder;
pub mod disk_manager;

/// The metadata of each vertex in bucket
#[derive(Debug)]
pub struct VertexMeta {
    // The id of this vertex
    pub vertex_id: VId,

    // The page id of this vertex
    pub page_id: PageId,

    // The inner offset of this vertex
    pub offset_inner: u16,
}

impl VertexMeta {
    pub fn encode(vertex_meta: &[VertexMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = std::mem::size_of::<u32>(); // number of vertices
        for _ in vertex_meta {
            estimated_size += std::mem::size_of::<u32>(); // vertex_id
            estimated_size += std::mem::size_of::<u32>(); // page_id
            estimated_size += std::mem::size_of::<u16>(); // offset_inner
        }

        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(vertex_meta.len() as u32);
        for meta in vertex_meta {
            buf.put_u32(meta.vertex_id);
            buf.put_u32(meta.page_id);
            buf.put_u16(meta.offset_inner);
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Vec<VertexMeta>> {
        let mut vertex_meta = Vec::new();
        let num = buf.get_u32() as usize;
        for _ in 0..num {
            let vertex_id = buf.get_u32();
            let page_id = buf.get_u32();
            let offset_inner = buf.get_u16();
            vertex_meta.push(VertexMeta {
                vertex_id,
                page_id,
                offset_inner,
            });
        }
        Ok(vertex_meta)
    }
}

/// The bucket structure, corresponding to the SsTable in LSM-Tree.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Bucket {
    // Disk manager.
    file: BktDiskManager,

    // Map to get the vertex block.
    vertex_block_map: Option<FxHashMap<VId, (PageId, u16)>>,

    // Meta data of the stored vertex.
    pub vertex_metas: Vec<VertexMeta>,

    // The offset of the meta data, i.e., the length of block segment.
    vertex_meta_offset: usize,

    // The length of bloom filter
    edge_bloom_length: usize,

    // Block size in this bucket
    block_size: usize,

    // The virtual community id of this bucket.
    virtual_comm_id: VirtualCommId,

    // Edge bloom filter of this bucket.
    pub(crate) edge_bloom: Option<Bloom>,
}

impl Bucket {
    /// Get virtual community id;
    pub fn get_vritual_community_id(&self) -> VirtualCommId {
        self.virtual_comm_id
    }

    /// Get block size
    pub fn get_block_size(&self) -> usize {
        self.block_size
    }

    /// Open bucket from a file.
    pub fn open(id: VirtualCommId, file: BktDiskManager, build_map: bool) -> Result<Self> {
        let len = file.size();

        // Read bloom size (last 4 bytes, file size - 4)
        let raw_bloom_size = file.read(len - 4, 4)?;
        println!("File size: {}", len);
        let bloom_size = (&raw_bloom_size[..]).get_u32() as u64;

        // Read vertex meta offset (file size - 8)
        let raw_meta_offset = file.read(len - 8, 4)?;
        let vertex_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;

        // Read block size (file size - 12)
        let raw_block_size = file.read(len - 12, 4)?;
        let block_size = (&raw_block_size[..]).get_u32() as usize;

        // Read bloom filter
        let bloom_offset = len - 12 - bloom_size;
        let raw_bloom = file.read(bloom_offset, bloom_size)?;
        let edge_bloom = Bloom::decode(&raw_bloom)?;

        // Read vertex metas
        let meta_len = bloom_offset - vertex_meta_offset;
        let raw_meta = file.read(vertex_meta_offset, meta_len)?;
        let vertex_metas = VertexMeta::decode(&raw_meta[..])?;

        let vertex_block_map = if build_map {
            let mut block_map = FxHashMap::<VId, (PageId, u16)>::default();
            // Travel vertex metas
            for vertex_meta in &vertex_metas {
                block_map.insert(
                    vertex_meta.vertex_id,
                    (vertex_meta.page_id, vertex_meta.offset_inner),
                );
            }
            Some(block_map)
        } else {
            None
        };

        Ok(Self {
            file,
            vertex_block_map,
            vertex_metas,
            vertex_meta_offset: vertex_meta_offset as usize,
            edge_bloom_length: bloom_size as usize,
            block_size,
            virtual_comm_id: id,
            edge_bloom: Some(edge_bloom),
        })
    }

    /// Read a block from the disk by page_id.
    pub fn read_block(&self, page_id: PageId) -> Result<Arc<Block>> {
        // Calculate block offset: page_id * block_size
        let offset = page_id as usize * self.block_size;

        // Ensure the offset is within the block segment
        if offset >= self.vertex_meta_offset {
            bail!(
                "Page {} is out of bounds (offset {} >= vertex_meta_offset {})",
                page_id,
                offset,
                self.vertex_meta_offset
            );
        }

        // Calculate the end offset (next block start or vertex_meta_offset)
        let offset_end = std::cmp::min(offset + self.block_size, self.vertex_meta_offset);

        let block_len = offset_end - offset;

        // Read block data from disk
        let block_data = self.file.read(offset as u64, block_len as u64)?;

        // Decode and return the block
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Get neighbors of a vertex
    pub fn get_neighbors_for_test(&mut self, vertex_id: VId) -> Result<Vec<VId>> {
        // Build block map if not exists
        if self.vertex_block_map.is_none() {
            let mut map = FxHashMap::default();
            for meta in &self.vertex_metas {
                map.insert(meta.vertex_id, (meta.page_id, meta.offset_inner));
            }
            self.vertex_block_map = Some(map);
        }

        // Find the page for this vertex
        let (page_id, inner_offset) = self
            .vertex_block_map
            .as_ref()
            .unwrap()
            .get(&vertex_id)
            .ok_or_else(|| anyhow::anyhow!("Vertex {} not found", vertex_id))?;

        // Read the block
        let block = self.read_block(*page_id)?;

        // Get neighbors from block
        let neighbors = block.get_neighbor_clone(*inner_offset as usize).unwrap();

        Ok(neighbors)
    }
}
