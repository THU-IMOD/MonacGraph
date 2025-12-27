use std::path::Path;

use crate::{
    block::builder::BlockBuilder,
    bucket::{Bucket, VertexMeta, bloom::Bloom, disk_manager::BktDiskManager},
    types::{PageId, VId, VirtualCommId},
};
use anyhow::Result;
use bytes::BufMut;

/// The bucket builder to build a bucket.
#[allow(dead_code)]
pub struct BucketBuilder {
    // The block builer instance
    builder: BlockBuilder,

    // The block size
    block_size: usize,

    // Edge hashs used for building bloom filter
    edge_hashes: Vec<u32>,

    // The encoded data
    data: Vec<u8>,

    // The vertex meta data
    pub(crate) vertex_metas: Vec<VertexMeta>,

    // Current page id (increments with each finished block)
    current_page_id: PageId,
}

impl BucketBuilder {
    /// Create a new bucket builder with specified block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            block_size,
            edge_hashes: Vec::new(),
            data: Vec::new(),
            vertex_metas: Vec::new(),
            current_page_id: 0,
        }
    }

    /// Add a vertex with its neighbors to the bucket.
    /// The neighbors are provided as an iterator.
    pub fn add(&mut self, vertex_id: VId, neighbors: impl Iterator<Item = VId>) {
        // Collect neighbors into a Vec (needed for BlockBuilder::add_vertex)
        let neighbors_vec: Vec<VId> = neighbors.collect();

        // Hash all edges for bloom filter
        for &neighbor in &neighbors_vec {
            // Hash the edge (vertex_id, neighbor)
            // You can use farmhash or any hash function
            let edge_u64 = ((vertex_id as u64) << 32) | (neighbor as u64);
            let edge_hash = farmhash::hash64(&edge_u64.to_le_bytes()) as u32;
            self.edge_hashes.push(edge_hash);
        }

        // Try to add vertex to current block
        if !self.builder.add_vertex(vertex_id, &neighbors_vec) {
            // Current block is full, finish it and start a new one
            self.finish_block();

            // Add to the new block (must succeed)
            let new_result = self.builder.add_vertex(vertex_id, &neighbors_vec);

            assert!(new_result, "vertex should fit in empty block");
        }
    }

    /// Finish the current block and append it to data.
    fn finish_block(&mut self) {
        // Build the current block and get vertex index mapping
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let (block, vertex_index_map) = builder.build();

        // Encode the block
        let encoded_block = block.encode();

        // Record vertex metadata for all vertices in this block
        for (vertex_id, vertex_index) in vertex_index_map {
            self.vertex_metas.push(VertexMeta {
                vertex_id,
                page_id: self.current_page_id,
                offset_inner: vertex_index,
            });
        }

        // Append encoded block to data
        self.data.extend_from_slice(encoded_block);

        // Increment page id for next block
        self.current_page_id += 1;
    }

    pub fn build(mut self, id: VirtualCommId, path: impl AsRef<Path>) -> Result<Bucket> {
        // Finish the last block if not empty
        if !self.builder.is_empty() {
            self.finish_block();
        }

        // Start with block data
        let mut buf = self.data;

        // Record vertex meta offset (where vertex metas start)
        let vertex_meta_offset = buf.len();

        // Encode vertex metas
        VertexMeta::encode(&self.vertex_metas, &mut buf);

        // Build bloom filter with adaptive FPR
        let bits_per_key = Self::calculate_bits_per_key(self.edge_hashes.len());
        let bloom = Bloom::build_from_key_hashes(&self.edge_hashes, bits_per_key);

        // Record bloom filter start position
        let bloom_offset = buf.len();

        // Encode bloom filter
        bloom.encode(&mut buf);

        // Calculate bloom size
        let bloom_size = buf.len() - bloom_offset;

        // Write footer (3 x u32 = 12 bytes)
        buf.put_u32(self.block_size as u32); // Block size
        buf.put_u32(vertex_meta_offset as u32); // Vertex meta offset
        buf.put_u32(bloom_size as u32); // Bloom size

        // Create disk file
        let file = BktDiskManager::create(path.as_ref(), &buf)?;

        // Build and return bucket
        Ok(Bucket {
            file,
            vertex_block_map: None,
            vertex_metas: self.vertex_metas,
            vertex_meta_offset,
            edge_bloom_length: bloom_size,
            block_size: self.block_size,
            virtual_comm_id: id,
            edge_bloom: Some(bloom),
        })
    }

    /// Calculate optimal bits per key for bloom filter based on edge count.
    /// Adaptive FPR: 1% for small, 3% for medium, 5% for large graphs.
    fn calculate_bits_per_key(edge_count: usize) -> usize {
        let fpr = if edge_count < 10_000_000 {
            0.01 // 1% FPR for < 10M edges
        } else if edge_count < 100_000_000 {
            0.03 // 3% FPR for 10M-100M edges
        } else {
            0.05 // 5% FPR for > 100M edges
        };

        Bloom::bloom_bits_per_key(edge_count, fpr)
    }
}
