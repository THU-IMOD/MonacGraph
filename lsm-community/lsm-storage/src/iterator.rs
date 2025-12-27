use std::{sync::Arc, vec::IntoIter};

use crate::{
    block::Block,
    types::{VId, VIdList},
};

pub struct GlobalNeighborIterator {
    mem_iter: IntoIter<VId>,
    block_arc: Option<Arc<Block>>,
    block_iter_state: Option<(usize, usize, usize)>,
    from_mem: bool,
}

impl GlobalNeighborIterator {
    pub fn new(
        mem_neighbors: Vec<VId>,
        block_arc: Option<Arc<Block>>,
        vertex_offset: Option<usize>,
    ) -> Self {
        let block_iter_state = if let (Some(block), Some(offset)) = (&block_arc, vertex_offset) {
            if offset < block.vertex_count as usize {
                let vertex_offset_in_data = block.vertex_list_view.offset + offset * 8;
                let start_index = u32::from_be_bytes([
                    block.data[vertex_offset_in_data + 4],
                    block.data[vertex_offset_in_data + 5],
                    block.data[vertex_offset_in_data + 6],
                    block.data[vertex_offset_in_data + 7],
                ]) as usize;

                let end_index = if offset + 1 < block.vertex_count as usize {
                    let next_vertex_offset = vertex_offset_in_data + 8;
                    u32::from_be_bytes([
                        block.data[next_vertex_offset + 4],
                        block.data[next_vertex_offset + 5],
                        block.data[next_vertex_offset + 6],
                        block.data[next_vertex_offset + 7],
                    ]) as usize
                } else {
                    block.edge_count as usize
                };

                Some((start_index, end_index, block.edge_list_view.offset))
            } else {
                None
            }
        } else {
            None
        };

        Self {
            mem_iter: mem_neighbors.into_iter(),
            block_arc,
            block_iter_state,
            from_mem: true,
        }
    }
}

impl Iterator for GlobalNeighborIterator {
    type Item = VId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.from_mem {
            if let Some(vid) = self.mem_iter.next() {
                return Some(vid);
            }
            self.from_mem = false;
        }

        let Some(block) = &self.block_arc else {
            return None;
        };

        let Some((current, end, edge_offset)) = &mut self.block_iter_state else {
            return None;
        };

        if *current < *end {
            let offset = *edge_offset + *current * 4;
            let vid = u32::from_be_bytes([
                block.data[offset],
                block.data[offset + 1],
                block.data[offset + 2],
                block.data[offset + 3],
            ]);
            *current += 1;
            Some(vid)
        } else {
            None
        }
    }
}

/// Unified iterator that can iterate over neighbors from either:
/// - LSM-Community internal storage (GlobalNeighborIterator)
/// - External storage for giant vertices (Arc<VIdList>)
pub enum UnifiedNeighborIterator {
    /// Iterator for regular vertices stored in LSM-Community
    Internal(GlobalNeighborIterator),
    /// Iterator for giant vertices stored in external RocksDB
    External {
        neighbors: Arc<VIdList>,
        index: usize,
    },
}

impl UnifiedNeighborIterator {
    /// Create iterator from LSM-Community internal storage
    pub fn from_internal(iter: GlobalNeighborIterator) -> Self {
        Self::Internal(iter)
    }

    /// Create iterator from external giant vertex storage
    pub fn from_external(neighbors: Arc<VIdList>) -> Self {
        Self::External {
            neighbors,
            index: 0,
        }
    }
}

impl Iterator for UnifiedNeighborIterator {
    type Item = VId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Internal(iter) => iter.next(),
            Self::External { neighbors, index } => {
                if *index < neighbors.len() {
                    let vid = neighbors[*index];
                    *index += 1;
                    Some(vid)
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Internal(iter) => iter.size_hint(),
            Self::External { neighbors, index } => {
                let remaining = neighbors.len().saturating_sub(*index);
                (remaining, Some(remaining))
            }
        }
    }
}

// Optional: implement ExactSizeIterator for External variant
impl ExactSizeIterator for UnifiedNeighborIterator {
    fn len(&self) -> usize {
        match self {
            Self::Internal(_) => {
                // GlobalNeighborIterator doesn't know exact size
                0
            }
            Self::External { neighbors, index } => neighbors.len().saturating_sub(*index),
        }
    }
}
