use super::Block;
use crate::types::{Offset, VId};

/// Iterator over vertices in the block (scans raw bytes)
pub struct VertexIterator<'a> {
    data: &'a [u8],
    current_offset: usize,
    end_offset: usize,
    current_index: usize,
    total_count: usize,
}

#[allow(dead_code)]
impl<'a> VertexIterator<'a> {
    pub fn new(block: &'a Block) -> Self {
        VertexIterator {
            data: &block.data,
            current_offset: block.vertex_list_view.offset,
            end_offset: block.vertex_list_view.offset + block.vertex_list_view.len,
            current_index: 0,
            total_count: block.vertex_count as usize,
        }
    }
}

impl<'a> Iterator for VertexIterator<'a> {
    type Item = (VId, Offset);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.end_offset || self.current_index >= self.total_count {
            return None;
        }

        // Parse VId (4 bytes, big-endian)
        let vid = u32::from_be_bytes([
            self.data[self.current_offset],
            self.data[self.current_offset + 1],
            self.data[self.current_offset + 2],
            self.data[self.current_offset + 3],
        ]);

        // Parse Offset (4 bytes, big-endian)
        let offset = u32::from_be_bytes([
            self.data[self.current_offset + 4],
            self.data[self.current_offset + 5],
            self.data[self.current_offset + 6],
            self.data[self.current_offset + 7],
        ]);

        self.current_offset += 8;
        self.current_index += 1;

        Some((vid, offset))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.total_count - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for VertexIterator<'a> {}

/// Iterator over edges in the block (scans raw bytes)
#[allow(dead_code)]
pub struct VIdIterator<'a> {
    data: &'a [u8],
    current_offset: usize,
    end_offset: usize,
    current_index: usize,
    total_count: usize,
}

#[allow(dead_code)]
impl<'a> VIdIterator<'a> {
    pub fn new(block: &'a Block) -> Self {
        VIdIterator {
            data: &block.data,
            current_offset: block.edge_list_view.offset,
            end_offset: block.edge_list_view.offset + block.edge_list_view.len,
            current_index: 0,
            total_count: block.edge_count as usize,
        }
    }
}

#[allow(dead_code)]
impl<'a> Iterator for VIdIterator<'a> {
    type Item = VId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.end_offset || self.current_index >= self.total_count {
            return None;
        }

        // Parse VId (4 bytes, big-endian)
        let vid = u32::from_be_bytes([
            self.data[self.current_offset],
            self.data[self.current_offset + 1],
            self.data[self.current_offset + 2],
            self.data[self.current_offset + 3],
        ]);

        self.current_offset += 4;
        self.current_index += 1;

        Some(vid)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.total_count - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for VIdIterator<'a> {}

/// Iterator over neighbors of a specific vertex (scans raw bytes in CSR format)
#[allow(dead_code)]
pub struct NeighborIterator<'a> {
    data: &'a [u8],
    edge_list_offset: usize,
    current_index: usize,
    end_index: usize,
}

#[allow(dead_code)]
impl<'a> NeighborIterator<'a> {
    pub fn new(block: &'a Block, vertex_index: usize) -> Option<Self> {
        if vertex_index >= block.vertex_count as usize {
            return None;
        }

        // Get start offset for this vertex
        let vertex_offset = block.vertex_list_view.offset + vertex_index * 8;
        let start_index = u32::from_be_bytes([
            block.data[vertex_offset + 4],
            block.data[vertex_offset + 5],
            block.data[vertex_offset + 6],
            block.data[vertex_offset + 7],
        ]) as usize;

        // Get end offset (from next vertex or total edge count)
        let end_index = if vertex_index + 1 < block.vertex_count as usize {
            let next_vertex_offset = vertex_offset + 8;
            u32::from_be_bytes([
                block.data[next_vertex_offset + 4],
                block.data[next_vertex_offset + 5],
                block.data[next_vertex_offset + 6],
                block.data[next_vertex_offset + 7],
            ]) as usize
        } else {
            block.edge_count as usize
        };

        Some(NeighborIterator {
            data: &block.data,
            edge_list_offset: block.edge_list_view.offset,
            current_index: start_index,
            end_index,
        })
    }
}

impl<'a> Iterator for NeighborIterator<'a> {
    type Item = VId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.end_index {
            return None;
        }

        // Calculate offset in edge list
        let offset = self.edge_list_offset + self.current_index * 4;

        // Parse VId (4 bytes, big-endian)
        let vid = u32::from_be_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ]);

        self.current_index += 1;

        Some(vid)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end_index - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for NeighborIterator<'a> {}

/// Iterator over all edges in the block (scans raw bytes in CSR format)
/// Returns (source_vid, destination_vid) pairs
pub struct BlockEdgeIterator<'a> {
    block: &'a Block,
    current_vertex_index: usize,
    neighbor_iter: Option<NeighborIterator<'a>>,
    current_vertex_id: VId,
}

impl<'a> BlockEdgeIterator<'a> {
    pub fn new(block: &'a Block) -> Self {
        let mut iterator = BlockEdgeIterator {
            block,
            current_vertex_index: 0,
            neighbor_iter: None,
            current_vertex_id: 0,
        };

        // Initialize with first vertex if exists
        if block.vertex_count > 0 {
            iterator.advance_to_next_vertex();
        }

        iterator
    }

    /// Get vertex ID at given index in vertex list
    fn get_vertex_id_at_index(&self, index: usize) -> VId {
        let vertex_offset = self.block.vertex_list_view.offset + index * 8;
        u32::from_be_bytes([
            self.block.data[vertex_offset],
            self.block.data[vertex_offset + 1],
            self.block.data[vertex_offset + 2],
            self.block.data[vertex_offset + 3],
        ])
    }

    /// Advance to the next vertex that has neighbors
    fn advance_to_next_vertex(&mut self) {
        while self.current_vertex_index < self.block.vertex_count as usize {
            // Get the vertex ID from vertex list
            let vid = self.get_vertex_id_at_index(self.current_vertex_index);

            // Create neighbor iterator for this vertex
            if let Some(neighbor_iter) =
                NeighborIterator::new(self.block, self.current_vertex_index)
            {
                // Check if this vertex has any neighbors
                if neighbor_iter.size_hint().0 > 0 {
                    self.current_vertex_id = vid;
                    self.neighbor_iter = Some(neighbor_iter);
                    return;
                }
            }

            // No neighbors for this vertex, move to next
            self.current_vertex_index += 1;
        }

        // No more vertices with neighbors
        self.neighbor_iter = None;
    }
}

impl<'a> Iterator for BlockEdgeIterator<'a> {
    type Item = (VId, VId);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next neighbor from current vertex
            if let Some(ref mut neighbor_iter) = self.neighbor_iter {
                if let Some(dst_vid) = neighbor_iter.next() {
                    return Some((self.current_vertex_id, dst_vid));
                }
            } else {
                // No valid neighbor iterator, we're done
                return None;
            }

            // Current vertex exhausted, move to next vertex
            self.current_vertex_index += 1;

            if self.current_vertex_index >= self.block.vertex_count as usize {
                // No more vertices
                return None;
            }

            self.advance_to_next_vertex();

            // If no more vertices with neighbors
            if self.neighbor_iter.is_none() {
                return None;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // The total number of edges is exactly edge_count
        let total_edges = self.block.edge_count as usize;

        // Calculate how many edges we've already consumed
        let mut consumed = 0;
        for i in 0..self.current_vertex_index {
            if let Some(iter) = NeighborIterator::new(self.block, i) {
                consumed += iter.size_hint().0;
            }
        }

        // Add edges consumed from current vertex's neighbor iterator
        if let Some(ref neighbor_iter) = self.neighbor_iter {
            // Get total neighbors for current vertex
            let current_vertex_total =
                if let Some(iter) = NeighborIterator::new(self.block, self.current_vertex_index) {
                    iter.size_hint().0
                } else {
                    0
                };
            let current_remaining = neighbor_iter.size_hint().0;
            consumed += current_vertex_total - current_remaining;
        }

        let remaining = total_edges.saturating_sub(consumed);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for BlockEdgeIterator<'a> {}
