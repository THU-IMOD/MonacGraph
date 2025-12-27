// Vertex ID, swith to u64 if the graph is too large;
pub type VId = u32;

// Offset type, represent the offset of vertex neighbor;
pub type Offset = u32;

// Vertex list type;
pub type VIdList = Vec<VId>;

// Edge list type;
pub type EdgeList = Vec<(VId, VId)>;

// Community ID, usually enough for u32;
pub type CommId = u32;

// Virtual community ID, used for locate buckets and memgrpahs.
pub type VirtualCommId = u16;

// Vertex entry in block;
pub type VertexEntry = (VId, Offset);

// Vertex list in block;
pub type VertexList = Vec<VertexEntry>;

// Page Id type;
pub type PageId = u32;

/// View of vertex list view;
#[derive(Debug, Clone)]
pub struct VertexListView {
    /// Offset in data where vertex list starts
    pub offset: usize,
    /// Length in bytes (vertex_count * 8)
    pub len: usize,
}

/// View into the edge list portion of the block data
#[derive(Debug, Clone)]
pub struct VIdListView {
    /// Offset in data where edge list starts
    pub offset: usize,
    /// Length in bytes (edge_count * 4)
    pub len: usize,
}
