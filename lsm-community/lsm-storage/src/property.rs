use crate::types::VId;

/// Key for vertex property storage
///
/// Encodes as: [vid: 4 bytes][name_len: 2 bytes][name: variable bytes]
/// Uses big-endian for vid to ensure lexicographic ordering by vertex ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VertexPropertyKey {
    pub vertex_id: VId,
    pub property_name: String,
}

impl VertexPropertyKey {
    /// Size overhead for encoding (excluding property name): 4 (vid) + 2 (name_len) = 6 bytes
    const PREFIX_SIZE: usize = 6;

    /// Create a new vertex property key
    #[inline]
    pub fn new(vertex_id: VId, property_name: String) -> Self {
        Self {
            vertex_id,
            property_name,
        }
    }

    /// Encode to bytes for RocksDB storage
    ///
    /// Format: [vid: 4][name_len: 2][name: variable]
    pub fn encode(&self) -> Vec<u8> {
        let name_bytes = self.property_name.as_bytes();
        let name_len = name_bytes.len() as u16;

        let mut bytes = Vec::with_capacity(Self::PREFIX_SIZE + name_bytes.len());
        bytes.extend_from_slice(&self.vertex_id.to_be_bytes());
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(name_bytes);

        bytes
    }

    /// Decode from bytes
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < Self::PREFIX_SIZE {
            anyhow::bail!("Invalid vertex property key length: {}", bytes.len());
        }

        let vertex_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let name_len = u16::from_be_bytes(bytes[4..6].try_into().unwrap()) as usize;

        if bytes.len() != Self::PREFIX_SIZE + name_len {
            anyhow::bail!(
                "Invalid vertex property key: expected length {}, got {}",
                Self::PREFIX_SIZE + name_len,
                bytes.len()
            );
        }

        let property_name = String::from_utf8(bytes[6..].to_vec())
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property name: {}", e))?;

        Ok(Self {
            vertex_id,
            property_name,
        })
    }

    /// Create a prefix for scanning all properties of a vertex
    ///
    /// Format: [vid: 4]
    #[inline]
    pub fn prefix(vertex_id: VId) -> [u8; 4] {
        vertex_id.to_be_bytes()
    }
}

/// Key for edge property storage
///
/// Encodes as: [src: 4 bytes][dst: 4 bytes][name_len: 2 bytes][name: variable bytes]
/// Uses big-endian for IDs to ensure lexicographic ordering by (src, dst).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgePropertyKey {
    pub source_id: VId,
    pub destination_id: VId,
    pub property_name: String,
}

impl EdgePropertyKey {
    /// Size overhead for encoding (excluding property name): 4 (src) + 4 (dst) + 2 (name_len) = 10 bytes
    const PREFIX_SIZE: usize = 10;

    /// Create a new edge property key
    #[inline]
    pub fn new(source_id: VId, destination_id: VId, property_name: String) -> Self {
        Self {
            source_id,
            destination_id,
            property_name,
        }
    }

    /// Encode to bytes for RocksDB storage
    ///
    /// Format: [src: 4][dst: 4][name_len: 2][name: variable]
    pub fn encode(&self) -> Vec<u8> {
        let name_bytes = self.property_name.as_bytes();
        let name_len = name_bytes.len() as u16;

        let mut bytes = Vec::with_capacity(Self::PREFIX_SIZE + name_bytes.len());
        bytes.extend_from_slice(&self.source_id.to_be_bytes());
        bytes.extend_from_slice(&self.destination_id.to_be_bytes());
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(name_bytes);

        bytes
    }

    /// Decode from bytes
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < Self::PREFIX_SIZE {
            anyhow::bail!("Invalid edge property key length: {}", bytes.len());
        }

        let source_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let destination_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        let name_len = u16::from_be_bytes(bytes[8..10].try_into().unwrap()) as usize;

        if bytes.len() != Self::PREFIX_SIZE + name_len {
            anyhow::bail!(
                "Invalid edge property key: expected length {}, got {}",
                Self::PREFIX_SIZE + name_len,
                bytes.len()
            );
        }

        let property_name = String::from_utf8(bytes[10..].to_vec())
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property name: {}", e))?;

        Ok(Self {
            source_id,
            destination_id,
            property_name,
        })
    }

    /// Create a prefix for scanning all properties of an edge
    ///
    /// Format: [src: 4][dst: 4]
    #[inline]
    pub fn prefix(source_id: VId, destination_id: VId) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(&source_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&destination_id.to_be_bytes());
        bytes
    }
}
