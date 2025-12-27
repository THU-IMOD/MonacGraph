use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

use crate::types::VId;

/// A single delta operation representing a change to a vertex's neighbor list.
///
/// Each operation is encoded as 16 bytes (128 bits) for memory alignment:
/// - timestamp: u64 (8 bytes) - Logical or physical timestamp for ordering
/// - neighbor: u32 (4 bytes) - The neighbor vertex ID being added or removed
/// - op_type: u32 (4 bytes) - Operation type (0=Add, 1=Remove, rest reserved)
///
/// The 128-bit alignment ensures efficient memory access and cache performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaOperation {
    /// Timestamp for ordering operations (monotonically increasing)
    pub timestamp: u64,
    /// The neighbor vertex ID involved in this operation
    pub neighbor: VId,
    /// Operation type: 0=AddNeighbor, 1=RemoveNeighbor, others reserved for future use
    pub op_type: u32,
}

/// Operation types for delta operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum DeltaOpType {
    /// Add a neighbor to the vertex's adjacency list
    AddNeighbor = 0,
    /// Remove a neighbor from the vertex's adjacency list
    RemoveNeighbor = 1,
}

impl From<DeltaOpType> for u32 {
    fn from(op_type: DeltaOpType) -> Self {
        op_type as u32
    }
}

impl DeltaOpType {
    /// Convert from u32 representation
    #[inline]
    pub fn from_u32(val: u32) -> Option<Self> {
        match val {
            0 => Some(Self::AddNeighbor),
            1 => Some(Self::RemoveNeighbor),
            _ => None,
        }
    }

    /// Convert to u32 representation
    #[inline]
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

impl DeltaOperation {
    /// Size of encoded delta operation in bytes (128 bits = 16 bytes)
    pub const ENCODED_SIZE: usize = 16;

    /// Create a new delta operation
    #[inline]
    pub fn new(timestamp: u64, op_type: DeltaOpType, neighbor: VId) -> Self {
        Self {
            timestamp,
            neighbor,
            op_type: op_type.as_u32(),
        }
    }

    /// Get the operation type
    #[inline]
    pub fn get_op_type(&self) -> Option<DeltaOpType> {
        DeltaOpType::from_u32(self.op_type)
    }

    /// Encode delta operation to 16 bytes
    ///
    /// Layout: [timestamp: 8 bytes][neighbor: 4 bytes][op_type: 4 bytes]
    /// Uses little-endian byte order for all fields.
    #[inline]
    pub fn encode(&self) -> [u8; Self::ENCODED_SIZE] {
        let mut bytes = [0u8; Self::ENCODED_SIZE];

        // Encode timestamp (bytes 0-7)
        bytes[0..8].copy_from_slice(&self.timestamp.to_le_bytes());

        // Encode neighbor (bytes 8-11)
        bytes[8..12].copy_from_slice(&self.neighbor.to_le_bytes());

        // Encode op_type (bytes 12-15)
        bytes[12..16].copy_from_slice(&self.op_type.to_le_bytes());

        bytes
    }

    /// Decode delta operation from 16 bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - Byte slice containing the encoded operation (must be exactly 16 bytes)
    ///
    /// # Returns
    ///
    /// Returns `Ok(DeltaOperation)` if decoding succeeds, or an error if the input is invalid.
    #[inline]
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != Self::ENCODED_SIZE {
            anyhow::bail!(
                "Invalid delta operation length: expected {}, got {}",
                Self::ENCODED_SIZE,
                bytes.len()
            );
        }

        // Decode timestamp (bytes 0-7)
        let timestamp =
            u64::from_le_bytes(bytes[0..8].try_into().expect("slice with incorrect length"));

        // Decode neighbor (bytes 8-11)
        let neighbor = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .expect("slice with incorrect length"),
        );

        // Decode op_type (bytes 12-15)
        let op_type = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .expect("slice with incorrect length"),
        );

        // Validate op_type
        if DeltaOpType::from_u32(op_type).is_none() {
            anyhow::bail!("Invalid operation type: {}", op_type);
        }

        Ok(Self {
            timestamp,
            neighbor,
            op_type,
        })
    }

    /// Encode a slice of delta operations into a byte vector
    ///
    /// This is useful for batch encoding operations before merge.
    pub fn encode_batch(ops: &[DeltaOperation]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ops.len() * Self::ENCODED_SIZE);
        for op in ops {
            bytes.extend_from_slice(&op.encode());
        }
        bytes
    }

    /// Decode a byte slice into a vector of delta operations
    ///
    /// # Arguments
    ///
    /// * `bytes` - Byte slice containing encoded operations (length must be multiple of 16)
    ///
    /// # Returns
    ///
    /// Returns a vector of decoded operations or an error if decoding fails.
    pub fn decode_batch(bytes: &[u8]) -> anyhow::Result<Vec<DeltaOperation>> {
        if bytes.len() % Self::ENCODED_SIZE != 0 {
            anyhow::bail!(
                "Invalid batch length: {} is not a multiple of {}",
                bytes.len(),
                Self::ENCODED_SIZE
            );
        }

        let count = bytes.len() / Self::ENCODED_SIZE;
        let mut ops = Vec::with_capacity(count);

        for chunk in bytes.chunks_exact(Self::ENCODED_SIZE) {
            ops.push(Self::decode(chunk)?);
        }

        Ok(ops)
    }
}

/// A log of delta operations for a single vertex.
///
/// DeltaLog stores a list of operations that modify a vertex's neighbor list.
/// Operations are maintained in sorted order by timestamp for efficient merging
/// and querying.
///
/// This structure is designed to be stored directly in RocksDB using the merge
/// operator, allowing efficient append-only writes without read-modify-write cycles.
#[derive(Debug, Clone, Default)]
pub struct DeltaLog {
    /// List of delta operations, sorted by timestamp in ascending order
    pub ops: Vec<DeltaOperation>,
}

impl DeltaLog {
    /// Create an empty delta log
    #[inline]
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Create a delta log from a vector of operations
    ///
    /// The operations will be sorted by timestamp automatically.
    pub fn from_ops(mut ops: Vec<DeltaOperation>) -> Self {
        ops.sort_unstable_by_key(|op| op.timestamp);
        Self { ops }
    }

    /// Get the number of operations in the log
    #[inline]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the log is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get a reference to the operations
    #[inline]
    pub fn ops(&self) -> &[DeltaOperation] {
        &self.ops
    }

    /// Add a single operation to the log
    ///
    /// The operation will be inserted in the correct position to maintain
    /// timestamp ordering.
    pub fn add_op(&mut self, op: DeltaOperation) {
        let pos = self
            .ops
            .binary_search_by_key(&op.timestamp, |o| o.timestamp)
            .unwrap_or_else(|e| e);
        self.ops.insert(pos, op);
    }

    /// Encode the delta log to bytes
    ///
    /// Format: [count: 4 bytes][op1: 16 bytes][op2: 16 bytes]...
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + self.ops.len() * DeltaOperation::ENCODED_SIZE);

        // Write operation count (4 bytes)
        bytes.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());

        // Write each operation (16 bytes each)
        for op in &self.ops {
            bytes.extend_from_slice(&op.encode());
        }

        bytes
    }

    /// Decode a delta log from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - Byte slice containing the encoded delta log
    ///
    /// # Returns
    ///
    /// Returns the decoded DeltaLog or an error if the format is invalid.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < 4 {
            anyhow::bail!("Delta log too short: need at least 4 bytes for count");
        }

        // Read operation count
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;

        // Validate total length
        let expected_len = 4 + count * DeltaOperation::ENCODED_SIZE;
        if bytes.len() != expected_len {
            anyhow::bail!(
                "Invalid delta log length: expected {}, got {}",
                expected_len,
                bytes.len()
            );
        }

        // Decode operations
        let ops = DeltaOperation::decode_batch(&bytes[4..])?;

        Ok(Self { ops })
    }

    /// Merge multiple delta logs into a single log.
    ///
    /// For operations on the same neighbor vertex with different timestamps,
    /// only the operation with the largest timestamp is kept. This implements
    /// a "last write wins" semantic.
    ///
    /// The resulting log is sorted by timestamp in ascending order.
    ///
    /// # Arguments
    ///
    /// * `logs` - Slice of delta logs to merge
    ///
    /// # Returns
    ///
    /// A new DeltaLog containing the merged operations.
    pub fn merge(logs: &[DeltaLog]) -> Self {
        if logs.is_empty() {
            return Self::new();
        }

        if logs.len() == 1 {
            return logs[0].clone();
        }

        // Use a hash map to track the latest operation for each neighbor
        // Key: neighbor VId, Value: (timestamp, operation)
        let mut latest_ops: FxHashMap<VId, DeltaOperation> = FxHashMap::default();

        // Process all operations from all logs
        for log in logs {
            for op in &log.ops {
                match latest_ops.entry(op.neighbor) {
                    Entry::Vacant(e) => {
                        e.insert(*op);
                    }
                    Entry::Occupied(mut e) => {
                        // Keep the operation with the larger timestamp
                        if op.timestamp > e.get().timestamp {
                            e.insert(*op);
                        }
                    }
                }
            }
        }

        // Collect and sort by timestamp
        let mut ops: Vec<DeltaOperation> = latest_ops.into_values().collect();
        ops.sort_unstable_by_key(|op| op.timestamp);

        Self { ops }
    }

    /// Merge this log with another log in place.
    ///
    /// This is more efficient than creating a new log when you want to
    /// accumulate operations into an existing log.
    pub fn merge_with(&mut self, other: &DeltaLog) {
        let merged = Self::merge(&[self.clone(), other.clone()]);
        self.ops = merged.ops;
    }

    /// Merge operation for RocksDB merge operator (for base value + operands).
    ///
    /// This is used in the full merge callback where we need to merge a base
    /// value (existing DeltaLog) with multiple operands (raw operation bytes).
    ///
    /// # Arguments
    ///
    /// * `base` - Optional base value (encoded DeltaLog)
    /// * `operands` - Slice of operand byte slices (each containing encoded operations)
    ///
    /// # Returns
    ///
    /// Encoded bytes of the merged DeltaLog, or None if merge fails.
    pub fn merge_for_rocksdb(base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>> {
        // Decode base log if it exists
        let mut logs = Vec::with_capacity(1 + operands.len());

        if let Some(base_bytes) = base {
            match Self::decode(base_bytes) {
                Ok(base_log) => logs.push(base_log),
                Err(_) => return None,
            }
        }

        // Decode each operand (each operand contains raw operations)
        for operand in operands {
            // Each operand should be a multiple of 16 bytes (DeltaOperation::ENCODED_SIZE)
            if operand.len() % DeltaOperation::ENCODED_SIZE != 0 {
                return None;
            }

            match DeltaOperation::decode_batch(operand) {
                Ok(ops) => logs.push(Self::from_ops(ops)),
                Err(_) => return None,
            }
        }

        // Merge all logs
        let merged = Self::merge(&logs);
        Some(merged.encode())
    }

    /// Partial merge operation for RocksDB merge operator (operands only).
    ///
    /// This is used in the partial merge callback where we only merge operands
    /// without the base value. The result is simply concatenated raw operations.
    ///
    /// # Arguments
    ///
    /// * `operands` - Slice of operand byte slices (each containing encoded operations)
    ///
    /// # Returns
    ///
    /// Concatenated raw operation bytes, or None if merge fails.
    pub fn partial_merge_for_rocksdb(operands: &[&[u8]]) -> Option<Vec<u8>> {
        // Validate all operands
        for operand in operands {
            if operand.len() % DeltaOperation::ENCODED_SIZE != 0 {
                return None;
            }
        }

        // Simply concatenate all operands
        let total_size: usize = operands.iter().map(|op| op.len()).sum();
        let mut result = Vec::with_capacity(total_size);

        for operand in operands {
            result.extend_from_slice(operand);
        }

        Some(result)
    }
}
