use std::hash::Hash;

/// Defines a trait for encoding data structures to bytes.
/// Implementors of this trait can convert themselves into a byte representation.
#[allow(dead_code)]
pub trait TopologyEncode {
    /// Encodes the implementing type to a vector of bytes.
    ///
    /// # Returns
    /// A vector of bytes representing the encoded data structure.
    fn encode_topology(&self) -> Vec<u8>;
}

/// Defines a trait for decoding data structures from bytes.
/// This trait enables reconstruction of objects from their byte representation.
#[allow(dead_code)]
pub trait TopologyDecode: Sized {
    /// Creates an instance of the implementing type from a byte slice.
    ///
    /// # Parameters
    /// * `bytes` - The byte slice containing the encoded data.
    ///
    /// # Returns
    /// `Some(Self)` if decoding was successful, `None` otherwise.
    fn from_bytes_topology(bytes: &[u8]) -> Option<Self>;
}

/// Defines a generic trait for types that can be encoded to and decoded from bytes.
/// This trait provides a unified interface for byte serialization and deserialization.
pub trait ByteEncodable: Sized {
    /// Converts the implementing type to a vector of bytes.
    ///
    /// # Returns
    /// A vector of bytes representing the encoded value.
    fn to_bytes(&self) -> Vec<u8>;

    /// Creates an instance of the implementing type from a byte slice.
    ///
    /// # Parameters
    /// * `bytes` - The byte slice containing the encoded value.
    ///
    /// # Returns
    /// `Some(Self)` if decoding was successful, `None` otherwise.
    fn from_bytes(bytes: &[u8]) -> Option<Self>;

    /// Returns the number of bytes required to encode this type.
    ///
    /// # Returns
    /// The size in bytes of the encoded representation.
    fn byte_size() -> usize;
}

/// Implementation of ByteEncodable for u16.
/// Provides methods to convert u16 values to and from byte arrays.
impl ByteEncodable for u16 {
    /// Converts an u16 value to a vector of bytes in little-endian format.
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    /// Creates an u16 value from a byte slice in little-endian format.
    ///
    /// # Parameters
    /// * `bytes` - The byte slice containing at least 2 bytes.
    ///
    /// # Returns
    /// `Some(u16)` if the slice contains enough bytes, `None` otherwise.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        let mut array = [0u8; 2];
        array.copy_from_slice(&bytes[0..2]);
        Some(u16::from_le_bytes(array))
    }

    /// Returns the size in bytes of an u16 value.
    fn byte_size() -> usize {
        2
    }
}

/// Implementation of ByteEncodable for u32.
/// Provides methods to convert u32 values to and from byte arrays.
impl ByteEncodable for u32 {
    /// Converts an u32 value to a vector of bytes in little-endian format.
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    /// Creates an u32 value from a byte slice in little-endian format.
    ///
    /// # Parameters
    /// * `bytes` - The byte slice containing at least 4 bytes.
    ///
    /// # Returns
    /// `Some(u32)` if the slice contains enough bytes, `None` otherwise.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let mut array = [0u8; 4];
        array.copy_from_slice(&bytes[0..4]);
        Some(u32::from_le_bytes(array))
    }

    /// Returns the size in bytes of an u32 value.
    fn byte_size() -> usize {
        4
    }
}

/// Implementation of ByteEncodable for u64.
/// Provides methods to convert u64 values to and from byte arrays.
impl ByteEncodable for u64 {
    /// Converts an u64 value to a vector of bytes in little-endian format.
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    /// Creates an u64 value from a byte slice in little-endian format.
    ///
    /// # Parameters
    /// * `bytes` - The byte slice containing at least 8 bytes.
    ///
    /// # Returns
    /// `Some(u64)` if the slice contains enough bytes, `None` otherwise.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let mut array = [0u8; 8];
        array.copy_from_slice(&bytes[0..8]);
        Some(u64::from_le_bytes(array))
    }

    /// Returns the size in bytes of an u64 value.
    fn byte_size() -> usize {
        8
    }
}

/// A trait for types that can be used as vertex identifiers in a graph.
/// This trait combines several requirements to ensure proper behavior in graph algorithms.
pub trait VertexId:
    ByteEncodable + Copy + Ord + PartialOrd + Eq + Hash + TryInto<usize> + TryFrom<usize>
{
}

/// Blanket implementation for all types that satisfy the VertexId trait bounds.
/// This enables any type that meets the requirements to be used as a vertex identifier.
impl<T> VertexId for T where
    T: ByteEncodable + Copy + Ord + PartialOrd + Eq + Hash + TryInto<usize> + TryFrom<usize>
{
}

/// A trait for types that can represent the length or weight of a vertex.
/// Provides a consistent interface for working with different numeric types.
#[allow(dead_code)]
pub trait VertexLength:
    ByteEncodable + Copy + Ord + PartialOrd + TryInto<usize> + TryFrom<usize>
{
}

/// Blanket implementation for all types that satisfy the VertexLength trait bounds.
/// This allows flexibility in choosing different numeric types for vertex lengths.
impl<L> VertexLength for L where
    L: ByteEncodable + Copy + Ord + PartialOrd + TryInto<usize> + TryFrom<usize>
{
}

/// A trait for types that can be used as offsets in data structures.
/// Typically used for indexing or addressing within memory structures.
#[allow(dead_code)]
pub trait Offset:
    ByteEncodable + Copy + Ord + PartialOrd + TryInto<usize> + TryFrom<usize>
{
}

/// Blanket implementation for all types that satisfy the Offset trait bounds.
/// This provides flexibility in choosing the appropriate numeric type for offsets.
impl<O> Offset for O where
    O: ByteEncodable + Copy + Ord + PartialOrd + TryInto<usize> + TryFrom<usize>
{
}
