use std::collections::BTreeMap;

use crate::types::graph_serialize::VertexId;

/// A trait that defines query operations for graph components.
///
/// This trait provides essential functionality for traversing and querying
/// graph data structures, allowing components to retrieve neighbor information,
/// verify the existence of vertices and edges, and obtain collections of vertices.
///
/// # Type Parameters
///
/// * `T` - The vertex identifier type, which must implement the `VertexId` trait.
/// * `V` - The vertex data type, representing the content or properties of vertices.
#[allow(dead_code)]
pub trait GraphQuery<T, V>
where
    T: VertexId,
{
    /// Retrieves all neighbors of a given vertex.
    ///
    /// This method returns a collection of neighboring vertices for the specified vertex.
    ///
    /// # Parameters
    ///
    /// * `vertex_id` - A reference to the identifier of the vertex whose neighbors are being queried.
    ///
    /// # Returns
    ///
    /// A vector containing the vertex data of all neighboring vertices.
    fn read_neighbor(&self, vertex_id: &T) -> Vec<V>;

    /// Checks if a specific vertex exists in the graph component.
    ///
    /// # Parameters
    ///
    /// * `vertex_id` - A reference to the identifier of the vertex to check.
    ///
    /// # Returns
    ///
    /// `true` if the vertex exists in this component, `false` otherwise.
    fn has_vertex(&self, vertex_id: &T) -> bool;

    /// Determines if an edge exists between two specified vertices.
    ///
    /// # Parameters
    ///
    /// * `src_id` - A reference to the identifier of the source vertex.
    /// * `dst_id` - A reference to the identifier of the destination vertex.
    ///
    /// # Returns
    ///
    /// `true` if an edge exists from the source vertex to the destination vertex,
    /// `false` otherwise.
    fn has_edge(&self, src_id: &T, dst_id: &T) -> bool;

    /// Retrieves a list of all vertices in this graph component.
    ///
    /// # Returns
    ///
    /// A vector containing the vertex data of all vertices in this component.
    fn vertex_list(&self) -> Vec<V>;

    /// Generates a complete representation of the graph component as a map.
    ///
    /// This method provides a comprehensive view of the graph structure, mapping
    /// each vertex identifier to a tuple containing its data and its neighbors' data.
    ///
    /// # Returns
    ///
    /// A `BTreeMap` where:
    /// - Keys are vertex identifiers.
    /// - Values are tuples containing:
    ///   - The vertex data of the key vertex.
    ///   - A vector of vertex data for all neighbors of the key vertex.
    fn all(&self) -> BTreeMap<T, (V, Vec<V>)>;
}
