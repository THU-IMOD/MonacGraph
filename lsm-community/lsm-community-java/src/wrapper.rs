use std::{sync::Arc, vec};

use lsm_storage::{
    LsmCommunity, LsmCommunityStorageOptions,
    types::{EdgeList, VId},
};

use crate::mapper::{EdgeIdMapper, VertexIdMapper};

/// LSM community wrapper for graph operations
#[allow(dead_code)]
pub struct LsmCommunityWrapper {
    /// The LSM-Community instance
    pub lsm_community: Arc<LsmCommunity>,

    /// The vertex id mapper for mapping external vertex IDs to internal IDs
    pub vertex_id_mapper: Arc<VertexIdMapper>,

    // The edge id mapper for mapping external edge IDs to internal edge handles
    pub edge_id_mapper: Arc<EdgeIdMapper>,
}

impl LsmCommunityWrapper {
    /// Open an existing LSM-Community graph
    pub fn open(graph_name: &str) -> anyhow::Result<Self> {
        let mut options = LsmCommunityStorageOptions::default();
        options.graph_name = graph_name.to_owned();

        // Construct log paths for vertex and edge ID mappers
        let vertex_log_path = format!(
            "./{}/{}/vertex-id-mapping.log",
            options.work_space_dir, options.graph_name
        );
        let edge_log_path = format!(
            "./{}/{}/edge-id-mapping.log",
            options.work_space_dir, options.graph_name
        );

        // Open LSM-Community and ID mappers
        let lsm_community = LsmCommunity::open(options)?;
        let vertex_id_mapper = Arc::new(VertexIdMapper::new(vertex_log_path)?);
        let edge_id_mapper = Arc::new(EdgeIdMapper::new(edge_log_path)?);

        Ok(Self {
            lsm_community,
            vertex_id_mapper,
            edge_id_mapper,
        })
    }

    /// Get all vertex (Inner) IDs in the graph
    pub fn get_all_vertices(&self) -> Vec<VId> {
        self.lsm_community.get_all_vertex_id()
    }

    /// Get all edges in the graph
    pub fn get_all_edges(&self) -> EdgeList {
        self.lsm_community.read_all_edges().unwrap()
    }

    /// Add a new vertex to the graph
    pub fn new_vertex(&self, new_outer_id: &[u8], vertex_property: &[u8]) -> anyhow::Result<VId> {
        // Step 1 - Generate the new inner vertex ID
        let new_inner_id = self.lsm_community.insert_vertex()?;
        // Step 2 - Map the new outer vertex ID to the new inner vertex ID
        self.vertex_id_mapper.insert(new_outer_id, new_inner_id)?;
        // Step 3 - Put the vertex property into the LSM-Community
        self.lsm_community
            .put_vertex_property(new_inner_id, vertex_property)?;
        Ok(new_inner_id)
    }

    /// Get vertex property by internal vertex ID
    /// Returns empty Vec if property doesn't exist
    pub fn get_vertex_property(&self, vertex_id: VId) -> anyhow::Result<Vec<u8>> {
        self.lsm_community
            .get_vertex_property(vertex_id)
            .map(|opt| opt.unwrap_or_else(|| Vec::new()))
    }

    /// Put vertex property by internal vertex ID
    pub fn put_vertex_property(&self, vertex_id: VId, property: &[u8]) -> anyhow::Result<()> {
        self.lsm_community.put_vertex_property(vertex_id, property)
    }

    /// Read neighbors of a vertex in a specific direction
    pub fn get_neighbor(&self, vertex_id: VId, direction: u16) -> anyhow::Result<Vec<VId>> {
        if direction == 0 {
            self.lsm_community.read_out_neighbor_clone(vertex_id)
        } else {
            self.lsm_community.read_in_neighbor_clone(vertex_id)
        }
    }

    /// Create a new edge;
    pub fn new_edge(
        &self,
        outer_id: &[u8],
        src: VId,
        dst: VId,
        edge_property: &[u8],
    ) -> anyhow::Result<()> {
        // Step 1 - Map the new outer edge ID to the new inner edge handle
        self.edge_id_mapper
            .insert(outer_id, EdgeIdMapper::pack_edge_handle(src, dst))?;
        // Step 2 - Put the edge property into the LSM-Community
        self.lsm_community
            .put_edge_property(src, dst, edge_property)?;
        // Step 3 - Insert the edge into the LSM-Community
        self.lsm_community.insert_edge(src, dst)?;
        Ok(())
    }

    /// Get edge property by internal vertex IDs
    pub fn get_edge_property(&self, src: VId, dst: VId) -> anyhow::Result<Vec<u8>> {
        self.lsm_community
            .get_edge_property(src, dst)
            .map(|opt| opt.unwrap_or_else(|| Vec::new()))
    }

    /// Remove an edge from the graph
    pub fn remove_edge(&self, src: VId, dst: VId) -> anyhow::Result<()> {
        // Lookup the outer edge ID and remove it from the edge id mapper
        let outer_edge_id_opt = self
            .edge_id_mapper
            .get_outer_id(EdgeIdMapper::pack_edge_handle(src, dst));
        if let Some(outer_edge_id) = outer_edge_id_opt {
            // Step 1 - Remove the edge from edge id mapper
            self.edge_id_mapper.remove(&outer_edge_id)?;
            // Step 2 - Remove the edge from the LSM-Community
            self.lsm_community.remove_edge(src, dst)?;
            // Step 3 - Remove the edge property from the LSM-Community
            self.lsm_community.put_edge_property(src, dst, &vec![])?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Create example graph for testing with properties
    ///
    /// Topology: 13 vertices, 20 edges (from example.graph)
    /// Vertex properties: {"id": vid, "name": "user:{vid}"}
    /// Edge properties: {"timestamp": current_timestamp_micros}
    pub fn create_example_for_test(graph_name: &str) -> anyhow::Result<Self> {
        use serde_json::json;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Open the wrapper (this will load the graph structure from example.graph)
        let wrapper = Self::open(graph_name)?;

        // Get current timestamp in microseconds
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        println!("Creating example graph with properties...");

        // Step 1: Add vertex mappings and properties
        println!("Adding vertex mappings and properties...");
        for vid in 0..13u32 {
            let outer_id = format!("user:{}", vid);

            // Create vertex property JSON
            let vertex_property = json!({
                "id": vid,
                "name": outer_id.clone()
            });
            let property_bytes = vertex_property.to_string().into_bytes();

            // Map outer ID to inner ID
            wrapper.vertex_id_mapper.insert(outer_id.as_bytes(), vid)?;

            // Put vertex property
            wrapper.put_vertex_property(vid, &property_bytes)?;

            println!("  Vertex {}: id={}, name={}", vid, vid, outer_id);
        }

        // Step 2: Add edge mappings and properties
        println!("\nAdding edge mappings and properties...");

        // Edge list from the example graph
        let edges = vec![
            (0, 2),
            (1, 0),
            (1, 2),
            (1, 3),
            (2, 3),
            (3, 0),
            (3, 4),
            (3, 11),
            (4, 6),
            (4, 7),
            (5, 4),
            (6, 5),
            (7, 3),
            (7, 8),
            (7, 9),
            (8, 9),
            (8, 10),
            (10, 7),
            (10, 9),
            (11, 12),
        ];

        for (src, dst) in edges {
            let outer_edge_id = format!("edge:{}:{}", src, dst);
            let edge_handle = EdgeIdMapper::pack_edge_handle(src, dst);

            // Create edge property JSON with timestamp
            let edge_property = json!({
                "timestamp": timestamp
            });
            let property_bytes = edge_property.to_string().into_bytes();

            // Map outer edge ID to edge handle
            wrapper
                .edge_id_mapper
                .insert(outer_edge_id.as_bytes(), edge_handle)?;

            // Put edge property (edge structure already exists from example.graph)
            wrapper
                .lsm_community
                .put_edge_property(src, dst, &property_bytes)?;

            println!("  Edge {} -> {}: timestamp={}", src, dst, timestamp);
        }

        println!("\n✓ Example graph created successfully!");
        println!("  Vertices: 13");
        println!("  Edges: 20");

        Ok(wrapper)
    }
}
