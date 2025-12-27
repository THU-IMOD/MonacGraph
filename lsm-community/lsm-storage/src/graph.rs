use crate::types::{CommId, VId, VIdList};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::vec;

#[cfg(all(not(debug_assertions), not(test)))]
use indicatif::{ProgressBar, ProgressStyle};

/// A graph structure stored in Compressed Sparse Row (CSR) format in memory.
///
/// The CSR format is a space-efficient representation for sparse graphs.
/// It uses two main arrays:
/// - `offsets`: Stores the starting position of each vertex's neighbors in the `neighbors` array
/// - `neighbors`: Stores all neighbors in a contiguous array
///
/// # Example Structure
///
/// For a graph with edges: 0->1, 0->2, 1->2, 2->0
/// - `offsets`: [0, 2, 3, 4]
/// - `neighbors`: [1, 2, 2, 0]
///
/// Vertex 0's neighbors are at `neighbors[offsets[0]..offsets[1]]` = [1, 2]
/// Vertex 1's neighbors are at `neighbors[offsets[1]..offsets[2]]` = [2]
/// Vertex 2's neighbors are at `neighbors[offsets[2]..offsets[3]]` = [0]
#[derive(Debug, Clone)]
pub struct CsrGraph {
    /// Total number of vertices in the graph
    num_vertices: usize,

    /// Total number of edges in the graph
    num_edges: usize,

    /// Offset array that stores the starting index of each vertex's adjacency list.
    /// The length is `num_vertices + 1`, where the last element marks the end of
    /// the neighbor list.
    ///
    /// For vertex `v`, its neighbors are stored in `neighbors[offsets[v]..offsets[v+1]]`
    offsets: Vec<usize>,

    /// Contiguous array storing all neighbors for all vertices.
    /// The neighbors of vertex `v` are located at indices `[offsets[v], offsets[v+1))`
    neighbors: VIdList,

    /// Community assignment for each vertex.
    /// `communities[v]` represents the community ID that vertex `v` belongs to.
    /// The length is `num_vertices`.
    pub community_map: Vec<CommId>,

    /// The max community Id.
    max_comm_id: CommId,

    /// Community structure which can be generated from the community map.
    pub community_list: Option<Vec<Vec<VId>>>,
}

impl CsrGraph {
    /// Creates a new empty CSR graph with zero vertices and edges.
    ///
    /// # Returns
    ///
    /// An empty `CsrGraph` instance with all arrays initialized to empty vectors.
    pub fn new() -> Self {
        Self {
            num_vertices: 0,
            num_edges: 0,
            offsets: Vec::new(),
            neighbors: Vec::new(),
            community_map: Vec::new(),
            max_comm_id: 0,
            community_list: None,
        }
    }

    /// Compute the community structure list from community_map.
    ///
    /// This function groups vertices by their community IDs and stores the result
    /// in `community_list`. Since community IDs are contiguous from 0 to max_comm_id,
    /// we can directly allocate a vector of the appropriate size.
    pub fn compute_community_list(&mut self) {
        // Initialize community_list with empty vectors for each community
        let mut communities: Vec<Vec<VId>> = vec![Vec::new(); (self.max_comm_id + 1) as usize];

        // Group vertices by their community ID
        for (vertex_id, &comm_id) in self.community_map.iter().enumerate() {
            communities[comm_id as usize].push(vertex_id as VId);
        }

        self.community_list = Some(communities);
    }

    /// Returns the total number of vertices in the graph.
    ///
    /// # Returns
    ///
    /// The number of vertices as a `usize`.
    #[inline]
    pub fn num_vertices(&self) -> usize {
        self.num_vertices
    }

    /// Returns the total number of edges in the graph.
    ///
    /// # Returns
    ///
    /// The number of edges as a `usize`.
    #[inline]
    pub fn num_edges(&self) -> usize {
        self.num_edges
    }

    /// Returns a reference to the offset array.
    ///
    /// # Returns
    ///
    /// A slice containing the offset values for all vertices.
    #[inline]
    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    /// Returns a reference to the neighbor list array.
    ///
    /// # Returns
    ///
    /// A slice containing all neighbor vertex IDs.
    #[inline]
    pub fn neighbors(&self) -> &[VId] {
        &self.neighbors
    }

    /// Returns a reference to the community assignment array.
    ///
    /// # Returns
    ///
    /// A slice containing the community ID for each vertex.
    #[inline]
    pub fn communities(&self) -> &[CommId] {
        &self.community_map
    }

    /// Loads a graph from a file in the specified format.
    ///
    /// # File Format
    ///
    /// The file should follow this format:
    /// - First line: `t <num_vertices> <num_edges>`
    /// - Vertex lines: `v <vertex_id> <label> <community_id>`
    /// - Edge lines: `e <source> <target>`
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the graph file
    ///
    /// # Returns
    ///
    /// * `Ok(CsrGraph)` - Successfully loaded graph
    /// * `Err(std::io::Error)` - File reading or parsing error
    ///
    /// # Performance Notes
    ///
    /// - Uses buffered I/O with 8MB buffer for large files
    /// - Pre-allocates vectors based on known sizes to avoid reallocation
    /// - Builds adjacency list in-place without intermediate copies
    pub fn from_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        // Use 8MB buffer for large file I/O
        println!("Loading Graph From File: {:?}", path.as_ref());
        const BUFFER_SIZE: usize = 8 * 1024 * 1024;
        let file = File::open(path.as_ref())?;
        // Get file size for progress bar

        let reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut lines = reader.lines();

        // Parse the first line to get graph metadata
        let first_line = lines
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Empty file"))??;

        let (num_vertices, num_edges) = Self::parse_metadata(&first_line)?;

        // Initialize progress bar only in release mode and non-test environment
        #[cfg(all(not(debug_assertions), not(test)))]
        let progress_bar = {
            let pb = ProgressBar::new(num_vertices as u64 + num_edges as u64 + 1);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_message("Loading graph data");
            pb
        };

        // Pre-allocate vectors with known capacity to avoid reallocation
        let mut communities = vec![0; num_vertices];
        let mut edge_lists: Vec<Vec<VId>> = vec![Vec::new(); num_vertices];

        // Reserve approximate capacity for each vertex's adjacency list
        // Assuming uniform degree distribution
        let avg_degree = if num_vertices > 0 {
            (num_edges / num_vertices) + 1
        } else {
            0
        };
        for list in edge_lists.iter_mut() {
            list.reserve(avg_degree);
        }

        // Parse vertices and edges
        let mut max_comm_id = 0u32;
        for line in lines {
            let line = line?;
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue;
            }

            match trimmed.chars().next() {
                Some('v') => {
                    Self::parse_vertex(&trimmed, &mut communities, &mut max_comm_id)?;

                    // Update progress bar for vertex parsing
                    #[cfg(all(not(debug_assertions), not(test)))]
                    progress_bar.inc(1);
                }
                Some('e') => {
                    Self::parse_edge(&trimmed, &mut edge_lists)?;

                    // Update progress bar for edge parsing
                    #[cfg(all(not(debug_assertions), not(test)))]
                    progress_bar.inc(1);
                }
                Some('t') => {
                    // Skip additional metadata lines
                    continue;
                }
                _ => {
                    // Skip unknown line types
                    continue;
                }
            }
        }

        // Finish progress bar
        #[cfg(all(not(debug_assertions), not(test)))]
        {
            progress_bar.finish_with_message("Graph loaded successfully");
        }

        // Convert edge lists to CSR format (zero-copy construction)
        Self::build_csr(
            num_vertices,
            num_edges,
            edge_lists,
            communities,
            max_comm_id,
        )
    }

    /// Parses the metadata line to extract vertex and edge counts.
    ///
    /// # Arguments
    ///
    /// * `line` - The metadata line (format: "t <num_vertices> <num_edges>")
    ///
    /// # Returns
    ///
    /// * `Ok((num_vertices, num_edges))` - Parsed counts
    /// * `Err(std::io::Error)` - Parsing error
    #[inline]
    fn parse_metadata(line: &str) -> std::io::Result<(usize, usize)> {
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() < 3 || parts[0] != "t" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid metadata line format",
            ));
        }

        let num_vertices = parts[1].parse::<usize>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid vertex count")
        })?;

        let num_edges = parts[2].parse::<usize>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid edge count")
        })?;

        Ok((num_vertices, num_edges))
    }

    /// Parses a vertex line and updates the community assignment.
    ///
    /// # Arguments
    ///
    /// * `line` - The vertex line (format: "v <vertex_id> <label> <community_id>")
    /// * `communities` - Mutable reference to community assignment vector
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Successfully parsed
    /// * `Err(std::io::Error)` - Parsing error
    #[inline]
    fn parse_vertex(
        line: &str,
        communities: &mut [CommId],
        max_comm_id: &mut CommId,
    ) -> std::io::Result<()> {
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() < 4 || parts[0] != "v" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid vertex line format: {}", line),
            ));
        }

        let vertex_id = parts[1].parse::<usize>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid vertex ID")
        })?;

        let community_id = parts[3].parse::<CommId>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid community ID")
        })?;

        *max_comm_id = (*max_comm_id).max(community_id);

        if vertex_id >= communities.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Vertex ID {} out of range", vertex_id),
            ));
        }

        communities[vertex_id] = community_id;
        Ok(())
    }

    /// Parses an edge line and adds it to the adjacency list.
    ///
    /// # Arguments
    ///
    /// * `line` - The edge line (format: "e <source> <target>")
    /// * `edge_lists` - Mutable reference to adjacency lists
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Successfully parsed
    /// * `Err(std::io::Error)` - Parsing error
    #[inline]
    fn parse_edge(line: &str, edge_lists: &mut [Vec<VId>]) -> std::io::Result<()> {
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() < 3 || parts[0] != "e" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid edge line format: {}", line),
            ));
        }

        let source = parts[1].parse::<usize>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid source vertex")
        })?;

        let target = parts[2].parse::<VId>().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid target vertex")
        })?;

        if source >= edge_lists.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Source vertex {} out of range", source),
            ));
        }

        edge_lists[source].push(target);
        Ok(())
    }

    /// Builds the CSR representation from adjacency lists.
    ///
    /// # Arguments
    ///
    /// * `num_vertices` - Total number of vertices
    /// * `num_edges` - Total number of edges
    /// * `edge_lists` - Adjacency lists for each vertex
    /// * `communities` - Community assignments
    ///
    /// # Returns
    ///
    /// * `Ok(CsrGraph)` - Constructed CSR graph
    /// * `Err(std::io::Error)` - Construction error
    fn build_csr(
        num_vertices: usize,
        num_edges: usize,
        edge_lists: Vec<Vec<VId>>,
        communities: Vec<CommId>,
        max_comm_id: CommId,
    ) -> std::io::Result<Self> {
        // Pre-allocate offset array
        let mut offsets = Vec::with_capacity(num_vertices + 1);
        offsets.push(0);

        // Pre-allocate neighbor array with exact capacity
        let mut neighbors = Vec::with_capacity(num_edges);

        // Build CSR structure with zero-copy move from edge_lists
        for edge_list in edge_lists {
            neighbors.extend(edge_list); // Moves data, no copy
            offsets.push(neighbors.len());
        }

        Ok(Self {
            num_vertices,
            num_edges,
            offsets,
            neighbors,
            community_map: communities,
            max_comm_id,
            community_list: None,
        })
    }

    /// Get the neighbor iterator for a vertex
    pub fn get_neighbor_iter(&self, vertex_id: VId) -> impl Iterator<Item = VId> + '_ {
        let start = self.offsets[vertex_id as usize];
        let end = self.offsets[vertex_id as usize + 1];
        self.neighbors[start..end].iter().copied()
    }

    /// Compute the induced graph
    ///
    /// Returns an iterator for each vertex in vertex_ids that yields all its neighbors.
    /// This is used for storage purposes where all edges need to be preserved.
    ///
    /// # Arguments
    ///
    /// * `vertex_ids` - The vertex IDs to retrieve neighbors for
    ///
    /// # Returns
    ///
    /// A vector of iterators, one for each vertex in vertex_ids. Each iterator yields
    /// all neighbors of that vertex.
    pub fn induced_graph(&self, vertex_ids: &[VId]) -> Vec<impl Iterator<Item = VId> + '_> {
        vertex_ids
            .iter()
            .map(|&vid| self.get_neighbor_iter(vid))
            .collect()
    }

    /// Get the community structure as a reference to avoid copying.
    ///
    /// Returns a reference to the community list. If the community list hasn't been
    /// computed yet, it will be computed first.
    ///
    /// # Returns
    ///
    /// A reference to `Vec<Vec<VId>>` where each inner vector contains all vertices
    /// belonging to that community. The index corresponds to the community ID.
    pub fn get_community_structure(&mut self) -> &Vec<Vec<VId>> {
        // Compute community list if it hasn't been computed yet
        if self.community_list.is_none() {
            self.compute_community_list();
        }

        // Safe to unwrap since we just ensured it's computed
        self.community_list.as_ref().unwrap()
    }

    /// Get the degree of a vertex
    pub fn get_degree(&self, vertex_id: VId) -> u32 {
        let start = self.offsets[vertex_id as usize];
        let end = self.offsets[vertex_id as usize + 1];
        (end - start) as u32
    }

    /// Take ownership of community_map
    pub fn take_community_map(self) -> Vec<CommId> {
        self.community_map
    }

    /// Take ownership of community_list (must be computed first)
    pub fn take_community_list(mut self) -> Vec<Vec<VId>> {
        self.community_list
            .take()
            .expect("Community list not computed")
    }

    /// Get reference to community_map
    pub fn community_map(&self) -> &[CommId] {
        &self.community_map
    }
}

impl Default for CsrGraph {
    /// Creates a default empty CSR graph.
    ///
    /// This is equivalent to calling `CsrGraph::new()`.
    fn default() -> Self {
        Self::new()
    }
}
