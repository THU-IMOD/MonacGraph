package com.graph.rocks;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TinkerPop 3.x compliant Graph implementation backed by LSM-Community (RocksDB)
 * Implements core graph operations (vertex/edge management) via JNI bridge to Rust-based LSM-tree storage
 * Provides persistent key-value storage with LSM-tree architecture for graph data
 */
public class LsmGraph implements Graph, Serializable {

    // Graph feature metadata (TinkerPop compliance)
    private final Features features = new LsmGraphFeatures();

    // Native graph handle for JNI operations
    private long graphHandle;
    private final RustJNI jni = new RustJNI();

    /**
     * Get JNI bridge instance for native LSM-Community operations
     * @return RustJNI instance for native graph operations
     */
    public RustJNI getJni() {
        return jni;
    }

    /**
     * Get native graph handle for low-level JNI operations
     * @return Long handle to native LSM-Community graph instance
     */
    public long handle() {
        return graphHandle;
    }

    /**
     * Open LsmGraph with configuration parameters
     * Creates required storage directories and initializes graph database
     *
     * @param configuration Graph configuration (supports "db.name" and "storage.path")
     * @return Initialized LsmGraph instance
     */
    public static LsmGraph open(final Configuration configuration) {
        // Extract configuration parameters with defaults
        String dbName = configuration.getString("db.name", "my_database");
        String storagePath = configuration.getString("storage.path", "./data");

        // Create storage directory if not exists
        File parentDir = new File(storagePath);
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new RuntimeException("Failed to create storage directory: " + parentDir.getAbsolutePath());
        }

        // Create graph data file
        File graphFile = new File(parentDir, dbName + ".graph");
        try {
            graphFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create graph file: " + graphFile.getAbsolutePath(), e);
        }

        return new LsmGraph(dbName);
    }

    /**
     * Open LsmGraph with default configuration
     * @return Initialized LsmGraph instance with default storage path and database name
     */
    public static LsmGraph open() {
        return open(new BaseConfiguration());
    }

    /**
     * Construct LsmGraph instance with specified database name
     * Initializes native graph handle via JNI
     *
     * @param dbName Name/path of the graph database (uses default if null/empty)
     */
    public LsmGraph(final String dbName) {
        // Use default name if input is invalid, trim whitespace
        final String actualDbName = (dbName == null || dbName.trim().isEmpty())
                ? "lsm-data"
                : dbName.trim();

        graphHandle = openDB(actualDbName);
    }

    /**
     * Open LSM-Community database via JNI bridge
     *
     * @param dbName Database name/path
     * @return Native handle to opened database
     */
    public long openDB(String dbName) {
        return jni.openDB(dbName);
    }

    /**
     * Open LsmGraph with specified database name (convenience method)
     * Creates data directory structure and initial graph file with header
     *
     * @param dbName Name of the graph database
     * @return Initialized LsmGraph instance
     */
    public static LsmGraph open(final String dbName) {
        // Create base data directory
        File parentDir = new File("./data");
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new RuntimeException("Failed to create data directory: " + parentDir.getAbsolutePath());
        }

        // Create graph file with header initialization
        File graphFile = new File(parentDir, dbName + ".graph");
        try {
            boolean isNewFile = graphFile.createNewFile();

            // Write initialization header for new files
            if (isNewFile) {
                try (BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(graphFile), StandardCharsets.UTF_8))) {
                    writer.write("t 0 0");
                    writer.newLine();
                    writer.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create graph file: " + graphFile.getAbsolutePath(), e);
        }

        return new LsmGraph(dbName);
    }

    // ------------------------------ Core Graph Operations ------------------------------

    /**
     * Get traversal source for graph traversals with second-order logic support
     * @return SecondOrderTraversalSource instance for this graph
     */
    @Override
    public SecondOrderTraversalSource traversal() {
        return new SecondOrderTraversalSource(this);
    }

    // ------------------------------ Vertex Operations ------------------------------

    /**
     * Add new vertex to the graph with specified properties
     * Supports T.id (custom ID) and T.label (vertex label) properties
     * Generates UUID if ID not provided
     *
     * @param keyValues Vertex property key-value pairs (supports T.id/T.label)
     * @return Newly created LsmVertex instance
     */
    @Override
    public Vertex addVertex(final Object... keyValues) {
        // Parse ID and label from properties
        Object vertexId = null;
        String label = Vertex.DEFAULT_LABEL;

        for (int i = 0; i < keyValues.length; i += 2) {
            final Object keyObj = keyValues[i];
            if (keyObj == T.id) {
                vertexId = keyValues[i + 1];
            } else if (keyObj == T.label) {
                label = keyValues[i + 1].toString();
            }
        }

        // Generate UUID if no ID provided
        if (vertexId == null) {
            vertexId = generateId();
        }

        // Create and persist vertex
        return new LsmVertex(this, vertexId, label, keyValues);
    }

    /**
     * Get vertex by its unique identifier
     *
     * @param id Vertex identifier (any type supported by IdCodec)
     * @return LsmVertex instance (null if not found)
     */
    public Vertex vertex(final Object id) {
        byte[] outerIdBytes = IdCodec.toBytes(id);
        long vertexHandle = getVertexHandleById(graphHandle, outerIdBytes);
        return new LsmVertex(this, vertexHandle);
    }

    /**
     * Get native vertex handle by external ID via JNI
     *
     * @param graphHandle Parent graph native handle
     * @param outerId External vertex ID as byte array
     * @return Native vertex handle (long)
     */
    public long getVertexHandleById(long graphHandle, byte[] outerId) {
        return jni.getVertexHandleById(graphHandle, outerId);
    }

    /**
     * Get vertices by their IDs (or all vertices if empty)
     *
     * @param ids Vertex identifiers (empty array returns all vertices)
     * @return Iterator of LsmVertex instances matching the IDs
     */
    @Override
    public Iterator<Vertex> vertices(final Object... ids) {
        if (ids.length == 0) {
            return scanAllVertices();
        } else {
            List<Vertex> vertexList = Arrays.stream(ids)
                    .map(this::vertex)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            return vertexList.iterator();
        }
    }

    /**
     * Scan all vertices in the graph via native JNI call
     * @return Iterator of all LsmVertex instances in the graph
     */
    private Iterator<Vertex> scanAllVertices() {
        long[] vertexHandleList = getAllVertices(graphHandle);

        return Arrays.stream(vertexHandleList)
                .boxed()
                .map(handle -> (Vertex) new LsmVertex(this, handle))
                .filter(Objects::nonNull)
                .iterator();
    }

    /**
     * Get all native vertex handles via JNI
     *
     * @param graphHandle Parent graph native handle
     * @return Array of native vertex handles (long[])
     */
    public long[] getAllVertices(long graphHandle) {
        return jni.getAllVertices(graphHandle);
    }

    // ------------------------------ Edge Operations ------------------------------

    /**
     * Add new edge between two vertices with specified properties
     * Supports T.id (custom edge ID) property
     * Generates UUID if ID not provided
     *
     * @param label Edge label (relationship type)
     * @param outVertex Source (OUT) vertex
     * @param inVertex Target (IN) vertex
     * @param keyValues Edge property key-value pairs (supports T.id)
     * @return Newly created LsmEdge instance
     * @throws IllegalArgumentException If vertices are not LsmVertex instances
     */
    public Edge addEdge(final String label, final Vertex outVertex, final Vertex inVertex, final Object... keyValues) {
        // Validate vertex types
        if (!(outVertex instanceof LsmVertex) || !(inVertex instanceof LsmVertex)) {
            throw new IllegalArgumentException("Vertices must be of type LsmVertex");
        }

        // Parse edge ID from properties
        Object edgeId = null;
        for (int i = 0; i < keyValues.length; i += 2) {
            if (keyValues[i].toString().equals(T.id.getAccessor())) {
                edgeId = keyValues[i + 1];
            }
        }

        // Generate UUID if no ID provided
        if (edgeId == null) {
            edgeId = generateId();
        }

        // Create and persist edge
        return new LsmEdge(
                this,
                edgeId,
                label,
                (LsmVertex) outVertex,
                (LsmVertex) inVertex,
                keyValues
        );
    }

    /**
     * Get edge by its unique identifier
     *
     * @param id Edge identifier (any type supported by IdCodec)
     * @return LsmEdge instance (null if not found)
     */
    public Edge edge(final Object id) {
        byte[] outerIdBytes = IdCodec.toBytes(id);
        long edgeHandle = getEdgeHandleById(graphHandle, outerIdBytes);
        return new LsmEdge(this, edgeHandle);
    }

    /**
     * Get native edge handle by external ID via JNI
     *
     * @param graphHandle Parent graph native handle
     * @param outerId External edge ID as byte array
     * @return Native edge handle (long)
     */
    public long getEdgeHandleById(long graphHandle, byte[] outerId) {
        return jni.getEdgeHandleById(graphHandle, outerId);
    }

    /**
     * Get edges by their IDs (or all edges if empty)
     *
     * @param ids Edge identifiers (empty array returns all edges)
     * @return Iterator of LsmEdge instances matching the IDs
     */
    @Override
    public Iterator<Edge> edges(final Object... ids) {
        if (ids.length == 0) {
            return scanAllEdges();
        } else {
            Stream<Edge> edgeStream = Arrays.stream(ids)
                    .map(this::edge)
                    .filter(Objects::nonNull);

            return edgeStream.iterator();
        }
    }

    /**
     * Scan all edges in the graph via native JNI call
     * @return Iterator of all LsmEdge instances in the graph
     */
    private Iterator<Edge> scanAllEdges() {
        long[] edgeHandleList = getAllEdges(graphHandle);

        return Arrays.stream(edgeHandleList)
                .boxed()
                .map(handle -> (Edge) new LsmEdge(this, handle))
                .filter(Objects::nonNull)
                .iterator();
    }

    /**
     * Get all native edge handles via JNI
     *
     * @param graphHandle Parent graph native handle
     * @return Array of native edge handles (long[])
     */
    public long[] getAllEdges(long graphHandle) {
        return jni.getAllEdges(graphHandle);
    }

    /**
     * Remove all edges connected to a specific vertex
     * JNI-based implementation for edge cleanup
     *
     * @param vertex Vertex to remove connected edges from
     */
    public void removeEdgesByVertex(final LsmVertex vertex) {
        // JNI implementation for edge cleanup
    }

    // ------------------------------ TinkerPop Interface Implementation ------------------------------

    /**
     * GraphComputer is not supported by LsmGraph
     * @throws IllegalArgumentException Always throws (GraphComputer not supported)
     */
    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
    }

    /**
     * Default GraphComputer is not supported by LsmGraph
     * @throws IllegalArgumentException Always throws (GraphComputer not supported)
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * Transactions are not supported by LsmGraph
     * @throws UnsupportedOperationException Always throws (transactions not supported)
     */
    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    /**
     * Graph variables are not supported by LsmGraph
     * @throws UnsupportedOperationException Always throws (variables not supported)
     */
    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    /**
     * Get graph configuration (empty for LsmGraph)
     * @return Empty BaseConfiguration instance
     */
    @Override
    public Configuration configuration() {
        return new BaseConfiguration();
    }

    /**
     * Get graph feature metadata (TinkerPop compliance)
     * @return LsmGraphFeatures instance defining supported features
     */
    @Override
    public Features features() {
        return features;
    }

    /**
     * Close graph database and release native resources
     * Closes JNI handle and cleans up native connections
     *
     * @throws Exception If database closure fails
     */
    @Override
    public void close() throws Exception {
        if (graphHandle == -1) {
            return;
        }
        closeDB(graphHandle);
        graphHandle = -1;
    }

    /**
     * Close LSM-Community database via JNI
     *
     * @param graphHandle Native graph handle to close
     */
    public void closeDB(long graphHandle) {
        jni.closeDB(graphHandle);
    }

    /**
     * Generate unique UUID for vertices/edges
     * @return UUID string as unique identifier
     */
    private static Object generateId() {
        return UUID.randomUUID().toString();
    }

    // ------------------------------ Graph Feature Definitions ------------------------------

    /**
     * TinkerPop-compliant feature definitions for LsmGraph
     * Defines supported/unsupported features per TinkerPop specifications
     */
    public static class LsmGraphFeatures implements Features {
        private final GraphFeatures graphFeatures = new LsmGraphGraphFeatures();
        private final VertexFeatures vertexFeatures = new LsmVertexFeatures();
        private final EdgeFeatures edgeFeatures = new LsmEdgeFeatures();

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        /**
         * Graph-level feature support definitions
         */
        public static class LsmGraphGraphFeatures implements GraphFeatures {
            @Override
            public boolean supportsTransactions() {
                return false;
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public boolean supportsPersistence() {
                return true;
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return true;
            }

            @Override
            public VariableFeatures variables() {
                return new VariableFeatures() {
                    @Override
                    public boolean supportsVariables() {
                        return false;
                    }
                };
            }
        }

        /**
         * Vertex feature support definitions
         */
        public static class LsmVertexFeatures implements VertexFeatures {
            private final VertexPropertyFeatures vertexPropertyFeatures = new LsmVertexPropertyFeatures();

            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }

            @Override
            public boolean supportsCustomIds() {
                return true;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return true;
            }
        }

        /**
         * Vertex property feature support definitions
         */
        public static class LsmVertexPropertyFeatures implements VertexPropertyFeatures {
            @Override
            public boolean supportsNullPropertyValues() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return true;
            }

            @Override
            public boolean supportsCustomIds() {
                return true;
            }

            @Override
            public boolean supportsProperties() {
                return true;
            }
        }

        /**
         * Edge feature support definitions
         */
        public static class LsmEdgeFeatures implements EdgeFeatures {
            private final EdgePropertyFeatures edgePropertyFeatures = new LsmEdgePropertyFeatures();

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }

            @Override
            public boolean supportsCustomIds() {
                return true;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return true;
            }
        }

        /**
         * Edge property feature support definitions
         */
        public static class LsmEdgePropertyFeatures implements EdgePropertyFeatures {
            public boolean supportsNullPropertyValues() {
                return false;
            }
        }
    }
}