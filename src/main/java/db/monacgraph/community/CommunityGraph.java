package db.monacgraph.community;

import db.monacgraph.reader.EdgePropertyCsvReader;
import db.monacgraph.reader.EdgePropertyJsonReader;
import db.monacgraph.reader.VertexPropertyCsvReader;
import db.monacgraph.reader.VertexPropertyJsonReader;
import db.monacgraph.serialize.IdCodec;
import db.monacgraph.jni.RustJNI;
import db.monacgraph.storage.GraphStorage;
import db.monacgraph.storage.BwGraphStorage;
import db.monacgraph.storage.LsmCommunityStorage;
import db.monacgraph.reader.BwGraphImporter;
import db.monacgraph.so.SecondOrderTraversalSource;
import db.monacgraph.reader.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TinkerPop 3.x compliant Graph implementation backed by LSM-Community
 * Implements core graph operations (vertex/edge management) via JNI bridge to Rust-based LSM-tree storage
 * Provides persistent key-value storage with LSM-tree architecture for graph data
 */
@SuppressWarnings("unused")
public class CommunityGraph implements Graph, Serializable {

    // Graph feature metadata (TinkerPop compliance)
    private final Features features = new LsmGraphFeatures();

    // Native graph handle for JNI operations
    private volatile long graphHandle = -1;
    private String databaseName;
    private final GraphStorage jni;
    private final BaseConfiguration config = new BaseConfiguration();
    private final boolean bwGraph;
    private static final Object LIFECYCLE = new Object();
    private static final Map<Path, Integer> OPEN_GRAPHS = new HashMap<>();
    private Path registeredPath;
    public boolean isBwGraph() { return bwGraph; }
    public Path dataDirectory() { return Path.of(config.getString("storage.path", "data")); }
    public Path databaseDirectory(String name) {
        return Path.of(bwGraph ? config.getString("bwgraph.storage.root", "workspace/bw-graph") : "workspace", name)
                .toAbsolutePath().normalize();
    }
    private static void checkName(String name) {
        if (name == null || !name.matches("[a-zA-Z0-9_-]{1,128}"))
            throw new IllegalArgumentException("Invalid database name");
    }

    /**
     * Get JNI bridge instance for native LSM-Community operations
     * @return RustJNI instance for native graph operations
     */
    public GraphStorage getJni() {
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
     * Open CommunityGraph with configuration parameters
     * Creates required storage directories and initializes graph database
     *
     * @param configuration Graph configuration (supports "db.name" and "storage.path")
     * @return Initialized CommunityGraph instance
     */
    public static CommunityGraph open(final Configuration configuration) {
        return new CommunityGraph(configuration);
    }

    /**
     * Loads vertex properties from JSON/CSV file (data directory)
     * Skips if filename is null, trims .json/.csv suffix automatically
     * @param dataName File name with .json/.csv suffix
     * @throws IOException If file access/parsing fails
     */
    public void loadVertexProperty(String dataName) throws IOException {
        if (dataName == null) {
            return;
        }
        if (dataName.endsWith(".json")) {
            VertexPropertyJsonReader vReader = new VertexPropertyJsonReader(this);
            vReader.readProperties(dataName.substring(0, dataName.length() - 5));
        }
        if (dataName.endsWith(".csv")) {
            VertexPropertyCsvReader vReader = new VertexPropertyCsvReader(this);
            vReader.readProperties(dataName.substring(0, dataName.length() - 4));
        }
    }

    /**
     * Loads edge properties from JSON/CSV file (data directory)
     * Skips if filename is null, trims .json/.csv suffix automatically
     * @param dataName File name with .json/.csv suffix
     * @throws IOException If file access/parsing fails
     */
    public void loadEdgeProperty(String dataName) throws IOException {
        if (dataName == null) {
            return;
        }
        if (dataName.endsWith(".json")) {
            EdgePropertyJsonReader eReader = new EdgePropertyJsonReader(this);
            eReader.readProperties(dataName.substring(0, dataName.length() - 5));
        }
        if (dataName.endsWith(".csv")) {
            EdgePropertyCsvReader eReader = new EdgePropertyCsvReader(this);
            eReader.readProperties(dataName.substring(0, dataName.length() - 4));
        }
    }

    /**
     * Open CommunityGraph with default configuration
     * @return Initialized CommunityGraph instance with default storage path and database name
     */
    public static CommunityGraph open() {
        return open(new BaseConfiguration());
    }

    /**
     * Construct CommunityGraph instance with specified database name
     * Initializes native graph handle via JNI
     *
     * @param dbName Name/path of the graph database (uses default if null/empty)
     */
    public CommunityGraph(final String dbName) { this(defaultConfiguration(dbName)); }

    private static Configuration defaultConfiguration(String name) {
        BaseConfiguration config = new BaseConfiguration();
        config.setProperty("db.name", name == null || name.isBlank() ? "lsm-data" : name.trim());
        return config;
    }

    private CommunityGraph(Configuration configuration) {
        configuration.getKeys().forEachRemaining(k -> config.setProperty(k, configuration.getProperty(k)));
        String backend = config.getString("storage.backend", "lsm-community").trim().toLowerCase(Locale.ROOT);
        if (!backend.equals("lsm-community") && !backend.equals("bw-graph"))
            throw new IllegalArgumentException("storage.backend must be lsm-community or bw-graph");
        config.setProperty("storage.backend", backend);
        bwGraph = backend.equals("bw-graph");
        if (!bwGraph && !dataDirectory().toAbsolutePath().normalize().equals(Path.of("data").toAbsolutePath().normalize()))
            throw new IllegalArgumentException("LSM-Community currently requires storage.path=data");
        jni = bwGraph ? new BwGraphStorage(config) : new LsmCommunityStorage();
        reload(config.getString("db.name", "my_database"));
    }

    public long openDB(String name) {
        synchronized (LIFECYCLE) {
            checkName(name);
            if (graphHandle != -1) throw new IllegalStateException("Close the current graph before openDB");
            Path store = databaseDirectory(name);
            Path source = dataDirectory().resolve(name + ".graph");
            Path importing = store.resolve("monac.importing");
            if (bwGraph && Files.exists(importing))
                throw new IllegalStateException("Incomplete import; preserve and inspect " + store);
            try {
                Files.createDirectories(dataDirectory());
                if (!bwGraph && !Files.exists(source)) Files.writeString(source, "t 0 0\n", StandardCharsets.UTF_8);
                graphHandle = jni.openDB(name);
                databaseName = name; config.setProperty("db.name", name);
                registeredPath = store;
                OPEN_GRAPHS.merge(store, 1, Integer::sum);
                if (bwGraph && ((BwGraphStorage) jni).newlyCreated(graphHandle) && config.getBoolean("bwgraph.import.on-create", true) && Files.exists(source)) {
                    Files.writeString(importing, source.toAbsolutePath().toString());
                    BwGraphImporter.load(this, source);
                    Files.delete(importing);
                }
                return graphHandle;
            } catch (IOException | RuntimeException e) {
                if (graphHandle != -1) close();
                throw new IllegalStateException("Cannot open graph " + name, e);
            }
        }
    }

    /** Clear only this backend's store, preserving the previous database in backups. */
    public void clear() throws IOException {
        synchronized (LIFECYCLE) {
            checkName(databaseName);
            Path storage = databaseDirectory(databaseName);
            if (OPEN_GRAPHS.getOrDefault(storage, 0) != 1)
                throw new IllegalStateException("Clear requires the only open graph lease");
            Path backups = bwGraph ? storage.getParent().resolve("backups") : Path.of("workspace", "backups");
            Files.createDirectories(backups);
            Path backup = Files.createTempDirectory(backups, databaseName + "-");
            close();
            if (Files.exists(storage)) Files.move(storage, backup.resolve("storage"));
            if (!bwGraph) {
                Path topology = dataDirectory().resolve(databaseName + ".graph");
                if (Files.exists(topology)) Files.move(topology, backup.resolve("graph.graph"));
                Files.writeString(topology, "t 0 0\n", StandardCharsets.UTF_8);
            }
            jni.resetDB(databaseName);
            boolean importOnCreate = config.getBoolean("bwgraph.import.on-create", true);
            config.setProperty("bwgraph.import.on-create", false);
            try { openDB(databaseName); }
            finally { config.setProperty("bwgraph.import.on-create", importOnCreate); }
        }
    }

    /**
     * Open CommunityGraph with specified database name (convenience method)
     * Creates data directory structure and initial graph file with header
     *
     * @param dbName Name of the graph database
     * @return Initialized CommunityGraph instance
     */
    public static CommunityGraph open(final String dbName) { return new CommunityGraph(dbName); }

    /** Changing graph name retains the selected backend, root and import policy. */
    public void reload(final String dbName) {
        synchronized (LIFECYCLE) {
            checkName(dbName);
            close();
            openDB(dbName);
        }
    }

    /** Explicit upload replaces only the selected backend's store, preserving a backup. */
    public void importUploadedGraph(String name) throws IOException {
        synchronized (LIFECYCLE) {
            checkName(name);
            if (!Files.isRegularFile(dataDirectory().resolve(name + ".graph")))
                throw new IOException("Missing uploaded graph " + name);
            Path target = databaseDirectory(name);
            int ownLease = target.equals(registeredPath) ? 1 : 0;
            if (OPEN_GRAPHS.getOrDefault(target, 0) > ownLease)
                throw new IllegalStateException("Import requires exclusive access to the target graph");
            close();
            if (Files.exists(target)) {
                Path backups = target.getParent().resolve("backups");
                Files.createDirectories(backups);
                Path backup = Files.createTempDirectory(backups, name + "-upload-");
                Files.move(target, backup.resolve("storage"));
            }
            jni.resetDB(name);
            boolean previous = config.getBoolean("bwgraph.import.on-create", true);
            config.setProperty("bwgraph.import.on-create", true);
            try { openDB(name); }
            finally { config.setProperty("bwgraph.import.on-create", previous); }
        }
    }

    /** File formats refer to vertex IDs; only legacy un-mapped imports use raw handles. */
    public CommunityVertex vertexFromFileId(long id) {
        if (!bwGraph) return new CommunityVertex(this, id);
        Vertex vertex = vertex(id);
        if (vertex == null) throw new NoSuchElementException("Unknown imported vertex " + id);
        return (CommunityVertex) vertex;
    }
    public CommunityEdge edgeFromFileEndpoints(long out, long in) {
        long src = vertexFromFileId(out).handle(), dst = vertexFromFileId(in).handle();
        return new CommunityEdge(this, (src << 32) | dst);
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
     * @return Newly created CommunityVertex instance
     */
    @Override
    public Vertex addVertex(final Object... keyValues) {
        org.apache.tinkerpop.gremlin.structure.util.ElementHelper.legalPropertyKeyValueArray(keyValues);
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
        return new CommunityVertex(this, vertexId, label, keyValues);
    }

    /**
     * Get vertex by its unique identifier
     *
     * @param id Vertex identifier (any type supported by IdCodec)
     * @return CommunityVertex instance (null if not found)
     */
    public Vertex vertex(Object id) {
        long vertexHandle = -1;
        if (id instanceof CommunityVertex) {
            return ((CommunityVertex) id).graph() == this ? (CommunityVertex) id : null;
        } else {
            byte[] outerIdBytes = IdCodec.toBytes(id);
            vertexHandle = getVertexHandleById(graphHandle, outerIdBytes);
            // Numeric file IDs are longs; Groovy integer literals should resolve them too.
            if (vertexHandle == -1 && bwGraph && id instanceof Integer)
                vertexHandle = getVertexHandleById(graphHandle, IdCodec.toBytes(((Integer) id).longValue()));
        }

        if (vertexHandle == -1) {
            if (!bwGraph && id instanceof Number && Arrays.stream(getAllVertices(graphHandle)).anyMatch(v -> v == ((Number) id).longValue())) {
                vertexHandle = ((Number) id).longValue();
            } else return null;
        }
        return new CommunityVertex(this, vertexHandle);
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
     * @return Iterator of CommunityVertex instances matching the IDs
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
     * @return Iterator of all CommunityVertex instances in the graph
     */
    private Iterator<Vertex> scanAllVertices() {
        long[] vertexHandleList = getAllVertices(graphHandle);

        return Arrays.stream(vertexHandleList)
                .boxed()
                .map(handle -> (Vertex) new CommunityVertex(this, handle))
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
     * @return Newly created CommunityEdge instance
     * @throws IllegalArgumentException If vertices are not CommunityVertex instances
     */
    public Edge addEdge(final String label, final Vertex outVertex, final Vertex inVertex, final Object... keyValues) {
        org.apache.tinkerpop.gremlin.structure.util.ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (outVertex.graph() != this || inVertex.graph() != this) throw new IllegalArgumentException("Endpoints belong to another graph");
        // Validate vertex types
        if (!(outVertex instanceof CommunityVertex) || !(inVertex instanceof CommunityVertex)) {
            throw new IllegalArgumentException("Vertices must be of type CommunityVertex");
        }

        // Parse edge ID from properties
        Object edgeId = null;
        for (int i = 0; i < keyValues.length; i += 2) {
            if (keyValues[i] == T.id) {
                edgeId = keyValues[i + 1];
            }
        }

        // Generate UUID if no ID provided
        if (edgeId == null) {
            edgeId = generateId();
        }

        // Create and persist edge
        return new CommunityEdge(
                this,
                edgeId,
                label,
                (CommunityVertex) outVertex,
                (CommunityVertex) inVertex,
                keyValues
        );
    }

    /**
     * Get edge by its unique identifier
     *
     * @param id Edge identifier (any type supported by IdCodec)
     * @return CommunityEdge instance (null if not found)
     */
    public Edge edge(final Object id) {
        byte[] outerIdBytes = IdCodec.toBytes(id);
        long edgeHandle = getEdgeHandleById(graphHandle, outerIdBytes);
        return edgeHandle == -1 ? null : new CommunityEdge(this, edgeHandle);
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
     * @return Iterator of CommunityEdge instances matching the IDs
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
     * @return Iterator of all CommunityEdge instances in the graph
     */
    private Iterator<Edge> scanAllEdges() {
        long[] edgeHandleList = getAllEdges(graphHandle);
        return Arrays.stream(edgeHandleList)
                .boxed()
                .map(handle -> (Edge) new CommunityEdge(this, handle))
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
    public void removeEdgesByVertex(final CommunityVertex vertex) {
        // JNI implementation for edge cleanup
    }

    // ------------------------------ TinkerPop Interface Implementation ------------------------------

    /**
     * GraphComputer is not supported by CommunityGraph
     * @throws IllegalArgumentException Always throws (GraphComputer not supported)
     */
    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
    }

    /**
     * Default GraphComputer is not supported by CommunityGraph
     * @throws IllegalArgumentException Always throws (GraphComputer not supported)
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * Transactions are not supported by CommunityGraph
     * @throws UnsupportedOperationException Always throws (transactions not supported)
     */
    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    /**
     * Graph variables are not supported by CommunityGraph
     * @throws UnsupportedOperationException Always throws (variables not supported)
     */
    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    /**
     * Get graph configuration (empty for CommunityGraph)
     * @return Empty BaseConfiguration instance
     */
    @Override
    public Configuration configuration() {
        BaseConfiguration copy = new BaseConfiguration();
        config.getKeys().forEachRemaining(k -> copy.setProperty(k, config.getProperty(k)));
        return copy;
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
     */
    @Override
    public void close() {
        synchronized (LIFECYCLE) {
            if (graphHandle == -1) return;
            closeDB(graphHandle);
            graphHandle = -1;
            OPEN_GRAPHS.computeIfPresent(registeredPath, (p, count) -> count == 1 ? null : count - 1);
            registeredPath = null;
        }
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
     * TinkerPop-compliant feature definitions for CommunityGraph
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
