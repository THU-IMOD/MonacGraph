package com.graph.rocks;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.list;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;

/**
 * Implementation of Vertex interface for LSM-Community storage
 * Provides vertex data management, serialization, and property operations
 */
public class LsmVertex extends LsmElement implements Vertex {

    public static final byte[] VERTEX_PREFIX = "V:".getBytes();
    private Map<String, VertexProperty<?>> properties;
    private final long vertexHandle;

    /**
     * Get the vertex handle for LSM-Community JNI operations
     * @return Vertex handle (long)
     */
    public long handle() {
        return vertexHandle;
    }

    /**
     * Primary constructor for creating new vertices
     * @param graph Parent graph instance
     * @param id Vertex identifier (null generates random UUID)
     * @param label Vertex label (null defaults to "vertex")
     */
    public LsmVertex(final LsmGraph graph, final Object id, final String label) {
        super((id == null) ? generateId() : id,
                (label == null) ? "vertex" : label,
                graph);
        this.properties = null;
        byte[] outerIdBytes = IdCodec.toBytes(id);
        vertexHandle = createVertex(graph.handle(), outerIdBytes, serialize());
    }

    /**
     * Constructor with initial properties
     * @param graph Parent graph instance
     * @param id Vertex identifier (null generates random UUID)
     * @param label Vertex label (null defaults to "vertex")
     * @param keyValues Property key-value pairs
     */
    public LsmVertex(final LsmGraph graph, final Object id, final String label, final Object... keyValues) {
        super((id == null) ? generateId() : id,
                (label == null) ? "vertex" : label,
                graph);
        this.properties = new HashMap<>();

        // Validate and clean property key-value pairs
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        List<Object> cleanedList = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object keyObj = keyValues[i];
            Object value = keyValues[i + 1];
            String key = keyObj.toString();

            // Filter hidden/empty keys
            if (!Graph.Hidden.isHidden(key) && !key.isEmpty()) {
                cleanedList.add(keyObj);
                cleanedList.add(value);
            }
        }

        // Initialize properties
        Object[] cleaned = cleanedList.toArray(new Object[0]);
        for (int i = 0; i < cleaned.length; i += 2) {
            final String key = cleaned[i].toString();
            final Object value = cleaned[i + 1];
            this.properties.put(key, new LsmVertexProperty<>(generateId(), this, key, value));
        }

        // Persist vertex data
        byte[] outerIdBytes = IdCodec.toBytes(id);
        vertexHandle = createVertex(graph.handle(), outerIdBytes, serialize());
    }

    /**
     * Constructor for deserializing existing vertices from LSM-Community
     * @param graph Parent graph instance
     * @param vertexHandle Existing vertex handle
     */
    public LsmVertex(final LsmGraph graph, long vertexHandle) {
        super("", "vertex", graph);
        byte[] data = getDataFromVertexHandle(graph.handle(), vertexHandle);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzis = new GZIPInputStream(bais);
             ObjectInputStream ois = new ObjectInputStream(gzis)) {

            // Deserialize vertex data
            Object id = ois.readObject();
            String label = (String) ois.readObject();
            Map<String, Object> props = (Map<String, Object>) ois.readObject();

            // Initialize core fields
            this.id = id;
            this.label = label;
            this.vertexHandle = vertexHandle;

            // Initialize properties
            this.properties = new HashMap<>();
            List<Object> keyValues = new ArrayList<>();
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                keyValues.add(entry.getKey());
                keyValues.add(entry.getValue());
            }

            for (int i = 0; i < keyValues.size(); i += 2) {
                final String key = keyValues.get(i).toString();
                final Object value = keyValues.get(i + 1);
                this.properties.put(key, new LsmVertexProperty<>(generateId(), this, key, value));
            }
        } catch (IOException e) {
            throw new RuntimeException("IO error initializing RocksVertex: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found during RocksVertex deserialization: " + e.getMessage(), e);
        }
    }

    /**
     * Create vertex in LSM-Community via JNI
     * @param graphHandle Parent graph handle
     * @param outerId External vertex ID bytes
     * @param data Serialized vertex data
     * @return Created vertex handle
     */
    public long createVertex(long graphHandle, byte[] outerId, byte[] data) {
        return graph.getJni().createVertex(graphHandle, outerId, data);
    }

    /**
     * Serialize vertex data to compressed byte array
     * Includes: id, label, and properties (as simple key-value map)
     * @return Compressed serialized byte array
     */
    public byte[] serialize() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzos = new GZIPOutputStream(baos);
             ObjectOutputStream oos = new ObjectOutputStream(gzos)) {

            // Write core vertex data
            oos.writeObject(id);
            oos.writeObject(label);

            // Convert properties to serializable map
            Map<String, Object> serializableProps = new HashMap<>();
            if (properties != null) {
                for (Map.Entry<String, VertexProperty<?>> entry : properties.entrySet()) {
                    serializableProps.put(entry.getKey(), entry.getValue().value());
                }
            }
            oos.writeObject(serializableProps);

            gzos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    /**
     * Retrieve vertex data from LSM-Community using vertex handle
     * @param graphHandle Parent graph handle
     * @param vertexHandle Target vertex handle
     * @return Serialized vertex data bytes
     */
    public byte[] getDataFromVertexHandle(long graphHandle, long vertexHandle) {
        return graph.getJni().getDataFromVertexHandle(graphHandle, vertexHandle);
    }

    /**
     * Add/update vertex property with specified cardinality
     * @param cardinality Property cardinality (single/set/list)
     * @param key Property key
     * @param value Property value
     * @param keyValues Additional property metadata
     * @return Created/updated VertexProperty
     */
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        // Validate property key
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (Graph.Hidden.isHidden(key) || key.isEmpty()) {
            throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key);
        }

        if (cardinality == list) {
            cardinality = single;
        }

        // Handle different cardinality strategies
        VertexProperty<V> vertexProperty;
        switch (cardinality) {
            case single:
                // Single cardinality: overwrite existing value
                vertexProperty = new LsmVertexProperty<>(generateId(), this, key, value, keyValues);
                this.properties.put(key, vertexProperty);
                putVertexData(graph.handle(), vertexHandle, serialize());
                break;

            case set:
                // Set cardinality: unique values only
                boolean exists = false;
                for (VertexProperty<?> prop : this.properties.values()) {
                    if (prop.key().equals(key) && prop.value().equals(value)) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    vertexProperty = new LsmVertexProperty<>(generateId(), this, key, value, keyValues);
                    this.properties.put(key, vertexProperty);
                    putVertexData(graph.handle(), vertexHandle, serialize());
                } else {
                    @SuppressWarnings("unchecked")
                    VertexProperty<V> existingProp = (VertexProperty<V>) this.properties.get(key);
                    return existingProp;
                }
                break;

            case list:
                // List cardinality: allow duplicate values with unique storage keys
                String uniqueKey = key + "_" + UUID.randomUUID().toString();
                vertexProperty = new LsmVertexProperty<>(generateId(), this, key, value, keyValues);
                this.properties.put(uniqueKey, vertexProperty);
                putVertexData(graph.handle(), vertexHandle, serialize());
                break;

            default:
                throw new IllegalArgumentException("Unsupported Cardinality type: " + cardinality);
        }

        return vertexProperty;
    }

    /**
     * Persist vertex data to LSM-Community via JNI
     * @param graphHandle Parent graph handle
     * @param vertexHandle Target vertex handle
     * @param data Serialized vertex data
     */
    public void putVertexData(long graphHandle, long vertexHandle, byte[] data) {
        graph.getJni().putVertexData(graphHandle, vertexHandle, data);
    }

    /**
     * Get all edges connected to this vertex with specified direction and labels
     * @param direction Edge direction (IN/OUT/BOTH)
     * @param edgeLabels Optional edge label filters
     * @return Iterator of connected edges
     */
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        // Collect edge handles for specified direction(s)
        List<Long> allEdgeHandles = new ArrayList<>();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            long[] outHandles = getEdgeHandleByVertex(graph.handle(), vertexHandle, 0);
            if (outHandles != null) {
                for (long h : outHandles) allEdgeHandles.add(h);
            }
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            long[] inHandles = getEdgeHandleByVertex(graph.handle(), vertexHandle, 1);
            if (inHandles != null) {
                for (long h : inHandles) allEdgeHandles.add(h);
            }
        }

        // Convert handles to Edge objects
        List<Edge> edgeList = new ArrayList<>();
        for (long handle : allEdgeHandles) {
            if (handle <= 0) continue;

            Edge edge = null;
            try {
                edge = (Edge) new LsmEdge(graph, handle);
            } catch (Exception e) {
                continue;
            }

            if (edge != null) {
                edgeList.add(edge);
            }
        }

        // Apply label filtering
        List<Edge> filteredEdges = new ArrayList<>();
        for (Edge edge : edgeList) {
            boolean keep = true;
            if (edgeLabels != null && edgeLabels.length > 0) {
                keep = Arrays.asList(edgeLabels).contains(edge.label());
            }
            if (keep) {
                filteredEdges.add(edge);
            }
        }

        return filteredEdges.iterator();
    }

    /**
     * Get edge handles from RocksDB via JNI
     * @param graphHandle Parent graph handle
     * @param vertexHandle Target vertex handle
     * @param direction Edge direction (0=OUT, 1=IN)
     * @return Array of edge handles
     */
    public long[] getEdgeHandleByVertex(long graphHandle, long vertexHandle, int direction) {
        return graph.getJni().getEdgeHandleByVertex(graphHandle, vertexHandle, direction);
    }

    /**
     * Get all vertices connected to this vertex with specified direction and edge labels
     * @param direction Edge direction (IN/OUT/BOTH)
     * @param edgeLabels Optional edge label filters
     * @return Iterator of connected vertices (deduped)
     */
    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edges = edges(direction, edgeLabels);
        final Set<Vertex> vertices = new LinkedHashSet<>();

        while (edges.hasNext()) {
            final Edge edge = edges.next();
            switch (direction) {
                case OUT:
                    vertices.add(edge.inVertex());
                    break;
                case IN:
                    vertices.add(edge.outVertex());
                    break;
                case BOTH:
                    final Vertex outV = edge.outVertex();
                    final Vertex inV = edge.inVertex();
                    if (!outV.id().equals(this.id())) vertices.add(outV);
                    if (!inV.id().equals(this.id())) vertices.add(inV);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid direction: " + direction);
            }
        }

        return vertices.iterator();
    }

    /**
     * Generate unique ID for new vertices/properties
     * @return UUID string
     */
    private static Object generateId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Build LSM-Community key for vertex storage
     * @param id Vertex identifier
     * @return Byte array key
     */
    public static byte[] buildVertexKey(final Object id) {
        return (new String(VERTEX_PREFIX) + id.toString()).getBytes();
    }

    /**
     * Encode vertex key for LSM-Community storage
     * @return Encoded key bytes
     */
    @Override
    protected byte[] encodeKey() {
        return buildVertexKey(id());
    }

    /**
     * Add edge from this vertex to target vertex
     * @param label Edge label
     * @param inVertex Target vertex (in direction)
     * @param keyValues Edge properties
     * @return Created edge
     */
    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        return graph.addEdge(label, this, inVertex, keyValues);
    }

    /**
     * Get parent graph instance
     * @return Parent LsmGraph
     */
    @Override
    public Graph graph() {
        return this.graph;
    }

    /**
     * Get vertex property by key
     * @param key Property key
     * @return VertexProperty (empty if not found)
     */
    @Override
    public <V> VertexProperty<V> property(final String key) {
        @SuppressWarnings("unchecked")
        final VertexProperty<V> prop = (VertexProperty<V>) properties.get(key);
        return prop != null ? prop : VertexProperty.empty();
    }

    /**
     * Set single cardinality vertex property
     * @param key Property key
     * @param value Property value
     * @return Created/updated VertexProperty
     */
    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final VertexProperty<V> newProp = new LsmVertexProperty<>(generateId(), this, key, value);
        properties.put(key, newProp);
        putVertexData(graph.handle(), vertexHandle, serialize());
        return newProp;
    }

    /**
     * Get all property keys for this vertex
     * @return Set of property keys
     */
    @Override
    public Set<String> keys() {
        return properties.keySet();
    }

    /**
     * Get vertex properties (filtered by keys if specified)
     * @param keys Optional property keys to filter
     * @return Iterator of VertexProperties
     */
    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... keys) {
        if (keys.length == 0) {
            @SuppressWarnings("unchecked")
            final Iterator<VertexProperty<V>> allProps = (Iterator<VertexProperty<V>>) (Iterator<?>) properties.values().iterator();
            return allProps;
        } else {
            return Arrays.stream(keys)
                    .map(k -> {
                        @SuppressWarnings("unchecked")
                        VertexProperty<V> prop = (VertexProperty<V>) property(k);
                        return prop;
                    })
                    .filter(VertexProperty::isPresent)
                    .iterator();
        }
    }

    /**
     * Remove this vertex from the graph
     */
    @Override
    public void remove() {
        removeVertex(graph.handle(), vertexHandle);
    }

    /**
     * Remove vertex from RocksDB via JNI
     * @param graphHandle Parent graph handle
     * @param vertexHandle Target vertex handle
     */
    public void removeVertex(long graphHandle, long vertexHandle) {
        graph.getJni().removeVertex(graphHandle, vertexHandle);
    }

    /**
     * Remove vertex property by key
     * @param key Property key to remove
     */
    public void removeProperty(String key) {
        this.properties.remove(key);
        putVertexData(graph.handle(), vertexHandle, serialize());
    }

    /**
     * Get string representation of vertex
     * @return Formatted vertex string
     */
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    /**
     * Get all vertex properties
     * @return Map of property keys to VertexProperties
     */
    public Map<String, VertexProperty<?>> getProperties() {
        return properties;
    }
}