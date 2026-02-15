package com.graph.rocks.community;

import com.graph.rocks.serialize.IdCodec;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * LSM-Community backed implementation of the TinkerPop Edge interface
 * Provides edge storage, serialization, and property management for LSM-Community based graph storage
 */
@SuppressWarnings("all")
public class CommunityEdge extends CommunityElement implements Edge {

    // Prefix for edge keys in LSM-Community storage
    public static final byte[] EDGE_PREFIX = "EDGE:".getBytes();

    private CommunityVertex outVertex;  // Outgoing vertex (source)
    private CommunityVertex inVertex;   // Incoming vertex (target)
    private Map<String, Property<?>> properties;  // Edge properties cache
    private long edgeHandle;               // Native handle for LSM-Community JNI operations

    /**
     * Get the native edge handle for LSM-Community JNI operations
     * @return Edge handle (long)
     */
    public long handle() {
        return edgeHandle;
    }

    /**
     * Primary constructor for creating new edges
     * @param graph Parent graph instance
     * @param id Edge identifier (null generates random UUID)
     * @param label Edge label (null defaults to "edge")
     * @param outVertex Source vertex (OUT direction)
     * @param inVertex Target vertex (IN direction)
     * @param keyValues Edge property key-value pairs
     */
    public CommunityEdge(final CommunityGraph graph, final Object id, final String label,
                         final CommunityVertex outVertex, final CommunityVertex inVertex, final Object... keyValues) {

        super((id == null) ? generateId(outVertex, label, inVertex) : id,
                (label == null) ? "edge" : label, graph);

        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.properties = new HashMap<>();

        // Validate and clean property key-value pairs
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        List<Object> cleanedList = new ArrayList<>();

        for (int i = 0; i < keyValues.length; i += 2) {
            Object keyObj = keyValues[i];
            Object value = keyValues[i + 1];
            String key = keyObj.toString();

            // Filter hidden/empty keys (per TinkerPop specifications)
            if (!Graph.Hidden.isHidden(key) && !key.isEmpty()) {
                cleanedList.add(keyObj);
                cleanedList.add(value);
            }
        }

        // Initialize properties with cleaned key-value pairs
        Object[] cleaned = cleanedList.toArray(new Object[0]);
        for (int i = 0; i < cleaned.length; i += 2) {
            final String key = cleaned[i].toString();
            final Object value = cleaned[i + 1];
            this.properties.put(key, new CommunityProperty<>(this, key, value, graph));
        }

        // Serialize and persist edge data
        byte[] outerIdBytes = IdCodec.toBytes(id);
        this.edgeHandle = createEdge(graph.handle(), outerIdBytes, outVertex.handle(), inVertex.handle(), serialize());
    }

    /**
     * Updates edge's label and properties, overwriting existing ones
     *
     * @param label New edge label
     * @param keyValues New property key-value pairs (even-length array: key1, value1, ...)
     */
    public void setData(final String label, final Object... keyValues) {
        this.label = label;
        this.properties = new HashMap<>();

        // Validate and clean property key-value pairs
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        List<Object> cleanedList = new ArrayList<>();

        for (int i = 0; i < keyValues.length; i += 2) {
            Object keyObj = keyValues[i];
            Object value = keyValues[i + 1];
            String key = keyObj.toString();

            // Filter hidden/empty keys (per TinkerPop specifications)
            if (!Graph.Hidden.isHidden(key) && !key.isEmpty()) {
                cleanedList.add(keyObj);
                cleanedList.add(value);
            }
        }

        // Initialize properties with cleaned key-value pairs
        Object[] cleaned = cleanedList.toArray(new Object[0]);
        for (int i = 0; i < cleaned.length; i += 2) {
            final String key = cleaned[i].toString();
            final Object value = cleaned[i + 1];
            this.properties.put(key, new CommunityProperty<>(this, key, value, graph));
        }
    }

    /**
     * Constructor for deserializing existing edges from LSM-Community handle
     * @param graph Parent graph instance
     * @param edgeHandle Native edge handle from LSM-Community
     */
    public CommunityEdge(final CommunityGraph graph, long edgeHandle) {
        super("", "edge", graph);
        this.edgeHandle = edgeHandle;

        // Retrieve and deserialize edge data from LSM-Community
        byte[] data = getDataFromEdgeHandle(graph.handle(), edgeHandle);

        if (data == null || data.length == 0) {
            this.edgeHandle = edgeHandle;
            this.id = edgeHandle;
            this.outVertex = new CommunityVertex(graph, (edgeHandle >>> 32) & 0xFFFFFFFFL);
            this.inVertex = new CommunityVertex(graph, edgeHandle & 0xFFFFFFFFL);
            this.properties = new HashMap<>();
            return;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzis = new GZIPInputStream(bais);
             ObjectInputStream ois = new ObjectInputStream(gzis)) {

            // Deserialize core edge data
            Object id = ois.readObject();
            String label = (String) ois.readObject();
            Object outVertexId = ois.readObject();
            Object inVertexId = ois.readObject();
            Map<String, Object> props = (Map<String, Object>) ois.readObject();

            // Initialize core properties
            this.id = id;
            this.label = label;

            // Resolve connected vertices
            this.outVertex = (CommunityVertex) graph.vertex(outVertexId);
            this.inVertex = (CommunityVertex) graph.vertex(inVertexId);

            // Initialize properties from deserialized data
            this.properties = new HashMap<>();
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                this.properties.put(entry.getKey(), new CommunityProperty<>(this, entry.getKey(), entry.getValue(), graph));
            }
        } catch (IOException e) {
            throw new RuntimeException("IO error initializing RocksEdge: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found during RocksEdge deserialization: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieve serialized edge data from LSM-Community using native handle
     * @param graphHandle Parent graph handle
     * @param edgeHandle Target edge handle
     * @return Serialized edge data bytes
     */
    public byte[] getDataFromEdgeHandle(long graphHandle, long edgeHandle) {
        return graph.getJni().getDataFromEdgeHandle(graphHandle, edgeHandle);
    }

    /**
     * Serialize edge data to compressed byte array
     * Includes: id, label, outVertexId, inVertexId, and properties
     * @return Compressed serialized byte array
     */
    public byte[] serialize() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzos = new GZIPOutputStream(baos);
             ObjectOutputStream oos = new ObjectOutputStream(gzos)) {

            // Write core edge identifiers
            oos.writeObject(id);
            oos.writeObject(label);
            oos.writeObject(outVertex.id());
            oos.writeObject(inVertex.id());

            // Convert properties to serializable map
            Map<String, Object> serializableProps = new HashMap<>();
            for (Map.Entry<String, Property<?>> entry : properties.entrySet()) {
                serializableProps.put(entry.getKey(), entry.getValue().value());
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
     * Create edge in LSM-Community via JNI
     * @param graphHandle Parent graph handle
     * @param outerId External edge ID bytes
     * @param outVertexHandle Source vertex handle
     * @param inVertexHandle Target vertex handle
     * @param data Serialized edge data
     * @return Created edge handle
     */
    public long createEdge(long graphHandle, byte[] outerId, long outVertexHandle, long inVertexHandle, byte[] data) {
        return graph.getJni().createEdge(graphHandle, outerId, outVertexHandle, inVertexHandle, data);
    }

    /**
     * Update edge data in LSM-Community via JNI
     * @param graphHandle Parent graph handle
     * @param edgeHandle Target edge handle
     * @param data Updated serialized edge data
     */
    public void putEdgeData(long graphHandle, long edgeHandle, byte[] data) {
        graph.getJni().putEdgeData(graphHandle, edgeHandle, data);
    }

    /**
     * Generate unique ID for new edges
     * Uses UUID for guaranteed uniqueness across distributed environments
     * @param outVertex Source vertex
     * @param label Edge label
     * @param inVertex Target vertex
     * @return UUID string as edge ID
     */
    private static Object generateId(final CommunityVertex outVertex, final String label, final CommunityVertex inVertex) {
        return UUID.randomUUID().toString();
    }

    /**
     * Build LSM-Community storage key for edge
     * @param id Edge identifier
     * @return Byte array key
     */
    public static byte[] buildEdgeKey(final Object id) {
        return (new String(EDGE_PREFIX) + id.toString()).getBytes();
    }

    // ====================== RocksElement Abstract Method Implementation ======================
    /**
     * Encode edge ID to LSM-Community storage key format
     * @return Encoded key bytes
     */
    @Override
    protected byte[] encodeKey() {
        return buildEdgeKey(id());
    }

    // ====================== Edge Interface Implementation ======================
    /**
     * Get the source (OUT) vertex of this edge
     * @return Source vertex
     */
    @Override
    public Vertex outVertex() {
        return outVertex;
    }

    /**
     * Get the target (IN) vertex of this edge
     * @return Target vertex
     */
    @Override
    public Vertex inVertex() {
        return inVertex;
    }

    /**
     * Get vertices connected by this edge in specified direction
     * @param direction Direction to retrieve vertices (IN/OUT/BOTH)
     * @return Iterator of connected vertices
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return Collections.singletonList((Vertex) outVertex).iterator();
            case IN:
                return Collections.singletonList((Vertex) inVertex).iterator();
            case BOTH:
                return Arrays.asList((Vertex) outVertex, (Vertex) inVertex).iterator();
            default:
                throw new IllegalArgumentException("Invalid direction: " + direction);
        }
    }

    /**
     * Get parent graph instance
     * @return Parent CommunityGraph
     */
    @Override
    public Graph graph() {
        return this.graph;
    }

    // ====================== Property Management Implementation ======================
    /**
     * Get edge property by key
     * @param key Property key
     * @return Property instance (empty if not found)
     */
    @Override
    public <V> Property<V> property(String key) {
        @SuppressWarnings("unchecked")
        final Property<V> prop = (Property<V>) properties.get(key);
        return prop != null ? prop : Property.empty();
    }

    /**
     * Set edge property with single cardinality
     * @param key Property key
     * @param value Property value
     * @return Created/updated Property instance
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        final Property<V> newProp = new CommunityProperty<>(this, key, value, graph);
        properties.put(key, newProp);
        putEdgeData(graph.handle(), edgeHandle, serialize());
        return newProp;
    }

    /**
     * Get all property keys for this edge
     * @return Set of property keys
     */
    @Override
    public Set<String> keys() {
        return properties.keySet();
    }

    /**
     * Get edge properties (filtered by keys if specified)
     * @param keys Optional property keys to filter
     * @return Iterator of Property instances
     */
    @Override
    public <V> Iterator<Property<V>> properties(String... keys) {
        if (keys.length == 0) {
            @SuppressWarnings("unchecked")
            final Iterator<Property<V>> allProps = (Iterator<Property<V>>) (Iterator<?>) properties.values().iterator();
            return allProps;
        } else {
            return Arrays.stream(keys)
                    .map(k -> {
                        @SuppressWarnings("unchecked")
                        Property<V> prop = (Property<V>) property(k);
                        return prop;
                    })
                    .filter(Property::isPresent)
                    .iterator();
        }
    }

    /**
     * Remove this edge from the graph
     * Deletes edge data and associated indices from LSM-Community
     */
    @Override
    public void remove() {
        removeEdge(graph.handle(), edgeHandle);
    }

    /**
     * Remove edge from LSM-Community via JNI
     * @param graphHandle Parent graph handle
     * @param edgeHandle Target edge handle
     */
    public void removeEdge(long graphHandle, long edgeHandle) {
        graph.getJni().removeEdge(graphHandle, edgeHandle);
    }

    /**
     * Remove edge property by key
     * @param key Property key to remove
     */
    public void removeProperty(String key) {
        this.properties.remove(key);
        putEdgeData(graph.handle(), edgeHandle, serialize());
    }

    /**
     * Get string representation of edge (compliant with TinkerPop standards)
     * @return Formatted edge string
     */
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    /**
     * Get all edge properties
     * @return Map of property keys to Property instances
     */
    public Map<String, Property<?>> getProperties() {
        return properties;
    }
}