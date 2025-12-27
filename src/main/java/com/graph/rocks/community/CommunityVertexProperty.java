package com.graph.rocks.community;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * LSM-Community-backed implementation of the TinkerPop VertexProperty interface
 *
 * @param <V> Type of the vertex property value
 */
@SuppressWarnings("all")
public class CommunityVertexProperty<V> extends CommunityElement implements VertexProperty<V> {

    private Map<String, Property<?>> properties;  // Meta-properties cache (thread-safe)
    private final CommunityVertex vertex;            // Parent vertex instance
    private final String key;                    // Property key
    private final V value;                             // Property value
    private final boolean allowNullPropertyValues;  // Null value support flag

    /**
     * Create a new vertex property with optional meta-properties
     *
     * @param id Unique identifier for the vertex property
     * @param vertex Parent vertex this property belongs to
     * @param key Property key (serves as label for the VertexProperty)
     * @param value Property value (validated against null value support)
     * @param propertyKeyValues Optional meta-property key-value pairs
     */
    public CommunityVertexProperty(final Object id, final CommunityVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, vertex.graph);  // Use property key as element label
        this.allowNullPropertyValues = vertex.graph.features().vertex().properties().supportsNullPropertyValues();

        // Validate null value against graph feature configuration
        if (!allowNullPropertyValues && null == value) {
            throw new IllegalArgumentException("Null property values are not supported (supportsNullPropertyValues=false)");
        }

        this.vertex = vertex;
        this.key = key;
        this.value = value;

        // Initialize meta-properties from key-value pairs
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    /**
     * Get the property key
     *
     * @return Property key string
     */
    @Override
    public String key() {
        return key;
    }

    /**
     * Get the property value
     *
     * @return Current property value
     */
    @Override
    public V value() {
        return value;
    }

    /**
     * Check if the property is present (always true for this implementation)
     * This implementation maintains property instances only for existing properties
     *
     * @return Always returns true
     */
    @Override
    public boolean isPresent() {
        return true;
    }

    /**
     * Get the parent vertex for this property
     *
     * @return Parent RocksVertex instance
     */
    @Override
    public Vertex element() {
        return vertex;
    }

    /**
     * Remove this vertex property and its meta-properties
     * Removes the property from the parent vertex and persists changes to RocksDB
     */
    @Override
    public void remove() {
        this.vertex.removeProperty(key);
    }

    /**
     * Get a meta-property by key
     *
     * @param key Meta-property key
     * @param <U> Type of the meta-property value
     * @return Meta-property instance (empty if not found)
     */
    @Override
    public <U> Property<U> property(final String key) {
        return properties == null ? Property.empty() :
                (Property<U>) properties.getOrDefault(key, Property.empty());
    }

    /**
     * Set a meta-property with validation against null value support
     *
     * @param key Meta-property key
     * @param value Meta-property value
     * @param <U> Type of the meta-property value
     * @return Created/updated meta-property (empty if null value not supported)
     */
    @Override
    public <U> Property<U> property(final String key, final U value) {
        // Handle null values according to graph feature configuration
        if (!allowNullPropertyValues && value == null) {
            property(key).remove();
            return Property.empty();
        }

        // Create and cache meta-property
        final CommunityProperty<U> property = new CommunityProperty<>(this, key, value, graph);
        if (properties == null) {
            properties = new ConcurrentHashMap<>();
        }
        properties.put(key, property);

        // Persist changes to parent vertex
        this.vertex.putVertexData(vertex.graph.handle(), vertex.handle(), vertex.serialize());
        return property;
    }

    /**
     * Get meta-properties (filtered by keys if specified)
     *
     * @param propertyKeys Optional meta-property keys to filter
     * @param <U> Type of the meta-property values
     * @return Iterator of meta-property instances
     */
    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        // Create base stream with optional key filtering
        Stream<Property<?>> baseStream = propertyKeys.length == 0
                ? properties.values().stream()
                : properties.entrySet().stream()
                .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                .map(Map.Entry::getValue);

        // Safely cast to target type (unchecked warning suppressed as logically safe)
        @SuppressWarnings("unchecked")
        Stream<Property<U>> targetStream = baseStream.map(prop -> (Property<U>) prop);

        return targetStream.iterator();
    }

    /**
     * Get all meta-property keys for this vertex property
     *
     * @return Set of meta-property keys (empty if no meta-properties)
     */
    @Override
    public Set<String> keys() {
        return properties == null ? Collections.emptySet() : properties.keySet();
    }

    /**
     * Get standardized string representation (compliant with TinkerPop specs)
     *
     * @return Formatted vertex property string
     */
    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    /**
     * Check equality using TinkerPop's standard vertex property equality logic
     * Compares parent vertex, key, value, and meta-properties for equality
     *
     * @param object Object to compare with
     * @return True if vertex properties are equal, false otherwise
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Generate hash code using TinkerPop's standard element hashing logic
     * Ensures consistency with equality contract
     *
     * @return Hash code integer
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    /**
     * Encode vertex property ID to LSM-Community storage key format
     * Key format: "VP|{vertexId}|{vpId}"
     *
     * @return Byte array representing LSM-Community storage key
     */
    @Override
    protected byte[] encodeKey() {
        return ("VP|" + vertex.id() + "|" + id()).getBytes();
    }
}