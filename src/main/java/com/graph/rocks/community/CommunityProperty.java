package com.graph.rocks.community;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * LSM-Community-backed implementation of the TinkerPop Property interface
 * Manages property storage and lifecycle for CommunityVertex and CommunityEdge elements
 *
 * @param <V> Type of the property value
 */
@SuppressWarnings("all")
public class CommunityProperty<V> implements Property<V> {

    private final CommunityElement element;  // Parent graph element (vertex/edge)
    private final String key;            // Property key (unique per element)
    private final V value;                     // Cached property value
    private final CommunityGraph graph;      // Parent graph instance for storage operations

    /**
     * Create a new property for a LSM-Community graph element
     *
     * @param element Parent graph element (CommunityVertex/CommunityEdge)
     * @param key Unique property key (validated per TinkerPop specs)
     * @param value Property value (non-null unless explicitly allowed)
     * @param graph Parent graph instance with KV store access
     */
    public CommunityProperty(final CommunityElement element, final String key, final V value, final CommunityGraph graph) {
        ElementHelper.validateProperty(key, value); // Validate key/value per TinkerPop standards
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = graph;
    }

    /**
     * Get the parent graph element (vertex/edge) for this property
     *
     * @return Parent CommunityElement instance
     */
    @Override
    public CommunityElement element() {
        return element;
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
     * Remove this property from its parent element
     * Removes the property from the element's cache and persists the change to RocksDB
     */
    @Override
    public void remove() {
        // Remove property from parent element's cache and persist changes
        if (element instanceof CommunityVertex) {
            ((CommunityVertex) element).removeProperty(key);
        } else if (element instanceof CommunityEdge) {
            ((CommunityEdge) element).removeProperty(key);
        }
    }

    /**
     * Get standardized string representation (compliant with TinkerPop specs)
     *
     * @return Formatted property string
     */
    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    /**
     * Check equality using TinkerPop's standard property equality logic
     * Compares element, key, and value for equality
     *
     * @param object Object to compare with
     * @return True if properties are equal, false otherwise
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Generate hash code using TinkerPop's standard property hashing logic
     * Ensures consistency with equality contract
     *
     * @return Hash code integer
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}