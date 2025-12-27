package com.graph.rocks.community;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * Abstract base class for LSM-Community graph elements (vertices/edges)
 * Implements core TinkerPop Element interface functionality with LSM-Community storage optimizations
 */
@SuppressWarnings("all")
public abstract class CommunityElement implements Element {

    protected Object id;               // Unique element identifier (maps to LSM-Community storage key)
    protected String label;            // Element type/classification label
    protected final CommunityGraph graph;  // Parent graph instance

    /**
     * Initialize core element properties with TinkerPop-compliant validation
     * @param id Unique identifier for the element (non-null)
     * @param label Element type label (validated against TinkerPop specifications)
     * @param graph Parent CommunityGraph instance
     */
    protected CommunityElement(final Object id, final String label, final CommunityGraph graph) {
        ElementHelper.validateLabel(label);
        this.id = id;
        this.label = label;
        this.graph = graph;
    }

    /**
     * Get the unique identifier of the element
     * @return Element's unique ID
     */
    @Override
    public Object id() {
        return this.id;
    }

    /**
     * Get the type/classification label of the element
     * @return Element label string
     */
    @Override
    public String label() {
        return this.label;
    }

    /**
     * Generate hash code using TinkerPop's standard element hashing implementation
     * Ensures consistency with TinkerPop's equality contract
     * @return Hash code integer
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    /**
     * Check element equality per TinkerPop's standard implementation
     * Compares element type, ID, and graph membership
     * @param object Object to compare against
     * @return True if elements are equal, false otherwise
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Encode element ID to LSM-Community-compatible storage key format
     * Must be implemented by subclasses to provide vertex/edge specific key encoding
     * @return Byte array representing the LSM-Community storage key
     */
    protected abstract byte[] encodeKey();

    /**
     * Create standardized exception for operations on removed elements
     * @param clazz Element class (Vertex/Edge) for exception message context
     * @param id ID of the removed element
     * @return IllegalStateException with consistent error message format
     */
    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }
}