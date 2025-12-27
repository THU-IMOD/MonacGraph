package com.graph.rocks;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.graph.rocks.GroovyGremlinQueryExecutor.VsetQuery;

/**
 * Builder pattern implementation for second-order logic vertex subset queries
 * Constructs and executes complex vertex set queries using existential/universal quantifiers
 * and Gremlin filter conditions
 */
public class VertexSubsetQueryBuilder {
    private final GraphTraversalSource g;
    private final List<Map.Entry<String, String>> conditions = new ArrayList<>();
    private String filterQuery;

    /**
     * Create a new VertexSubsetQueryBuilder instance
     * @param g GraphTraversalSource for executing Gremlin queries
     */
    public VertexSubsetQueryBuilder(GraphTraversalSource g) {
        this.g = g;
    }

    /**
     * Static factory method to initialize second-order query builder
     * @param g GraphTraversalSource for query execution
     * @return New SecondOrderQueryBuilder instance
     */
    public static SecondOrderQueryBuilder secondOrder(GraphTraversalSource g) {
        return new SecondOrderQueryBuilder(g);
    }

    /**
     * Declare an existential quantifier variable (∃ varName)
     * @param varName Variable name for existential quantification
     * @return This builder instance (fluent API)
     */
    public VertexSubsetQueryBuilder exist(String varName) {
        conditions.add(Map.entry(varName, "exist"));
        return this;
    }

    /**
     * Declare a universal quantifier variable (∀ varName)
     * @param varName Variable name for universal quantification
     * @return This builder instance (fluent API)
     */
    public VertexSubsetQueryBuilder forall(String varName) {
        conditions.add(Map.entry(varName, "forall"));
        return this;
    }

    /**
     * Set the Gremlin filter condition for the second-order logic query
     * @param gremlinQuery Gremlin query string representing the filter condition
     * @return This builder instance (fluent API)
     */
    public VertexSubsetQueryBuilder filter(String gremlinQuery) {
        this.filterQuery = gremlinQuery;
        return this;
    }

    /**
     * Execute the second-order logic query to get vertex subsets
     * Validates required conditions before execution
     * @return Set of vertex sets matching the second-order logic conditions
     * @throws IllegalArgumentException If query conditions are incomplete
     */
    public Set<Set<Vertex>> execute() {
        if (filterQuery == null || conditions.isEmpty()) {
            throw new IllegalArgumentException("Incomplete query conditions - filter and quantifier variables required");
        }
        return VsetQuery(g, filterQuery, conditions);
    }
}