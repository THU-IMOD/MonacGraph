package com.graph.rocks.so;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Builder pattern implementation for second-order logic graph queries
 * Constructs and executes logical queries using existential/universal quantifiers
 * with Gremlin filter conditions against vertex sets
 */
public class SecondOrderQueryBuilder {
    private final GraphTraversalSource g;
    private final List<Map.Entry<String, String>> conditions = new ArrayList<>();
    private String filterQuery;

    /**
     * Create a new SecondOrderQueryBuilder instance
     * @param g GraphTraversalSource for executing Gremlin queries
     */
    public SecondOrderQueryBuilder(GraphTraversalSource g) {
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
    public SecondOrderQueryBuilder exist(String varName) {
        conditions.add(Map.entry(varName, "exist"));
        return this;
    }

    /**
     * Declare a universal quantifier variable (∀ varName)
     * @param varName Variable name for universal quantification
     * @return This builder instance (fluent API)
     */
    public SecondOrderQueryBuilder forall(String varName) {
        conditions.add(Map.entry(varName, "forall"));
        return this;
    }

    /**
     * Set the Gremlin filter condition for the second-order logic query
     * @param gremlinQuery Gremlin query string representing the logical condition
     * @return This builder instance (fluent API)
     */
    public SecondOrderQueryBuilder filter(String gremlinQuery) {
        this.filterQuery = gremlinQuery;
        return this;
    }

    /**
     * Execute the second-order logic query against all vertices in the graph
     * Validates required conditions before execution
     * @return Boolean result of the logical query evaluation
     * @throws IllegalArgumentException If query conditions are incomplete
     */
    public boolean execute() {
        if (filterQuery == null || conditions.isEmpty()) {
            throw new IllegalArgumentException("Incomplete query conditions - filter and quantifier variables required");
        }
        return GroovyGremlinQueryExecutor.evaluateGremlinQueryWithConditions(
                g,
                new HashSet<>(g.V().toList()),
                filterQuery,
                conditions
        );
    }
}