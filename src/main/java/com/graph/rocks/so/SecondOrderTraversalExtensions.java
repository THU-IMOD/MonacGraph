package com.graph.rocks.so;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * Extension methods for GraphTraversalSource to support second-order logic queries
 * Provides fluent, semantic API for building complex vertex subset queries with quantifiers
 */
@SuppressWarnings("unused")
public class SecondOrderTraversalExtensions {

    /**
     * Add second-order query entry point to GraphTraversalSource
     * Initializes a builder for constructing second-order logic graph queries
     *
     * @param g GraphTraversalSource to extend
     * @return SecondOrderQueryBuilder instance for query construction
     */
    public static SecondOrderQueryBuilder secondOrder(GraphTraversalSource g) {
        return SecondOrderQueryBuilder.secondOrder(g); // Reuse existing initialization logic
    }

    /**
     * Semantic shortcut for existential quantifier (∃) query initialization
     * Directly declares an existential variable and returns the query builder
     *
     * @param g GraphTraversalSource to extend
     * @param varName Variable name for existential quantification (∃ varName)
     * @return SecondOrderQueryBuilder with existential variable declared
     */
    public static SecondOrderQueryBuilder exists(GraphTraversalSource g, String varName) {
        return new SecondOrderQueryBuilder(g).exist(varName);
    }

    /**
     * Semantic shortcut for universal quantifier (∀) query initialization
     * Directly declares a universal variable and returns the query builder
     *
     * @param g GraphTraversalSource to extend
     * @param varName Variable name for universal quantification (∀ varName)
     * @return SecondOrderQueryBuilder with universal variable declared
     */
    public static SecondOrderQueryBuilder forAll(GraphTraversalSource g, String varName) {
        return new SecondOrderQueryBuilder(g).forall(varName);
    }
}