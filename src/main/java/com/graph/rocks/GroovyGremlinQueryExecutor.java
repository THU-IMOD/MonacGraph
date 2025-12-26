package com.graph.rocks;

import groovy.lang.GroovyShell;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Executes second-order logic Gremlin queries using Groovy evaluation
 * Provides core functionality for evaluating complex logical conditions
 * with existential/universal quantifiers over vertex sets
 */
public class GroovyGremlinQueryExecutor {

    /**
     * Execute a Gremlin query string using GroovyShell with variable bindings
     * Handles conversion of traversal results to concrete collections
     *
     * @param groovyQuery Gremlin query string to execute
     * @param variables Map of variable names to their bound objects
     * @return Query result (List for traversals/iterables, raw value for primitives, null on failure)
     */
    public static Object executeGremlinQuery(String groovyQuery, Map<String, Object> variables) {
        GroovyShell shell = new GroovyShell();

        // Bind variables to Groovy execution context
        for (Map.Entry<String, Object> entry : variables.entrySet()) {
            shell.setVariable(entry.getKey(), entry.getValue());
        }

        try {
            Object result = shell.evaluate(groovyQuery);

            if (result == null) return null;

            // Convert GraphTraversal results to List
            if (result instanceof org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal) {
                return ((org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal) result).toList();
            }

            // Convert Iterable results to List
            if (result instanceof Iterable) {
                List<?> resultList = (List<?>) result;
                return resultList;
            }

            // Return primitive boolean values directly
            if (result instanceof Boolean) {
                return result;
            }

            // Wrap single values in List for consistency
            return Collections.singletonList(result);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Recursively enumerate vertex combinations for second-order logic evaluation
     * Implements existential (∃) and universal (∀) quantifier logic over vertex sets
     *
     * @param g GraphTraversalSource for query execution
     * @param vertices Set of vertices to enumerate
     * @param variables Current variable bindings
     * @param groovyQuery Gremlin condition to evaluate
     * @param conditions List of quantifier conditions (varName → "exist"/"forall")
     * @param index Current condition index in recursion
     * @return Boolean result of logical evaluation for current variable binding
     */
    private static boolean enumerateSecondOrder(
            GraphTraversalSource g,
            Set<Vertex> vertices,
            Map<String, Object> variables,
            String groovyQuery,
            List<Map.Entry<String, String>> conditions,
            int index) {

        // Base case: all variables bound - evaluate the query
        if (index >= conditions.size()) {
            Object result = executeGremlinQuery(groovyQuery, variables);
            return result != null && !((List<?>) result).isEmpty();
        }

        Map.Entry<String, String> condition = conditions.get(index);
        String varName = condition.getKey();
        String quantifier = condition.getValue();

        // Recursively bind each vertex to the current quantifier variable
        for (Vertex vertex : vertices) {
            variables.put(varName, vertex);
            boolean evaluationResult = enumerateSecondOrder(g, vertices, variables, groovyQuery, conditions, index + 1);
            variables.remove(varName);

            // Short-circuit evaluation for quantifiers
            if ("exist".equals(quantifier) && evaluationResult) return true;
            if ("forall".equals(quantifier) && !evaluationResult) return false;
        }

        // Final result based on quantifier type when loop completes
        return !"exist".equals(quantifier);
    }

    /**
     * Evaluate second-order logical condition over a selected vertex set
     * Implements (∃ var1)(∀ var2)... S(var1, var2, ...) logic
     *
     * @param g GraphTraversalSource for query execution
     * @param selectedVertices Vertex set to evaluate against
     * @param groovyQuery Gremlin condition to evaluate
     * @param conditions List of quantifier conditions (varName → "exist"/"forall")
     * @return True if logical condition is satisfied, false otherwise
     */
    public static boolean evaluateGremlinQueryWithConditions(
            GraphTraversalSource g,
            Set<Vertex> selectedVertices,
            String groovyQuery,
            List<Map.Entry<String, String>> conditions) {

        Map<String, Object> variables = new HashMap<>();
        variables.put("g", g); // Bind traversal source to Groovy context

        return enumerateSecondOrder(g, selectedVertices, variables, groovyQuery, conditions, 0);
    }

    /**
     * Recursively generate all possible vertex subsets and evaluate logical condition
     * Builds power set of vertices and filters by second-order logic condition
     *
     * @param g GraphTraversalSource for query execution
     * @param vertices Full list of vertices to generate subsets from
     * @param selectedVertices Current subset being built
     * @param groovyQuery Gremlin condition to evaluate
     * @param conditions List of quantifier conditions
     * @param subsets Result set to collect valid vertex subsets
     * @param index Current vertex index in recursion
     */
    private static void enumerateVset(
            GraphTraversalSource g,
            List<Vertex> vertices,
            Set<Vertex> selectedVertices,
            String groovyQuery,
            List<Map.Entry<String, String>> conditions,
            Set<Set<Vertex>> subsets,
            int index) {

        // Base case: full subset built - evaluate condition
        if (index >= vertices.size()) {
            if (evaluateGremlinQueryWithConditions(g, selectedVertices, groovyQuery, conditions)) {
                subsets.add(new HashSet<>(selectedVertices));
            }
            return;
        }

        Vertex vertex = vertices.get(index);

        // Include current vertex in subset
        selectedVertices.add(vertex);
        enumerateVset(g, vertices, selectedVertices, groovyQuery, conditions, subsets, index + 1);

        // Exclude current vertex from subset
        selectedVertices.remove(vertex);
        enumerateVset(g, vertices, selectedVertices, groovyQuery, conditions, subsets, index + 1);
    }

    /**
     * Find all vertex subsets that satisfy the second-order logical condition
     * Generates power set of all vertices and filters by logical condition
     *
     * @param g GraphTraversalSource for query execution
     * @param groovyQuery Gremlin condition to evaluate
     * @param conditions List of quantifier conditions
     * @return Set of valid vertex subsets satisfying the logical condition
     */
    public static Set<Set<Vertex>> VsetQuery(
            GraphTraversalSource g,
            String groovyQuery,
            List<Map.Entry<String, String>> conditions) {

        Set<Set<Vertex>> validSubsets = new HashSet<>();
        Set<Vertex> currentSubset = new HashSet<>();
        List<Vertex> allVertices = g.V().toList();

        enumerateVset(g, allVertices, currentSubset, groovyQuery, conditions, validSubsets, 0);
        return validSubsets;
    }
}