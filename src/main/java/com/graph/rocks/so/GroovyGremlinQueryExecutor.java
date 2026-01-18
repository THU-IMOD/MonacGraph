package com.graph.rocks.so;

import groovy.lang.GroovyShell;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Executes second-order logic Gremlin queries using Groovy evaluation
 * Provides core functionality for evaluating complex logical conditions
 * with existential/universal quantifiers over vertex sets
 *
 * Now supports manual parsing of logical expressions (||, &&, !) to ensure
 * all results are boolean type
 */
@SuppressWarnings("all")
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
                List<Object> resultList = new ArrayList<>();
                for (Object item : (Iterable<?>) result) {
                    resultList.add(item);
                }
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
     * Manually parses and evaluates logical expressions
     * Supports ||, &&, ! operators and parentheses
     *
     * @param expression Logical expression string
     * @param variables Variable bindings
     * @return Boolean result of the expression evaluation
     */
    private static boolean evaluateLogicalExpression(String expression, Map<String, Object> variables) {
        expression = expression.trim();

        // Handle parentheses (recursive parsing)
        while (true) {
            String nextExpression = evaluateParentheses(expression, variables);
            if (nextExpression == expression) {
                break;
            }
            expression = nextExpression;
        }

        // Handle || operator (lowest priority)
        List<String> orParts = splitByOperator(expression, "||");
        if (orParts.size() > 1) {
            for (String part : orParts) {
                if (evaluateLogicalExpression(part.trim(), variables)) {
                    return true;  // Short-circuit evaluation
                }
            }
            return false;
        }

        // Handle && operator (medium priority)
        List<String> andParts = splitByOperator(expression, "&&");
        if (andParts.size() > 1) {
            for (String part : andParts) {
                if (!evaluateLogicalExpression(part.trim(), variables)) {
                    return false;  // Short-circuit evaluation
                }
            }
            return true;
        }

        // Handle ! operator (highest priority)
        if (expression.trim().startsWith("!")) {
            String innerExpr = expression.trim().substring(1).trim();
            return !evaluateLogicalExpression(innerExpr, variables);
        }

        // Basic expression: execute query and convert to boolean value
        return evaluateBasicExpression(expression, variables);
    }

    /**
     * Processes expressions within parentheses
     *
     * @param expression Expression containing parentheses
     * @param variables Variable bindings
     * @return New expression with parentheses content replaced by evaluation result
     */
    private static String evaluateParentheses(String expression, Map<String, Object> variables) {
        // Find the innermost parentheses
        int openIndex = -1;
        int closeIndex = -1;
        int len = expression.length();

        for (int i = len - 1; i >= 0 ; i--) {
            if (expression.charAt(i) == '(') {
                if (i == 0) {
                    openIndex = i;
                    break;
                } else {
                    char pre = expression.charAt(i - 1);
                    if (!('a' <= pre && pre <= 'z' || 'A' <= pre && pre <= 'Z' || '0' <= pre && pre <= '9')) {
                        openIndex = i;
                        break;
                    }
                }
            }
        }

        if (openIndex != -1) {
            int badBracket = 0;
            for (int i = openIndex + 1; i < len; i++) {
                if (expression.charAt(i) == '(') {
                    badBracket++;
                }
                if (expression.charAt(i) == ')') {
                    if (badBracket == 0) {
                        closeIndex = i;
                        break;
                    }
                    badBracket--;
                }
            }
        }

        if (openIndex == -1 || closeIndex == -1) {
            return expression;
        }

        // Extract the expression inside the parentheses
        String innerExpr = expression.substring(openIndex + 1, closeIndex);

        // Recursively evaluate the inner expression
        boolean result = evaluateLogicalExpression(innerExpr, variables);

        // Replace the parentheses expression with the result
        String before = expression.substring(0, openIndex);
        String after = expression.substring(closeIndex + 1);

        return before + result + after;
    }

    /**
     * Splits expression by specified operator (considering nested parentheses)
     *
     * @param expression Target expression
     * @param operator Operator to split on ("||" or "&&")
     * @return List of split expression parts
     */
    private static List<String> splitByOperator(String expression, String operator) {
        List<String> parts = new ArrayList<>();
        int parenthesesLevel = 0;
        int lastSplit = 0;

        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);

            if (c == '(') {
                parenthesesLevel++;
            } else if (c == ')') {
                parenthesesLevel--;
            } else if (parenthesesLevel == 0) {
                // Check operator only outside parentheses
                if (i < expression.length() - operator.length() + 1) {
                    String sub = expression.substring(i, i + operator.length());
                    if (sub.equals(operator)) {
                        parts.add(expression.substring(lastSplit, i));
                        lastSplit = i + operator.length();
                        i += operator.length() - 1;  // Skip the operator
                    }
                }
            }
        }

        // Add the last part of the expression
        if (lastSplit < expression.length()) {
            parts.add(expression.substring(lastSplit));
        }

        // Return original expression if no operator found
        if (parts.isEmpty()) {
            parts.add(expression);
        }

        return parts;
    }

    /**
     * Evaluates basic expressions (without logical operators)
     *
     * @param expression Basic expression string
     * @param variables Variable bindings
     * @return Boolean result of the basic expression
     */
    private static boolean evaluateBasicExpression(String expression, Map<String, Object> variables) {
        expression = expression.trim();

        // Return directly if expression is "true" or "false"
        if ("true".equalsIgnoreCase(expression)) {
            return true;
        }
        if ("false".equalsIgnoreCase(expression)) {
            return false;
        }

        // Execute Gremlin query
        Object result = executeGremlinQuery(expression, variables);

        // Convert result to boolean value
        if (result == null) {
            return false;
        }

        if (result instanceof Boolean) {
            return (Boolean) result;
        }

        if (result instanceof List) {
            return !((List<?>) result).isEmpty();
        }

        // Treat other types as true
        return true;
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
            return evaluateLogicalExpression(groovyQuery, variables);
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