package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java-side phi predicate, invoked from C++ via JNI during backtracking.
 *
 * coords[i] = 0-based data-graph index of the vertex bound to
 * the i-th quantifier variable (same order as conditions list).
 */
public class SubgraphPhiCallback {

    private final GraphTraversalSource g;
    private final List<Map.Entry<String, String>> conditions;
    private final Map<Integer, Vertex> indexToVertex;

    /**
     * Pre-parsed AST — replaces the non-existent compileFilter().
     * parseLogicalExpression() already compiles every leaf Groovy Script
     * at parse time, so this is compiled exactly once.
     */
    private final GroovyGremlinQueryExecutor.LogicalNode ast;

    public SubgraphPhiCallback(GraphTraversalSource g,
                               List<Map.Entry<String, String>> conditions,
                               Map<Integer, Vertex> indexToVertex,
                               String filterQuery) {
        this.g             = g;
        this.conditions    = conditions;
        this.indexToVertex = indexToVertex;
        // parseLogicalExpression already compiles Groovy scripts inside
        // LeafNode — zero re-compilation on every evaluate() call
        this.ast = GroovyGremlinQueryExecutor.parseLogicalExpression(filterQuery);
    }

    /**
     * Called from C++ via JNI.
     * Builds the variable-binding map and delegates to LogicalNode.evaluate().
     */
    public boolean evaluate(int[] coords) {
        Map<String, Object> variables = new HashMap<>(conditions.size() + 2);
        // g must be in scope for Gremlin traversal expressions like g.V(x)...
        variables.put("g", g);

        for (int i = 0; i < conditions.size(); i++) {
            String varName = conditions.get(i).getKey();
            Vertex vertex  = indexToVertex.get(coords[i]);
            if (vertex == null) return false;   // unknown index → predicate fails
            // Bind the vertex ID so that g.V(x) resolves correctly in Groovy
            variables.put(varName, vertex);
        }

//        System.out.println(Arrays.toString(coords));
//        System.out.println(ast.evaluate(variables));
        return ast.evaluate(variables);
    }
}