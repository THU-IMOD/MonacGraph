package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static db.monacgraph.so.GroovyGremlinQueryExecutor.VsetQuery;
import db.monacgraph.serialize.VsetResultSerializer;

/**
 * Builder pattern implementation for second-order logic vertex subset queries
 * Constructs and executes complex vertex set queries using existential/universal quantifiers
 * and Gremlin filter conditions
 */
@SuppressWarnings("unused")
public class VertexSubsetQueryBuilder {
    private final GraphTraversalSource g;
    private final List<Map.Entry<String, String>> conditions = new ArrayList<>();
    private String filterQuery = "true";
    private String aggregationQuery = "true";

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
    public static SecondOrderQueryBuilder SecondOrder(GraphTraversalSource g) {
        return new SecondOrderQueryBuilder(g);
    }

    public VertexSubsetQueryBuilder having(String aggregationCondition) {
        this.aggregationQuery = aggregationCondition;
        return this;
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
        return VsetQuery(g, filterQuery, aggregationQuery, conditions);
    }

    /**
     * Execute the query and automatically serialize for web visualization
     * No need to manually call VsetResultSerializer.serialize()
     *
     * This method is perfect for web demos:
     * - Automatically formats result for frontend
     * - Includes vertex properties
     * - Returns JSON-friendly format
     *
     * Example usage in Groovy:
     * <pre>
     * result = g.Vset()
     *   .forall('x')
     *   .forall('y')
     *   .filter('g.V(x).out("knows").is(y) || g.V(y).out("knows").is(x) || g.V(x).is(y)')
     *   .executeForWeb()  // <-- No import or serialize() needed!
     * </pre>
     *
     * @return Map containing serialized Vset result ready for web display
     * @throws IllegalArgumentException If query conditions are incomplete
     */
    public Map<String, Object> executeForWeb() {
        Set<Set<Vertex>> result = execute();
        return VsetResultSerializer.serialize(result);
    }
}