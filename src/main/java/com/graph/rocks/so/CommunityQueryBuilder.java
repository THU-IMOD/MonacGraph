package com.graph.rocks.so;

import com.graph.rocks.RustJNI;
import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.community.CommunityVertex;
import com.graph.rocks.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

import static com.graph.rocks.so.GroovyGremlinQueryExecutor.CommunityQuery;
import static com.graph.rocks.so.GroovyGremlinQueryExecutor.VsetQuery;
import com.graph.rocks.serialize.VsetResultSerializer;
import com.graph.rocks.so.CommunityType;

/**
 * Builder pattern implementation for second-order logic vertex subset queries
 * Constructs and executes complex vertex set queries using existential/universal quantifiers
 * and Gremlin filter conditions
 */
@SuppressWarnings("unused")
public class CommunityQueryBuilder {
    private final GraphTraversalSource g;
    private final List<Map.Entry<String, String>> conditions = new ArrayList<>();
    private String filterQuery = "true";
    private String aggregationQuery = "true";
    private CommunityType type = CommunityType.COMMUNITY;

    /**
     * Constructs a CommunityQueryBuilder with the given traversal source and community type.
     * @param g the graph traversal source
     * @param type the community detection type
     */
    public CommunityQueryBuilder(GraphTraversalSource g, CommunityType type) {
        this.g = g;
        this.type = type;
    }

    /**
     * Factory method for creating a SecondOrderQueryBuilder.
     * @param g the graph traversal source
     * @return a new SecondOrderQueryBuilder instance
     */
    public static SecondOrderQueryBuilder SecondOrder(GraphTraversalSource g) {
        return new SecondOrderQueryBuilder(g);
    }

    /**
     * Sets the aggregation condition for the community query.
     * @param aggregationCondition the aggregation condition
     * @return the current CommunityQueryBuilder instance
     */
    public CommunityQueryBuilder having(String aggregationCondition) {
        this.aggregationQuery = aggregationCondition;
        return this;
    }

    /**
     * Adds an existential quantifier condition for the specified variable.
     * @param varName the variable name
     * @return the current CommunityQueryBuilder instance
     */
    public CommunityQueryBuilder exist(String varName) {
        conditions.add(Map.entry(varName, "exist"));
        return this;
    }

    /**
     * Adds a universal quantifier condition for the specified variable.
     * @param varName the variable name
     * @return the current CommunityQueryBuilder instance
     */
    public CommunityQueryBuilder forall(String varName) {
        conditions.add(Map.entry(varName, "forall"));
        return this;
    }

    /**
     * Sets the Gremlin filter query for vertex subset filtering.
     * @param gremlinQuery the Gremlin filter query
     * @return the current CommunityQueryBuilder instance
     */
    public CommunityQueryBuilder filter(String gremlinQuery) {
        this.filterQuery = gremlinQuery;
        return this;
    }

    /**
     * Executes the community query and returns the resulting vertex subsets.
     * @return a set of vertex subsets (communities) matching the query conditions
     */
    public Set<Set<Vertex>> execute() {
        Set<Set<Vertex>> communities = getCommunities();
        return CommunityQuery(g, filterQuery, aggregationQuery, conditions, communities);
    }

    /**
     * Executes the community query and returns serialized results for web visualization.
     * @return serialized community query results in a web-friendly format
     */
    public Map<String, Object> executeForWeb() {
        Set<Set<Vertex>> result = execute();
        return VsetResultSerializer.serialize(result);
    }

    /**
     * Retrieves communities from the native storage engine based on the community type.
     * @return a set of vertex subsets representing communities
     */
    public Set<Set<Vertex>> getCommunities() {
        RustJNI jni = new RustJNI();
        CommunityGraph graph = (CommunityGraph) g.getGraph();
        long graphHandle = graph.handle();
        long[] communityList = new long[0];
        switch (type) {
            case COMMUNITY:
                communityList = jni.getCommunities(graphHandle);
            case WCC:
                communityList = jni.getWCC(graphHandle);
                break;
            case SCC:
                communityList = jni.getSCC(graphHandle);
                break;
        }
        Set<Set<Vertex>> communities = new HashSet<>();
        int len = communityList.length;
        int k = (int)communityList[0];
        Set<Vertex>[] communityArray = new Set[k];
        for (int i = 0; i < k; i++) {
            communityArray[i] = new HashSet<>();
        }
        for (int i = 1; i < len; i+=2) {
            long vertexHandle = communityList[i];
            int communityId = (int) communityList[i + 1];
            communityArray[communityId].add(new CommunityVertex(graph, vertexHandle));
        }
        for (int i = 0; i < k; i++) {
            communities.add(communityArray[i]);
        }
        return communities;
    }
}