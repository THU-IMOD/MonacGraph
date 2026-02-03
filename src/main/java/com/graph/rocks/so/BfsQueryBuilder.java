package com.graph.rocks.so;

import com.graph.rocks.RustJNI;
import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.community.CommunityVertex;
import com.graph.rocks.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class BfsQueryBuilder {
    private final GraphTraversalSource g;
    private final Vertex vertex;

    /**
     * Constructs a BfsQueryBuilder with the given traversal source and starting vertex ID.
     * @param g the graph traversal source
     * @param id the ID of the BFS starting vertex
     */
    public BfsQueryBuilder(GraphTraversalSource g, Object id) {
        this.g = g;
        this.vertex = g.V(id).next();
    }

    /**
     * Factory method for creating a SecondOrderQueryBuilder (placeholder).
     * @param g the graph traversal source
     * @return a new SecondOrderQueryBuilder instance
     */
    public static SecondOrderQueryBuilder secondOrder(GraphTraversalSource g) {
        return new SecondOrderQueryBuilder(g);
    }

    /**
     * Executes BFS via native RustJNI and returns all reachable vertices.
     * @return a set of vertices reachable from the starting vertex
     */
    public Set<Vertex> execute() {
        RustJNI jni = new RustJNI();
        CommunityGraph graph = (CommunityGraph) g.getGraph();
        long graphHandle = graph.handle();
        long vertexHandle = ((CommunityVertex)vertex).handle();
        long[] bfsAnswer = jni.getBfsVertices(graphHandle, vertexHandle);
        int len = bfsAnswer.length;
        Set<Vertex> answer = new HashSet<>();
        for (int i = 0; i < len; i += 2) {
            answer.add(new CommunityVertex(graph, bfsAnswer[i]));
        }
        return answer;
    }

    /**
     * Executes BFS and returns serialized results formatted for web visualization.
     * @return serialized BFS results in a web-friendly map structure
     */
    public Map<String, Object> executeForWeb() {
        Set<Vertex> answer = execute();
        Set<Set<Vertex>> result = new HashSet<>();
        result.add(answer);
        return VsetResultSerializer.serialize(result);
    }
}