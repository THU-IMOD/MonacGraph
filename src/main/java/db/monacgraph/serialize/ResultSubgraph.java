package db.monacgraph.serialize;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * One piece of a query result to draw on the Web: a vertex list plus the edges
 * that belong to that piece. Edges need not be the induced subgraph.
 */
public final class ResultSubgraph {
    private final List<Vertex> vertices;
    private final List<Edge> edges;
    private final boolean induced;

    private ResultSubgraph(List<Vertex> vertices, List<Edge> edges, boolean induced) {
        this.vertices = vertices;
        this.edges = edges;
        this.induced = induced;
    }

    /**
     * Draw exactly these vertices and edges (path, match, sparse subgraph, …).
     * Extra edges between the vertices are not added.
     */
    public static ResultSubgraph of(Collection<? extends Vertex> vertices,
                                    Collection<? extends Edge> edges) {
        return new ResultSubgraph(copyVertices(vertices), copyEdges(edges), false);
    }

    /**
     * Draw these vertices and every edge whose both ends lie in the set
     * (induced subgraph). Used by BFS / WCC / Community / Vset.
     */
    public static ResultSubgraph induced(Collection<? extends Vertex> vertices) {
        return new ResultSubgraph(copyVertices(vertices), List.of(), true);
    }

    /**
     * One induced piece per vertex set (WCC / Community / Vset).
     */
    public static List<ResultSubgraph> inducedAll(
            Iterable<? extends Collection<? extends Vertex>> pieces) {
        List<ResultSubgraph> subgraphs = new ArrayList<>();
        if (pieces == null) {
            return subgraphs;
        }
        for (Collection<? extends Vertex> vertices : pieces) {
            subgraphs.add(induced(vertices));
        }
        return subgraphs;
    }

    List<Vertex> vertices() {
        return vertices;
    }

    List<Edge> edges() {
        return edges;
    }

    boolean induced() {
        return induced;
    }

    private static List<Vertex> copyVertices(Collection<? extends Vertex> vertices) {
        if (vertices == null || vertices.isEmpty()) {
            return List.of();
        }
        return Collections.unmodifiableList(new ArrayList<>(vertices));
    }

    private static List<Edge> copyEdges(Collection<? extends Edge> edges) {
        if (edges == null || edges.isEmpty()) {
            return List.of();
        }
        return Collections.unmodifiableList(new ArrayList<>(edges));
    }
}
