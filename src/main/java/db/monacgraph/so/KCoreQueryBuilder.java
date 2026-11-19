package db.monacgraph.so;

import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Computes the k-core of the graph or of a specified induced subgraph.
 */
public final class KCoreQueryBuilder {

    private final GraphTraversalSource g;
    private Collection<? extends Vertex> candidateVertices;
    private int k;

    public KCoreQueryBuilder(GraphTraversalSource g) {
        this.g = Objects.requireNonNull(
                g,
                "Traversal source must not be null"
        );
    }

    /**
     * Restricts the computation to the induced subgraph of vertices.
     */
    public KCoreQueryBuilder on(
            Collection<? extends Vertex> vertices
    ) {
        this.candidateVertices = Objects.requireNonNull(
                vertices,
                "Candidate vertices must not be null"
        );
        return this;
    }

    /**
     * Sets the minimum internal degree.
     */
    public KCoreQueryBuilder k(int k) {
        if (k < 0) {
            throw new IllegalArgumentException(
                    "K-core parameter must be non-negative"
            );
        }

        this.k = k;
        return this;
    }

    /**
     * Returns the vertices in the maximum k-core.
     */
    public Set<Vertex> execute() {
        List<Vertex> vertices = loadCandidateVertices();

        Map<Object, Vertex> vertexById = new LinkedHashMap<>();
        for (Vertex vertex : vertices) {
            vertexById.put(vertex.id(), vertex);
        }

        if (vertexById.isEmpty()) {
            return new LinkedHashSet<>();
        }

        Map<Object, Set<Object>> neighborsById = new LinkedHashMap<>();

        for (Vertex vertex : vertices) {
            Set<Object> neighborIds = new LinkedHashSet<>();

            var iterator = vertex.vertices(Direction.BOTH);
            while (iterator.hasNext()) {
                Vertex neighbor = iterator.next();

                if (vertexById.containsKey(neighbor.id())) {
                    neighborIds.add(neighbor.id());
                }
            }

            neighborsById.put(vertex.id(), neighborIds);
        }

        Map<Object, Integer> degreeById = new LinkedHashMap<>();
        for (Map.Entry<Object, Set<Object>> entry
                : neighborsById.entrySet()) {
            degreeById.put(entry.getKey(), entry.getValue().size());
        }

        Set<Object> activeIds = new LinkedHashSet<>(vertexById.keySet());
        Deque<Object> removalQueue = new ArrayDeque<>();

        for (Map.Entry<Object, Integer> entry
                : degreeById.entrySet()) {
            if (entry.getValue() < k) {
                removalQueue.addLast(entry.getKey());
            }
        }

        while (!removalQueue.isEmpty()) {
            Object removedId = removalQueue.removeFirst();

            if (!activeIds.remove(removedId)) {
                continue;
            }

            for (Object neighborId : neighborsById.get(removedId)) {
                if (!activeIds.contains(neighborId)) {
                    continue;
                }

                int newDegree = degreeById.get(neighborId) - 1;
                degreeById.put(neighborId, newDegree);

                if (newDegree < k) {
                    removalQueue.addLast(neighborId);
                }
            }
        }

        Set<Vertex> result = new LinkedHashSet<>();
        for (Vertex vertex : vertices) {
            if (activeIds.contains(vertex.id())) {
                result.add(vertex);
            }
        }

        return result;
    }

    /**
     * Serializes the k-core as one induced subgraph for the web UI.
     */
    public Map<String, Object> executeForWeb() {
        Set<Vertex> vertices = execute();

        return VsetResultSerializer.serialize(
                ResultSubgraph.induced(vertices)
        );
    }

    private List<Vertex> loadCandidateVertices() {
        if (candidateVertices == null) {
            return new ArrayList<>(g.V().toList());
        }

        return new ArrayList<>(candidateVertices);
    }
}