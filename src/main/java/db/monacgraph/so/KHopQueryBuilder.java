package db.monacgraph.so;

import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.util.*;

/** Vertices at undirected hop distance <= depth, including the seed. */
public final class KHopQueryBuilder {
    private final GraphTraversalSource g;
    private final Object id;
    private int depth = 1;

    public KHopQueryBuilder(GraphTraversalSource g, Object id) {
        this.g = Objects.requireNonNull(g, "Traversal source must not be null");
        this.id = Objects.requireNonNull(id, "Seed ID must not be null");
    }

    public KHopQueryBuilder depth(int k) {
        if (k < 0) throw new IllegalArgumentException("Depth must be non-negative");
        this.depth = k;
        return this;
    }

    /** Missing seeds throw NoSuchElementException, as with g.V(id).next(). */
    public Set<Vertex> execute() {
        Vertex seed = g.V(id).next();
        Map<Object, Vertex> visited = new LinkedHashMap<>();
        Deque<Vertex> frontier = new ArrayDeque<>();
        visited.put(seed.id(), seed);
        frontier.add(seed);
        for (int level = 0; level < depth && !frontier.isEmpty(); level++) {
            int levelSize = frontier.size();
            for (int i = 0; i < levelSize; i++) {
                Iterator<Vertex> neighbors = frontier.remove().vertices(Direction.BOTH);
                while (neighbors.hasNext()) {
                    Vertex neighbor = neighbors.next();
                    if (!visited.containsKey(neighbor.id())) {
                        visited.put(neighbor.id(), neighbor);
                        frontier.add(neighbor);
                    }
                }
            }
        }
        return new LinkedHashSet<>(visited.values());
    }

    /** Existing serializer includes all original edges between selected vertices. */
    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serialize(Collections.singleton(execute()));
    }
}