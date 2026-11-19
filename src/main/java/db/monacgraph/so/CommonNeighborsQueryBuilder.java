package db.monacgraph.so;

import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Finds the shared one-hop neighbors of two vertices, ignoring edge direction. */
public final class CommonNeighborsQueryBuilder {
    private final GraphTraversalSource g;
    private final Object sourceId;
    private final Object targetId;

    public CommonNeighborsQueryBuilder(GraphTraversalSource g, Object sourceId, Object targetId) {
        this.g = Objects.requireNonNull(g, "Traversal source must not be null");
        this.sourceId = Objects.requireNonNull(sourceId, "Source ID must not be null");
        this.targetId = Objects.requireNonNull(targetId, "Target ID must not be null");
    }

    public Set<Vertex> execute() {
        Vertex source = g.V(sourceId).next();
        Vertex target = g.V(targetId).next();

        Set<Object> sourceNeighborIds = new HashSet<>();
        source.vertices(Direction.BOTH).forEachRemaining(vertex -> sourceNeighborIds.add(vertex.id()));
        sourceNeighborIds.remove(source.id());
        sourceNeighborIds.remove(target.id());

        Map<Object, Vertex> sharedNeighbors = new LinkedHashMap<>();
        Iterator<Vertex> targetNeighbors = target.vertices(Direction.BOTH);
        while (targetNeighbors.hasNext()) {
            Vertex neighbor = targetNeighbors.next();
            if (sourceNeighborIds.contains(neighbor.id())) {
                sharedNeighbors.putIfAbsent(neighbor.id(), neighbor);
            }
        }

        return new LinkedHashSet<>(sharedNeighbors.values());
    }

    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serializeSubgraphs(List.of(ResultSubgraph.induced(execute())));
    }
}
