package db.monacgraph.so;

import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Preserves the supplied vertices and all original edges between them. */
public final class InducedQueryBuilder {
    private final Map<Object, Vertex> vertices = new LinkedHashMap<>();

    public InducedQueryBuilder(Collection<? extends Vertex> vertices) {
        Objects.requireNonNull(vertices, "Vertex collection must not be null");
        for (Vertex vertex : vertices) {
            Objects.requireNonNull(vertex, "Vertex must not be null");
            this.vertices.putIfAbsent(vertex.id(), vertex);
        }
    }

    public ResultSubgraph execute() {
        Map<Object, Edge> edges = new LinkedHashMap<>();
        for (Vertex vertex : vertices.values()) {
            Iterator<Edge> outgoing = vertex.edges(Direction.OUT);
            while (outgoing.hasNext()) {
                Edge edge = outgoing.next();
                if (vertices.containsKey(edge.inVertex().id())) {
                    edges.putIfAbsent(edge.id(), edge);
                }
            }
        }
        return ResultSubgraph.of(vertices.values(), edges.values());
    }

    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serializeSubgraphs(List.of(execute()));
    }
}
