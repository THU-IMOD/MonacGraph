package db.monacgraph.retrieval.graph;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.community.CommunityVertex;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.serialize.IdCodec;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Real KG edges written during ingestion. */
public final class TinkerPopNeighborhood implements GraphNeighborhood {
    private final CommunityGraph graph;

    public TinkerPopNeighborhood(CommunityGraph graph) {
        this.graph = Objects.requireNonNull(graph, "graph");
    }

    @Override
    public List<GraphElementRef> neighbors(GraphElementRef vertex) {
        if (vertex == null || vertex.kind() != ElementKind.VERTEX) {
            return List.of();
        }
        long handle;
        try {
            handle = graph.getVertexHandleById(graph.handle(), IdCodec.toBytes(vertex.id()));
        } catch (RuntimeException e) {
            return List.of();
        }
        if (handle <= 0) {
            return List.of();
        }
        List<GraphElementRef> neighbors = new ArrayList<>();
        CommunityVertex node = new CommunityVertex(graph, handle);
        node.vertices(Direction.BOTH).forEachRemaining(other -> neighbors.add(ref(other)));
        return neighbors;
    }

    private static GraphElementRef ref(Vertex vertex) {
        return new GraphElementRef(ElementKind.VERTEX, vertex.id(), vertex.label());
    }
}
