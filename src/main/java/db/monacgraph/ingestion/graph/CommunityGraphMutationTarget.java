package db.monacgraph.ingestion.graph;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.ingestion.IngestionIds;
import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.serialize.IdCodec;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CommunityGraphMutationTarget implements GraphMutationTarget {
    private final CommunityGraph graph;
    private final Map<Object, Vertex> vertices = new ConcurrentHashMap<>();

    public CommunityGraphMutationTarget(CommunityGraph graph) {
        this.graph = graph;
    }

    @Override
    public synchronized GraphElementRef upsertVertex(CanonicalEntity entity) {
        Vertex vertex = vertices.get(entity.vertexId());
        if (vertex == null) {
            long handle = existingVertexHandle(entity.vertexId());
            if (handle == -1) {
                vertex = graph.addVertex(
                        T.id, entity.vertexId(),
                        T.label, entity.type(),
                        "name", entity.canonicalName(),
                        "aliases", String.join("|", entity.aliases()));
            } else {
                vertex = new db.monacgraph.community.CommunityVertex(graph, handle);
            }
            vertices.put(entity.vertexId(), vertex);
        }
        return new GraphElementRef(ElementKind.VERTEX, entity.vertexId(), entity.type());
    }

    @Override
    public synchronized GraphElementRef upsertEdge(
            GraphElementRef source, String relation, GraphElementRef target) {
        String normalizedRelation = relation == null ? "" : relation.trim();
        if (normalizedRelation.isBlank()) {
            throw new IllegalArgumentException("relation cannot be blank");
        }
        String edgeId = IngestionIds.deterministicUuid(
                "edge", source.id() + "\u0000" + normalizedRelation + "\u0000" + target.id());
        if (missingEdge(edgeId)) {
            Vertex out = requireCached(source.id(), "source");
            Vertex in = requireCached(target.id(), "target");
            graph.addEdge(normalizedRelation, out, in, T.id, edgeId);
        }
        return new GraphElementRef(ElementKind.EDGE, edgeId, normalizedRelation);
    }

    private long existingVertexHandle(String vertexId) {
        try {
            return graph.getVertexHandleById(graph.handle(), IdCodec.toBytes(vertexId));
        } catch (RuntimeException e) {
            if (isMissingNativeId(e)) {
                return -1;
            }
            throw e;
        }
    }

    /**
     * Older JNI builds throw {@code NoSuchElementException("Vertex not found")}
     * when an edge outer-id is absent. Treat that as missing instead of failing
     * the whole ingestion job.
     */
    private boolean missingEdge(String edgeId) {
        try {
            return graph.getEdgeHandleById(graph.handle(), IdCodec.toBytes(edgeId)) == -1;
        } catch (RuntimeException e) {
            if (isMissingNativeId(e)) {
                return true;
            }
            throw e;
        }
    }

    private static boolean isMissingNativeId(RuntimeException error) {
        String message = error.getMessage();
        return message != null && message.toLowerCase().contains("not found");
    }

    private Vertex requireCached(Object vertexId, String role) {
        Vertex vertex = vertices.get(String.valueOf(vertexId));
        if (vertex == null) {
            vertex = vertices.get(vertexId);
        }
        if (vertex == null) {
            throw new IllegalStateException(role + " vertex was not upserted: " + vertexId);
        }
        return vertex;
    }
}
