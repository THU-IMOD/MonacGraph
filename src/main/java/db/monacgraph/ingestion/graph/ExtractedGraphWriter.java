package db.monacgraph.ingestion.graph;

import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.resolution.EntityResolver;
import db.monacgraph.retrieval.hipporag.HippoRagIndex;
import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.LinkRole;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ExtractedGraphWriter {
    private final EntityResolver resolver;
    private final GraphMutationTarget graph;
    private final HippoRagIndex hippoRagIndex;

    public ExtractedGraphWriter(EntityResolver resolver, GraphMutationTarget graph) {
        this(resolver, graph, null);
    }

    public ExtractedGraphWriter(
            EntityResolver resolver, GraphMutationTarget graph, HippoRagIndex hippoRagIndex) {
        this.resolver = resolver;
        this.graph = graph;
        this.hippoRagIndex = hippoRagIndex;
    }

    public ContentRecord write(Passage passage, ExtractedKnowledge knowledge) {
        Map<String, GraphElementRef> vertices = new LinkedHashMap<>();
        Set<ContentElementLink> links = new LinkedHashSet<>();
        for (ExtractedEntity extracted : knowledge.entities()) {
            CanonicalEntity canonical = resolver.resolve(extracted);
            GraphElementRef vertex = graph.upsertVertex(canonical);
            vertices.put(extracted.localId(), vertex);
            links.add(new ContentElementLink(vertex, LinkRole.MENTIONS));
            if (hippoRagIndex != null) {
                hippoRagIndex.putPhrase(new PhraseNode(
                        vertex, canonical.canonicalName(), canonical.aliases()));
            }
        }
        for (ExtractedRelation relation : knowledge.relations()) {
            GraphElementRef source = requireVertex(vertices, relation.source());
            GraphElementRef target = requireVertex(vertices, relation.target());
            GraphElementRef edge = graph.upsertEdge(source, relation.relation(), target);
            links.add(new ContentElementLink(edge, LinkRole.SUPPORTS));
            if (hippoRagIndex != null) {
                hippoRagIndex.putFact(new FactTriple(edge, source, target));
            }
        }
        return new ContentRecord(
                passage.contentId(),
                passage.documentId(),
                passage.text(),
                passage.sectionPath(),
                null,
                1,
                links.stream().toList());
    }

    private static GraphElementRef requireVertex(
            Map<String, GraphElementRef> vertices, String localId) {
        GraphElementRef vertex = vertices.get(localId);
        if (vertex == null) {
            throw new IllegalArgumentException("Unknown extracted entity localId: " + localId);
        }
        return vertex;
    }
}
