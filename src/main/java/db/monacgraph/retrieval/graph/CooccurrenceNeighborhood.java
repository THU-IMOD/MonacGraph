package db.monacgraph.retrieval.graph;

import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Connects vertices that share a passage via {@code MENTIONS}. HippoRAG's
 * extracted KG is often sparse; co-occurrence keeps PPR from dying on
 * isolated OpenIE nodes that still appear in the same evidence.
 */
public final class CooccurrenceNeighborhood implements GraphNeighborhood {
    private final ContentStore store;

    public CooccurrenceNeighborhood(ContentStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public List<GraphElementRef> neighbors(GraphElementRef vertex) {
        if (vertex == null || vertex.kind() != ElementKind.VERTEX) {
            return List.of();
        }
        Set<GraphElementRef> neighbors = new LinkedHashSet<>();
        for (String contentId : store.contentIdsForElement(vertex)) {
            ContentRecord record = store.get(contentId).orElse(null);
            if (record == null) {
                continue;
            }
            for (ContentElementLink link : record.links()) {
                if (link.element().kind() == ElementKind.VERTEX
                        && !link.element().id().equals(vertex.id())) {
                    neighbors.add(link.element());
                }
            }
        }
        return new ArrayList<>(neighbors);
    }
}
