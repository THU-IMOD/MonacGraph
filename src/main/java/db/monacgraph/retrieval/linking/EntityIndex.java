package db.monacgraph.retrieval.linking;

import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;

/** Read-only view of canonical entities used by {@code entityLink()}. */
public interface EntityIndex {
    List<IndexedEntity> entities();

    record IndexedEntity(
            GraphElementRef vertex,
            String canonicalName,
            List<String> aliases) {
        public IndexedEntity {
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }
    }
}
