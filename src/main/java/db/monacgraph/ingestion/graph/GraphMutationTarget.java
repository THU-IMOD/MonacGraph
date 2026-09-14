package db.monacgraph.ingestion.graph;

import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

public interface GraphMutationTarget {
    GraphElementRef upsertVertex(CanonicalEntity entity);

    GraphElementRef upsertEdge(
            GraphElementRef source, String relation, GraphElementRef target);
}
