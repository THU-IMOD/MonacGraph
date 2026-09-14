package db.monacgraph.retrieval;

import db.monacgraph.retrieval.generation.AnswerGenerator;
import db.monacgraph.retrieval.graph.GraphNeighborhood;
import db.monacgraph.retrieval.hipporag.HippoRagIndex;
import db.monacgraph.retrieval.hipporag.QueryNer;
import db.monacgraph.retrieval.linking.EntityIndex;

import java.util.Objects;

/** Dependencies required to execute a typed retrieval pipeline. */
public record RetrievalRuntime(
        RetrievalServices services,
        EntityIndex entities,
        GraphNeighborhood neighborhood,
        AnswerGenerator generator,
        HippoRagIndex hippoRagIndex,
        QueryNer queryNer) {
    public RetrievalRuntime {
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(entities, "entities");
        Objects.requireNonNull(neighborhood, "neighborhood");
    }

    public RetrievalRuntime(
            RetrievalServices services,
            EntityIndex entities,
            GraphNeighborhood neighborhood,
            AnswerGenerator generator) {
        this(services, entities, neighborhood, generator, null, null);
    }
}
