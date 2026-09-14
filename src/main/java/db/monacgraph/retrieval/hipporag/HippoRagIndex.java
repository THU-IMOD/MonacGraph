package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.hipporag.HippoRagModels.SynonymEdge;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;
import java.util.Optional;

/** Durable HippoRAG 1 materials: phrases, fact triples, phrase vectors, synonym edges. */
public interface HippoRagIndex extends AutoCloseable {
    void putPhrase(PhraseNode phrase);

    void putFact(FactTriple fact);

    void putVector(Object vertexId, float[] vector);

    void putSynonym(SynonymEdge edge);

    List<PhraseNode> phrases();

    List<FactTriple> facts();

    List<SynonymEdge> synonyms();

    Optional<float[]> vector(Object vertexId);

    default Optional<PhraseNode> phrase(Object vertexId) {
        return phrases().stream()
                .filter(phrase -> phrase.vertex().id().equals(vertexId))
                .findFirst();
    }

    default GraphElementRef vertex(Object vertexId) {
        return phrase(vertexId)
                .map(PhraseNode::vertex)
                .orElse(null);
    }

    @Override
    default void close() {}
}
