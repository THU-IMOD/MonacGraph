package db.monacgraph.retrieval.embedding;

import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;

import java.util.List;

public interface EmbeddingProvider extends AutoCloseable {
    EmbeddingSpace space();

    List<float[]> embedDocuments(List<String> texts);

    float[] embedQuery(String query);

    @Override
    default void close() {}
}
