package db.monacgraph.retrieval.index;

import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;

import java.util.List;

public interface ContentIndex extends AutoCloseable {
    void indexText(ContentRecord content);

    void indexVector(ContentRecord content, EmbeddingSpace space, float[] vector);

    List<ContentCandidate> textSearch(String query, int topK);

    List<ContentCandidate> vectorSearch(float[] queryVector, EmbeddingSpace space, int topK);

    void delete(String contentId);

    @Override
    void close();
}
