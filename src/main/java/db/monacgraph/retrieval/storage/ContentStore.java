package db.monacgraph.retrieval.storage;

import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingJob;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;
import java.util.Optional;

public interface ContentStore extends AutoCloseable {
    void put(ContentRecord content);

    Optional<ContentRecord> get(String contentId);

    void delete(String contentId);

    List<String> contentIdsForElement(GraphElementRef element);

    void enqueueEmbedding(String contentId, EmbeddingSpace space);

    List<EmbeddingJob> pendingEmbeddings(String spaceId, int limit);

    Optional<EmbeddingJob> embeddingJob(String contentId, String spaceId);

    void markEmbeddingReady(String contentId, String spaceId);

    void markEmbeddingFailed(String contentId, String spaceId, String error);

    @Override
    void close();
}
