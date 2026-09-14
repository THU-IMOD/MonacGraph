package db.monacgraph.retrieval.indexing;

import db.monacgraph.retrieval.index.ContentIndex;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.storage.ContentStore;

/** Coordinates authoritative writes and rebuildable retrieval indexes. */
public final class RetrievalIngestor {
    private final ContentStore contentStore;
    private final ContentIndex contentIndex;

    public RetrievalIngestor(ContentStore contentStore, ContentIndex contentIndex) {
        this.contentStore = contentStore;
        this.contentIndex = contentIndex;
    }

    public void ingest(ContentRecord content, EmbeddingSpace space) {
        contentStore.put(content);
        contentIndex.indexText(content);
        contentStore.enqueueEmbedding(content.contentId(), space);
    }

    public void delete(String contentId) {
        contentStore.delete(contentId);
        contentIndex.delete(contentId);
    }
}
