package db.monacgraph.retrieval.indexing;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.index.ContentIndex;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingJob;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.List;

/** Drains durable embedding jobs in bounded, idempotent batches. */
public final class EmbeddingIndexer {
    private final ContentStore contentStore;
    private final ContentIndex contentIndex;
    private final EmbeddingProvider provider;

    public EmbeddingIndexer(
            ContentStore contentStore,
            ContentIndex contentIndex,
            EmbeddingProvider provider) {
        this.contentStore = contentStore;
        this.contentIndex = contentIndex;
        this.provider = provider;
    }

    public int runBatch(int batchSize) {
        EmbeddingSpace space = provider.space();
        List<EmbeddingJob> jobs = contentStore.pendingEmbeddings(space.spaceId(), batchSize);
        List<EmbeddingJob> currentJobs = new ArrayList<>();
        List<ContentRecord> contents = new ArrayList<>();

        for (EmbeddingJob job : jobs) {
            contentStore.get(job.contentId()).ifPresent(content -> {
                if (!content.contentHash().equals(job.contentHash())) {
                    contentStore.enqueueEmbedding(content.contentId(), space);
                    return;
                }
                currentJobs.add(job);
                contents.add(content);
            });
        }
        if (contents.isEmpty()) {
            return 0;
        }

        List<float[]> vectors;
        try {
            vectors = provider.embedDocuments(contents.stream().map(ContentRecord::text).toList());
        } catch (RuntimeException e) {
            for (EmbeddingJob job : currentJobs) {
                contentStore.markEmbeddingFailed(job.contentId(), space.spaceId(), e.getMessage());
            }
            throw e;
        }
        if (vectors.size() != contents.size()) {
            throw new IllegalStateException("Embedding provider returned " + vectors.size()
                    + " vectors for " + contents.size() + " contents");
        }

        int completed = 0;
        for (int i = 0; i < contents.size(); i++) {
            ContentRecord content = contents.get(i);
            try {
                contentIndex.indexVector(content, space, vectors.get(i));
                contentStore.markEmbeddingReady(content.contentId(), space.spaceId());
                completed++;
            } catch (RuntimeException e) {
                contentStore.markEmbeddingFailed(
                        content.contentId(), space.spaceId(), e.getMessage());
            }
        }
        return completed;
    }
}
