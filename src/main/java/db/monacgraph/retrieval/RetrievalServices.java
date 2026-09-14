package db.monacgraph.retrieval;

import db.monacgraph.retrieval.embedding.EmbeddingProcessManager;
import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.embedding.HttpEmbeddingProvider;
import db.monacgraph.retrieval.index.ContentIndex;
import db.monacgraph.retrieval.index.LuceneContentIndex;
import db.monacgraph.retrieval.indexing.EmbeddingIndexer;
import db.monacgraph.retrieval.indexing.RetrievalIngestor;
import db.monacgraph.retrieval.storage.ContentStore;
import db.monacgraph.retrieval.storage.RocksContentStore;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

/** Owns all per-graph retrieval resources and their lifecycle. */
public final class RetrievalServices implements AutoCloseable {
    private final ContentStore contentStore;
    private final ContentIndex contentIndex;
    private final EmbeddingProvider embeddingProvider;
    private final EmbeddingProcessManager processManager;
    private final RetrievalIngestor ingestor;
    private final EmbeddingIndexer embeddingIndexer;

    public static RetrievalServices open(RetrievalConfig config) {
        try {
            Files.createDirectories(config.rootDirectory());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create retrieval directory", e);
        }
        EmbeddingProcessManager manager = null;
        HttpEmbeddingProvider provider = new HttpEmbeddingProvider(
                config.embeddingServiceUri(), config.embeddingSpace());
        if (config.manageEmbeddingProcess()) {
            int port = config.embeddingServiceUri().getPort() < 0
                    ? 80 : config.embeddingServiceUri().getPort();
            manager = new EmbeddingProcessManager(
                    config.pythonCommand(),
                    config.embeddingServiceDirectory(),
                    config.embeddingSpace(),
                    port);
            manager.awaitHealthy(provider, Duration.ofMinutes(3));
        }
        return new RetrievalServices(
                new RocksContentStore(config.rootDirectory().resolve("content_db")),
                new LuceneContentIndex(config.rootDirectory().resolve("lucene")),
                provider,
                manager);
    }

    public RetrievalServices(
            ContentStore contentStore,
            ContentIndex contentIndex,
            EmbeddingProvider embeddingProvider,
            EmbeddingProcessManager processManager) {
        this.contentStore = contentStore;
        this.contentIndex = contentIndex;
        this.embeddingProvider = embeddingProvider;
        this.processManager = processManager;
        this.ingestor = new RetrievalIngestor(contentStore, contentIndex);
        this.embeddingIndexer = new EmbeddingIndexer(contentStore, contentIndex, embeddingProvider);
    }

    public ContentStore contentStore() {
        return contentStore;
    }

    public ContentIndex contentIndex() {
        return contentIndex;
    }

    public EmbeddingProvider embeddingProvider() {
        return embeddingProvider;
    }

    public RetrievalIngestor ingestor() {
        return ingestor;
    }

    public EmbeddingIndexer embeddingIndexer() {
        return embeddingIndexer;
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            contentIndex.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            contentStore.close();
        } catch (RuntimeException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        try {
            embeddingProvider.close();
        } catch (RuntimeException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        if (processManager != null) {
            processManager.close();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
