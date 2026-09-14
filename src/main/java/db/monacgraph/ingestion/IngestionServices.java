package db.monacgraph.ingestion;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.ingestion.chunk.ParagraphAwareChunker;
import db.monacgraph.ingestion.document.TikaDocumentParser;
import db.monacgraph.ingestion.extraction.KnowledgeExtractor;
import db.monacgraph.ingestion.extraction.OpenAiCompatibleKnowledgeExtractor;
import db.monacgraph.ingestion.graph.CommunityGraphMutationTarget;
import db.monacgraph.ingestion.graph.ExtractedGraphWriter;
import db.monacgraph.ingestion.job.IngestionJobStore;
import db.monacgraph.ingestion.job.RocksIngestionJobStore;
import db.monacgraph.ingestion.resolution.EntityCatalog;
import db.monacgraph.ingestion.resolution.ExactAliasEntityResolver;
import db.monacgraph.ingestion.resolution.RocksEntityCatalog;
import db.monacgraph.retrieval.RetrievalConfig;
import db.monacgraph.retrieval.RetrievalServices;
import db.monacgraph.retrieval.hipporag.HippoRagIndex;
import db.monacgraph.retrieval.hipporag.HippoRagIndexer;
import db.monacgraph.retrieval.hipporag.RocksHippoRagIndex;

public final class IngestionServices implements AutoCloseable {
    private final IngestionJobStore jobStore;
    private final EntityCatalog entityCatalog;
    private final KnowledgeExtractor extractor;
    private final IngestionCoordinator coordinator;
    private final IngestionWorker worker;
    private final HippoRagIndex hippoRagIndex;

    public static IngestionServices open(
            CommunityGraph graph,
            RetrievalServices retrieval,
            RetrievalConfig retrievalConfig,
            IngestionConfig config) {
        IngestionJobStore jobs = new RocksIngestionJobStore(
                retrievalConfig.rootDirectory().resolve("ingestion_jobs"));
        EntityCatalog entities = new RocksEntityCatalog(
                retrievalConfig.rootDirectory().resolve("entity_catalog"));
        KnowledgeExtractor extractor = new OpenAiCompatibleKnowledgeExtractor(
                config.extractorEndpoint(),
                config.extractorModel(),
                config.extractorApiKey(),
                config.extractorTimeout(),
                config.extractorConcurrency());
        HippoRagIndex hippoRagIndex = new RocksHippoRagIndex(
                retrievalConfig.rootDirectory().resolve("hipporag"));
        ExtractedGraphWriter writer = new ExtractedGraphWriter(
                new ExactAliasEntityResolver(entities),
                new CommunityGraphMutationTarget(graph),
                hippoRagIndex);
        IngestionCoordinator coordinator = new IngestionCoordinator(
                new TikaDocumentParser(),
                new ParagraphAwareChunker(config.chunkCharacters(), config.chunkOverlap()),
                extractor,
                writer,
                jobs,
                retrieval,
                retrievalConfig.embeddingSpace(),
                config.embeddingBatchSize(),
                config.maxAttempts(),
                new HippoRagIndexer(hippoRagIndex));
        IngestionWorker worker = config.workerEnabled()
                ? new IngestionWorker(coordinator, config.embeddingBatchSize(), config.workerInterval())
                : null;
        return new IngestionServices(jobs, entities, extractor, coordinator, worker, hippoRagIndex);
    }

    public IngestionServices(
            IngestionJobStore jobStore,
            EntityCatalog entityCatalog,
            KnowledgeExtractor extractor,
            IngestionCoordinator coordinator,
            IngestionWorker worker) {
        this(jobStore, entityCatalog, extractor, coordinator, worker, null);
    }

    public IngestionServices(
            IngestionJobStore jobStore,
            EntityCatalog entityCatalog,
            KnowledgeExtractor extractor,
            IngestionCoordinator coordinator,
            IngestionWorker worker,
            HippoRagIndex hippoRagIndex) {
        this.jobStore = jobStore;
        this.entityCatalog = entityCatalog;
        this.extractor = extractor;
        this.coordinator = coordinator;
        this.worker = worker;
        this.hippoRagIndex = hippoRagIndex;
    }

    public IngestionCoordinator coordinator() {
        return coordinator;
    }

    public EntityCatalog entityCatalog() {
        return entityCatalog;
    }

    public HippoRagIndex hippoRagIndex() {
        return hippoRagIndex;
    }

    @Override
    public void close() {
        if (worker != null) worker.close();
        extractor.close();
        if (hippoRagIndex != null) {
            hippoRagIndex.close();
        }
        entityCatalog.close();
        jobStore.close();
    }
}
