package db.monacgraph.ingestion;

import db.monacgraph.ingestion.chunk.ParagraphAwareChunker;
import db.monacgraph.ingestion.document.TikaDocumentParser;
import db.monacgraph.ingestion.extraction.KnowledgeExtractor;
import db.monacgraph.ingestion.graph.ExtractedGraphWriter;
import db.monacgraph.ingestion.graph.GraphMutationTarget;
import db.monacgraph.ingestion.job.RocksIngestionJobStore;
import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;
import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.IngestionStatus;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.resolution.ExactAliasEntityResolver;
import db.monacgraph.ingestion.resolution.RocksEntityCatalog;
import db.monacgraph.retrieval.RetrievalServices;
import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.index.LuceneContentIndex;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.storage.RocksContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IngestionPipelineIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void ingestsDocumentIdempotentlyAndProducesSearchableContent() throws Exception {
        Path source = temporaryDirectory.resolve("apple.txt");
        Files.writeString(source, "Steve Jobs co-founded Apple.");
        EmbeddingSpace space = new EmbeddingSpace(
                "test-space", "fake", "1", 2, true, "");

        RocksContentStore contents = new RocksContentStore(temporaryDirectory.resolve("contents"));
        LuceneContentIndex index = new LuceneContentIndex(temporaryDirectory.resolve("lucene"));
        EmbeddingProvider embeddings = new FakeEmbeddingProvider(space);
        try (RetrievalServices retrieval = new RetrievalServices(contents, index, embeddings, null);
             RocksIngestionJobStore jobs = new RocksIngestionJobStore(temporaryDirectory.resolve("jobs"));
             RocksEntityCatalog entities = new RocksEntityCatalog(temporaryDirectory.resolve("entities"))) {
            FakeGraph graph = new FakeGraph();
            ExtractedGraphWriter writer = new ExtractedGraphWriter(
                    new ExactAliasEntityResolver(entities), graph);
            IngestionCoordinator coordinator = new IngestionCoordinator(
                    new TikaDocumentParser(),
                    new ParagraphAwareChunker(512, 32),
                    new FakeExtractor(),
                    writer,
                    jobs,
                    retrieval,
                    space,
                    16,
                    3);

            IngestionJob submitted = coordinator.submit(source);
            IngestionJob completed = coordinator.run(submitted.jobId());
            assertEquals(IngestionStatus.READY, completed.status());
            assertEquals(2, graph.vertices.size());
            assertEquals(1, graph.edges.size());
            assertFalse(index.textSearch("Apple", 10).isEmpty());
            assertFalse(index.vectorSearch(new float[]{1, 0}, space, 10).isEmpty());

            IngestionJob duplicate = coordinator.submit(source);
            assertEquals(submitted.jobId(), duplicate.jobId());
            assertEquals(IngestionStatus.READY, coordinator.run(duplicate.jobId()).status());
            assertEquals(2, graph.vertices.size());
            assertEquals(1, graph.edges.size());

            coordinator.delete(submitted.jobId());
            assertFalse(coordinator.status(submitted.jobId()).isPresent());
            assertEquals(0, index.textSearch("Apple", 10).size());
            IngestionJob reimported = coordinator.submit(source);
            assertEquals(IngestionStatus.READY, coordinator.run(reimported.jobId()).status());
            assertEquals(2, graph.vertices.size());
            assertEquals(1, graph.edges.size());
        }
    }

    @Test
    void resumesFromCheckpointAfterExtractorFailure() throws Exception {
        Path source = temporaryDirectory.resolve("retry.txt");
        Files.writeString(source, "Steve Jobs co-founded Apple.");
        EmbeddingSpace space = new EmbeddingSpace("retry-space", "fake", "1", 2, true, "");
        RocksContentStore contents = new RocksContentStore(temporaryDirectory.resolve("retry-contents"));
        LuceneContentIndex index = new LuceneContentIndex(temporaryDirectory.resolve("retry-lucene"));
        try (RetrievalServices retrieval = new RetrievalServices(
                    contents, index, new FakeEmbeddingProvider(space), null);
             RocksIngestionJobStore jobs = new RocksIngestionJobStore(
                     temporaryDirectory.resolve("retry-jobs"));
             RocksEntityCatalog entities = new RocksEntityCatalog(
                     temporaryDirectory.resolve("retry-entities"))) {
            AtomicInteger calls = new AtomicInteger();
            KnowledgeExtractor extractor = passage -> {
                if (calls.getAndIncrement() == 0) {
                    throw new IllegalStateException("temporary extractor failure");
                }
                return new FakeExtractor().extract(passage);
            };
            IngestionCoordinator coordinator = new IngestionCoordinator(
                    new TikaDocumentParser(),
                    new ParagraphAwareChunker(512, 32),
                    extractor,
                    new ExtractedGraphWriter(
                            new ExactAliasEntityResolver(entities), new FakeGraph()),
                    jobs,
                    retrieval,
                    space,
                    16,
                    3);
            IngestionJob submitted = coordinator.submit(source);
            IngestionJob failed = coordinator.run(submitted.jobId());
            assertEquals(IngestionStatus.FAILED, failed.status());
            assertEquals(IngestionStatus.CHUNKED, failed.checkpoint());
            assertEquals(IngestionStatus.READY, coordinator.run(submitted.jobId()).status());
        }
    }

    private static final class FakeExtractor implements KnowledgeExtractor {
        @Override
        public ExtractedKnowledge extract(Passage passage) {
            return new ExtractedKnowledge(
                    List.of(
                            new ExtractedEntity("e1", "Steve Jobs", "person", List.of()),
                            new ExtractedEntity("e2", "Apple", "company", List.of("Apple Inc."))),
                    List.of(new ExtractedRelation("e1", "co-founded", "e2", passage.text())));
        }
    }

    private static final class FakeGraph implements GraphMutationTarget {
        private final Map<String, GraphElementRef> vertices = new HashMap<>();
        private final Map<String, GraphElementRef> edges = new HashMap<>();

        @Override
        public GraphElementRef upsertVertex(CanonicalEntity entity) {
            return vertices.computeIfAbsent(entity.vertexId(), id ->
                    new GraphElementRef(ElementKind.VERTEX, id, entity.type()));
        }

        @Override
        public GraphElementRef upsertEdge(
                GraphElementRef source, String relation, GraphElementRef target) {
            String id = IngestionIds.deterministicUuid(
                    "edge", source.id() + "\u0000" + relation + "\u0000" + target.id());
            return edges.computeIfAbsent(id, ignored ->
                    new GraphElementRef(ElementKind.EDGE, id, relation));
        }
    }

    private static final class FakeEmbeddingProvider implements EmbeddingProvider {
        private final EmbeddingSpace space;

        private FakeEmbeddingProvider(EmbeddingSpace space) {
            this.space = space;
        }

        @Override
        public EmbeddingSpace space() {
            return space;
        }

        @Override
        public List<float[]> embedDocuments(List<String> texts) {
            return texts.stream().map(ignored -> new float[]{1, 0}).toList();
        }

        @Override
        public float[] embedQuery(String query) {
            return new float[]{1, 0};
        }
    }
}
