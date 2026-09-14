package db.monacgraph.retrieval;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.hipporag.HippoRagIndexer;
import db.monacgraph.retrieval.hipporag.HippoRagModels;
import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.hipporag.HippoRagV1;
import db.monacgraph.retrieval.hipporag.RocksHippoRagIndex;
import db.monacgraph.retrieval.hipporag.Vectors;
import db.monacgraph.retrieval.index.LuceneContentIndex;
import db.monacgraph.retrieval.indexing.RetrievalIngestor;
import db.monacgraph.retrieval.linking.EntityIndex;
import db.monacgraph.retrieval.linking.EntityIndex.IndexedEntity;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.PprOptions;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.LinkRole;
import db.monacgraph.retrieval.storage.RocksContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HippoRagV1Test {
    private static final GraphElementRef SCOTT =
            new GraphElementRef(ElementKind.VERTEX, "scott", "person");
    private static final GraphElementRef ED =
            new GraphElementRef(ElementKind.VERTEX, "ed", "person");
    private static final GraphElementRef EDGE =
            new GraphElementRef(ElementKind.EDGE, "same-nationality", "same nationality");

    @TempDir
    Path temporaryDirectory;

    @Test
    void writesSynonymEdgesAboveThreshold() {
        try (RocksHippoRagIndex index = new RocksHippoRagIndex(temporaryDirectory.resolve("syn"))) {
            index.putPhrase(new PhraseNode(SCOTT, "Scott Derrickson", List.of()));
            index.putPhrase(new PhraseNode(ED, "Scott Derrickson clone", List.of()));
            int added = new HippoRagIndexer(index).refresh(new NameEmbedding());
            assertEquals(1, added);
            assertEquals(1, index.synonyms().size());
            assertTrue(index.synonyms().get(0).score() >= HippoRagModels.SYNONYM_THRESHOLD);
        }
    }

    @Test
    void linksMentionsByNearestPhraseAndSpecificity() {
        try (RocksHippoRagIndex index = new RocksHippoRagIndex(temporaryDirectory.resolve("link"));
             RetrievalServices services = services("link")) {
            index.putPhrase(new PhraseNode(SCOTT, "Scott Derrickson", List.of()));
            index.putPhrase(new PhraseNode(ED, "Ed Wood", List.of()));
            new HippoRagIndexer(index).refresh(services.embeddingProvider());
            services.contentStore().put(passage("p1", List.of(
                    new ContentElementLink(SCOTT, LinkRole.MENTIONS),
                    new ContentElementLink(EDGE, LinkRole.SUPPORTS))));
            HippoRagV1 hippo = hippo(index, services, List.of("Scott Derrickson"));
            AnchorSet anchors = hippo.link("Were Scott Derrickson and Ed Wood of the same nationality?");
            assertEquals(1, anchors.mentions().size());
            assertEquals("scott", anchors.mentions().get(0).candidates().get(0).element().id());
            assertEquals(1.0, anchors.mentions().get(0).candidates().get(0).scores().stream()
                    .filter(score -> score.name().equals("specificity"))
                    .findFirst()
                    .orElseThrow()
                    .value());
        }
    }

    @Test
    void projectsPhraseScoresThroughFactsOntoPassages() {
        try (RocksHippoRagIndex index = new RocksHippoRagIndex(temporaryDirectory.resolve("facts"));
             RetrievalServices services = services("facts")) {
            index.putPhrase(new PhraseNode(SCOTT, "Scott Derrickson", List.of()));
            index.putPhrase(new PhraseNode(ED, "Ed Wood", List.of()));
            index.putFact(new FactTriple(EDGE, SCOTT, ED));
            new HippoRagIndexer(index).refresh(services.embeddingProvider());
            services.contentStore().put(passage("p1", List.of(
                    new ContentElementLink(SCOTT, LinkRole.MENTIONS),
                    new ContentElementLink(ED, LinkRole.MENTIONS),
                    new ContentElementLink(EDGE, LinkRole.SUPPORTS))));
            HippoRagV1 hippo = hippo(index, services, List.of("Scott Derrickson", "Ed Wood"));
            CandidateSet<ContentRef> docs = hippo.retrieve(
                    "Were Scott Derrickson and Ed Wood of the same nationality?");
            assertEquals("p1", docs.items().get(0).item().contentId());
        }
    }

    @Test
    void fallsBackToDenseRetrievalWhenNerIsEmpty() {
        try (RocksHippoRagIndex index = new RocksHippoRagIndex(temporaryDirectory.resolve("dense"));
             RetrievalServices services = services("dense")) {
            index.putPhrase(new PhraseNode(SCOTT, "Scott Derrickson", List.of()));
            new HippoRagIndexer(index).refresh(services.embeddingProvider());
            ContentRecord record = passage("p1", List.of(new ContentElementLink(SCOTT, LinkRole.MENTIONS)));
            new RetrievalIngestor(services.contentStore(), services.contentIndex()).ingest(
                    record, services.embeddingProvider().space());
            services.embeddingIndexer().runBatch(10);
            HippoRagV1 hippo = hippo(index, services, List.of());
            CandidateSet<ContentRef> docs =
                    hippo.retrieve("What nationality is the director?");
            assertEquals("p1", docs.items().get(0).item().contentId());
        }
    }

    @Test
    void cosineHelperMatchesIdenticalVectors() {
        assertEquals(1.0, Vectors.cosine(new float[]{1, 0}, new float[]{1, 0}), 1e-6);
        assertEquals(0.0, Vectors.cosine(new float[]{1, 0}, new float[]{0, 1}), 1e-6);
    }

    private HippoRagV1 hippo(
            RocksHippoRagIndex index, RetrievalServices services, List<String> mentions) {
        EntityIndex entities = () -> List.of(
                new IndexedEntity(SCOTT, "Scott Derrickson", List.of()),
                new IndexedEntity(ED, "Ed Wood", List.of()));
        return new HippoRagV1(
                index,
                entities,
                services.embeddingProvider(),
                services.contentStore(),
                services.contentIndex(),
                query -> mentions,
                PprOptions.hippoRag(),
                10);
    }

    private RetrievalServices services(String name) {
        return new RetrievalServices(
                new RocksContentStore(temporaryDirectory.resolve(name + "-store")),
                new LuceneContentIndex(temporaryDirectory.resolve(name + "-lucene")),
                new NameEmbedding(),
                null);
    }

    private static ContentRecord passage(String id, List<ContentElementLink> links) {
        return new ContentRecord(
                id, "doc",
                "Scott Derrickson is an American director. Ed Wood was an American filmmaker.",
                "body", null, 1, links);
    }

    private static final class NameEmbedding implements EmbeddingProvider {
        private final EmbeddingSpace space = new EmbeddingSpace("t", "fake", "1", 2, true, "");

        @Override
        public EmbeddingSpace space() {
            return space;
        }

        @Override
        public List<float[]> embedDocuments(List<String> texts) {
            return texts.stream().map(NameEmbedding::vector).toList();
        }

        @Override
        public float[] embedQuery(String query) {
            return vector(query);
        }

        private static float[] vector(String text) {
            String lower = text.toLowerCase();
            if (lower.contains("ed wood") && !lower.contains("scott")) {
                return new float[]{0, 1};
            }
            if (lower.contains("scott")) {
                return new float[]{1, 0};
            }
            if (lower.contains("ed")) {
                return new float[]{0, 1};
            }
            return new float[]{1, 0};
        }
    }
}
