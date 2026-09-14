package db.monacgraph.retrieval;

import db.monacgraph.retrieval.generation.AnswerGenerator;
import db.monacgraph.retrieval.graph.CooccurrenceNeighborhood;
import db.monacgraph.retrieval.graph.GraphNeighborhood;
import db.monacgraph.retrieval.index.LuceneContentIndex;
import db.monacgraph.retrieval.linking.EntityIndex;
import db.monacgraph.retrieval.linking.EntityIndex.IndexedEntity;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.AnswerResult;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.Citation;
import db.monacgraph.retrieval.model.PipelineModels.GenerationMetadata;
import db.monacgraph.retrieval.model.PipelineModels.PprOptions;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.LinkRole;
import db.monacgraph.retrieval.pipeline.PersonalizedPageRank;
import db.monacgraph.retrieval.storage.RocksContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HippoRagPipelineTest {
    private static final GraphElementRef SCOTT =
            new GraphElementRef(ElementKind.VERTEX, "scott", "person");
    private static final GraphElementRef ED =
            new GraphElementRef(ElementKind.VERTEX, "ed", "person");
    private static final GraphElementRef OTHER =
            new GraphElementRef(ElementKind.VERTEX, "other", "person");

    @TempDir
    Path temporaryDirectory;

    @Test
    void pprPrefersNodesCloserToTheQuerySeed() {
        GraphElementRef alpha = new GraphElementRef(ElementKind.VERTEX, "alpha", "n");
        GraphElementRef beta = new GraphElementRef(ElementKind.VERTEX, "beta", "n");
        GraphElementRef gamma = new GraphElementRef(ElementKind.VERTEX, "gamma", "n");
        GraphNeighborhood graph = new MapNeighborhood()
                .edge(alpha, beta)
                .edge(beta, gamma);
        EntityIndex index = () -> List.of(
                new IndexedEntity(alpha, "Alpha", List.of()),
                new IndexedEntity(beta, "Beta", List.of()),
                new IndexedEntity(gamma, "Gamma", List.of()));
        try (RetrievalServices services = services("ppr")) {
            AnchorSet seeds = new RetrievalPipelineBuilder(
                    new RetrievalRuntime(services, index, graph, unusedGenerator()),
                    "Tell me about Alpha")
                    .entityLink()
                    .executeTo(AnchorSet.class);
            CandidateSet<GraphElementRef> ranked = new PersonalizedPageRank(graph, index)
                    .expand(seeds, PprOptions.hippoRag());
            assertEquals("alpha", ranked.items().get(0).item().id());
            assertEquals("beta", ranked.items().get(1).item().id());
            assertEquals("gamma", ranked.items().get(2).item().id());
        }
    }

    @Test
    void hippoRagRetrievesSharedPassageAndAnswers() {
        ContentRecord passage = new ContentRecord(
                "p1",
                "doc",
                "Scott Derrickson is an American director. Ed Wood was an American filmmaker.",
                "body",
                null,
                1,
                List.of(
                        new ContentElementLink(SCOTT, LinkRole.MENTIONS),
                        new ContentElementLink(ED, LinkRole.MENTIONS)));
        try (RetrievalServices services = services("full")) {
            services.contentStore().put(passage);
            EntityIndex index = () -> List.of(
                    new IndexedEntity(SCOTT, "Scott Derrickson", List.of()),
                    new IndexedEntity(ED, "Ed Wood", List.of()),
                    new IndexedEntity(OTHER, "Someone Else", List.of()));
            RetrievalRuntime runtime = new RetrievalRuntime(
                    services,
                    index,
                    new CooccurrenceNeighborhood(services.contentStore()),
                    (request, evidence) -> new AnswerResult(
                            "Yes.",
                            List.of(new Citation(0, 4, List.of())),
                            evidence,
                            new GenerationMetadata("fake", "test", 0, 0, "stop")));

            AnswerResult result = new RetrievalPipelineBuilder(runtime,
                    "Were Scott Derrickson and Ed Wood of the same nationality?")
                    .hippoRag()
                    .execute();

            assertEquals("Yes.", result.answer());
            assertEquals(1, result.evidence().contents().size());
            assertEquals("p1", result.evidence().contents().get(0).contentId());
            assertTrue(result.evidence().vertices().stream()
                    .anyMatch(vertex -> vertex.vertex().id().equals("scott")));
            assertTrue(result.evidence().vertices().stream()
                    .anyMatch(vertex -> vertex.vertex().id().equals("ed")));
        }
    }

    @Test
    void pprReachesCooccurringEntityFromOneAnchor() {
        ContentRecord passage = new ContentRecord(
                "p1", "doc",
                "Scott Derrickson directed and later mentioned Ed Wood.",
                "body", null, 1,
                List.of(
                        new ContentElementLink(SCOTT, LinkRole.MENTIONS),
                        new ContentElementLink(ED, LinkRole.MENTIONS)));
        try (RetrievalServices services = services("one-anchor")) {
            services.contentStore().put(passage);
            EntityIndex index = () -> List.of(
                    new IndexedEntity(SCOTT, "Scott Derrickson", List.of()),
                    new IndexedEntity(ED, "Ed Wood", List.of()));
            RetrievalRuntime runtime = new RetrievalRuntime(
                    services, index, new CooccurrenceNeighborhood(services.contentStore()), unusedGenerator());
            @SuppressWarnings("unchecked")
            CandidateSet<GraphElementRef> nodes = new RetrievalPipelineBuilder(
                    runtime, "Who is Scott Derrickson?")
                    .entityLink()
                    .ppr()
                    .executeTo(CandidateSet.class);
            assertEquals("scott", nodes.items().get(0).item().id());
            assertTrue(nodes.items().stream().anyMatch(item -> item.item().id().equals("ed")));
        }
    }

    @Test
    void rejectsPprWithoutSeeds() {
        try (RetrievalServices services = services("empty-seeds")) {
            RetrievalPipelineBuilder pipeline = new RetrievalPipelineBuilder(
                    new RetrievalRuntime(services, List::of, vertex -> List.of(), unusedGenerator()),
                    "no anchors here");
            assertThrows(IllegalStateException.class, () -> pipeline.entityLink().ppr().execute());
        }
    }

    @Test
    void capturesAnchorSetForHotpotQuery() {
        EntityIndex index = () -> List.of(
                new IndexedEntity(SCOTT, "Scott Derrickson", List.of()),
                new IndexedEntity(ED, "Ed Wood", List.of()));
        try (RetrievalServices services = services("anchors")) {
            AnchorSet anchors = new RetrievalPipelineBuilder(
                    new RetrievalRuntime(services, index, vertex -> List.of(), unusedGenerator()),
                    "Were Scott Derrickson and Ed Wood of the same nationality?")
                    .entityLink()
                    .executeTo(AnchorSet.class);
            assertEquals(2, anchors.mentions().size());
        }
    }

    private RetrievalServices services(String name) {
        return new RetrievalServices(
                new RocksContentStore(temporaryDirectory.resolve(name + "-store")),
                new LuceneContentIndex(temporaryDirectory.resolve(name + "-lucene")),
                new FakeEmbedding(),
                null);
    }

    private static AnswerGenerator unusedGenerator() {
        return (request, evidence) -> new AnswerResult(
                "", List.of(), evidence, new GenerationMetadata("fake", "test", 0, 0, "stop"));
    }

    private static final class MapNeighborhood implements GraphNeighborhood {
        private final Map<Object, List<GraphElementRef>> adjacency = new HashMap<>();

        private MapNeighborhood edge(GraphElementRef left, GraphElementRef right) {
            adjacency.computeIfAbsent(left.id(), key -> new java.util.ArrayList<>()).add(right);
            adjacency.computeIfAbsent(right.id(), key -> new java.util.ArrayList<>()).add(left);
            return this;
        }

        @Override
        public List<GraphElementRef> neighbors(GraphElementRef vertex) {
            return adjacency.getOrDefault(vertex.id(), List.of());
        }
    }

    private static final class FakeEmbedding implements db.monacgraph.retrieval.embedding.EmbeddingProvider {
        private final EmbeddingSpace space = new EmbeddingSpace("t", "fake", "1", 2, true, "");

        @Override
        public EmbeddingSpace space() {
            return space;
        }

        @Override
        public List<float[]> embedDocuments(List<String> texts) {
            return texts.stream().map(text -> new float[]{1, 0}).toList();
        }

        @Override
        public float[] embedQuery(String query) {
            return new float[]{1, 0};
        }
    }
}
