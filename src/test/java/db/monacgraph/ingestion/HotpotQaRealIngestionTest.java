package db.monacgraph.ingestion;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.community.CommunityVertex;
import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.IngestionStatus;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.LinkRole;
import db.monacgraph.serialize.IdCodec;
import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Official HotpotQA distractor sample against the real JNI graph and local
 * Ollama / embedding services. Skips when those services are not running.
 */
class HotpotQaRealIngestionTest {
    private static final Path ARTICLE = Path.of(
            "data", "hotpotqa-smoke", "scott-derrickson-ed-wood.txt");
    private static final String QUESTION =
            "Were Scott Derrickson and Ed Wood of the same nationality?";

    static boolean liveServicesAvailable() {
        return Files.isRegularFile(ARTICLE)
                && reachable(URI.create("http://127.0.0.1:8099/health"))
                && reachable(URI.create("http://127.0.0.1:11434/api/tags"));
    }

    @Test
    @EnabledIf("liveServicesAvailable")
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void ingestsOfficialHotpotQaArticleAndRecallsTheGoldPassages() throws Exception {
        String dbName = "hp" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        Path graphFile = Path.of("data", dbName + ".graph");
        Path workspace = Path.of("workspace", dbName);
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("db.name", dbName);
        configuration.setProperty("storage.path", "data");
        configuration.setProperty("retrieval.path", workspace.resolve("retrieval").toString());
        configuration.setProperty("ingestion.worker.enabled", false);
        configuration.setProperty("ingestion.extractor.url", "http://127.0.0.1:11434/v1/chat/completions");
        configuration.setProperty("ingestion.extractor.model", extractorModel());
        configuration.setProperty("ingestion.extractor.timeoutSeconds", 120);
        configuration.setProperty("retrieval.embedding.uri", "http://127.0.0.1:8099/");
        configuration.setProperty("retrieval.embedding.manageProcess", false);

        try (CommunityGraph graph = CommunityGraph.open(configuration)) {
            IngestionJob completed = graph.ingestion().run(graph.ingestion().submit(ARTICLE).jobId());
            assertEquals(IngestionStatus.READY, completed.status(),
                    () -> "ingestion failed: " + completed.error());

            List<ContentCandidate> bm25 = graph.traversal().retrieve(QUESTION)
                    .textRecall().topK(5).executeRecall();
            List<ContentCandidate> vectors = graph.traversal().retrieve(QUESTION)
                    .vectorRecall().topK(5).executeRecall();
            assertFalse(bm25.isEmpty(), "BM25 returned no passage for the official question");
            assertFalse(vectors.isEmpty(), "vector recall returned no passage for the official question");

            ContentRecord content = graph.getRetrievalServices().contentStore()
                    .get(bm25.get(0).content().contentId())
                    .orElseThrow(() -> new AssertionError("recalled content is missing from RocksDB"));
            assertTrue(mentionsBothPeople(content.text()),
                    "BM25 top hit should mention both people: " + content.text());
            ContentRecord vectorContent = graph.getRetrievalServices().contentStore()
                    .get(vectors.get(0).content().contentId())
                    .orElseThrow(() -> new AssertionError("vector-recalled content is missing from RocksDB"));
            assertTrue(mentionsBothPeople(vectorContent.text()),
                    "vector top hit should mention both people: " + vectorContent.text());
            assertFalse(content.links().isEmpty(),
                    "real extraction should link the passage to graph elements");
            assertTrue(content.links().stream().anyMatch(link -> link.role() == LinkRole.MENTIONS),
                    "passage should mention at least one vertex");

            List<String> names = new ArrayList<>();
            content.links().stream()
                    .filter(link -> link.element().kind() == ElementKind.VERTEX)
                    .forEach(link -> {
                        long handle = graph.getVertexHandleById(
                                graph.handle(), IdCodec.toBytes(link.element().id()));
                        assertTrue(handle != -1,
                                "linked vertex was not written through JNI: " + link.element().id());
                        CommunityVertex vertex = new CommunityVertex(graph, handle);
                        names.add(String.valueOf(vertex.property("name").orElse(vertex.label())));
                    });
            assertFalse(names.isEmpty(), "JNI graph should contain extracted vertices");
            assertTrue(
                    names.stream().anyMatch(name -> containsIgnoreCase(name, "Derrickson"))
                            || names.stream().anyMatch(name -> containsIgnoreCase(name, "Wood")),
                    "JNI vertices should include Scott Derrickson or Ed Wood, got " + names);

            printResults(completed, content, names, bm25, vectors);
        } finally {
            deleteQuietly(graphFile);
            deleteQuietly(workspace);
        }
    }

    private static void printResults(
            IngestionJob job,
            ContentRecord content,
            List<String> vertexNames,
            List<ContentCandidate> bm25,
            List<ContentCandidate> vectors) {
        System.out.println("======== HotpotQA real ingestion ========");
        System.out.println("article: " + ARTICLE.toAbsolutePath());
        System.out.println("question: " + QUESTION);
        System.out.println("gold answer: yes");
        System.out.println("job: " + job.status() + " " + job.jobId());
        System.out.println("checkpoint: " + job.checkpoint());
        System.out.println();
        System.out.println("[passage]");
        System.out.println(content.text());
        System.out.println();
        System.out.println("[links]");
        content.links().forEach(link -> System.out.println(
                link.role() + "\t" + link.element().kind()
                        + "\t" + link.element().label()
                        + "\t" + link.element().id()));
        System.out.println();
        System.out.println("[jni vertices]");
        vertexNames.forEach(name -> System.out.println("- " + name));
        System.out.println();
        printCandidates("BM25", bm25);
        printCandidates("VECTOR", vectors);
        System.out.println("========================================");
    }

    private static void printCandidates(String channel, List<ContentCandidate> candidates) {
        System.out.println("[" + channel + "]");
        for (ContentCandidate candidate : candidates) {
            System.out.printf(
                    Locale.ROOT,
                    "%d\t%.5f\t%s%n%s%n",
                    candidate.rank(),
                    candidate.score(),
                    candidate.content().contentId(),
                    candidate.matchedText());
        }
        System.out.println();
    }

    private static String extractorModel() {
        String model = System.getenv().getOrDefault("MONACGRAPH_EXTRACTOR_MODEL", "qwen3:0.6b");
        return model.isBlank() ? "qwen3:0.6b" : model;
    }

    private static boolean mentionsBothPeople(String text) {
        return containsIgnoreCase(text, "Derrickson") && containsIgnoreCase(text, "Wood");
    }

    private static boolean containsIgnoreCase(String value, String needle) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(needle.toLowerCase(Locale.ROOT));
    }

    private static boolean reachable(URI uri) {
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<Void> response = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(2))
                    .build()
                    .send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() / 100 == 2;
        } catch (Exception e) {
            return false;
        }
    }

    private static void deleteQuietly(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(entry -> {
                        try {
                            Files.deleteIfExists(entry);
                        } catch (Exception ignored) {
                            // Best-effort cleanup of the one-shot JNI workspace.
                        }
                    });
        } catch (Exception ignored) {
            // Best-effort cleanup of the one-shot JNI workspace.
        }
    }
}
