package db.monacgraph.ingestion;

import db.monacgraph.ingestion.chunk.ParagraphAwareChunker;
import db.monacgraph.ingestion.extraction.KnowledgeValidation;
import db.monacgraph.ingestion.extraction.OpenAiCompatibleKnowledgeExtractor;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.resolution.ExactAliasEntityResolver;
import db.monacgraph.ingestion.resolution.RocksEntityCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IngestionComponentsTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void chunkIdsAreStable() {
        ParsedDocument document = new ParsedDocument(
                "doc", "Title", "First paragraph.\n\nSecond paragraph.", "text/plain", Map.of());
        ParagraphAwareChunker chunker = new ParagraphAwareChunker(128, 16);
        List<Passage> first = chunker.split(document);
        List<Passage> second = chunker.split(document);
        assertEquals(first, second);
    }

    @Test
    void rejectsRelationsWithUnknownEndpoints() {
        ExtractedKnowledge invalid = new ExtractedKnowledge(
                List.of(new ExtractedEntity("e1", "Apple", "company", List.of())),
                List.of(new ExtractedRelation("e1", "founded-by", "missing", "")));
        assertThrows(IllegalArgumentException.class, () -> KnowledgeValidation.validate(invalid));
    }

    @Test
    void repairsUniqueEntityNamesUsedAsRelationEndpoints() {
        ExtractedKnowledge raw = new ExtractedKnowledge(
                List.of(
                        new ExtractedEntity("e1", "Scott Derrickson", "person", List.of()),
                        new ExtractedEntity("e2", "American", "nationality", List.of())),
                List.of(new ExtractedRelation(
                        "Scott Derrickson", "nationality", "American", "")));
        ExtractedKnowledge repaired = KnowledgeValidation.validate(
                KnowledgeValidation.repairUnambiguousEndpoints(raw));
        assertEquals("e1", repaired.relations().get(0).source());
        assertEquals("e2", repaired.relations().get(0).target());
    }

    @Test
    void dropsSelfLoopsAndUnknownEndpoints() {
        ExtractedKnowledge raw = new ExtractedKnowledge(
                List.of(new ExtractedEntity("e1", "Scott Derrickson", "person", List.of())),
                List.of(
                        new ExtractedRelation("e1", "birthdate", "e1", "July 16, 1966"),
                        new ExtractedRelation("e1", "related-to", "missing", "")));
        ExtractedKnowledge cleaned = KnowledgeValidation.validate(
                KnowledgeValidation.dropInvalidRelations(raw));
        assertEquals(0, cleaned.relations().size());
        assertEquals(1, cleaned.entities().size());
    }

    @Test
    void materializesUndeclaredConceptEndpointsDeterministically() {
        ExtractedKnowledge raw = new ExtractedKnowledge(
                List.of(new ExtractedEntity(
                        "e1", "Scott Derrickson", "person", List.of())),
                List.of(new ExtractedRelation(
                        "e1", "nationality", "American", "")));
        ExtractedKnowledge repaired = KnowledgeValidation.validate(
                KnowledgeValidation.materializeMissingEndpoints(raw));
        assertEquals(2, repaired.entities().size());
        assertEquals(
                repaired.entities().get(1).localId(),
                repaired.relations().get(0).target());
    }

    @Test
    void mergesAnUnambiguousAlias() {
        try (RocksEntityCatalog catalog = new RocksEntityCatalog(temporaryDirectory.resolve("catalog"))) {
            ExactAliasEntityResolver resolver = new ExactAliasEntityResolver(catalog);
            var first = resolver.resolve(new ExtractedEntity(
                    "e1", "Apple Inc.", "company", List.of("Apple")));
            var second = resolver.resolve(new ExtractedEntity(
                    "e2", "Apple", "company", List.of()));
            assertEquals(first.vertexId(), second.vertexId());
        }
    }

    @Test
    void parsesOpenAiCompatibleJsonResponse() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            byte[] response = """
                    {"choices":[{"message":{"content":"{\\"entities\\":[{\\"localId\\":\\"e1\\",\\"name\\":\\"Apple\\",\\"type\\":\\"company\\",\\"aliases\\":[]}],\\"relations\\":[]}"}}]}
                    """.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var extractor = new OpenAiCompatibleKnowledgeExtractor(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()
                            + "/v1/chat/completions"),
                    "test-model", "", Duration.ofSeconds(5), 1);
            var result = extractor.extract(new Passage(
                    "c1", "d1", "Apple is a company.", 0, 19, "test"));
            assertEquals("Apple", result.entities().get(0).name());
        } finally {
            server.stop(0);
        }
    }
}
