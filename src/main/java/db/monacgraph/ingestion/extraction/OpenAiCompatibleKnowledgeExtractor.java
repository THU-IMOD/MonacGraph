package db.monacgraph.ingestion.extraction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.Passage;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Two-step OpenAI-compatible extraction: entities first, then relations only
 * among those entities. Follows GraphRAG / LightRAG staging without injecting
 * a closed entity-type list.
 */
public final class OpenAiCompatibleKnowledgeExtractor implements KnowledgeExtractor {
    private static final String ENTITY_PROMPT = """
            Identify clearly mentioned entities in the passage.
            Return only one JSON object with this schema:
            {
              "entities": [{"localId":"e1","name":"...","type":"...","aliases":[],"description":"..."}]
            }
            Rules:
            - Give each entity a unique localId.
            - type is a short free-text label you choose from the passage; do not invent a taxonomy.
            - Put attributes, roles, and other details in description, not in extra placeholder entities.
            - Do not emit relations in this step.
            - Do not infer entities that are not explicitly supported by the passage.
            """;

    private static final String RELATION_PROMPT = """
            Given a passage and a frozen entity list, identify pairs that are clearly related.
            Return only one JSON object with this schema:
            {
              "relations": [{"source":"<localId>","relation":"...","target":"<localId>","evidence":"..."}]
            }
            Rules:
            - source and target must be two different localId values from the provided entity list.
            - Never use surface names as endpoints.
            - Only emit a relation when the passage states a relationship between those two entities.
            - evidence must be a short span from the passage that supports that pair.
            - Do not attach a fact about one entity onto another entity that is merely nearby.
            - Do not emit self-loops. If no clearly related pair exists, return {"relations":[]}.
            - Do not add, remove, or rename entities.
            """;

    private final URI endpoint;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final Semaphore concurrency;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public OpenAiCompatibleKnowledgeExtractor(
            URI endpoint,
            String model,
            String apiKey,
            Duration timeout,
            int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        this.endpoint = endpoint;
        this.model = model;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.concurrency = new Semaphore(maxConcurrency);
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .build();
    }

    @Override
    public ExtractedKnowledge extract(Passage passage) {
        boolean acquired = false;
        try {
            concurrency.acquire();
            acquired = true;
            ExtractedKnowledge entitiesOnly = request(
                    ENTITY_PROMPT, "PASSAGE:\n" + passage.text());
            List<ExtractedEntity> entities = entitiesOnly.entities();
            if (entities.isEmpty()) {
                return new ExtractedKnowledge(List.of(), List.of());
            }
            ExtractedKnowledge relationsOnly = request(
                    RELATION_PROMPT,
                    "PASSAGE:\n" + passage.text()
                            + "\n\nENTITIES:\n"
                            + mapper.writeValueAsString(entities));
            ExtractedKnowledge merged = new ExtractedKnowledge(entities, relationsOnly.relations());
            return KnowledgeValidation.validate(
                    KnowledgeValidation.dropInvalidRelations(
                            KnowledgeValidation.repairUnambiguousEndpoints(merged)));
        } catch (IOException e) {
            throw new IllegalStateException("Knowledge extraction request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Knowledge extraction interrupted", e);
        } finally {
            if (acquired) {
                concurrency.release();
            }
        }
    }

    private ExtractedKnowledge request(String systemPrompt, String userContent)
            throws IOException, InterruptedException {
        Map<String, Object> requestBody = Map.of(
                "model", model,
                "temperature", 0,
                "response_format", Map.of("type", "json_object"),
                "messages", List.of(
                        Map.of("role", "system", "content", systemPrompt),
                        Map.of("role", "user", "content", userContent)));
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json");
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        HttpRequest request = builder.POST(
                HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(requestBody))).build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("Extractor returned HTTP " + response.statusCode());
        }
        JsonNode root = mapper.readTree(response.body());
        JsonNode content = root.path("choices").path(0).path("message").path("content");
        if (!content.isTextual()) {
            throw new IllegalStateException("Extractor response has no choices[0].message.content");
        }
        return mapper.readValue(stripFence(content.textValue()), ExtractedKnowledge.class);
    }

    private static String stripFence(String value) {
        String trimmed = value.trim();
        if (!trimmed.startsWith("```")) {
            return trimmed;
        }
        int firstNewline = trimmed.indexOf('\n');
        int closing = trimmed.lastIndexOf("```");
        if (firstNewline < 0 || closing <= firstNewline) {
            throw new IllegalArgumentException("Malformed fenced JSON response");
        }
        return trimmed.substring(firstNewline + 1, closing).trim();
    }
}
