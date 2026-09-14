package db.monacgraph.retrieval.hipporag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** HippoRAG 1 query NER over the same OpenAI-compatible chat endpoint. */
public final class OpenAiCompatibleQueryNer implements QueryNer {
    private static final String PROMPT = """
            Extract named entities from the question.
            Return only one JSON object: {"named_entities":["..."]}
            Use surface forms from the question. If none, return {"named_entities":[]}.
            """;

    private final URI endpoint;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public OpenAiCompatibleQueryNer(URI endpoint, String model, String apiKey, Duration timeout) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.model = Objects.requireNonNull(model, "model");
        this.apiKey = apiKey == null ? "" : apiKey;
        this.timeout = timeout == null ? Duration.ofSeconds(60) : timeout;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(this.timeout)
                .build();
    }

    @Override
    public List<String> namedEntities(String query) {
        try {
            Map<String, Object> body = Map.of(
                    "model", model,
                    "temperature", 0,
                    "response_format", Map.of("type", "json_object"),
                    "messages", List.of(
                            Map.of("role", "system", "content", PROMPT),
                            Map.of("role", "user", "content", query)));
            HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint)
                    .timeout(timeout)
                    .header("Content-Type", "application/json");
            if (!apiKey.isBlank()) {
                builder.header("Authorization", "Bearer " + apiKey);
            }
            HttpResponse<byte[]> response = client.send(
                    builder.POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body))).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2) {
                return List.of();
            }
            JsonNode content = mapper.readTree(response.body())
                    .path("choices").path(0).path("message").path("content");
            if (!content.isTextual()) {
                return List.of();
            }
            JsonNode entities = mapper.readTree(stripFence(content.textValue())).path("named_entities");
            List<String> names = new ArrayList<>();
            if (entities.isArray()) {
                entities.forEach(node -> {
                    if (node.isTextual() && !node.textValue().isBlank()) {
                        names.add(node.textValue().trim());
                    }
                });
            }
            return List.copyOf(names);
        } catch (IOException e) {
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    private static String stripFence(String value) {
        String trimmed = value == null ? "" : value.trim();
        if (!trimmed.startsWith("```")) {
            return trimmed;
        }
        int firstNewline = trimmed.indexOf('\n');
        int closing = trimmed.lastIndexOf("```");
        if (firstNewline < 0 || closing <= firstNewline) {
            return trimmed;
        }
        return trimmed.substring(firstNewline + 1, closing).trim();
    }
}
