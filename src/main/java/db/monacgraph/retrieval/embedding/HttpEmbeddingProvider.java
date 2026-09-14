package db.monacgraph.retrieval.embedding;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Client for the bundled local Python embedding service. */
public final class HttpEmbeddingProvider implements EmbeddingProvider {
    private final URI baseUri;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    private final EmbeddingSpace configuredSpace;
    private volatile EmbeddingSpace verifiedSpace;

    public HttpEmbeddingProvider(URI baseUri, EmbeddingSpace configuredSpace) {
        this.baseUri = baseUri;
        this.configuredSpace = configuredSpace;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public EmbeddingSpace space() {
        EmbeddingSpace cached = verifiedSpace;
        if (cached != null) {
            return cached;
        }
        ModelInfo info = get("/model-info", ModelInfo.class);
        if (!configuredSpace.modelName().equals(info.modelName())
                || !configuredSpace.modelRevision().equals(info.modelRevision())
                || configuredSpace.dimension() != info.dimension()
                || configuredSpace.normalized() != info.normalized()
                || !configuredSpace.queryPromptTemplate().equals(info.queryPrompt())) {
            throw new IllegalStateException("Embedding service model does not match configured space "
                    + configuredSpace.spaceId());
        }
        verifiedSpace = configuredSpace;
        return configuredSpace;
    }

    @Override
    public List<float[]> embedDocuments(List<String> texts) {
        space();
        EmbeddingResponse response = post("/embed/documents", java.util.Map.of("texts", texts), EmbeddingResponse.class);
        validate(response.embeddings());
        return response.embeddings();
    }

    @Override
    public float[] embedQuery(String query) {
        space();
        EmbeddingResponse response = post(
                "/embed/query", java.util.Map.of("texts", List.of(query)), EmbeddingResponse.class);
        validate(response.embeddings());
        if (response.embeddings().size() != 1) {
            throw new IllegalStateException("Expected one query embedding");
        }
        return response.embeddings().get(0);
    }

    public boolean healthy() {
        try {
            get("/health", Health.class);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void validate(List<float[]> embeddings) {
        for (float[] vector : embeddings) {
            if (vector.length != configuredSpace.dimension()) {
                throw new IllegalStateException("Embedding service returned " + vector.length
                        + " dimensions; expected " + configuredSpace.dimension());
            }
        }
    }

    private <T> T get(String path, Class<T> responseType) {
        HttpRequest request = HttpRequest.newBuilder(baseUri.resolve(path))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        return send(request, responseType);
    }

    private <T> T post(String path, Object body, Class<T> responseType) {
        try {
            HttpRequest request = HttpRequest.newBuilder(baseUri.resolve(path))
                    .timeout(Duration.ofMinutes(2))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body)))
                    .build();
            return send(request, responseType);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize embedding request", e);
        }
    }

    private <T> T send(HttpRequest request, Class<T> responseType) {
        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("Embedding service returned HTTP "
                        + response.statusCode() + ": "
                        + new String(response.body(), StandardCharsets.UTF_8));
            }
            return mapper.readValue(response.body(), responseType);
        } catch (IOException e) {
            throw new IllegalStateException("Embedding service request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Embedding service request interrupted", e);
        }
    }

    private record EmbeddingResponse(List<float[]> embeddings) {}

    private record Health(String status) {}

    private record ModelInfo(
            @JsonProperty("model_name") String modelName,
            @JsonProperty("model_revision") String modelRevision,
            int dimension,
            boolean normalized,
            @JsonProperty("query_prompt") String queryPrompt) {}
}
