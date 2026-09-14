package db.monacgraph.retrieval.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import db.monacgraph.retrieval.model.PipelineModels.AnswerResult;
import db.monacgraph.retrieval.model.PipelineModels.Citation;
import db.monacgraph.retrieval.model.PipelineModels.EvidenceResult;
import db.monacgraph.retrieval.model.PipelineModels.GenerationMetadata;
import db.monacgraph.retrieval.model.PipelineModels.GenerationRequest;
import db.monacgraph.retrieval.pipeline.BudgetAndEvidence;

/** Minimal OpenAI-compatible generation over recalled passages. */
public final class OpenAiCompatibleAnswerGenerator implements AnswerGenerator {
    private static final String SYSTEM_PROMPT = """
            Answer the question using only the supplied evidence passages.
            If it is a yes/no question, put yes or no on the first line.
            If the evidence is insufficient, say so.
            Do not invent facts that are not in the evidence.
            """;

    private final URI endpoint;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public OpenAiCompatibleAnswerGenerator(URI endpoint, String model, String apiKey, Duration timeout) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.model = Objects.requireNonNull(model, "model");
        this.apiKey = apiKey == null ? "" : apiKey;
        this.timeout = timeout == null ? Duration.ofSeconds(120) : timeout;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(this.timeout)
                .build();
    }

    @Override
    public AnswerResult generate(GenerationRequest request, EvidenceResult evidence) {
        try {
            String answer = request(userPrompt(request.query(), List.of(request.evidenceContext())));
            return new AnswerResult(
                    answer,
                    List.of(new Citation(0, answer.length(), BudgetAndEvidence.references(evidence))),
                    evidence,
                    new GenerationMetadata("openai-compatible", model, 0, 0, "stop"));
        } catch (Exception e) {
            return new AnswerResult(
                    "",
                    List.of(),
                    evidence,
                    new GenerationMetadata(
                            "openai-compatible",
                            model,
                            0,
                            0,
                            "error: " + e.getMessage()));
        }
    }

    public String answer(String question, List<String> passages) {
        if (question == null || question.isBlank()) {
            throw new IllegalArgumentException("question cannot be blank");
        }
        try {
            return request(userPrompt(question, passages));
        } catch (IOException e) {
            throw new IllegalStateException("Answer generation request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Answer generation interrupted", e);
        }
    }

    private static String userPrompt(String question, List<String> passages) {
        StringBuilder user = new StringBuilder("Question:\n").append(question).append("\n\nEvidence:\n");
        if (passages == null || passages.isEmpty()) {
            user.append("(none)");
        } else {
            for (int i = 0; i < passages.size(); i++) {
                String passage = passages.get(i);
                if (passage == null || passage.isBlank()) {
                    continue;
                }
                if (passage.contains("[1]")) {
                    user.append(passage);
                } else {
                    user.append("[").append(i + 1).append("]\n").append(passage).append("\n\n");
                }
            }
        }
        return user.toString();
    }

    private String request(String userContent) throws IOException, InterruptedException {
        Map<String, Object> body = Map.of(
                "model", model,
                "temperature", 0,
                "messages", List.of(
                        Map.of("role", "system", "content", SYSTEM_PROMPT),
                        Map.of("role", "user", "content", userContent)));
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json");
        if (!apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        HttpRequest request = builder.POST(
                HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body))).build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("Generator returned HTTP " + response.statusCode()
                    + ": " + new String(response.body()));
        }
        JsonNode content = mapper.readTree(response.body())
                .path("choices").path(0).path("message").path("content");
        if (!content.isTextual()) {
            throw new IllegalStateException("Generator response has no choices[0].message.content");
        }
        return stripThink(content.textValue());
    }

    private static String stripThink(String value) {
        String trimmed = value == null ? "" : value.trim();
        int start = trimmed.indexOf("<think>");
        int end = trimmed.indexOf("</think>");
        if (start >= 0 && end > start) {
            trimmed = (trimmed.substring(0, start) + trimmed.substring(end + "</think>".length())).trim();
        }
        return trimmed;
    }
}
