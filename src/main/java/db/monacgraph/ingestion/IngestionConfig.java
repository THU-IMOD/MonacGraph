package db.monacgraph.ingestion;

import org.apache.commons.configuration2.Configuration;

import java.net.URI;
import java.time.Duration;

public record IngestionConfig(
        URI extractorEndpoint,
        String extractorModel,
        String extractorApiKey,
        Duration extractorTimeout,
        int extractorConcurrency,
        int chunkCharacters,
        int chunkOverlap,
        int embeddingBatchSize,
        int maxAttempts,
        boolean workerEnabled,
        Duration workerInterval) {

    public static IngestionConfig from(Configuration configuration) {
        String apiKey = configuration.getString("ingestion.extractor.apiKey", "");
        String apiKeyEnv = configuration.getString("ingestion.extractor.apiKeyEnv", "");
        if (apiKey.isBlank() && !apiKeyEnv.isBlank()) {
            apiKey = System.getenv().getOrDefault(apiKeyEnv, "");
        }
        return new IngestionConfig(
                URI.create(configuration.getString(
                        "ingestion.extractor.url",
                        "http://127.0.0.1:11434/v1/chat/completions")),
                configuration.getString("ingestion.extractor.model", "qwen3:8b"),
                apiKey,
                Duration.ofSeconds(configuration.getInt("ingestion.extractor.timeoutSeconds", 120)),
                configuration.getInt("ingestion.extractor.maxConcurrency", 2),
                configuration.getInt("ingestion.chunk.maxCharacters", 1600),
                configuration.getInt("ingestion.chunk.overlapCharacters", 200),
                configuration.getInt("ingestion.embedding.batchSize", 32),
                configuration.getInt("ingestion.maxAttempts", 3),
                configuration.getBoolean("ingestion.worker.enabled", true),
                Duration.ofMillis(configuration.getLong("ingestion.worker.intervalMillis", 1000)));
    }
}
