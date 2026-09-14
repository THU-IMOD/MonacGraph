package db.monacgraph.retrieval;

import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import org.apache.commons.configuration2.Configuration;

import java.net.URI;
import java.nio.file.Path;

public record RetrievalConfig(
        Path rootDirectory,
        URI embeddingServiceUri,
        EmbeddingSpace embeddingSpace,
        boolean manageEmbeddingProcess,
        String pythonCommand,
        Path embeddingServiceDirectory) {

    public static RetrievalConfig from(Configuration configuration, String graphName) {
        Path root = Path.of(configuration.getString(
                "retrieval.path", "./workspace/" + graphName + "/retrieval"));
        String model = configuration.getString(
                "retrieval.embedding.model", "BAAI/bge-small-zh-v1.5");
        String revision = configuration.getString("retrieval.embedding.revision", "main");
        int dimension = configuration.getInt("retrieval.embedding.dimension", 512);
        boolean normalized = configuration.getBoolean("retrieval.embedding.normalized", true);
        String prompt = configuration.getString(
                "retrieval.embedding.queryPrompt", "为这个句子生成表示以用于检索相关文章：");
        EmbeddingSpace space = new EmbeddingSpace(
                configuration.getString("retrieval.embedding.spaceId", "bge-small-zh-v1.5-v1"),
                model, revision, dimension, normalized, prompt);
        return new RetrievalConfig(
                root,
                URI.create(configuration.getString(
                        "retrieval.embedding.uri", "http://127.0.0.1:8099/")),
                space,
                configuration.getBoolean("retrieval.embedding.manageProcess", false),
                configuration.getString("retrieval.embedding.python", "python"),
                Path.of(configuration.getString("retrieval.embedding.servicePath", "./embedding-service")));
    }
}
