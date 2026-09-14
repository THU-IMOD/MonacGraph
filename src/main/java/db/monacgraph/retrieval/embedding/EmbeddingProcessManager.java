package db.monacgraph.retrieval.embedding;

import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.runtime.LocalRuntime;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Starts and stops the optional bundled Python model process. */
public final class EmbeddingProcessManager implements AutoCloseable {
    private final Process process;

    public EmbeddingProcessManager(
            String pythonCommand,
            Path serviceDirectory,
            EmbeddingSpace space,
            int port) {
        List<String> command = new ArrayList<>();
        command.add(pythonCommand);
        command.add("-m");
        command.add("uvicorn");
        command.add("app:app");
        command.add("--host");
        command.add("127.0.0.1");
        command.add("--port");
        command.add(Integer.toString(port));
        this.process = start(command, serviceDirectory, space, port);
    }

    private static Process start(
            List<String> command,
            Path directory,
            EmbeddingSpace space,
            int port) {
        ProcessBuilder builder = new ProcessBuilder(command)
                .directory(directory.toFile())
                .inheritIO();
        builder.environment().put("EMBEDDING_MODEL", space.modelName());
        builder.environment().put("EMBEDDING_REVISION", space.modelRevision());
        builder.environment().put("EMBEDDING_QUERY_PROMPT", space.queryPromptTemplate());
        builder.environment().put("EMBEDDING_NORMALIZED", Boolean.toString(space.normalized()));
        builder.environment().put("EMBEDDING_PORT", Integer.toString(port));
        LocalRuntime.applyTo(builder);
        try {
            return builder.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start local embedding service", e);
        }
    }

    public void awaitHealthy(HttpEmbeddingProvider provider, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException("Embedding process exited with code " + process.exitValue());
            }
            if (provider.healthy()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while starting embedding service", e);
            }
        }
        throw new IllegalStateException("Embedding service did not become healthy within " + timeout);
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
