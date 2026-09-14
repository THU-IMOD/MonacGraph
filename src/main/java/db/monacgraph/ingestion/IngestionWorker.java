package db.monacgraph.ingestion;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class IngestionWorker implements AutoCloseable {
    private final IngestionCoordinator coordinator;
    private final int batchSize;
    private final ScheduledExecutorService executor;

    public IngestionWorker(
            IngestionCoordinator coordinator, int batchSize, Duration interval) {
        this.coordinator = coordinator;
        this.batchSize = batchSize;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "monacgraph-ingestion");
            thread.setDaemon(true);
            return thread;
        });
        long delay = Math.max(100, interval.toMillis());
        executor.scheduleWithFixedDelay(this::runSafely, delay, delay, TimeUnit.MILLISECONDS);
    }

    private void runSafely() {
        try {
            coordinator.runPending(batchSize);
        } catch (RuntimeException ignored) {
            // Individual failures are persisted on the job and retried by policy.
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
