package db.monacgraph.ingestion;

import db.monacgraph.ingestion.chunk.Chunker;
import db.monacgraph.ingestion.document.DocumentParser;
import db.monacgraph.ingestion.extraction.KnowledgeExtractor;
import db.monacgraph.ingestion.graph.ExtractedGraphWriter;
import db.monacgraph.ingestion.job.IngestionJobStore;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.IngestionStatus;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.model.IngestionModels.PassageExtraction;
import db.monacgraph.retrieval.RetrievalServices;
import db.monacgraph.retrieval.hipporag.HippoRagIndexer;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingJob;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Durable, restartable orchestration from a source document to searchable graph data. */
public final class IngestionCoordinator {
    private final DocumentParser parser;
    private final Chunker chunker;
    private final KnowledgeExtractor extractor;
    private final ExtractedGraphWriter graphWriter;
    private final IngestionJobStore jobs;
    private final RetrievalServices retrieval;
    private final EmbeddingSpace space;
    private final int embeddingBatchSize;
    private final int maxAttempts;
    private final HippoRagIndexer hippoRagIndexer;

    public IngestionCoordinator(
            DocumentParser parser,
            Chunker chunker,
            KnowledgeExtractor extractor,
            ExtractedGraphWriter graphWriter,
            IngestionJobStore jobs,
            RetrievalServices retrieval,
            EmbeddingSpace space,
            int embeddingBatchSize,
            int maxAttempts) {
        this(parser, chunker, extractor, graphWriter, jobs, retrieval, space,
                embeddingBatchSize, maxAttempts, null);
    }

    public IngestionCoordinator(
            DocumentParser parser,
            Chunker chunker,
            KnowledgeExtractor extractor,
            ExtractedGraphWriter graphWriter,
            IngestionJobStore jobs,
            RetrievalServices retrieval,
            EmbeddingSpace space,
            int embeddingBatchSize,
            int maxAttempts,
            HippoRagIndexer hippoRagIndexer) {
        this.parser = parser;
        this.chunker = chunker;
        this.extractor = extractor;
        this.graphWriter = graphWriter;
        this.jobs = jobs;
        this.retrieval = retrieval;
        this.space = space;
        this.embeddingBatchSize = embeddingBatchSize;
        this.maxAttempts = maxAttempts;
        this.hippoRagIndexer = hippoRagIndexer;
    }

    public IngestionJob submit(Path source) {
        Path normalized = source.toAbsolutePath().normalize();
        String hash = fileHash(normalized);
        String documentId = IngestionIds.deterministicUuid("document", normalized + ":" + hash);
        String jobId = IngestionIds.deterministicUuid("ingestion-job", documentId);
        Optional<IngestionJob> existing = jobs.get(jobId);
        if (existing.isPresent()) {
            return existing.get();
        }
        IngestionJob created = new IngestionJob(
                jobId, documentId, normalized.toString(), hash,
                IngestionStatus.PENDING, IngestionStatus.PENDING,
                0, null, System.currentTimeMillis());
        jobs.put(created);
        return created;
    }

    public Optional<IngestionJob> status(String jobId) {
        return jobs.get(jobId);
    }

    public List<PassageExtraction> extractions(String jobId) {
        return jobs.extractions(jobId);
    }

    /**
     * Removes document passages and job artifacts. Shared graph entities and
     * facts are retained; orphan cleanup requires global provenance analysis.
     */
    public void delete(String jobId) {
        IngestionJob job = jobs.get(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown ingestion job: " + jobId));
        for (Passage passage : jobs.passages(job.jobId())) {
            retrieval.ingestor().delete(passage.contentId());
        }
        jobs.delete(jobId);
    }

    public IngestionJob run(String jobId) {
        IngestionJob job = jobs.get(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown ingestion job: " + jobId));
        if (job.status() == IngestionStatus.READY) {
            return job;
        }
        IngestionStatus checkpoint = job.checkpoint();
        job = update(job, checkpoint, checkpoint, job.attempts() + 1, null);
        try {
            ParsedDocument parsed = loadOrParse(job, checkpoint);
            if (before(checkpoint, IngestionStatus.PARSED)) {
                jobs.saveParsed(jobId, parsed);
                job = transition(job, IngestionStatus.PARSED);
                checkpoint = job.checkpoint();
            }

            List<Passage> passages = loadOrChunk(jobId, parsed, checkpoint);
            if (passages.isEmpty()) {
                throw new IllegalStateException("Document produced no non-empty passages");
            }
            if (before(checkpoint, IngestionStatus.CHUNKED)) {
                jobs.savePassages(jobId, passages);
                job = transition(job, IngestionStatus.CHUNKED);
                checkpoint = job.checkpoint();
            }

            List<PassageExtraction> extractions = loadOrExtract(jobId, passages, checkpoint);
            if (extractions.size() != passages.size()) {
                throw new IllegalStateException("Persisted extraction count does not match passages");
            }
            if (before(checkpoint, IngestionStatus.EXTRACTED)) {
                jobs.saveExtractions(jobId, extractions);
                job = transition(job, IngestionStatus.EXTRACTED);
                checkpoint = job.checkpoint();
            }

            List<ContentRecord> contents = new ArrayList<>();
            if (before(checkpoint, IngestionStatus.INDEXED)) {
                for (PassageExtraction extraction : extractions) {
                    contents.add(graphWriter.write(extraction.passage(), extraction.knowledge()));
                }
                if (before(checkpoint, IngestionStatus.GRAPH_WRITTEN)) {
                    job = transition(job, IngestionStatus.GRAPH_WRITTEN);
                }
                for (ContentRecord content : contents) {
                    retrieval.ingestor().ingest(content, space);
                }
                job = transition(job, IngestionStatus.INDEXED);
            } else {
                for (PassageExtraction extraction : extractions) {
                    contents.add(graphWriter.write(extraction.passage(), extraction.knowledge()));
                }
            }

            requeueFailedEmbeddings(contents);
            while (retrieval.embeddingIndexer().runBatch(embeddingBatchSize) > 0) {
                // Drain all currently pending vectors, including work from recovered jobs.
            }
            assertEmbeddingsReady(contents);
            if (hippoRagIndexer != null) {
                hippoRagIndexer.refresh(retrieval.embeddingProvider());
            }
            return transition(job, IngestionStatus.READY);
        } catch (RuntimeException e) {
            IngestionJob failed = new IngestionJob(
                    job.jobId(), job.documentId(), job.sourcePath(), job.contentHash(),
                    IngestionStatus.FAILED, job.checkpoint(), job.attempts(),
                    summarizeError(e), System.currentTimeMillis());
            jobs.put(failed);
            return failed;
        }
    }

    public List<IngestionJob> runPending(int limit) {
        List<IngestionJob> results = new ArrayList<>();
        for (IngestionJob job : jobs.runnable(maxAttempts, limit)) {
            results.add(run(job.jobId()));
        }
        return List.copyOf(results);
    }

    public IngestionJob awaitReady(String jobId, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            IngestionJob job = status(jobId)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown ingestion job: " + jobId));
            if (job.status() == IngestionStatus.READY || job.status() == IngestionStatus.FAILED) {
                return job;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for ingestion", e);
            }
        }
        throw new IllegalStateException("Ingestion did not finish within " + timeout);
    }

    private ParsedDocument loadOrParse(IngestionJob job, IngestionStatus checkpoint) {
        if (!before(checkpoint, IngestionStatus.PARSED)) {
            return jobs.parsed(job.jobId()).orElseThrow(() ->
                    new IllegalStateException("Missing persisted parsed document"));
        }
        Path path = Path.of(job.sourcePath());
        if (!fileHash(path).equals(job.contentHash())) {
            throw new IllegalStateException("Source file changed after ingestion submission");
        }
        ParsedDocument value = parser.parse(path);
        return new ParsedDocument(
                job.documentId(), value.title(), value.text(), value.mediaType(), value.metadata());
    }

    private List<Passage> loadOrChunk(
            String jobId, ParsedDocument parsed, IngestionStatus checkpoint) {
        if (!before(checkpoint, IngestionStatus.CHUNKED)) {
            return jobs.passages(jobId);
        }
        return chunker.split(parsed);
    }

    private List<PassageExtraction> loadOrExtract(
            String jobId, List<Passage> passages, IngestionStatus checkpoint) {
        if (!before(checkpoint, IngestionStatus.EXTRACTED)) {
            return jobs.extractions(jobId);
        }
        return passages.stream()
                .map(passage -> {
                    ExtractedKnowledge knowledge = extractor.extract(passage);
                    return new PassageExtraction(passage, knowledge);
                })
                .toList();
    }

    private void requeueFailedEmbeddings(List<ContentRecord> contents) {
        for (ContentRecord content : contents) {
            retrieval.contentStore().embeddingJob(content.contentId(), space.spaceId())
                    .filter(job -> job.status() == EmbeddingStatus.FAILED)
                    .ifPresent(job -> retrieval.contentStore().enqueueEmbedding(content.contentId(), space));
        }
    }

    private void assertEmbeddingsReady(List<ContentRecord> contents) {
        for (ContentRecord content : contents) {
            EmbeddingJob job = retrieval.contentStore()
                    .embeddingJob(content.contentId(), space.spaceId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Missing embedding job for " + content.contentId()));
            if (job.status() != EmbeddingStatus.READY) {
                throw new IllegalStateException(
                        "Embedding is not ready for " + content.contentId() + ": " + job.status());
            }
        }
    }

    private IngestionJob transition(IngestionJob job, IngestionStatus status) {
        return update(job, status, status, job.attempts(), null);
    }

    private IngestionJob update(
            IngestionJob job,
            IngestionStatus status,
            IngestionStatus checkpoint,
            int attempts,
            String error) {
        IngestionJob updated = new IngestionJob(
                job.jobId(), job.documentId(), job.sourcePath(), job.contentHash(),
                status, checkpoint, attempts, error, System.currentTimeMillis());
        jobs.put(updated);
        return updated;
    }

    private static boolean before(IngestionStatus current, IngestionStatus target) {
        return current.ordinal() < target.ordinal();
    }

    private static String summarizeError(Throwable error) {
        StringBuilder summary = new StringBuilder(error.getClass().getSimpleName());
        if (error.getMessage() != null && !error.getMessage().isBlank()) {
            summary.append(": ").append(error.getMessage());
        }
        if (error.getCause() != null && error.getCause().getMessage() != null) {
            summary.append(" (").append(error.getCause().getClass().getSimpleName())
                    .append(": ").append(error.getCause().getMessage()).append(')');
        }
        return summary.toString();
    }

    private static String fileHash(Path path) {
        try {
            return IngestionIds.sha256(java.util.Base64.getEncoder().encodeToString(Files.readAllBytes(path)));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read source document " + path, e);
        }
    }
}
