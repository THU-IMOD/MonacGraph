package db.monacgraph.ingestion.job;

import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.model.IngestionModels.PassageExtraction;

import java.util.List;
import java.util.Optional;

public interface IngestionJobStore extends AutoCloseable {
    void put(IngestionJob job);

    Optional<IngestionJob> get(String jobId);

    void delete(String jobId);

    List<IngestionJob> runnable(int maxAttempts, int limit);

    void saveParsed(String jobId, ParsedDocument document);

    Optional<ParsedDocument> parsed(String jobId);

    void savePassages(String jobId, List<Passage> passages);

    List<Passage> passages(String jobId);

    void saveExtractions(String jobId, List<PassageExtraction> extractions);

    List<PassageExtraction> extractions(String jobId);

    @Override
    void close();
}
