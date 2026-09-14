package db.monacgraph.ingestion.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.IngestionStatus;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;
import db.monacgraph.ingestion.model.IngestionModels.PassageExtraction;
import db.monacgraph.runtime.LocalRuntime;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class RocksIngestionJobStore implements IngestionJobStore {
    private static final byte[] JOB_PREFIX = bytes("job:");

    static {
        LocalRuntime.install();
        RocksDB.loadLibrary();
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksIngestionJobStore(Path directory) {
        try {
            options = new Options().setCreateIfMissing(true);
            writeOptions = new WriteOptions().setSync(true);
            db = RocksDB.open(options, directory.toAbsolutePath().toString());
        } catch (RocksDBException e) {
            throw failure("open ingestion job store", e);
        }
    }

    @Override
    public void put(IngestionJob job) {
        write(jobKey(job.jobId()), job);
    }

    @Override
    public Optional<IngestionJob> get(String jobId) {
        return read(jobKey(jobId), IngestionJob.class);
    }

    @Override
    public void delete(String jobId) {
        try {
            db.delete(writeOptions, jobKey(jobId));
            db.delete(writeOptions, artifactKey(jobId, "parsed"));
            db.delete(writeOptions, artifactKey(jobId, "passages"));
            db.delete(writeOptions, artifactKey(jobId, "extractions"));
        } catch (RocksDBException e) {
            throw failure("delete ingestion job", e);
        }
    }

    @Override
    public List<IngestionJob> runnable(int maxAttempts, int limit) {
        List<IngestionJob> jobs = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(JOB_PREFIX);
                 iterator.isValid() && startsWith(iterator.key(), JOB_PREFIX);
                 iterator.next()) {
                IngestionJob job = mapper.readValue(iterator.value(), IngestionJob.class);
                if (job.status() != IngestionStatus.READY && job.attempts() < maxAttempts) {
                    jobs.add(job);
                }
            }
            iterator.status();
        } catch (Exception e) {
            throw failure("scan ingestion jobs", e);
        }
        return jobs.stream()
                .sorted(Comparator.comparingLong(IngestionJob::updatedAtEpochMillis))
                .limit(limit)
                .toList();
    }

    @Override
    public void saveParsed(String jobId, ParsedDocument document) {
        write(artifactKey(jobId, "parsed"), document);
    }

    @Override
    public Optional<ParsedDocument> parsed(String jobId) {
        return read(artifactKey(jobId, "parsed"), ParsedDocument.class);
    }

    @Override
    public void savePassages(String jobId, List<Passage> passages) {
        write(artifactKey(jobId, "passages"), passages);
    }

    @Override
    public List<Passage> passages(String jobId) {
        return readList(artifactKey(jobId, "passages"), new TypeReference<>() {});
    }

    @Override
    public void saveExtractions(String jobId, List<PassageExtraction> extractions) {
        write(artifactKey(jobId, "extractions"), extractions);
    }

    @Override
    public List<PassageExtraction> extractions(String jobId) {
        return readList(artifactKey(jobId, "extractions"), new TypeReference<>() {});
    }

    private <T> Optional<T> read(byte[] key, Class<T> type) {
        try {
            byte[] value = db.get(key);
            return value == null ? Optional.empty() : Optional.of(mapper.readValue(value, type));
        } catch (Exception e) {
            throw failure("read ingestion data", e);
        }
    }

    private <T> List<T> readList(byte[] key, TypeReference<List<T>> type) {
        try {
            byte[] value = db.get(key);
            return value == null ? List.of() : List.copyOf(mapper.readValue(value, type));
        } catch (Exception e) {
            throw failure("read ingestion artifact", e);
        }
    }

    private void write(byte[] key, Object value) {
        try {
            db.put(writeOptions, key, mapper.writeValueAsBytes(value));
        } catch (Exception e) {
            throw failure("write ingestion data", e);
        }
    }

    private static byte[] jobKey(String jobId) {
        return bytes("job:" + jobId);
    }

    private static byte[] artifactKey(String jobId, String type) {
        return bytes("artifact:" + jobId + ":" + type);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) return false;
        }
        return true;
    }

    private static IllegalStateException failure(String operation, Exception cause) {
        return new IllegalStateException("Failed to " + operation, cause);
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }
}
