package db.monacgraph.retrieval.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingJob;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingStatus;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.runtime.LocalRuntime;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Authoritative passage store. Forward links, reverse links and embedding jobs
 * are committed atomically in one RocksDB write batch.
 */
public final class RocksContentStore implements ContentStore {
    private static final byte[] JOB = bytes("job:");

    static {
        LocalRuntime.install();
        RocksDB.loadLibrary();
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksContentStore(Path directory) {
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.writeOptions = new WriteOptions().setSync(true);
            this.db = RocksDB.open(options, directory.toAbsolutePath().toString());
        } catch (RocksDBException e) {
            throw failure("open content store", e);
        }
    }

    @Override
    public synchronized void put(ContentRecord content) {
        try (WriteBatch batch = new WriteBatch()) {
            get(content.contentId()).ifPresent(previous -> removeReverseLinks(batch, previous));
            batch.put(contentKey(content.contentId()), json(content));
            for (ContentElementLink link : content.links()) {
                batch.put(reverseKey(link.element(), content.contentId()), new byte[0]);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw failure("write content " + content.contentId(), e);
        }
    }

    @Override
    public Optional<ContentRecord> get(String contentId) {
        try {
            byte[] value = db.get(contentKey(contentId));
            return value == null ? Optional.empty() : Optional.of(fromJson(value, ContentRecord.class));
        } catch (RocksDBException e) {
            throw failure("read content " + contentId, e);
        }
    }

    @Override
    public synchronized void delete(String contentId) {
        Optional<ContentRecord> existing = get(contentId);
        if (existing.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(contentKey(contentId));
            removeReverseLinks(batch, existing.get());
            removeEmbeddingJobs(batch, contentId);
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw failure("delete content " + contentId, e);
        }
    }

    @Override
    public List<String> contentIdsForElement(GraphElementRef element) {
        byte[] prefix = reversePrefix(element);
        List<String> ids = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid() && startsWith(iterator.key(), prefix); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                ids.add(decode(key.substring(key.lastIndexOf(':') + 1)));
            }
            iterator.status();
            return ids;
        } catch (RocksDBException e) {
            throw failure("scan reverse content links", e);
        }
    }

    @Override
    public void enqueueEmbedding(String contentId, EmbeddingSpace space) {
        ContentRecord content = get(contentId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown contentId: " + contentId));
        EmbeddingJob job = new EmbeddingJob(
                contentId, space.spaceId(), content.contentHash(), EmbeddingStatus.PENDING, null);
        putValue(jobKey(contentId, space.spaceId()), job);
    }

    @Override
    public List<EmbeddingJob> pendingEmbeddings(String spaceId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        byte[] prefix = jobPrefix(spaceId);
        List<EmbeddingJob> jobs = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix);
                 iterator.isValid() && startsWith(iterator.key(), prefix) && jobs.size() < limit;
                 iterator.next()) {
                EmbeddingJob job = fromJson(iterator.value(), EmbeddingJob.class);
                if (job.status() == EmbeddingStatus.PENDING) {
                    jobs.add(job);
                }
            }
            iterator.status();
            return jobs;
        } catch (RocksDBException e) {
            throw failure("scan embedding jobs", e);
        }
    }

    @Override
    public Optional<EmbeddingJob> embeddingJob(String contentId, String spaceId) {
        try {
            byte[] value = db.get(jobKey(contentId, spaceId));
            return value == null
                    ? Optional.empty()
                    : Optional.of(fromJson(value, EmbeddingJob.class));
        } catch (RocksDBException e) {
            throw failure("read embedding job", e);
        }
    }

    @Override
    public void markEmbeddingReady(String contentId, String spaceId) {
        updateJob(contentId, spaceId, EmbeddingStatus.READY, null);
    }

    @Override
    public void markEmbeddingFailed(String contentId, String spaceId, String error) {
        updateJob(contentId, spaceId, EmbeddingStatus.FAILED, error);
    }

    private void updateJob(String contentId, String spaceId, EmbeddingStatus status, String error) {
        ContentRecord content = get(contentId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown contentId: " + contentId));
        putValue(jobKey(contentId, spaceId),
                new EmbeddingJob(contentId, spaceId, content.contentHash(), status, error));
    }

    private void putValue(byte[] key, Object value) {
        try {
            db.put(writeOptions, key, json(value));
        } catch (RocksDBException e) {
            throw failure("write retrieval metadata", e);
        }
    }

    private void removeReverseLinks(WriteBatch batch, ContentRecord content) {
        for (ContentElementLink link : content.links()) {
            try {
                batch.delete(reverseKey(link.element(), content.contentId()));
            } catch (RocksDBException e) {
                throw failure("remove reverse content link", e);
            }
        }
    }

    private void removeEmbeddingJobs(WriteBatch batch, String contentId) {
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(JOB); iterator.isValid() && startsWith(iterator.key(), JOB); iterator.next()) {
                EmbeddingJob job = fromJson(iterator.value(), EmbeddingJob.class);
                if (contentId.equals(job.contentId())) {
                    batch.delete(iterator.key());
                }
            }
            iterator.status();
        } catch (RocksDBException e) {
            throw failure("remove embedding jobs", e);
        }
    }

    private byte[] json(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw failure("serialize retrieval metadata", e);
        }
    }

    private <T> T fromJson(byte[] value, Class<T> type) {
        try {
            return mapper.readValue(value, type);
        } catch (Exception e) {
            throw failure("deserialize retrieval metadata", e);
        }
    }

    private static byte[] contentKey(String contentId) {
        return bytes("content:" + encode(contentId));
    }

    private static byte[] reversePrefix(GraphElementRef element) {
        return bytes("reverse:" + ElementIdCodec.encode(element) + ":");
    }

    private static byte[] reverseKey(GraphElementRef element, String contentId) {
        return bytes("reverse:" + ElementIdCodec.encode(element) + ":" + encode(contentId));
    }

    private static byte[] jobPrefix(String spaceId) {
        return bytes("job:" + encode(spaceId) + ":");
    }

    private static byte[] jobKey(String contentId, String spaceId) {
        return bytes("job:" + encode(spaceId) + ":" + encode(contentId));
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
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
