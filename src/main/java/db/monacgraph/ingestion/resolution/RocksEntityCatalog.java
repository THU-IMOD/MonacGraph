package db.monacgraph.ingestion.resolution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.runtime.LocalRuntime;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

public final class RocksEntityCatalog implements EntityCatalog {
    static {
        LocalRuntime.install();
        RocksDB.loadLibrary();
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksEntityCatalog(Path directory) {
        try {
            options = new Options().setCreateIfMissing(true);
            writeOptions = new WriteOptions().setSync(true);
            db = RocksDB.open(options, directory.toAbsolutePath().toString());
        } catch (RocksDBException e) {
            throw failure("open entity catalog", e);
        }
    }

    @Override
    public Optional<CanonicalEntity> findCanonical(String type, String normalizedName) {
        try {
            byte[] id = db.get(key("canonical", type, normalizedName));
            return id == null ? Optional.empty() : findById(string(id));
        } catch (RocksDBException e) {
            throw failure("read canonical entity", e);
        }
    }

    @Override
    public List<CanonicalEntity> findAlias(String type, String normalizedAlias) {
        try {
            byte[] value = db.get(key("alias", type, normalizedAlias));
            if (value == null) {
                return List.of();
            }
            List<String> ids = mapper.readValue(value, new TypeReference<>() {});
            List<CanonicalEntity> entities = new ArrayList<>();
            for (String id : ids) {
                findById(id).ifPresent(entities::add);
            }
            return List.copyOf(entities);
        } catch (Exception e) {
            throw failure("read entity alias", e);
        }
    }

    @Override
    public List<CanonicalEntity> listAll() {
        byte[] prefix = bytes("entity:");
        List<CanonicalEntity> entities = new ArrayList<>();
        try (var iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid() && startsWith(iterator.key(), prefix); iterator.next()) {
                entities.add(mapper.readValue(iterator.value(), CanonicalEntity.class));
            }
            iterator.status();
            return List.copyOf(entities);
        } catch (Exception e) {
            throw failure("list canonical entities", e);
        }
    }

    @Override
    public synchronized void put(CanonicalEntity entity) {
        String type = EntityNameNormalizer.normalize(entity.type());
        String name = EntityNameNormalizer.normalize(entity.canonicalName());
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(bytes("entity:" + entity.vertexId()), mapper.writeValueAsBytes(entity));
            batch.put(key("canonical", type, name), bytes(entity.vertexId()));
            for (String alias : entity.aliases()) {
                String normalized = EntityNameNormalizer.normalize(alias);
                if (!normalized.isBlank()) {
                    byte[] aliasKey = key("alias", type, normalized);
                    LinkedHashSet<String> ids = new LinkedHashSet<>();
                    byte[] previous = db.get(aliasKey);
                    if (previous != null) {
                        ids.addAll(mapper.readValue(previous, new TypeReference<List<String>>() {}));
                    }
                    ids.add(entity.vertexId());
                    batch.put(aliasKey, mapper.writeValueAsBytes(ids));
                }
            }
            db.write(writeOptions, batch);
        } catch (Exception e) {
            throw failure("write canonical entity", e);
        }
    }

    private Optional<CanonicalEntity> findById(String id) {
        try {
            byte[] value = db.get(bytes("entity:" + id));
            return value == null
                    ? Optional.empty()
                    : Optional.of(mapper.readValue(value, CanonicalEntity.class));
        } catch (Exception e) {
            throw failure("read entity " + id, e);
        }
    }

    private static byte[] key(String prefix, String type, String name) {
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(
                (type + "\u0000" + name).getBytes(StandardCharsets.UTF_8));
        return bytes(prefix + ":" + encoded);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
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
