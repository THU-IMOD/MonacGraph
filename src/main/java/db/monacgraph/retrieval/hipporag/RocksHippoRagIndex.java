package db.monacgraph.retrieval.hipporag;

import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.hipporag.HippoRagModels.SynonymEdge;
import db.monacgraph.runtime.LocalRuntime;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RocksHippoRagIndex implements HippoRagIndex {
    static {
        LocalRuntime.install();
        RocksDB.loadLibrary();
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksHippoRagIndex(Path directory) {
        try {
            options = new Options().setCreateIfMissing(true);
            writeOptions = new WriteOptions().setSync(true);
            db = RocksDB.open(options, directory.toAbsolutePath().toString());
        } catch (RocksDBException e) {
            throw failure("open HippoRAG index", e);
        }
    }

    @Override
    public synchronized void putPhrase(PhraseNode phrase) {
        put("phrase:" + phrase.vertex().id(), phrase);
    }

    @Override
    public synchronized void putFact(FactTriple fact) {
        put("fact:" + fact.edge().id(), fact);
    }

    @Override
    public synchronized void putVector(Object vertexId, float[] vector) {
        put("vector:" + vertexId, vector);
    }

    @Override
    public synchronized void putSynonym(SynonymEdge edge) {
        String left = String.valueOf(edge.left().id());
        String right = String.valueOf(edge.right().id());
        String key = left.compareTo(right) <= 0
                ? left + "\u0000" + right
                : right + "\u0000" + left;
        put("synonym:" + key, edge);
    }

    @Override
    public List<PhraseNode> phrases() {
        return scan("phrase:", PhraseNode.class);
    }

    @Override
    public List<FactTriple> facts() {
        return scan("fact:", FactTriple.class);
    }

    @Override
    public List<SynonymEdge> synonyms() {
        return scan("synonym:", SynonymEdge.class);
    }

    @Override
    public Optional<float[]> vector(Object vertexId) {
        try {
            byte[] value = db.get(bytes("vector:" + vertexId));
            return value == null
                    ? Optional.empty()
                    : Optional.of(mapper.readValue(value, float[].class));
        } catch (Exception e) {
            throw failure("read phrase vector", e);
        }
    }

    private void put(String key, Object value) {
        try {
            db.put(writeOptions, bytes(key), mapper.writeValueAsBytes(value));
        } catch (Exception e) {
            throw failure("write " + key, e);
        }
    }

    private <T> List<T> scan(String prefix, Class<T> type) {
        byte[] start = bytes(prefix);
        List<T> values = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(start); iterator.isValid() && startsWith(iterator.key(), start); iterator.next()) {
                values.add(mapper.readValue(iterator.value(), type));
            }
            iterator.status();
            return List.copyOf(values);
        } catch (Exception e) {
            throw failure("scan " + prefix, e);
        }
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
