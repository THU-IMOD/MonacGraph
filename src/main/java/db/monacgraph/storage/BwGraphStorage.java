package db.monacgraph.storage;

import db.monacgraph.jni.BwGraphJNI;
import org.apache.commons.configuration2.Configuration;
import java.nio.file.*;
import java.io.*;

/** Backend adapter; loading Bw-Graph never initializes RustJNI. */
public final class BwGraphStorage implements GraphStorage {
    private final BwGraphJNI jni;
    private static String loadedLibrary;
    private static boolean initialized;
    public boolean newlyCreated(long graph) { return jni.newlyCreated(graph); }
    @Override public void resetDB(String name) { jni.resetDB(name); }
    public BwGraphStorage(Configuration config) {
        String actual = System.getenv().getOrDefault("BWGRAPH_STORAGE_BACKEND", "local");
        String expected = config.getString("bwgraph.storage.backend", actual);
        if (!actual.equals(expected)) throw new IllegalStateException(
            "bwgraph.storage.backend requires matching BWGRAPH_STORAGE_BACKEND environment");
        synchronized (BwGraphStorage.class) {
            String requested = config.getString("bwgraph.library.path", System.getProperty("bwgraph.jni.path"));
            if (requested != null) requested = Path.of(requested).toAbsolutePath().normalize().toString();
            if (initialized && requested != null && !requested.equals(loadedLibrary))
                throw new IllegalArgumentException("One Bw-Graph native library version is allowed per JVM");
            if (!initialized) {
                if (requested == null) {
                    String os = System.getProperty("os.name").toLowerCase();
                    String suffix = os.contains("mac") ? ".dylib" : ".so";
                    String resource = "/storage/" + (os.contains("mac") ? "macos" : "linux") + "/libbwgraph_jni" + suffix;
                    try (InputStream input = BwGraphStorage.class.getResourceAsStream(resource)) {
                        if (input != null) {
                            Path library = Files.createTempFile("bwgraph_jni-", suffix);
                            Files.copy(input, library, StandardCopyOption.REPLACE_EXISTING);
                            library.toFile().deleteOnExit(); requested = library.toString();
                        }
                    } catch (IOException e) { throw new IllegalStateException("Cannot extract Bw-Graph JNI", e); }
                }
                if (requested != null) System.setProperty("bwgraph.jni.path", requested);
                loadedLibrary = requested;
            }
            jni = new BwGraphJNI(config.getString("bwgraph.storage.root", "workspace/bw-graph"));
            initialized = true;
        }
    }
    @Override public long openDB(String dbName) { return jni.openDB(dbName); }
    @Override public void closeDB(long graphHandle) { jni.closeDB(graphHandle); }
    @Override public long getVertexHandleById(long graphHandle, byte[] outerId) { return jni.getVertexHandleById(graphHandle, outerId); }
    @Override public long[] getAllVertices(long graphHandle) { return jni.getAllVertices(graphHandle); }
    @Override public long createVertex(long graphHandle, byte[] outerId, byte[] data) { return jni.createVertex(graphHandle, outerId, data); }
    @Override public byte[] getDataFromVertexHandle(long graphHandle, long vertexHandle) { return jni.getDataFromVertexHandle(graphHandle, vertexHandle); }
    @Override public void putVertexData(long graphHandle, long vertexHandle, byte[] data) { jni.putVertexData(graphHandle, vertexHandle, data); }
    @Override public void removeVertex(long graphHandle, long vertexHandle) { jni.removeVertex(graphHandle, vertexHandle); }
    @Override public long[] getEdgeHandleByVertex(long graphHandle, long vertexHandle, int direction) { return jni.getEdgeHandleByVertex(graphHandle, vertexHandle, direction); }
    @Override public long getEdgeHandleById(long graphHandle, byte[] outerId) { return jni.getEdgeHandleById(graphHandle, outerId); }
    @Override public long[] getAllEdges(long graphHandle) { return jni.getAllEdges(graphHandle); }
    @Override public long createEdge(long graphHandle, byte[] outerId, long outVertexHandle, long inVertexHandle, byte[] data) { return jni.createEdge(graphHandle, outerId, outVertexHandle, inVertexHandle, data); }
    @Override public byte[] getDataFromEdgeHandle(long graphHandle, long edgeHandle) { return jni.getDataFromEdgeHandle(graphHandle, edgeHandle); }
    @Override public void putEdgeData(long graphHandle, long edgeHandle, byte[] data) { jni.putEdgeData(graphHandle, edgeHandle, data); }
    @Override public void removeEdge(long graphHandle, long edgeHandle) { jni.removeEdge(graphHandle, edgeHandle); }
    @Override public long[] getBfsVertices(long graphHandle, long vertexHandle) { return jni.getBfsVertices(graphHandle, vertexHandle); }
    @Override public long[] getWCC(long graphHandle) { return jni.getWCC(graphHandle); }
    @Override public long[] getSCC(long graphHandle) { return jni.getSCC(graphHandle); }
    @Override public long[] getCommunities(long graphHandle) { return jni.getCommunities(graphHandle); }
}
