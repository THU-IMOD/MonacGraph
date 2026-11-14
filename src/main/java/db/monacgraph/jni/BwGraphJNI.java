package db.monacgraph.jni;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Local Bw-Graph transport, mirroring the LSM-Community core JNI signatures.
 * IDs and property values are opaque bytes; no Java serialization is performed here.
 * Handles are process-local, graph-scoped tokens, not pointers or durable IDs.
 * Close every graph lease. The last close checkpoints the native graph.
 * OUT=0, IN=1, BOTH=2; parallel edges with the same endpoints are not supported.
 */
public final class BwGraphJNI {
    static {
        String library = System.getProperty("bwgraph.jni.path");
        if (library == null) System.loadLibrary("bwgraph_jni");
        else System.load(Path.of(library).toAbsolutePath().toString());
    }
    private final byte[] root;
    public BwGraphJNI() { this(System.getProperty("bwgraph.root", "workspace/bw-jni")); }
    public BwGraphJNI(String root) {
        this.root = Path.of(Objects.requireNonNull(root)).toAbsolutePath()
                .toString().getBytes(StandardCharsets.UTF_8);
    }
    private native long openNative(byte[] root, byte[] name);
    public long openDB(String name) {
        return openNative(root, Objects.requireNonNull(name).getBytes(StandardCharsets.UTF_8));
    }
    /** Explicit maintenance reset; caller must close all leases first. Local mode is unchanged. */
    public void resetDB(String name) {
        resetNative(root, Objects.requireNonNull(name).getBytes(StandardCharsets.UTF_8));
    }
    private native void resetNative(byte[] root, byte[] name);
    public native boolean newlyCreated(long graph);
    public native void closeDB(long graph);
    public native long getVertexHandleById(long graph, byte[] id);
    public native long[] getAllVertices(long graph);
    public native long createVertex(long graph, byte[] id, byte[] data);
    public native byte[] getDataFromVertexHandle(long graph, long vertex);
    public native void putVertexData(long graph, long vertex, byte[] data);
    public native void removeVertex(long graph, long vertex);
    public native long[] getEdgeHandleByVertex(long graph, long vertex, int direction);
    public native long getEdgeHandleById(long graph, byte[] id);
    public native long[] getAllEdges(long graph);
    public native long createEdge(long graph, byte[] id, long src, long dst, byte[] data);
    public native byte[] getDataFromEdgeHandle(long graph, long edge);
    public native void putEdgeData(long graph, long edge, byte[] data);
    public native void removeEdge(long graph, long edge);
    /** Packed pairs (vertex handle, BFS distance), only reachable vertices. */
    public native long[] getBfsVertices(long graph, long source);
    /** Component count followed by packed (vertex handle, component label) pairs. */
    public native long[] getWCC(long graph);
    public native long[] getSCC(long graph);
    public native byte[] getVertexExternalId(long graph, long vertex);
    public native byte[] getEdgeExternalId(long graph, long edge);
    /** Louvain on the simple undirected projection; count followed by (vertex, label). */
    public native long[] getCommunities(long graph);
}
