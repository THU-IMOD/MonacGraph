package com.graph.rocks;

import java.io.File;
import java.net.URL;

/**
 * JNI bridge for Rust-based LSM-tree (RocksDB) native operations
 * Loads the native library and exposes core graph storage operations via JNI
 */
public class RustJNI {

    // Load native library on class initialization
    static {
        try {
            ClassLoader loader = RustJNI.class.getClassLoader();
            URL libUrl = loader.getResource("native/windows-x86_64/lsm_community_java.dll");

            if (libUrl == null) {
                throw new RuntimeException("Native DLL library not found");
            }

            String libPath = libUrl.toURI().getPath().replace("/", File.separator);
            System.load(libPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load lsm_community_java.dll", e);
        }
    }

    // ------------------------------ Core Database Operations ------------------------------

    /**
     * Open a RocksDB database instance
     * @param dbName Database name/path
     * @return Native handle to the opened database
     */
    public native long openDB(String dbName);

    /**
     * Close a RocksDB database instance and release resources
     * @param graphHandle Native database handle to close
     */
    public native void closeDB(long graphHandle);

    // ------------------------------ Vertex Operations ------------------------------

    /**
     * Get native vertex handle by external ID
     * @param graphHandle Native database handle
     * @param outerId External vertex ID (byte array encoding)
     * @return Native vertex handle
     */
    public native long getVertexHandleById(long graphHandle, byte[] outerId);

    /**
     * Get all native vertex handles in the database
     * @param graphHandle Native database handle
     * @return Array of native vertex handles
     */
    public native long[] getAllVertices(long graphHandle);

    /**
     * Create a new vertex in the database
     * @param graphHandle Native database handle
     * @param outerId External vertex ID (byte array encoding)
     * @param data Serialized vertex data (properties, label, etc.)
     * @return Native handle to the created vertex
     */
    public native long createVertex(long graphHandle, byte[] outerId, byte[] data);

    /**
     * Retrieve serialized data from a vertex handle
     * @param graphHandle Native database handle
     * @param vertexHandle Native vertex handle
     * @return Serialized vertex data as byte array
     */
    public native byte[] getDataFromVertexHandle(long graphHandle, long vertexHandle);

    /**
     * Update vertex data in the database
     * @param graphHandle Native database handle
     * @param vertexHandle Native vertex handle
     * @param data Updated serialized vertex data
     */
    public native void putVertexData(long graphHandle, long vertexHandle, byte[] data);

    /**
     * Remove a vertex from the database
     * @param graphHandle Native database handle
     * @param vertexHandle Native vertex handle to remove
     */
    public native void removeVertex(long graphHandle, long vertexHandle);

    /**
     * Get edge handles connected to a vertex by direction
     * @param graphHandle Native database handle
     * @param vertexHandle Native vertex handle
     * @param direction Edge direction (0=OUT, 1=IN, 2=BOTH)
     * @return Array of native edge handles
     */
    public native long[] getEdgeHandleByVertex(long graphHandle, long vertexHandle, int direction);

    // ------------------------------ Edge Operations ------------------------------

    /**
     * Get native edge handle by external ID
     * @param graphHandle Native database handle
     * @param outerId External edge ID (byte array encoding)
     * @return Native edge handle
     */
    public native long getEdgeHandleById(long graphHandle, byte[] outerId);

    /**
     * Get all native edge handles in the database
     * @param graphHandle Native database handle
     * @return Array of native edge handles
     */
    public native long[] getAllEdges(long graphHandle);

    /**
     * Create a new edge between two vertices
     * @param graphHandle Native database handle
     * @param outerId External edge ID (byte array encoding)
     * @param outVertexHandle Native handle of source vertex
     * @param inVertexHandle Native handle of target vertex
     * @param data Serialized edge data (properties, label, etc.)
     * @return Native handle to the created edge
     */
    public native long createEdge(long graphHandle, byte[] outerId, long outVertexHandle, long inVertexHandle, byte[] data);

    /**
     * Retrieve serialized data from an edge handle
     * @param graphHandle Native database handle
     * @param edgeHandle Native edge handle
     * @return Serialized edge data as byte array
     */
    public native byte[] getDataFromEdgeHandle(long graphHandle, long edgeHandle);

    /**
     * Update edge data in the database
     * @param graphHandle Native database handle
     * @param edgeHandle Native edge handle
     * @param data Updated serialized edge data
     */
    public native void putEdgeData(long graphHandle, long edgeHandle, byte[] data);

    /**
     * Remove an edge from the database
     * @param graphHandle Native database handle
     * @param edgeHandle Native edge handle to remove
     */
    public native void removeEdge(long graphHandle, long edgeHandle);
}