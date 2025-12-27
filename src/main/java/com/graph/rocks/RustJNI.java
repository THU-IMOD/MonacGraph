package com.graph.rocks;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge for LSM-Community native operations
 * Loads the native library and exposes core graph storage operations via JNI
 */
public class RustJNI {

    // Load native library on class initialization
    static {
        try {
            // 1. Get the name of OS.
            String osName = System.getProperty("os.name").toLowerCase();
            String resourcePath;
            String libExtension;


            // 2. Load dyn lib according to the os name
            if (osName.contains("win")) {
                resourcePath = "storage/windows/lsm_community_java.dll";
                libExtension = ".dll";
            } else if (osName.contains("mac")) {
                resourcePath = "storage/macos/liblsm_community_java.dylib";
                libExtension = ".dylib";
            } else {
                resourcePath = "storage/linux/liblsm_community_java.so";
                libExtension = ".so";
            }

            // 3. Get the resource URL
            InputStream in = RustJNI.class.getClassLoader().getResourceAsStream(resourcePath);
            if (in == null) {
                throw new RuntimeException("Native library not found in classpath: " + resourcePath);
            }
            File tempFile = File.createTempFile("liblsm_community_java", libExtension);
            tempFile.deleteOnExit();

            // 5. Copy Jar
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            in.close();

            // 6. Load this temp file
            // System.out.println("Loading native lib from: " + tempFile.getAbsolutePath());
            System.load(tempFile.getAbsolutePath());

        } catch (Exception e) {
            throw new RuntimeException("Failed to load native library", e);
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