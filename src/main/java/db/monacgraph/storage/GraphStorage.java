package db.monacgraph.storage;

/** Opaque graph-scoped handles and byte payloads shared by native backends. */
public interface GraphStorage {
    default void resetDB(String name) { }
    long openDB(String dbName);
    void closeDB(long graphHandle);
    long getVertexHandleById(long graphHandle, byte[] outerId);
    long[] getAllVertices(long graphHandle);
    long createVertex(long graphHandle, byte[] outerId, byte[] data);
    byte[] getDataFromVertexHandle(long graphHandle, long vertexHandle);
    void putVertexData(long graphHandle, long vertexHandle, byte[] data);
    void removeVertex(long graphHandle, long vertexHandle);
    long[] getEdgeHandleByVertex(long graphHandle, long vertexHandle, int direction);
    long getEdgeHandleById(long graphHandle, byte[] outerId);
    long[] getAllEdges(long graphHandle);
    long createEdge(long graphHandle, byte[] outerId, long outVertexHandle, long inVertexHandle, byte[] data);
    byte[] getDataFromEdgeHandle(long graphHandle, long edgeHandle);
    void putEdgeData(long graphHandle, long edgeHandle, byte[] data);
    void removeEdge(long graphHandle, long edgeHandle);
    long[] getBfsVertices(long graphHandle, long vertexHandle);
    long[] getWCC(long graphHandle);
    long[] getSCC(long graphHandle);
    long[] getCommunities(long graphHandle);
}
