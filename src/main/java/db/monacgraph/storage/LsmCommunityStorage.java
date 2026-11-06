package db.monacgraph.storage;

import db.monacgraph.jni.RustJNI;
import java.util.ArrayList;

/** Normalize legacy JNI formats without changing its native ABI or store format. */
public final class LsmCommunityStorage extends RustJNI {
    @Override public long getVertexHandleById(long graph, byte[] id) {
        try { return super.getVertexHandleById(graph, id); }
        catch (java.util.NoSuchElementException missing) { return -1; }
    }
    @Override public long getEdgeHandleById(long graph, byte[] id) {
        try { return super.getEdgeHandleById(graph, id); }
        catch (java.util.NoSuchElementException missing) { return -1; }
    }
    @Override public long[] getCommunities(long graph) {
        long[] packed = super.getCommunities(graph);
        if (packed.length == 0) throw new IllegalStateException("Missing community count");
        ArrayList<Long> pairs = new ArrayList<>();
        pairs.add(packed[0]);
        int offset = 1;
        for (long community = 0; community < packed[0]; community++) {
            if (offset >= packed.length) throw new IllegalStateException("Truncated communities");
            long size = packed[offset++];
            if (size < 0 || size > packed.length - offset) throw new IllegalStateException("Invalid community size");
            for (long i = 0; i < size; i++) { pairs.add(packed[offset++]); pairs.add(community); }
        }
        if (offset != packed.length) throw new IllegalStateException("Trailing community data");
        return pairs.stream().mapToLong(Long::longValue).toArray();
    }
}
