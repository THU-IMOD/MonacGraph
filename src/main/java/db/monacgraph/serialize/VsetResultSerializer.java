package db.monacgraph.serialize;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.util.*;

/**
 * Serializer for Vset query results
 * Converts Set<Set<Vertex>> to JSON-friendly format for web visualization
 * Optimized: Preprocess all vertices/edges first to avoid repeated edge extraction
 */
public class VsetResultSerializer {

    /**
     * Serialize Vset result to Map format with induced subgraph edges
     * Optimized version: Preprocess all vertices and edges first to avoid repeated traversal
     */
    public static Map<String, Object> serialize(Set<Set<Vertex>> vsetResult) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", "VsetResult");

        // ========== Step 1: Global Preprocessing - Collect all unique vertices and corresponding edges ==========
        // 1. Collect all unique vertices (deduplication)
        Set<Vertex> allVertices = new HashSet<>();
        for (Set<Vertex> vertexSet : vsetResult) {
            allVertices.addAll(vertexSet);
        }

        // 2. Extract all associated edges of unique vertices at once and cache them (key: edge ID, value: edge data)
        Map<Object, EdgeData> allEdgeCache = new HashMap<>();
        Set<Object> processedEdgeIds = new HashSet<>();
        for (Vertex v : allVertices) {
            Iterator<Edge> edgeIterator = v.edges(Direction.OUT);
            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                Object edgeId = edge.id();
                if (processedEdgeIds.contains(edgeId)) {
                    continue;
                }

                // Encapsulate edge data (including endpoint IDs, properties, etc.)
                EdgeData edgeData = new EdgeData();
                edgeData.edgeId = edgeId;
                edgeData.label = edge.label();
                edgeData.outId = edge.outVertex().id();
                edgeData.inId = edge.inVertex().id();

                // Extract edge properties
                Map<String, Object> properties = new LinkedHashMap<>();
                edge.keys().forEach(key -> {
                    properties.put(key, edge.property(key).value());
                });
                edgeData.properties = properties;

                allEdgeCache.put(edgeId, edgeData);
                processedEdgeIds.add(edgeId);
            }
        }

        // ========== Step 2: Process each subset and filter edges from the cache ==========
        List<Map<String, Object>> subsets = new ArrayList<>();
        for (Set<Vertex> vertexSet : vsetResult) {
            Map<String, Object> subset = new LinkedHashMap<>();

            // Extract vertex data and vertex ID set of the current subset
            List<Map<String, Object>> vertices = new ArrayList<>();
            Set<Object> vertexIds = new HashSet<>();
            for (Vertex v : vertexSet) {
                Object id = v.id();
                vertexIds.add(id);

                Map<String, Object> vertexData = new LinkedHashMap<>();
                vertexData.put("id", id);
                vertexData.put("label", v.label());

                // Extract vertex properties
                Map<String, Object> properties = new LinkedHashMap<>();
                v.keys().forEach(key -> {
                    properties.put(key, v.property(key).value());
                });
                vertexData.put("properties", properties);

                vertices.add(vertexData);
            }

            // Filter the induced subgraph edges of the current subset from the global edge cache
            // (both endpoints are within the subset)
            List<Map<String, Object>> edges = new ArrayList<>();
            for (EdgeData edgeData : allEdgeCache.values()) {
                if (vertexIds.contains(edgeData.outId) && vertexIds.contains(edgeData.inId)) {
                    // Convert to the format required by the frontend
                    Map<String, Object> edgeMap = new LinkedHashMap<>();
                    edgeMap.put("id", edgeData.edgeId);
                    edgeMap.put("label", edgeData.label);
                    edgeMap.put("source", edgeData.outId);
                    edgeMap.put("target", edgeData.inId);
                    edgeMap.put("properties", edgeData.properties);
                    edges.add(edgeMap);
                }
            }

            subset.put("vertices", vertices);
            subset.put("edges", edges);
            subset.put("size", vertices.size());
            subsets.add(subset);
        }

        result.put("subsets", subsets);
        result.put("totalCount", subsets.size());
        return result;
    }

    /**
     * Helper class: Cache core edge data to avoid repeated parsing
     */
    private static class EdgeData {
        Object edgeId;
        String label;
        Object outId;
        Object inId;
        Map<String, Object> properties;
    }

    // ========== The following are the original compatible methods, unchanged ==========
    public static Map<String, Object> serializeVerticesOnly(Set<Set<Vertex>> vsetResult) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", "VsetResult");

        List<Map<String, Object>> subsets = new ArrayList<>();

        for (Set<Vertex> vertexSet : vsetResult) {
            Map<String, Object> subset = new LinkedHashMap<>();

            List<Object> vertexIds = new ArrayList<>();
            Map<Object, Map<String, Object>> vertexProperties = new LinkedHashMap<>();

            for (Vertex v : vertexSet) {
                Object id = v.id();
                vertexIds.add(id);

                Map<String, Object> props = new LinkedHashMap<>();
                props.put("id", id);
                props.put("label", v.label());

                v.keys().forEach(key -> {
                    props.put(key, v.property(key).value());
                });

                vertexProperties.put(id, props);
            }

            subset.put("vertices", vertexIds);
            subset.put("size", vertexIds.size());
            subset.put("properties", vertexProperties);

            subsets.add(subset);
        }

        result.put("subsets", subsets);
        result.put("totalCount", subsets.size());

        return result;
    }

    /**
     * Serialize subgraph match results for web visualization.
     *
     * Unlike serialize(), this method only includes edges that correspond to
     * query graph edges, not all edges in the induced subgraph.
     *
     * <p>All data is passed in pre-built form from SubgraphQueryBuilder:
     * <ul>
     *   <li>{@code rawMatches} — int index lists straight from JNI decoding,
     *       no Vertex objects created yet</li>
     *   <li>{@code indexToVertex} — plain array for O(1) index→Vertex lookup</li>
     *   <li>{@code edgeIndex} — flat long-keyed HashMap built during the single
     *       graph traversal in writeDataGraph(); values are pre-serialized edge Maps</li>
     * </ul>
     * No graph traversal is performed here.
     *
     * @param rawMatches     List of matches as raw int index lists; index i in each
     *                       inner list is the data-graph index of the vertex matched
     *                       to query vertex i
     * @param queryEdges     Query graph edges as {u, v} int pairs (query vertex indices)
     * @param indexToVertex  Array mapping 0-based data-graph index → Vertex
     * @param edgeIndex      Pre-built edge index keyed by
     *                       {@code (long) srcIdx << 32 | dstIdx}; values are
     *                       Edge references — serialized on demand so that only
     *                       edges that appear in a match are ever serialized
     * @return Map containing serialized Vset result ready for web display
     */
    public static Map<String, Object> serializeSubgraph(
            List<List<Integer>> rawMatches,
            List<int[]> queryEdges,
            Vertex[] indexToVertex,
            Map<Long, Edge> edgeIndex) {

        // ── Step 1: Collect unique vertex indices across all matches ──────
        Set<Integer> uniqueIndices = new HashSet<>();
        for (List<Integer> match : rawMatches)
            uniqueIndices.addAll(match);

        // ── Step 2: Pre-serialize vertex data once, reuse across all matches ─
        Map<Integer, Map<String, Object>> vertexDataCache = new HashMap<>();
        for (int idx : uniqueIndices) {
            Vertex v = indexToVertex[idx];
            Map<String, Object> vertexData = new LinkedHashMap<>();
            vertexData.put("id",    v.id());
            vertexData.put("label", v.label());
            Map<String, Object> props = new LinkedHashMap<>();
            v.keys().forEach(key -> props.put(key, v.property(key).value()));
            vertexData.put("properties", props);
            vertexDataCache.put(idx, vertexData);
        }

        // ── Step 3: Assemble subsets — array lookup + long-keyed edge lookup ─
        List<Map<String, Object>> subsets = new ArrayList<>();

        for (List<Integer> match : rawMatches) {
            List<Map<String, Object>> vertices = new ArrayList<>(match.size());
            for (int idx : match)
                vertices.add(vertexDataCache.get(idx));

            List<Map<String, Object>> edges = new ArrayList<>();
            Set<Object> seenEdgeIds = new HashSet<>();

            for (int[] qEdge : queryEdges) {
                int srcIdx = match.get(qEdge[0]);
                int dstIdx = match.get(qEdge[1]);

                // Try srcIdx→dstIdx then dstIdx→srcIdx (undirected query edge)
                Edge edge = edgeIndex.get(edgeKey(srcIdx, dstIdx));
                if (edge == null)
                    edge = edgeIndex.get(edgeKey(dstIdx, srcIdx));
                if (edge == null) continue;

                Object edgeId = edge.id();
                if (seenEdgeIds.contains(edgeId)) continue;
                seenEdgeIds.add(edgeId);

                // Serialize on demand — only edges that appear in a match
                Map<String, Object> edgeMap = new LinkedHashMap<>();
                edgeMap.put("id",     edgeId);
                edgeMap.put("label",  edge.label());
                edgeMap.put("source", edge.outVertex().id());
                edgeMap.put("target", edge.inVertex().id());
                Map<String, Object> eProps = new LinkedHashMap<>();
                final Edge matchedEdge = edge;
                matchedEdge.keys().forEach(key -> eProps.put(key, matchedEdge.property(key).value()));
                edgeMap.put("properties", eProps);
                edges.add(edgeMap);
            }

            Map<String, Object> subset = new LinkedHashMap<>();
            subset.put("vertices", vertices);
            subset.put("edges",    edges);
            subset.put("size",     vertices.size());
            subsets.add(subset);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type",       "VsetResult");
        result.put("subsets",    subsets);
        result.put("totalCount", subsets.size());
        return result;
    }

    /**
     * Packs two 32-bit vertex indices into a single {@code long} edge key.
     * Must match the packing used in SubgraphQueryBuilder.edgeKey().
     */
    private static long edgeKey(int u, int v) {
        return ((long) u << 32) | (v & 0xFFFFFFFFL);
    }

    public static boolean isVsetQuery(String query) {
        return query != null &&
                (query.contains(".Vset()") ||
                        query.contains("g.Vset()") ||
                        query.trim().startsWith("Vset()"));
    }
}