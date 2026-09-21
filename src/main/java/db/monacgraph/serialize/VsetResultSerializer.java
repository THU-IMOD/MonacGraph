package db.monacgraph.serialize;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serializer for Vset / subgraph query results.
 * Converts result pieces to a JSON-friendly {@code VsetResult} map for the Web UI.
 */
public class VsetResultSerializer {

    /**
     * One subgraph: vertices plus the caller-supplied edges.
     */
    public static Map<String, Object> serialize(Collection<Vertex> vertices, Collection<Edge> edges) {
        return serialize(ResultSubgraph.of(vertices, edges));
    }

    /**
     * One result piece.
     */
    public static Map<String, Object> serialize(ResultSubgraph subgraph) {
        return serializeSubgraphs(List.of(subgraph));
    }

    /**
     * Same as {@link #serializeSubgraphs(List)}.
     */
    public static Map<String, Object> serialize(List<ResultSubgraph> subgraphs) {
        return serializeSubgraphs(subgraphs);
    }

    /**
     * Vertex sets only: each inner set is drawn as its <em>induced</em> subgraph
     * (every edge with both ends in the set). Used by BFS, WCC, Community, Vset.
     */
    public static Map<String, Object> serialize(Set<Set<Vertex>> vsetResult) {
        List<ResultSubgraph> subgraphs = new ArrayList<>();
        if (vsetResult != null) {
            for (Set<Vertex> vertexSet : vsetResult) {
                subgraphs.add(ResultSubgraph.induced(vertexSet));
            }
        }
        return serializeSubgraphs(subgraphs);
    }

    /**
     * Web 入口：一块或多块 {@link ResultSubgraph}。
     * 各算子算出点、边之后收成列表，交给本方法即可。
     */
    public static Map<String, Object> serializeSubgraphs(List<ResultSubgraph> subgraphs) {
        Map<Object, Map<String, Object>> vertexCache = new HashMap<>();
        Map<Object, Map<String, Object>> edgeCache = new HashMap<>();
        List<Map<String, Object>> subsets = new ArrayList<>();

        if (subgraphs != null) {
            for (ResultSubgraph subgraph : subgraphs) {
                if (subgraph == null) {
                    continue;
                }
                List<Vertex> vertices = subgraph.vertices();
                List<Edge> edges = subgraph.induced() ? inducedEdges(vertices) : subgraph.edges();

                List<Map<String, Object>> vertexMaps = new ArrayList<>(vertices.size());
                for (Vertex v : vertices) {
                    vertexMaps.add(cachedVertex(v, vertexCache));
                }

                List<Map<String, Object>> edgeMaps = new ArrayList<>();
                Set<Object> seenEdgeIds = new HashSet<>();
                for (Edge e : edges) {
                    if (e == null || !seenEdgeIds.add(e.id())) {
                        continue;
                    }
                    edgeMaps.add(cachedEdge(e, edgeCache));
                }

                Map<String, Object> subset = new LinkedHashMap<>();
                subset.put("vertices", vertexMaps);
                subset.put("edges", edgeMaps);
                subset.put("size", vertexMaps.size());
                subsets.add(subset);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", "VsetResult");
        result.put("subsets", subsets);
        result.put("totalCount", subsets.size());
        return result;
    }

    private static List<Edge> inducedEdges(List<Vertex> vertices) {
        Set<Object> vertexIds = new HashSet<>();
        for (Vertex v : vertices) {
            vertexIds.add(v.id());
        }
        List<Edge> edges = new ArrayList<>();
        Set<Object> seen = new HashSet<>();
        for (Vertex v : vertices) {
            Iterator<Edge> it = v.edges(Direction.OUT);
            while (it.hasNext()) {
                Edge edge = it.next();
                if (!seen.add(edge.id())) {
                    continue;
                }
                if (vertexIds.contains(edge.outVertex().id()) && vertexIds.contains(edge.inVertex().id())) {
                    edges.add(edge);
                }
            }
        }
        return edges;
    }

    private static Map<String, Object> cachedVertex(Vertex v, Map<Object, Map<String, Object>> cache) {
        return cache.computeIfAbsent(v.id(), id -> {
            Map<String, Object> vertexData = new LinkedHashMap<>();
            vertexData.put("id", v.id());
            vertexData.put("label", v.label());
            Map<String, Object> properties = new LinkedHashMap<>();
            v.keys().forEach(key -> properties.put(key, v.property(key).value()));
            vertexData.put("properties", properties);
            return vertexData;
        });
    }

    private static Map<String, Object> cachedEdge(Edge edge, Map<Object, Map<String, Object>> cache) {
        return cache.computeIfAbsent(edge.id(), id -> {
            Map<String, Object> edgeMap = new LinkedHashMap<>();
            edgeMap.put("id", edge.id());
            edgeMap.put("label", edge.label());
            edgeMap.put("source", edge.outVertex().id());
            edgeMap.put("target", edge.inVertex().id());
            Map<String, Object> properties = new LinkedHashMap<>();
            edge.keys().forEach(key -> properties.put(key, edge.property(key).value()));
            edgeMap.put("properties", properties);
            return edgeMap;
        });
    }

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

                v.keys().forEach(key -> props.put(key, v.property(key).value()));

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
     * Subgraph matcher: each match is a copy of the query graph's edges,
     * not the induced subgraph on the matched vertices.
     */
    public static Map<String, Object> serializeSubgraph(
            List<List<Integer>> rawMatches,
            List<int[]> queryEdges,
            Vertex[] indexToVertex,
            Map<Long, Edge> edgeIndex) {

        List<ResultSubgraph> subgraphs = new ArrayList<>();
        if (rawMatches != null) {
            for (List<Integer> match : rawMatches) {
                List<Vertex> vertices = new ArrayList<>(match.size());
                for (int idx : match) {
                    vertices.add(indexToVertex[idx]);
                }
                List<Edge> edges = new ArrayList<>();
                Set<Object> seenEdgeIds = new HashSet<>();
                if (queryEdges != null) {
                    for (int[] qEdge : queryEdges) {
                        int srcIdx = match.get(qEdge[0]);
                        int dstIdx = match.get(qEdge[1]);
                        Edge edge = edgeIndex.get(edgeKey(srcIdx, dstIdx));
                        if (edge == null) {
                            edge = edgeIndex.get(edgeKey(dstIdx, srcIdx));
                        }
                        if (edge == null || !seenEdgeIds.add(edge.id())) {
                            continue;
                        }
                        edges.add(edge);
                    }
                }
                subgraphs.add(ResultSubgraph.of(vertices, edges));
            }
        }
        return serializeSubgraphs(subgraphs);
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
