package db.monacgraph.so;

import db.monacgraph.jni.SubgraphJNI;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.*;
import java.util.*;

/**
 * Builder for second-order subgraph matching queries.
 *
 * Usage:
 * <pre>
 *   g.Subgraph()
 *    .addV(0, "Person").addV(1, "Person").addV(2, "City")
 *    .addE(0, 1).addE(1, 2)
 *    .forall("x").forall("y")
 *    .filter("g.V(x).out('knows').is(y) || g.V(x).is(y)")
 *    .having("size > 1")
 *    .execute()
 * </pre>
 *
 * Execution pipeline:
 *  1. Export the full data graph to transfer/data.graph (.graph format)
 *  2. Export the query graph (from addV/addE) to transfer/query.graph
 *     with string labels replaced by integer IDs
 *  3. Build an int→Vertex reverse lookup table for the phi callback
 *  4. Invoke C++ subgraph matcher via JNI, passing a SubgraphPhiCallback
 *  5. Decode the returned flat match array into Set<Set<Vertex>>
 */
@SuppressWarnings("unused")
public class SubgraphQueryBuilder {

    private final GraphTraversalSource g;

    // ── Query graph spec (from addV / addE calls) ──────────────────────
    /** queryVertexLabels.get(i) = label string of query vertex i */
    private final List<String> queryVertexLabels = new ArrayList<>();
    /** Each element is {u, v} — an undirected query edge */
    private final List<int[]>  queryEdges        = new ArrayList<>();

    // ── Second-order logic clauses ──────────────────────────────────────
    /** Ordered list of (variableName, "exist"|"forall") declarations */
    private final List<Map.Entry<String, String>> conditions    = new ArrayList<>();
    private String filterQuery      = "true";
    private String aggregationQuery = "true";

    // ── Label ↔ int mapping (shared by data graph and query graph) ───────
    /**
     * Insertion-ordered map so that label IDs are assigned
     * in a deterministic, reproducible order across both files.
     */
    private final Map<String, Integer> labelToInt = new LinkedHashMap<>();

    // ── Transfer directory ──────────────────────────────────────────────
    public static final String TRANSFER_DIR      = "transfer" + File.separator;
    public static final String DATA_GRAPH_PATH   = TRANSFER_DIR + "data.graph";
    public static final String QUERY_GRAPH_PATH  = TRANSFER_DIR + "query.graph";

    public SubgraphQueryBuilder(GraphTraversalSource g) {
        this.g = g;
        new File(TRANSFER_DIR).mkdirs();
    }

    // ── Query graph construction ────────────────────────────────────────

    /**
     * Declares a query vertex with the given 0-based ID and label.
     * IDs must form a contiguous range [0, N).
     */
    public SubgraphQueryBuilder addV(int id, String label) {
        while (queryVertexLabels.size() <= id) queryVertexLabels.add(null);
        queryVertexLabels.set(id, label);
        internLabel(label);   // register now so data-graph labels share the same map
        return this;
    }

    /** Declares an undirected query edge between query vertices u and v. */
    public SubgraphQueryBuilder addE(int u, int v) {
        queryEdges.add(new int[]{u, v});
        return this;
    }

    // ── Second-order logic clauses ──────────────────────────────────────

    public SubgraphQueryBuilder exist(String varName) {
        conditions.add(Map.entry(varName, "exist"));
        return this;
    }

    public SubgraphQueryBuilder forall(String varName) {
        conditions.add(Map.entry(varName, "forall"));
        return this;
    }

    public SubgraphQueryBuilder filter(String gremlinQuery) {
        this.filterQuery = gremlinQuery;
        return this;
    }

    public SubgraphQueryBuilder having(String aggregationCondition) {
        this.aggregationQuery = aggregationCondition;
        return this;
    }

    // ── Execution ───────────────────────────────────────────────────────

    /**
     * Runs the full pipeline and returns all valid subgraph matches.
     * Each element of the returned set is the set of data vertices
     * comprising one match of the query graph that also satisfies
     * the second-order filter clause.
     */
    public Set<Set<Vertex>> execute() {

        // 1. Collect data vertices in a stable, deterministic order
        List<Vertex> dataVertices = collectDataVertices();
        Map<Object, Integer> vertexToIndex = buildVertexIndexMap(dataVertices);

        // 2. Intern all data-graph labels into the shared label map
        //    (query labels were already interned by addV)
        for (Vertex v : dataVertices) {
            internLabel(v.label());
        }

        // 3. Write both graph files
        writeDataGraph(dataVertices, vertexToIndex);
        writeQueryGraph();

        // 4. Build int → Vertex reverse lookup for the phi callback
        Map<Integer, Vertex> indexToVertex = new HashMap<>();
        for (int i = 0; i < dataVertices.size(); i++) {
            indexToVertex.put(i, dataVertices.get(i));
//            System.out.println(i + ": " + dataVertices.get(i).value("name") + " " + dataVertices.get(i).value("age"));
        }

        // 5. Build the phi callback (compiles filter + holds vertex lookup)
        SubgraphPhiCallback phiCallback =
                new SubgraphPhiCallback(g, conditions, indexToVertex, filterQuery);

        // 6. Encode quantifiers as int[] (0 = FORALL, 1 = EXIST)
        int[] quantifiers = conditions.stream()
                .mapToInt(e -> "exist".equals(e.getValue()) ? 1 : 0)
                .toArray();

        // 7. Call C++ via JNI
        //    Returns: [matchCount, m0v0, m0v1, ..., m_{M-1}v_{qn-1}]
        //    where m_i_v_j is the 0-based data-graph index of the j-th
        //    query vertex in the i-th match.
        SubgraphJNI jni = new SubgraphJNI();
        if (!SubgraphJNI.isAvailable()) {
            throw new UnsupportedOperationException(
                    "g.Subgraph() requires the native subgraph matching library. " +
                            "Please build the C++ engine following the instructions in README.md.");
        }
        long[] rawMatches = jni.runSubgraphMatch(
                DATA_GRAPH_PATH, QUERY_GRAPH_PATH, quantifiers, phiCallback);

        // 8. Decode raw matches → Set<Set<Vertex>>
        return decodeMatches(rawMatches, indexToVertex, queryVertexLabels.size());
    }

    /** Convenience wrapper: execute + serialize for web visualization. */
    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serialize(execute());
    }

    // ── Graph file writers ──────────────────────────────────────────────

    /**
     * Writes the full data graph to {@value}.
     *
     * <p>Format (same as Graph::load in graph.h):
     * <pre>
     *   t N M
     *   v &lt;id&gt; &lt;labelInt&gt; &lt;degree&gt;
     *   ...
     *   e &lt;u&gt; &lt;v&gt;
     *   ...
     * </pre>
     * Edges are treated as undirected: each edge line contributes +1 to
     * the degree of both endpoints.
     */
    private void writeDataGraph(List<Vertex> vertices,
                                Map<Object, Integer> vertexToIndex) {

        // First pass: collect edges and compute per-vertex degrees
        List<int[]> edges  = new ArrayList<>();
        int[]       degree = new int[vertices.size()];

        g.getGraph().edges().forEachRemaining(edge -> {
            Integer uIdx = vertexToIndex.get(edge.outVertex().id());
            Integer vIdx = vertexToIndex.get(edge.inVertex().id());
            if (uIdx != null && vIdx != null) {
                edges.add(new int[]{uIdx, vIdx});
                degree[uIdx]++;
                degree[vIdx]++;   // undirected: both sides count
            }
        });

        // Second pass: write file
        try (PrintWriter pw = new PrintWriter(new FileWriter(DATA_GRAPH_PATH))) {
            pw.printf("t %d %d%n", vertices.size(), edges.size());
            for (int i = 0; i < vertices.size(); i++) {
                int labelInt = internLabel(vertices.get(i).label());
                pw.printf("v %d %d %d%n", i, labelInt, degree[i]);
            }
            for (int[] e : edges) {
                pw.printf("e %d %d%n", e[0], e[1]);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(
                    "Failed to write data graph to " + DATA_GRAPH_PATH, ex);
        }
    }

    /**
     * Writes the query graph to {@value}.
     * String labels declared via addV() are replaced by their integer IDs
     * from the shared {@link #labelToInt} map.
     */
    private void writeQueryGraph() {
        int qn = queryVertexLabels.size();
        int qm = queryEdges.size();

        // Compute per-query-vertex degree (undirected)
        int[] degree = new int[qn];
        for (int[] e : queryEdges) {
            degree[e[0]]++;
            degree[e[1]]++;
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(QUERY_GRAPH_PATH))) {
            pw.printf("t %d %d%n", qn, qm);
            for (int i = 0; i < qn; i++) {
                String lbl    = queryVertexLabels.get(i);
                int labelInt  = (lbl != null) ? internLabel(lbl) : 0;
                pw.printf("v %d %d %d%n", i, labelInt, degree[i]);
            }
            for (int[] e : queryEdges) {
                pw.printf("e %d %d%n", e[0], e[1]);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(
                    "Failed to write query graph to " + QUERY_GRAPH_PATH, ex);
        }
    }

    // ── Result decoding ─────────────────────────────────────────────────

    /**
     * Converts the flat long[] returned by JNI into a Set of vertex sets.
     *
     * <p>Wire format:
     * <pre>
     *   raw[0]          = M  (total number of matches)
     *   raw[1..M*qn]    = M blocks of qn data-vertex indices each
     * </pre>
     *
     * @param raw         flat array from JNI
     * @param idxToVertex 0-based data index → TinkerPop Vertex
     * @param qn          number of query vertices
     */
    private Set<Set<Vertex>> decodeMatches(long[] raw,
                                           Map<Integer, Vertex> idxToVertex,
                                           int qn) {
        Set<Set<Vertex>> results = new LinkedHashSet<>();
        if (raw == null || raw.length < 1) return results;

        long matchCount = raw[0];
        int  offset     = 1;

        for (long mi = 0; mi < matchCount; mi++) {
            Set<Vertex> matchSet = new LinkedHashSet<>();
            for (int qi = 0; qi < qn; qi++) {
                Vertex v = idxToVertex.get((int) raw[offset++]);
                if (v != null) matchSet.add(v);
            }
            if (evaluateHaving(matchSet.size())) {
                results.add(matchSet);
            }
        }
        return results;
    }

    // ── Small utilities ─────────────────────────────────────────────────

    /** Assigns a stable integer ID to a label string if not already seen. */
    private int internLabel(String label) {
        return labelToInt.computeIfAbsent(label, k -> labelToInt.size());
    }

    /** Returns all data-graph vertices sorted by ID for deterministic output. */
    private List<Vertex> collectDataVertices() {
        List<Vertex> list = new ArrayList<>();
        g.getGraph().vertices().forEachRemaining(list::add);
        list.sort(Comparator.comparing(v -> v.id().toString()));
        return list;
    }

    /** Builds a TinkerPop vertex ID → 0-based index map. */
    private Map<Object, Integer> buildVertexIndexMap(List<Vertex> vertices) {
        Map<Object, Integer> map = new HashMap<>();
        for (int i = 0; i < vertices.size(); i++) map.put(vertices.get(i).id(), i);
        return map;
    }

    /**
     * Evaluates the {@code having} size constraint.
     * Supports expressions like {@code "size > 2"}, {@code "size >= 3 && size <= 10"}.
     */
    private boolean evaluateHaving(int size) {
        if ("true".equals(aggregationQuery)) return true;
        String expr = aggregationQuery.trim()
                .replace("size", String.valueOf(size));
        try {
            Object result = new groovy.lang.GroovyShell().evaluate(expr);
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            return true;   // fallback: do not filter
        }
    }
}