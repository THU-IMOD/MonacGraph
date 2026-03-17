package db.monacgraph.so;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.jni.SubgraphJNI;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.*;
import java.util.*;

/**
 * Builder for second-order subgraph matching queries.
 *
 * <p>The query graph can be constructed in two ways:
 *
 * <p><b>Option A — inline builder API:</b>
 * <pre>
 *   g.Subgraph()
 *    .addV(0, "person").addV(1, "person").addV(2, "person")
 *    .addE(0, 1).addE(1, 2).addE(0, 2)
 *    .forall("x").forall("y")
 *    .filter("g.V(x).out('knows').is(y) || g.V(x).is(y)")
 *    .having("size > 1")
 *    .execute()
 * </pre>
 *
 * <p><b>Option B — load from file:</b>
 * <pre>
 *   g.Subgraph()
 *    .fromFile("query.graph", "labelMap.json")
 *    .forall("x").forall("y")
 *    .filter("g.V(x).out('knows').is(y) || g.V(x).is(y)")
 *    .having("size > 1")
 *    .execute()
 * </pre>
 *
 * The two options are mutually exclusive: calling {@link #fromFile} after
 * {@link #addV} / {@link #addE}, or vice versa, throws
 * {@link IllegalStateException}.
 *
 * <p>Execution pipeline:
 * <ol>
 *   <li>Export the full data graph to transfer/data.graph (.graph format)</li>
 *   <li>Export the query graph to transfer/query.graph
 *       with string labels replaced by integer IDs</li>
 *   <li>Build an int→Vertex reverse lookup table for the phi callback</li>
 *   <li>Invoke C++ subgraph matcher via JNI, passing a SubgraphPhiCallback</li>
 *   <li>Decode the returned flat match array into Set&lt;Set&lt;Vertex&gt;&gt;</li>
 * </ol>
 */
@SuppressWarnings("unused")
public class SubgraphQueryBuilder {

    private final GraphTraversalSource g;

    // ── Query graph spec (from addV/addE or fromFile) ───────────────────
    /** queryVertexLabels.get(i) = label string of query vertex i */
    private final List<String> queryVertexLabels = new ArrayList<>();
    /** Each element is {u, v} — an undirected query edge */
    private final List<int[]>  queryEdges        = new ArrayList<>();

    /** Tracks which construction mode has been used, to enforce mutual exclusion. */
    private enum BuildMode { NONE, INLINE, FILE }
    private BuildMode buildMode = BuildMode.NONE;

    // ── Second-order logic clauses ──────────────────────────────────────
    /** Ordered list of (variableName, "exist"|"forall") declarations */
    private final List<Map.Entry<String, String>> conditions    = new ArrayList<>();
    private String filterQuery      = "true";
    private String aggregationQuery = "true";
    /**
     * When true, matches with identical vertex sets AND edge sets are deduplicated.
     * Two matches are considered equal if they map to the same set of data vertices
     * and the same set of data edges (regardless of which query vertex maps to which).
     */
    private boolean uniqueOnly = false;

    // ── Label ↔ int mapping (shared by data graph and query graph) ───────
    /**
     * Insertion-ordered map so that label IDs are assigned
     * in a deterministic, reproducible order across both files.
     */
    private final Map<String, Integer> labelToInt = new LinkedHashMap<>();

    // ── Execution-time caches (populated by writeDataGraph / execute) ─────

    /**
     * 0-based data-graph index → Vertex.
     * A plain array is used because the indices are dense and contiguous,
     * giving O(1) access with no hashing overhead.
     * Populated by execute(); reused by executeForWeb().
     */
    private Vertex[] indexToVertexArray = null;

    /**
     * Edge index built once during writeDataGraph(), reused by executeForWeb().
     * Key: two 32-bit indices packed into one long — {@code (long)srcIdx << 32 | dstIdx}.
     * Value: the Edge object itself; serialization is deferred to VsetResultSerializer
     * so that edges not matched by any query are never serialized at all.
     */
    private Map<Long, Edge> dataEdgeIndex = null;

    // ── Directories ─────────────────────────────────────────────────────
    /** Directory for user-provided graph and label map files. */
    public static final String DATA_DIR          = "data" + File.separator;
    /** Directory for intermediate transfer files consumed by the C++ engine. */
    public static final String TRANSFER_DIR      = "transfer" + File.separator;
    public static final String DATA_GRAPH_PATH   = TRANSFER_DIR + "data.graph";
    public static final String QUERY_GRAPH_PATH  = TRANSFER_DIR + "query.graph";

    public SubgraphQueryBuilder(GraphTraversalSource g) {
        this.g = g;
        new File(TRANSFER_DIR).mkdirs();
    }

    // ── Query graph construction — Option A: inline ─────────────────────

    /**
     * Declares a query vertex with the given 0-based ID and label.
     * IDs must form a contiguous range [0, N).
     *
     * @throws IllegalStateException if {@link #fromFile} has already been called
     */
    public SubgraphQueryBuilder addV(int id, String label) {
        requireMode(BuildMode.INLINE);
        while (queryVertexLabels.size() <= id) queryVertexLabels.add(null);
        queryVertexLabels.set(id, label);
        internLabel(label);
        return this;
    }

    /**
     * Declares an undirected query edge between query vertices u and v.
     *
     * @throws IllegalStateException if {@link #fromFile} has already been called
     */
    public SubgraphQueryBuilder addE(int u, int v) {
        requireMode(BuildMode.INLINE);
        queryEdges.add(new int[]{u, v});
        return this;
    }

    // ── Query graph construction — Option B: from file ──────────────────

    /**
     * Loads the query graph topology from a {@code .graph} file and a label
     * map JSON file under the {@value DATA_DIR} directory, instead of calling
     * {@link #addV} / {@link #addE} manually.
     *
     * <p>Both paths are resolved relative to {@value DATA_DIR}:
     * <pre>
     *   fromFile("query.graph", "labelMap.json")
     *   // resolves to: data/query.graph  and  data/labelMap.json
     * </pre>
     *
     * <p><b>.graph file format:</b>
     * <pre>
     *   t &lt;vertex_count&gt; &lt;edge_count&gt;
     *   v &lt;id&gt; &lt;label_int&gt; &lt;degree&gt;
     *   ...
     *   e &lt;src_id&gt; &lt;dst_id&gt;
     *   ...
     * </pre>
     *
     * <p><b>Label map file format</b> (JSON object mapping label int strings
     * to vertex label names):
     * <pre>
     *   {
     *     "0": "person",
     *     "1": "paper"
     *   }
     * </pre>
     * Label integers present in the .graph file but absent from the map are
     * kept as their string representation (e.g. {@code "2"}).
     *
     * @param graphFileName    filename of the .graph file under {@value DATA_DIR}
     * @param labelMapFileName filename of the JSON label map file under {@value DATA_DIR}
     * @return this builder
     * @throws IllegalStateException if {@link #addV} / {@link #addE} have already been called
     * @throws UncheckedIOException  if either file cannot be read or parsed
     */
    public SubgraphQueryBuilder fromFile(String graphFileName, String labelMapFileName) {
        requireMode(BuildMode.FILE);
        String graphPath    = DATA_DIR + graphFileName;
        String labelMapPath = DATA_DIR + labelMapFileName;
        try {
            // 1. Parse label map: {"0": "person", "1": "paper"}
            Map<Integer, String> labelMap = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> rawMap = mapper.readValue(
                    new File(labelMapPath),
                    new TypeReference<Map<String, String>>() {});
            for (Map.Entry<String, String> entry : rawMap.entrySet()) {
                labelMap.put(Integer.parseInt(entry.getKey()), entry.getValue());
            }

            // 2. Parse .graph file
            try (BufferedReader br = new BufferedReader(new FileReader(graphPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] parts = line.split("\\s+");
                    switch (parts[0]) {
                        case "v": {
                            int id       = Integer.parseInt(parts[1]);
                            int labelInt = Integer.parseInt(parts[2]);
                            String label = labelMap.getOrDefault(labelInt, String.valueOf(labelInt));
                            while (queryVertexLabels.size() <= id) queryVertexLabels.add(null);
                            queryVertexLabels.set(id, label);
                            internLabel(label);
                            break;
                        }
                        case "e": {
                            int u = Integer.parseInt(parts[1]);
                            int v = Integer.parseInt(parts[2]);
                            queryEdges.add(new int[]{u, v});
                            break;
                        }
                        default:
                            break; // skip "t" header and unknown lines
                    }
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(
                    "Failed to load query graph from files: " + ex.getMessage(), ex);
        }
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

    /**
     * Deduplicate results so that two matches with the same vertex set AND
     * edge set are counted only once.
     *
     * <p>This eliminates the automorphic duplicates that arise when the query
     * graph has symmetry — e.g. a triangle query on an undirected graph produces
     * 3! = 6 matches per triangle (one per permutation of the three query vertices),
     * all mapping to the same three data vertices and three data edges.
     * Calling {@code unique()} keeps only one representative per such group.
     *
     * <p>Deduplication is a cheap O(M × (qn + qe)) post-processing step applied
     * after the C++ enumeration returns; it does not affect the matching itself.
     *
     * @return this builder (fluent API)
     */
    public SubgraphQueryBuilder unique() {
        this.uniqueOnly = true;
        return this;
    }

    // ── Execution ───────────────────────────────────────────────────────

    /**
     * Runs the full pipeline and returns all valid subgraph matches.
     * Each inner list has one Vertex per query vertex, in query-vertex order
     * (index i = data vertex matched to query vertex i).
     * Order is preserved; if {@link #unique()} was called, matches with the same
     * vertex set and edge set are deduplicated before conversion to Vertex objects.
     */
    public List<List<Vertex>> execute() {
        List<List<Integer>> rawResults = executeRaw();

        if (uniqueOnly) rawResults = deduplicateMatches(rawResults);

        // Convert int indices → Vertex objects using the array built in executeRaw()
        List<List<Vertex>> results = new ArrayList<>(rawResults.size());
        for (List<Integer> match : rawResults) {
            List<Vertex> vertexMatch = new ArrayList<>(match.size());
            for (int idx : match)
                vertexMatch.add(indexToVertexArray[idx]);
            results.add(vertexMatch);
        }
        return results;
    }

    /**
     * Runs the full pipeline and returns matches as raw 0-based integer index lists.
     * Also populates {@link #indexToVertexArray} and {@link #dataEdgeIndex} as a
     * side effect, so that {@link #executeForWeb()} can use them without re-running.
     *
     * @return list of matches; each match is a list of data-graph vertex indices
     *         in query-vertex order (index i = data vertex matched to query vertex i)
     */
    private List<List<Integer>> executeRaw() {

        // 1. Collect data vertices in a stable, deterministic order
        List<Vertex> dataVertices = collectDataVertices();
        Map<Object, Integer> vertexToIndex = buildVertexIndexMap(dataVertices);

        // 2. Build Vertex[] array for O(1) index → Vertex lookup (no hashing)
        indexToVertexArray = new Vertex[dataVertices.size()];
        for (int i = 0; i < dataVertices.size(); i++)
            indexToVertexArray[i] = dataVertices.get(i);

        // 3. Intern all data-graph labels into the shared label map
        //    (query labels were already interned by addV / fromFile)
        for (Vertex v : dataVertices)
            internLabel(v.label());

        // 4. Write both graph files; writeDataGraph also populates dataEdgeIndex
        writeDataGraph(dataVertices, vertexToIndex);
        writeQueryGraph();

        // 5. Build the phi callback (compiles filter + holds vertex lookup)
        SubgraphPhiCallback phiCallback =
                new SubgraphPhiCallback(g, conditions, indexToVertexArray, filterQuery);

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

        // 8. Decode raw matches → List<List<Integer>>
        return decodeRawMatches(rawMatches, queryVertexLabels.size());
    }

    /**
     * Convenience wrapper: execute + serialize for web visualization.
     * Uses raw int matches together with the pre-built Vertex array and edge
     * index — no additional graph traversal or index construction is performed.
     */
    public Map<String, Object> executeForWeb() {
        List<List<Integer>> rawMatches = executeRaw();
        if (uniqueOnly) rawMatches = deduplicateMatches(rawMatches);
        // indexToVertexArray and dataEdgeIndex are populated as side effects of executeRaw()
        return VsetResultSerializer.serializeSubgraph(
                rawMatches, queryEdges, indexToVertexArray, dataEdgeIndex);
    }

    /**
     * Writes the full data graph to {@value DATA_GRAPH_PATH}.
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
    /**
     * Writes the full data graph to {@value DATA_GRAPH_PATH} and simultaneously
     * builds {@link #dataEdgeIndex} for use by {@link #executeForWeb()}.
     *
     * <p>Format (same as Graph::load in graph.h):
     * <pre>
     *   t N M
     *   v &lt;id&gt; &lt;labelInt&gt; &lt;degree&gt;
     *   ...
     *   e &lt;u&gt; &lt;v&gt;
     *   ...
     * </pre>
     *
     * <p>The edge index key packs two 32-bit indices into one {@code long}:
     * {@code key = (long) uIdx << 32 | vIdx}.
     * A single-level HashMap with a primitive-backed long key is faster than
     * two nested HashMaps and avoids extra boxing.
     * The edge map is pre-serialized here so VsetResultSerializer needs no
     * further graph traversal.
     */
    private void writeDataGraph(List<Vertex> vertices,
                                Map<Object, Integer> vertexToIndex) {

        List<int[]> edges  = new ArrayList<>();
        int[]       degree = new int[vertices.size()];

        // Reset edge index — populated in the same traversal below
        dataEdgeIndex = new HashMap<>();

        g.getGraph().edges().forEachRemaining(edge -> {
            Integer uIdx = vertexToIndex.get(edge.outVertex().id());
            Integer vIdx = vertexToIndex.get(edge.inVertex().id());
            if (uIdx != null && vIdx != null) {
                // ── Write to .graph file ──────────────────────────────────
                edges.add(new int[]{uIdx, vIdx});
                degree[uIdx]++;
                degree[vIdx]++;

                // ── Build long-keyed edge index ───────────────────────────
                // Store the Edge reference directly; serialization is deferred
                // to VsetResultSerializer so only matched edges are serialized.
                dataEdgeIndex.put(edgeKey(uIdx, vIdx), edge);
            }
        });

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
     * Writes the query graph to {@value QUERY_GRAPH_PATH}.
     * String labels declared via {@link #addV} or loaded via {@link #fromFile}
     * are replaced by their integer IDs from the shared {@link #labelToInt} map.
     */
    private void writeQueryGraph() {
        int qn = queryVertexLabels.size();
        int qm = queryEdges.size();

        int[] degree = new int[qn];
        for (int[] e : queryEdges) {
            degree[e[0]]++;
            degree[e[1]]++;
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(QUERY_GRAPH_PATH))) {
            pw.printf("t %d %d%n", qn, qm);
            for (int i = 0; i < qn; i++) {
                String lbl   = queryVertexLabels.get(i);
                int labelInt = (lbl != null) ? internLabel(lbl) : 0;
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
     * Removes matches that are automorphic duplicates — i.e. matches that map to
     * the same set of data vertices AND the same set of data edges, differing only
     * in how query vertices are assigned to data vertices.
     *
     * <p>Key construction:
     * <ul>
     *   <li>Vertex key: sort the vertex indices in the match → canonical form independent
     *       of query-vertex ordering</li>
     *   <li>Edge key: for each query edge {u, v}, resolve to an actual data edge via
     *       {@link #dataEdgeIndex}, collect the packed long keys, sort them</li>
     * </ul>
     * The combined (sorted vertices, sorted edge keys) pair is the deduplication key.
     * Two matches are considered identical iff both parts are equal.
     *
     * <p>Complexity: O(M × (qn log qn + qe log qe)) where M = match count,
     * qn = query vertex count, qe = query edge count. Negligible relative to the
     * C++ enumeration cost.
     */
    private List<List<Integer>> deduplicateMatches(List<List<Integer>> matches) {
        Set<String> seen = new HashSet<>();
        List<List<Integer>> unique = new ArrayList<>();

        for (List<Integer> match : matches) {
            // Sorted vertex indices → vertex part of key
            int[] sortedVerts = match.stream().mapToInt(Integer::intValue).sorted().toArray();

            // Resolved + sorted edge long-keys → edge part of key
            long[] sortedEdgeKeys = queryEdges.stream()
                    .mapToLong(qEdge -> {
                        int u = match.get(qEdge[0]);
                        int v = match.get(qEdge[1]);
                        // Normalize direction: always store smaller index first in the key
                        long fwd = edgeKey(u, v);
                        long rev = edgeKey(v, u);
                        // Use whichever direction actually exists in the index
                        return dataEdgeIndex.containsKey(fwd) ? fwd : rev;
                    })
                    .sorted()
                    .toArray();

            String key = Arrays.toString(sortedVerts) + Arrays.toString(sortedEdgeKeys);
            if (seen.add(key)) unique.add(match);
        }
        return unique;
    }

    private List<List<Integer>> decodeRawMatches(long[] raw, int qn) {
        List<List<Integer>> results = new ArrayList<>();
        if (raw == null || raw.length < 1) return results;

        long matchCount = raw[0];
        int  offset     = 1;

        for (long mi = 0; mi < matchCount; mi++) {
            List<Integer> matchList = new ArrayList<>(qn);
            for (int qi = 0; qi < qn; qi++)
                matchList.add((int) raw[offset++]);
            if (evaluateHaving(matchList.size()))
                results.add(matchList);
        }
        return results;
    }

    /**
     * Computes a canonical key for a match that is invariant to query-vertex
     * permutations, i.e. two matches with the same vertex set AND edge set
     * (considering the actual data edges implied by queryEdges) produce the
     * same key.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Collect the set of matched data-vertex indices.</li>
     *   <li>For each query edge {u, v}, pack the corresponding data-vertex
     *       pair as a sorted {@code long} (smaller index in high 32 bits) so
     *       that direction does not matter for undirected matching.</li>
     *   <li>Sort both sets and hash them together with a simple polynomial
     *       rolling hash.</li>
     * </ol>
     *
     * Collisions are astronomically unlikely for realistic graph sizes
     * (indices fit in 32 bits), but the implementation is correct-by-design:
     * two structurally identical matches always produce the same key.
     */
    private long canonicalKey(List<Integer> match) {
        // --- vertex set ---
        int[] verts = match.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(verts);

        // --- edge set (sorted pairs from query edges applied to this match) ---
        long[] edgePairs = new long[queryEdges.size()];
        for (int i = 0; i < queryEdges.size(); i++) {
            int[] qe = queryEdges.get(i);
            int a = match.get(qe[0]);
            int b = match.get(qe[1]);
            // Normalize direction: smaller index first
            edgePairs[i] = (a <= b) ? edgeKey(a, b) : edgeKey(b, a);
        }
        Arrays.sort(edgePairs);

        // --- rolling hash combining both sets ---
        long h = 17L;
        for (int v : verts)   h = h * 31 + v;
        h = h * 1000003L;
        for (long e : edgePairs) h = h * 31 + e;
        return h;
    }

    // ── Small utilities ─────────────────────────────────────────────────

    /**
     * Enforces mutual exclusion between inline (addV/addE) and file-based
     * (fromFile) construction modes.
     *
     * @param required the mode that the current call belongs to
     * @throws IllegalStateException if the builder is already in the other mode
     */
    private void requireMode(BuildMode required) {
        if (buildMode == BuildMode.NONE) {
            buildMode = required;
        } else if (buildMode != required) {
            String current = buildMode == BuildMode.INLINE
                    ? "addV() / addE()" : "fromFile()";
            String attempted = required == BuildMode.INLINE
                    ? "addV() / addE()" : "fromFile()";
            throw new IllegalStateException(
                    "Cannot call " + attempted + " after " + current +
                            " has already been used. Use one construction mode per query.");
        }
    }

    /** Assigns a stable integer ID to a label string if not already seen. */
    private int internLabel(String label) {
        return labelToInt.computeIfAbsent(label, k -> labelToInt.size());
    }

    /**
     * Packs two 32-bit vertex indices into a single {@code long} for use as
     * the edge index key.  Using a flat {@code long} key avoids the overhead
     * of two nested {@code HashMap} lookups and the boxing of {@code Integer}.
     *
     * @param u source vertex index
     * @param v destination vertex index
     * @return packed key: {@code (long) u << 32 | (v & 0xFFFFFFFFL)}
     */
    private static long edgeKey(int u, int v) {
        return ((long) u << 32) | (v & 0xFFFFFFFFL);
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
        for (int i = 0; i < vertices.size(); i++) {
            map.put(vertices.get(i).id(), i);
//            System.out.println(i + " " + vertices.get(i).id());
        }
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
            return true;
        }
    }
}
