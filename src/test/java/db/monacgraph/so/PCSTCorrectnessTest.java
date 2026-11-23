package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PCST Correctness Test
 *
 * Compare PCSTFast approximation results
 * against an independent exhaustive exact solver.
 *
 * The exact solver enumerates all possible trees
 * for small input graphs.
 */
class PCSTCorrectnessTest {

    private static final double EPS = 1e-9;

    // =====================================================
    // Test 1: High prize versus high edge cost
    // =====================================================

    @Test
    void testHighPrizeHighCostAgainstOracle() {

        try (TinkerGraph graph = TinkerGraph.open()) {

            System.out.println(
                    "\n[TEST] High prize versus high cost"
            );

            Vertex a = graph.addVertex(T.id, 9501);
            Vertex b = graph.addVertex(T.id, 9502);
            Vertex c = graph.addVertex(T.id, 9503);

            Edge e1 = a.addEdge("link", b);
            Edge e2 = b.addEdge("link", c);

            Map<Object, Double> prizes = Map.of(
                    9501, 100.0,
                    9502, 1.0,
                    9503, 90.0
            );

            Map<Object, Double> costs = Map.of(
                    e1.id(), 80.0,
                    e2.id(), 80.0
            );

            assertAgainstOracle(
                    "High prize versus high cost",
                    graph,
                    prizes,
                    costs
            );
        }
    }


    // =====================================================
    // Test 2: Steiner vertex
    // =====================================================

    @Test
    void testSteinerTreeAgainstOracle() {

        try (TinkerGraph graph = TinkerGraph.open()) {

            System.out.println(
                    "\n[TEST] Steiner tree"
            );

            Vertex center = graph.addVertex(T.id, 9810);

            Vertex a = graph.addVertex(T.id, 9811);
            Vertex b = graph.addVertex(T.id, 9812);
            Vertex c = graph.addVertex(T.id, 9813);

            Edge e1 = center.addEdge("link", a);
            Edge e2 = center.addEdge("link", b);
            Edge e3 = center.addEdge("link", c);

            Map<Object, Double> prizes = Map.of(
                    9810, 0.0,
                    9811, 20.0,
                    9812, 20.0,
                    9813, 20.0
            );

            Map<Object, Double> costs = Map.of(
                    e1.id(), 1.0,
                    e2.id(), 1.0,
                    e3.id(), 1.0
            );

            assertAgainstOracle(
                    "Steiner tree",
                    graph,
                    prizes,
                    costs
            );
        }
    }


    // =====================================================
    // Test 3: Graph containing a cycle
    // =====================================================

    @Test
    void testCycleGraphAgainstOracle() {

        try (TinkerGraph graph = TinkerGraph.open()) {

            System.out.println(
                    "\n[TEST] Cycle graph"
            );

            Vertex a = graph.addVertex(T.id, 9821);
            Vertex b = graph.addVertex(T.id, 9822);
            Vertex c = graph.addVertex(T.id, 9823);

            Edge e1 = a.addEdge("link", b);
            Edge e2 = b.addEdge("link", c);
            Edge e3 = a.addEdge("link", c);

            Map<Object, Double> prizes = Map.of(
                    9821, 10.0,
                    9822, 8.0,
                    9823, 7.0
            );

            Map<Object, Double> costs = Map.of(
                    e1.id(), 2.0,
                    e2.id(), 2.0,
                    e3.id(), 20.0
            );

            assertAgainstOracle(
                    "Cycle graph",
                    graph,
                    prizes,
                    costs
            );
        }
    }


    // =====================================================
    // Test 4: Disconnected graph
    // =====================================================

    @Test
    void testDisconnectedGraphAgainstOracle() {

        try (TinkerGraph graph = TinkerGraph.open()) {

            System.out.println(
                    "\n[TEST] Disconnected graph"
            );

            Vertex a = graph.addVertex(T.id, 9831);
            Vertex b = graph.addVertex(T.id, 9832);

            Vertex c = graph.addVertex(T.id, 9833);
            Vertex d = graph.addVertex(T.id, 9834);

            Edge e1 = a.addEdge("link", b);
            Edge e2 = c.addEdge("link", d);

            Map<Object, Double> prizes = Map.of(
                    9831, 10.0,
                    9832, 8.0,
                    9833, 4.0,
                    9834, 3.0
            );

            Map<Object, Double> costs = Map.of(
                    e1.id(), 3.0,
                    e2.id(), 1.0
            );

            assertAgainstOracle(
                    "Disconnected graph",
                    graph,
                    prizes,
                    costs
            );
        }
    }


    @Test
    void testRandomSmallGraphsAgainstOracle() {

        System.out.println(
                "\n================================"
        );

        System.out.println(
                "RANDOMIZED PCST CORRECTNESS TEST"
        );

        System.out.println(
                "================================"
        );

        Random random = new Random(20261123L);

        int testCases = 20;

        for (int testCase = 0;
            testCase < testCases;
            testCase++) {

            try (TinkerGraph graph = TinkerGraph.open()) {

                int n = 2 + random.nextInt(5);

                List<Vertex> vertices = new ArrayList<>();

                Map<Object, Double> prizes = new HashMap<>();

                Map<Object, Double> costs = new HashMap<>();

                // 1. Generate vertices and prizes
                for (int i = 0; i < n; i++) {

                    int id = 100000 + testCase * 10 + i;

                    Vertex vertex = graph.addVertex(
                            T.id,
                            id
                    );

                    vertices.add(vertex);

                    prizes.put(
                            id,
                            (double) random.nextInt(31)
                    );
                }

                // 2. Generate random edges and costs
                for (int i = 0; i < n; i++) {

                    for (int j = i + 1; j < n; j++) {

                        if (random.nextDouble() < 0.4) {

                            Edge edge = vertices.get(i)
                                    .addEdge(
                                            "link",
                                            vertices.get(j)
                                    );

                            costs.put(
                                    edge.id(),
                                    (double) (
                                            1 + random.nextInt(20)
                                    )
                            );
                        }
                    }
                }

                System.out.println(
                        "\n[Random Test " +
                        (testCase + 1) +
                        "/" + testCases + "]"
                );

                System.out.println(
                        "Vertices: " + n
                );

                System.out.println(
                        "Edges: " + costs.size()
                );

                // 3. Compare PCSTFast against exact Oracle
                assertAgainstOracle(
                        "Random Graph " + (testCase + 1),
                        graph,
                        prizes,
                        costs
                );
            }
        }

        System.out.println(
                "\n================================"
        );

        System.out.println(
                "ALL RANDOMIZED TESTS PASSED"
        );

        System.out.println(
                "================================"
        );
    }

    // =====================================================
    // Main comparison method
    // =====================================================

    private void assertAgainstOracle(
            String testName,
            TinkerGraph graph,
            Map<Object, Double> prizes,
            Map<Object, Double> costs) {

        // 1. Execute PCSTFast
        SecondOrderTraversalSource g =
                graph.traversal(
                        SecondOrderTraversalSource.class
                );

        PCSTResult result = g.PCST()
                .prize(prizes)
                .cost(costs)
                .execute();

        assertNotNull(result);

        // 2. Verify the returned tree
        assertValidTree(
                graph,
                result,
                prizes,
                costs
        );

        // 3. Calculate the algorithm's minimization cost
        double totalInputPrize = prizes.values()
                .stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        double omittedPrize =
                totalInputPrize - result.getTotalPrize();

        double algorithmCost =
                result.getTotalCost() + omittedPrize;

        // 4. Calculate exact optimal cost
        double optimalCost =
                exactPcstCost(
                        graph,
                        prizes,
                        costs
                );

        // 5. Calculate approximation ratio
        double ratio;

        if (optimalCost <= EPS) {

            ratio = algorithmCost <= EPS
                    ? 1.0
                    : Double.POSITIVE_INFINITY;

        } else {

            ratio = algorithmCost / optimalCost;
        }

        // 6. Print results
        System.out.println("--------------------------------");

        System.out.println("Test: " + testName);

        System.out.println(
                "Selected vertices: " +
                result.getVertices()
        );

        System.out.println(
                "Selected edges: " +
                result.getEdges()
        );

        System.out.println(
                "Total Prize: " +
                result.getTotalPrize()
        );

        System.out.println(
                "Total Cost: " +
                result.getTotalCost()
        );

        System.out.println(
                "Objective: " +
                result.getObjective()
        );

        System.out.println(
                "Algorithm minimization cost: " +
                algorithmCost
        );

        System.out.println(
                "Exact optimal cost: " +
                optimalCost
        );

        System.out.println(
                "Approximation ratio: " +
                ratio
        );

        // 7. Verify objective calculation
        assertEquals(
                result.getTotalPrize()
                        - result.getTotalCost(),
                result.getObjective(),
                EPS,
                "Incorrect objective"
        );

        // 8. Verify expected 2-approximation bound
        assertTrue(
                algorithmCost <= 2.0 * optimalCost + EPS,
                "PCSTFast exceeds the expected 2-approximation bound. "
                        + "Algorithm cost = " + algorithmCost
                        + ", Optimal cost = " + optimalCost
                        + ", Ratio = " + ratio
        );

        System.out.println(
                "[PASS] " + testName
        );
    }


    // =====================================================
    // Independent exact PCST solver
    //
    // Enumerate all edge subsets.
    // Keep only connected acyclic subgraphs (trees).
    // Also consider every single-vertex tree.
    //
    // This is exponential and intended only for
    // small graphs.
    // =====================================================

    private double exactPcstCost(
            TinkerGraph graph,
            Map<Object, Double> prizes,
            Map<Object, Double> costs) {

        List<Vertex> vertices = new ArrayList<>();

        graph.vertices().forEachRemaining(
                vertices::add
        );

        List<Edge> graphEdges = new ArrayList<>();

        graph.edges().forEachRemaining(
                graphEdges::add
        );

        int n = vertices.size();
        int m = graphEdges.size();

        if (n == 0) {
            return 0.0;
        }

        if (m > 20) {
            throw new IllegalArgumentException(
                    "Exact oracle supports at most 20 edges."
            );
        }

        // 1. Map vertex IDs to integer indices
        Map<Object, Integer> indexMap = new HashMap<>();

        double totalPrize = 0.0;

        for (int i = 0; i < n; i++) {

            Object vertexId = vertices.get(i).id();

            indexMap.put(vertexId, i);

            if (!prizes.containsKey(vertexId)) {
                throw new IllegalArgumentException(
                        "Missing prize for vertex: " + vertexId
                );
            }

            totalPrize += prizes.get(vertexId);
        }

        // 2. Convert graph edges into indexed edges
        List<OracleEdge> edges = new ArrayList<>();

        for (Edge edge : graphEdges) {

            Object uId = edge.outVertex().id();
            Object vId = edge.inVertex().id();

            if (!costs.containsKey(edge.id())) {
                throw new IllegalArgumentException(
                        "Missing cost for edge: " + edge.id()
                );
            }

            int u = indexMap.get(uId);
            int v = indexMap.get(vId);

            edges.add(
                    new OracleEdge(
                            u,
                            v,
                            costs.get(edge.id())
                    )
            );
        }

        // 3. Initial upper bound:
        // Select no vertices.
        double bestCost = totalPrize;

        // 4. Consider every single-vertex tree
        for (Vertex vertex : vertices) {

            double candidate =
                    totalPrize - prizes.get(vertex.id());

            bestCost = Math.min(
                    bestCost,
                    candidate
            );
        }

        // 5. Enumerate every possible edge subset
        long subsetCount = 1L << m;

        for (long mask = 1; mask < subsetCount; mask++) {

            int[] parent = new int[n];
            int[] rank = new int[n];

            boolean[] included = new boolean[n];

            for (int i = 0; i < n; i++) {
                parent[i] = i;
            }

            double edgeCost = 0.0;

            boolean hasCycle = false;

            // 6. Build the selected edge subgraph
            for (int i = 0; i < m; i++) {

                if ((mask & (1L << i)) == 0) {
                    continue;
                }

                OracleEdge edge = edges.get(i);

                int u = edge.u();
                int v = edge.v();

                // A selected edge includes both endpoints
                included[u] = true;
                included[v] = true;

                edgeCost += edge.cost();

                // Detect cycles using Union-Find
                if (!union(parent, rank, u, v)) {

                    hasCycle = true;
                    break;
                }
            }

            if (hasCycle) {
                continue;
            }

            // 7. Check whether all included vertices
            // belong to the same connected component
            int root = -1;

            boolean connected = true;

            for (int i = 0; i < n; i++) {

                if (!included[i]) {
                    continue;
                }

                int currentRoot = find(parent, i);

                if (root == -1) {
                    root = currentRoot;
                } else if (root != currentRoot) {
                    connected = false;
                    break;
                }
            }

            if (!connected) {
                continue;
            }

            // 8. Calculate selected vertex prize
            double selectedPrize = 0.0;

            for (int i = 0; i < n; i++) {

                if (included[i]) {

                    Object vertexId = vertices.get(i).id();

                    selectedPrize += prizes.get(vertexId);
                }
            }

            // 9. Calculate minimization objective
            double candidateCost =
                    edgeCost
                    + totalPrize
                    - selectedPrize;

            bestCost = Math.min(
                    bestCost,
                    candidateCost
            );
        }

        return bestCost;
    }


    // =====================================================
    // Union-Find
    // =====================================================

    private int find(int[] parent, int x) {

        if (parent[x] != x) {

            parent[x] = find(
                    parent,
                    parent[x]
            );
        }

        return parent[x];
    }


    private boolean union(
            int[] parent,
            int[] rank,
            int x,
            int y) {

        int rootX = find(parent, x);
        int rootY = find(parent, y);

        if (rootX == rootY) {
            return false;
        }

        if (rank[rootX] < rank[rootY]) {

            parent[rootX] = rootY;

        } else if (rank[rootX] > rank[rootY]) {

            parent[rootY] = rootX;

        } else {

            parent[rootY] = rootX;
            rank[rootX]++;
        }

        return true;
    }


    // =====================================================
    // Verify PCST output is a valid tree
    // =====================================================

    private void assertValidTree(
            TinkerGraph graph,
            PCSTResult result,
            Map<Object, Double> prizes,
            Map<Object, Double> costs) {

        List<Vertex> selectedVertices =
                result.getVertices();

        List<Edge> selectedEdges =
                result.getEdges();

        assertFalse(
                selectedVertices.isEmpty(),
                "PCST should return a non-empty tree"
        );

        Set<Object> vertexIds = new HashSet<>();

        double calculatedPrize = 0.0;

        // 1. Verify selected vertices
        for (Vertex vertex : selectedVertices) {

            Object id = vertex.id();

            assertTrue(
                    prizes.containsKey(id),
                    "Selected vertex does not belong to input graph"
            );

            assertTrue(
                    vertexIds.add(id),
                    "Duplicate selected vertex"
            );

            calculatedPrize += prizes.get(id);
        }

        // 2. Build adjacency list
        Map<Object, Set<Object>> adjacency =
                new HashMap<>();

        for (Object id : vertexIds) {
            adjacency.put(id, new HashSet<>());
        }

        double calculatedCost = 0.0;

        Set<Object> edgeIds = new HashSet<>();

        // 3. Verify selected edges
        for (Edge edge : selectedEdges) {

            Object edgeId = edge.id();

            Object u = edge.outVertex().id();
            Object v = edge.inVertex().id();

            assertTrue(
                    costs.containsKey(edgeId),
                    "Selected edge does not belong to input graph"
            );

            assertTrue(
                    edgeIds.add(edgeId),
                    "Duplicate selected edge"
            );

            assertTrue(
                    vertexIds.contains(u),
                    "Edge endpoint is not selected"
            );

            assertTrue(
                    vertexIds.contains(v),
                    "Edge endpoint is not selected"
            );

            adjacency.get(u).add(v);
            adjacency.get(v).add(u);

            calculatedCost += costs.get(edgeId);
        }

        // 4. Verify prize and cost calculations
        assertEquals(
                calculatedPrize,
                result.getTotalPrize(),
                EPS
        );

        assertEquals(
                calculatedCost,
                result.getTotalCost(),
                EPS
        );

        // 5. A tree with n vertices has n - 1 edges
        assertEquals(
                selectedVertices.size() - 1,
                selectedEdges.size(),
                "Selected subgraph must be a tree"
        );

        // 6. BFS connectivity check
        Set<Object> visited = new HashSet<>();

        Queue<Object> queue = new ArrayDeque<>();

        Object start = selectedVertices.get(0).id();

        visited.add(start);
        queue.add(start);

        while (!queue.isEmpty()) {

            Object current = queue.poll();

            for (Object neighbor : adjacency.get(current)) {

                if (visited.add(neighbor)) {

                    queue.add(neighbor);
                }
            }
        }

        assertEquals(
                vertexIds,
                visited,
                "Selected vertices must form one connected component"
        );
    }


    // =====================================================
    // Oracle edge representation
    // =====================================================

    private record OracleEdge(
            int u,
            int v,
            double cost
    ) {
    }
}