package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


/**
 * PCST parameter validation tests.
 *
 * Tests invalid inputs before calling the native PCST algorithm.
 *
 * Test cases:
 * 1. Missing vertex prize property
 * 2. Missing edge cost property
 * 3. Non-numeric vertex prize
 * 4. Non-numeric edge cost
 * 5. Negative prize
 * 6. Negative cost
 * 7. NaN prize
 * 8. Infinite cost
 * 9. Unknown vertex ID
 * 10. Unknown edge ID
 * 11. Missing prize configuration
 * 12. Invalid property keys
 */
class PCSTValidationTest {


    // ==========================================
    // 1. Create a standard test graph
    // ==========================================

    private TinkerGraph createGraph() {

        TinkerGraph graph = TinkerGraph.open();

        Vertex a = graph.addVertex(T.id, 7001);
        Vertex b = graph.addVertex(T.id, 7002);
        Vertex c = graph.addVertex(T.id, 7003);
        Vertex d = graph.addVertex(T.id, 7004);

        Edge e1 = a.addEdge("link", b);
        Edge e2 = b.addEdge("link", c);
        Edge e3 = b.addEdge("link", d);

        // Vertex prizes
        a.property("prize", 10.0);
        b.property("prize", 0.0);
        c.property("prize", 8.0);
        d.property("prize", 1.0);

        // Edge costs
        e1.property("cost", 2.0);
        e2.property("cost", 2.0);
        e3.property("cost", 5.0);

        return graph;
    }


    // ==========================================
    // 2. Create traversal source
    // ==========================================

    private SecondOrderTraversalSource traversal(
            TinkerGraph graph) {

        return graph.traversal(
                SecondOrderTraversalSource.class
        );
    }


    // ==========================================
    // Test 1: Missing vertex prize property
    // ==========================================

    @Test
    void testMissingVertexPrizeProperty() {

        try (TinkerGraph graph = createGraph()) {

            Vertex vertex = graph.vertices(7004).next();

            vertex.property("prize").remove();

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Missing vertex prize property"
            );
        }
    }


    // ==========================================
    // Test 2: Missing edge cost property
    // ==========================================

    @Test
    void testMissingEdgeCostProperty() {

        try (TinkerGraph graph = createGraph()) {

            Edge edge = graph.traversal()
                    .E()
                    .has("cost", 5.0)
                    .next();

            edge.property("cost").remove();

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Missing edge cost property"
            );
        }
    }


    // ==========================================
    // Test 3: Non-numeric vertex prize
    // ==========================================

    @Test
    void testNonNumericVertexPrize() {

        try (TinkerGraph graph = createGraph()) {

            Vertex vertex = graph.vertices(7001).next();

            vertex.property("prize", "ten");

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Non-numeric vertex prize"
            );
        }
    }


    // ==========================================
    // Test 4: Non-numeric edge cost
    // ==========================================

    @Test
    void testNonNumericEdgeCost() {

        try (TinkerGraph graph = createGraph()) {

            Edge edge = graph.traversal()
                    .E()
                    .has("cost", 2.0)
                    .next();

            edge.property("cost", "expensive");

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Non-numeric edge cost"
            );
        }
    }


    // ==========================================
    // Test 5: Negative vertex prize
    // ==========================================

    @Test
    void testNegativeVertexPrize() {

        try (TinkerGraph graph = createGraph()) {

            Vertex vertex = graph.vertices(7001).next();

            vertex.property("prize", -10.0);

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Negative vertex prize"
            );
        }
    }


    // ==========================================
    // Test 6: Negative edge cost
    // ==========================================

    @Test
    void testNegativeEdgeCost() {

        try (TinkerGraph graph = createGraph()) {

            Edge edge = graph.traversal()
                    .E()
                    .has("cost", 2.0)
                    .next();

            edge.property("cost", -2.0);

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Negative edge cost"
            );
        }
    }


    // ==========================================
    // Test 7: NaN vertex prize
    // ==========================================

    @Test
    void testNaNVertexPrize() {

        try (TinkerGraph graph = createGraph()) {

            Vertex vertex = graph.vertices(7001).next();

            vertex.property("prize", Double.NaN);

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] NaN vertex prize"
            );
        }
    }


    // ==========================================
    // Test 8: Infinite edge cost
    // ==========================================

    @Test
    void testInfiniteEdgeCost() {

        try (TinkerGraph graph = createGraph()) {

            Edge edge = graph.traversal()
                    .E()
                    .has("cost", 5.0)
                    .next();

            edge.property("cost", Double.POSITIVE_INFINITY);

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize("prize")
                            .cost("cost")
                            .execute()
            );

            System.out.println(
                    "[PASS] Infinite edge cost"
            );
        }
    }


    // ==========================================
    // Test 9: Unknown vertex ID in prize Map
    // ==========================================

    @Test
    void testUnknownVertexId() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> prizes = Map.of(
                    9999, 10.0
            );

            Map<Object, Double> costs = Map.of();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize(prizes)
                            .cost(costs)
                            .execute()
            );

            System.out.println(
                    "[PASS] Unknown vertex ID"
            );
        }
    }


    // ==========================================
    // Test 10: Unknown edge ID in cost Map
    // ==========================================

    @Test
    void testUnknownEdgeId() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> prizes = Map.of();

            Map<Object, Double> costs = Map.of(
                    "missing-edge", 2.0
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST()
                            .prize(prizes)
                            .cost(costs)
                            .execute()
            );

            System.out.println(
                    "[PASS] Unknown edge ID"
            );
        }
    }


    // ==========================================
    // Test 11: Missing prize configuration
    // ==========================================

    @Test
    void testMissingPrizeConfiguration() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalStateException.class,
                    () -> g.PCST()
                            .cost(Map.of())
                            .execute()
            );

            System.out.println(
                    "[PASS] Missing prize configuration"
            );
        }
    }


    // ==========================================
    // Test 12: Invalid property keys
    // ==========================================

    @Test
    void testInvalidPropertyKeys() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            // Empty prize property key
            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().prize("")
            );

            // Blank prize property key
            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().prize("   ")
            );

            // Empty cost property key
            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().cost("")
            );

            // Blank cost property key
            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().cost("   ")
            );

            System.out.println(
                    "[PASS] Invalid property keys"
            );
        }
    }


    // ==========================================
    // Test 13: Null prize Map
    // ==========================================

    @Test
    void testNullPrizeMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().prize(
                            (Map<?, ? extends Number>) null
                    )
            );

            System.out.println(
                    "[PASS] Null prize Map"
            );
        }
    }


    // ==========================================
    // Test 14: Null cost Map
    // ==========================================

    @Test
    void testNullCostMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().cost(
                            (Map<?, ? extends Number>) null
                    )
            );

            System.out.println(
                    "[PASS] Null cost Map"
            );
        }
    }


    // ==========================================
    // Test 15: Negative prize in Map
    // ==========================================

    @Test
    void testNegativePrizeInMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> prizes = Map.of(
                    7001, -10.0
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().prize(prizes)
            );

            System.out.println(
                    "[PASS] Negative prize in Map"
            );
        }
    }


    // ==========================================
    // Test 16: Negative cost in Map
    // ==========================================

    @Test
    void testNegativeCostInMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> costs = Map.of(
                    8001, -2.0
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().cost(costs)
            );

            System.out.println(
                    "[PASS] Negative cost in Map"
            );
        }
    }


    // ==========================================
    // Test 17: Non-finite prize in Map
    // ==========================================

    @Test
    void testNaNPrizeInMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> prizes = Map.of(
                    7001, Double.NaN
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().prize(prizes)
            );

            System.out.println(
                    "[PASS] NaN prize in Map"
            );
        }
    }


    // ==========================================
    // Test 18: Infinite cost in Map
    // ==========================================

    @Test
    void testInfiniteCostInMap() {

        try (TinkerGraph graph = createGraph()) {

            SecondOrderTraversalSource g = traversal(graph);

            Map<Object, Double> costs = Map.of(
                    8001, Double.POSITIVE_INFINITY
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.PCST().cost(costs)
            );

            System.out.println(
                    "[PASS] Infinite cost in Map"
            );
        }
    }
}
