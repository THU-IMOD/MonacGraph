package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


/**
 * PCST QueryBuilder test with TinkerGraph.
 *
 * Tests:
 *
 * 1. Graph creation
 * 2. Map-based prize and cost configuration
 * 3. Property-based prize and cost configuration
 * 4. Selected vertices and edges
 * 5. Objective function
 * 6. Web serialization
 */
class PCSTQueryBuilderTest {

    @Test
    void testPCSTWithTinkerGraph() {

        try (TinkerGraph graph = TinkerGraph.open()) {

            System.out.println("\n================================");
            System.out.println("PCST QUERY BUILDER TEST");
            System.out.println("================================");


            // =====================================
            // 1. Create vertices
            // =====================================

            Vertex a = graph.addVertex(
                    T.id, 7001
            );

            Vertex b = graph.addVertex(
                    T.id, 7002
            );

            Vertex c = graph.addVertex(
                    T.id, 7003
            );

            Vertex d = graph.addVertex(
                    T.id, 7004
            );

            System.out.println("[OK] Vertices created");


            // =====================================
            // 2. Create edges
            // =====================================

            Edge e1 = a.addEdge(
                    "link", b
            );

            Edge e2 = b.addEdge(
                    "link", c
            );

            Edge e3 = b.addEdge(
                    "link", d
            );

            System.out.println("[OK] Edges created");


            // =====================================
            // 3. Configure vertex prizes
            // =====================================

            a.property("prize", 10.0);

            b.property("prize", 0.0);

            c.property("prize", 8.0);

            d.property("prize", 1.0);


            // =====================================
            // 4. Configure edge costs
            // =====================================

            e1.property("cost", 2.0);

            e2.property("cost", 2.0);

            e3.property("cost", 5.0);

            System.out.println(
                    "[OK] Graph properties configured"
            );


            // =====================================
            // 5. Create traversal source
            // =====================================

            SecondOrderTraversalSource g =
                    graph.traversal(
                            SecondOrderTraversalSource.class
                    );

            System.out.println(
                    "[OK] SecondOrderTraversalSource created"
            );


            // =====================================
            // 6. Configure Map-based prizes
            // =====================================

            Map<Object, Double> prizes = Map.of(

                    7001, 10.0,

                    7002, 0.0,

                    7003, 8.0,

                    7004, 1.0

            );


            // =====================================
            // 7. Configure Map-based edge costs
            // =====================================

            Map<Object, Double> costs = Map.of(

                    e1.id(), 2.0,

                    e2.id(), 2.0,

                    e3.id(), 5.0

            );


            // =====================================
            // 8. Test Map-based PCST
            // =====================================

            System.out.println(
                    "[RUN] Testing Map-based PCST..."
            );

            PCSTResult result = g.PCST()

                    .prize(prizes)

                    .cost(costs)

                    .execute();

            assertNotNull(result);

            System.out.println(
                    "[OK] Map-based PCST executed"
            );


            // =====================================
            // 9. Verify selected vertices
            // =====================================

            Set<Object> selectedVertexIds =
                    result.getVertices()
                            .stream()
                            .map(Vertex::id)
                            .collect(Collectors.toSet());

            System.out.println(
                    "Selected vertices: " + selectedVertexIds
            );

            assertEquals(
                    Set.of(7001, 7002, 7003),
                    selectedVertexIds
            );


            // =====================================
            // 10. Verify selected edges
            // =====================================

            Set<Object> selectedEdgeIds =
                    result.getEdges()
                            .stream()
                            .map(Edge::id)
                            .collect(Collectors.toSet());

            System.out.println(
                    "Selected edges: " + selectedEdgeIds
            );

            assertEquals(
                    Set.of(e1.id(), e2.id()),
                    selectedEdgeIds
            );


            // =====================================
            // 11. Verify Map-based objective
            // =====================================

            assertEquals(
                    18.0,
                    result.getTotalPrize(),
                    1e-9
            );

            assertEquals(
                    4.0,
                    result.getTotalCost(),
                    1e-9
            );

            assertEquals(
                    14.0,
                    result.getObjective(),
                    1e-9
            );

            System.out.println(
                    "[OK] Map-based objective verified"
            );

            System.out.println(result);


            // =====================================
            // 12. Test Web serialization
            // =====================================

            Map<String, Object> webResult = g.PCST()

                    .prize(prizes)

                    .cost(costs)

                    .executeForWeb();

            assertNotNull(webResult);

            assertEquals(
                    "VsetResult",
                    webResult.get("type")
            );

            assertEquals(
                    1,
                    webResult.get("totalCount")
            );

            System.out.println(
                    "[OK] Web serialization verified"
            );


            // =====================================
            // 13. Test Property-based PCST
            // =====================================

            System.out.println(
                    "[RUN] Testing Property-based PCST..."
            );

            PCSTResult propertyResult = g.PCST()

                    .prize("prize")

                    .cost("cost")

                    .execute();

            assertNotNull(propertyResult);

            System.out.println(
                    "[OK] Property-based PCST executed"
            );


            // =====================================
            // 14. Verify Property-based vertices
            // =====================================

            Set<Object> propertyVertexIds =
                    propertyResult.getVertices()
                            .stream()
                            .map(Vertex::id)
                            .collect(Collectors.toSet());

            System.out.println(
                    "Property selected vertices: "
                            + propertyVertexIds
            );

            assertEquals(
                    Set.of(7001, 7002, 7003),
                    propertyVertexIds
            );


            // =====================================
            // 15. Verify Property-based edges
            // =====================================

            Set<Object> propertyEdgeIds =
                    propertyResult.getEdges()
                            .stream()
                            .map(Edge::id)
                            .collect(Collectors.toSet());

            System.out.println(
                    "Property selected edges: "
                            + propertyEdgeIds
            );

            assertEquals(
                    Set.of(e1.id(), e2.id()),
                    propertyEdgeIds
            );


            // =====================================
            // 16. Verify Property-based objective
            // =====================================

            assertEquals(
                    18.0,
                    propertyResult.getTotalPrize(),
                    1e-9
            );

            assertEquals(
                    4.0,
                    propertyResult.getTotalCost(),
                    1e-9
            );

            assertEquals(
                    14.0,
                    propertyResult.getObjective(),
                    1e-9
            );

            System.out.println(
                    "[OK] Property-based objective verified"
            );

            System.out.println(propertyResult);


            // =====================================
            // 17. Final result
            // =====================================

            System.out.println("--------------------------------");

            System.out.println(
                    "MAP-BASED PCST: PASSED"
            );

            System.out.println(
                    "PROPERTY-BASED PCST: PASSED"
            );

            System.out.println(
                    "WEB SERIALIZATION: PASSED"
            );

            System.out.println(
                    "PCST QUERY BUILDER TEST PASSED!"
            );

            System.out.println("================================\n");
        }
    }
}