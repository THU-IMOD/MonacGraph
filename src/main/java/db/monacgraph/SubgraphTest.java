package db.monacgraph;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.so.SecondOrderTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class SubgraphTest {
    public static void main(String[] args) {

        try {
            // 1. Open graph and load data
            CommunityGraph graph = CommunityGraph.open("example");
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);

            graph.loadVertexProperty("exampleVertexProperty.csv");
            graph.loadEdgeProperty("exampleEdgeProperty.csv");

            System.out.println("Vertices: " + g.V().count().next());  // expected 13
            System.out.println("Edges:    " + g.E().count().next());  // expected 20

            // 2. Execute second-order subgraph matching query
            //
            // Query graph: triangle with three "person" vertices
            //
            //   0 (person)
            //  / \
            // 1 - 2      edges: 0-1, 1-2, 0-2
            //
            // Second-order constraint (∀x ∀y in the matched vertex set):
            //   x == y                              (same vertex, always true)
            //   || x.age + y.age >= 60              (sum of ages at least 60)
            //   || |x.age - y.age| < 4              (age difference less than 4)
            //
            // Semantics: any two vertices in a matched triangle must either
            // have similar ages (diff < 4) or a large combined age (>= 60);
            // triangles that violate this for any pair are filtered out.
            //
            // Expected matches (4 total):
            //   {Alice(30), Charlie(28), David(35)}
            //   {Bob(25),   Charlie(28), David(35)}
            //   {Henry(27), Irene(26),   Jack(29)}
            //   {Henry(27), Jack(29),    Karen(33)}
            //
            Set<Set<Vertex>> results = g.Subgraph()
                    .addV(2, "person")
                    .addV(1, "person")
                    .addV(0, "person")
                    .addE(0, 1)
                    .addE(1, 2)
                    .addE(0, 2)
                    .forall("x")
                    .forall("y")
                    .filter("x == y" +
                            " || (x.value('age') + y.value('age') >= 60)" +
                            " || (Math.abs(x.value('age') - y.value('age')) < 4)")
                    .execute();

            // 3. Print results
            System.out.println("\n=== Subgraph matches (expected 4) ===");
            System.out.println("Total: " + results.size());

            int idx = 1;
            for (Set<Vertex> match : results) {
                System.out.print("Match " + idx++ + ": ");
                for (Vertex v : match) {
                    System.out.printf("%s(age=%s) ", v.value("name"), v.value("age"));
                }
                System.out.println();
            }

            // 4. Assertion
            assert results.size() == 4 : "Expected 4 matches, got " + results.size();
            System.out.println("\nAll assertions passed.");

            graph.close();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}