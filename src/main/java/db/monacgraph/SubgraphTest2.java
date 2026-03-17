package db.monacgraph;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.so.SecondOrderTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class SubgraphTest2 {
    public static void main(String[] args) {

        try {
            // 1. Open graph and load data
            CommunityGraph graph = CommunityGraph.open("data");
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);

            graph.loadVertexProperty("dataVertexProperty.csv");

            System.out.println("Vertices: " + g.V().count().next());
            System.out.println("Edges:    " + g.E().count().next());

            // 2. Execute second-order subgraph matching query
            //
            // Query graph topology is loaded from:
            //   data/query.graph    — defines vertices and edges
            //   data/labelMap.json  — maps integer label IDs to vertex label names
            //
            // Second-order constraint (∀x ∀y in the matched vertex set):
            //   x == y                                          (same vertex, always true)
            //   || |x.identity - y.identity| > 100             (identity values differ by more than 100)
            //
            // Semantics: any two distinct vertices in a matched subgraph must have
            // identity values that differ by more than 100; matches where any pair
            // violates this condition are filtered out.
            //
            List<List<Vertex>> results = g.Subgraph()
                    .fromFile("query.graph", "labelMap.json")
                    .forall("x")
                    .forall("y")
                    .filter("x == y" +
                            " || (Math.abs(x.value('identity') - y.value('identity')) > 100)")
                    .execute();

            // 3. Print results
            System.out.println("\n=== Subgraph matches ===");
            System.out.println("Total: " + results.size());

            int idx = 1;
            for (List<Vertex> match : results) {
                System.out.print("Match " + idx++ + ": ");
                for (Vertex v : match) {
                    System.out.printf("(identity=%d) ", (Long)v.value("identity"));
                }
                System.out.println();
                if (idx > 10)
                    break;
            }

            graph.close();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}