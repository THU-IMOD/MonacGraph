package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ShortestPathQueryBuilderTest {

    private static Set<Object> ids(Set<Vertex> vs) {
        return vs.stream().map(Vertex::id).collect(Collectors.toSet());
    }

    @Test
    void weightedCostAndHopLimit() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex s = graph.addVertex(T.id, 1);
            Vertex a = graph.addVertex(T.id, 2);
            Vertex b = graph.addVertex(T.id, 3);
            Vertex t = graph.addVertex(T.id, 4);
            graph.addVertex(T.id, 5);
            s.addEdge("e", a);
            a.addEdge("e", b);
            b.addEdge("e", t);
            Edge direct = s.addEdge("e", t);

            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);

            assertEquals(Set.of(1, 4), ids(g.ShortestPath(1, 4).execute()));
            assertEquals(Set.of(1), ids(g.ShortestPath(1, 1).execute()));
            assertTrue(g.ShortestPath(1, 5).execute().isEmpty());
            assertTrue(g.ShortestPath(4, 1).execute().isEmpty());
            assertTrue(g.ShortestPath(1, 4).maxLength(0).execute().isEmpty());
            assertThrows(NoSuchElementException.class, () -> g.ShortestPath(99, 1).execute());

            Map<Edge, Double> cost = Map.of(direct, 10.0);
            assertEquals(Set.of(1, 2, 3, 4), ids(g.ShortestPath(1, 4).cost(cost).execute()));
            assertEquals(Set.of(1, 4), ids(g.ShortestPath(1, 4).cost(cost).maxLength(1).execute()));
            assertEquals(Set.of(1, 2, 3, 4), ids(g.ShortestPath(1, 4).cost(cost).maxLength(3).execute()));
            assertTrue(g.ShortestPath(2, 4).maxLength(1).execute().isEmpty());
            assertEquals(Set.of(2, 3, 4), ids(g.ShortestPath(2, 4).maxLength(2).execute()));

            assertEquals("VsetResult", g.ShortestPath(1, 4).cost(cost).executeForWeb().get("type"));
        }
    }

    @Test
    void undirectedAndDeterministic() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex s = graph.addVertex(T.id, 1);
            Vertex a = graph.addVertex(T.id, 2);
            Vertex b = graph.addVertex(T.id, 3);
            Vertex t = graph.addVertex(T.id, 4);
            s.addEdge("e", a);
            s.addEdge("e", b);
            a.addEdge("e", t);
            b.addEdge("e", t);

            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);

            assertTrue(g.ShortestPath(4, 1).execute().isEmpty());
            assertEquals(Set.of(1, 2, 4), ids(g.ShortestPath(4, 1).direction(Direction.BOTH).execute()));

            for (int i = 0; i < 5; i++) {
                assertEquals(Set.of(1, 2, 4), ids(g.ShortestPath(1, 4).execute()));
            }
        }
    }
}
