package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

class KHopQueryBuilderTest {
    @Test
    void boundedBothDirectionsAndWebInducedEdges() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            Vertex c = graph.addVertex(T.id, 3);
            Vertex d = graph.addVertex(T.id, 4);
            graph.addVertex(T.id, 5);
            a.addEdge("link", b);
            c.addEdge("link", b); // Requires incoming traversal to reach c.
            c.addEdge("link", d);
            b.addEdge("parallel", a);
            b.addEdge("loop", b);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            KHopQueryBuilder query = g.KHop(1);
            assertEquals(Set.of(1, 2), ids(query.execute()));
            assertEquals(Set.of(1), ids(query.depth(0).execute()));
            assertEquals(Set.of(1, 2, 3), ids(query.depth(2).execute()));
            Map<String, Object> web = query.executeForWeb();
            assertEquals("VsetResult", web.get("type"));
            List<?> subsets = (List<?>) web.get("subsets");
            assertEquals(1, subsets.size());
            Map<?, ?> subset = (Map<?, ?>) subsets.get(0);
            assertEquals(3, ((List<?>) subset.get("vertices")).size());
            assertEquals(4, ((List<?>) subset.get("edges")).size());
            assertEquals(Set.of(1, 2, 3, 4), ids(query.depth(100).execute()));
            assertEquals(Set.of(5), ids(g.KHop(5).depth(10).execute()));
            assertThrows(IllegalArgumentException.class, () -> query.depth(-1));
            assertThrows(NoSuchElementException.class, () -> g.KHop(99).execute());
        }
    }

    private static Set<Object> ids(Set<Vertex> vertices) {
        return vertices.stream().map(Vertex::id).collect(Collectors.toSet());
    }
}