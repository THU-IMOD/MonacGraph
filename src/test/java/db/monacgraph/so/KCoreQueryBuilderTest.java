package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KCoreQueryBuilderTest {

    @Test
    void computesTwoCore() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex v1 = graph.addVertex(T.id, 1);
            Vertex v2 = graph.addVertex(T.id, 2);
            Vertex v3 = graph.addVertex(T.id, 3);
            Vertex v4 = graph.addVertex(T.id, 4);
            graph.addVertex(T.id, 5);

            // 1、2、3 构成三角形
            v1.addEdge("link", v2);
            v2.addEdge("link", v3);
            v3.addEdge("link", v1);

            // 4 只有一个邻居
            v3.addEdge("link", v4);

            // 5 是孤立点

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.KCore()
                            .k(2)
                            .execute()
            );

            assertEquals(Set.of(1, 2, 3), result);
        }
    }

    @Test
    void zeroCoreContainsAllVertices() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);
            graph.addVertex(T.id, 2);
            graph.addVertex(T.id, 3);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.KCore()
                            .k(0)
                            .execute()
            );

            assertEquals(Set.of(1, 2, 3), result);
        }
    }

    @Test
    void computesCoreOnlyInsideCandidateVertices() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex v1 = graph.addVertex(T.id, 1);
            Vertex v2 = graph.addVertex(T.id, 2);
            Vertex v3 = graph.addVertex(T.id, 3);
            Vertex v4 = graph.addVertex(T.id, 4);

            v1.addEdge("link", v2);
            v2.addEdge("link", v3);
            v3.addEdge("link", v1);

            // 4 与 3 相连，但 4 不放入候选顶点集
            v3.addEdge("link", v4);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            List<Vertex> candidates = g.V(1, 2, 3).toList();

            Set<Object> result = ids(
                    g.KCore()
                            .on(candidates)
                            .k(2)
                            .execute()
            );

            assertEquals(Set.of(1, 2, 3), result);
        }
    }

    @Test
    void webResultContainsInducedEdges() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex v1 = graph.addVertex(T.id, 1);
            Vertex v2 = graph.addVertex(T.id, 2);
            Vertex v3 = graph.addVertex(T.id, 3);
            Vertex v4 = graph.addVertex(T.id, 4);

            // 1、2、3 构成三角形
            v1.addEdge("link", v2);
            v2.addEdge("link", v3);
            v3.addEdge("link", v1);

            // 4 会因为度数不足被移除
            v3.addEdge("link", v4);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Map<String, Object> result = g.KCore()
                    .k(2)
                    .executeForWeb();

            assertEquals("VsetResult", result.get("type"));

            List<?> subsets =
                    (List<?>) result.get("subsets");

            assertEquals(1, subsets.size());

            Map<?, ?> subset =
                    (Map<?, ?>) subsets.get(0);

            List<?> vertices =
                    (List<?>) subset.get("vertices");

            List<?> edges =
                    (List<?>) subset.get("edges");

            assertEquals(3, vertices.size());
            assertEquals(3, edges.size());
        }
    }

    @Test
    void rejectsNegativeK() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> g.KCore().k(-1)
            );
        }
    }

    private static Set<Object> ids(Set<Vertex> vertices) {
        return vertices.stream()
                .map(Vertex::id)
                .collect(Collectors.toSet());
    }
}