package db.monacgraph.serialize;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VsetResultSerializerTest {

    @Test
    void inducedIncludesEveryEdgeInsideTheVertexSet() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Triangle triangle = Triangle.create(graph);

            Map<String, Object> result = VsetResultSerializer.serialize(
                    ResultSubgraph.induced(List.of(triangle.a, triangle.b, triangle.c)));

            assertEquals("VsetResult", result.get("type"));
            assertEquals(1, result.get("totalCount"));
            assertEquals(3, edgeCount(result, 0));
            assertEquals(3, vertexCount(result, 0));
        }
    }

    @Test
    void ofDrawsOnlyTheEdgesThatWerePassedIn() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Triangle triangle = Triangle.create(graph);

            Map<String, Object> result = VsetResultSerializer.serialize(
                    ResultSubgraph.of(
                            List.of(triangle.a, triangle.b, triangle.c),
                            List.of(triangle.ab)));

            assertEquals(1, edgeCount(result, 0));
            assertEquals(triangle.ab.id(), firstEdgeId(result, 0));
        }
    }

    @Test
    void inducedAllAndLegacyVertexSetsProduceTheSameWebPayload() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Triangle triangle = Triangle.create(graph);
            Set<Vertex> component = Set.of(triangle.a, triangle.b, triangle.c);
            Set<Set<Vertex>> boxed = new HashSet<>();
            boxed.add(component);

            Map<String, Object> legacy = VsetResultSerializer.serialize(boxed);
            Map<String, Object> rewritten = VsetResultSerializer.serialize(
                    ResultSubgraph.inducedAll(boxed));

            assertEquals(legacy.get("type"), rewritten.get("type"));
            assertEquals(legacy.get("totalCount"), rewritten.get("totalCount"));
            assertEquals(edgeCount(legacy, 0), edgeCount(rewritten, 0));
            assertEquals(vertexCount(legacy, 0), vertexCount(rewritten, 0));
        }
    }

    @SuppressWarnings("unchecked")
    private static int edgeCount(Map<String, Object> result, int subset) {
        List<Map<String, Object>> subsets = (List<Map<String, Object>>) result.get("subsets");
        List<?> edges = (List<?>) subsets.get(subset).get("edges");
        return edges.size();
    }

    @SuppressWarnings("unchecked")
    private static int vertexCount(Map<String, Object> result, int subset) {
        List<Map<String, Object>> subsets = (List<Map<String, Object>>) result.get("subsets");
        List<?> vertices = (List<?>) subsets.get(subset).get("vertices");
        return vertices.size();
    }

    @SuppressWarnings("unchecked")
    private static Object firstEdgeId(Map<String, Object> result, int subset) {
        List<Map<String, Object>> subsets = (List<Map<String, Object>>) result.get("subsets");
        List<Map<String, Object>> edges = (List<Map<String, Object>>) subsets.get(subset).get("edges");
        return edges.get(0).get("id");
    }

    private static final class Triangle {
        final Vertex a;
        final Vertex b;
        final Vertex c;
        final Edge ab;

        private Triangle(Vertex a, Vertex b, Vertex c, Edge ab) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.ab = ab;
        }

        static Triangle create(TinkerGraph graph) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            Vertex c = graph.addVertex(T.id, 3);
            Edge ab = a.addEdge("link", b);
            b.addEdge("link", c);
            a.addEdge("link", c);
            return new Triangle(a, b, c, ab);
        }
    }
}
