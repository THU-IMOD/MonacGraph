package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class PprQueryBuilderTest {
    @Test
    void directedCycleMatchesClosedFormAndWebTopK() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            Vertex c = graph.addVertex(T.id, 3);
            Vertex isolated = graph.addVertex(T.id, 4);
            a.addEdge("link", b);
            b.addEdge("link", c);
            c.addEdge("link", a);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            Map<Vertex, Double> scores = g.PPR(1).execute();
            double denominator = 1 + 0.85 + 0.85 * 0.85;
            assertEquals(1 / denominator, scores.get(a), 1e-9);
            assertEquals(0.85 / denominator, scores.get(b), 1e-9);
            assertEquals(0.85 * 0.85 / denominator, scores.get(c), 1e-9);
            assertEquals(0.0, scores.get(isolated));
            assertEquals(1.0, scores.values().stream().mapToDouble(Double::doubleValue).sum(), 1e-12);
            assertEquals(List.of(a, b), List.copyOf(g.PPR(1).topK(2).execute().keySet()));
            Map<?, ?> subset = (Map<?, ?>) ((List<?>) g.PPR(1).topK(2)
                    .executeForWeb().get("subsets")).get(0);
            assertEquals(2, ((List<?>) subset.get("vertices")).size());
            assertEquals(1, ((List<?>) subset.get("edges")).size());
            assertFalse(a.property("pprScore").isPresent());
        }
    }

    @Test
    void danglingMassReturnsToSeedAndIsolatedSeedKeepsAllMass() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            Vertex isolated = graph.addVertex(T.id, 3);
            a.addEdge("link", b);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            Map<Vertex, Double> scores = g.PPR(1).execute();
            assertEquals(1 / 1.85, scores.get(a), 1e-9);
            assertEquals(0.85 / 1.85, scores.get(b), 1e-9);
            assertEquals(Map.of(isolated, 1.0), g.PPR(3).topK(1).execute());
        }
    }

    @Test
    void multipleSeedsAreUniformAndDuplicatesDoNotChangeWeights() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            a.addEdge("link", b);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            Map<Vertex, Double> scores = g.PPR(List.of(1, 1, 2)).reset(1).execute();
            assertEquals(Map.of(a, 0.5, b, 0.5), scores);
            assertEquals(scores, g.PPR(1, 2).reset(1).execute());
        }
    }

    @Test
    void parallelEdgesAndSelfLoopsContributeTransitionProbability() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            a.addEdge("loop", a);
            a.addEdge("first", b);
            a.addEdge("second", b);
            b.addEdge("loop", b);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            Map<Vertex, Double> scores = g.PPR(1).reset(0.3).execute();
            assertEquals(0.3 / (1 - 0.7 / 3), scores.get(a), 1e-9);
            assertEquals(1 - scores.get(a), scores.get(b), 1e-9);
        }
    }

    @Test
    void validatesParametersAndSupportsEmptyTopK() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);
            SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);
            assertTrue(g.PPR(1).topK(0).execute().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> g.PPR());
            assertThrows(NullPointerException.class, () -> g.PPR((Object) null));
            assertThrows(NoSuchElementException.class, () -> g.PPR(99).execute());
            assertThrows(IllegalArgumentException.class, () -> g.PPR(1).reset(0));
            assertThrows(IllegalArgumentException.class, () -> g.PPR(1).reset(Double.NaN));
            assertThrows(IllegalArgumentException.class, () -> g.PPR(1).reset(1.1));
            assertThrows(IllegalArgumentException.class, () -> g.PPR(1).topK(-1));
        }
    }
}
