
package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class RandomWalkQueryBuilderTest {

    @Test
    void walksAndLengthReturnVisitedVertices() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);

            a.addEdge("link", b);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.RandomWalk(1)
                            .walks(3)
                            .length(2)
                            .execute()
            );

            // Both vertices must be visited in this two-vertex graph.
            assertEquals(Set.of(1, 2), result);
        }
    }

    @Test
    void zeroLengthReturnsOnlyStartVertex() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);
            graph.addVertex(T.id, 2);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.RandomWalk(1)
                            .walks(10)
                            .length(0)
                            .execute()
            );

            assertEquals(Set.of(1), result);
        }
    }

    @Test
    void resetProbabilityOneAlwaysRestartsAtStart() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);
            a.addEdge("link", b);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.RandomWalk(1)
                            .walks(10)
                            .length(5)
                            .reset(1.0)
                            .execute()
            );

            assertEquals(Set.of(1), result);
        }
    }

    @Test
    void isolatedVertexReturnsOnlyStartVertex() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Set<Object> result = ids(
                    g.RandomWalk(1)
                            .walks(5)
                            .length(10)
                            .execute()
            );

            assertEquals(Set.of(1), result);
        }
    }

    @Test
    void executeForWebReturnsInducedSubgraph() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            Vertex a = graph.addVertex(T.id, 1);
            Vertex b = graph.addVertex(T.id, 2);

            a.addEdge("link", b);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            Map<String, Object> web = g.RandomWalk(1)
                    .walks(1)
                    .length(1)
                    .executeForWeb();

            assertEquals("VsetResult", web.get("type"));

            List<?> subsets = (List<?>) web.get("subsets");
            assertEquals(1, subsets.size());

            Map<?, ?> subset = (Map<?, ?>) subsets.get(0);

            assertEquals(2, ((List<?>) subset.get("vertices")).size());
            assertEquals(1, ((List<?>) subset.get("edges")).size());
        }
    }

    @Test
    void invalidParametersAreRejected() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            RandomWalkQueryBuilder query = g.RandomWalk(1);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> query.walks(0)
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> query.length(-1)
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> query.reset(-0.1)
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> query.reset(1.1)
            );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> query.reset(Double.NaN)
            );
        }
    }

    @Test
    void executionRequiresWalksAndLength() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            assertThrows(
                    IllegalStateException.class,
                    () -> g.RandomWalk(1).execute()
            );
        }
    }

    @Test
    void missingStartVertexThrowsException() {
        try (TinkerGraph graph = TinkerGraph.open()) {
            graph.addVertex(T.id, 1);

            SecondOrderTraversalSource g =
                    graph.traversal(SecondOrderTraversalSource.class);

            assertThrows(
                    NoSuchElementException.class,
                    () -> g.RandomWalk(99)
                            .walks(1)
                            .length(1)
                            .execute()
            );
        }
    }
    // Extract vertex IDs for result comparison.
    private static Set<Object> ids(Set<Vertex> vertices) {
        return vertices.stream()
                .map(Vertex::id)
                .collect(Collectors.toSet());
    }
}

