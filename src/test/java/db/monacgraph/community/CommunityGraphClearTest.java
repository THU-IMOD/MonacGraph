package db.monacgraph.community;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommunityGraphClearTest {
    @Test
    void clearRemovesVerticesEdgesAndIdMappingsAcrossReload() throws Exception {
        String name = "test_clear_" + UUID.randomUUID().toString().replace("-", "");
        CommunityGraph graph = CommunityGraph.open(name);
        try {
            var g = graph.traversal();
            var alice = graph.addVertex(T.id, 1, "name", "Alice");
            var bob = graph.addVertex(T.id, 2, "name", "Bob");
            alice.addEdge("knows", bob);
            assertEquals(2L, g.V().count().next());
            assertEquals(1L, g.E().count().next());

            graph.clear();
            assertEquals(0L, g.V().count().next());
            assertEquals(0L, g.E().count().next());

            graph.addVertex(T.id, 1, "name", "New Alice");
            assertEquals("New Alice", g.V(1).values("name").next());
            graph.clear();
            graph.reload(name);
            assertEquals(0L, g.V().count().next());
            assertEquals(0L, g.E().count().next());
        } finally {
            graph.close();
            Files.deleteIfExists(Path.of("data", name + ".graph"));
        }
    }
}
