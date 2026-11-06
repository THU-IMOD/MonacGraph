package db.monacgraph.community;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="bwgraph.integration", matches="true")
@EnabledIfEnvironmentVariable(named="BWGRAPH_STORAGE_BACKEND", matches="s3")
class CloudBackendTest {
    @TempDir Path root;
    @Test void restoresAllStateWithoutLocalCacheAndDoesNotReimport() throws Exception {
        String name="cloud_"+UUID.randomUUID().toString().replace("-", "");
        Path data=root.resolve("data"), stores=root.resolve("stores");Files.createDirectories(data);
        Files.writeString(data.resolve(name+".graph"),"t 2 1\nv 0 person 0\nv 1 person 0\ne 0 1\n");
        var config=new BaseConfiguration();
        config.setProperty("storage.backend","bw-graph");config.setProperty("bwgraph.storage.backend","s3");
        config.setProperty("storage.path",data.toString());config.setProperty("db.name",name);
        config.setProperty("bwgraph.storage.root",stores.toString());config.setProperty("bwgraph.import.on-create",true);
        try (var graph=CommunityGraph.open(config)) {
            graph.vertex(0).property("name","Alice");
            graph.addVertex(T.id,"uuid-like",T.label,"person","name","Carol");
        }
        Files.move(stores.resolve(name),root.resolve("old-local-cache"));
        try (var graph=CommunityGraph.open(config)) {
            assertEquals(3L,graph.traversal().V().count().next());
            assertEquals("Alice",graph.vertex(0).value("name"));
            assertEquals("Carol",graph.vertex("uuid-like").value("name"));
            assertEquals(1L,graph.traversal().V(1).in().count().next());
            graph.clear();
        }
        Files.move(stores.resolve(name),root.resolve("empty-local-cache"));
        try (var graph=CommunityGraph.open(config)) {
            assertEquals(0L,graph.traversal().V().count().next());
            assertTrue(Files.exists(data.resolve(name+".graph")));
        }
    }
}
