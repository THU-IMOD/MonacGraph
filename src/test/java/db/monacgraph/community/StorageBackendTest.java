package db.monacgraph.community;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="bwgraph.integration", matches="true")
class StorageBackendTest {
    @TempDir Path temporary;
    BaseConfiguration config(String backend, String name) {
        BaseConfiguration c = new BaseConfiguration();
        c.setProperty("storage.backend", backend); c.setProperty("db.name", name);
        c.setProperty("bwgraph.storage.root", temporary.resolve("stores").toString());
        c.setProperty("bwgraph.import.on-create", false);
        return c;
    }
    static String name() { return "test_backends_" + UUID.randomUUID().toString().replace("-", ""); }
    @ParameterizedTest @ValueSource(strings={"lsm-community", "bw-graph"})
    void crudTraversalAnalyticsAndReopen(String backend) throws Exception {
        String name = name(); var config = config(backend, name); UUID id = UUID.randomUUID();
        try (CommunityGraph graph = CommunityGraph.open(config)) {
            var g = graph.traversal();
            assertEquals(backend, graph.configuration().getString("storage.backend"));
            assertEquals(0L, g.V().count().next());
            var a = graph.addVertex(T.id,id,T.label,"person","name","Alice","age",20);
            var b = graph.addVertex(T.id,"b",T.label,"person","name","Bob");
            var c = graph.addVertex(T.id,"c",T.label,"person");
            a.addEdge("knows",b,T.id,"ab","weight",2);
            b.addEdge("knows",a,T.id,"ba"); b.addEdge("knows",c,T.id,"bc");
            assertEquals(3L,g.V().count().next()); assertEquals(3L,g.E().count().next());
            assertEquals("Bob",g.V(id).out("knows").values("name").next());
            assertEquals(id,g.V("b").in("knows").id().next());
            assertFalse(g.V("missing").hasNext()); assertFalse(g.E("missing").hasNext());
            a.property("age",21); assertEquals(21,graph.vertex(id).<Integer>value("age"));
            graph.edge("ab").property("weight",3); assertEquals(3,graph.edge("ab").<Integer>value("weight"));
            graph.vertex(id).property("age").remove(); assertFalse(graph.vertex(id).property("age").isPresent());
            assertEquals(3,g.BFS(id).execute().size());
            assertEquals(1,g.WCC().getCommunities().size());
            assertEquals(2,g.SCC().getCommunities().size());
            assertEquals(3,g.Community().getCommunities().stream().mapToInt(Set::size).sum());
            assertNotNull(g.BFS(id).executeForWeb());
            assertNotNull(g.Vset().forall("x").filter("true").executeForWeb());
            assertFalse(g.Subgraph().addV(0,"person").addV(1,"person").addE(0,1).forall("x").forall("y").filter("true").execute().isEmpty());
            graph.edge("bc").remove(); assertEquals(2L,g.E().count().next());
        }
        try (CommunityGraph graph = CommunityGraph.open(config)) {
            assertEquals(3L,graph.traversal().V().count().next());
            assertEquals(2L,graph.traversal().E().count().next());
            assertEquals("Alice",graph.vertex(id).value("name"));
            assertEquals(3,graph.edge("ab").<Integer>value("weight"));
        }
        Files.deleteIfExists(Path.of("data",name+".graph"));
    }
    @Test void subgraphCallbackExceptionPropagates() throws Exception {
        Path file=temporary.resolve("callback.graph");
        Files.writeString(file,"t 2 1\nv 0 0 1\nv 1 0 1\ne 0 1\n");
        var callback=new db.monacgraph.so.SubgraphPhiCallback(null,List.of(),new Vertex[0],"true") {
            @Override public boolean evaluate(int[] coords) { throw new IllegalStateException("callback-test"); }
        };
        var error=assertThrows(IllegalStateException.class,()->new db.monacgraph.jni.SubgraphJNI()
                .runSubgraphMatch(file.toString(),file.toString(),new int[]{0,0},callback));
        assertEquals("callback-test",error.getMessage());
    }
    @Test void bwColdStartDeleteSelfLoopClearAndIsolation() throws Exception {
        String name=name(); var cfg=config("bw-graph",name);
        try (CommunityGraph graph=CommunityGraph.open(cfg);
             CommunityGraph second=CommunityGraph.open(cfg)) {
            var a=graph.addVertex(T.id,"zero"); var b=graph.addVertex(T.id,"one");
            a.addEdge("self",a,T.id,"loop"); a.addEdge("edge",b,T.id,"ab");
            assertEquals(2L,graph.traversal().V("zero").bothE().count().next());
            assertTrue(graph.traversal().V("zero").out("self").hasNext());
            assertTrue(graph.traversal().V("zero").both("self").hasNext());
            assertThrows(IllegalStateException.class,graph::clear);
            b.remove(); assertEquals(1L,second.traversal().V().count().next());
            assertEquals(1L,second.traversal().E().count().next());
        }
        try (CommunityGraph graph=CommunityGraph.open(cfg)) {
            graph.clear(); assertEquals(0L,graph.traversal().V().count().next());
            graph.addVertex(T.id,"again"); graph.reload(name);
            assertTrue(graph.isBwGraph()); assertEquals(1L,graph.traversal().V().count().next());
            try (CommunityGraph other=CommunityGraph.open(config("bw-graph",name+"_other"))) {
                assertEquals(0L,other.traversal().V().count().next());
                var x=other.addVertex(T.id,"x");
                assertThrows(IllegalArgumentException.class,()->graph.vertex("again").addEdge("bad",x));
            }
        }
    }
    @Test void bwImportPropertiesReloadAndNoReimportAfterClear() throws Exception {
        String name=name(); Path data=temporary.resolve("data");Files.createDirectories(data);
        Files.writeString(data.resolve(name+".graph"),"t 3 2\nv 0 person 1\nv 5 person 1\nv 9 person 0\ne 0 5\ne 5 9\n");
        Files.writeString(data.resolve("vertices.csv"),"vertex,label,name\n0,person,Alice\n5,person,Bob\n9,person,Carol\n");
        Files.writeString(data.resolve("edges.csv"),"outVertex,inVertex,label,weight\n0,5,knows,3\n5,9,knows,4\n");
        Files.writeString(data.resolve("vertices.json"),"[{\"vertex\":5,\"label\":\"person\",\"name\":\"Bobby\"}]");
        var cfg=config("bw-graph",name);cfg.setProperty("storage.path",data.toString());cfg.setProperty("bwgraph.import.on-create",true);
        try (CommunityGraph graph=CommunityGraph.open(cfg)) {
            graph.loadVertexProperty("vertices.csv");graph.loadEdgeProperty("edges.csv");
            assertEquals(3L,graph.traversal().V().count().next());
            assertEquals("Bob",graph.traversal().V(0).out("knows").values("name").next());
            assertEquals(3L,graph.traversal().V(0).outE().values("weight").next());
            graph.loadVertexProperty("vertices.json");
            assertEquals("Bobby",graph.vertex(5).value("name"));
            graph.reload(name); assertEquals(2L,graph.traversal().E().count().next());
            graph.clear(); graph.reload(name);
            assertEquals(0L,graph.traversal().V().count().next());
            assertTrue(Files.exists(data.resolve(name+".graph")));
        }
    }
    @Test void bwConcurrentRequestsAndDuplicateIds() throws Exception {
        try (CommunityGraph graph=CommunityGraph.open(config("bw-graph",name()))) {
            ExecutorService pool=Executors.newFixedThreadPool(4);
            try {
                List<Callable<Void>> work=new ArrayList<>();
                for(int t=0;t<4;t++){final int worker=t;work.add(()->{
                    for(int i=0;i<30;i++){
                        String id=worker+":"+i;graph.addVertex(T.id,id,"value",i);
                        assertEquals(i,graph.vertex(id).<Integer>value("value"));
                    }return null;
                });}
                for(var result:pool.invokeAll(work))result.get();
                assertEquals(120L,graph.traversal().V().count().next());
                assertThrows(IllegalArgumentException.class,()->graph.addVertex(T.id,"0:0"));
            }finally{pool.shutdownNow();}
        }
    }
    @Test void unknownBackendDoesNotOpenAnything() {
        assertThrows(IllegalArgumentException.class,()->CommunityGraph.open(config("typo",name())));
    }
}
