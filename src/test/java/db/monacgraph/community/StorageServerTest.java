package db.monacgraph.community;

import db.monacgraph.app.MonacGraphServer;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="bwgraph.integration", matches="true")
class StorageServerTest {
    @TempDir Path temp;
    @ParameterizedTest @ValueSource(strings={"lsm-community", "bw-graph"})
    void remoteTraversalAndUploadRetainBackend(String backend) throws Exception {
        String name="test_server_"+UUID.randomUUID().toString().replace("-", "");
        int port; try (var socket=new ServerSocket(0)) { port=socket.getLocalPort(); }
        Path properties=temp.resolve("graph.properties");
        Files.writeString(properties,"gremlin.graph=db.monacgraph.community.CommunityGraph\n"
            +"storage.backend="+backend+"\ndb.name="+name+"\n"
            +"bwgraph.storage.root="+temp.resolve("stores")+"\nbwgraph.import.on-create=false\n");
        String yaml=Files.readString(Path.of("src/main/resources/gremlin-server-bw.yaml"))
            .replace("host: 0.0.0.0", "host: 127.0.0.1")
            .replace("port: 8182", "port: "+port)
            .replace("conf/bw-graph.properties",properties.toString());
        Path config=temp.resolve("server.yaml"); Files.writeString(config,yaml);
        var server=new MonacGraphServer(0);
        try {
            server.start(config.toString());
            var cluster=Cluster.build("127.0.0.1").port(port).create();
            var client=cluster.connect();
            try {
                assertEquals(backend,client.submit("graph.configuration().getString('storage.backend')").one().getString());
                assertEquals(2,client.submit("a=graph.addVertex(T.id,'a',T.label,'person'); b=graph.addVertex(T.id,'b',T.label,'person'); a.addEdge('knows',b,T.id,'ab'); g.V().count().next()").one().getInt());
                assertEquals("b",client.submit("g.V('a').out('knows').id().next()").one().getString());
                assertEquals(1,client.submit("g.WCC().getCommunities().size()").one().getInt());
                assertEquals(2,client.submit("g.BFS('a').execute().size()").one().getInt());
                assertNotNull(client.submit("g.Vset().forall('x').filter('true').executeForWeb()").one().getObject());
                var http=HttpClient.newHttpClient();
                URI base=URI.create("http://127.0.0.1:"+server.uploadPort());
                assertEquals(200,http.send(HttpRequest.newBuilder(base.resolve("/api/health")).build(),HttpResponse.BodyHandlers.ofString()).statusCode());
                String boundary="monacTestBoundary";
                for (int size : new int[]{2,3}) {
                    String graph="t "+size+" 1\nv 0 0 0\nv 1 0 0\n"+(size==3?"v 2 0 0\n":"")+"e 0 1\n";
                    String body="--"+boundary+"\r\nContent-Disposition: form-data; name=\"graphName\"\r\n\r\n"+name
                        +"\r\n--"+boundary+"\r\nContent-Disposition: form-data; name=\"graphFile\"; filename=\"test.graph\"\r\nContent-Type: text/plain\r\n\r\n"+graph+"\r\n--"+boundary+"--\r\n";
                    var result=http.send(HttpRequest.newBuilder(base.resolve("/api/graph/upload"))
                        .header("Content-Type","multipart/form-data; boundary="+boundary)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),HttpResponse.BodyHandlers.ofString());
                    assertEquals(200,result.statusCode(),result.body());
                    assertEquals(size,client.submit("g.V().count().next()").one().getInt());
                    assertEquals(1,client.submit("g.E().count().next()").one().getInt());
                    assertEquals(backend,client.submit("graph.configuration().getString('storage.backend')").one().getString());
                }
                assertEquals(0,client.submit("graph.clear(); g.V().count().next()").one().getInt());
                assertEquals(backend,client.submit("graph.configuration().getString('storage.backend')").one().getString());
            } finally { client.close(); cluster.close(); }
        } finally { server.stop(); Files.deleteIfExists(Path.of("data",name+".graph")); }
    }
}
