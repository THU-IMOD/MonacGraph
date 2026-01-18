package com.graph.rocks.example;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * Client - Preserve the creation of graph and g, do not delete existing vertices
 */
public class SecondOrderClient {
    private static final Logger logger = LoggerFactory.getLogger(SecondOrderClient.class);

    public static void main(String[] args) {
        logger.info("Connecting to CommunityGraph Server...");

        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .serializer(new GraphSONMessageSerializerV3())
                .maxWaitForConnection(10000)
                .create();

        String sessionId = UUID.randomUUID().toString();
        Client client = cluster.connect(sessionId);

        logger.info("Connected with session: {}", sessionId);

        try {
            // Initialize session: create graph and g
            initializeSecondOrderSession(client);

            // Initialize test data (do not delete existing vertices), create edges using variables
            initializeTestData(client);

            // Run tests
            runStandardTests(client);
            runSecondOrderTests(client);
            runVertexSubsetTests(client);

        } catch (Exception e) {
            logger.error("Error during graph operations", e);
            e.printStackTrace(System.err);
        } finally {
            client.close();
            cluster.close();
            logger.info("Connection closed");
        }
    }

    /**
     * Initialize Session - Create graph and g (Important: keep this part)
     */
    private static void initializeSecondOrderSession(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Initializing Session with SecondOrderTraversalSource");
        logger.info("=".repeat(60));

        // Manually create graph and g (Important: keep this part)
        ResultSet results = client.submit(
                "graph = CommunityGraph.open('new'); " +
                        "g = graph.traversal(SecondOrderTraversalSource.class); " +
                        "'Session initialized with SecondOrderTraversalSource'"
        );
        results.all().get();

        logger.info("✓ Graph initialized with SecondOrderTraversalSource");
        logger.info("✓ Available: g.secondOrder(), g.Vset(), and standard Gremlin methods");
        logger.info("=".repeat(60));
    }

    /**
     * Initialize Test Data - Do not delete existing vertices, create edges using variables
     */
    private static void initializeTestData(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Initializing Test Data");
        logger.info("=".repeat(60));

        // Do not delete existing data, directly create new vertices
        logger.info("Creating vertices...");
        client.submit(
                "alice = g.addV('person').property(T.id, 1).property('name', 'Alice').next(); " +
                        "bob = g.addV('person').property(T.id, 2).property('name', 'Bob').next(); " +
                        "charlie = g.addV('person').property(T.id, 3).property('name', 'Charlie').next(); " +
                        "david = g.addV('person').property(T.id, 4).property('name', 'David').next(); " +
                        "'Vertices created'"
        ).all().get();
        logger.info("✓ Created 4 vertices (alice, bob, charlie, david)");

        // Create edges using variables (corrected syntax)
        logger.info("Creating edges...");
        client.submit(
                "alice.addEdge('knows', bob); " +
                        "bob.addEdge('knows', charlie); " +
                        "charlie.addEdge('knows', alice); " +
                        "'Edges created'"
        ).all().get();
        logger.info("✓ Created edges: Alice->Bob, Bob->Charlie, Charlie->Alice");
        logger.info("Note: David is isolated (no edges)");

        logger.info("=".repeat(60));
    }

    /**
     * Standard Tests
     */
    private static void runStandardTests(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Standard Gremlin Tests");
        logger.info("=".repeat(60));

        // Test 1: Count vertices
        ResultSet results = client.submit("g.V().count().next()");
        List<Result> resultList = results.all().get();
        logger.info("[Test 1] Vertex count: {}", resultList.get(0).getObject());

        // Test 2: Count edges
        results = client.submit("g.E().count().next()");
        resultList = results.all().get();
        logger.info("[Test 2] Edge count: {}", resultList.get(0).getObject());

        // Test 3: Get all names
        results = client.submit("g.V().values('name').toList()");
        resultList = results.all().get();
        for (int i = 0; i < resultList.size(); i++)
            logger.info("[Test 3] All names: {}", resultList.get(i).getObject());

        // Test 4: Verify variables are still valid
        results = client.submit("alice.value('name')");
        resultList = results.all().get();
        logger.info("[Test 4] Alice variable still valid: {}", resultList.get(0).getObject());

        logger.info("=".repeat(60));
    }

    /**
     * Second-Order Logic Tests
     */
    private static void runSecondOrderTests(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Second-Order Logic Tests");
        logger.info("=".repeat(60));

        // Test 1: (∃x)(∃y) x knows y
        logger.info("\n[Test 1] Does there exist a person who knows someone?");
        logger.info("  Query: (∃x)(∃y) x knows y");

        ResultSet results = client.submit(
                "g.secondOrder()" +
                        ".exist('x')" +
                        ".exist('y')" +
                        ".filter('g.V(x).out(\"knows\").is(y)')" +
                        ".execute()"
        );
        List<Result> resultList = results.all().get();
        boolean result1 = (Boolean) resultList.get(0).getObject();

        logger.info("  Result: {}", result1);
        logger.info("  Explanation: {}", result1 ?
                "✓ There exists a person who knows someone (expected)" :
                "✗ No one knows anyone at all");

        // Test 2: (∃x)(∀y) y knows x OR y=x
        logger.info("\n[Test 2] Is there someone known by everyone?");
        logger.info("  Query: (∃x)(∀y) y knows x OR y=x");

        results = client.submit(
                "g.secondOrder()" +
                        ".exist('x')" +
                        ".forall('y')" +
                        ".filter('g.V(y).out(\"knows\").is(x) || g.V(y).is(x)')" +
                        ".execute()"
        );
        resultList = results.all().get();
        boolean result2 = (Boolean) resultList.get(0).getObject();

        logger.info("  Result: {}", result2);
        logger.info("  Explanation: {}", result2 ?
                "✓ Someone is universally known" :
                "✗ No one is universally known");

        // Test 3: (∀x)(∀y) x knows y → y knows x
        logger.info("\n[Test 3] Is 'knows' symmetric?");
        logger.info("  Query: (∀x)(∀y) if x knows y then y knows x");

        results = client.submit(
                "g.secondOrder()" +
                        ".forall('x')" +
                        ".forall('y')" +
                        ".filter('!(g.V(x).out(\"knows\").is(y)) || g.V(y).out(\"knows\").is(x)')" +
                        ".execute()"
        );
        resultList = results.all().get();
        boolean result3 = (Boolean) resultList.get(0).getObject();

        logger.info("  Result: {}", result3);
        logger.info("  Explanation: {}", result3 ?
                "✓ Friendship is mutual" :
                "✗ Some friendships are one-way");

        logger.info("\n" + "=".repeat(60));
    }

    /**
     * Vertex Subset Query Tests
     */
    private static void runVertexSubsetTests(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Vertex Subset Query Tests");
        logger.info("=".repeat(60));

        // Test 1: Find all internally connected subsets
        logger.info("\n[Test 1] Find subsets where (∀x)(∃y) x knows y");

        ResultSet results = client.submit(
                "result = g.Vset()" +
                        ".forall('x')" +
                        ".exist('y')" +
                        ".filter('g.V(x).out(\"knows\").is(y)')" +
                        ".execute(); " +
                        "result.size()"
        );
        List<Result> resultList = results.all().get();
        int subsetCount = ((Number) resultList.get(0).getObject()).intValue();
        logger.info("  Found {} valid subsets", subsetCount);

        // Get subset names (convert to human-readable format)
        results = client.submit(
                "result = g.Vset()" +
                        ".forall('x')" +
                        ".exist('y')" +
                        ".filter('g.V(x).out(\"knows\").is(y)')" +
                        ".execute(); " +
                        "result.collect { subset -> " +
                        "  subset.collect { v -> v.value('name') }.sort() " +
                        "}.toList()"
        );
        resultList = results.all().get();
        for (int i = 0; i < subsetCount; i++)
            logger.info("  Subsets: {}", resultList.get(i).getObject());

        // Test 2: Find cliques
        logger.info("\n[Test 2] Find cliques where (∀x)(∀y) x knows y OR x=y");

        results = client.submit(
                "result = g.Vset()" +
                        ".forall('x')" +
                        ".forall('y')" +
                        ".filter('g.V(x).out(\"knows\").is(y) || g.V(y).out(\"knows\").is(x) || g.V(x).is(y)')" +
                        ".execute(); " +
                        "result.size()"
        );
        resultList = results.all().get();
        subsetCount = ((Number) resultList.get(0).getObject()).intValue();
        logger.info("  Found {} valid subsets", subsetCount);

        results = client.submit(
                "result = g.Vset()" +
                        ".forall('x')" +
                        ".forall('y')" +
                        ".filter('g.V(x).out(\"knows\").is(y) || g.V(y).out(\"knows\").is(x) || g.V(x).is(y)')" +
                        ".execute(); " +
                        "result.collect { subset -> " +
                        "  subset.collect { v -> v.value('name') }.sort() " +
                        "}.toList()"
        );
        resultList = results.all().get();
        for (int i = 0; i < subsetCount; i++)
            logger.info("  All cliques: {}", resultList.get(i).getObject());

        logger.info("\n" + "=".repeat(60));
        logger.info("All Tests Completed!");
        logger.info("=".repeat(60));
    }
}