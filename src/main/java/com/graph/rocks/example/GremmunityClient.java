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
 * Client implementation for interacting with CommunityGraph Gremlin Server
 * Demonstrates session-based communication, graph operations, and query execution
 * using Apache TinkerPop Gremlin Driver
 */
public class GremmunityClient {
    private static final Logger logger = LoggerFactory.getLogger(GremmunityClient.class);

    /**
     * Main entry point for CommunityGraph client application
     * Establishes session connection and executes test operations
     *
     * @param args Command line arguments (unused)
     */
    public static void main(String[] args) {
        logger.info("Connecting to CommunityGraph Server with Session mode...");

        // Build cluster configuration and establish connection
        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .serializer(new GraphSONMessageSerializerV3())
                .maxWaitForConnection(10000)
                .create();

        // Create unique session ID and session-based client
        String sessionId = UUID.randomUUID().toString();
        Client client = cluster.connect(sessionId);

        logger.info("Connected with session: {}", sessionId);

        try {
            // Initialize graph session and traversal source
            initializeSession(client);

            // Execute comprehensive graph operation tests
            runTests(client);
        } catch (Exception e) {
            logger.error("Error during graph operations", e);
            e.printStackTrace(System.err);
        } finally {
            // Clean up resources
            client.close();
            cluster.close();
            logger.info("Connection closed");
        }
    }

    /**
     * Initialize CommunityGraph session with traversal source
     * Creates 'g' traversal source for graph operations
     *
     * @param client Session-based Gremlin client
     * @throws Exception If session initialization fails
     */
    private static void initializeSession(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Initializing CommunityGraph Session");
        logger.info("=".repeat(60));

        // Initialize traversal source for the session
        ResultSet results = client.submit(
                "g = graph.traversal(); " +
                        "'CommunityGraph session initialized'"
        );
        results.all().get();

        logger.info("Session initialized with CommunityGraph and 'g' traversal source");
        logger.info("=".repeat(60));
    }

    /**
     * Execute comprehensive graph operation tests
     * Demonstrates CRUD operations, queries, and traversals on CommunityGraph
     *
     * @param client Session-based Gremlin client
     * @throws Exception If any graph operation fails
     */
    private static void runTests(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Running CommunityGraph Tests");
        logger.info("=".repeat(60));

        // Test 1: Clear existing graph data
        logger.info("\nTest 1: Clearing graph data");
        ResultSet results = client.submit("g.V().drop().iterate(); 'cleared'");
        List<Result> resultList = results.all().get();
        logger.info("Graph cleared: {}", resultList);

        // Test 2: Create vertices with properties
        logger.info("\nTest 2: Creating vertices");
        results = client.submit(
                "g.addV('user').property('name', 'Alice').property('age', 28).next(); " +
                        "'Alice vertex created'"
        );
        resultList = results.all().get();
        logger.info("Created Alice vertex: {}", resultList);

        results = client.submit(
                "g.addV('user').property('name', 'Bob').property('age', 30).next(); " +
                        "'Bob vertex created'"
        );
        resultList = results.all().get();
        logger.info("Created Bob vertex: {}", resultList);

        results = client.submit(
                "g.addV('product').property('name', '手机').property('price', 2999).next(); " +
                        "'Phone vertex created'"
        );
        resultList = results.all().get();
        logger.info("Created Phone vertex: {}", resultList);

        // Test 3: Query vertices by label
        logger.info("\nTest 3: Querying user vertices");
        results = client.submit("g.V().hasLabel('user').valueMap('name', 'age').toList()");
        resultList = results.all().get();
        logger.info("All user vertices: {}", resultList);

        // Test 4: Create edges between vertices
        logger.info("\nTest 4: Creating edges");
        results = client.submit(
                "alice = g.V().has('name', 'Alice').next(); " +
                        "bob = g.V().has('name', 'Bob').next(); " +
                        "phone = g.V().has('name', '手机').next(); " +
                        "g.addE('friend').from(alice).to(bob).property('since', 2020).next(); " +
                        "g.addE('buy').from(alice).to(phone).property('time', 2025).next(); " +
                        "'Edges created between vertices'"
        );
        resultList = results.all().get();
        logger.info("Created edges: {}", resultList);

        // Test 5: Execute graph traversal queries
        logger.info("\nTest 5: Executing traversal queries");
        results = client.submit(
                "g.V().has('name', 'Alice').out('friend').values('name').toList()"
        );
        resultList = results.all().get();
        logger.info("Alice's friends: {}", resultList);

        results = client.submit(
                "g.V().has('name', 'Alice').out('buy').valueMap('name', 'price').toList()"
        );
        resultList = results.all().get();
        logger.info("Alice's purchases: {}", resultList);

        // Test 6: Execute count aggregation queries
        logger.info("\nTest 6: Executing count queries");
        results = client.submit("g.V().count().next()");
        resultList = results.all().get();
        logger.info("Total vertices in graph: {}", resultList);

        results = client.submit("g.E().count().next()");
        resultList = results.all().get();
        logger.info("Total edges in graph: {}", resultList);

        // Test 7: Update vertex properties
        logger.info("\nTest 7: Updating vertex properties");
        results = client.submit(
                "g.V().has('name', 'Alice').property('age', 29).next(); " +
                        "g.V().has('name', 'Alice').values('age').next()"
        );
        resultList = results.all().get();
        logger.info("Alice's updated age: {}", resultList);

        // Test completion
        logger.info("\n{}", "=".repeat(60));
        logger.info("All CommunityGraph operations completed successfully!");
        logger.info("=".repeat(60));
    }
}