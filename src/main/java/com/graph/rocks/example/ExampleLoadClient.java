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
public class ExampleLoadClient {
    private static final Logger logger = LoggerFactory.getLogger(ExampleLoadClient.class);

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

            // Additional operations from Test.java
            runTestOperations(client);
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
                "graph = CommunityGraph.open('example'); " +
                        "g = graph.traversal(); " +
                        "'Example CommunityGraph session initialized'"
        );
        results.all().get();

        // Load properties from Test.java
        client.submit("graph.loadVertexProperty('exampleVertexProperty.json')").all().get();
        client.submit("graph.loadEdgeProperty('exampleEdgeProperty.csv')").all().get();

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
        logger.info("Starting to read example.graph data");
        logger.info("=".repeat(60));

        // 1. Query total number of vertices
        logger.info("\nQuery 1: Total number of vertices");
        ResultSet results = client.submit("g.V().count().next()");
        List<Result> resultList = results.all().get();
        logger.info("Total number of vertices: {}", resultList.get(0).getObject());

        // 2. Query total number of edges
        logger.info("\nQuery 2: Total number of edges");
        results = client.submit("g.E().count().next()");
        resultList = results.all().get();
        logger.info("Total number of edges: {}", resultList.get(0).getObject());

        logger.info("\n{}", "=".repeat(60));
        logger.info("example.graph data reading completed");
        logger.info("=".repeat(60));
    }

    /**
     * Additional operations from Test.java adapted for client-server model
     */
    private static void runTestOperations(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Executing additional test operations");
        logger.info("=".repeat(60));

        // Get vertex values for IDs 0-12
        logger.info("\nVertex values (ID 0-12):");
        for (int i = 0; i < 13; i++) {
            ResultSet results = client.submit("g.V(" + i + ").values('name').toList()");
            List<Result> resultList = results.all().get();
            logger.info("Vertex {} values: {}", i, resultList.get(0).getObject());
        }

        // Get edge details
        logger.info("\nEdge details:");
        ResultSet edgeResults = client.submit("g.E().values('details').toList()");
        List<Result> edgeDetails = edgeResults.all().get();
        for (Result detail : edgeDetails) {
            logger.info(detail.getObject().toString());
        }

        logger.info("\n{}", "=".repeat(60));
        logger.info("Additional test operations completed");
        logger.info("=".repeat(60));
    }
}