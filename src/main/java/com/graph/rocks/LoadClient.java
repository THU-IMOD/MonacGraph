package com.graph.rocks;

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
 * Client implementation for interacting with LsmGraph Gremlin Server
 * Demonstrates session-based communication, graph operations, and query execution
 * using Apache TinkerPop Gremlin Driver
 */
public class LoadClient {
    private static final Logger logger = LoggerFactory.getLogger(LoadClient.class);

    /**
     * Main entry point for LsmGraph client application
     * Establishes session connection and executes test operations
     *
     * @param args Command line arguments (unused)
     */
    public static void main(String[] args) {
        logger.info("Connecting to LsmGraph Server with Session mode...");

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
            e.printStackTrace();
        } finally {
            // Clean up resources
            client.close();
            cluster.close();
            logger.info("Connection closed");
        }
    }

    /**
     * Initialize LsmGraph session with traversal source
     * Creates 'g' traversal source for graph operations
     *
     * @param client Session-based Gremlin client
     * @throws Exception If session initialization fails
     */
    private static void initializeSession(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Initializing LsmGraph Session");
        logger.info("=".repeat(60));

        // Initialize traversal source for the session
        ResultSet results = client.submit(
                "graph = LsmGraph.open('example'); " +
                        "g = graph.traversal(); " +
                        "'LsmGraph session initialized'"
        );
        results.all().get();

        logger.info("Session initialized with LsmGraph and 'g' traversal source");
        logger.info("=".repeat(60));
    }

    /**
     * Execute comprehensive graph operation tests
     * Demonstrates CRUD operations, queries, and traversals on LsmGraph
     *
     * @param client Session-based Gremlin client
     * @throws Exception If any graph operation fails
     */
    private static void runTests(Client client) throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("开始读取example.graph数据");
        logger.info("=".repeat(60));

        // 1. 查询所有顶点数量
        logger.info("\n查询1: 总顶点数量");
        ResultSet results = client.submit("g.V().count().next()");
        List<Result> resultList = results.all().get();
        logger.info("顶点总数: {}", resultList);

        logger.info("\n查询6: 总边数量");
        results = client.submit("g.E().count().next()");
        resultList = results.all().get();
        logger.info("边总数: {}", resultList);

        // 读取操作完成
        logger.info("\n{}", "=".repeat(60));
        logger.info("example.graph数据读取完成");
        logger.info("=".repeat(60));
    }
}