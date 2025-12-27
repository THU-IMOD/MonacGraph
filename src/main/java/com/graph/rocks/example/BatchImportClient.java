package com.graph.rocks.example;

import groovy.util.logging.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * High-performance batch import client for LSMCommunity
 * Implements bulk insertion of vertices and edges with configurable batch sizes
 * and import verification capabilities
 */
@Slf4j
public class BatchImportClient {
    private static final Logger logger = LoggerFactory.getLogger(BatchImportClient.class);

    // Configuration parameters for batch import
    private static final String CONTACT_POINT = "localhost";
    private static final int PORT = 8182;
    private static final int TOTAL_NODES = 10000;    // Total vertices to import
    private static final int TOTAL_EDGES = 20000;    // Total edges to import
    private static final int BATCH_SIZE = 1000;       // Batch size for bulk operations
    private static final String NODE_LABEL = "user";  // Vertex label for imported nodes
    private static final String EDGE_LABEL = "follows"; // Edge label for imported relationships

    /**
     * Main entry point for batch import process
     * Orchestrates connection setup, data import, and verification
     *
     * @param args Command line arguments (unused)
     */
    public static void main(String[] args) {
        logger.info("Starting batch import of {} nodes and {} edges", TOTAL_NODES, TOTAL_EDGES);

        // Build Gremlin Server cluster configuration
        Cluster cluster = Cluster.build()
                .addContactPoint(CONTACT_POINT)
                .port(PORT)
                .serializer(new GraphSONMessageSerializerV3())
                .maxWaitForConnection(10000)
                .create();

        // Create session-based client with unique session ID
        String sessionId = UUID.randomUUID().toString();
        Client client = cluster.connect(sessionId);

        try {
            // Initialize graph session and clear existing data
            initializeSession(client);
            clearExistingData(client);

            // Execute batch import operations with timing metrics
            long nodeStartTime = System.currentTimeMillis();
            batchImportVertex(client);
            long nodeEndTime = System.currentTimeMillis();
            logger.info("Node import completed in {}s",
                    TimeUnit.MILLISECONDS.toSeconds(nodeEndTime - nodeStartTime));

            long edgeStartTime = System.currentTimeMillis();
            batchImportEdges(client);
            long edgeEndTime = System.currentTimeMillis();
            logger.info("Edge import completed in {}s",
                    TimeUnit.MILLISECONDS.toSeconds(edgeEndTime - edgeStartTime));

            // Verify import integrity and consistency
            verifyImport(client);

        } catch (Exception e) {
            logger.error("Batch import failed", e);
        } finally {
            // Clean up resources
            client.close();
            cluster.close();
            logger.info("Connection closed");
        }
    }

    /**
     * Initialize CommunityGraph session with traversal source
     *
     * @param client Session-based Gremlin client
     * @throws Exception If session initialization fails
     */
    private static void initializeSession(Client client) throws Exception {
        logger.info("Initializing graph session...");
        ResultSet results = client.submit(
                "graph = CommunityGraph.open('batch_database'); " +
                        "g = graph.traversal(); " +
                        "'Session initialized'"
        );
        results.all().get();
    }

    /**
     * Clear existing graph data to prepare for fresh import
     *
     * @param client Session-based Gremlin client
     * @throws Exception If data cleanup fails
     */
    private static void clearExistingData(Client client) throws Exception {
        logger.info("Clearing existing graph data...");
        ResultSet results = client.submit("g.V().drop().iterate(); g.E().drop().iterate(); 'Cleared'");
        results.all().get();
    }

    /**
     * Perform batch import of vertices with incremental IDs and properties
     * Processes vertices in configurable batches for memory efficiency
     *
     * @param client Session-based Gremlin client
     * @throws Exception If vertex import fails
     */
    private static void batchImportVertex(Client client) throws Exception {
        logger.info("Starting vertex import ({} total, batch size {})", TOTAL_NODES, BATCH_SIZE);

        int totalImported = 0;
        while (totalImported < TOTAL_NODES) {
            // Calculate batch boundaries
            int currentBatchSize = Math.min(BATCH_SIZE, TOTAL_NODES - totalImported);
            StringBuilder script = buildGremlinQuery(totalImported, currentBatchSize);

            // Execute batch import
            ResultSet results = client.submit(script.toString());
            List<Result> resultList = results.all().get();
            logger.info("Batch Import Vertex Result List: {}", resultList);

            // Update progress
            totalImported += currentBatchSize;

            // Log progress at 10-batch intervals or completion
            if (totalImported % (BATCH_SIZE * 10) == 0 || totalImported == TOTAL_NODES) {
                logger.info("Imported {} / {} vertices ({}%)",
                        totalImported, TOTAL_NODES, (totalImported * 100) / TOTAL_NODES);
            }
        }
    }

    private static StringBuilder buildGremlinQuery(int totalImported, int currentBatchSize) {
        int startId = totalImported + 1;
        int endId = totalImported + currentBatchSize;

        // Build batch import script
        StringBuilder script = new StringBuilder();
        for (int i = startId; i <= endId; i++) {
            script.append(String.format(
                    "g.addV('%s').property(T.id, %d).property('name', 'user_%d').property('age', %d).next(); ",
                    NODE_LABEL, i, i, 18 + (i % 60)  // Age range: 18-77
            ));
        }
        script.append("'Batch completed'");
        return script;
    }

    /**
     * Perform batch import of edges between randomly selected vertices
     * Generates non-self-referential edges with random weight properties
     *
     * @param client Session-based Gremlin client
     * @throws Exception If edge import fails
     */
    private static void batchImportEdges(Client client) throws Exception {
        logger.info("Starting edge import ({} total, batch size {})", TOTAL_EDGES, BATCH_SIZE);

        int totalImported = 0;
        while (totalImported < TOTAL_EDGES) {
            // Calculate batch size for current iteration
            int currentBatchSize = Math.min(BATCH_SIZE, TOTAL_EDGES - totalImported);

            // Build batch import script
            StringBuilder script = new StringBuilder();
            for (int i = 0; i < currentBatchSize; i++) {
                // Generate random but valid vertex IDs for edge endpoints
                int sourceId = 1 + (int)(Math.random() * TOTAL_NODES);
                int targetId = 1 + (int)(Math.random() * TOTAL_NODES);

                // Prevent self-loop edges
                if (sourceId == targetId) {
                    targetId = (targetId % TOTAL_NODES) + 1;
                }

                script.append(String.format(
                        "g.V(%d).addE('%s').to(__.V(%d)).property('weight', %.2f).next(); ",
                        sourceId, EDGE_LABEL, targetId, Math.random()
                ));
            }
            script.append("'Batch completed'");

            // Execute batch import
            ResultSet results = client.submit(script.toString());
            List<Result> resultList = results.all().get();
            logger.info("Batch Import Edge Result List: {}", resultList);

            // Update progress
            totalImported += currentBatchSize;

            // Log progress at 10-batch intervals or completion
            if (totalImported % (BATCH_SIZE * 10) == 0 || totalImported == TOTAL_EDGES) {
                logger.info("Imported {} / {} edges ({}%)",
                        totalImported, TOTAL_EDGES, (totalImported * 100) / TOTAL_EDGES);
            }
        }
    }

    /**
     * Verify integrity of imported data through count validation and random sampling
     *
     * @param client Session-based Gremlin client
     * @throws Exception If verification queries fail
     */
    private static void verifyImport(Client client) throws Exception {
        logger.info("\nVerifying import results...");

        // Validate total vertex count
        ResultSet nodeCountResult = client.submit("g.V().count().next()");
        long nodeCount = (Long) nodeCountResult.all().get().get(0).getObject();
        logger.info("Actual vertex count: {} (expected: {})", nodeCount, TOTAL_NODES);

        // Validate total edge count
        ResultSet edgeCountResult = client.submit("g.E().count().next()");
        long edgeCount = (Long) edgeCountResult.all().get().get(0).getObject();
        logger.info("Actual edge count: {} (expected: {})", edgeCount, TOTAL_EDGES);

        // Random sample verification of vertex properties
        int sampleId = 1 + (int)(Math.random() * TOTAL_NODES);
        ResultSet sampleResult = client.submit(
                String.format("g.V(%d).valueMap().next()", sampleId)
        );
        logger.info("Sample vertex {} data: {}", sampleId, sampleResult.all().get().get(0).getObject());
    }
}