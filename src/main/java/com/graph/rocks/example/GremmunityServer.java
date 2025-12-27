package com.graph.rocks.example;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Gremlin Server implementation for CommunityGraph (RocksDB-backed graph database)
 * Manages server lifecycle, configuration loading, and resource preparation
 * Compliant with Apache TinkerPop Gremlin Server specifications
 */
@SuppressWarnings("unused")
public class GremmunityServer {
    private static final Logger logger = LoggerFactory.getLogger(GremmunityServer.class);
    private GremlinServer server;

    /**
     * Start the CommunityGraph Gremlin Server with specified configuration file
     *
     * @param configFile Path to Gremlin Server YAML configuration file
     * @throws Exception If server initialization or startup fails
     */
    public void start(String configFile) throws Exception {
        logger.info("=".repeat(60));
        logger.info("Starting CommunityGraph Gremlin Server");
        logger.info("Config: {}", configFile);
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("=".repeat(60));

        // Prepare required configuration directories and files
        prepareConfigFiles();

        // Load server configuration from YAML resource
        InputStream input = getClass().getClassLoader().getResourceAsStream(configFile);
        if (input == null) {
            throw new IllegalArgumentException("Config file not found: " + configFile);
        }
        Settings settings = Settings.read(input);

        // Log core server configuration
        logger.info("Server Configuration:");
        logger.info("  Host: {}", settings.host);
        logger.info("  Port: {}", settings.port);
        logger.info("  Graphs: {}", settings.graphs.keySet());
        logger.info("  Script Engines: {}", settings.scriptEngines.keySet());

        // Initialize and start Gremlin Server
        server = new GremlinServer(settings);
        server.start().join();

        // Log successful startup information
        logger.info("=".repeat(60));
        logger.info("✓ CommunityGraph Gremlin Server Started!");
        logger.info("  TinkerPop: 3.8.0");
        logger.info("  Java: 17");
        logger.info("  Serialization: GraphSON v3 (JSON)");
        logger.info("  Endpoint: ws://{}:{}/gremlin", settings.host, settings.port);
        logger.info("  Variables: graph, g (from init script)");
        logger.info("Press Ctrl+C to stop");
        logger.info("=".repeat(60));
    }

    /**
     * Prepare required configuration directories and files for CommunityGraph
     * Creates conf/ and scripts/ directories if they don't exist
     *
     * @throws Exception If directory creation fails
     */
    public void prepareConfigFiles() throws Exception {
        // Create required configuration directories
        Path confDir = Paths.get("conf");
        Path scriptsDir = Paths.get("scripts");

        Files.createDirectories(confDir);
        Files.createDirectories(scriptsDir);

        logger.info("CommunityGraph configuration files prepared");
    }

    /**
     * Copy resource file from classpath to target filesystem location
     * Creates parent directories and empty file if resource not found
     *
     * @param resourcePath Classpath path to source resource
     * @param targetPath Filesystem path for target file
     * @throws Exception If file copy operation fails
     */
    private void copyResourceToFile(String resourcePath, String targetPath) throws Exception {
        InputStream input = getClass().getClassLoader().getResourceAsStream(resourcePath);

        // Create empty file if resource not found
        if (input == null) {
            logger.warn("Resource not found: {}, creating empty file", resourcePath);
            File targetFile = new File(targetPath);
            boolean mkdirResult = targetFile.getParentFile().mkdirs();
            boolean createResult = targetFile.createNewFile();
            if (!mkdirResult || !createResult) {
                System.out.println("Could not create directory: " + targetPath);
            }
            return;
        }

        // Copy resource to target file
        File targetFile = new File(targetPath);
        boolean targetCreateSuccess = targetFile.getParentFile().mkdirs();
        if (!targetCreateSuccess) {
            System.out.println("Could not create directory: " + targetPath);
        }

        try (FileOutputStream output = new FileOutputStream(targetFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        }

        logger.info("Copied: {} -> {}", resourcePath, targetFile.getAbsolutePath());
    }

    /**
     * Stop the CommunityGraph Gremlin Server gracefully
     *
     */
    public void stop() {
        if (server != null) {
            logger.info("Stopping CommunityGraph Gremlin Server...");
            server.stop().join();
            logger.info("CommunityGraph Gremlin Server stopped");
        }
    }

    /**
     * Main entry point for CommunityGraph Gremlin Server
     *
     * @param args Command line arguments (optional config file path)
     */
    public static void main(String[] args) {
        // Use default config file or override with command line argument
        String configFile = "gremlin-server.yaml";
        if (args.length > 0) {
            configFile = args[0];
        }

        final GremmunityServer lsmServer = new GremmunityServer();

        // Register shutdown hook for graceful server termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("\nReceived shutdown signal");
                lsmServer.stop();
            } catch (Exception e) {
                logger.error("Error stopping server", e);
            }
        }));

        try {
            // Start server and keep main thread alive
            lsmServer.start(configFile);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.info("Server interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}