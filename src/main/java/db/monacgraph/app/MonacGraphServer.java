package db.monacgraph.app;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Enhanced Gremlin Server with File Upload Support
 * Manages both Gremlin Server (WebSocket/HTTP on 8182) and File Upload Server (HTTP on 8284)
 */
public class MonacGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(MonacGraphServer.class);

    private GremlinServer gremlinServer;
    private Server uploadServer;

    private final int uploadPort;

    public MonacGraphServer() { this(Integer.getInteger("monac.upload.port", 8284)); }
    public MonacGraphServer(int uploadPort) { this.uploadPort = uploadPort; }
    public int uploadPort() { return ((ServerConnector) uploadServer.getConnectors()[0]).getLocalPort(); }
    private static final String DATA_DIR = "data";

    /**
     * Start both Gremlin Server and File Upload Server
     */
    public void start(String configFile) throws Exception {
        logger.info("=".repeat(60));
        logger.info("Starting MonacGraph Server");
        logger.info("Config: {}", configFile);
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("=".repeat(60));

        // Prepare required directories
        prepareConfigFiles();

        // Start Gremlin Server (WebSocket/HTTP on 8182)
        startGremlinServer(configFile);

        // Start File Upload Server (HTTP on 8080)
        startUploadServer();

        logger.info("=".repeat(60));
        logger.info("  MonacGraph Server Started!");
        logger.info("  Gremlin API: ws://localhost:8182/gremlin");
        logger.info("  Upload API:  http://localhost:{}/api/graph/upload", uploadPort);
        logger.info("  Health Check: http://localhost:{}/api/health", uploadPort);
        logger.info("Press Ctrl+C to stop");
        logger.info("=".repeat(60));
    }

    /**
     * Start Gremlin Server
     */
    private void startGremlinServer(String configFile) throws Exception {
        logger.info("\n[1/2] Starting Gremlin Server...");

        InputStream input = Files.isRegularFile(Paths.get(configFile))
                ? Files.newInputStream(Paths.get(configFile))
                : getClass().getClassLoader().getResourceAsStream(configFile);
        if (input == null) {
            throw new IllegalArgumentException("Config file not found: " + configFile);
        }
        Settings settings;
        try (input) { settings = Settings.read(input); }

        logger.info("Gremlin Server Configuration:");
        logger.info("  Host: {}", settings.host);
        logger.info("  Port: {}", settings.port);
        logger.info("  Graphs: {}", settings.graphs.keySet());

        gremlinServer = new GremlinServer(settings);
        gremlinServer.start().join();

        logger.info("  Gremlin Server started on port {}", settings.port);


    }

    /**
     * Start embedded Jetty server for file upload
     */
    private void startUploadServer() throws Exception {
        logger.info("\n[2/2] Starting File Upload Server...");

        uploadServer = new Server();
        ServerConnector connector = new ServerConnector(uploadServer);
        connector.setPort(uploadPort);
        connector.setHost(System.getProperty("monac.upload.host"));
        uploadServer.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        uploadServer.setHandler(context);

        // Health check endpoint
        context.addServlet(new ServletHolder(new HealthCheckServlet()), "/api/health");

        // File upload endpoint with multipart config
        ServletHolder uploadHolder = new ServletHolder(new FileUploadServlet(this));  // 传递 this
        uploadHolder.getRegistration().setMultipartConfig(
                new MultipartConfigElement(
                        System.getProperty("java.io.tmpdir"),
                        100 * 1024 * 1024, // Max file size: 100MB
                        100 * 1024 * 1024, // Max request size: 100MB
                        10 * 1024 * 1024   // File size threshold: 10MB
                )
        );
        context.addServlet(uploadHolder, "/api/graph/upload");

        uploadServer.start();
        logger.info("  Upload Server started on port {}", uploadPort);
    }

    private db.monacgraph.community.CommunityGraph currentGraph() {
        return (db.monacgraph.community.CommunityGraph) gremlinServer.getServerGremlinExecutor()
                .getGraphManager().getGraph("graph");
    }

    private synchronized void executeGraphInitialization(String graphName, String vertexFile, String edgeFile)
            throws IOException {
        var graph = currentGraph();
        graph.importUploadedGraph(graphName);
        if (vertexFile != null) graph.loadVertexProperty(vertexFile);
        if (edgeFile != null) graph.loadEdgeProperty(edgeFile);
    }

    /**
     * Health Check Servlet
     */
    private static class HealthCheckServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            enableCORS(resp);

            resp.setContentType("application/json");
            resp.setStatus(HttpServletResponse.SC_OK);

            String json = "{\"status\":\"OK\",\"message\":\"Gremmunity Server is running\"}";
            resp.getWriter().write(json);
        }

        @Override
        protected void doOptions(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            enableCORS(resp);
            resp.setStatus(HttpServletResponse.SC_OK);
        }
    }

    /**
     * File Upload Servlet
     */
    private static class FileUploadServlet extends HttpServlet {
        private final MonacGraphServer server;

        public FileUploadServlet(MonacGraphServer server) {
            this.server = server;
        }

        @Override
        protected synchronized void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            enableCORS(resp);
            resp.setContentType("application/json");

            try {
                // 1. Get graph name
                String graphName = req.getParameter("graphName");
                if (graphName == null || graphName.trim().isEmpty()) {
                    sendError(resp, "Graph name is required");
                    return;
                }

                // Validate graph name
                if (!graphName.matches("^[a-zA-Z][a-zA-Z0-9_-]*$")) {
                    sendError(resp, "Invalid graph name. Must start with letter and contain only letters, numbers, underscore, hyphen");
                    return;
                }

                logger.info("Processing upload for graph: {}", graphName);

                Path dataPath = server.currentGraph().dataDirectory();
                Files.createDirectories(dataPath);

                // 3. Process graph file (required)
                String graphFileName = null;
                Part graphFilePart = req.getPart("graphFile");
                if (graphFilePart != null && graphFilePart.getSize() > 0) {
                    graphFileName = getFileName(graphFilePart);
                    if (graphFileName != null) {
                        if (!graphFileName.endsWith(".graph")) {
                            sendError(resp, "Graph file must be .graph");
                            return;
                        }
                        Path graphFilePath = dataPath.resolve(graphName + ".graph");
                        saveFile(graphFilePart, graphFilePath);
                        logger.info("  Saved vertex properties: {}", graphFilePath);
                    }
                }

                // 4. Process vertex properties file (optional)
                String vertexFileName = null;
                Part vertexFilePart = req.getPart("vertexFile");
                if (vertexFilePart != null && vertexFilePart.getSize() > 0) {
                    vertexFileName = getFileName(vertexFilePart);
                    if (vertexFileName != null) {
                        if (!Paths.get(vertexFileName).getFileName().toString().equals(vertexFileName))
                            throw new IOException("Invalid property filename");
                        if (!vertexFileName.endsWith(".json") && !vertexFileName.endsWith(".csv")) {
                            sendError(resp, "Vertex properties file must be .json or .csv");
                            return;
                        }
                        Path vertexFilePath = dataPath.resolve(vertexFileName);
                        saveFile(vertexFilePart, vertexFilePath);
                        logger.info("  Saved vertex properties: {}", vertexFilePath);
                    }
                }

                // 5. Process edge properties file (optional)
                String edgeFileName = null;
                Part edgeFilePart = req.getPart("edgeFile");
                if (edgeFilePart != null && edgeFilePart.getSize() > 0) {
                    edgeFileName = getFileName(edgeFilePart);
                    if (edgeFileName != null) {
                        if (!Paths.get(edgeFileName).getFileName().toString().equals(edgeFileName))
                            throw new IOException("Invalid property filename");
                        if (!edgeFileName.endsWith(".json") && !edgeFileName.endsWith(".csv")) {
                            sendError(resp, "Edge properties file must be .json or .csv");
                            return;
                        }
                        Path edgeFilePath = dataPath.resolve(edgeFileName);
                        saveFile(edgeFilePart, edgeFilePath);
                        logger.info("  Saved edge properties: {}", edgeFilePath);
                    }
                }

                if (graphFileName == null) {
                    sendError(resp, "Graph file is required");
                    return;
                }
                // Activation must succeed before acknowledging success.
                server.executeGraphInitialization(graphName, vertexFileName, edgeFileName);

                // 6. Build success response
                Map<String, Object> data = new HashMap<>();
                data.put("graphName", graphName);
                data.put("graphFile", graphFileName);
                data.put("vertexFile", vertexFileName);
                data.put("edgeFile", edgeFileName);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "Graph files uploaded successfully");
                response.put("data", data);

                String jsonResponse = toJson(response);
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().write(jsonResponse);

                logger.info("  Upload completed successfully for graph: {}", graphName);

                // Persist graph selection explicitly in the selected properties file for restart;
                // never overwrite a shared initialization script from an upload request.

            } catch (Exception e) {
                logger.error("Upload failed", e);
                sendError(resp, "Failed to upload files: " + e.getMessage());
            }
        }

        @Override
        protected void doOptions(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            enableCORS(resp);
            resp.setStatus(HttpServletResponse.SC_OK);
        }

        /**
         * Save uploaded file to specified path (overwrites if exists)
         * Smart strategy with retry and file comparison
         *
         * Strategy:
         * 1. Save to temp file (no conflict)
         * 2. Close InputStream and wait for lock release
         * 3. Try to delete target file with retry
         * 4. If delete fails, check if files are identical
         * 5. If identical, skip (no need to update)
         * 6. If different, throw error for manual intervention
         * 7. Rename temp to target (atomic)
         */
        private void saveFile(Part filePart, Path targetPath) throws IOException {
            Path tempFile = targetPath.resolveSibling(targetPath.getFileName() + ".tmp");

            try {
                // Step 1: Save uploaded content to temporary file
                try (InputStream input = filePart.getInputStream()) {
                    Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
                    logger.debug("  Saved to temporary file: {}", tempFile);
                }
                // InputStream is now closed

                // Step 2: Wait for OS to release locks
                try {
                    Thread.sleep(200); // Increased to 200ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // Step 3: Try to delete target file with retry
                if (Files.exists(targetPath)) {
                    boolean deleted = tryDeleteWithRetry(targetPath, 3);

                    if (!deleted) {
                        // Delete failed, check if files are identical
                        logger.warn("Could not delete target file (still locked). Checking if update is needed...");

                        if (filesAreIdentical(tempFile, targetPath)) {
                            // Files are identical, no need to update
                            logger.info("  File already up-to-date, skipping update: {}", targetPath);
                            Files.delete(tempFile); // Clean up temp file
                            return; // Success - no update needed
                        } else {
                            // Files are different but can't delete - this is a real problem
                            Files.delete(tempFile); // Clean up
                            throw new IOException("Target file is locked and content is different. " +
                                    "Please close the graph and try again: " + targetPath);
                        }
                    }
                    logger.debug("  Deleted existing target file: {}", targetPath);
                }

                // Step 4: Rename temporary file to target file
                logger.debug("  Renaming {} to {}", tempFile, targetPath);
                Files.move(tempFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("  File saved successfully: {}", targetPath);

            } catch (IOException e) {
                // Clean up temporary file
                logger.error("Failed to save file to {}: {}", targetPath, e.getMessage());
                try {
                    if (Files.exists(tempFile)) {
                        Files.delete(tempFile);
                        logger.debug("Cleaned up temporary file: {}", tempFile);
                    }
                } catch (IOException cleanupError) {
                    logger.warn("Failed to clean up temporary file: {}", tempFile, cleanupError);
                }
                throw e;
            }
        }

        /**
         * Try to delete a file with retry and exponential backoff
         */
        private boolean tryDeleteWithRetry(Path path, int maxRetries) {
            for (int i = 0; i < maxRetries; i++) {
                try {
                    Files.delete(path);
                    return true;
                } catch (IOException e) {
                    logger.debug("Delete attempt {} failed: {}", i + 1, e.getMessage());
                    if (i < maxRetries - 1) {
                        try {
                            Thread.sleep(200 * (i + 1)); // Exponential backoff: 200ms, 400ms, 600ms
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                }
            }
            return false;
        }

        /**
         * Check if two files have identical content
         */
        private boolean filesAreIdentical(Path file1, Path file2) throws IOException {
            if (!Files.exists(file1) || !Files.exists(file2)) {
                return false;
            }

            // Quick check: compare file sizes
            if (Files.size(file1) != Files.size(file2)) {
                return false;
            }

            // Compare content byte by byte
            try (InputStream is1 = Files.newInputStream(file1);
                 InputStream is2 = Files.newInputStream(file2)) {

                byte[] buffer1 = new byte[8192];
                byte[] buffer2 = new byte[8192];

                int read1, read2;
                while ((read1 = is1.read(buffer1)) != -1) {
                    read2 = is2.read(buffer2);
                    if (read1 != read2) {
                        return false;
                    }
                    for (int i = 0; i < read1; i++) {
                        if (buffer1[i] != buffer2[i]) {
                            return false;
                        }
                    }
                }

                return is2.read() == -1; // Ensure both files ended at the same time
            }
        }

        /**
         * Get original filename from Part
         */
        private String getFileName(Part part) {
            String contentDisposition = part.getHeader("content-disposition");
            if (contentDisposition != null) {
                for (String token : contentDisposition.split(";")) {
                    if (token.trim().startsWith("filename")) {
                        String filename = token.substring(token.indexOf('=') + 1).trim()
                                .replace("\"", "");
                        return filename;
                    }
                }
            }
            return null;
        }

        /**
         * Send error response
         */
        private void sendError(HttpServletResponse resp, String message) throws IOException {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", message);

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write(toJson(error));
        }

        /**
         * Simple JSON serialization (for small objects)
         */
        private String toJson(Map<String, Object> map) {
            StringBuilder json = new StringBuilder("{");
            boolean first = true;

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) json.append(",");
                first = false;

                json.append("\"").append(entry.getKey()).append("\":");

                Object value = entry.getValue();
                if (value == null) {
                    json.append("null");
                } else if (value instanceof String) {
                    json.append("\"").append(escapeJson((String) value)).append("\"");
                } else if (value instanceof Boolean) {
                    json.append(value);
                } else if (value instanceof Map) {
                    json.append(toJson((Map<String, Object>) value));
                } else {
                    json.append("\"").append(value.toString()).append("\"");
                }
            }

            json.append("}");
            return json.toString();
        }

        /**
         * Escape JSON string
         */
        private String escapeJson(String s) {
            return s.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }

    }

    /**
     * Enable CORS for all responses
     */
    private static void enableCORS(HttpServletResponse resp) {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
        resp.setHeader("Access-Control-Max-Age", "3600");
    }

    /**
     * Prepare required configuration directories
     */
    public void prepareConfigFiles() throws Exception {
        Path confDir = Paths.get("conf");
        Path scriptsDir = Paths.get("scripts");
        Path dataDir = Paths.get(DATA_DIR);

        Files.createDirectories(confDir);
        Files.createDirectories(scriptsDir);
        Files.createDirectories(dataDir);

        logger.info("Configuration directories prepared:");
        logger.info("  conf/    - Server configuration");
        logger.info("  scripts/ - Initialization scripts");
        logger.info("  data/    - Graph data files");
    }

    /**
     * Stop both servers gracefully
     */
    public void stop() {
        logger.info("\nStopping Gremmunity Server...");

        // Stop upload server
        if (uploadServer != null) {
            try {
                uploadServer.stop();
                logger.info("  Upload Server stopped");
            } catch (Exception e) {
                logger.error("Error stopping upload server", e);
            }
        }

        // Stop Gremlin server
        if (gremlinServer != null) {
            gremlinServer.stop().join();
            logger.info("  Gremlin Server stopped");
        }

        logger.info("MonacGraph Server stopped");
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        String configFile = "gremlin-server.yaml";
        if (args.length > 0) {
            configFile = args[0];
        }

        final MonacGraphServer server = new MonacGraphServer();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("\nReceived shutdown signal");
                server.stop();
            } catch (Exception e) {
                logger.error("Error stopping server", e);
            }
        }));

        try {
            server.start(configFile);
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
