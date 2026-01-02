package com.graph.rocks.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.community.CommunityVertex;
import org.apache.tinkerpop.gremlin.structure.Graph;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * JSON property reader based on vertex handle, supports locating vertices via long-type handles and setting their properties
 */
public class VertexPropertyJsonReader {
    private final CommunityGraph graph;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for VertexPropertyJsonReader
     * @param graph Target CommunityGraph instance to operate on
     */
    public VertexPropertyJsonReader(CommunityGraph graph) {
        this.graph = graph;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Reads JSON file with specified name under the "data" directory (fails directly if the directory does not exist)
     * @param fileName Name of the JSON file (with/without .json suffix)
     * @throws IOException Thrown when file/directory does not exist, is unreadable, or JSON parsing fails
     * @throws IllegalArgumentException Thrown when file type is invalid or JSON structure is incorrect
     */
    public void readProperties(String fileName) throws IOException {
        System.out.println(fileName);
        // 1. Define data directory path (data folder under project root, can be changed to absolute path)
        String dataDirPath = "data";
        File dataDir = new File(dataDirPath);

        // Core check: fail fast if data directory does not exist
        if (!dataDir.exists()) {
            throw new IOException("Failed to read JSON: data directory does not exist. Path: " + dataDir.getAbsolutePath());
        }
        // Additional check: fail if path exists but is not a directory (e.g., a file with the same name)
        if (!dataDir.isDirectory()) {
            throw new IllegalArgumentException("data path exists but is not a directory. Path: " + dataDir.getAbsolutePath());
        }

        // 2. Assemble full JSON file path (automatically add .json suffix to avoid duplication)
        String jsonFileName = fileName.endsWith(".json") ? fileName : fileName + ".json";
        File jsonFile = new File(dataDir, jsonFileName); // Cross-platform path concatenation

        // 3. Validate JSON file validity
        if (!jsonFile.exists()) {
            throw new IOException("JSON file not found in data directory. File: " + jsonFile.getAbsolutePath());
        }
        if (!jsonFile.isFile()) {
            throw new IllegalArgumentException("Path is not a valid file (maybe a directory). Path: " + jsonFile.getAbsolutePath());
        }
        if (!jsonFile.canRead()) {
            throw new IOException("No read permission for JSON file. File: " + jsonFile.getAbsolutePath());
        }

        // 4. Read and parse JSON (retain original business logic)
        JsonNode rootNode = objectMapper.readTree(jsonFile);

        if (rootNode.isArray()) {
            processVertexArray(rootNode);
        } else if (rootNode.isObject()) {
            processSingleVertex(rootNode);
        } else {
            throw new IllegalArgumentException(
                    "JSON root must be an object or array of vertex properties. File: " + jsonFile.getAbsolutePath()
            );
        }
    }

    /**
     * Processes an array of vertex property objects from JSON
     * @param arrayNode JSON array node containing multiple vertex property objects
     */
    private void processVertexArray(JsonNode arrayNode) {
        for (JsonNode vertexNode : arrayNode) {
            processSingleVertex(vertexNode);
        }
    }

    /**
     * Processes a single vertex property object from JSON
     * @param vertexNode JSON object node containing vertex handle and properties
     */
    private void processSingleVertex(JsonNode vertexNode) {
        // 1. Check if vertex object contains "vertex" field (correct usage of has method)
        if (!vertexNode.has("vertex")) {
            throw new IllegalArgumentException("Vertex JSON must contain 'vertex' field (long handle)");
        }
        JsonNode vertexFieldNode = vertexNode.get("vertex"); // Get node of "vertex" field

        // 2. First validate it's a numeric type (compatible with IntNode/LongNode), then exclude decimals
        if (!vertexFieldNode.isNumber()) {
            throw new IllegalArgumentException("'vertex' field must be a number (long integer)");
        }
        if (vertexFieldNode.isFloatingPointNumber()) { // Exclude decimals (e.g., 1.5)
            throw new IllegalArgumentException("'vertex' field must be an integer (not a decimal)");
        }

        // 3. Safely convert to long (IntNode will be automatically converted to long without precision loss)
        long vertexHandle = vertexFieldNode.asLong();

        // Get vertex instance via handle
        CommunityVertex vertex = new CommunityVertex(graph, vertexHandle);
        if (vertex.id() == null) {
            throw new NoSuchElementException("Vertex with handle " + vertexHandle + " not found");
        }

        // Process label field (if exists)
        String newLabel = "vertex";
        if (vertexNode.has("label")) {
            newLabel = vertexNode.get("label").asText();
        }

        List<Object> keyValuesList = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = vertexNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();

            // Skip fields that have been specially processed
            if ("vertex".equals(key) || "label".equals(key)) {
                continue;
            }

            JsonNode valueNode = field.getValue();
            Object value = convertJsonNodeToObject(valueNode);

            // Validate property key-value pairs (comply with TinkerPop specifications)
            if (!Graph.Hidden.isHidden(key) && !key.isEmpty() && value != null) {
                keyValuesList.add(key);
                keyValuesList.add(value);
            }
        }

        // Save modifications to storage
        vertex.setData(newLabel, keyValuesList.toArray());
        vertex.putVertexData(graph.handle(), vertex.handle(), vertex.serialize());
    }

    /**
     * Converts JsonNode to corresponding Java object
     * @param node JsonNode to be converted
     * @return Converted Java object (Boolean/Number/String), or JSON string for other types
     */
    private Object convertJsonNodeToObject(JsonNode node) {
        if (node.isBoolean()) return node.booleanValue();
        if (node.isNumber()) return node.numberValue();
        if (node.isTextual()) return node.asText();
        return node.toString();
    }
}