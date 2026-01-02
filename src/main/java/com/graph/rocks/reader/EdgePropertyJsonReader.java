package com.graph.rocks.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.community.CommunityEdge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * JSON property reader based on edge handle, supports locating edges via source/target vertex handles and setting their properties
 */
public class EdgePropertyJsonReader {
    private final CommunityGraph graph;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for EdgePropertyJsonReader
     * @param graph Target CommunityGraph instance to operate on
     */
    public EdgePropertyJsonReader(CommunityGraph graph) {
        this.graph = graph;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Reads JSON file with specified name under the "data" directory (fails fast if the directory does not exist)
     * @param fileName Name of the JSON file (with/without .json suffix)
     * @throws IOException Thrown when file/directory does not exist, is unreadable, or JSON parsing fails
     * @throws IllegalArgumentException Thrown when file type is invalid or JSON structure is incorrect
     */
    public void readProperties(String fileName) throws IOException {
        System.out.println(fileName);
        // 1. Define data directory path
        String dataDirPath = "data";
        File dataDir = new File(dataDirPath);

        // Validate data directory validity
        if (!dataDir.exists()) {
            throw new IOException("Failed to read JSON: data directory does not exist. Path: " + dataDir.getAbsolutePath());
        }
        if (!dataDir.isDirectory()) {
            throw new IllegalArgumentException("data path exists but is not a directory. Path: " + dataDir.getAbsolutePath());
        }

        // 2. Assemble full JSON file path
        String jsonFileName = fileName.endsWith(".json") ? fileName : fileName + ".json";
        File jsonFile = new File(dataDir, jsonFileName);

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

        // 4. Read and parse JSON
        JsonNode rootNode = objectMapper.readTree(jsonFile);

        if (rootNode.isArray()) {
            processEdgeArray(rootNode);
        } else if (rootNode.isObject()) {
            processSingleEdge(rootNode);
        } else {
            throw new IllegalArgumentException(
                    "JSON root must be an object or array of edge properties. File: " + jsonFile.getAbsolutePath()
            );
        }
    }

    /**
     * Processes an array of edge property objects from JSON
     * @param arrayNode JSON array node containing multiple edge property objects
     */
    private void processEdgeArray(JsonNode arrayNode) {
        for (JsonNode edgeNode : arrayNode) {
            processSingleEdge(edgeNode);
        }
    }

    /**
     * Processes a single edge property object from JSON
     * @param edgeNode JSON object node containing edge vertex handles and properties
     */
    private void processSingleEdge(JsonNode edgeNode) {
        // Validate presence of outVertex and inVertex fields
        if (!edgeNode.has("outVertex")) {
            throw new IllegalArgumentException("Edge JSON must contain 'outVertex' field (long handle)");
        }
        if (!edgeNode.has("inVertex")) {
            throw new IllegalArgumentException("Edge JSON must contain 'inVertex' field (long handle)");
        }

        // Parse outVertex handle
        JsonNode outVertexNode = edgeNode.get("outVertex");
        if (!outVertexNode.isNumber() || outVertexNode.isFloatingPointNumber()) {
            throw new IllegalArgumentException("'outVertex' field must be an integer (not a decimal)");
        }
        long outVertexHandle = outVertexNode.asLong();

        // Parse inVertex handle
        JsonNode inVertexNode = edgeNode.get("inVertex");
        if (!inVertexNode.isNumber() || inVertexNode.isFloatingPointNumber()) {
            throw new IllegalArgumentException("'inVertex' field must be an integer (not a decimal)");
        }
        long inVertexHandle = inVertexNode.asLong();

        // Get edge instance via source/target vertex handles (assuming CommunityEdge constructor exists)
        CommunityEdge edge = new CommunityEdge(graph, outVertexHandle << 32 | inVertexHandle);
        if (edge.id() == null) {
            throw new NoSuchElementException("Edge with outVertex " + outVertexHandle +
                    " and inVertex " + inVertexHandle + " not found");
        }

        // Process label field (default value is "edge")
        String newLabel = "edge";
        if (edgeNode.has("label")) {
            newLabel = edgeNode.get("label").asText();
        }

        // Collect property key-value pairs
        List<Object> keyValuesList = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = edgeNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();

            // Skip fields that have been specially processed
            if ("outVertex".equals(key) || "inVertex".equals(key) || "label".equals(key)) {
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

        // Save modifications to storage (assuming CommunityEdge has corresponding methods)
        edge.setData(newLabel, keyValuesList.toArray());
        edge.putEdgeData(graph.handle(), edge.handle(), edge.serialize());
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