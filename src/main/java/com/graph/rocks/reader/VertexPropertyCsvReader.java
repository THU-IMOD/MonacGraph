package com.graph.rocks.reader;

import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.community.CommunityVertex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VertexPropertyCsvReader {
    private final CommunityGraph graph;

    /**
     * Constructor for VertexPropertyCsvReader
     * @param graph The CommunityGraph instance to read vertex properties into
     */
    public VertexPropertyCsvReader(CommunityGraph graph) {
        this.graph = graph;
    }

    /**
     * Reads vertex properties from a CSV file and populates them into the graph
     * @param fileName Name of the CSV file (with or without .csv extension)
     * @throws IOException If file access or parsing fails
     */
    public void readProperties(String fileName) throws IOException {
        // 1. Validate data directory
        String dataDirPath = "data";
        File dataDir = new File(dataDirPath);
        if (!dataDir.exists()) {
            throw new IOException("Data directory does not exist: " + dataDir.getAbsolutePath());
        }
        if (!dataDir.isDirectory()) {
            throw new IllegalArgumentException("Data path is not a directory: " + dataDir.getAbsolutePath());
        }

        // 2. Build CSV file path
        String csvFileName = fileName.endsWith(".csv") ? fileName : fileName + ".csv";
        File csvFile = new File(dataDir, csvFileName);

        // 3. Validate CSV file
        if (!csvFile.exists()) {
            throw new IOException("CSV file does not exist: " + csvFile.getAbsolutePath());
        }
        if (!csvFile.isFile()) {
            throw new IllegalArgumentException("Path is not a file: " + csvFile.getAbsolutePath());
        }

        // 4. Read and parse CSV file (Using non-deprecated CSVFormat.Builder)
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader() // Use first row as header (replaces withHeader())
                .setSkipHeaderRecord(true) // Skip the header row when iterating records
                .build();

        try (FileReader reader = new FileReader(csvFile);
             CSVParser parser = new CSVParser(reader, csvFormat)) {

            for (CSVRecord record : parser) {
                processSingleVertex(record);
            }
        }
    }

    /**
     * Processes a single vertex record from CSV
     * @param record Single CSV record containing vertex properties
     */
    private void processSingleVertex(CSVRecord record) {
        // Validate required field
        if (!record.isMapped("vertex")) {
            throw new IllegalArgumentException("CSV must contain 'vertex' field");
        }

        // Parse vertex ID
        long vertexHandle;
        try {
            vertexHandle = Long.parseLong(record.get("vertex"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("'vertex' field must be an integer", e);
        }

        // Get vertex instance
        CommunityVertex vertex = new CommunityVertex(graph, vertexHandle);
        if (vertex.id() == null) {
            throw new IllegalArgumentException("Vertex does not exist: " + vertexHandle);
        }

        // Process label (default to "vertex")
        String label = record.isMapped("label") ? record.get("label") : "vertex";

        // Collect properties
        List<Object> keyValues = new ArrayList<>();
        for (String header : record.getParser().getHeaderMap().keySet()) {
            if ("vertex".equals(header) || "label".equals(header)) {
                continue;
            }

            String value = record.get(header);
            if (value != null && !value.isEmpty() && !Graph.Hidden.isHidden(header)) {
                keyValues.add(header);
                keyValues.add(convertValue(value));
            }
        }

        // Save properties
        vertex.setData(label, keyValues.toArray());
        vertex.putVertexData(graph.handle(), vertex.handle(), vertex.serialize());
    }

    /**
     * Converts string value from CSV to appropriate data type
     * @param value String value from CSV cell
     * @return Converted value (Long, Double, Boolean, or String)
     */
    private Object convertValue(String value) {
        // Try to convert to numeric type
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Try to convert to boolean
            if ("true".equalsIgnoreCase(value)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(value)) {
                return Boolean.FALSE;
            }
            // Otherwise return as string
            return value;
        }
    }
}