package com.graph.rocks.reader;

import com.graph.rocks.community.CommunityEdge;
import com.graph.rocks.community.CommunityGraph;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EdgePropertyCsvReader {
    private final CommunityGraph graph;

    /**
     * Constructor for EdgePropertyCsvReader
     * @param graph The CommunityGraph instance to read edge properties into
     */
    public EdgePropertyCsvReader(CommunityGraph graph) {
        this.graph = graph;
    }

    /**
     * Reads edge properties from a CSV file and populates them into the graph
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
                .setHeader() // Use first row as header (replaces deprecated withHeader())
                .setSkipHeaderRecord(true) // Skip header row when processing records
                .build();

        try (FileReader reader = new FileReader(csvFile);
             CSVParser parser = new CSVParser(reader, csvFormat)) {

            for (CSVRecord record : parser) {
                processSingleEdge(record);
            }
        }
    }

    /**
     * Processes a single edge record from CSV
     * @param record Single CSV record containing edge properties
     */
    private void processSingleEdge(CSVRecord record) {
        // Validate required fields
        if (!record.isMapped("outVertex") || !record.isMapped("inVertex")) {
            throw new IllegalArgumentException("CSV must contain 'outVertex' and 'inVertex' fields");
        }

        // Parse vertex IDs
        long outVertexHandle, inVertexHandle;
        try {
            outVertexHandle = Long.parseLong(record.get("outVertex"));
            inVertexHandle = Long.parseLong(record.get("inVertex"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Vertex IDs must be integers", e);
        }

        // Get edge instance
        CommunityEdge edge = new CommunityEdge(graph, outVertexHandle << 32 | inVertexHandle);
        if (edge.id() == null) {
            throw new IllegalArgumentException("Edge does not exist: " + outVertexHandle + "->" + inVertexHandle);
        }

        // Process label (default to "edge")
        String label = record.isMapped("label") ? record.get("label") : "edge";

        // Collect properties
        List<Object> keyValues = new ArrayList<>();
        for (String header : record.getParser().getHeaderMap().keySet()) {
            if ("outVertex".equals(header) || "inVertex".equals(header) || "label".equals(header)) {
                continue;
            }

            String value = record.get(header);
            if (value != null && !value.isEmpty() && !Graph.Hidden.isHidden(header)) {
                keyValues.add(header);
                keyValues.add(convertValue(value));
            }
        }

        // Save properties
        edge.setData(label, keyValues.toArray());
        edge.putEdgeData(graph.handle(), edge.handle(), edge.serialize());
    }

    /**
     * Converts string value from CSV to appropriate data type
     * Same conversion logic as vertex property values
     * @param value String value from CSV cell
     * @return Converted value (Long, Double, Boolean, or String)
     */
    private Object convertValue(String value) {
        // Same conversion logic as vertex properties
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            if ("true".equalsIgnoreCase(value)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(value)) {
                return Boolean.FALSE;
            }
            return value;
        }
    }
}