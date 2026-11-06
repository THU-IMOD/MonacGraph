package db.monacgraph.reader;

import db.monacgraph.community.CommunityGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** One-time import of MonacGraph t/v/e text files into a new Bw-Graph store. */
public final class BwGraphImporter {
    private BwGraphImporter() { }
    public static void load(CommunityGraph graph, Path file) throws IOException {
        SortedMap<Long,String> vertices = new TreeMap<>();
        List<long[]> edges = new ArrayList<>();
        long expectedVertices = -1, expectedEdges = -1;
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim(); if (line.isEmpty() || line.startsWith("#")) continue;
                String[] fields = line.split("\\s+");
                switch (fields[0]) {
                    case "t":
                        if (fields.length < 3 || expectedVertices != -1) throw new IOException("Invalid graph header");
                        expectedVertices = Long.parseLong(fields[1]); expectedEdges = Long.parseLong(fields[2]);
                        if (expectedVertices < 0 || expectedEdges < 0) throw new IOException("Negative graph counts");
                        break;
                    case "v":
                        if (fields.length < 2) throw new IOException("Invalid vertex record");
                        long id = Long.parseLong(fields[1]);
                        if (id < 0 || id >= 0xffffffffL || vertices.putIfAbsent(id, fields.length > 2 ? fields[2] : "vertex") != null)
                            throw new IOException("Invalid or duplicate vertex ID: " + id);
                        break;
                    case "e":
                        if (fields.length < 3) throw new IOException("Invalid edge record");
                        edges.add(new long[]{Long.parseLong(fields[1]), Long.parseLong(fields[2])});
                        break;
                    default: throw new IOException("Unknown graph record: " + fields[0]);
                }
            }
        } catch (NumberFormatException e) { throw new IOException("Invalid numeric graph field", e); }
        if (expectedVertices != vertices.size() || expectedEdges != edges.size())
            throw new IOException("Graph header counts do not match records");
        Set<Long> unique = new HashSet<>();
        for (long[] edge : edges) {
            if (!vertices.containsKey(edge[0]) || !vertices.containsKey(edge[1]))
                throw new IOException("An edge endpoint does not exist");
            if (!unique.add((edge[0] << 32) | edge[1])) throw new IOException("Parallel edges are not supported");
        }
        Map<Long,Vertex> created = new HashMap<>();
        for (var entry : vertices.entrySet())
            created.put(entry.getKey(), graph.addVertex(T.id, entry.getKey(), T.label, entry.getValue()));
        for (long[] edge : edges)
            created.get(edge[0]).addEdge("edge", created.get(edge[1]), T.id, (edge[0] << 32) | edge[1]);
    }
}
