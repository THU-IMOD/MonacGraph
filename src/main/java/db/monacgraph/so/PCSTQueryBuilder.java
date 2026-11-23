package db.monacgraph.so;

import db.monacgraph.jni.PCSTJNI;
import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Prize-Collecting Steiner Tree Query Builder.
 *
 * Supports two ways to configure vertex prizes and edge costs:
 *
 * 1. Map-based configuration:
 *      .prize(Map)
 *      .cost(Map)
 *
 * 2. Graph-property-based configuration:
 *      .prize("prize")
 *      .cost("cost")
 *
 * Converts a Gremlin graph into PCSTFast input,
 * executes the native GW algorithm and maps
 * the result back to original graph elements.
 */
public final class PCSTQueryBuilder {

    private final GraphTraversalSource g;


    // ==========================================
    // 1. Weight configuration
    // ==========================================

    // Vertex ID -> Prize
    private Map<Object, Double> vertexPrizes =
            Collections.emptyMap();

    // Edge ID -> Cost
    private Map<Object, Double> edgeCosts =
            Collections.emptyMap();


    // Vertex property key
    private String vertexPrizePropertyKey;

    // Edge property key
    private String edgeCostPropertyKey;


    private boolean prizeConfigured = false;


    public PCSTQueryBuilder(GraphTraversalSource g) {

        this.g = Objects.requireNonNull(
                g,
                "GraphTraversalSource must not be null"
        );
    }


    // ==========================================
    // 2. Configure vertex prizes using Map
    // ==========================================

    public PCSTQueryBuilder prize(
            Map<?, ? extends Number> prizes) {

        this.vertexPrizes = normalizeWeights(
                prizes,
                "Vertex prize"
        );

        // Clear property-based configuration
        this.vertexPrizePropertyKey = null;

        this.prizeConfigured = true;

        return this;
    }


    // ==========================================
    // 3. Configure vertex prizes using property
    // ==========================================

    public PCSTQueryBuilder prize(String propertyKey) {

        this.vertexPrizePropertyKey = validatePropertyKey(
                propertyKey,
                "Vertex prize"
        );

        // Clear Map-based configuration
        this.vertexPrizes = Collections.emptyMap();

        this.prizeConfigured = true;

        return this;
    }


    // ==========================================
    // 4. Configure edge costs using Map
    // ==========================================

    public PCSTQueryBuilder cost(
            Map<?, ? extends Number> costs) {

        this.edgeCosts = normalizeWeights(
                costs,
                "Edge cost"
        );

        // Clear property-based configuration
        this.edgeCostPropertyKey = null;

        return this;
    }


    // ==========================================
    // 5. Configure edge costs using property
    // ==========================================

    public PCSTQueryBuilder cost(String propertyKey) {

        this.edgeCostPropertyKey = validatePropertyKey(
                propertyKey,
                "Edge cost"
        );

        // Clear Map-based configuration
        this.edgeCosts = Collections.emptyMap();

        return this;
    }


    // ==========================================
    // 6. Validate property key
    // ==========================================

    private static String validatePropertyKey(
            String key,
            String name) {

        if (key == null || key.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    name + " property key must not be null or empty."
            );
        }

        return key;
    }


    // ==========================================
    // 7. Validate and normalize Map weights
    // ==========================================

    private static Map<Object, Double> normalizeWeights(
            Map<?, ? extends Number> values,
            String name) {

        if (values == null) {

            throw new IllegalArgumentException(
                    name + " map must not be null."
            );
        }

        Map<Object, Double> result =
                new LinkedHashMap<>();

        for (Map.Entry<?, ? extends Number> entry
                : values.entrySet()) {

            Object id = entry.getKey();

            Number number = entry.getValue();

            if (id == null || number == null) {

                throw new IllegalArgumentException(
                        name + " ID and value must not be null."
                );
            }

            double value = number.doubleValue();

            validateWeight(value, name, id);

            result.put(id, value);
        }

        return result;
    }


    // ==========================================
    // 8. Validate numeric weight
    // ==========================================

    private static void validateWeight(
            double value,
            String name,
            Object elementId) {

        if (!Double.isFinite(value) || value < 0) {

            throw new IllegalArgumentException(
                    name +
                    " must be finite and non-negative. ID: " +
                    elementId
            );
        }
    }


    // ==========================================
    // 9. Read numeric property from graph element
    // ==========================================

    private static double readNumericProperty(
            Element element,
            String propertyKey,
            String description) {

        Property<?> property = element.property(propertyKey);

        // Check whether property exists
        if (property == null || !property.isPresent()) {

            throw new IllegalArgumentException(
                    "Missing " + description +
                    " property '" + propertyKey +
                    "' on element ID: " + element.id()
            );
        }

        Object value = property.value();

        // Property must be numeric
        if (!(value instanceof Number)) {

            throw new IllegalArgumentException(
                    description +
                    " property '" + propertyKey +
                    "' must be numeric. Element ID: " +
                    element.id()
            );
        }

        double weight = ((Number) value).doubleValue();

        validateWeight(
                weight,
                description,
                element.id()
        );

        return weight;
    }


    // ==========================================
    // 10. Execute PCST
    // ==========================================

    public PCSTResult execute() {

        if (!prizeConfigured) {

            throw new IllegalStateException(
                    "Please configure vertex prizes using prize()."
            );
        }

        if (!PCSTJNI.isAvailable()) {

            throw new IllegalStateException(
                    "PCST native library is unavailable."
            );
        }


        // --------------------------------------
        // Step 1. Extract graph vertices and edges
        // --------------------------------------

        List<Vertex> vertices = g.V().toList();

        List<Edge> edges = g.E().toList();


        if (vertices.isEmpty()) {

            throw new IllegalStateException(
                    "Cannot execute PCST on an empty graph."
            );
        }


        // --------------------------------------
        // Step 2. Build vertex ID -> index mapping
        // --------------------------------------

        Map<Object, Integer> vertexIndex =
                new HashMap<>();

        for (int i = 0; i < vertices.size(); i++) {

            Object id = vertices.get(i).id();

            if (vertexIndex.put(id, i) != null) {

                throw new IllegalStateException(
                        "Duplicate vertex ID: " + id
                );
            }
        }


        // Validate prize IDs in Map mode
        if (vertexPrizePropertyKey == null) {

            for (Object id : vertexPrizes.keySet()) {

                if (!vertexIndex.containsKey(id)) {

                    throw new IllegalArgumentException(
                            "Unknown vertex ID in prize map: " + id
                    );
                }
            }
        }


        // --------------------------------------
        // Step 3. Build edge ID -> index mapping
        // --------------------------------------

        Map<Object, Integer> edgeIndex =
                new HashMap<>();

        for (int i = 0; i < edges.size(); i++) {

            Object id = edges.get(i).id();

            if (edgeIndex.put(id, i) != null) {

                throw new IllegalStateException(
                        "Duplicate edge ID: " + id
                );
            }
        }


        // Validate edge cost IDs in Map mode
        if (edgeCostPropertyKey == null) {

            for (Object id : edgeCosts.keySet()) {

                if (!edgeIndex.containsKey(id)) {

                    throw new IllegalArgumentException(
                            "Unknown edge ID in cost map: " + id
                    );
                }
            }
        }


        // --------------------------------------
        // Step 4. Prepare PCST arrays
        // --------------------------------------

        int vertexCount = vertices.size();

        int edgeCount = edges.size();

        int[] edgeU = new int[edgeCount];

        int[] edgeV = new int[edgeCount];

        double[] prizes = new double[vertexCount];

        double[] costs = new double[edgeCount];


        // --------------------------------------
        // Step 5. Fill vertex prizes
        // --------------------------------------

        for (int i = 0; i < vertexCount; i++) {

            Vertex vertex = vertices.get(i);

            if (vertexPrizePropertyKey != null) {

                // Property-based mode
                prizes[i] = readNumericProperty(
                        vertex,
                        vertexPrizePropertyKey,
                        "Vertex prize"
                );

            } else {

                // Map-based mode
                prizes[i] = vertexPrizes.getOrDefault(
                        vertex.id(),
                        0.0
                );
            }
        }


        // --------------------------------------
        // Step 6. Fill edges and edge costs
        // --------------------------------------

        for (int i = 0; i < edgeCount; i++) {

            Edge edge = edges.get(i);

            Object sourceId = edge.outVertex().id();

            Object targetId = edge.inVertex().id();

            Integer u = vertexIndex.get(sourceId);

            Integer v = vertexIndex.get(targetId);


            if (u == null || v == null) {

                throw new IllegalStateException(
                        "Edge endpoint not found: " + edge.id()
                );
            }


            edgeU[i] = u;

            edgeV[i] = v;


            if (edgeCostPropertyKey != null) {

                // Property-based mode
                costs[i] = readNumericProperty(
                        edge,
                        edgeCostPropertyKey,
                        "Edge cost"
                );

            } else {

                // Map-based mode
                costs[i] = edgeCosts.getOrDefault(
                        edge.id(),
                        1.0
                );
            }
        }


        // --------------------------------------
        // Step 7. Call native PCST
        // --------------------------------------

        PCSTJNI jni = new PCSTJNI();

        int[][] rawResult = jni.solve(
                vertexCount,
                edgeU,
                edgeV,
                prizes,
                costs
        );


        // --------------------------------------
        // Step 8. Validate native result
        // --------------------------------------

        if (rawResult == null ||
                rawResult.length != 2 ||
                rawResult[0] == null ||
                rawResult[1] == null) {

            throw new IllegalStateException(
                    "Invalid PCST native result."
            );
        }


        int[] selectedNodeIndices = rawResult[0];

        int[] selectedEdgeIndices = rawResult[1];


        // --------------------------------------
        // Step 9. Map selected vertices back
        // --------------------------------------

        List<Vertex> selectedVertices =
                new ArrayList<>();

        boolean[] selectedFlags =
                new boolean[vertexCount];

        double totalPrize = 0.0;


        for (int index : selectedNodeIndices) {

            if (index < 0 || index >= vertexCount) {

                throw new IllegalStateException(
                        "Invalid selected vertex index: " + index
                );
            }

            if (selectedFlags[index]) {

                throw new IllegalStateException(
                        "Duplicate selected vertex index: " + index
                );
            }

            selectedFlags[index] = true;

            selectedVertices.add(vertices.get(index));

            totalPrize += prizes[index];
        }


        // --------------------------------------
        // Step 10. Map selected edges back
        // --------------------------------------

        List<Edge> selectedEdges =
                new ArrayList<>();

        double totalCost = 0.0;


        for (int index : selectedEdgeIndices) {

            if (index < 0 || index >= edgeCount) {

                throw new IllegalStateException(
                        "Invalid selected edge index: " + index
                );
            }

            int u = edgeU[index];

            int v = edgeV[index];

            if (!selectedFlags[u] || !selectedFlags[v]) {

                throw new IllegalStateException(
                        "Selected edge has an unselected endpoint."
                );
            }

            selectedEdges.add(edges.get(index));

            totalCost += costs[index];
        }


        // --------------------------------------
        // Step 11. Build PCSTResult
        // --------------------------------------

        return new PCSTResult(
                selectedVertices,
                selectedEdges,
                totalPrize,
                totalCost
        );
    }


    // ==========================================
    // 12. Serialize for web visualization
    // ==========================================

    public Map<String, Object> executeForWeb() {

        PCSTResult result = execute();

        ResultSubgraph subgraph = ResultSubgraph.of(
                result.getVertices(),
                result.getEdges()
        );

        return VsetResultSerializer.serialize(subgraph);
    }
}
