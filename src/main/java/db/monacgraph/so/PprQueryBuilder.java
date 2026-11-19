package db.monacgraph.so;

import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Directed, unweighted personalized PageRank using power iteration.
 * Each outgoing edge has equal transition probability (including parallel edges
 * and self-loops). Dangling mass returns to the uniform distribution over the
 * distinct seeds. Top-K scores retain their original probability values.
 */
public final class PprQueryBuilder {
    private static final double ERROR_TOLERANCE = 1e-10;
    private static final int MAX_ITERATIONS = 10000;
    private final GraphTraversalSource g;
    private final List<Object> seedIds;
    private double resetProbability = 0.15;
    private int topK = 50;

    public PprQueryBuilder(GraphTraversalSource g, Collection<?> seedIds) {
        this.g = Objects.requireNonNull(g, "Traversal source must not be null");
        Objects.requireNonNull(seedIds, "Seed IDs must not be null");
        if (seedIds.isEmpty()) {
            throw new IllegalArgumentException("At least one seed is required");
        }
        this.seedIds = new ArrayList<>();
        for (Object id : seedIds) {
            this.seedIds.add(Objects.requireNonNull(id, "Seed ID must not be null"));
        }
    }

    public PprQueryBuilder reset(double probability) {
        if (!Double.isFinite(probability) || probability <= 0 || probability > 1) {
            throw new IllegalArgumentException("Reset probability must be finite and in (0, 1]");
        }
        resetProbability = probability;
        return this;
    }

    public PprQueryBuilder topK(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("topK must be non-negative");
        }
        topK = count;
        return this;
    }

    /** Returns up to K vertices with scores in descending order, including zero scores. */
    public Map<Vertex, Double> execute() {
        Set<Object> resolvedSeeds = new LinkedHashSet<>();
        for (Object id : seedIds) {
            resolvedSeeds.add(g.V(id).next().id());
        }
        List<Vertex> vertices = new ArrayList<>(g.V().toList());
        vertices.sort(Comparator.comparing(vertex -> vertex.id().getClass().getName()
                + ":" + vertex.id()));
        Map<Object, Integer> indexById = new HashMap<>();
        for (int i = 0; i < vertices.size(); i++) {
            indexById.put(vertices.get(i).id(), i);
        }
        double[] personalization = new double[vertices.size()];
        for (Object id : resolvedSeeds) {
            personalization[indexById.get(id)] = 1.0 / resolvedSeeds.size();
        }
        List<int[]> adjacency = new ArrayList<>();
        for (Vertex vertex : vertices) {
            List<Integer> destinations = new ArrayList<>();
            Iterator<Edge> edges = vertex.edges(Direction.OUT);
            while (edges.hasNext()) {
                destinations.add(indexById.get(edges.next().inVertex().id()));
            }
            adjacency.add(destinations.stream().mapToInt(Integer::intValue).toArray());
        }
        double[] scores = iterate(adjacency, personalization);
        List<Integer> ranking = new ArrayList<>();
        for (int i = 0; i < vertices.size(); i++) {
            ranking.add(i);
        }
        // Stable sorting uses the vertex-ID order above to resolve equal scores.
        ranking.sort(Comparator.comparingDouble((Integer i) -> scores[i]).reversed());
        Map<Vertex, Double> result = new LinkedHashMap<>();
        for (int i = 0; i < Math.min(topK, ranking.size()); i++) {
            int index = ranking.get(i);
            result.put(vertices.get(index), scores[index]);
        }
        return result;
    }

    private double[] iterate(List<int[]> adjacency, double[] personalization) {
        double[] scores = personalization.clone();
        double damping = 1.0 - resetProbability;
        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            double[] next = new double[scores.length];
            double restartMass = resetProbability;
            for (int source = 0; source < scores.length; source++) {
                int[] destinations = adjacency.get(source);
                if (destinations.length == 0) {
                    restartMass += damping * scores[source];
                } else {
                    double share = damping * scores[source] / destinations.length;
                    for (int destination : destinations) {
                        next[destination] += share;
                    }
                }
            }
            double difference = 0;
            for (int i = 0; i < scores.length; i++) {
                next[i] += restartMass * personalization[i];
                difference += Math.abs(next[i] - scores[i]);
            }
            scores = next;
            // The reset probability bounds the remaining L1 error by difference / reset.
            if (difference <= ERROR_TOLERANCE * resetProbability) {
                return scores;
            }
        }
        throw new IllegalStateException("PPR did not converge within " + MAX_ITERATIONS
                + " iterations; increase reset probability");
    }

    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serializeSubgraphs(
                List.of(ResultSubgraph.induced(execute().keySet())));
    }
}
