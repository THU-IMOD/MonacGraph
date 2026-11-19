package db.monacgraph.so;

import db.monacgraph.serialize.VsetResultSerializer;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Query builder for random walk operations on a graph.
 */
public final class RandomWalkQueryBuilder {

    private final GraphTraversalSource g;
    private final Object id;

    private Integer walks;
    private Integer length;
    private double resetProbability = 0.0;

    public RandomWalkQueryBuilder(GraphTraversalSource g, Object id) {
        this.g = Objects.requireNonNull(
                g, "Traversal source must not be null");

        this.id = Objects.requireNonNull(
                id, "Start vertex ID must not be null");
    }

    /**
     * Sets the number of random walks.
     */
    public RandomWalkQueryBuilder walks(int walks) {
        if (walks <= 0) {
            throw new IllegalArgumentException(
                    "Walks must be positive");
        }

        this.walks = walks;
        return this;
    }

    /**
     * Sets the maximum number of steps per walk.
     */
    public RandomWalkQueryBuilder length(int length) {
        if (length < 0) {
            throw new IllegalArgumentException(
                    "Length must be non-negative");
        }

        this.length = length;
        return this;
    }

    /**
     * Sets the probability of restarting from the source vertex.
     */
    public RandomWalkQueryBuilder reset(double probability) {
        if (!Double.isFinite(probability)
                || probability < 0.0
                || probability > 1.0) {
            throw new IllegalArgumentException(
                    "Reset probability must be between 0 and 1");
        }

        this.resetProbability = probability;
        return this;
    }

    /**
     * Executes random walks and returns all distinct visited vertices.
     */
    public Set<Vertex> execute() {

        if (walks == null || length == null) {
            throw new IllegalStateException(
                    "Please set walks and length before execution");
        }

        // Retrieve the starting vertex.
        Vertex start = g.V(id).next();

        // Store visited vertices by ID to avoid duplicates.
        Map<Object, Vertex> visited = new LinkedHashMap<>();
        visited.put(start.id(), start);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // Each walk starts independently from the source vertex.
        for (int w = 0; w < walks; w++) {

            Vertex current = start;

            // Perform at most 'length' movement steps per walk.
            for (int step = 0; step < length; step++) {

                // Restart at the source vertex with the configured probability.
                if (random.nextDouble() < resetProbability) {
                    current = start;
                    visited.putIfAbsent(current.id(), current);
                    continue;
                }

                // Collect distinct neighboring vertices.
                Map<Object, Vertex> neighborMap = new LinkedHashMap<>();

                Iterator<Vertex> iterator =
                        current.vertices(Direction.BOTH);

                while (iterator.hasNext()) {
                    Vertex neighbor = iterator.next();

                    neighborMap.putIfAbsent(
                            neighbor.id(), neighbor);
                }

                // Stop the current walk if no neighbors are available.
                if (neighborMap.isEmpty()) {
                    break;
                }

                List<Vertex> neighbors =
                        new ArrayList<>(neighborMap.values());

                // Uniformly select one neighbor at random.
                int index = random.nextInt(neighbors.size());
                current = neighbors.get(index);

                visited.putIfAbsent(current.id(), current);
            }
        }

        return new LinkedHashSet<>(visited.values());
    }

    /**
     * Executes random walks and serializes the result for web visualization.
     */
    public Map<String, Object> executeForWeb() {
        return VsetResultSerializer.serialize(
                Collections.singleton(execute())
        );
    }
}
