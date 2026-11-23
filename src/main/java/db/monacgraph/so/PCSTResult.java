package db.monacgraph.so;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of Prize-Collecting Steiner Tree.
 *
 * Contains selected vertices, selected edges,
 * total prize, total cost and objective value.
 */
public final class PCSTResult {

    private final List<Vertex> vertices;
    private final List<Edge> edges;

    private final double totalPrize;
    private final double totalCost;
    private final double objective;


    public PCSTResult(
            List<Vertex> vertices,
            List<Edge> edges,
            double totalPrize,
            double totalCost) {

        this.vertices = Collections.unmodifiableList(
            new ArrayList<>(vertices)
        );

        this.edges = Collections.unmodifiableList(
            new ArrayList<>(edges)
        );

        this.totalPrize = totalPrize;
        this.totalCost = totalCost;
        this.objective = totalPrize - totalCost;
    }


    public List<Vertex> getVertices() {
        return vertices;
    }


    public List<Edge> getEdges() {
        return edges;
    }


    public double getTotalPrize() {
        return totalPrize;
    }


    public double getTotalCost() {
        return totalCost;
    }


    public double getObjective() {
        return objective;
    }


    @Override
    public String toString() {

        return "PCSTResult{" +
                "vertices=" + vertices.size() +
                ", edges=" + edges.size() +
                ", totalPrize=" + totalPrize +
                ", totalCost=" + totalCost +
                ", objective=" + objective +
                '}';
    }
}

