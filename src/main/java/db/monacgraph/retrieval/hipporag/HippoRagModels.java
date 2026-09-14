package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;
import java.util.Objects;

public final class HippoRagModels {
    private HippoRagModels() {}

    public static final double SYNONYM_THRESHOLD = 0.8;
    public static final double RECOGNITION_THRESHOLD = 0.9;

    public record PhraseNode(
            GraphElementRef vertex,
            String name,
            List<String> aliases) {
        public PhraseNode {
            Objects.requireNonNull(vertex, "vertex");
            Objects.requireNonNull(name, "name");
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }
    }

    public record FactTriple(
            GraphElementRef edge,
            GraphElementRef source,
            GraphElementRef target) {
        public FactTriple {
            Objects.requireNonNull(edge, "edge");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
        }
    }

    public record SynonymEdge(GraphElementRef left, GraphElementRef right, double score) {
        public SynonymEdge {
            Objects.requireNonNull(left, "left");
            Objects.requireNonNull(right, "right");
        }
    }

    public record WeightedNeighbor(GraphElementRef vertex, double weight) {
        public WeightedNeighbor {
            Objects.requireNonNull(vertex, "vertex");
            if (weight <= 0) {
                throw new IllegalArgumentException("weight must be positive");
            }
        }
    }
}
