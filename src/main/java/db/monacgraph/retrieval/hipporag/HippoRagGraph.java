package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.graph.WeightedNeighborhood;
import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.hipporag.HippoRagModels.SynonymEdge;
import db.monacgraph.retrieval.hipporag.HippoRagModels.WeightedNeighbor;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Undirected weighted graph: OpenIE/fact edges ∪ synonym edges. */
public final class HippoRagGraph implements WeightedNeighborhood {
    private final Map<Object, Map<Object, WeightedNeighbor>> adjacency = new HashMap<>();

    public HippoRagGraph(HippoRagIndex index) {
        this(index.facts(), index.synonyms());
    }

    public HippoRagGraph(List<FactTriple> facts, List<SynonymEdge> synonyms) {
        for (FactTriple fact : facts) {
            add(fact.source(), fact.target(), 1.0);
        }
        for (SynonymEdge synonym : synonyms) {
            add(synonym.left(), synonym.right(), synonym.score());
        }
    }

    @Override
    public List<WeightedNeighbor> neighbors(GraphElementRef vertex) {
        Map<Object, WeightedNeighbor> next = adjacency.get(vertex.id());
        return next == null ? List.of() : List.copyOf(next.values());
    }

    private void add(GraphElementRef left, GraphElementRef right, double weight) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        if (left.id().equals(right.id()) || weight <= 0) {
            return;
        }
        put(left, right, weight);
        put(right, left, weight);
    }

    private void put(GraphElementRef from, GraphElementRef to, double weight) {
        adjacency.computeIfAbsent(from.id(), key -> new HashMap<>())
                .merge(to.id(), new WeightedNeighbor(to, weight),
                        (previous, added) -> new WeightedNeighbor(to, previous.weight() + added.weight()));
    }
}
