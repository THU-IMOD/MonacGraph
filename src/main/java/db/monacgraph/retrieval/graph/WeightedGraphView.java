package db.monacgraph.retrieval.graph;

import db.monacgraph.retrieval.hipporag.HippoRagModels.WeightedNeighbor;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;
import java.util.Objects;

/** Exposes a weighted graph as an unweighted TinkerPop-style neighborhood. */
public final class WeightedGraphView implements GraphNeighborhood {
    private final WeightedNeighborhood neighborhood;

    public WeightedGraphView(WeightedNeighborhood neighborhood) {
        this.neighborhood = Objects.requireNonNull(neighborhood, "neighborhood");
    }

    @Override
    public List<GraphElementRef> neighbors(GraphElementRef vertex) {
        return neighborhood.neighbors(vertex).stream().map(WeightedNeighbor::vertex).toList();
    }
}
