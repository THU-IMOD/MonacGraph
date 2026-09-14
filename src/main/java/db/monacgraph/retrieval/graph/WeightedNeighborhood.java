package db.monacgraph.retrieval.graph;

import db.monacgraph.retrieval.hipporag.HippoRagModels.WeightedNeighbor;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;

public interface WeightedNeighborhood {
    List<WeightedNeighbor> neighbors(GraphElementRef vertex);

    static WeightedNeighborhood unweighted(GraphNeighborhood neighborhood) {
        return vertex -> neighborhood.neighbors(vertex).stream()
                .map(neighbor -> new WeightedNeighbor(neighbor, 1.0))
                .toList();
    }
}
