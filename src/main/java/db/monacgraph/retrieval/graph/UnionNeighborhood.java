package db.monacgraph.retrieval.graph;

import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class UnionNeighborhood implements GraphNeighborhood {
    private final List<GraphNeighborhood> delegates;

    public UnionNeighborhood(GraphNeighborhood... delegates) {
        this.delegates = List.of(delegates);
    }

    @Override
    public List<GraphElementRef> neighbors(GraphElementRef vertex) {
        Set<GraphElementRef> unique = new LinkedHashSet<>();
        for (GraphNeighborhood delegate : delegates) {
            unique.addAll(delegate.neighbors(vertex));
        }
        return new ArrayList<>(unique);
    }
}
