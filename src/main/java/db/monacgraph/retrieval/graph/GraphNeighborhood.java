package db.monacgraph.retrieval.graph;

import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;

/** Undirected adjacency used by HippoRAG PPR. */
public interface GraphNeighborhood {
    List<GraphElementRef> neighbors(GraphElementRef vertex);
}
