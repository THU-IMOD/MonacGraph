package db.monacgraph.retrieval.pipeline;

import db.monacgraph.retrieval.model.PipelineModels.AnchorCandidate;
import db.monacgraph.retrieval.model.PipelineModels.AnchorMention;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSubgraph;
import db.monacgraph.retrieval.model.PipelineModels.ExpandedVertex;
import db.monacgraph.retrieval.model.PipelineModels.ExpandedVertexSet;
import db.monacgraph.retrieval.model.PipelineModels.PathSet;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalValue;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.ScoredPath;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.RetrievalItemRef;

import java.util.LinkedHashSet;
import java.util.Set;

/** Frozen seed projection from the type contract. */
public final class SeedProjection {
    private SeedProjection() {}

    public static Set<GraphElementRef> seeds(RetrievalValue value) {
        LinkedHashSet<GraphElementRef> seeds = new LinkedHashSet<>();
        if (value instanceof AnchorSet anchors) {
            for (AnchorMention mention : anchors.mentions()) {
                for (AnchorCandidate candidate : mention.candidates()) {
                    addVertex(seeds, candidate.element());
                }
            }
        } else if (value instanceof CandidateSet<?> candidates) {
            for (ScoredCandidate<?> item : candidates.items()) {
                addItem(seeds, item.item());
            }
        } else if (value instanceof ExpandedVertexSet expanded) {
            for (ExpandedVertex vertex : expanded.vertices()) {
                addVertex(seeds, vertex.vertex());
            }
        } else if (value instanceof PathSet paths) {
            for (ScoredPath path : paths.paths()) {
                if (path.elements().isEmpty()) {
                    continue;
                }
                addVertex(seeds, path.elements().get(0));
                addVertex(seeds, path.elements().get(path.elements().size() - 1));
            }
        } else if (value instanceof CandidateSubgraph subgraph) {
            subgraph.vertices().forEach(vertex -> addVertex(seeds, vertex));
        }
        return seeds;
    }

    private static void addItem(Set<GraphElementRef> seeds, RetrievalItemRef item) {
        if (item instanceof GraphElementRef ref) {
            addVertex(seeds, ref);
        } else if (item instanceof ContentRef content) {
            content.linkedElements().forEach(element -> addVertex(seeds, element));
        }
    }

    private static void addVertex(Set<GraphElementRef> seeds, GraphElementRef ref) {
        if (ref != null && ref.kind() == ElementKind.VERTEX) {
            seeds.add(ref);
        }
    }
}
