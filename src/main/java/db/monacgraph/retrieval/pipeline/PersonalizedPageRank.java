package db.monacgraph.retrieval.pipeline;

import db.monacgraph.retrieval.graph.GraphNeighborhood;
import db.monacgraph.retrieval.graph.WeightedNeighborhood;
import db.monacgraph.retrieval.hipporag.HippoRagModels.WeightedNeighbor;
import db.monacgraph.retrieval.linking.EntityIndex;
import db.monacgraph.retrieval.linking.EntityIndex.IndexedEntity;
import db.monacgraph.retrieval.model.PipelineModels.AnchorCandidate;
import db.monacgraph.retrieval.model.PipelineModels.AnchorMention;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.PprOptions;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalValue;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Weighted personalized PageRank. HippoRAG 1 uses igraph damping=0.1, which is
 * {@code resetProbability=0.9} here (teleport probability).
 */
public final class PersonalizedPageRank {
    private final WeightedNeighborhood neighborhood;
    private final EntityIndex entities;

    public PersonalizedPageRank(GraphNeighborhood neighborhood, EntityIndex entities) {
        this(WeightedNeighborhood.unweighted(neighborhood), entities);
    }

    public PersonalizedPageRank(WeightedNeighborhood neighborhood, EntityIndex entities) {
        this.neighborhood = Objects.requireNonNull(neighborhood, "neighborhood");
        this.entities = Objects.requireNonNull(entities, "entities");
    }

    public CandidateSet<GraphElementRef> expand(RetrievalValue input, PprOptions options) {
        Set<GraphElementRef> seeds = SeedProjection.seeds(input);
        if (seeds.isEmpty()) {
            throw new IllegalStateException("ppr() requires a non-empty seed set");
        }
        List<GraphElementRef> nodes = universe(seeds);
        Map<Object, Integer> index = new LinkedHashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            index.put(nodes.get(i).id(), i);
        }
        int n = nodes.size();
        double[] reset = resetVector(input, seeds, index, n);
        List<int[]> adjacency = new ArrayList<>();
        List<double[]> weights = new ArrayList<>();
        for (GraphElementRef node : nodes) {
            List<Integer> next = new ArrayList<>();
            List<Double> edgeWeights = new ArrayList<>();
            for (WeightedNeighbor neighbor : neighborhood.neighbors(node)) {
                Integer at = index.get(neighbor.vertex().id());
                if (at != null && !neighbor.vertex().id().equals(node.id())) {
                    next.add(at);
                    edgeWeights.add(neighbor.weight());
                }
            }
            adjacency.add(next.stream().mapToInt(Integer::intValue).toArray());
            weights.add(edgeWeights.stream().mapToDouble(Double::doubleValue).toArray());
        }

        double alpha = options.resetProbability();
        double[] rank = reset.clone();
        for (int iteration = 0; iteration < options.iterations(); iteration++) {
            double[] next = new double[n];
            for (int i = 0; i < n; i++) {
                int[] neighbors = adjacency.get(i);
                double[] edgeWeight = weights.get(i);
                if (neighbors.length == 0) {
                    next[i] += (1 - alpha) * rank[i];
                    continue;
                }
                double degree = 0;
                for (double weight : edgeWeight) {
                    degree += weight;
                }
                if (degree <= 0) {
                    next[i] += (1 - alpha) * rank[i];
                    continue;
                }
                double mass = (1 - alpha) * rank[i];
                for (int k = 0; k < neighbors.length; k++) {
                    next[neighbors[k]] += mass * (edgeWeight[k] / degree);
                }
            }
            for (int i = 0; i < n; i++) {
                next[i] += alpha * reset[i];
            }
            rank = next;
        }

        List<ScoredCandidate<GraphElementRef>> scored = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            GraphElementRef node = nodes.get(i);
            scored.add(new ScoredCandidate<>(
                    node,
                    0,
                    List.of(new RetrievalScore("ppr", rank[i], ScoreSemantics.PPR_PROBABILITY)),
                    new Provenance("ppr", List.copyOf(seeds.stream().map(GraphElementRef::id).toList()),
                            Map.of("resetProbability", alpha))));
        }
        scored.sort(Comparator.comparingDouble((ScoredCandidate<GraphElementRef> candidate) ->
                candidate.scores().get(0).value()).reversed());
        List<ScoredCandidate<GraphElementRef>> top = new ArrayList<>();
        int limit = Math.min(options.topK(), scored.size());
        for (int i = 0; i < limit; i++) {
            ScoredCandidate<GraphElementRef> item = scored.get(i);
            top.add(new ScoredCandidate<>(item.item(), i + 1, item.scores(), item.provenance()));
        }
        return new CandidateSet<>(top);
    }

    private static double[] resetVector(
            RetrievalValue input,
            Set<GraphElementRef> seeds,
            Map<Object, Integer> index,
            int n) {
        double[] reset = new double[n];
        if (input instanceof AnchorSet anchors) {
            for (AnchorMention mention : anchors.mentions()) {
                for (AnchorCandidate candidate : mention.candidates()) {
                    Integer at = index.get(candidate.element().id());
                    if (at == null) {
                        continue;
                    }
                    reset[at] += specificity(candidate);
                }
            }
        } else {
            double share = 1.0 / seeds.size();
            for (GraphElementRef seed : seeds) {
                Integer at = index.get(seed.id());
                if (at != null) {
                    reset[at] += share;
                }
            }
        }
        double sum = 0;
        for (double value : reset) {
            sum += value;
        }
        if (sum <= 0) {
            throw new IllegalStateException("ppr() reset vector is empty");
        }
        for (int i = 0; i < n; i++) {
            reset[i] /= sum;
        }
        return reset;
    }

    private static double specificity(AnchorCandidate candidate) {
        for (RetrievalScore score : candidate.scores()) {
            if ("specificity".equals(score.name()) && score.value() > 0) {
                return score.value();
            }
        }
        return 1.0;
    }

    private List<GraphElementRef> universe(Set<GraphElementRef> seeds) {
        LinkedHashSet<GraphElementRef> nodes = new LinkedHashSet<>(seeds);
        for (IndexedEntity entity : entities.entities()) {
            nodes.add(entity.vertex());
        }
        List<GraphElementRef> frontier = new ArrayList<>(nodes);
        for (int i = 0; i < frontier.size(); i++) {
            for (WeightedNeighbor neighbor : neighborhood.neighbors(frontier.get(i))) {
                if (nodes.add(neighbor.vertex())) {
                    frontier.add(neighbor.vertex());
                }
            }
        }
        return new ArrayList<>(nodes);
    }
}
