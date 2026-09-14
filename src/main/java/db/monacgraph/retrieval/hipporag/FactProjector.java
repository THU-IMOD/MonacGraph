package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.hipporag.HippoRagModels.FactTriple;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HippoRAG 1 document scores: phrase PPR → facts → supporting passages.
 */
public final class FactProjector {
    private final HippoRagIndex index;
    private final ContentStore store;

    public FactProjector(HippoRagIndex index, ContentStore store) {
        this.index = Objects.requireNonNull(index, "index");
        this.store = Objects.requireNonNull(store, "store");
    }

    public CandidateSet<ContentRef> project(CandidateSet<GraphElementRef> phrases) {
        Map<Object, Double> phraseMass = new HashMap<>();
        for (ScoredCandidate<GraphElementRef> item : phrases.items()) {
            double mass = item.scores().isEmpty() ? 0 : item.scores().get(0).value();
            phraseMass.put(item.item().id(), mass);
        }
        Map<String, Double> docMass = new HashMap<>();
        Map<String, ContentRef> docs = new HashMap<>();
        for (FactTriple fact : index.facts()) {
            double factScore = phraseMass.getOrDefault(fact.source().id(), 0.0)
                    + phraseMass.getOrDefault(fact.target().id(), 0.0);
            if (factScore <= 0) {
                continue;
            }
            for (String contentId : store.contentIdsForElement(fact.edge())) {
                store.get(contentId).ifPresent(record -> {
                    docs.putIfAbsent(contentId, ref(record));
                    docMass.merge(contentId, factScore, Double::sum);
                });
            }
        }
        if (docs.isEmpty()) {
            for (ScoredCandidate<GraphElementRef> item : phrases.items()) {
                double mass = phraseMass.getOrDefault(item.item().id(), 0.0);
                for (String contentId : store.contentIdsForElement(item.item())) {
                    store.get(contentId).ifPresent(record -> {
                        docs.putIfAbsent(contentId, ref(record));
                        docMass.merge(contentId, mass, Double::sum);
                    });
                }
            }
        }
        List<String> ranked = new ArrayList<>(docs.keySet());
        ranked.sort(Comparator.comparingDouble((String id) -> docMass.getOrDefault(id, 0.0)).reversed());
        List<ScoredCandidate<ContentRef>> items = new ArrayList<>();
        for (int i = 0; i < ranked.size(); i++) {
            String contentId = ranked.get(i);
            items.add(new ScoredCandidate<>(
                    docs.get(contentId),
                    i + 1,
                    List.of(new RetrievalScore(
                            "fact-ppr", docMass.getOrDefault(contentId, 0.0), ScoreSemantics.PPR_PROBABILITY)),
                    new Provenance("projectByFacts", List.of(contentId), Map.of())));
        }
        return new CandidateSet<>(items);
    }

    private static ContentRef ref(ContentRecord record) {
        return new ContentRef(
                record.contentId(),
                record.links().stream().map(link -> link.element()).toList(),
                record.sourceField());
    }
}
