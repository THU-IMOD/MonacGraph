package db.monacgraph.retrieval.pipeline;

import db.monacgraph.retrieval.model.PipelineModels.BudgetOptions;
import db.monacgraph.retrieval.model.PipelineModels.BudgetSelection;
import db.monacgraph.retrieval.model.PipelineModels.BudgetStats;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.ContentEvidence;
import db.monacgraph.retrieval.model.PipelineModels.ContentEvidenceRef;
import db.monacgraph.retrieval.model.PipelineModels.EdgeEvidence;
import db.monacgraph.retrieval.model.PipelineModels.EvidenceReference;
import db.monacgraph.retrieval.model.PipelineModels.EvidenceResult;
import db.monacgraph.retrieval.model.PipelineModels.ExecutionTrace;
import db.monacgraph.retrieval.model.PipelineModels.PathEvidence;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.QueryContext;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalValue;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.VertexEvidence;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Budget selects already-scored refs; Evidence materializes text. HippoRAG
 * passage score is the sum of PPR mass on mentioned vertices.
 */
public final class BudgetAndEvidence {
    private final ContentStore store;

    public BudgetAndEvidence(ContentStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    public BudgetSelection budget(RetrievalValue value, BudgetOptions options) {
        List<GraphElementRef> vertices = new ArrayList<>(SeedProjection.seeds(value));
        Map<String, Double> passageScores = new LinkedHashMap<>();
        Map<String, ContentRef> passages = new LinkedHashMap<>();
        if (value instanceof CandidateSet<?> candidates) {
            for (ScoredCandidate<?> item : candidates.items()) {
                if (item.item() instanceof ContentRef content) {
                    passages.putIfAbsent(content.contentId(), content);
                    passageScores.merge(content.contentId(), score(item), Double::sum);
                }
            }
        }
        Map<Object, Double> vertexScore = new LinkedHashMap<>();
        if (value instanceof CandidateSet<?> candidates) {
            for (ScoredCandidate<?> item : candidates.items()) {
                if (item.item() instanceof GraphElementRef ref && ref.kind() == ElementKind.VERTEX) {
                    vertexScore.put(ref.id(), score(item));
                }
            }
        }
        vertices.sort(Comparator.comparingDouble((GraphElementRef vertex) ->
                vertexScore.getOrDefault(vertex.id(), 0.0)).reversed());
        if (vertices.size() > options.maxVertices()) {
            vertices = new ArrayList<>(vertices.subList(0, options.maxVertices()));
        }
        if (passages.isEmpty()) {
            for (GraphElementRef vertex : vertices) {
                double nodeMass = vertexScore.getOrDefault(vertex.id(), 0.0);
                for (String contentId : store.contentIdsForElement(vertex)) {
                    store.get(contentId).ifPresent(record -> {
                        passages.putIfAbsent(contentId, ref(record));
                        passageScores.merge(contentId, nodeMass, Double::sum);
                    });
                }
            }
        }
        List<String> ranked = new ArrayList<>(passages.keySet());
        ranked.sort(Comparator.comparingDouble((String id) ->
                passageScores.getOrDefault(id, 0.0)).reversed());
        boolean truncated = ranked.size() > options.maxContents();
        List<ContentRef> selected = new ArrayList<>();
        int tokens = 0;
        for (String contentId : ranked) {
            if (selected.size() >= options.maxContents()) {
                truncated = true;
                break;
            }
            ContentRef content = passages.get(contentId);
            int add = tokenEstimate(store.get(contentId).map(ContentRecord::text).orElse(""));
            if (tokens + add > options.maxTokens() && !selected.isEmpty()) {
                truncated = true;
                break;
            }
            selected.add(content);
            tokens += add;
        }
        return new BudgetSelection(
                selected,
                vertices,
                List.of(),
                List.of(),
                new BudgetStats(
                        ranked.size(),
                        SeedProjection.seeds(value).size(),
                        0,
                        0,
                        truncated,
                        tokens));
    }

    public EvidenceResult evidence(QueryContext query, BudgetSelection selection) {
        List<ContentEvidence> contents = new ArrayList<>();
        List<Provenance> provenance = new ArrayList<>();
        StringBuilder context = new StringBuilder();
        int index = 1;
        for (ContentRef ref : selection.contents()) {
            ContentRecord record = store.get(ref.contentId()).orElse(null);
            if (record == null) {
                continue;
            }
            Provenance source = new Provenance(
                    "evidence", List.of(record.contentId()), Map.of("rank", index));
            contents.add(new ContentEvidence(
                    record.contentId(),
                    record.text(),
                    record.links().stream().map(link -> link.element()).toList(),
                    record.sourceField(),
                    source));
            provenance.add(source);
            context.append('[').append(index).append("]\n").append(record.text()).append("\n\n");
            index++;
        }
        List<VertexEvidence> vertices = selection.vertices().stream()
                .map(vertex -> new VertexEvidence(
                        vertex,
                        Map.of(),
                        new Provenance("evidence", List.of(vertex.id()), Map.of())))
                .toList();
        return new EvidenceResult(
                query.text(),
                contents,
                vertices,
                List.<EdgeEvidence>of(),
                List.<PathEvidence>of(),
                context.toString().trim(),
                provenance,
                new ExecutionTrace(
                        List.of("budget", "evidence"),
                        selection.stats().inputVertices(),
                        contents.size(),
                        selection.stats().truncated()));
    }

    public static List<EvidenceReference> references(EvidenceResult evidence) {
        List<EvidenceReference> refs = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (ContentEvidence content : evidence.contents()) {
            if (seen.add(content.contentId())) {
                refs.add(new ContentEvidenceRef(content.contentId()));
            }
        }
        return refs;
    }

    private static ContentRef ref(ContentRecord record) {
        return new ContentRef(
                record.contentId(),
                record.links().stream().map(link -> link.element()).toList(),
                record.sourceField());
    }

    private static double score(ScoredCandidate<?> item) {
        return item.scores().isEmpty() ? 0 : item.scores().get(0).value();
    }

    static int tokenEstimate(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        return Math.max(1, text.length() / 4);
    }
}
