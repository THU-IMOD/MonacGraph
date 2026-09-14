package db.monacgraph.retrieval;

import db.monacgraph.retrieval.generation.AnswerGenerator;
import db.monacgraph.retrieval.model.PipelineModels.AnswerResult;
import db.monacgraph.retrieval.model.PipelineModels.BudgetOptions;
import db.monacgraph.retrieval.model.PipelineModels.BudgetSelection;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.EvidenceResult;
import db.monacgraph.retrieval.model.PipelineModels.GenerationMetadata;
import db.monacgraph.retrieval.model.PipelineModels.GenerationOptions;
import db.monacgraph.retrieval.model.PipelineModels.GenerationRequest;
import db.monacgraph.retrieval.model.PipelineModels.PprOptions;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.QueryContext;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalValue;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.hipporag.FactProjector;
import db.monacgraph.retrieval.hipporag.HippoRagGraph;
import db.monacgraph.retrieval.hipporag.HippoRagV1;
import db.monacgraph.retrieval.pipeline.BudgetAndEvidence;
import db.monacgraph.retrieval.pipeline.EntityLinker;
import db.monacgraph.retrieval.pipeline.PersonalizedPageRank;
import db.monacgraph.retrieval.pipeline.SeedProjection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Fluent retrieval pipeline. {@code hippoRag()} runs HippoRAG 1:
 * NER + dense phrase link → weighted PPR → fact projection → optional dense ensemble.
 */
public final class RetrievalPipelineBuilder {
    private enum Stage {
        ENTITY_LINK, TEXT_RECALL, VECTOR_RECALL, PPR, BUDGET, EVIDENCE, GENERATE
    }

    private final RetrievalRuntime runtime;
    private final QueryContext query;
    private final List<Stage> stages = new ArrayList<>();
    private int topK = 50;
    private PprOptions pprOptions = PprOptions.hippoRag();
    private BudgetOptions budgetOptions = BudgetOptions.defaults();
    private AnswerGenerator generator;
    private boolean hippoRagV1;

    public RetrievalPipelineBuilder(RetrievalServices services, String query) {
        this(new RetrievalRuntime(services, List::of, vertex -> List.of(), null), query);
    }

    public RetrievalPipelineBuilder(RetrievalRuntime runtime, String query) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("query cannot be blank");
        }
        this.query = new QueryContext(query, Optional.empty());
        this.generator = runtime.generator();
    }

    /** HippoRAG 1 (NeurIPS 2024) official retrieve path. */
    public RetrievalPipelineBuilder hippoRag() {
        this.hippoRagV1 = true;
        return entityLink().ppr();
    }

    public RetrievalPipelineBuilder entityLink() {
        stages.add(Stage.ENTITY_LINK);
        return this;
    }

    public RetrievalPipelineBuilder textRecall() {
        stages.add(Stage.TEXT_RECALL);
        return this;
    }

    public RetrievalPipelineBuilder vectorRecall() {
        stages.add(Stage.VECTOR_RECALL);
        return this;
    }

    public RetrievalPipelineBuilder ppr() {
        stages.add(Stage.PPR);
        return this;
    }

    public RetrievalPipelineBuilder ppr(PprOptions options) {
        this.pprOptions = Objects.requireNonNull(options, "options");
        return ppr();
    }

    public RetrievalPipelineBuilder topK(int topK) {
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        this.topK = topK;
        this.pprOptions = new PprOptions(
                pprOptions.resetProbability(), pprOptions.iterations(), topK);
        return this;
    }

    public RetrievalPipelineBuilder budget() {
        stages.add(Stage.BUDGET);
        return this;
    }

    public RetrievalPipelineBuilder budget(BudgetOptions options) {
        this.budgetOptions = Objects.requireNonNull(options, "options");
        return budget();
    }

    public RetrievalPipelineBuilder evidence() {
        stages.add(Stage.EVIDENCE);
        return this;
    }

    public RetrievalPipelineBuilder generate() {
        stages.add(Stage.GENERATE);
        return this;
    }

    public RetrievalPipelineBuilder generate(AnswerGenerator generator) {
        this.generator = generator;
        return generate();
    }

    public List<ContentCandidate> executeRecall() {
        Stage recall = exclusiveRecall();
        if (recall == Stage.TEXT_RECALL) {
            return runtime.services().contentIndex().textSearch(query.text(), topK);
        }
        float[] queryVector = runtime.services().embeddingProvider().embedQuery(query.text());
        return runtime.services().contentIndex().vectorSearch(
                queryVector, runtime.services().embeddingProvider().space(), topK);
    }

    public AnswerResult execute() {
        return executeTo(AnswerResult.class);
    }

    public <T> T executeTo(Class<T> type) {
        Objects.requireNonNull(type, "type");
        if (hippoRagV1 && hippo() != null && type == AnswerResult.class) {
            return type.cast(generateAnswer(hippo().retrieve(query.text())));
        }
        if (hippoRagV1 && hippo() != null && type == CandidateSet.class) {
            return type.cast(hippo().retrieve(query.text()));
        }
        List<Stage> plan = type == AnswerResult.class ? withTerminalStages() : List.copyOf(stages);
        Object current = null;
        for (Stage stage : plan) {
            current = apply(stage, current);
            if (type != AnswerResult.class && type.isInstance(current)) {
                return type.cast(current);
            }
        }
        if (!type.isInstance(current)) {
            throw new IllegalStateException(
                    "Pipeline ended at " + (current == null ? "null" : current.getClass().getSimpleName())
                            + ", not " + type.getSimpleName());
        }
        return type.cast(current);
    }

    private Object apply(Stage stage, Object current) {
        return switch (stage) {
            case ENTITY_LINK -> {
                requireNull(current, stage);
                if (hippo() != null) {
                    yield hippo().link(query.text());
                }
                yield new EntityLinker(runtime.entities()).link(query);
            }
            case TEXT_RECALL -> {
                requireNull(current, stage);
                yield toCandidates(
                        runtime.services().contentIndex().textSearch(query.text(), topK),
                        ScoreSemantics.BM25,
                        "textRecall");
            }
            case VECTOR_RECALL -> {
                requireNull(current, stage);
                float[] queryVector = runtime.services().embeddingProvider().embedQuery(query.text());
                yield toCandidates(
                        runtime.services().contentIndex().vectorSearch(
                                queryVector, runtime.services().embeddingProvider().space(), topK),
                        ScoreSemantics.COSINE_SIMILARITY,
                        "vectorRecall");
            }
            case PPR -> {
                RetrievalValue value = requireValue(current, stage);
                if (SeedProjection.seeds(value).isEmpty()) {
                    throw new IllegalStateException("ppr() refused empty seeds");
                }
                if (runtime.hippoRagIndex() != null) {
                    yield new PersonalizedPageRank(
                            new HippoRagGraph(runtime.hippoRagIndex()), runtime.entities())
                            .expand(value, pprOptions);
                }
                yield new PersonalizedPageRank(runtime.neighborhood(), runtime.entities())
                        .expand(value, pprOptions);
            }
            case BUDGET -> {
                RetrievalValue value = requireValue(current, stage);
                if (runtime.hippoRagIndex() != null && value instanceof CandidateSet<?> candidates
                        && !candidates.items().isEmpty()
                        && candidates.items().get(0).item() instanceof GraphElementRef) {
                    @SuppressWarnings("unchecked")
                    CandidateSet<GraphElementRef> nodes = (CandidateSet<GraphElementRef>) value;
                    value = new FactProjector(runtime.hippoRagIndex(), runtime.services().contentStore())
                            .project(nodes);
                }
                yield new BudgetAndEvidence(runtime.services().contentStore())
                        .budget(value, budgetOptions);
            }
            case EVIDENCE -> {
                BudgetSelection selection = current instanceof BudgetSelection budget
                        ? budget
                        : new BudgetAndEvidence(runtime.services().contentStore())
                                .budget(requireValue(current, stage), budgetOptions);
                yield new BudgetAndEvidence(runtime.services().contentStore()).evidence(query, selection);
            }
            case GENERATE -> generateAnswer(current);
        };
    }

    private AnswerResult generateAnswer(Object current) {
        EvidenceResult evidence = current instanceof EvidenceResult ready
                ? ready
                : new BudgetAndEvidence(runtime.services().contentStore()).evidence(
                        query,
                        current instanceof BudgetSelection budget
                                ? budget
                                : new BudgetAndEvidence(runtime.services().contentStore())
                                        .budget(requireValue(current, Stage.GENERATE), budgetOptions));
        GenerationRequest request = new GenerationRequest(
                query.text(),
                evidence.llmContext(),
                BudgetAndEvidence.references(evidence),
                GenerationOptions.defaults());
        if (generator == null) {
            return new AnswerResult(
                    "",
                    List.of(),
                    evidence,
                    new GenerationMetadata("none", "", 0, 0, "unavailable"));
        }
        try {
            return generator.generate(request, evidence);
        } catch (RuntimeException e) {
            return new AnswerResult(
                    "",
                    List.of(),
                    evidence,
                    new GenerationMetadata("error", "", 0, 0, e.getMessage()));
        }
    }

    private List<Stage> withTerminalStages() {
        List<Stage> plan = new ArrayList<>(stages);
        if (plan.isEmpty()) {
            throw new IllegalStateException("Add at least one Recall or Expand stage before execute()");
        }
        if (!plan.contains(Stage.BUDGET)) {
            plan.add(Stage.BUDGET);
        }
        if (!plan.contains(Stage.EVIDENCE)) {
            plan.add(Stage.EVIDENCE);
        }
        if (!plan.contains(Stage.GENERATE)) {
            plan.add(Stage.GENERATE);
        }
        return plan;
    }

    private Stage exclusiveRecall() {
        Stage recall = null;
        for (Stage stage : stages) {
            if (stage == Stage.TEXT_RECALL || stage == Stage.VECTOR_RECALL) {
                if (recall != null) {
                    throw new IllegalStateException("Only one Recall operator can run in executeRecall()");
                }
                recall = stage;
            }
        }
        if (recall == null) {
            throw new IllegalStateException("Select textRecall() or vectorRecall() before execution");
        }
        return recall;
    }

    private static void requireNull(Object current, Stage stage) {
        if (current != null) {
            throw new IllegalStateException(stage + " must be the first Recall stage");
        }
    }

    private static RetrievalValue requireValue(Object current, Stage stage) {
        if (current instanceof RetrievalValue value) {
            return value;
        }
        throw new IllegalStateException(stage + " requires a RetrievalValue, not "
                + (current == null ? "null" : current.getClass().getSimpleName()));
    }

    private HippoRagV1 hippo() {
        if (runtime.hippoRagIndex() == null || runtime.queryNer() == null) {
            return null;
        }
        return new HippoRagV1(
                runtime.hippoRagIndex(),
                runtime.entities(),
                runtime.services().embeddingProvider(),
                runtime.services().contentStore(),
                runtime.services().contentIndex(),
                runtime.queryNer(),
                pprOptions,
                topK);
    }

    private static CandidateSet<ContentRef> toCandidates(
            List<ContentCandidate> hits, ScoreSemantics semantics, String operator) {
        List<ScoredCandidate<ContentRef>> items = new ArrayList<>();
        for (ContentCandidate hit : hits) {
            items.add(new ScoredCandidate<>(
                    hit.content(),
                    hit.rank(),
                    List.of(new RetrievalScore(operator, hit.score(), semantics)),
                    new Provenance(operator, List.of(hit.content().contentId()), Map.of())));
        }
        return new CandidateSet<>(items);
    }
}
