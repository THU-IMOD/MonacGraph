package db.monacgraph.retrieval.model;

import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.RetrievalItemRef;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Typed values passed between retrieval stages. Persistent storage types stay
 * in {@link RetrievalModels}; these records must not carry passage bodies
 * until {@link EvidenceResult}.
 */
public final class PipelineModels {
    private PipelineModels() {}

    public enum ScoreSemantics {
        LINKING_SCORE,
        COSINE_SIMILARITY,
        BM25,
        PPR_PROBABILITY,
        PATH_RELEVANCE
    }

    public record QueryEmbedding(String spaceId, float[] values) {
        public QueryEmbedding {
            Objects.requireNonNull(spaceId, "spaceId");
            Objects.requireNonNull(values, "values");
        }
    }

    public record QueryContext(String text, Optional<QueryEmbedding> embedding) {
        public QueryContext {
            Objects.requireNonNull(text, "text");
            embedding = embedding == null ? Optional.empty() : embedding;
        }
    }

    public record RetrievalScore(String name, double value, ScoreSemantics semantics) {
        public RetrievalScore {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(semantics, "semantics");
        }
    }

    public record Provenance(
            String operator,
            List<Object> sourceIds,
            Map<String, Object> parameters) {
        public Provenance {
            Objects.requireNonNull(operator, "operator");
            sourceIds = sourceIds == null ? List.of() : List.copyOf(sourceIds);
            parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        }
    }

    public sealed interface RetrievalValue permits
            AnchorSet,
            CandidateSet,
            ExpandedVertexSet,
            PathSet,
            CandidateSubgraph {}

    public record AnchorSet(List<AnchorMention> mentions) implements RetrievalValue {
        public AnchorSet {
            mentions = mentions == null ? List.of() : List.copyOf(mentions);
        }
    }

    public record AnchorMention(
            String surface,
            int startOffset,
            int endOffset,
            List<AnchorCandidate> candidates) {
        public AnchorMention {
            Objects.requireNonNull(surface, "surface");
            candidates = candidates == null ? List.of() : List.copyOf(candidates);
        }
    }

    public record AnchorCandidate(
            GraphElementRef element,
            List<RetrievalScore> scores,
            Provenance provenance) {
        public AnchorCandidate {
            Objects.requireNonNull(element, "element");
            scores = scores == null ? List.of() : List.copyOf(scores);
        }
    }

    public record ScoredCandidate<T extends RetrievalItemRef>(
            T item,
            int rank,
            List<RetrievalScore> scores,
            Provenance provenance) {
        public ScoredCandidate {
            Objects.requireNonNull(item, "item");
            scores = scores == null ? List.of() : List.copyOf(scores);
        }
    }

    public record CandidateSet<T extends RetrievalItemRef>(
            List<ScoredCandidate<T>> items) implements RetrievalValue {
        public CandidateSet {
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    public record TraversalRef(Object parentVertexId, Object edgeId, String direction) {}

    public record ExpandedVertex(
            GraphElementRef vertex,
            int depth,
            List<TraversalRef> reachedBy,
            List<RetrievalScore> scores,
            Provenance provenance) {
        public ExpandedVertex {
            Objects.requireNonNull(vertex, "vertex");
            reachedBy = reachedBy == null ? List.of() : List.copyOf(reachedBy);
            scores = scores == null ? List.of() : List.copyOf(scores);
        }
    }

    public record ExpandedVertexSet(List<ExpandedVertex> vertices) implements RetrievalValue {
        public ExpandedVertexSet {
            vertices = vertices == null ? List.of() : List.copyOf(vertices);
        }
    }

    public record ScoredPath(
            List<GraphElementRef> elements,
            List<RetrievalScore> scores,
            Provenance provenance) {
        public ScoredPath {
            elements = elements == null ? List.of() : List.copyOf(elements);
            scores = scores == null ? List.of() : List.copyOf(scores);
        }
    }

    public record PathSet(List<ScoredPath> paths) implements RetrievalValue {
        public PathSet {
            paths = paths == null ? List.of() : List.copyOf(paths);
        }
    }

    public record CandidateSubgraph(
            List<GraphElementRef> vertices,
            List<GraphElementRef> edges,
            List<RetrievalScore> scores,
            Provenance provenance) implements RetrievalValue {
        public CandidateSubgraph {
            vertices = vertices == null ? List.of() : List.copyOf(vertices);
            edges = edges == null ? List.of() : List.copyOf(edges);
            scores = scores == null ? List.of() : List.copyOf(scores);
        }
    }

    public record BudgetOptions(
            int maxContents,
            int maxVertices,
            int maxEdges,
            int maxPaths,
            int maxTokens) {
        public BudgetOptions {
            if (maxContents <= 0 || maxVertices <= 0 || maxEdges <= 0
                    || maxPaths <= 0 || maxTokens <= 0) {
                throw new IllegalArgumentException("Budget limits must be positive");
            }
        }

        public static BudgetOptions defaults() {
            return new BudgetOptions(20, 40, 80, 10, 4000);
        }
    }

    public record BudgetStats(
            int inputContents,
            int inputVertices,
            int inputEdges,
            int inputPaths,
            boolean truncated,
            int tokenEstimate) {}

    public record BudgetSelection(
            List<ContentRef> contents,
            List<GraphElementRef> vertices,
            List<GraphElementRef> edges,
            List<ScoredPath> paths,
            BudgetStats stats) {
        public BudgetSelection {
            contents = contents == null ? List.of() : List.copyOf(contents);
            vertices = vertices == null ? List.of() : List.copyOf(vertices);
            edges = edges == null ? List.of() : List.copyOf(edges);
            paths = paths == null ? List.of() : List.copyOf(paths);
        }
    }

    public record PprOptions(double resetProbability, int iterations, int topK) {
        public PprOptions {
            if (resetProbability <= 0 || resetProbability > 1) {
                throw new IllegalArgumentException("resetProbability must be in (0, 1]");
            }
            if (iterations <= 0 || topK <= 0) {
                throw new IllegalArgumentException("iterations and topK must be positive");
            }
        }

        /** HippoRAG 1: igraph damping=0.1 is teleport 0.9. */
        public static PprOptions hippoRag() {
            return new PprOptions(0.9, 20, 50);
        }
    }

    public record ContentEvidence(
            String contentId,
            String text,
            List<GraphElementRef> linkedElements,
            String sourceField,
            Provenance provenance) {
        public ContentEvidence {
            Objects.requireNonNull(contentId, "contentId");
            Objects.requireNonNull(text, "text");
            linkedElements = linkedElements == null ? List.of() : List.copyOf(linkedElements);
        }
    }

    public record VertexEvidence(
            GraphElementRef vertex,
            Map<String, Object> properties,
            Provenance provenance) {
        public VertexEvidence {
            Objects.requireNonNull(vertex, "vertex");
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }
    }

    public record EdgeEvidence(
            GraphElementRef edge,
            GraphElementRef source,
            GraphElementRef target,
            Provenance provenance) {
        public EdgeEvidence {
            Objects.requireNonNull(edge, "edge");
        }
    }

    public record PathEvidence(List<GraphElementRef> elements, Provenance provenance) {
        public PathEvidence {
            elements = elements == null ? List.of() : List.copyOf(elements);
        }
    }

    public record ExecutionTrace(
            List<String> stages,
            int inputCount,
            int outputCount,
            boolean truncated) {
        public ExecutionTrace {
            stages = stages == null ? List.of() : List.copyOf(stages);
        }
    }

    public sealed interface EvidenceReference permits
            ContentEvidenceRef,
            VertexEvidenceRef,
            EdgeEvidenceRef,
            PathEvidenceRef {}

    public record ContentEvidenceRef(String contentId) implements EvidenceReference {}

    public record VertexEvidenceRef(Object vertexId) implements EvidenceReference {}

    public record EdgeEvidenceRef(Object edgeId) implements EvidenceReference {}

    public record PathEvidenceRef(int pathIndex) implements EvidenceReference {}

    public record EvidenceResult(
            String query,
            List<ContentEvidence> contents,
            List<VertexEvidence> vertices,
            List<EdgeEvidence> edges,
            List<PathEvidence> paths,
            String llmContext,
            List<Provenance> provenance,
            ExecutionTrace trace) {
        public EvidenceResult {
            Objects.requireNonNull(query, "query");
            contents = contents == null ? List.of() : List.copyOf(contents);
            vertices = vertices == null ? List.of() : List.copyOf(vertices);
            edges = edges == null ? List.of() : List.copyOf(edges);
            paths = paths == null ? List.of() : List.copyOf(paths);
            llmContext = llmContext == null ? "" : llmContext;
            provenance = provenance == null ? List.of() : List.copyOf(provenance);
        }
    }

    public record GenerationOptions(String model, double temperature) {
        public static GenerationOptions defaults() {
            return new GenerationOptions("", 0);
        }
    }

    public record GenerationRequest(
            String query,
            String evidenceContext,
            List<EvidenceReference> references,
            GenerationOptions options) {
        public GenerationRequest {
            Objects.requireNonNull(query, "query");
            evidenceContext = evidenceContext == null ? "" : evidenceContext;
            references = references == null ? List.of() : List.copyOf(references);
            options = options == null ? GenerationOptions.defaults() : options;
        }
    }

    public record Citation(
            int answerStartOffset,
            int answerEndOffset,
            List<EvidenceReference> references) {
        public Citation {
            references = references == null ? List.of() : List.copyOf(references);
        }
    }

    public record GenerationMetadata(
            String provider,
            String model,
            int promptTokens,
            int completionTokens,
            String finishReason) {}

    public record AnswerResult(
            String answer,
            List<Citation> citations,
            EvidenceResult evidence,
            GenerationMetadata generation) {
        public AnswerResult {
            Objects.requireNonNull(evidence, "evidence");
            answer = answer == null ? "" : answer;
            citations = citations == null ? List.of() : List.copyOf(citations);
        }
    }
}
