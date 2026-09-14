package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.model.PipelineModels.AnchorCandidate;
import db.monacgraph.retrieval.model.PipelineModels.AnchorMention;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HippoRAG 1 linking: each NER mention → nearest phrase vector, weight = 1/df.
 */
public final class DensePhraseLinker {
    private final HippoRagIndex index;
    private final EmbeddingProvider embeddings;
    private final ContentStore contents;
    private final QueryNer ner;

    public DensePhraseLinker(
            HippoRagIndex index,
            EmbeddingProvider embeddings,
            ContentStore contents,
            QueryNer ner) {
        this.index = Objects.requireNonNull(index, "index");
        this.embeddings = Objects.requireNonNull(embeddings, "embeddings");
        this.contents = Objects.requireNonNull(contents, "contents");
        this.ner = Objects.requireNonNull(ner, "ner");
    }

    public AnchorSet link(String query) {
        List<String> mentions = ner.namedEntities(query);
        if (mentions.isEmpty()) {
            return new AnchorSet(List.of());
        }
        List<PhraseNode> phrases = index.phrases();
        if (phrases.isEmpty()) {
            return new AnchorSet(List.of());
        }
        List<float[]> mentionVectors = embeddings.embedDocuments(mentions);
        List<AnchorMention> result = new ArrayList<>();
        int cursor = 0;
        for (int i = 0; i < mentions.size(); i++) {
            String surface = mentions.get(i);
            float[] queryVector = mentionVectors.get(i);
            PhraseNode best = null;
            double bestScore = Double.NEGATIVE_INFINITY;
            for (PhraseNode phrase : phrases) {
                float[] vector = index.vector(phrase.vertex().id()).orElse(null);
                if (vector == null) {
                    continue;
                }
                double score = Vectors.cosine(queryVector, vector);
                if (score > bestScore) {
                    bestScore = score;
                    best = phrase;
                }
            }
            if (best == null) {
                continue;
            }
            int start = indexOfIgnoreCase(query, surface, cursor);
            if (start < 0) {
                start = indexOfIgnoreCase(query, surface, 0);
            }
            if (start < 0) {
                start = 0;
            }
            int end = Math.min(query.length(), start + surface.length());
            cursor = end;
            int df = Math.max(1, contents.contentIdsForElement(best.vertex()).size());
            result.add(new AnchorMention(
                    surface,
                    start,
                    end,
                    List.of(new AnchorCandidate(
                            best.vertex(),
                            List.of(
                                    new RetrievalScore("cosine", bestScore, ScoreSemantics.LINKING_SCORE),
                                    new RetrievalScore("specificity", 1.0 / df, ScoreSemantics.LINKING_SCORE)),
                            new Provenance(
                                    "densePhraseLink",
                                    List.of(best.vertex().id()),
                                    Map.of("df", df, "cosine", bestScore))))));
        }
        return new AnchorSet(result);
    }

    static double minLinkingCosine(AnchorSet anchors) {
        double min = Double.POSITIVE_INFINITY;
        for (AnchorMention mention : anchors.mentions()) {
            for (AnchorCandidate candidate : mention.candidates()) {
                for (RetrievalScore score : candidate.scores()) {
                    if ("cosine".equals(score.name())) {
                        min = Math.min(min, score.value());
                    }
                }
            }
        }
        return min == Double.POSITIVE_INFINITY ? 0 : min;
    }

    private static int indexOfIgnoreCase(String haystack, String needle, int from) {
        return haystack.toLowerCase().indexOf(needle.toLowerCase(), from);
    }
}
