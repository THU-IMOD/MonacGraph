package db.monacgraph.retrieval.pipeline;

import db.monacgraph.retrieval.linking.EntityIndex;
import db.monacgraph.retrieval.linking.EntityIndex.IndexedEntity;
import db.monacgraph.retrieval.model.PipelineModels.AnchorCandidate;
import db.monacgraph.retrieval.model.PipelineModels.AnchorMention;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.QueryContext;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * HippoRAG query-node linking without a second LLM call: longest catalog
 * name/alias match in the query surface.
 */
public final class EntityLinker {
    private final EntityIndex index;

    public EntityLinker(EntityIndex index) {
        this.index = Objects.requireNonNull(index, "index");
    }

    public AnchorSet link(QueryContext query) {
        String text = query.text();
        String lower = text.toLowerCase(Locale.ROOT);
        List<Phrase> phrases = new ArrayList<>();
        for (IndexedEntity entity : index.entities()) {
            addPhrase(phrases, entity.canonicalName(), entity);
            for (String alias : entity.aliases()) {
                addPhrase(phrases, alias, entity);
            }
        }
        phrases.sort(Comparator.comparingInt((Phrase phrase) -> phrase.surface.length()).reversed());

        boolean[] taken = new boolean[text.length()];
        Map<String, MentionBuilder> mentions = new LinkedHashMap<>();
        for (Phrase phrase : phrases) {
            String needle = phrase.surface.toLowerCase(Locale.ROOT);
            int from = 0;
            while (from <= lower.length() - needle.length()) {
                int start = lower.indexOf(needle, from);
                if (start < 0) {
                    break;
                }
                int end = start + needle.length();
                if (!overlaps(taken, start, end) && bounded(text, start, end)) {
                    mark(taken, start, end);
                    String key = start + ":" + end + ":" + text.substring(start, end);
                    mentions.computeIfAbsent(key, ignored -> new MentionBuilder(
                                    text.substring(start, end), start, end))
                            .add(phrase.entity, phrase.surface.length());
                }
                from = start + 1;
            }
        }
        List<AnchorMention> result = mentions.values().stream()
                .map(MentionBuilder::build)
                .toList();
        return new AnchorSet(result);
    }

    private static void addPhrase(List<Phrase> phrases, String surface, IndexedEntity entity) {
        if (surface == null) {
            return;
        }
        String trimmed = surface.trim();
        if (trimmed.length() < 2) {
            return;
        }
        phrases.add(new Phrase(trimmed, entity));
    }

    private static boolean bounded(String text, int start, int end) {
        return !isTokenChar(text, start - 1) && !isTokenChar(text, end);
    }

    private static boolean isTokenChar(String text, int index) {
        return index >= 0 && index < text.length() && Character.isLetterOrDigit(text.charAt(index));
    }

    private static boolean overlaps(boolean[] taken, int start, int end) {
        for (int i = start; i < end; i++) {
            if (taken[i]) {
                return true;
            }
        }
        return false;
    }

    private static void mark(boolean[] taken, int start, int end) {
        for (int i = start; i < end; i++) {
            taken[i] = true;
        }
    }

    private record Phrase(String surface, IndexedEntity entity) {}

    private static final class MentionBuilder {
        private final String surface;
        private final int start;
        private final int end;
        private final List<AnchorCandidate> candidates = new ArrayList<>();

        private MentionBuilder(String surface, int start, int end) {
            this.surface = surface;
            this.start = start;
            this.end = end;
        }

        private void add(IndexedEntity entity, int matchLength) {
            if (entity.vertex().kind() != ElementKind.VERTEX) {
                return;
            }
            boolean duplicate = candidates.stream()
                    .anyMatch(candidate -> candidate.element().id().equals(entity.vertex().id()));
            if (duplicate) {
                return;
            }
            candidates.add(new AnchorCandidate(
                    entity.vertex(),
                    List.of(new RetrievalScore("alias-length", matchLength, ScoreSemantics.LINKING_SCORE)),
                    new Provenance("entityLink", List.of(entity.vertex().id()), Map.of("name", entity.canonicalName()))));
        }

        private AnchorMention build() {
            return new AnchorMention(surface, start, end, candidates);
        }
    }
}
