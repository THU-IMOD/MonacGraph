package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.hipporag.HippoRagModels.PhraseNode;
import db.monacgraph.retrieval.hipporag.HippoRagModels.SynonymEdge;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Embeds phrase names and writes synonym edges at similarity ≥ 0.8. */
public final class HippoRagIndexer {
    private final HippoRagIndex index;

    public HippoRagIndexer(HippoRagIndex index) {
        this.index = Objects.requireNonNull(index, "index");
    }

    public int refresh(EmbeddingProvider embeddings) {
        List<PhraseNode> phrases = index.phrases();
        List<PhraseNode> missing = phrases.stream()
                .filter(phrase -> index.vector(phrase.vertex().id()).isEmpty())
                .toList();
        if (!missing.isEmpty()) {
            List<float[]> vectors = embeddings.embedDocuments(
                    missing.stream().map(PhraseNode::name).toList());
            if (vectors.size() != missing.size()) {
                throw new IllegalStateException("Phrase embedding count mismatch");
            }
            for (int i = 0; i < missing.size(); i++) {
                index.putVector(missing.get(i).vertex().id(), vectors.get(i));
            }
        }
        int added = 0;
        List<PhraseNode> all = index.phrases();
        List<float[]> vectors = new ArrayList<>();
        for (PhraseNode phrase : all) {
            vectors.add(index.vector(phrase.vertex().id()).orElse(null));
        }
        for (int i = 0; i < all.size(); i++) {
            float[] left = vectors.get(i);
            if (left == null) {
                continue;
            }
            for (int j = i + 1; j < all.size(); j++) {
                float[] right = vectors.get(j);
                if (right == null) {
                    continue;
                }
                double score = Vectors.cosine(left, right);
                if (score >= HippoRagModels.SYNONYM_THRESHOLD) {
                    index.putSynonym(new SynonymEdge(all.get(i).vertex(), all.get(j).vertex(), score));
                    added++;
                }
            }
        }
        return added;
    }
}
