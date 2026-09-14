package db.monacgraph.retrieval.hipporag;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.index.ContentIndex;
import db.monacgraph.retrieval.linking.EntityIndex;
import db.monacgraph.retrieval.model.PipelineModels.AnchorSet;
import db.monacgraph.retrieval.model.PipelineModels.CandidateSet;
import db.monacgraph.retrieval.model.PipelineModels.PprOptions;
import db.monacgraph.retrieval.model.PipelineModels.Provenance;
import db.monacgraph.retrieval.model.PipelineModels.RetrievalScore;
import db.monacgraph.retrieval.model.PipelineModels.ScoredCandidate;
import db.monacgraph.retrieval.model.PipelineModels.ScoreSemantics;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.pipeline.PersonalizedPageRank;
import db.monacgraph.retrieval.storage.ContentStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Official HippoRAG 1 retrieve path (Contriever-style encoder, not ColBERT).
 */
public final class HippoRagV1 {
    private final DensePhraseLinker linker;
    private final PersonalizedPageRank pagerank;
    private final FactProjector projector;
    private final ContentIndex contentIndex;
    private final EmbeddingProvider embeddings;
    private final PprOptions pprOptions;
    private final int topK;

    public HippoRagV1(
            HippoRagIndex index,
            EntityIndex entities,
            EmbeddingProvider embeddings,
            ContentStore contents,
            ContentIndex contentIndex,
            QueryNer ner,
            PprOptions pprOptions,
            int topK) {
        this.linker = new DensePhraseLinker(index, embeddings, contents, ner);
        this.pagerank = new PersonalizedPageRank(new HippoRagGraph(index), entities);
        this.projector = new FactProjector(index, contents);
        this.contentIndex = Objects.requireNonNull(contentIndex, "contentIndex");
        this.embeddings = Objects.requireNonNull(embeddings, "embeddings");
        this.pprOptions = pprOptions == null ? PprOptions.hippoRag() : pprOptions;
        this.topK = topK <= 0 ? 50 : topK;
    }

    public AnchorSet link(String query) {
        return linker.link(query);
    }

    public CandidateSet<GraphElementRef> expand(AnchorSet anchors) {
        return pagerank.expand(anchors, pprOptions);
    }

    public CandidateSet<ContentRef> project(CandidateSet<GraphElementRef> nodes) {
        return projector.project(nodes);
    }

    public CandidateSet<ContentRef> retrieve(String query) {
        AnchorSet anchors = link(query);
        CandidateSet<ContentRef> dense = densePassages(query);
        if (anchors.mentions().isEmpty()) {
            return dense;
        }
        CandidateSet<ContentRef> graphDocs = project(expand(anchors));
        if (DensePhraseLinker.minLinkingCosine(anchors) > HippoRagModels.RECOGNITION_THRESHOLD) {
            return cap(graphDocs);
        }
        return cap(fuse(graphDocs, dense));
    }

    private CandidateSet<ContentRef> densePassages(String query) {
        float[] vector = embeddings.embedQuery(query);
        List<ContentCandidate> hits = contentIndex.vectorSearch(vector, embeddings.space(), topK);
        List<ScoredCandidate<ContentRef>> items = new ArrayList<>();
        for (ContentCandidate hit : hits) {
            items.add(new ScoredCandidate<>(
                    hit.content(),
                    hit.rank(),
                    List.of(new RetrievalScore("vectorRecall", hit.score(), ScoreSemantics.COSINE_SIMILARITY)),
                    new Provenance("vectorRecall", List.of(hit.content().contentId()), Map.of())));
        }
        return new CandidateSet<>(items);
    }

    private CandidateSet<ContentRef> fuse(
            CandidateSet<ContentRef> graphDocs, CandidateSet<ContentRef> denseDocs) {
        Map<String, ContentRef> refs = new LinkedHashMap<>();
        Map<String, Double> graph = scores(graphDocs, refs);
        Map<String, Double> dense = scores(denseDocs, refs);
        double[] graphValues = new double[refs.size()];
        double[] denseValues = new double[refs.size()];
        List<String> ids = new ArrayList<>(refs.keySet());
        for (int i = 0; i < ids.size(); i++) {
            graphValues[i] = graph.getOrDefault(ids.get(i), 0.0);
            denseValues[i] = dense.getOrDefault(ids.get(i), 0.0);
        }
        graphValues = Vectors.minMaxNormalize(graphValues);
        denseValues = Vectors.minMaxNormalize(denseValues);
        List<ScoredCandidate<ContentRef>> fused = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            fused.add(new ScoredCandidate<>(
                    refs.get(ids.get(i)),
                    0,
                    List.of(new RetrievalScore(
                            "ensemble",
                            0.5 * graphValues[i] + 0.5 * denseValues[i],
                            ScoreSemantics.PPR_PROBABILITY)),
                    new Provenance("fuse", List.of(ids.get(i)), Map.of("graph", 0.5, "dense", 0.5))));
        }
        fused.sort(Comparator.comparingDouble((ScoredCandidate<ContentRef> item) ->
                item.scores().get(0).value()).reversed());
        List<ScoredCandidate<ContentRef>> ranked = new ArrayList<>();
        for (int i = 0; i < fused.size(); i++) {
            ScoredCandidate<ContentRef> item = fused.get(i);
            ranked.add(new ScoredCandidate<>(item.item(), i + 1, item.scores(), item.provenance()));
        }
        return new CandidateSet<>(ranked);
    }

    private CandidateSet<ContentRef> cap(CandidateSet<ContentRef> docs) {
        if (docs.items().size() <= topK) {
            return docs;
        }
        return new CandidateSet<>(docs.items().subList(0, topK));
    }

    private static Map<String, Double> scores(
            CandidateSet<ContentRef> docs, Map<String, ContentRef> refs) {
        Map<String, Double> values = new LinkedHashMap<>();
        for (ScoredCandidate<ContentRef> item : docs.items()) {
            refs.putIfAbsent(item.item().contentId(), item.item());
            values.put(item.item().contentId(),
                    item.scores().isEmpty() ? 0 : item.scores().get(0).value());
        }
        return values;
    }
}
