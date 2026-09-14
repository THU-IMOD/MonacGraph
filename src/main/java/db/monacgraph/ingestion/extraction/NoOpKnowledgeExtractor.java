package db.monacgraph.ingestion.extraction;

import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.Passage;

/** Allows parser/chunker/index development without an LLM service. */
public final class NoOpKnowledgeExtractor implements KnowledgeExtractor {
    @Override
    public ExtractedKnowledge extract(Passage passage) {
        return new ExtractedKnowledge(null, null);
    }
}
