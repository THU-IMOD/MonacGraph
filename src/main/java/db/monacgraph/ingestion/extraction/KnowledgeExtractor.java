package db.monacgraph.ingestion.extraction;

import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.Passage;

public interface KnowledgeExtractor extends AutoCloseable {
    ExtractedKnowledge extract(Passage passage);

    @Override
    default void close() {}
}
