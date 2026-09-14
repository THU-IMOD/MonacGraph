package db.monacgraph.ingestion.chunk;

import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;

import java.util.List;

public interface Chunker {
    List<Passage> split(ParsedDocument document);
}
