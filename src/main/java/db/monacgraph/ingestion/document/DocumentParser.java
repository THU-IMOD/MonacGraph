package db.monacgraph.ingestion.document;

import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;

import java.nio.file.Path;

public interface DocumentParser {
    ParsedDocument parse(Path path);
}
