package db.monacgraph.ingestion.resolution;

import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;

public interface EntityResolver {
    CanonicalEntity resolve(ExtractedEntity entity);
}
