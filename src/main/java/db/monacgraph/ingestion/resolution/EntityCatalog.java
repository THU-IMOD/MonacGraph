package db.monacgraph.ingestion.resolution;

import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;

import java.util.List;
import java.util.Optional;

public interface EntityCatalog extends AutoCloseable {
    Optional<CanonicalEntity> findCanonical(String type, String normalizedName);

    List<CanonicalEntity> findAlias(String type, String normalizedAlias);

    void put(CanonicalEntity entity);

    List<CanonicalEntity> listAll();

    @Override
    void close();
}
