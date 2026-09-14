package db.monacgraph.retrieval.linking;

import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.ingestion.resolution.EntityCatalog;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;

import java.util.List;
import java.util.Objects;

public final class CatalogEntityIndex implements EntityIndex {
    private final EntityCatalog catalog;

    public CatalogEntityIndex(EntityCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public List<IndexedEntity> entities() {
        return catalog.listAll().stream()
                .map(CatalogEntityIndex::toIndexed)
                .toList();
    }

    private static IndexedEntity toIndexed(CanonicalEntity entity) {
        return new IndexedEntity(
                new GraphElementRef(ElementKind.VERTEX, entity.vertexId(), entity.type()),
                entity.canonicalName(),
                entity.aliases());
    }
}
