package db.monacgraph.ingestion.resolution;

import db.monacgraph.ingestion.IngestionIds;
import db.monacgraph.ingestion.model.IngestionModels.CanonicalEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Conservative P0 resolver: exact canonical names and unambiguous aliases only.
 */
public final class ExactAliasEntityResolver implements EntityResolver {
    private final EntityCatalog catalog;

    public ExactAliasEntityResolver(EntityCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public synchronized CanonicalEntity resolve(ExtractedEntity entity) {
        String normalizedType = EntityNameNormalizer.normalize(entity.type());
        String normalizedName = EntityNameNormalizer.normalize(entity.name());
        if (normalizedName.isBlank()) {
            throw new IllegalArgumentException("Entity name becomes empty after normalization");
        }

        CanonicalEntity existing = catalog.findCanonical(normalizedType, normalizedName)
                .orElseGet(() -> uniqueAliasMatch(normalizedType, normalizedName));
        if (existing != null) {
            LinkedHashSet<String> aliases = new LinkedHashSet<>(existing.aliases());
            aliases.add(entity.name());
            aliases.addAll(entity.aliases());
            CanonicalEntity updated = new CanonicalEntity(
                    existing.vertexId(), existing.canonicalName(), existing.type(), List.copyOf(aliases));
            catalog.put(updated);
            return updated;
        }

        LinkedHashSet<String> aliases = new LinkedHashSet<>(entity.aliases());
        aliases.add(entity.name());
        CanonicalEntity created = new CanonicalEntity(
                IngestionIds.deterministicUuid(
                        "vertex", normalizedType + "\u0000" + normalizedName),
                entity.name(),
                normalizedType.isBlank() ? "entity" : normalizedType,
                List.copyOf(aliases));
        catalog.put(created);
        return created;
    }

    private CanonicalEntity uniqueAliasMatch(String type, String alias) {
        List<CanonicalEntity> candidates = catalog.findAlias(type, alias);
        return candidates.size() == 1 ? candidates.get(0) : null;
    }
}
