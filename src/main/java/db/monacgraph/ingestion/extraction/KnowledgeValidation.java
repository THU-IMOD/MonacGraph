package db.monacgraph.ingestion.extraction;

import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedKnowledge;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.text.Normalizer;
import java.util.Locale;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class KnowledgeValidation {
    private KnowledgeValidation() {}

    public static ExtractedKnowledge validate(ExtractedKnowledge knowledge) {
        if (knowledge == null) {
            throw new IllegalArgumentException("Extractor returned null");
        }
        Set<String> localIds = new HashSet<>();
        for (ExtractedEntity entity : knowledge.entities()) {
            if (entity.localId().isBlank() || entity.name().isBlank()) {
                throw new IllegalArgumentException("Entity localId and name cannot be blank");
            }
            if (!localIds.add(entity.localId())) {
                throw new IllegalArgumentException("Duplicate entity localId: " + entity.localId());
            }
        }
        for (ExtractedRelation relation : knowledge.relations()) {
            if (relation.relation().isBlank()) {
                throw new IllegalArgumentException("Relation label cannot be blank");
            }
            if (relation.source().equals(relation.target())) {
                throw new IllegalArgumentException(
                        "Relation source and target must be different entities");
            }
            if (!localIds.contains(relation.source()) || !localIds.contains(relation.target())) {
                throw new IllegalArgumentException(
                        "Relation endpoints must reference entities in the same passage");
            }
        }
        return knowledge;
    }

    /**
     * Drops self-loops and edges whose endpoints are not declared. Used after
     * two-step extraction so one bad edge does not fail the whole passage.
     */
    public static ExtractedKnowledge dropInvalidRelations(ExtractedKnowledge knowledge) {
        if (knowledge == null) {
            throw new IllegalArgumentException("Extractor returned null");
        }
        Set<String> localIds = new HashSet<>();
        for (ExtractedEntity entity : knowledge.entities()) {
            if (!entity.localId().isBlank()) {
                localIds.add(entity.localId());
            }
        }
        List<ExtractedRelation> kept = knowledge.relations().stream()
                .filter(relation -> !relation.source().equals(relation.target()))
                .filter(relation -> localIds.contains(relation.source())
                        && localIds.contains(relation.target()))
                .toList();
        return new ExtractedKnowledge(knowledge.entities(), kept);
    }

    /**
     * Repairs a common small-model schema error only when an endpoint name or
     * alias maps to exactly one declared entity. Ambiguous values remain invalid.
     */
    public static ExtractedKnowledge repairUnambiguousEndpoints(ExtractedKnowledge knowledge) {
        Map<String, String> names = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();
        for (ExtractedEntity entity : knowledge.entities()) {
            register(names, ambiguous, entity.name(), entity.localId());
            for (String alias : entity.aliases()) {
                register(names, ambiguous, alias, entity.localId());
            }
        }
        var relations = knowledge.relations().stream()
                .map(relation -> new ExtractedRelation(
                        repair(relation.source(), names, ambiguous),
                        relation.relation(),
                        repair(relation.target(), names, ambiguous),
                        relation.evidence()))
                .toList();
        return new ExtractedKnowledge(knowledge.entities(), relations);
    }

    /**
     * Converts otherwise undeclared literal/concept endpoints into explicit
     * entities. This is applied only after a corrective model request failed
     * to use declared IDs.
     */
    public static ExtractedKnowledge materializeMissingEndpoints(ExtractedKnowledge knowledge) {
        List<ExtractedEntity> entities = new ArrayList<>(knowledge.entities());
        Set<String> ids = new HashSet<>();
        entities.forEach(entity -> ids.add(entity.localId()));
        Map<String, String> generated = new HashMap<>();
        List<ExtractedRelation> relations = new ArrayList<>();
        for (ExtractedRelation relation : knowledge.relations()) {
            String source = materialize(relation.source(), ids, generated, entities);
            String target = materialize(relation.target(), ids, generated, entities);
            relations.add(new ExtractedRelation(
                    source, relation.relation(), target, relation.evidence()));
        }
        return new ExtractedKnowledge(entities, relations);
    }

    private static void register(
            Map<String, String> names, Set<String> ambiguous, String value, String localId) {
        String key = normalize(value);
        String previous = names.putIfAbsent(key, localId);
        if (previous != null && !previous.equals(localId)) {
            ambiguous.add(key);
        }
    }

    private static String repair(
            String endpoint, Map<String, String> names, Set<String> ambiguous) {
        String key = normalize(endpoint);
        return ambiguous.contains(key) ? endpoint : names.getOrDefault(key, endpoint);
    }

    private static String normalize(String value) {
        return Normalizer.normalize(value == null ? "" : value, Normalizer.Form.NFKC)
                .toLowerCase(Locale.ROOT).trim();
    }

    private static String materialize(
            String endpoint,
            Set<String> ids,
            Map<String, String> generated,
            List<ExtractedEntity> entities) {
        if (ids.contains(endpoint)) {
            return endpoint;
        }
        if (endpoint == null || endpoint.isBlank()) {
            return endpoint;
        }
        String key = normalize(endpoint);
        return generated.computeIfAbsent(key, ignored -> {
            String localId = "auto-" + UUID.nameUUIDFromBytes(
                    key.getBytes(StandardCharsets.UTF_8));
            entities.add(new ExtractedEntity(
                    localId, endpoint.trim(), "concept", List.of()));
            ids.add(localId);
            return localId;
        });
    }
}
