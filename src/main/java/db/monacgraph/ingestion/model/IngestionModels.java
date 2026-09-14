package db.monacgraph.ingestion.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class IngestionModels {
    private IngestionModels() {}

    public enum IngestionStatus {
        PENDING,
        PARSED,
        CHUNKED,
        EXTRACTED,
        GRAPH_WRITTEN,
        INDEXED,
        READY,
        FAILED
    }

    public record ParsedDocument(
            String documentId,
            String title,
            String text,
            String mediaType,
            Map<String, String> metadata) {
        public ParsedDocument {
            Objects.requireNonNull(documentId, "documentId");
            Objects.requireNonNull(text, "text");
            metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }
    }

    public record Passage(
            String contentId,
            String documentId,
            String text,
            int startOffset,
            int endOffset,
            String sectionPath) {
        public Passage {
            Objects.requireNonNull(contentId, "contentId");
            Objects.requireNonNull(documentId, "documentId");
            Objects.requireNonNull(text, "text");
            if (startOffset < 0 || endOffset < startOffset) {
                throw new IllegalArgumentException("Invalid passage offsets");
            }
        }
    }

    public record ExtractedEntity(
            String localId,
            String name,
            String type,
            List<String> aliases,
            String description) {
        public ExtractedEntity {
            Objects.requireNonNull(localId, "localId");
            Objects.requireNonNull(name, "name");
            type = type == null || type.isBlank() ? "entity" : type;
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
            description = description == null ? "" : description;
        }

        public ExtractedEntity(String localId, String name, String type, List<String> aliases) {
            this(localId, name, type, aliases, "");
        }
    }

    public record ExtractedRelation(
            String source,
            String relation,
            String target,
            String evidence) {
        public ExtractedRelation {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(relation, "relation");
            Objects.requireNonNull(target, "target");
        }
    }

    public record ExtractedKnowledge(
            List<ExtractedEntity> entities,
            List<ExtractedRelation> relations) {
        public ExtractedKnowledge {
            entities = entities == null ? List.of() : List.copyOf(entities);
            relations = relations == null ? List.of() : List.copyOf(relations);
        }
    }

    public record PassageExtraction(Passage passage, ExtractedKnowledge knowledge) {
        public PassageExtraction {
            Objects.requireNonNull(passage, "passage");
            Objects.requireNonNull(knowledge, "knowledge");
        }
    }

    public record CanonicalEntity(
            String vertexId,
            String canonicalName,
            String type,
            List<String> aliases) {}

    public record IngestionJob(
            String jobId,
            String documentId,
            String sourcePath,
            String contentHash,
            IngestionStatus status,
            IngestionStatus checkpoint,
            int attempts,
            String error,
            long updatedAtEpochMillis) {
        public IngestionJob {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(documentId, "documentId");
            Objects.requireNonNull(sourcePath, "sourcePath");
            Objects.requireNonNull(contentHash, "contentHash");
            Objects.requireNonNull(status, "status");
            checkpoint = checkpoint == null ? IngestionStatus.PENDING : checkpoint;
        }
    }
}
