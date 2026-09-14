package db.monacgraph.retrieval.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Persistent and transport models shared by retrieval storage and indexes.
 */
public final class RetrievalModels {
    private RetrievalModels() {}

    public enum ElementKind { VERTEX, EDGE }

    public enum LinkRole { MENTIONS, SUPPORTS }

    public enum EmbeddingStatus { PENDING, READY, FAILED }

    public sealed interface RetrievalItemRef permits GraphElementRef, ContentRef {}

    public record GraphElementRef(ElementKind kind, Object id, String label)
            implements RetrievalItemRef {
        public GraphElementRef {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(id, "id");
        }
    }

    public record ContentElementLink(GraphElementRef element, LinkRole role) {
        public ContentElementLink {
            Objects.requireNonNull(element, "element");
            Objects.requireNonNull(role, "role");
        }
    }

    public record ContentRecord(
            String contentId,
            String documentId,
            String text,
            String sourceField,
            String contentHash,
            long version,
            List<ContentElementLink> links) {
        public ContentRecord {
            Objects.requireNonNull(contentId, "contentId");
            Objects.requireNonNull(text, "text");
            links = links == null ? List.of() : List.copyOf(links);
            contentHash = contentHash == null ? sha256(text) : contentHash;
        }
    }

    public record ContentRef(
            String contentId,
            List<GraphElementRef> linkedElements,
            String sourceField) implements RetrievalItemRef {
        public ContentRef {
            Objects.requireNonNull(contentId, "contentId");
            linkedElements = linkedElements == null ? List.of() : List.copyOf(linkedElements);
        }
    }

    public record ContentCandidate(ContentRef content, float score, int rank, String matchedText) {}

    public record EmbeddingSpace(
            String spaceId,
            String modelName,
            String modelRevision,
            int dimension,
            boolean normalized,
            String queryPromptTemplate) {
        public EmbeddingSpace {
            Objects.requireNonNull(spaceId, "spaceId");
            Objects.requireNonNull(modelName, "modelName");
            modelRevision = modelRevision == null ? "main" : modelRevision;
            queryPromptTemplate = queryPromptTemplate == null ? "" : queryPromptTemplate;
            if (dimension <= 0) {
                throw new IllegalArgumentException("dimension must be positive");
            }
        }
    }

    public record EmbeddingJob(
            String contentId,
            String spaceId,
            String contentHash,
            EmbeddingStatus status,
            String error) {}

    private static String sha256(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
