package db.monacgraph.ingestion.document;

import db.monacgraph.ingestion.IngestionIds;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Parses common document formats through Apache Tika. */
public final class TikaDocumentParser implements DocumentParser {
    private final Tika tika;

    public TikaDocumentParser() {
        this.tika = new Tika();
        this.tika.setMaxStringLength(-1);
    }

    @Override
    public ParsedDocument parse(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized)) {
            throw new IllegalArgumentException("Document does not exist: " + normalized);
        }
        try (InputStream input = Files.newInputStream(normalized)) {
            Metadata metadata = new Metadata();
            metadata.set("resourceName", normalized.getFileName().toString());
            String text = tika.parseToString(input, metadata);
            String contentHash = IngestionIds.sha256(text);
            String documentId = IngestionIds.deterministicUuid(
                    "document", normalized + ":" + contentHash);
            Map<String, String> values = Arrays.stream(metadata.names())
                    .collect(Collectors.toUnmodifiableMap(
                            name -> name,
                            name -> String.join(", ", metadata.getValues(name)),
                            (left, right) -> left));
            String title = firstNonBlank(metadata.get("title"), normalized.getFileName().toString());
            return new ParsedDocument(
                    documentId, title, normalizeNewlines(text),
                    firstNonBlank(metadata.get("Content-Type"), "application/octet-stream"), values);
        } catch (IOException | org.apache.tika.exception.TikaException e) {
            throw new IllegalStateException("Failed to parse document " + normalized, e);
        }
    }

    private static String normalizeNewlines(String value) {
        return value.replace("\r\n", "\n").replace('\r', '\n').trim();
    }

    private static String firstNonBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
