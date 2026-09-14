package db.monacgraph.ingestion;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

public final class IngestionIds {
    private IngestionIds() {}

    public static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public static String deterministicUuid(String namespace, String value) {
        return UUID.nameUUIDFromBytes(
                (namespace + ":" + value).getBytes(StandardCharsets.UTF_8)).toString();
    }
}
