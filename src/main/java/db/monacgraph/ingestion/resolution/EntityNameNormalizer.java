package db.monacgraph.ingestion.resolution;

import java.text.Normalizer;
import java.util.Locale;

public final class EntityNameNormalizer {
    private EntityNameNormalizer() {}

    public static String normalize(String value) {
        if (value == null) {
            return "";
        }
        return Normalizer.normalize(value, Normalizer.Form.NFKC)
                .toLowerCase(Locale.ROOT)
                .replaceAll("[\\p{Punct}\\p{P}]+", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }
}
