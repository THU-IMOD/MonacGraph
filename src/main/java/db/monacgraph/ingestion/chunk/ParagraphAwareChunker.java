package db.monacgraph.ingestion.chunk;

import db.monacgraph.ingestion.IngestionIds;
import db.monacgraph.ingestion.model.IngestionModels.ParsedDocument;
import db.monacgraph.ingestion.model.IngestionModels.Passage;

import java.util.ArrayList;
import java.util.List;

/**
 * Deterministic paragraph-aware character chunking. Character boundaries keep
 * passage IDs independent of a particular embedding model tokenizer.
 */
public final class ParagraphAwareChunker implements Chunker {
    private final int maxCharacters;
    private final int overlapCharacters;

    public ParagraphAwareChunker(int maxCharacters, int overlapCharacters) {
        if (maxCharacters < 128) {
            throw new IllegalArgumentException("maxCharacters must be at least 128");
        }
        if (overlapCharacters < 0 || overlapCharacters >= maxCharacters / 2) {
            throw new IllegalArgumentException("overlapCharacters must be between 0 and half the chunk");
        }
        this.maxCharacters = maxCharacters;
        this.overlapCharacters = overlapCharacters;
    }

    @Override
    public List<Passage> split(ParsedDocument document) {
        String text = document.text();
        if (text.isBlank()) {
            return List.of();
        }
        List<Passage> passages = new ArrayList<>();
        int start = 0;
        while (start < text.length()) {
            while (start < text.length() && Character.isWhitespace(text.charAt(start))) {
                start++;
            }
            if (start >= text.length()) {
                break;
            }
            int hardEnd = Math.min(text.length(), start + maxCharacters);
            int end = chooseBoundary(text, start, hardEnd);
            while (end > start && Character.isWhitespace(text.charAt(end - 1))) {
                end--;
            }
            String passageText = text.substring(start, end);
            String contentId = IngestionIds.deterministicUuid(
                    "passage",
                    document.documentId() + ":" + start + ":" + end + ":" + IngestionIds.sha256(passageText));
            passages.add(new Passage(
                    contentId,
                    document.documentId(),
                    passageText,
                    start,
                    end,
                    nearestHeading(text, start, document.title())));
            if (end >= text.length()) {
                break;
            }
            int next = Math.max(start + 1, end - overlapCharacters);
            start = next;
        }
        return List.copyOf(passages);
    }

    private int chooseBoundary(String text, int start, int hardEnd) {
        if (hardEnd == text.length()) {
            return hardEnd;
        }
        int minimum = start + maxCharacters / 2;
        int paragraph = text.lastIndexOf("\n\n", hardEnd);
        if (paragraph >= minimum) {
            return paragraph;
        }
        int newline = text.lastIndexOf('\n', hardEnd);
        if (newline >= minimum) {
            return newline;
        }
        return hardEnd;
    }

    private static String nearestHeading(String text, int offset, String fallback) {
        int cursor = Math.min(offset, text.length());
        while (cursor > 0) {
            int lineEnd = cursor;
            int lineStart = text.lastIndexOf('\n', Math.max(0, lineEnd - 1)) + 1;
            String line = text.substring(lineStart, lineEnd).trim();
            if (isHeading(line)) {
                return line;
            }
            cursor = Math.max(0, lineStart - 1);
        }
        return fallback;
    }

    private static boolean isHeading(String line) {
        return !line.isBlank() && line.length() <= 100
                && (line.startsWith("#")
                || line.matches("^\\d+(\\.\\d+)*[、.\\s].+")
                || line.matches("^[一二三四五六七八九十]+[、.].+"));
    }
}
