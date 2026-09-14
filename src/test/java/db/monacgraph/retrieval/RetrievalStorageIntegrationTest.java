package db.monacgraph.retrieval;

import db.monacgraph.retrieval.embedding.EmbeddingProvider;
import db.monacgraph.retrieval.index.LuceneContentIndex;
import db.monacgraph.retrieval.indexing.EmbeddingIndexer;
import db.monacgraph.retrieval.indexing.RetrievalIngestor;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ElementKind;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.model.RetrievalModels.LinkRole;
import db.monacgraph.retrieval.storage.RocksContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetrievalStorageIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void persistsLinksAndSupportsTextAndVectorRecall() {
        Path storePath = temporaryDirectory.resolve("content");
        Path indexPath = temporaryDirectory.resolve("lucene");
        GraphElementRef apple = new GraphElementRef(ElementKind.VERTEX, "apple", "company");
        ContentRecord first = content(
                "c1", "Apple was founded by Steve Jobs.",
                new ContentElementLink(apple, LinkRole.MENTIONS));
        ContentRecord second = content(
                "c2", "Bananas are a yellow fruit.",
                new ContentElementLink(
                        new GraphElementRef(ElementKind.VERTEX, "banana", "fruit"),
                        LinkRole.MENTIONS));
        EmbeddingSpace space = new EmbeddingSpace(
                "test-space", "fake", "1", 2, true, "");

        try (RocksContentStore store = new RocksContentStore(storePath);
             LuceneContentIndex index = new LuceneContentIndex(indexPath);
             EmbeddingProvider provider = new FakeEmbeddingProvider(space)) {
            RetrievalIngestor ingestor = new RetrievalIngestor(store, index);
            ingestor.ingest(first, space);
            ingestor.ingest(second, space);

            assertEquals(List.of("c1"), store.contentIdsForElement(apple));
            assertEquals("c1", index.textSearch("Apple", 1).get(0).content().contentId());

            EmbeddingIndexer indexer = new EmbeddingIndexer(store, index, provider);
            assertEquals(2, indexer.runBatch(10));
            List<ContentCandidate> vectorResults = index.vectorSearch(
                    provider.embedQuery("Apple"), space, 1);
            assertEquals("c1", vectorResults.get(0).content().contentId());

            ingestor.delete("c2");
            assertTrue(store.get("c2").isEmpty());
            assertTrue(index.textSearch("Bananas", 10).isEmpty());
        }

        try (RocksContentStore reopened = new RocksContentStore(storePath)) {
            assertTrue(reopened.get("c1").isPresent());
            assertEquals(List.of("c1"), reopened.contentIdsForElement(apple));
        }
    }

    private static ContentRecord content(
            String id, String text, ContentElementLink link) {
        return new ContentRecord(id, "doc", text, "body", null, 1, List.of(link));
    }

    private static final class FakeEmbeddingProvider implements EmbeddingProvider {
        private final EmbeddingSpace space;

        private FakeEmbeddingProvider(EmbeddingSpace space) {
            this.space = space;
        }

        @Override
        public EmbeddingSpace space() {
            return space;
        }

        @Override
        public List<float[]> embedDocuments(List<String> texts) {
            return texts.stream().map(FakeEmbeddingProvider::vector).toList();
        }

        @Override
        public float[] embedQuery(String query) {
            return vector(query);
        }

        private static float[] vector(String text) {
            return text.toLowerCase().contains("apple")
                    ? new float[]{1, 0}
                    : new float[]{0, 1};
        }
    }
}
