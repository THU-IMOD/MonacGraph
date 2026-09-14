package db.monacgraph.retrieval.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.retrieval.model.RetrievalModels.ContentElementLink;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRef;
import db.monacgraph.retrieval.model.RetrievalModels.EmbeddingSpace;
import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.retrieval.storage.ElementIdCodec;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Embedded BM25 and HNSW index over passage-sized content records. */
public final class LuceneContentIndex implements ContentIndex {
    private static final String ID = "content_id";
    private static final String TEXT = "text";
    private static final String SOURCE = "source_field";
    private static final String LINKS = "links";
    private static final String ELEMENT = "element";
    private static final String SPACE = "space_id";
    private static final String VECTOR = "embedding";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Directory directory;
    private final Analyzer analyzer;
    private final IndexWriter writer;

    public LuceneContentIndex(Path path) {
        try {
            this.directory = FSDirectory.open(path);
            this.analyzer = new SmartChineseAnalyzer();
            this.writer = new IndexWriter(directory,
                    new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND));
        } catch (IOException e) {
            throw failure("open Lucene content index", e);
        }
    }

    @Override
    public synchronized void indexText(ContentRecord content) {
        update(content, null, null);
    }

    @Override
    public synchronized void indexVector(ContentRecord content, EmbeddingSpace space, float[] vector) {
        if (vector.length != space.dimension()) {
            throw new IllegalArgumentException(
                    "Expected " + space.dimension() + " dimensions, got " + vector.length);
        }
        update(content, space, vector);
    }

    private void update(ContentRecord content, EmbeddingSpace space, float[] vector) {
        try {
            Document document = new Document();
            document.add(new StringField(ID, content.contentId(), Field.Store.YES));
            document.add(new TextField(TEXT, content.text(), Field.Store.YES));
            document.add(new StoredField(SOURCE, nullToEmpty(content.sourceField())));
            document.add(new StoredField(LINKS, mapper.writeValueAsString(content.links())));
            for (ContentElementLink link : content.links()) {
                document.add(new StringField(ELEMENT, ElementIdCodec.encode(link.element()), Field.Store.NO));
            }
            if (space != null && vector != null) {
                document.add(new StringField(SPACE, space.spaceId(), Field.Store.YES));
                document.add(new KnnFloatVectorField(VECTOR, vector));
            }
            writer.updateDocument(new Term(ID, content.contentId()), document);
            writer.commit();
        } catch (IOException e) {
            throw failure("update Lucene content " + content.contentId(), e);
        }
    }

    @Override
    public List<ContentCandidate> textSearch(String queryText, int topK) {
        if (topK <= 0) {
            return List.of();
        }
        try {
            Query query = new QueryParser(TEXT, analyzer).parse(QueryParser.escape(queryText));
            return search(query, topK);
        } catch (Exception e) {
            throw failure("run BM25 search", e);
        }
    }

    @Override
    public List<ContentCandidate> vectorSearch(float[] queryVector, EmbeddingSpace space, int topK) {
        if (queryVector.length != space.dimension()) {
            throw new IllegalArgumentException(
                    "Expected " + space.dimension() + " dimensions, got " + queryVector.length);
        }
        if (topK <= 0) {
            return List.of();
        }
        Query filter = new org.apache.lucene.search.TermQuery(new Term(SPACE, space.spaceId()));
        return search(new KnnFloatVectorQuery(VECTOR, queryVector, topK, filter), topK);
    }

    private List<ContentCandidate> search(Query query, int topK) {
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(query, topK);
            List<ContentCandidate> results = new ArrayList<>(hits.scoreDocs.length);
            int rank = 1;
            for (ScoreDoc hit : hits.scoreDocs) {
                Document document = searcher.storedFields().document(hit.doc);
                List<ContentElementLink> links = mapper.readValue(
                        document.get(LINKS), new TypeReference<>() {});
                List<GraphElementRef> elements = links.stream()
                        .map(ContentElementLink::element)
                        .distinct()
                        .toList();
                String text = document.get(TEXT);
                ContentRef ref = new ContentRef(document.get(ID), elements, document.get(SOURCE));
                results.add(new ContentCandidate(ref, hit.score, rank++, excerpt(text)));
            }
            return results;
        } catch (IOException e) {
            throw failure("read Lucene search results", e);
        }
    }

    @Override
    public synchronized void delete(String contentId) {
        try {
            writer.deleteDocuments(new Term(ID, contentId));
            writer.commit();
        } catch (IOException e) {
            throw failure("delete Lucene content " + contentId, e);
        }
    }

    private static String excerpt(String text) {
        int max = 240;
        return text.length() <= max ? text : text.substring(0, max) + "…";
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static IllegalStateException failure(String operation, Exception cause) {
        return new IllegalStateException("Failed to " + operation, cause);
    }

    @Override
    public void close() {
        try {
            writer.close();
            analyzer.close();
            directory.close();
        } catch (IOException e) {
            throw failure("close Lucene content index", e);
        }
    }
}
