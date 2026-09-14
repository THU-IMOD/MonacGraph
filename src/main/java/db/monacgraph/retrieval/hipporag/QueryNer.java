package db.monacgraph.retrieval.hipporag;

import java.util.List;

public interface QueryNer {
    List<String> namedEntities(String query);
}
