package db.monacgraph.retrieval.storage;

import db.monacgraph.retrieval.model.RetrievalModels.GraphElementRef;
import db.monacgraph.serialize.IdCodec;

import java.util.Base64;

/** Produces stable retrieval-index keys from external TinkerPop element IDs. */
public final class ElementIdCodec {
    private ElementIdCodec() {}

    public static String encode(GraphElementRef element) {
        String id = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(IdCodec.toBytes(element.id()));
        return element.kind().name().charAt(0) + ":" + id;
    }
}
