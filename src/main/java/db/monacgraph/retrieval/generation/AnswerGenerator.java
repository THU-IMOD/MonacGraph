package db.monacgraph.retrieval.generation;

import db.monacgraph.retrieval.model.PipelineModels.AnswerResult;
import db.monacgraph.retrieval.model.PipelineModels.EvidenceResult;
import db.monacgraph.retrieval.model.PipelineModels.GenerationRequest;

public interface AnswerGenerator {
    AnswerResult generate(GenerationRequest request, EvidenceResult evidence);
}
