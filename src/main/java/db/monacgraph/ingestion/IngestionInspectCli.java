package db.monacgraph.ingestion;

import db.monacgraph.ingestion.job.RocksIngestionJobStore;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;
import db.monacgraph.ingestion.model.IngestionModels.PassageExtraction;
import db.monacgraph.retrieval.model.RetrievalModels.ContentRecord;
import db.monacgraph.retrieval.storage.RocksContentStore;
import db.monacgraph.runtime.LocalRuntime;

import java.nio.file.Path;

/** Prints persisted extraction and content links without opening the JNI graph. */
public final class IngestionInspectCli {
    private IngestionInspectCli() {}

    public static void main(String[] args) {
        LocalRuntime.install();
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage: IngestionInspectCli <workspace-name> <job-id> [content-id]");
        }
        Path retrieval = Path.of("workspace", args[0], "retrieval");
        try (RocksIngestionJobStore jobs = new RocksIngestionJobStore(retrieval.resolve("ingestion_jobs"));
             RocksContentStore contents = new RocksContentStore(retrieval.resolve("content_db"))) {
            System.out.println("workspace: " + args[0]);
            jobs.get(args[1]).ifPresent(job ->
                    System.out.println("job: " + job.status() + " " + job.jobId()));
            for (PassageExtraction extraction : jobs.extractions(args[1])) {
                System.out.println();
                System.out.println("[entities]");
                for (ExtractedEntity entity : extraction.knowledge().entities()) {
                    System.out.println(entity.localId() + "\t" + entity.type() + "\t" + entity.name()
                            + (entity.aliases().isEmpty() ? "" : "\t" + entity.aliases()));
                }
                System.out.println();
                System.out.println("[relations]");
                for (ExtractedRelation relation : extraction.knowledge().relations()) {
                    System.out.println(relation.source() + " -[" + relation.relation() + "]-> "
                            + relation.target()
                            + (relation.evidence() == null || relation.evidence().isBlank()
                            ? "" : "\t" + relation.evidence()));
                }
            }
            if (args.length >= 3) {
                contents.get(args[2]).ifPresent(IngestionInspectCli::printContent);
            }
        }
    }

    private static void printContent(ContentRecord content) {
        System.out.println();
        System.out.println("[content links] " + content.contentId());
        content.links().forEach(link -> System.out.println(
                link.role() + "\t" + link.element().kind()
                        + "\t" + link.element().label()
                        + "\t" + link.element().id()));
    }
}
