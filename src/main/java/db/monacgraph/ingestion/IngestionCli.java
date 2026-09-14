package db.monacgraph.ingestion;

import db.monacgraph.community.CommunityGraph;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedEntity;
import db.monacgraph.ingestion.model.IngestionModels.ExtractedRelation;
import db.monacgraph.ingestion.model.IngestionModels.IngestionJob;
import db.monacgraph.ingestion.model.IngestionModels.IngestionStatus;
import db.monacgraph.ingestion.model.IngestionModels.PassageExtraction;
import db.monacgraph.retrieval.model.PipelineModels.AnswerResult;
import db.monacgraph.retrieval.model.PipelineModels.ContentEvidence;
import db.monacgraph.retrieval.model.RetrievalModels.ContentCandidate;
import db.monacgraph.runtime.LocalRuntime;
import org.apache.commons.configuration2.BaseConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Batch document ingestion without changing the existing MainTest entry point. */
public final class IngestionCli {
    private IngestionCli() {}

    public static void main(String[] args) {
        LocalRuntime.install();
        Map<String, String> options = parseArgs(args);
        if (!options.containsKey("db") || !options.containsKey("input")) {
            throw new IllegalArgumentException(
                    "Usage: IngestionCli --db <name> --input <file-or-directory> "
                            + "[--recursive] [--extractor-url <url>] [--extractor-model <model>] "
                            + "[--embedding-uri <url>] [--query <question>] [--output-dir <dir>]");
        }

        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("db.name", options.get("db"));
        configuration.setProperty("ingestion.worker.enabled", false);
        put(configuration, options, "extractor-url", "ingestion.extractor.url");
        put(configuration, options, "extractor-model", "ingestion.extractor.model");
        put(configuration, options, "embedding-uri", "retrieval.embedding.uri");
        put(configuration, options, "api-key-env", "ingestion.extractor.apiKeyEnv");

        boolean failed = false;
        List<String> jobIds = new ArrayList<>();
        Path input = Path.of(options.get("input"));
        Path outputDir = Path.of(options.getOrDefault("output-dir", defaultOutputDir(input)));
        try (CommunityGraph graph = CommunityGraph.open(configuration)) {
            IngestionCoordinator coordinator = graph.ingestion();
            for (Path file : inputFiles(input, options.containsKey("recursive"))) {
                IngestionJob submitted = coordinator.submit(file);
                IngestionJob result = coordinator.run(submitted.jobId());
                jobIds.add(result.jobId());
                System.out.printf("%s\t%s\t%s%n",
                        result.status(), result.jobId(), file.toAbsolutePath());
                if (result.status() != IngestionStatus.READY) {
                    failed = true;
                    if (result.error() != null) {
                        System.err.println("  " + result.error());
                    }
                }
            }
            if (!failed) {
                writeText(outputDir.resolve("graph.txt"), renderGraph(coordinator, jobIds));
                System.out.println("wrote " + outputDir.resolve("graph.txt").toAbsolutePath());
            }
            if (options.containsKey("query") && !failed) {
                String question = options.get("query");
                List<ContentCandidate> bm25 = graph.traversal().retrieve(question)
                        .textRecall().topK(10).executeRecall();
                List<ContentCandidate> vectors = graph.traversal().retrieve(question)
                        .vectorRecall().topK(10).executeRecall();
                printCandidates("BM25", bm25);
                printCandidates("VECTOR", vectors);
                AnswerResult result = graph.traversal().retrieve(question)
                        .hippoRag()
                        .topK(10)
                        .budget()
                        .evidence()
                        .generate()
                        .execute();
                System.out.println("[HIPPORAG]");
                for (ContentEvidence content : result.evidence().contents()) {
                    System.out.printf("%s\t%s%n", content.sourceField(), content.contentId());
                }
                System.out.println("[ANSWER]");
                System.out.println(result.answer());
                if (result.generation() != null && result.generation().finishReason() != null) {
                    System.out.println("[GENERATION] " + result.generation().finishReason());
                }
                writeText(outputDir.resolve("answer.txt"),
                        "question: " + question + System.lineSeparator()
                                + "pipeline: HippoRAG entityLink → ppr → budget → evidence → generate"
                                + System.lineSeparator()
                                + "answer: " + result.answer() + System.lineSeparator());
                System.out.println("wrote " + outputDir.resolve("answer.txt").toAbsolutePath());
            }
        }
        if (failed) {
            throw new IllegalStateException("One or more documents failed ingestion");
        }
    }

    private static List<Path> inputFiles(Path input, boolean recursive) {
        if (Files.isRegularFile(input)) {
            return List.of(input);
        }
        if (!Files.isDirectory(input)) {
            throw new IllegalArgumentException("Input does not exist: " + input);
        }
        try (Stream<Path> stream = recursive ? Files.walk(input) : Files.list(input)) {
            return stream.filter(Files::isRegularFile).sorted().toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to enumerate input " + input, e);
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        List<String> flags = new ArrayList<>(List.of("recursive"));
        for (int i = 0; i < args.length; i++) {
            String argument = args[i];
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + argument);
            }
            String key = argument.substring(2);
            if (flags.contains(key)) {
                options.put(key, "true");
            } else {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Missing value for --" + key);
                }
                options.put(key, args[i]);
            }
        }
        return options;
    }

    private static void put(
            BaseConfiguration configuration,
            Map<String, String> options,
            String option,
            String property) {
        if (options.containsKey(option)) {
            configuration.setProperty(property, options.get(option));
        }
    }

    private static String defaultOutputDir(Path input) {
        Path normalized = input.toAbsolutePath().normalize();
        return Files.isDirectory(normalized)
                ? normalized.toString()
                : normalized.getParent().toString();
    }

    private static String renderGraph(IngestionCoordinator coordinator, List<String> jobIds) {
        StringBuilder out = new StringBuilder();
        List<ExtractedEntity> entities = new ArrayList<>();
        List<ExtractedRelation> relations = new ArrayList<>();
        for (String jobId : jobIds) {
            for (PassageExtraction extraction : coordinator.extractions(jobId)) {
                entities.addAll(extraction.knowledge().entities());
                relations.addAll(extraction.knowledge().relations());
            }
        }
        out.append("# extracted graph").append(System.lineSeparator());
        out.append(System.lineSeparator()).append("[entities]").append(System.lineSeparator());
        for (ExtractedEntity entity : entities) {
            out.append(entity.localId()).append('\t')
                    .append(entity.type()).append('\t')
                    .append(entity.name());
            if (!entity.aliases().isEmpty()) {
                out.append('\t').append(entity.aliases());
            }
            if (entity.description() != null && !entity.description().isBlank()) {
                out.append('\t').append(entity.description());
            }
            out.append(System.lineSeparator());
        }
        out.append(System.lineSeparator()).append("[relations]").append(System.lineSeparator());
        for (ExtractedRelation relation : relations) {
            out.append(relation.source()).append(" -[")
                    .append(relation.relation()).append("]-> ")
                    .append(relation.target());
            if (relation.evidence() != null && !relation.evidence().isBlank()) {
                out.append('\t').append(relation.evidence());
            }
            out.append(System.lineSeparator());
        }
        out.append(System.lineSeparator()).append("[mermaid]").append(System.lineSeparator());
        out.append("graph LR").append(System.lineSeparator());
        for (ExtractedEntity entity : entities) {
            out.append("  ").append(safeId(entity.localId()))
                    .append("[\"").append(escapeMermaid(entity.name()))
                    .append(" (").append(escapeMermaid(entity.type())).append(")\"]")
                    .append(System.lineSeparator());
        }
        for (ExtractedRelation relation : relations) {
            out.append("  ").append(safeId(relation.source()))
                    .append(" -->|").append(escapeMermaid(relation.relation())).append("| ")
                    .append(safeId(relation.target()))
                    .append(System.lineSeparator());
        }
        return out.toString();
    }

    private static String safeId(String localId) {
        return localId.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String escapeMermaid(String value) {
        return value.replace("\"", "'").replace("]", "\\]").replace("|", "/");
    }

    private static void writeText(Path path, String text) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private static void printCandidates(String channel, List<ContentCandidate> candidates) {
        System.out.println("[" + channel + "]");
        for (ContentCandidate candidate : candidates) {
            System.out.printf("%d\t%.5f\t%s\t%s%n",
                    candidate.rank(),
                    candidate.score(),
                    candidate.content().sourceField(),
                    candidate.content().contentId());
        }
    }
}
