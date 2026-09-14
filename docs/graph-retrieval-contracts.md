# MonacGraph 接口与类型契约

本文是**阶段输入/输出、标识空间和公开 Java 类型**的权威定义。算子动机、论文对照与实施优先级见：

- [`graph-retrieval-recall-expand.md`](graph-retrieval-recall-expand.md)
- [`graph-retrieval-storage-embedding.md`](graph-retrieval-storage-embedding.md)

实现、评测或 Fluent API 与本文冲突时，以本文为准，并同步改代码与上述两份设计。新增算子必须先补本文件的 I/O 行，再写实现。

---

## 0. 两条流水线

系统只有两条可执行流水线。它们通过共享标识对接，**不得**另开第三套 ID。

```text
Ingestion (write)
  Path → ParsedDocument → List<Passage> → ExtractedKnowledge
       → GraphElementRef + ContentRecord → Lucene / EmbeddingJob
       → IngestionJob.READY

Retrieval (read)
  Query → Recall → Expand → [Prune] → [Fusion] → Budget
       → EvidenceResult → Generation → AnswerResult
```

`EvidenceResult` 是中间结果，不是第二种运行模式。`execute()` 的默认返回类型是 `AnswerResult`。

摄取不是 Gremlin 算子。公开入口：

```java
IngestionJob job = graph.ingestion().submit(path);
IngestionJob done = graph.ingestion().run(job.jobId());
```

检索公开入口：

```java
AnswerResult answer = graph.traversal().retrieve(query)
    /* Recall / Expand / … */
    .execute();
```

---

## 1. 标识空间

| 标识 | 类型 | 作用域 | 生成规则 | 禁止 |
|---|---|---|---|---|
| `documentId` | `String` (UUID) | 一篇源文件 | `nameUUID("document", absPath + ":" + fileHash)` | 用路径当主键 |
| `jobId` | `String` (UUID) | 一次摄取任务 | `nameUUID("ingestion-job", documentId)` | 每次运行重新生成 |
| `contentId` | `String` (UUID) | 一个 passage | `nameUUID("passage", documentId + ":" + start + ":" + end + ":" + sha256(text))` | 用 Lucene docId / 模型 token 边界 |
| `localId` | `String` | **单个 passage 抽取结果内** | extractor 分配（`e1`, `e2`…） | 写入图、写入 `GraphElementRef.id`、跨 passage 引用 |
| `vertexId` | `String` (UUID) | 规范实体 | `nameUUID("vertex", normType + "\0" + normName)` | LSM-Community 内部 `u32` handle |
| `GraphElementRef.id` | `Object` | TinkerPop 外部 ID | 顶点 = `vertexId`；边 = 图写入返回的外部 ID | 检索索引持久化内部 handle |
| `spaceId` | `String` | 一个向量空间 | 模型名 + revision + 维度 + 归一化 + query prompt 的注册名 | 跨 `spaceId` 比向量 |

`contentId` 与 `GraphElementRef.id` 是摄取下游和检索上游的**唯二**桥接键。

反向索引必须同时存在：

```text
contentId → ContentRecord
GraphElementRef → contentId[]
contentId + spaceId → EmbeddingJob
```

---

## 2. 摄取契约

### 2.1 阶段与类型

状态机只允许向前（或进入 `FAILED`）。`run()` 从 `checkpoint` 恢复，不得跳过未完成阶段。

| 阶段 | `IngestionStatus` | 输入 | 输出 | 失败 |
|---|---|---|---|---|
| submit | `PENDING` | `Path` | `IngestionJob` | 源文件不可读 |
| parse | `PARSED` | `Path` | `ParsedDocument` | 解析器空正文 |
| chunk | `CHUNKED` | `ParsedDocument` | `List<Passage>` | 空切片（空白文档） |
| extract | `EXTRACTED` | `Passage` | `PassageExtraction` | extractor 协议错误（校验失败） |
| graph | `GRAPH_WRITTEN` | `Passage + ExtractedKnowledge` | `ContentRecord` | 图写入 / 未知 `localId` |
| index | `INDEXED` | `ContentRecord` | BM25 文档 + `EmbeddingJob(PENDING)` | Lucene 写失败（RocksDB 记录保留） |
| embed | `READY` | `EmbeddingJob(PENDING)` | 向量 + `READY` | embedding 失败 → job `FAILED`，可重入队 |

`submit` 只登记任务，不读模型、不写图。`run` 才执行。

### 2.2 摄取类型

与 `db.monacgraph.ingestion.model.IngestionModels` 一致。

```java
enum IngestionStatus {
    PENDING, PARSED, CHUNKED, EXTRACTED, GRAPH_WRITTEN, INDEXED, READY, FAILED
}

record ParsedDocument(
    String documentId,
    String title,
    String text,                 // 必填，非 null
    String mediaType,
    Map<String, String> metadata
) {}

record Passage(
    String contentId,
    String documentId,           // == ParsedDocument.documentId
    String text,                 // == ParsedDocument.text[startOffset, endOffset)
    int startOffset,             // >= 0
    int endOffset,               // >= startOffset
    String sectionPath
) {}

record ExtractedEntity(
    String localId,              // passage 内唯一
    String name,                 // 非空
    String type,                 // 缺省 "entity"；不得由 prompt 枚举固定类型表
    List<String> aliases,
    String description           // 属性写这里，不另造值节点
) {}

record ExtractedRelation(
    String source,               // 必须是同 passage 的 localId
    String relation,             // 非空
    String target,               // != source，且必须是同 passage 的 localId
    String evidence
) {}

record ExtractedKnowledge(
    List<ExtractedEntity> entities,
    List<ExtractedRelation> relations
) {}

record PassageExtraction(Passage passage, ExtractedKnowledge knowledge) {}

record CanonicalEntity(
    String vertexId,
    String canonicalName,
    String type,
    List<String> aliases
) {}

record IngestionJob(
    String jobId,
    String documentId,
    String sourcePath,
    String contentHash,
    IngestionStatus status,
    IngestionStatus checkpoint,
    int attempts,
    String error,
    long updatedAtEpochMillis
) {}
```

### 2.3 摄取接口

```java
interface DocumentParser {
    ParsedDocument parse(Path path);
}

interface Chunker {
    List<Passage> split(ParsedDocument document);
}

interface KnowledgeExtractor extends AutoCloseable {
    ExtractedKnowledge extract(Passage passage);
}

interface EntityResolver {
    CanonicalEntity resolve(ExtractedEntity entity);
}

interface GraphMutationTarget {
    GraphElementRef upsertVertex(CanonicalEntity entity);
    GraphElementRef upsertEdge(GraphElementRef source, String relation, GraphElementRef target);
}

final class ExtractedGraphWriter {
    ContentRecord write(Passage passage, ExtractedKnowledge knowledge);
}

interface IngestionJobStore extends AutoCloseable {
    void put(IngestionJob job);
    Optional<IngestionJob> get(String jobId);
    void delete(String jobId);
}

final class IngestionCoordinator {
    IngestionJob submit(Path source);
    Optional<IngestionJob> status(String jobId);
    IngestionJob run(String jobId);
    void delete(String jobId);   // 删 passage / 索引 / 任务产物，不删共享顶点边
}
```

`ExtractedGraphWriter.write` 是摄取与检索存储的**硬边界**：

| 抽取对象 | 图操作 | `ContentRecord.links` |
|---|---|---|
| `ExtractedEntity` | `resolve` → `upsertVertex` | `MENTIONS` + `GraphElementRef(VERTEX, vertexId, label)` |
| `ExtractedRelation` | `upsertEdge(src, relation, dst)` | `SUPPORTS` + `GraphElementRef(EDGE, edgeId, relation)` |

下游 Recall / Expand **只认** `ContentRecord` 与 `GraphElementRef`，不再读取 `localId` 或 `ExtractedKnowledge`。

### 2.4 抽取不变量

在写入图之前必须成立。`validate` 失败则整段抽取失败；`dropInvalidRelations` 只丢边、不丢实体。

1. `entities` 中 `localId`、`name` 非空；`localId` 不重复。
2. `relations.source != relations.target`（禁止自环）。
3. `source` / `target` 必须出现在**同一** `ExtractedKnowledge.entities`。
4. 属性与类型信息写入 `ExtractedEntity.description` / `type`，不得为了填槽位再造值节点。
5. 关系跨 passage 不在抽取阶段建立；跨文档合并只通过 `EntityResolver` 的规范名 / 无歧义别名。

### 2.5 切片不变量

1. `Passage.documentId == ParsedDocument.documentId`。
2. `Passage.text == document.text.substring(startOffset, endOffset)`。
3. `contentId` 由 `documentId + offsets + sha256(text)` 决定，与 embedding tokenizer 无关。
4. 同一文档、同一切片器参数下，重跑必须得到同一组 `contentId`。

---

## 3. 检索值类型

### 3.1 引用

与 `db.monacgraph.retrieval.model.RetrievalModels` 对齐；流水线层再包一层评分。

```java
enum ElementKind { VERTEX, EDGE }
enum LinkRole { MENTIONS, SUPPORTS }

sealed interface RetrievalItemRef permits GraphElementRef, ContentRef {}

record GraphElementRef(ElementKind kind, Object id, String label)
    implements RetrievalItemRef {}

record ContentRef(
    String contentId,
    List<GraphElementRef> linkedElements,  // 无 role；role 只存在于 ContentRecord
    String sourceField
) implements RetrievalItemRef {}

record ContentElementLink(GraphElementRef element, LinkRole role) {}

record ContentRecord(
    String contentId,
    String documentId,
    String text,                 // 正文只出现在存储层与 Evidence
    String sourceField,
    String contentHash,
    long version,
    List<ContentElementLink> links
) {}
```

规则：

- 中间候选**不**复制 `ContentRecord.text` 或完整图属性。
- `ContentRef.linkedElements` 可同时含顶点（MENTIONS）和边（SUPPORTS）。
- 图索引禁止持久化 LSM 内部 handle。

### 3.2 分数与来源

```java
enum ScoreSemantics {
    LINKING_SCORE, COSINE_SIMILARITY, BM25, PPR_PROBABILITY, PATH_RELEVANCE
}

record RetrievalScore(String name, double value, ScoreSemantics semantics) {}

record Provenance(
    String operator,
    List<Object> sourceIds,
    Map<String, Object> parameters
) {}
```

不同 `ScoreSemantics` 不得直接加减。Fusion / Rerank 必须先归一化或改用名次。

### 3.3 流水线值

```java
sealed interface RetrievalValue permits
    AnchorSet,
    CandidateSet,
    ExpandedVertexSet,
    PathSet,
    CandidateSubgraph {}

record QueryContext(String text, Optional<QueryEmbedding> embedding) {}
record QueryEmbedding(String spaceId, float[] values) {}
```

`QueryContext` 不是 `RetrievalValue`。它是整条流水线的只读查询上下文。

```java
record AnchorSet(List<AnchorMention> mentions) implements RetrievalValue {}

record AnchorMention(
    String surface,
    int startOffset,
    int endOffset,
    List<AnchorCandidate> candidates
) {}

record AnchorCandidate(
    GraphElementRef element,     // 必须 VERTEX
    List<RetrievalScore> scores,
    Provenance provenance
) {}

record CandidateSet<T extends RetrievalItemRef>(
    List<ScoredCandidate<T>> items
) implements RetrievalValue {}

record ScoredCandidate<T extends RetrievalItemRef>(
    T item,
    int rank,
    List<RetrievalScore> scores,
    Provenance provenance
) {}
```

`textRecall()` / `vectorRecall()` 的输出类型冻结为 **`CandidateSet<ContentRef>`**，不是 `String`，不是 `ContentRecord`。

当前实现的 `ContentCandidate` 是 Lucene 适配器，映射必须一一对应：

```java
record ContentCandidate(ContentRef content, float score, int rank, String matchedText) {}
```

| `ContentCandidate` | `ScoredCandidate<ContentRef>` |
|---|---|
| `content` | `item` |
| `score` | `scores[0].value`（BM25 或 COSINE_SIMILARITY） |
| `rank` | `rank` |
| `matchedText` | 高亮，不进入后续 Expand seed |

```java
record ExpandedVertexSet(List<ExpandedVertex> vertices) implements RetrievalValue {}

record ExpandedVertex(
    GraphElementRef vertex,      // kind == VERTEX
    int depth,
    List<TraversalRef> reachedBy,
    List<RetrievalScore> scores,
    Provenance provenance
) {}

record TraversalRef(Object parentVertexId, Object edgeId, Direction direction) {}

record PathSet(List<ScoredPath> paths) implements RetrievalValue {}

record ScoredPath(
    List<GraphElementRef> elements,  // V-E-V-… 交替；两端必须 VERTEX
    List<RetrievalScore> scores,
    Provenance provenance
) {}

record CandidateSubgraph(
    List<GraphElementRef> vertices,
    List<GraphElementRef> edges,
    List<RetrievalScore> scores,
    Provenance provenance
) implements RetrievalValue {}
```

`ExpandedVertexSet` 只保留实际走过的边（`TraversalRef`），**不是**诱导子图。诱导子图只允许由 `selectConnectedSubgraph` 产出 `CandidateSubgraph`。

### 3.4 Budget / Evidence / Answer

这三类不实现 `RetrievalValue`。Budget 之后禁止再跑 Recall / Expand。

```java
record BudgetSelection(
    List<ContentRef> contents,
    List<GraphElementRef> vertices,   // 仅 VERTEX
    List<GraphElementRef> edges,      // 仅 EDGE
    List<ScoredPath> paths,
    BudgetStats stats
) {}

record BudgetStats(
    int inputContents,
    int inputVertices,
    int inputEdges,
    int inputPaths,
    boolean truncated,
    int tokenEstimate
) {}

record EvidenceResult(
    String query,
    List<ContentEvidence> contents,
    List<VertexEvidence> vertices,
    List<EdgeEvidence> edges,
    List<PathEvidence> paths,
    String llmContext,
    List<Provenance> provenance,
    ExecutionTrace trace
) {}

record ContentEvidence(
    String contentId,
    String text,
    List<GraphElementRef> linkedElements,
    String sourceField,
    Provenance provenance
) {}

record GenerationRequest(
    String query,
    String evidenceContext,          // == EvidenceResult.llmContext
    List<EvidenceReference> references,
    GenerationOptions options
) {}

record AnswerResult(
    String answer,
    List<Citation> citations,
    EvidenceResult evidence,
    GenerationMetadata generation
) {}

record Citation(
    int answerStartOffset,
    int answerEndOffset,
    List<EvidenceReference> references
) {}
```

`EvidenceReference` 只能指向 `EvidenceResult` 里已有的 `contentId` / 顶点 id / 边 id / path 下标。Generation 不得发明图 ID。

---

## 4. 检索算子契约

### 4.1 执行模型

```java
interface RetrievalStep {
    String name();
    RetrievalType inputType();
    RetrievalType outputType();
    RetrievalValue execute(QueryContext query, RetrievalValue input);
}

RetrievalPipelineBuilder retrieve(String query);
RetrievalPipelineBuilder retrieve(QueryContext query);

AnswerResult execute();                 // 默认：跑到 Generation
<T> T executeTo(Class<T> stageType);    // 评测 / 调试，不改变默认语义
List<ContentCandidate> executeRecall(); // 仅当前过渡 API，见 §8
```

Builder 只追加 Step。Executor 按书写顺序执行：

```text
value = null
for step in steps:
    assertCompatible(step.inputType, value)
    value = step.execute(queryContext, value)
EvidenceResult evidence = evidence(queryContext, budget(value))
AnswerResult   answer   = generate(queryContext, evidence)
```

类型不兼容必须在执行前失败，禁止静默丢弃或隐式转换。

`retrieve(query)` 只创建 `QueryContext` 与空 Step 列表，不访问数据库。

### 4.2 Recall

| 算子 | 输入 | 输出 | 分数 |
|---|---|---|---|
| `entityLink()` | `null` + `QueryContext.text` | `AnchorSet` | `LINKING_SCORE` |
| `textRecall()` | `null` + `QueryContext.text` | `CandidateSet<ContentRef>` | `BM25` |
| `vectorRecall()` | `null` + `QueryContext.embedding` | `CandidateSet<ContentRef>` | `COSINE_SIMILARITY` |
| `exactRecall()` | `null` + 显式 ID | `CandidateSet<RetrievalItemRef>` | 无或常量 |

约束：

- `vectorRecall()` 的 `QueryEmbedding.spaceId` 必须等于索引 `EmbeddingSpace.spaceId`，否则立即失败。
- 仅 `entityLink()` / `textRecall()` 时 `embedding` 可为 empty。
- Recall 不得读图邻接；不得把 passage 当顶点返回。

### 4.3 Seed 投影

Expand / 部分 Budget 只认顶点。投影函数 `seeds(value) → Set<GraphElementRef VERTEX>` 冻结如下：

| 输入 | `seeds` |
|---|---|
| `AnchorSet` | 每个 `AnchorCandidate.element`（必须已是 VERTEX） |
| `CandidateSet<GraphElementRef>` | `kind == VERTEX` 的 item |
| `CandidateSet<ContentRef>` | 各 `linkedElements` 中 `kind == VERTEX` 的并集 |
| `ExpandedVertexSet` | 全部 `vertex` |
| `PathSet` | 每条路径两端顶点；路径算子另有约定时在算子行写明 |
| `CandidateSubgraph` | 全部顶点 |
| `BudgetSelection` / `EvidenceResult` / `AnswerResult` | **禁止**再 Expand |

`CandidateSet<ContentRef>` 中没有任何 `VERTEX` 关联时，`seeds` 为空。空 seed **禁止**进入任何 Expand，该集合只能进 Budget / Evidence。

边引用（`SUPPORTS`）不进入 `seeds`。

### 4.4 Expand

| 算子 | 允许输入 | 输出 | 拒绝 |
|---|---|---|---|
| `neighbors()` | `AnchorSet`；`CandidateSet<GraphElementRef>`；带 VERTEX 关联的 `CandidateSet<ContentRef>`；`ExpandedVertexSet` | `ExpandedVertexSet` | `seeds` 为空 |
| `boundedBfs()` | 同上 | `ExpandedVertexSet` | 同上；深度/节点/边预算 ≤ 0 |
| `ppr()` | 同上 | `CandidateSet<GraphElementRef>` | `seeds` 为空 |
| `constrainedPaths()` | `AnchorSet` | `PathSet` | mention 不足 2 个可解析锚点 |
| `shortestPaths()` | 两组可投影到顶点的 Recall 结果 | `PathSet` | 只有一组端点 |
| `beamPaths()` | 可投影到顶点的 Recall / Expand 结果 | `PathSet` | `seeds` 为空 |
| `randomWalk()` | 同上 | `PathSet` 或 `CandidateSet<GraphElementRef>` | `seeds` 为空 |

没有任何 Recall 结果（`value == null`）时禁止 Expand。

`ExpandedVertexSet` 不得补全未走过的边。

### 4.5 Prune

同型进出，禁止改类型。

| 算子 | 输入 | 输出 |
|---|---|---|
| `pruneCandidates()` | `CandidateSet<T>` 或 `ExpandedVertexSet` | 同类型 |
| `prunePaths()` | `PathSet` | `PathSet` |
| `selectConnectedSubgraph()` | `CandidateSet` / `ExpandedVertexSet` / `PathSet` | `CandidateSubgraph` |
| `knowledgeFilter()` | `CandidateSet` / `PathSet` / `CandidateSubgraph` | **同类型** |

`selectConnectedSubgraph` 是唯一允许 `* → CandidateSubgraph` 的 Prune。其余 Prune 不得把 `PathSet` 压扁成 `CandidateSet`。

### 4.6 Fusion

| 算子 | 输入 | 输出 | 拒绝 |
|---|---|---|---|
| `fuse()` | 两个及以上**相同** `RetrievalValue` 具体类型 | 同类型 | `CandidateSet<ContentRef>` 与 `CandidateSet<GraphElementRef>` 混融 |
| `rerank()` | `CandidateSet<T>` | `CandidateSet<T>` | 其他类型 |

Fusion 不改变 item 身份，只改 `rank` / `scores` / `provenance`。

### 4.7 Budget

| 输入 | `contents` | `vertices` | `edges` | `paths` |
|---|---|---|---|---|
| `CandidateSet<ContentRef>` | 全部 item | `seeds` + 关联边不自动入选 | `linkedElements` 中 EDGE | 空 |
| `CandidateSet<GraphElementRef>` | 可选：`contentIdsForElement` 回填 | VERTEX items | EDGE items | 空 |
| `AnchorSet` | 可选回填 | `seeds` | 空 | 空 |
| `ExpandedVertexSet` | 可选回填 | 全部 vertex | `reachedBy.edgeId` | 空 |
| `PathSet` | 可选回填 | 路径中的 VERTEX | 路径中的 EDGE | 原路径 |
| `CandidateSubgraph` | 可选回填 | 子图顶点 | 子图边 | 空 |

P0 要求：从 `CandidateSet<ContentRef>` 进入 Budget 时，**必须**带上已选 passage，不得丢 `contentId` 后再用 BM25 重查。

可选回填（由 `ContentStore.contentIdsForElement`）不得改变已选 `contentId` 集合的优先级，只允许在 token 预算剩余时补证据。

Budget 参数 `maxContents / maxVertices / maxEdges / maxPaths / maxTokens` 必须为正。Budget **不**物化正文。

### 4.8 Evidence

输入：`QueryContext` + `BudgetSelection`。输出：`EvidenceResult`。

1. 按 `contentId` 读 `ContentRecord.text`。不得重新执行 `textSearch` / `vectorSearch`。
2. 按已选 ID 读顶点 / 边属性。
3. 按已选 `ScoredPath.elements` 还原路径，不得另找最短路。
4. `llmContext` 与 token 计数必须使用与 Budget 相同的序列化。
5. 不得新增候选、不得重排序、不得替换 Budget 的选择。

### 4.9 Generation

输入：`QueryContext` + `EvidenceResult`。输出：`AnswerResult`。

```text
query + evidence.llmContext → LLM → AnswerResult
```

- Citation 必须能解析回 `EvidenceResult` 中的对象。
- LLM 失败时必须仍能拿到 `EvidenceResult`，不得丢检索结果。
- 禁止在 Generation 内再次调用 Recall / Expand / 改写流水线。

---

## 5. 兼容矩阵（执行前检查）

行 = 当前 `value`，列 = 下一算子族。`Y` = 允许，`S` = 仅当 `seeds` 非空，`-` = 拒绝。

| 当前值 | Recall | Expand | Prune | Fuse 同型 | Budget | Evidence | Generate |
|---|---|---|---|---|---|---|---|
| `null`（仅 QueryContext） | Y | - | - | - | - | - | - |
| `AnchorSet` | - | S | - | Y | Y | - | - |
| `CandidateSet<ContentRef>` | - | S | Y | Y | Y | - | - |
| `CandidateSet<GraphElementRef>` | - | S | Y | Y | Y | - | - |
| `ExpandedVertexSet` | - | S | Y | Y | Y | - | - |
| `PathSet` | - | - | Y | Y | Y | - | - |
| `CandidateSubgraph` | - | - | Y | Y | Y | - | - |
| `BudgetSelection` | - | - | - | - | - | Y | - |
| `EvidenceResult` | - | - | - | - | - | - | Y |
| `AnswerResult` | - | - | - | - | - | - | - |

Recall 只允许接在 `null` 上。需要多路 Recall 时，在 Builder 上并列追加 Recall Step，由 Fusion 合并；不得把第一个 Recall 的输出喂给第二个 Recall。

`shortestPaths()` 额外要求：**两组**可投影端点，不走单输入 Expand 列。

---

## 6. 存储与索引接口

```java
interface ContentStore extends AutoCloseable {
    void put(ContentRecord content);
    Optional<ContentRecord> get(String contentId);
    void delete(String contentId);
    List<String> contentIdsForElement(GraphElementRef element);
    void enqueueEmbedding(String contentId, EmbeddingSpace space);
    List<EmbeddingJob> pendingEmbeddings(String spaceId, int limit);
    Optional<EmbeddingJob> embeddingJob(String contentId, String spaceId);
    void markEmbeddingReady(String contentId, String spaceId);
    void markEmbeddingFailed(String contentId, String spaceId, String error);
}

interface ContentIndex extends AutoCloseable {
    void indexText(ContentRecord content);
    void indexVector(ContentRecord content, EmbeddingSpace space, float[] vector);
    List<ContentCandidate> textSearch(String query, int topK);
    List<ContentCandidate> vectorSearch(float[] queryVector, EmbeddingSpace space, int topK);
    void delete(String contentId);
}

interface EmbeddingProvider extends AutoCloseable {
    EmbeddingSpace space();
    List<float[]> embedDocuments(List<String> texts);
    float[] embedQuery(String query);
}

record EmbeddingSpace(
    String spaceId,
    String modelName,
    String modelRevision,
    int dimension,               // > 0
    boolean normalized,
    String queryPromptTemplate
) {}

enum EmbeddingStatus { PENDING, READY, FAILED }

record EmbeddingJob(
    String contentId,
    String spaceId,
    String contentHash,          // 必须等于当前 ContentRecord.contentHash
    EmbeddingStatus status,
    String error
) {}
```

写入顺序冻结：

```text
ContentStore.put(ContentRecord)     // 权威
→ ContentIndex.indexText
→ ContentStore.enqueueEmbedding
→ EmbeddingProvider.embedDocuments
→ ContentIndex.indexVector
→ markEmbeddingReady
```

`contentHash + spaceId` 不一致时，旧向量不得覆盖新正文。Lucene 可重建；RocksDB 不可从 Lucene 反推。

HTTP embedding 契约（本机服务）：

```text
GET  /health
GET  /model-info
POST /embed/documents
POST /embed/query
```

`model-info` 的维度 / 归一化必须等于 `EmbeddingSpace`。Java 客户端必须使用 HTTP/1.1。

---

## 7. 公开 Fluent API（目标签名）

```java
g.retrieve(String|QueryContext)
  .entityLink()
  .textRecall() | .vectorRecall() | .exactRecall()
  .topK(int)
  .neighbors(...) | .boundedBfs(...) | .ppr(...)
  .shortestPaths(...) | .constrainedPaths(...)
  .pruneCandidates(...) | .prunePaths(...) | .selectConnectedSubgraph(...)
  .fuse() | .rerank(...)
  .budget().maxContents(int).maxVertices(int).maxEdges(int).maxPaths(int).maxTokens(int)
  .evidence()
  .generate()
  .execute()            // AnswerResult
```

未写出的可选阶段可以省略，但省略后仍必须满足 §5。合法最短闭环：

```text
retrieve → textRecall|vectorRecall|entityLink → budget → evidence → generate → execute
```

`textRecall` / `vectorRecall` 之后直接 `budget` 合法（无图 hop）。`entityLink` 之后直接 `boundedBfs` 合法。`textRecall` 之后 `boundedBfs` 仅当 `seeds` 非空。

---

## 8. 实现对照

| 契约 | 代码现状 |
|---|---|
| 摄取阶段与类型 §2 | 已落地 |
| `ContentStore` / `ContentIndex` / `EmbeddingProvider` | 已落地 |
| `ContentRef` / `GraphElementRef` / `ContentRecord` | 已落地；`contentId` 为 `String` |
| `textRecall` / `vectorRecall` | 已落地；`executeRecall()` 仍返回 `List<ContentCandidate>`，`execute()` 收成 `CandidateSet<ContentRef>` |
| `entityLink()` / `AnchorSet` | HippoRAG 1：查询 NER + phrase 向量近邻 + `1/df`；无索引时回退目录匹配 |
| `ppr()` / `CandidateSet<GraphElementRef>` | HippoRAG 1：加权 PPR，teleport=0.9（igraph damping=0.1）；图 = fact 边 ∪ 同义边 |
| `projectByFacts` | phrase PPR 经 `SUPPORTS` fact 投到 passage；无 fact 时回退 MENTIONS |
| `budget()` / `evidence()` / `generate()` / `execute() → AnswerResult` | 已落地；链接余弦 ≤ 0.9 时与 dense 文档分 0.5/0.5 融合 |
| Prune / 路径算子 / `neighbors` / `boundedBfs` | **未落地** |

HippoRAG 1（Contriever 风格，不含 ColBERT）默认接线：

```text
retrieve(q).hippoRag().execute()
```

摄取在写图时登记 phrase/fact，并在 READY 前嵌入 phrase、写入相似度 ≥ 0.8 的同义边。`localId` 仍不得进入检索类型。

---

## 9. 变更规则

1. 改任意阶段输出类型，必须同时改本文件 §2 / §4 / §5，以及所有直接消费者。
2. 禁止用 `Map<String,Object>` 或未密封的 `Object` 在阶段间传结果。
3. 禁止为了“跑通”把 `ContentRef` 提升成顶点，或把 `GraphElementRef` 降成字符串再检索。
4. 禁止在 Evidence / Generation 重跑 BM25 / ANN。
5. 新增向量模型必须新 `spaceId`，旧向量空间视为不兼容。
6. `localId` 永远不得泄漏到检索类型。
