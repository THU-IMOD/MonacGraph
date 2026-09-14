# MonacGraph GraphRAG 图检索流水线实施方案

**类型、阶段 I/O 与兼容矩阵以 [`graph-retrieval-contracts.md`](graph-retrieval-contracts.md) 为准。** 方案综述见 [`graph-retrieval-intro.md`](graph-retrieval-intro.md)。本文保留算子语义、优先级、验收标准与论文对照；与契约冲突时先改契约再改本文。代码是否落地见契约 §8，不以本文第 10 节里程碑是否写完为准。

## 1. 文档目标

MonacGraph 当前方案聚焦于“检索优先”的 GraphRAG 执行能力。用户通过 Fluent API 显式描述流水线，系统严格按照书写顺序执行，不进行自动规划、算子重排或代价优化。

当前总体流程为：

```text
Query
→ Recall
→ Expand
→ [Prune]
→ [Fusion]
   ├── Fuse
   └── Rerank（可选）
→ Budget
→ Evidence
   └── EvidenceResult
→ Generation
   └── AnswerResult
```

本文面向完整检索流水线定义阶段边界、算子语义、输入输出类型、项目优先级、实施顺序与验收标准：

1. **Recall**：从全图中确定查询锚点或召回一批相关候选。
2. **Expand**：利用图结构从锚点或候选出发，探索邻居、子图或路径。
3. **Prune**：可选地对 Expand 结果进行后置质量筛选。
4. **Fusion**：可选地合并不同检索分支，并形成统一、有序的候选集合。
5. **Budget**：在节点数、边数、路径数和 LLM token 上限内选择最终证据。
6. **Evidence**：生成可追踪的节点、路径或证据子图。
7. **Generation**：默认将查询和证据交给可插拔 LLM，生成自然语言回答。
8. **Answer**：携带回答文本、引用、证据和生成元数据的最终结果，不属于检索算子。

Fusion 的内部操作为：

- **Fuse**：合并不同检索分支的候选、排名和来源。
- **Rerank**：可选地使用更精细的相关性与图结构信号重新排序。

本文中的 API 用于定义目标语义。公开 Java 签名以契约为准。

Passage 持久化、图元素双向关联、Lucene 索引和 Embedding 生成的实现见
[`graph-retrieval-storage-embedding.md`](graph-retrieval-storage-embedding.md)。本文只使用其公开的 `ContentRef`、内容索引和向量空间接口。

## 2. 优先级定义

| 优先级 | 含义 |
|---|---|
| **P0** | 项目最高优先级；构成最小可运行 GraphRAG 闭环 |
| **P1** | 项目较高优先级；补充主要多跳图检索能力 |
| **P2** | 项目中等优先级；用于算法对比、消融实验或高级策略 |
| **P3** | 项目暂缓项；依赖训练数据、复杂优化或较高在线模型成本 |

优先级是工程顺序，不代表算法价值高低。

### 2.1 与代表性论文的关系

本方案的优先级用于安排项目实施，不是 GraphRAG 领域的统一阶段定义：

- 项目将实体/文本/向量召回与受限邻域遍历列为 P0，以尽快形成可运行闭环。
- HippoRAG（NeurIPS 2024）代表实体锚点与 PPR 节点扩散。
- G-Retriever（NeurIPS 2024）同时计算节点和边的查询相关性，再使用 PCST 构造连通子图。
- RoG（ICLR 2024）使用关系序列约束图路径检索。
- Think-on-Graph（ICLR 2024）交替进行关系探索、实体探索和语义剪枝。
- LEGO-GraphRAG（PVLDB 2025）系统组合并比较候选子图提取与路径检索策略。
- GraphRAG-FI（EMNLP 2025）对已检索知识执行粗粒度与细粒度两阶段过滤。

论文归属用于说明算子依据，不决定项目优先级。P0/P1/P2 仅表示 MonacGraph 的实施顺序。

---

## 3. 公共数据模型

### 3.1 QueryContext

`QueryContext` 保存一次检索使用的原始查询及其派生表示：

```java
record QueryContext(
    String text,
    Optional<QueryEmbedding> embedding
) {}

record QueryEmbedding(
    String spaceId,
    float[] values
) {}
```

- `text`：用户输入的完整自然语言查询。
- `embedding`：查询在默认向量空间中的数值表示；只有语义检索或语义评分需要时才生成。

`QueryEmbedding`字段含义：

| 字段 | 含义 |
|---|---|
| `spaceId` | 数据库注册的向量空间标识，对应确定的模型、版本、维度和归一化方式 |
| `values` | 将完整查询文本编码得到的浮点向量 |

查询 embedding 只表示当前查询，不包含数据库索引项的向量；文本块或图元素 embedding 保存在对应向量索引中。`spaceId`保证查询向量只与同一空间中的索引项向量比较。仅使用 `entityLink()`或 `textRecall()`时，`embedding`可以为空。

节点标签、属性名称、候选类型、时间过滤条件和检索策略不放入 `QueryContext`。这些信息分别来自实际图数据、数据库索引配置或显式流水线算子。请求追踪 ID、权限、超时和取消信号属于独立的执行上下文；查询语言由文本分析器检测或由数据库配置。

`QueryContext`是整条流水线共享的查询输入上下文，不是某个 Recall 算子的输出。普通调用方只需传入字符串：

```java
g.retrieve("用户查询")
```

`retrieve(...)`的接口语义为：

```java
RetrievalPipelineBuilder retrieve(String query);
```

它立即返回 `RetrievalPipelineBuilder`，同时在 Builder 内部创建 `QueryContext`。此时不会执行数据库检索：

```text
g.retrieve(query)
→ 创建 QueryContext
→ 创建空的 RetrievalStep 列表
→ 返回 RetrievalPipelineBuilder
```

后续 Fluent API 调用向 Builder 追加步骤，并继续返回 Builder：

```java
RetrievalPipelineBuilder pipeline = g.retrieve("用户查询")
    .entityLink()
    .boundedBfs()
    .budget()
    .evidence()
    .generate();
```

默认执行完整问答流水线并返回最终结果：

```java
AnswerResult answer = pipeline.execute();
```

Recall、Expand、Prune、Fusion、Budget 和 Evidence 都会产生对应的类型化中间结果。执行器可以按配置保留并暴露这些结果，用于调试、评测和前端展示；这不会把完整流水线拆分成多个默认运行模式。

只调用 `g.retrieve("用户查询")`而不追加完整流水线和 `execute()`，不会访问数据库，也不会产生检索结果。

需要复用已有 embedding 的高级调用方也可以直接构造 `QueryContext`：

```java
g.retrieve(queryContext)
```

每个算子都可以读取同一个 `QueryContext`，同时接收上一步的检索结果：

```java
RetrievalValue execute(
    QueryContext query,
    RetrievalValue previous
);
```

### 3.2 RetrievalStep 与 RetrievalValue

`RetrievalStep`是流水线内部对一个可执行算子的统一表示。Fluent API 每调用一次算子方法，就向 Builder 追加一个对应的 Step：

```text
.entityLink()       → EntityLinkStep
.vectorRecall()     → VectorRecallStep
.boundedBfs()       → BoundedBfsStep
.pruneCandidates()  → PruneCandidatesStep
.budget()           → BudgetStep
```

概念接口为：

```java
interface RetrievalStep {
    String name();
    RetrievalType inputType();
    RetrievalType outputType();

    RetrievalValue execute(
        QueryContext query,
        RetrievalValue input
    );
}
```

- `name()`：算子名称，用于执行日志和 provenance。
- `inputType()`：允许接收的上一步结果类型。
- `outputType()`：本算子产生的结果类型。
- `execute(...)`：读取公共查询上下文和上一步结果，执行当前算子。

`RetrievalValue`是中间结果的公共父类型：

```java
sealed interface RetrievalValue permits
    AnchorSet,
    CandidateSet,
    ExpandedVertexSet,
    PathSet,
    CandidateSubgraph {}
```

它本身不保存字段，也不会作为实际结果实例返回。以下具体类型保存真正的数据并实现 `RetrievalValue`：

```text
AnchorSet
CandidateSet
ExpandedVertexSet
PathSet
CandidateSubgraph
```

例如：

```text
EntityLinkStep
  上一步结果：无
  公共上下文：QueryContext
  输出：AnchorSet

BoundedBfsStep
  输入：AnchorSet、CandidateSet<GraphElementRef> 或带顶点关联的 CandidateSet<ContentRef>
  输出：ExpandedVertexSet

PrunePathsStep
  输入：PathSet
  输出：PathSet
```

Builder 只负责收集 Step：

```java
public RetrievalPipelineBuilder boundedBfs(BfsOptions options) {
    steps.add(new BoundedBfsStep(options));
    return this;
}
```

Executor 按书写顺序执行：

```java
RetrievalValue value = null;

for (RetrievalStep step : steps) {
    validateType(step.inputType(), value);
    value = step.execute(queryContext, value);
}
```

因此三者关系为：

```text
RetrievalPipelineBuilder：保存整条流水线
RetrievalStep：表示流水线中的一个算子
RetrievalValue：表示算子之间传递的结果
```

`RetrievalStep`是内部执行抽象，不要求普通调用方直接创建或操作。

### 3.3 RetrievalItemRef、GraphElementRef 与 ContentRef

候选召回既可能返回图元素，也可能返回文本证据，因此使用公共候选引用类型：

```java
sealed interface RetrievalItemRef permits
    GraphElementRef,
    ContentRef {}

record GraphElementRef(
    ElementKind kind,
    Object id,
    String label
) implements RetrievalItemRef {}

record ContentRef(
    Object contentId,
    List<GraphElementRef> linkedElements,
    String sourceField
) implements RetrievalItemRef {}

enum ElementKind {
    VERTEX,
    EDGE
}
```

`GraphElementRef`不是完整的顶点或边对象，也不是一次查询的完整输出。它只是指向数据库中某个图元素的轻量引用：

| 字段 | 含义 |
|---|---|
| `kind` | 图元素类型：顶点或边 |
| `id` | 图元素在数据库中的唯一 ID |
| `label` | 从实际图数据读取的标签；调用方不需要提前知道 |

完整属性在评分、Evidence 物化或调用方明确请求时再读取。这样候选集合只传递 ID 和必要类型信息，避免为大量中间候选复制完整属性。

`ContentRef`表示一段可独立检索的文本证据：

| 字段 | 含义 |
|---|---|
| `contentId` | 文本块在内容索引中的唯一 ID |
| `linkedElements` | 该文本块关联到的图顶点和边 |
| `sourceField` | 文本来源，例如原始文档字段或图元素属性键，由索引数据产生 |

文本正文不复制到每个中间候选中，需要评分或 Evidence 物化时再通过 `contentId`读取。`linkedElements`中的顶点可作为 Expand 的 seed，边关联用于关系证据和来源追踪。

两类引用通常嵌套在真正的算子输出中：

```text
AnchorCandidate
└── GraphElementRef

ScoredCandidate
└── RetrievalItemRef
    ├── GraphElementRef
    └── ContentRef

PathSet
└── 路径中的多个 GraphElementRef

EvidenceResult
└── 物化后的文本块、顶点、边和路径
```

### 3.4 Score 与 Provenance

不同检索器的原始分数采用独立语义，跨检索器组合时先进行归一化或排名融合：

```java
record RetrievalScore(
    String name,
    double value,
    ScoreSemantics semantics
) {}

enum ScoreSemantics {
    LINKING_SCORE,
    COSINE_SIMILARITY,
    BM25,
    PPR_PROBABILITY,
    PATH_RELEVANCE
}
```

每个结果必须记录来源：

```java
record Provenance(
    String operator,
    List<Object> sourceIds,
    Map<String, Object> parameters
) {}
```

### 3.5 中间结果与边表示

不同算子保留自身最直接的结果语义，不强制把所有中间结果转换成子图：

```text
entityLink()                     → AnchorSet
textRecall() / vectorRecall()    → CandidateSet
neighbors() / boundedBfs()       → ExpandedVertexSet
ppr()                            → CandidateSet
shortestPaths() / beamPaths()    → PathSet
selectConnectedSubgraph()        → CandidateSubgraph
evidence()                       → EvidenceResult
```

`ExpandedVertexSet`是带遍历来源的顶点集合：

```java
record ExpandedVertexSet(
    List<ExpandedVertex> vertices
) implements RetrievalValue {}

record ExpandedVertex(
    GraphElementRef vertex,
    int depth,
    List<TraversalRef> reachedBy,
    List<RetrievalScore> scores,
    Provenance provenance
) {}

record TraversalRef(
    Object parentVertexId,
    Object edgeId,
    Direction direction
) {}
```

其中 `vertex`必须引用顶点；`reachedBy`记录该顶点由哪些前驱顶点、边和方向到达。一个顶点可以具有多个 `TraversalRef`，用于保留多来源扩展。

`ExpandedVertexSet`不是完整子图：

- 它保存扩展得到的顶点。
- 它保存实际用于到达这些顶点的遍历边。
- 它不自动读取这些顶点之间存在的所有其他边。

因此它能够还原遍历树或遍历 DAG，但不等于由候选顶点形成的诱导子图。路径算子使用 `PathSet`保留明确路径；PCST 使用 `CandidateSubgraph`保留连通候选子图；最终由 `EvidenceResult`物化需要返回的文本块、节点、边和路径。

以下能力排在后续优先级：

- 将查询中的自然语言关系映射到图中的 edge label。
- 将边作为 BM25 或向量检索的直接召回目标。
- 自动判断应当沿哪些关系类型遍历。

默认 Expand 在资源预算内遍历所有可用边。调用方明确了解图模式时，可以选择方向或提供边标签白名单。边级相关性检索和关系引导路径探索分别参考 G-Retriever、RoG 和 Think-on-Graph，并按项目资源安排优先级。

---

## 4. Recall 阶段

Recall 包含两类检索入口：

```text
Query
├── Anchor Linking   → AnchorSet
└── Candidate Recall → CandidateSet
```

- **Anchor Linking** 回答：“查询中的 mention 对应图中的哪个实体？”
- **Candidate Recall** 回答：“图中哪些元素与整个查询相关？”

两类入口可以单独使用或形成并行分支。跨检索器融合前必须对分数进行归一化或采用基于排名的融合方法。

### 4.1 Anchor Linking

#### 4.1.1 `entityLink()` — P0

输入完整查询，自动完成 mention detection、跨标签候选召回和上下文消歧。

```java
g.retrieve("乔布斯和苹果公司是什么关系？")
 .entityLink()
 .maxCandidatesPerMention(3)
```

候选召回默认跨节点标签执行，节点 `label` 随候选结果返回。当调用方具有明确的业务类型约束时，可以通过可选参数 `allowLabels(...)` 限制候选范围。

返回值必须保留 mention 分组：

```java
record AnchorSet(
    List<AnchorMention> mentions
) implements RetrievalValue {}

record AnchorMention(
    String mentionId,
    String text,
    int startOffset,
    int endOffset,
    List<AnchorCandidate> candidates
) {}

record AnchorCandidate(
    GraphElementRef element,
    double linkingScore,
    String matchedText,
    String method,
    Provenance provenance
) {}
```

三层结构分别表示：

#### `AnchorSet`：一次查询的实体链接结果

`AnchorSet` 是 `entityLink()` 的完整返回值，包含查询中发现的所有实体提及。

| 字段 | 含义 |
|---|---|
| `mentions` | 查询中识别出的全部实体提及及其候选 |

例如，查询中识别出 `乔布斯`和 `苹果公司`，`AnchorSet`中就包含两个 `AnchorMention`。

#### `AnchorMention`：查询中的一个实体提及

`AnchorMention` 表示原始查询中的一段文本，以及这段文本可能对应的图节点。

| 字段 | 含义 |
|---|---|
| `mentionId` | 本次查询内的唯一编号，例如 `m1` |
| `text` | 查询中的原始文本，例如 `乔布斯` |
| `startOffset` | 该文本在查询字符串中的起始位置 |
| `endOffset` | 该文本在查询字符串中的结束位置 |
| `candidates` | 该 mention 可能对应的图节点列表，按链接分数降序排列 |

文本位置用于区分查询中名称相同但语境不同的 mention，并支持结果高亮和问题追踪。

#### `AnchorCandidate`：一个可能匹配的图节点

`AnchorCandidate` 表示某个 mention 与图中一个节点之间的候选链接。

| 字段 | 含义 |
|---|---|
| `element` | 候选图节点的类型、ID 和标签 |
| `linkingScore` | 当前实体链接器用于候选排序的分数；不解释为概率 |
| `matchedText` | 图节点中实际匹配的名称或别名 |
| `method` | 产生候选的方法，例如 `exact-name`、`alias`、`embedding` |
| `provenance` | 本次链接使用的算子、参数和来源信息 |

三者的包含关系为：

```text
AnchorSet                         一次查询的完整结果
├── AnchorMention: "乔布斯"      查询中的第一个实体提及
│   ├── AnchorCandidate: v102    候选图节点 1
│   └── AnchorCandidate: v378    候选图节点 2
└── AnchorMention: "苹果公司"    查询中的第二个实体提及
    └── AnchorCandidate: v7      候选图节点 1
```

示例：

```text
乔布斯   → [v102 "Steve Jobs" (0.94), v378 "Steve Jobs (film)" (0.58)]
苹果公司 → [v7 "Apple Inc." (0.97), v55 "Apple Records" (0.51)]
```

返回结构按 mention 分组，使下游能够保留候选归属并连接不同 mention 对应的实体组。

最低实现要求：

1. 基于名称和别名的 mention 匹配。
2. 每个 mention 独立保留候选。
3. 结合查询文本进行候选排序和消歧。
4. 返回候选节点标签，但不预设标签。
5. 无法链接的 mention 保留在结果中，并标记为未解析。

`entityLink()`不规定没有训练依据的固定加权公式。初始实现按以下匹配层级生成和排列候选：

```text
规范名称精确匹配
→ 别名精确匹配
→ 规范化或模糊名称匹配
```

每个候选同时返回 `method`和 `linkingScore`。该分数只用于同一 mention 内部排序，不解释为概率，也不与 BM25、余弦相似度等分数直接比较。

后续可以使用专门的实体链接模型或上下文 reranker 对候选重新评分。具体评分函数由所选模型定义，并通过 Entity Linking Recall@K、MRR 等指标验证。

`entityLink()`的 P1 增强可以加入以下图辅助消歧信号：

- 查询上下文与节点描述的语义相似度。
- 多个 mention 候选之间的图距离。
- 候选邻接关系与查询关系词的匹配程度。
- 节点流行度，但必须限制其权重，避免总是选择高度节点。

消歧完成后仍然返回 `AnchorSet`。无法可靠解析的 mention 保留多个候选，或输出 `UNRESOLVED` 状态，不增加独立的流水线算子。

### 4.2 Candidate Recall

所有 Candidate Recall 算子返回全局排序的 `CandidateSet`：

```java
record CandidateSet<T extends RetrievalItemRef>(
    String source,
    List<ScoredCandidate<T>> items
) implements RetrievalValue {}

record ScoredCandidate<T extends RetrievalItemRef>(
    T item,
    List<RetrievalScore> scores,
    int rank,
    String matchedText,
    Provenance provenance
) {}
```

#### 4.2.1 `textRecall()` — P0

使用 BM25 或其他倒排索引检索文本块：

```java
g.retrieve(query)
 .textRecall()
 .topK(50)
```

查询执行时不要求调用方了解节点标签和属性名称。数据库在数据导入时维护文本索引目录：

```java
record IndexedText(
    ContentRef content,
    String text
) {}
```

数据导入时，文档正文或允许检索的图元素字符串属性被切分为文本块。每个文本块通过 `ContentRef`保留来源字段及其关联顶点和边。管理员可以在建库时配置排除规则；普通查询使用数据库的默认检索范围。

主要参数：

- `topK`：全局召回候选上限。
- `minScore`：可选的最低 BM25 分数。

返回类型为 `CandidateSet<ContentRef>`，原始分数语义为 `BM25`：

```text
CandidateSet<ContentRef>
└── ScoredCandidate
    ├── item: { contentId, linkedElements, sourceField }
    ├── score: { semantics=BM25, value }
    └── matchedText: 命中文本摘要
```

`textRecall()`采用 Lucene 等成熟全文检索组件。

#### 4.2.2 `vectorRecall()` — P0

将完整查询编码为 embedding，并召回语义相似的文本块：

```java
g.retrieve(query)
 .vectorRecall()
 .topK(50)
```

向量模型、维度、文本块表示方法和 ANN 索引在建库时注册为默认向量检索配置。查询文本由该配置对应的模型编码，调用方不指定节点标签、属性名称或 embedding 字段。

主要参数：

- `topK`：召回候选上限。
- `minScore`：可选阈值。

返回类型为 `CandidateSet<ContentRef>`，原始分数语义通常为 `COSINE_SIMILARITY`。

`vectorRecall()`采用一个数据库级默认向量空间及外部 ANN 库或离线向量快照。存在多个不兼容向量空间时，由管理员配置默认空间；显式选择其他空间作为后续高级接口。

#### 4.2.3 `exactRecall()` — P1

通过图元素 ID、唯一名称、属性索引或明确标签条件召回候选：

```java
.exactRecall("doi", "10.xxxx/xxxx")
```

该算子适用于程序化查询、评测和测试，不属于 AI 特有能力，但可以：

- 构造确定性 Anchor。
- 建立其他 Recall 算子的正确性基线。
- 支持用户已经知道唯一标识符的场景。

### 4.3 Recall 算子优先级

| 算子 | 输出 | 优先级 | 实现范围 |
|---|---|---:|---|
| `entityLink()` | `AnchorSet` | P0 | mention 检测、名称/别名召回、分组候选、上下文消歧 |
| `textRecall()` | `CandidateSet` | P0 | BM25 Top-K |
| `vectorRecall()` | `CandidateSet` | P0 | 查询编码、ANN/精确 KNN Top-K |
| `exactRecall()` | `CandidateSet` | P1 | ID、唯一属性和测试入口 |

### 4.4 Recall 阶段验收标准

至少建立以下指标：

- Entity Linking：Recall@K、MRR、每个 mention 的候选覆盖率。
- Text/Vector Recall：Recall@K、nDCG@K、查询延迟。
- 空结果率和无法链接率。
- 不同候选规模下的召回率—延迟曲线。

`topK` 是可调实验参数，应通过召回率—延迟曲线选择默认值。

---

## 5. Expand 阶段

Expand 接收 `AnchorSet` 或能够映射到顶点的 `CandidateSet`，利用图结构产生新的候选或路径。

遍历算法可以在内部维护 `Frontier`，表示下一轮等待展开的边界节点；该状态不作为公开 API 的输入。`PathSet` 是 `constrainedPaths()`、`shortestPaths()`和 `beamPaths()`等路径算子的输出。

Expand 由语义明确的具体算子组成。每个算子显式声明遍历方式、方向、边类型、深度、候选规模、时间预算和相关性筛选策略。

### 5.1 基础结构探索

#### 5.1.1 `neighbors()` — P0

获取输入顶点的一跳邻居：

```java
.neighbors()
    .direction(BOTH)
    .maxNeighborsPerVertex(100)
    .maxTotalVertices(1000)
```

输入：

- `AnchorSet`：对每个锚点候选展开。
- `CandidateSet<GraphElementRef>`：对其中的顶点候选展开。
- `CandidateSet<ContentRef>`：提取各文本块 `linkedElements`中的顶点并合并为 seed 后展开；没有关联顶点的文本块只能进入 Evidence，不能参与图遍历。

输出为 `ExpandedVertexSet`，公共结构见第 3.5 节。

最低要求：

1. 支持 `IN`、`OUT`、`BOTH`。
2. 支持可选边标签白名单。
3. 保留父节点和经过的边。
4. 节点去重，同时保留多来源路径。
5. 必须同时支持单点和批量邻接访问。
6. 必须设置全局上限，避免高度节点耗尽内存。

`neighbors()`严格表示一跳，不接受 `hops` 参数。

#### 5.1.2 `boundedBfs()` — P0

执行受资源限制的多跳 BFS：

```java
.boundedBfs()
    .maxDepth(2)
    .direction(BOTH)
    .maxVisitedVertices(5000)
    .maxTraversedEdges(20000)
    .timeoutMillis(100)
```

与 `neighbors()`相比，它明确支持多跳，并要求同时设置深度与资源边界。

必要参数：

- `maxDepth`：最大深度。
- `maxVisitedVertices`：最多访问节点数。
- `maxTraversedEdges`：最多访问边数。
- `timeoutMillis`：执行时间上限。

可选参数：

- `edgeLabels`：边标签白名单。
- `direction`：遍历方向。
- `deduplicate`：按节点或按路径去重。

输出应保留：

- 每个节点的最小深度。
- 来源 Anchor 或 Recall Candidate。
- 至少一条到达路径。
- 是否因为预算被提前截断。

多跳遍历同时使用深度、节点数、边数和执行时间作为停止条件。

### 5.2 概率与相关性引导探索

#### 5.2.1 `ppr()` — P1

将 Anchor 或候选分数转化为重启分布，执行 Personalized PageRank：

```java
.ppr()
    .restartProbability(0.15)
    .topK(100)
    .maxIterations(30)
    .tolerance(1e-6)
```

输出为带 `PPR_PROBABILITY` 的全局排序 `CandidateSet<GraphElementRef>`。

实现要求：

- 支持多个 seed。
- 支持 uniform 和 score-weighted 两种重启分布。
- 返回收敛状态与实际迭代次数。
- 结果记录 seed 到输出节点的来源关系。
- 大图可以采用近似 PPR，但必须与精确实现进行 Recall@K 对比。

适用场景：

- 不确定正确 hop 的关联检索。
- 多个 Anchor 的共同结构关联。
- 需要比完整 BFS 更平滑的多跳传播。

代表工作：HippoRAG（NeurIPS 2024）。

#### 5.2.2 `beamPaths()` — P2

以路径为状态，每轮枚举下一跳并仅保留得分最高的若干条路径：

```java
.beamPaths()
    .beamWidth(5)
    .maxLength(4)
    .maxExpansionsPerPath(50)
    .scorer(EMBEDDING)
```

基本执行过程：

```text
当前 Top-B 路径
→ 枚举可行的下一跳
→ 计算路径与查询的相关性
→ 保留新的 Top-B
→ 达到长度、预算或无候选时停止
```

输出：

```java
record PathSet(
    List<ScoredPath> paths,
    boolean truncated
) implements RetrievalValue {}

record ScoredPath(
    List<Object> vertexIds,
    List<Object> edgeIds,
    double score,
    int length,
    Provenance provenance
) {}
```

首个 Beam Search 评分器优先使用 embedding 或轻量 reranker。在线 LLM 评分器作为可选实验实现。

代表工作：

- Think-on-Graph（ICLR 2024）：逐轮进行关系探索、实体探索和 LLM 剪枝。
- PropRAG（EMNLP 2025）：先用 PPR 提取局部子图，再执行 embedding-guided beam search。

### 5.3 路径约束探索

#### 5.3.1 `constrainedPaths()` — P1

只沿调用方明确给出的关系序列或关系集合进行路径检索：

```java
.constrainedPaths()
    .relationSequence("authored", "cites")
    .maxPaths(50)
```

`constrainedPaths()`由用户显式指定 `"authored"`、`"cites"` 等关系。根据自然语言自动生成关系序列属于后续查询编译能力。

两种约束模式：

- `relationSequence(...)`：每一跳必须匹配指定顺序。
- `allowRelations(...)`：每一跳只能从白名单中选择，但顺序不限。

输出为 `PathSet`，每条路径必须保留完整的顶点、边和方向。

代表工作：RoG（ICLR 2024）先生成关系路径，再从查询实体出发检索符合关系序列的真实路径。

#### 5.3.2 `shortestPaths()` — P1

连接两个或多个 Anchor 组，或者连接 Anchor 与候选结果：

```java
.shortestPaths()
    .maxLength(4)
    .maxPathsPerPair(10)
```

输入必须提供明确的路径起点集合和终点集合。

适用场景：

- 查询多个实体之间的关系。
- 为候选答案构造可解释证据。
- 为后续 Evidence 阶段提供紧凑连接结构。

风险：

- 最短路径不一定是语义上最相关的路径。
- 高度节点可能产生无意义捷径。
- 多端点组合可能导致路径对数量爆炸。

因此需要 `maxLength`、`maxPathsPerPair` 和边标签约束。

GNN-RAG（ACL Findings 2025）采用“预测答案候选，再提取查询实体到候选答案的最短路径”的方式构造证据。

### 5.4 采样与学习式探索

#### 5.4.1 `randomWalk()` — P2

从输入节点执行多次有限长度随机游走：

```java
.randomWalk()
    .walksPerSeed(20)
    .maxLength(4)
    .restartProbability(0.15)
    .randomSeed(42)
```

算子支持指定 `randomSeed`，用于可复现评测，并作为固定成本探索基线。

#### 5.4.2 `learnedFrontier()` — P3

每轮由训练模型判断哪些 Frontier 节点值得继续展开：

```java
.learnedFrontier()
    .model("frontier-selector")
    .maxRounds(3)
    .maxVerticesPerRound(20)
```

代表工作：PullNet（EMNLP 2019）使用 Graph CNN 选择下一轮需要扩展的节点，并从知识库和文本语料拉取新事实。

该能力需要任务数据、训练流程和模型部署，因此优先级定为 P3。

### 5.5 后续阶段的相关算法

以下算法划分到后续流水线阶段：

- **PCST**：从带 prize 的节点和边中选择紧凑连通子图，属于 Prune。
- **RRF**：融合多个 Recall 分支的排名，属于 Fusion/Fuse。
- **Cross-Encoder 排序**：属于 Fusion/Rerank。
- **Token 上限内的证据选择**：属于 Budget。

G-Retriever（NeurIPS 2024）的 PCST 用于补充必要桥接节点并选择候选证据子图。MonacGraph 将其建模为可选的 Prune 算子。

### 5.6 Expand 算子优先级

| 算子 | 输出 | 优先级 | 实现范围 |
|---|---|---:|---|
| `neighbors()` | `ExpandedVertexSet` | P0 | 一跳、方向、批量邻接、上限和边来源 |
| `boundedBfs()` | `ExpandedVertexSet` | P0 | 深度、节点、边和时间四类边界 |
| `ppr()` | `CandidateSet` | P1 | 多 seed、重启分布、Top-K |
| `constrainedPaths()` | `PathSet` | P1 | 关系序列/白名单约束 |
| `shortestPaths()` | `PathSet` | P1 | Anchor—Anchor、Anchor—Candidate |
| `beamPaths()` | `PathSet` | P2 | embedding 引导的 Beam Search |
| `randomWalk()` | `PathSet`/`CandidateSet` | P2 | 可复现的固定预算基线 |
| `learnedFrontier()` | 可变 | P3 | 暂不实现 |

---

## 6. Prune 阶段

Prune 是可选的后置阶段，接收 Expand 已经产生的候选节点、遍历边或路径，并删除低相关、冗余或不适合进入后续处理的内容：

```text
Recall
→ Expand
→ [Prune]
→ [Fusion]
→ Budget
→ Evidence
```

“可选”描述的是查询运行时是否必须经过该阶段；P0/P1/P2 描述的是项目实现优先级，二者相互独立。

并非每条流水线都需要 Prune：

- `ppr()`已经输出带概率的 Top-K 节点时，可以直接进入后续阶段。
- `beamPaths()`内部已经按 beam width 逐轮剪枝，但仍可对最终路径执行后置 Prune。
- `boundedBfs()`产生大规模局部候选时，通常需要后置 Prune。

### 6.1 Prune 与 Expand 内部搜索控制

以下参数属于 Expand 算子内部的可执行性控制，不构成独立 Prune：

- `maxDepth`
- `maxVisitedVertices`
- `maxTraversedEdges`
- `maxNeighborsPerVertex`
- `beamWidth`
- `maxExpansionsPerPath`
- `timeoutMillis`

独立 Prune 在 Expand 完成后读取其完整输出，并根据查询相关性、路径质量或连通子图目标重新选择结果。两类裁剪可以同时存在：

```text
beamPaths 内部保留 Top-B 路径
→ prunePaths 对最终路径进行统一语义筛选
```

### 6.2 公共输出约定

普通 Prune 算子保持输入类型，只减少其中的元素：

```text
CandidateSet      → CandidateSet
ExpandedVertexSet → ExpandedVertexSet
PathSet           → PathSet
```

每次裁剪附加统计信息：

```java
record PruneStats(
    int inputCount,
    int outputCount,
    int removedCount,
    String scorer,
    Map<String, Object> parameters
) {}
```

被保留元素继续携带原始 Recall 和 Expand provenance，并增加本次 Prune 的评分与保留原因。

### 6.3 候选单元语义筛选

#### 6.3.1 `pruneCandidates()` — P0

对 Expand 产生的节点、边或三元组进行查询相关性评分，并保留满足条件的候选：

```java
.pruneCandidates()
    .unit(TRIPLE)
    .scorer(EMBEDDING)
    .keepTopK(100)
```

`unit`决定独立评分和保留的基本单位：

- `NODE`：对节点评分，保留相关节点及其必要来源边。
- `EDGE`：对边评分，保留相关边及其两个端点。
- `TRIPLE`：将 `(source, edge, target)`作为一个整体评分和保留。

评分器可以是：

- `EXISTING_SCORE`：使用 Recall 或 Expand 已有分数。
- `EMBEDDING`：计算候选文本表示与查询的向量相似度。
- `RERANKER`：使用 Cross-Encoder 等精排模型。

筛选条件可以使用：

- `minScore`：删除低于相关性阈值的候选。
- `keepTopK`：只保留当前评分器下的 Top-K。
- `keepPercent`：保留最高分的一定比例。

节点、边和三元组裁剪对应 LEGO-GraphRAG（PVLDB 2025）总结的 Node Pruning、Edge Pruning 和 Triple Pruning。

`keepTopK`用于当前质量模型下的候选筛选；后续 Budget 仍负责节点数、边数、路径数和 token 数等最终资源限制。

### 6.4 路径后置筛选

#### 6.4.1 `prunePaths()` — P1

对 Expand 已经生成的完整路径统一评分：

```java
.prunePaths()
    .scorer(EMBEDDING)
    .keepTopK(20)
```

输入和输出均为 `PathSet`。路径评分可以综合：

- 整条路径与查询的语义相关性。
- 路径中节点和边的相关性。
- 路径长度惩罚。
- 重复节点、重复边和路径间冗余。
- 是否覆盖多个查询 Anchor。

该算子对应 LEGO-GraphRAG 中的 One-way Semantic-Augmented Retrieval（OSAR）：

```text
先使用 BFS、最短路径等方法生成候选路径
→ 再使用 embedding、reranker 或 LLM 统一筛选
```

它与 `beamPaths()`的区别是：

- `beamPaths()`在每一跳扩展时剪枝，属于交互式搜索。
- `prunePaths()`在所有候选路径生成后统一筛选，属于后置 Prune。

### 6.5 连通子图选择

#### 6.5.1 `selectConnectedSubgraph()` — P2

根据节点 prize、边 prize 和连接成本选择紧凑连通子图：

```java
.selectConnectedSubgraph()
    .algorithm(PCST)
    .maxVertices(50)
    .maxEdges(100)
```

输入要求：

- 候选节点具有查询相关性分数。
- 候选边具有相关性分数或连接成本。
- Expand 结果保留候选节点之间可使用的连接边。

输出：

```java
record CandidateSubgraph(
    List<GraphElementRef> vertices,
    List<GraphElementRef> edges,
    double objectiveScore,
    PruneStats pruneStats,
    Provenance provenance
) implements RetrievalValue {}
```

PCST 的目标不是分别选择最高分节点，而是在以下因素之间取舍：

```text
保留高价值节点和边
+ 补充必要桥接节点
- 支付连接成本
```

该策略对应 G-Retriever（NeurIPS 2024）。原论文将 PCST 作为图检索本身；在 MonacGraph 显式流水线中，将它表示为候选评分之后的连通子图选择算子。

### 6.6 两阶段知识过滤

#### 6.6.1 `knowledgeFilter()` — P2

先用低成本模型粗筛，再用高成本模型细筛：

```java
.knowledgeFilter()
    .coarse(ATTENTION_SCORE)
    .fine(LLM)
```

执行过程：

```text
候选知识
→ attention / embedding 粗过滤
→ LLM 判断剩余候选是否有助于回答查询
→ 保留最终知识
```

输入可以是 `CandidateSet`、`PathSet`或 `CandidateSubgraph`；输出保持相同类型。在线 LLM 调用必须设置候选数、token 数、超时和失败降级策略。

该策略对应 GraphRAG-FI（EMNLP 2025）的两阶段 GraphRAG-Filtering。由于依赖模型内部信号或在线 LLM，项目优先级定为 P2。

### 6.7 不建模为独立 Prune 算子的操作

以下操作由产生候选的算子直接完成：

- 节点 ID 去重。
- 路径生成过程中的环路检查。
- 无效边和悬空引用检查。
- Expand 的访问上限和超时。
- Beam Search 每轮 Top-B 保留。

原因是这些操作属于结果正确性或搜索可执行性，而不是可选的后置质量策略。

### 6.8 Prune 算子优先级

| 算子 | 输入/输出 | 优先级 | 实现范围 |
|---|---|---:|---|
| `pruneCandidates()` | Candidate/Expanded → 同类型 | P0 | 节点、边、三元组语义筛选 |
| `prunePaths()` | `PathSet → PathSet` | P1 | 完整路径后置评分与选择 |
| `selectConnectedSubgraph()` | Expanded → `CandidateSubgraph` | P2 | PCST 连通子图选择 |
| `knowledgeFilter()` | Candidate/Path/Subgraph → 同类型 | P2 | 粗筛 + LLM 细筛 |

`pruneCandidates()`优先实现基于已有分数或 embedding 的节点/三元组筛选。去重、环路检查和搜索上限仍分别归入公共执行逻辑和 Expand 算子内部。

### 6.9 Prune 阶段验收标准

至少建立以下指标：

- Prune 前后的候选数量和压缩率。
- 节点、边、三元组或路径的 Precision、Recall、F1。
- Evidence Recall：裁剪后是否仍保留回答所需证据。
- 端到端 QA 准确率或 F1。
- Prune 延迟及在线模型调用成本。
- 不同阈值、Top-K 和评分器的质量—成本曲线。

核心约束是：减少噪声不能以显著丢失关键桥接节点或中间路径为代价。

---

## 7. Evidence、Generation 与 Answer

### 7.1 默认执行路径与中间结果

MonacGraph 默认执行完整问答路径：

```text
Budget → Evidence → EvidenceResult → Generation → AnswerResult
```

`EvidenceResult`是 Generation 的输入和完整流水线中的中间结果，不是与问答模式并列的另一种模式。Recall、Expand、Prune、Fusion 和 Budget 同样产生各自的中间结果。调用方可以选择捕获某一步的结果，用于前端图展示、检索评测、缓存、调试或其他程序分析；正常执行仍继续到 `AnswerResult`。

中间结果的公开方式可以采用执行选项、监听器或分阶段执行接口，具体 Java 签名在接口设计评审时确定。无论采用哪种方式，都不得改变算子的书写顺序和默认完整执行语义。

### 7.2 `evidence()` — P0

`evidence()`接收 Budget 已选择的文本块、节点、边和路径，将内部引用物化为可追踪结果：

```java
.budget()
    .maxContents(20)
    .maxVertices(40)
    .maxEdges(80)
    .maxPaths(10)
    .maxTokens(4000)
.evidence()
```

Evidence 负责：

1. 按 `contentId`读取选中文本块，并读取选中节点和边需要返回的属性。
2. 按顶点和边顺序还原路径。
3. 保留 Recall、Expand、Prune 和 Fusion 的 provenance。
4. 生成结构化图结果和 LLM 可用的文本上下文。
5. 使用与 Budget 相同的序列化和 tokenizer，确保输出不超过 token 上限。

Evidence 不增加候选、不重新排序，也不替换 Budget 的选择结果。

建议输出类型：

```java
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
    Object contentId,
    String text,
    List<GraphElementRef> linkedElements,
    String sourceField,
    Provenance provenance
) {}
```

其中：

- `contents`保存物化后的文本块及其来源。
- `vertices`、`edges`、`paths`保存结构化证据。
- `llmContext`是同一批证据的文本化表示。
- `provenance`记录每项证据经过的算子和来源。
- `trace`记录各步骤的输入数、输出数、耗时和截断状态。

### 7.3 `generate()` — P0

`generate()`是默认完整流水线的最终处理算子，输入为原始查询和 `EvidenceResult`，输出为 `AnswerResult`：

```java
.evidence()
.generate()
.execute()
```

LLM provider、模型、鉴权、超时和默认生成参数由系统配置，不要求普通查询指定模型名称。

Generation Request 至少包含：

```java
record GenerationRequest(
    String query,
    String evidenceContext,
    List<EvidenceReference> references,
    GenerationOptions options
) {}
```

生成约束：

- 回答以 Evidence 中的信息为主要依据。
- 证据不足时允许明确返回“现有证据不足”。
- 引用必须指向 `EvidenceReference`，不能由模型自由生成图元素 ID。
- 默认使用确定性较高的生成配置。
- LLM 调用失败时保留并返回 `EvidenceResult`，不得丢失检索结果。

`generate()`属于应用适配层，不绑定数据库内核与特定 LLM SDK。P0 至少实现一个统一 provider 接口和一个可运行适配器。

### 7.4 `AnswerResult`

Answer 是 Generation 的结果类型，不是继续修改候选集合的流水线阶段：

```java
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

record GenerationMetadata(
    String provider,
    String model,
    int promptTokens,
    int completionTokens,
    String finishReason
) {}
```

返回完整 `EvidenceResult`使调用方能够核验回答、显示证据子图，并追踪答案引用来源。

### 7.5 Generation 边界

Generation 只消费已经完成 Budget 和 Evidence 的结果：

```text
query + evidence.llmContext → LLM → AnswerResult
```

以下能力不纳入当前 Generation：

- 由 LLM 自动修改前面的检索流水线。
- 生成中再次自主调用 Recall 或 Expand。
- 自动查询规划和算子重排。
- 无证据约束的多轮 Agent 检索。

这些能力属于后续迭代式或 Agentic GraphRAG，而不是当前显式单次流水线。

### 7.6 验收标准

- 能够按需捕获完整 `EvidenceResult`，并在 LLM 不可用时作为降级结果保留。
- 配置 LLM 后能够返回 `AnswerResult`及有效引用。
- 所有 Citation 均能解析到 Evidence 中的文本块、节点、边或路径。
- Evidence 序列化结果不超过 Budget 的 token 上限。
- LLM 超时或失败时返回 Evidence 和结构化错误。
- 记录首 token 延迟、总生成延迟、prompt tokens 和 completion tokens。

---

## 8. 类型兼容与流水线规则

当前方案不做自动规划，但必须做静态或执行前类型校验。

| 输入 | 允许的 Expand |
|---|---|
| `AnchorSet` | `neighbors`、`boundedBfs`、`ppr`、`constrainedPaths` |
| `CandidateSet<GraphElementRef>` | `neighbors`、`boundedBfs`、`ppr` |
| 带顶点关联的 `CandidateSet<ContentRef>` | `neighbors`、`boundedBfs`、`ppr` |
| 两组 Anchor/Candidate | `shortestPaths` |

必须拒绝的情况：

- 对无法映射到顶点的候选执行图邻接。
- 没有端点集合就执行 `shortestPaths()`。
- 没有任何 Recall 结果就执行 Expand。
- 预算参数为零或负数。
- 对不支持的图后端要求特定能力。

每个 Expand 算子都必须返回：

- 是否完整执行或因预算截断。
- 实际访问节点数和边数。
- 执行耗时。
- 输入结果与新增结果之间的 provenance。

---

## 9. 最低可运行 API 闭环

### 9.1 实体中心查询

```java
g.retrieve("乔布斯和苹果公司是什么关系？")
 .entityLink()
 .maxCandidatesPerMention(3)
 .shortestPaths()
    .maxLength(3)
    .maxPathsPerPair(10)
 .budget()
 .evidence()
 .generate()
 .execute();
```

`shortestPaths()`的项目优先级为 P1。只使用 P0 算子时，可以先返回两个 Anchor 集合各自的一跳邻居，再在后续阶段求交集。

### 9.2 语义检索查询

```java
g.retrieve("社区结构如何改善图数据访问局部性？")
 .vectorRecall()
 .topK(50)
 .boundedBfs()
    .maxDepth(2)
    .maxVisitedVertices(2000)
    .maxTraversedEdges(10000)
    .timeoutMillis(100)
 .budget()
 .evidence()
 .generate()
 .execute();
```

### 9.3 完整 GraphRAG 问答

```java
AnswerResult answer = g.retrieve(query)
 .entityLink()
 .boundedBfs()
    .maxDepth(2)
    .maxVisitedVertices(2000)
    .maxTraversedEdges(10000)
 .pruneCandidates()
    .unit(TRIPLE)
    .scorer(EMBEDDING)
    .keepTopK(100)
 .budget()
    .maxVertices(40)
    .maxEdges(80)
    .maxTokens(4000)
 .evidence()
 .generate()
 .execute();
```

执行时可以按需捕获 `EvidenceResult`以及更早阶段的类型化中间结果；默认返回值仍为 `AnswerResult`。

---

## 10. 实施顺序与现状

下列里程碑是设计时的实施顺序。**是否已落地以契约 §8 为准。**

当前最小可运行闭环由 `retrieve(q).hippoRag().execute()` 提供（实体链接、加权 PPR、事实投影，再接 Budget / Evidence / Generation），**不依赖**里程碑 C 的 `neighbors()` / `boundedBfs()`。

| 里程碑 | 状态 |
|---|---|
| A 公共类型 | 阶段类型已在契约与代码中定义；逐步统计未作为独立模块完成 |
| B Recall P0 | `textRecall()`、`vectorRecall()` 已落地；`entityLink()` 走 HippoRAG 1 路径 |
| C Expand P0 | `neighbors()`、`boundedBfs()` **未落地** |
| D Prune P0 | **未落地** |
| E P1 图检索 | `ppr()` 已在 HippoRAG 1 中落地；路径类算子未落地 |
| F P2 研究策略 | **未落地** |
| G Evidence 与 Generation | Budget / Evidence / `generate()` / `AnswerResult` 已落地 |

未完成项仍按原顺序推进，条文保留如下。

### 里程碑 A：公共类型与可观测性

1. 定义 `QueryContext`、`AnchorSet`、`CandidateSet`、`PathSet`。
2. 定义统一 `ScoreSemantics` 和 `Provenance`。
3. 定义算子输入/输出类型检查。
4. 建立每步输入数、输出数、访问边数和耗时统计。

### 里程碑 B：Recall 的 P0 算子

1. `textRecall()`。
2. `vectorRecall()`。
3. `entityLink()`的名称/别名基线。
4. Recall@K、MRR、nDCG 和延迟评测。

### 里程碑 C：Expand 的 P0 算子

1. `neighbors()`及底层批量邻接 API。
2. `boundedBfs()`。
3. 深度、节点、边和超时预算。
4. 来源路径与截断状态。

完成 A—C 后即可得到首个最小 GraphRAG 检索闭环。

### 里程碑 D：Prune 的 P0 算子

1. `pruneCandidates()`。
2. 支持 `EXISTING_SCORE` 和 `EMBEDDING`。
3. 支持节点和三元组两种筛选单位。
4. 测量 Evidence Recall、压缩率和额外延迟。

### 里程碑 E：P1 图检索

建议按以下顺序推进：

1. `ppr()`。
2. `shortestPaths()`。
3. `constrainedPaths()`。
4. `prunePaths()`。
5. 使用图距离和邻接关系增强 `entityLink()` 消歧。

### 里程碑 F：P2 研究策略

1. `beamPaths()`。
2. `randomWalk()`基线。
3. `selectConnectedSubgraph(PCST)`。
4. `knowledgeFilter()`及其无 LLM 降级路径。
5. 对比 BFS、PPR、最短路径和 Beam Search。
6. 对比 Expand 内部剪枝与后置路径筛选。

### 里程碑 G：Evidence 与 Generation

1. P0：定义 `EvidenceResult`、结构化证据和统一 provenance。
2. P0：实现与 Budget 共用的文本序列化和 token 计数。
3. P0：定义 LLM provider 接口并接入一个适配器。
4. P0：定义 `AnswerResult`和可验证 Citation。
5. 实现 LLM 超时、失败降级和调用指标。

---

## 11. 相关论文

按写入 / 检索阶段整理的清单见 [`graph-retrieval-papers.md`](graph-retrieval-papers.md)。下文为实施方案中直接引用过的文献。

1. **LEGO-GraphRAG: Modularizing Graph-based Retrieval-Augmented Generation for Design Space Exploration**, PVLDB 2025.  
   https://www.vldb.org/pvldb/vol18/p3269-cao.pdf
2. **HippoRAG: Neurobiologically Inspired Long-Term Memory for Large Language Models**, NeurIPS 2024.  
   https://papers.nips.cc/paper_files/paper/2024/file/6ddc001d07ca4f319af96a3024f6dbd1-Paper-Conference.pdf
3. **Reasoning on Graphs: Faithful and Interpretable Large Language Model Reasoning**, ICLR 2024.  
   https://proceedings.iclr.cc/paper_files/paper/2024/file/3e2aeb66481dd63a32421bf032b70384-Paper-Conference.pdf
4. **Think-on-Graph: Deep and Responsible Reasoning of Large Language Model on Knowledge Graph**, ICLR 2024.  
   https://proceedings.iclr.cc/paper_files/paper/2024/file/10a6bdcabbd5a3d36b760daa295f63c1-Paper-Conference.pdf
5. **G-Retriever: Retrieval-Augmented Generation for Textual Graph Understanding and Question Answering**, NeurIPS 2024.  
   https://proceedings.neurips.cc/paper_files/paper/2024/file/efaf1c9726648c8ba363a5c927440529-Paper-Conference.pdf
6. **PullNet: Open Domain Question Answering with Iterative Retrieval on Knowledge Bases and Text**, EMNLP-IJCNLP 2019.  
   https://aclanthology.org/D19-1242.pdf
7. **GNN-RAG: Graph Neural Retrieval for Efficient Large Language Model Reasoning on Knowledge Graphs**, ACL Findings 2025.  
   https://aclanthology.org/2025.findings-acl.856/
8. **PropRAG: Guiding Retrieval with Beam Search over Proposition Paths**, EMNLP 2025.  
   https://aclanthology.org/2025.emnlp-main.317/
9. **Empowering GraphRAG with Knowledge Filtering and Integration**, EMNLP 2025.  
   https://aclanthology.org/2025.emnlp-main.1293/
