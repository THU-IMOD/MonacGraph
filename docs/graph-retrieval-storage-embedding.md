# MonacGraph 内容存储与 Embedding

**标识空间、摄取阶段类型与存储接口以 [`graph-retrieval-contracts.md`](graph-retrieval-contracts.md) 为准。** 本文保留目录布局、embedding 运维与恢复策略；与契约冲突时先改契约再改本文。

## 1. 范围

该模块为 GraphRAG 检索提供 passage、图元素来源关系、BM25 索引和向量索引。图拓扑仍由 LSM-Community 管理；检索模块不持久化 Rust 内部顶点 handle。

默认目录：

```text
workspace/<graphName>/retrieval/
├── content_db/    # passage、双向关联、embedding outbox
├── entity_catalog/ # 规范实体和别名
├── ingestion_jobs/ # 摄取状态与阶段产物
└── lucene/         # 可重建的 BM25 与 HNSW 索引
```

RocksDB 内容库是权威数据源，Lucene 是派生索引。Lucene 损坏或模型空间改变时，可以从内容库重建。

## 2. 数据模型

`ContentRecord`保存文本正文、文档来源、内容哈希、版本以及图元素关联。关联角色包括：

- `MENTIONS`：passage 提到某个顶点。
- `SUPPORTS`：passage 支持某条边或事实。

内容库原子维护：

```text
contentId → ContentRecord
GraphElementRef → contentId[]
contentId + spaceId → EmbeddingJob
```

图元素 ID 使用外部 TinkerPop ID 的规范化编码。检索索引不得保存 LSM-Community 的内部 `u32` ID。

## 3. 写入流程

```text
RetrievalIngestor.ingest
→ RocksDB 写入 ContentRecord 与双向关联
→ Lucene 写入可搜索文本
→ RocksDB 写入 PENDING embedding job

EmbeddingIndexer.runBatch
→ 批量读取 passage
→ 调用 EmbeddingProvider
→ Lucene 写入向量
→ job 标记为 READY 或 FAILED
```

`contentHash + spaceId`用于避免旧任务覆盖新内容。权威写入先于派生索引，因此索引失败后可以重新执行导入或补建。

## 4. 本机 Embedding 服务

推荐用仓库脚本（会把模型下载到 `{repo}/.cache/huggingface`，无需设置 `HF_HOME`）：

```bash
# Windows
powershell -ExecutionPolicy Bypass -File .\scripts\start-embedding.ps1
# Linux / macOS
bash scripts/start-embedding.sh
```

等价的手工安装：

```bash
cd embedding-service
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
.venv/Scripts/python -m uvicorn app:app --host 127.0.0.1 --port 8099
```

Linux/macOS 将 `.venv/Scripts`替换为 `.venv/bin`。服务启动时会把 Hugging Face 缓存设到仓库 `.cache/`，除非环境里已经有 `HF_HOME`。

默认模型为 `BAAI/bge-small-zh-v1.5`。可通过环境变量配置：

```text
EMBEDDING_MODEL
EMBEDDING_REVISION
EMBEDDING_QUERY_PROMPT
EMBEDDING_NORMALIZED
```

服务提供：

```text
GET  /health
GET  /model-info
POST /embed/documents
POST /embed/query
```

MonacGraph 默认连接 `http://127.0.0.1:8099/`。设置 `retrieval.embedding.manageProcess=true`时，MonacGraph 可以启动和关闭该进程；生产环境建议由外部进程管理器负责。

## 5. Java 配置

```properties
retrieval.path=./workspace/my_graph/retrieval
retrieval.embedding.uri=http://127.0.0.1:8099/
retrieval.embedding.spaceId=bge-small-zh-v1.5-v1
retrieval.embedding.model=BAAI/bge-small-zh-v1.5
retrieval.embedding.revision=main
retrieval.embedding.dimension=512
retrieval.embedding.normalized=true
retrieval.embedding.manageProcess=false
```

模型名称、revision、维度、归一化和 query prompt 共同定义向量空间。任一项变化都应使用新的 `spaceId`并重建 passage 向量。

## 6. 当前接入 API

`CommunityGraph`惰性创建并管理 `RetrievalServices`。

仅召回（不跑生成）：

```java
List<ContentCandidate> results = graph.traversal()
    .retrieve("乔布斯与苹果公司有什么关系？")
    .textRecall()
    .topK(20)
    .executeRecall();
```

默认跑到回答：

```java
AnswerResult answer = graph.traversal()
    .retrieve("乔布斯与苹果公司有什么关系？")
    .hippoRag()
    .execute();
```

`textRecall()`不需要 embedding 服务。`vectorRecall()` 与 `hippoRag()` 需要本机 embedding。通用 Expand（`neighbors()`、`boundedBfs()`）、Prune 与通用 Fusion 尚未实现，对照见契约 §8。

## 7. 故障与恢复

- Embedding 服务不可用：BM25 仍可工作，任务保留状态后可重新入队。
- Lucene 更新失败：RocksDB 中的 passage 和关联不丢失。
- 模型空间不匹配：查询和索引应立即失败，不允许比较不同模型生成的向量。
- 删除 passage：同时删除权威内容、反向关联和 Lucene 文档。
- 默认 CI 使用 fake `EmbeddingProvider`，不下载真实模型。

## 8. 自动文档摄取

`CommunityGraph.ingestion()`返回当前图的 `IngestionCoordinator`：

```java
IngestionJob submitted = graph.ingestion().submit(Path.of("documents/apple.pdf"));
IngestionJob completed = graph.ingestion().run(submitted.jobId());
graph.ingestion().delete(submitted.jobId()); // 删除 passage、索引和任务产物
```

默认处理流程：

```text
PENDING
→ PARSED
→ CHUNKED
→ EXTRACTED
→ GRAPH_WRITTEN
→ INDEXED
→ READY
```

- Apache Tika 解析 PDF、Office、HTML和文本。
- `ParagraphAwareChunker`生成稳定 passage ID并保留原文 offset。
- OpenAI-compatible extractor 输出经过校验的实体和关系 JSON。
- `ExactAliasEntityResolver`只合并精确名称和无歧义别名。
- 顶点和边使用确定性 ID，任务重试不会产生新的图元素。
- passage 通过 `MENTIONS`关联顶点，通过 `SUPPORTS`关联边。
- 每个阶段及其产物写入 RocksDB；失败任务从最后成功 checkpoint 恢复。

删除摄取任务不会直接删除共享顶点和边，因为其他文档可能仍然支持这些图元素；孤立图元素清理需要在全局 provenance 检查后另行执行。

配置示例：

```properties
ingestion.extractor.url=http://127.0.0.1:11434/v1/chat/completions
ingestion.extractor.model=qwen3:8b
ingestion.extractor.apiKeyEnv=MONACGRAPH_LLM_API_KEY
ingestion.extractor.timeoutSeconds=120
ingestion.extractor.maxConcurrency=2
ingestion.chunk.maxCharacters=1600
ingestion.chunk.overlapCharacters=200
ingestion.embedding.batchSize=32
ingestion.maxAttempts=3
ingestion.worker.enabled=true
ingestion.worker.intervalMillis=1000
```

`ingestion.worker.enabled=false`时，可由测试、CLI或应用显式调用 `runPending(limit)`。

## 9. 批量 CLI

先启动 embedding 服务和 OpenAI-compatible 抽取服务，再执行：

```bash
mvn exec:java \
  -Dexec.mainClass=db.monacgraph.ingestion.IngestionCli \
  -Dexec.args="--db demo --input ./documents --recursive \
  --extractor-url http://127.0.0.1:11434/v1/chat/completions \
  --extractor-model qwen3:8b \
  --embedding-uri http://127.0.0.1:8099/"
```

CLI 对每个文件输出 `READY`或 `FAILED`、job ID和路径。重复导入内容未变化的同一路径会返回同一任务，不重复建图。

## 10. 抽取服务响应约束

抽取模型必须返回：

```json
{
  "entities": [
    {"localId": "e1", "name": "Steve Jobs", "type": "person", "aliases": []}
  ],
  "relations": [
    {
      "source": "e1",
      "relation": "co-founded",
      "target": "e2",
      "evidence": "Steve Jobs co-founded Apple."
    }
  ]
}
```

关系端点必须引用同一 passage 的实体 `localId`。空名称、重复 localId、空关系以及未知端点都会使该任务进入 `FAILED`，不会写入图。
