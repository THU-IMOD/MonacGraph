# 各阶段可参考论文

与 [`graph-retrieval-intro.md`](graph-retrieval-intro.md) 中的写入、检索两条线对齐。做某一阶段时选读，不是按顺序通读。流水线如何拆成算子，先看 LEGO-GraphRAG。

算子语义与项目内的论文对照见 [`graph-retrieval-recall-expand.md`](graph-retrieval-recall-expand.md)。本文只列阶段与文献的对应关系。

---

## 总览

| 论文 | venue | 主要对应 |
|---|---|---|
| [LEGO-GraphRAG](https://www.vldb.org/pvldb/vol18/p3269-cao.pdf) | PVLDB 2025 | 把 GraphRAG 拆成可替换模块，对照整条检索链 |
| [From Local to Global: GraphRAG](https://arxiv.org/abs/2404.16130) | arXiv 2024 | 文档抽成图、社区摘要；写入侧的完整例子 |
| [HippoRAG](https://papers.nips.cc/paper_files/paper/2024/file/6ddc001d07ca4f319af96a3024f6dbd1-Paper-Conference.pdf) | NeurIPS 2024 | 抽取短语与事实、同义边、PPR 扩图 |
| [Retrieval-Augmented Generation](https://arxiv.org/abs/2005.11401) | NeurIPS 2020 | 检索后再生成；Generation 的基本设定 |

---

## 一、写入

| 步骤 | 可参考 | 读什么 |
|---|---|---|
| 切片 | [RAPTOR](https://arxiv.org/abs/2401.18059) | 分层摘要切片，不只切固定字数 |
| 切片 | [Late Chunking](https://arxiv.org/abs/2409.04701) | 先编码长文再切，减轻切片边界丢上下文 |
| 知识抽取 | [Stanford OpenIE](https://aclanthology.org/D15-1076.pdf) | 从句子抽出开放关系三元组 |
| 知识抽取 | HippoRAG | 用 LLM 做 OpenIE 式实体与事实，再链到短语节点 |
| 知识抽取 | Microsoft GraphRAG | 实体、关系、社区摘要的文档建图流程 |
| 写入图 | HippoRAG；Microsoft GraphRAG | 前者偏短语–事实二部图；后者偏实体图 + 社区 |
| 建立索引 | [BM25](https://www.staff.city.ac.uk/~sb317/papers/foundations_bm25_review.pdf) | 关键词倒排，对应 `textRecall()` |
| 建立索引 | [DPR](https://arxiv.org/abs/2004.04906) | 段落级稠密检索 |
| 建立索引 | [Contriever](https://arxiv.org/abs/2112.09118) | 无监督稠密检索；HippoRAG 1 的对照路径 |
| 建立索引 | [BGE](https://arxiv.org/abs/2309.07597) | 多语言句向量；当前默认 embedding 一类工作 |
| 建立索引 | [ColBERT](https://arxiv.org/abs/2004.12832) | 词级交互检索；HippoRAG 官方另一路编码器 |

---

## 二、检索

| 阶段 | 可参考 | 读什么 |
|---|---|---|
| Recall（关键词） | BM25 | 全文召回 |
| Recall（向量） | DPR；Contriever；BGE | 用查询向量取相似段落 |
| Recall（实体链接） | HippoRAG | mention → 短语节点，并用文档频率加权 |
| Expand（邻居 / BFS） | [Think-on-Graph](https://proceedings.iclr.cc/paper_files/paper/2024/file/10a6bdcabbd5a3d36b760daa295f63c1-Paper-Conference.pdf) | 从实体出发沿关系向外探索 |
| Expand（PPR） | HippoRAG | 个性化 PageRank 在短语图上扩散 |
| Expand（关系路径） | [RoG](https://proceedings.iclr.cc/paper_files/paper/2024/file/3e2aeb66481dd63a32421bf032b70384-Paper-Conference.pdf) | 先规划关系序列，再在图上实例化路径 |
| Expand（最短路） | [GNN-RAG](https://aclanthology.org/2025.findings-acl.856/) | 查询实体到答案候选的最短路径作证据 |
| Expand（Beam） | [PropRAG](https://aclanthology.org/2025.emnlp-main.317/) | 命题路径上的 beam search |
| Expand（迭代拉事实） | [PullNet](https://aclanthology.org/D19-1242.pdf) | 多轮从图和文本拉取新事实 |
| Prune（点/边/三元组） | LEGO-GraphRAG | Node / Edge / Triple pruning 的分类 |
| Prune（连通子图） | [G-Retriever](https://proceedings.neurips.cc/paper_files/paper/2024/file/efaf1c9726648c8ba363a5c927440529-Paper-Conference.pdf) | 用 PCST 选出与查询相关的连通子图 |
| Prune（两阶段过滤） | [GraphRAG-FI](https://aclanthology.org/2025.emnlp-main.1293/) | 对已检索知识做粗过滤再细过滤 |
| Fusion / Fuse | [RRF](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf) | 多路排名互惠融合，不直接加不同量纲的分数 |
| Fusion / Rerank | BGE；ColBERT | 用交叉编码器或词级交互做精排 |
| Budget | G-Retriever | 在 token / 节点预算内保留子图 |
| Evidence / Generation | RAG（Lewis et al.） | 证据进提示、回答须能回溯到来源 |
| Evidence / Generation | Microsoft GraphRAG | 局部检索与全局社区摘要两种回答方式 |

当前框架里的 `hippoRag()` 主要对齐 HippoRAG 1（Contriever 风格，不含 ColBERT）。换 Expand 或 Prune 时，优先看上表中该阶段的论文，而不是整篇重写流水线。
