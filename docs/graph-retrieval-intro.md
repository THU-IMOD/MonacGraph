# MonacGraph 图检索流水线

拟在 MonacGraph 中做两件事，彼此衔接、入口分开：

1. **写入**：把文档切成段落、抽成图、建好检索索引。
2. **检索**：用与 Gremlin 相同的链式调用（Fluent API）写出图检索过程，按书写顺序执行，不自动改流水线。

当前两条线都有一版用 AI 写成的简单框架，用来验证通路。具体实现可由有兴趣的同学补充、替换或增加新步骤。

---

## 一、写入

文档不走 Gremlin 检索链。写入负责把原文变成图上的点和边，以及可供召回的文本 / 向量索引。

```text
文档
→ 解析
→ 切片
→ 知识抽取
→ 写入图
→ 建立索引
```

| 步骤 | 作用 |
|---|---|
| 解析 | 读入文件，得到正文。 |
| 切片 | 按段落等把正文切成检索单元。 |
| 知识抽取 | 从切片中抽出实体与关系。 |
| 写入图 | 把实体、关系落到图存储。 |
| 建立索引 | 为切片建立关键词索引和向量索引，供后面 Recall 使用。 |

可做的工作包括：换解析器、改切片策略、改进抽取、换编码器或索引结构。检索只使用这里已经写好的内容和图，不在回答时重新读整篇文档。

---

## 二、检索

目标写法如下，最终得到带引用的回答。

```text
g.retrieve("问句")
 .recall()
 .expand()
 .budget()
 .evidence()
 .generation()
 .execute()
```

`recall()`、`expand()` 表示阶段。实现时换成具体算子，例如按关键词召回、按向量召回、按一跳邻居扩展。

方括号内的阶段可以不做；不做时仍按这一顺序衔接。Expand、Prune、Fusion 都可以跳过，召回之后直接进入 Budget。

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

| 阶段 | 作用 |
|---|---|
| Query | 用户的问题。 |
| Recall | 从索引或图元素中找出第一批相关候选。 |
| Expand | 从这些候选出发，沿图向外扩展邻居或路径。 |
| Prune（可选） | 去掉低相关或重复的结果。 |
| Fusion（可选） | 把多路召回合成一路排序结果。 |
| Fuse | Fusion 内部：合并各路候选与来源。 |
| Rerank（可选） | Fusion 内部：再精排一次。 |
| Budget | 按数量和长度上限，选出准备交给模型的内容。 |
| Evidence | 把选中的对象整理成可核对的证据，得到 `EvidenceResult`。 |
| Generation | 根据问题与证据生成自然语言回答。 |
| AnswerResult | 最终结果：回答、引用与所用证据。 |

约定：

1. 问题只来自 Query。`evidence()` 整理证据，不改写问题。
2. 没有召回结果时，不能做 Expand。
3. 进入 Budget 之后，不再召回、不再扩图。
4. Generation 不再检索；回答中的引用必须能对回 Evidence 里已有的对象。

可做的工作包括：实现或更换某一阶段的算子（召回、扩图、剪枝、融合、生成等），以及增加新的算子。新步骤须能接到上述流水线中。

各阶段可参考的论文见 [`graph-retrieval-papers.md`](graph-retrieval-papers.md)。
