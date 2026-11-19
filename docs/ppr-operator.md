# Personalized PageRank 算子

调用 `g.PPR(1).reset(0.15).topK(2).execute()` 返回按分数降序排列的 `Map<Vertex, Double>`。多起点可写为 `g.PPR(1, 3)` 或 `g.PPR([1, 3])`，起点按实际顶点 ID 去重并均匀分配重启概率。起点必须存在。

本实现采用有向、无权 PPR。每条出边具有相同转移概率，保留平行边和自环的转移贡献。没有出边的顶点把其传播概率重新分配到起点分布。原图及其属性不被修改。

令 p 为起点上的均匀分布，α 为重启概率，P 为沿出边转移的矩阵（无出边的行替换为 p），用幂迭代求解 `r = αp + (1−α)Pᵀr`。停止条件为相邻迭代的 L1 差值不超过 `α × 1e-10`，最多 10000 次。超过上限明确报错，提示增大重启概率。

默认 α=0.15、topK=50。α 必须是 (0,1] 内的有限数，topK 必须为非负整数。topK=0 返回空结果，topK 超过总点数时返回全部点，包括分数为零的点。topK 截取后不重新归一化分数。相同分数按顶点 ID 的类型名及字符串表示排序。

`executeForWeb()` 展示选中点集的诱导子图，网页节点的属性保持为原图属性。需要查看分数时可使用下面的查询。

```groovy
g = graph.traversal(SecondOrderTraversalSource.class);
g.PPR(1).reset(0.15).topK(4).execute().collect { v, score ->
    [id: v.id(), name: v.value('name'), score: score]
}
```

## 演示

使用现有 Create Test Data 创建的 Alice(1)→Bob(2)→Charlie(3)→Alice(1) 三角形，以及孤立点 David(4)。

```groovy
g = graph.traversal(SecondOrderTraversalSource.class);
g.PPR(1).reset(0.15).topK(2).executeForWeb()
```

结果应为 Alice、Bob 两个点和 Alice→Bob 一条边。完整分数约为 Alice=0.3887269193，Bob=0.3304178814，Charlie=0.2808551992，David=0。

```groovy
g.PPR(4).topK(1).executeForWeb()
g.PPR([1, 3]).reset(1.0).topK(2).executeForWeb()
```

以上查询分别应显示 David 单点，以及 Alice、Charlie 两点和 Charlie→Alice 一条边。后者两个点分数各为 0.5。

## 验证

`mvn test` 运行 PprQueryBuilderTest 的 5 个测试，覆盖有向环的解析解、概率和、topK 的网页序列化、悬挂点、孤立起点、多起点去重、自环及平行边、空 topK 和参数异常。算法每轮复杂度为 O(|V|+|E|)，排序为 O(|V|log|V|)，内存为 O(|V|+|E|)。
