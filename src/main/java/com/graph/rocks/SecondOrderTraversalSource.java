package com.graph.rocks;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Extended GraphTraversalSource with support for second-order logic graph queries
 * Adds fluent API entry points for vertex subset and second-order queries while maintaining
 * full compatibility with standard Gremlin traversal operations
 */
public class SecondOrderTraversalSource extends GraphTraversalSource {

    /**
     * 显式序列化方法，确保父类状态正确写入
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject(); // 序列化子类字段（当前无，但未来可能有）
        // 如需自定义序列化父类状态，可在此处补充
    }

    /**
     * 显式反序列化方法，确保父类状态正确恢复
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject(); // 反序列化子类字段
        // 如需自定义恢复父类状态，可在此处补充
    }

    /**
     * Create a SecondOrderTraversalSource with default traversal strategies
     * @param graph Parent graph instance
     */
    public SecondOrderTraversalSource(final Graph graph) {
        super(graph);
    }

    /**
     * Create a SecondOrderTraversalSource with custom traversal strategies
     * @param graph Parent graph instance
     * @param strategies Custom TraversalStrategies for query execution
     */
    public SecondOrderTraversalSource(final Graph graph, final TraversalStrategies strategies) {
        super(graph, strategies);
    }

    /**
     * Entry point for building second-order logic queries
     * @return SecondOrderQueryBuilder instance for constructing complex queries
     */
    public SecondOrderQueryBuilder secondOrder() {
        return new SecondOrderQueryBuilder(this);
    }

    /**
     * Entry point for building vertex subset queries (Vset)
     * @return VertexSubsetQueryBuilder instance for vertex set operations
     */
    public VertexSubsetQueryBuilder Vset() {
        return new VertexSubsetQueryBuilder(this);
    }

    /**
     * Clone the traversal source with copied traversal strategies
     * Maintains proper object inheritance and strategy isolation
     * @return Cloned SecondOrderTraversalSource instance
     */
    @Override
    public SecondOrderTraversalSource clone() {
        return new SecondOrderTraversalSource(this.graph, this.getStrategies().clone());
    }
}