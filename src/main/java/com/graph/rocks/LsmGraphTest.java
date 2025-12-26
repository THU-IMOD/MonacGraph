package com.graph.rocks;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class LsmGraphTest {
    private static final Logger logger = LoggerFactory.getLogger(LsmGraphTest.class);
    private static GraphTraversalSource g; // 全局遍历源

    /**
     * 初始化LsmGraph会话（改用Java API）
     */
    private static void initializeSession() throws Exception {
        logger.info("\n{}", "=".repeat(60));
        logger.info("Initializing LsmGraph Session");
        logger.info("=".repeat(60));

        // 直接初始化LsmGraph和遍历源（替换原字符串脚本）
        // 注意：这里需要根据你的LsmGraph实际初始化方式调整
        // 如果是远程连接，使用：g = traversal().withRemote(DriverRemoteConnection.using(client));
        // 如果是本地图，使用：Graph graph = LsmGraph.open("my_database"); g = graph.traversal();
        // 以下是通用写法，你需要根据实际环境调整


        // 假设你使用的是远程连接（适配原Client方式）
        // DriverRemoteConnection conn = DriverRemoteConnection.using(client);
        // g = traversal().withRemote(conn);

        logger.info("Session initialized with LsmGraph and 'g' traversal source");
        logger.info("=".repeat(60));
    }

    /**
     * 执行测试（全Java API实现，无字符串脚本）
     */
    private static void runTests() throws Exception {
        g = LsmGraph.open("my_database").traversal();
        logger.info("\n{}", "=".repeat(60));
        logger.info("Running LsmGraph Tests");
        logger.info("=".repeat(60));

        // 测试 0: 清空图数据
        logger.info("\nTest 0: Clearing graph");
        g.V().drop().iterate(); // 直接执行删除遍历
        logger.info("Graph cleared: [cleared]");

        // 测试 1: 添加顶点
        logger.info("\nTest 1: Adding vertices");
        Vertex alice = g.addV("user")
                .property("name", "Alice")
                .property("age", 28)
                .next();
        logger.info("Added Alice: [Alice added], vertex id: {}", alice.id());

//        Vertex bob = g.addV("user")
//                .property("name", "Bob")
//                .property("age", 30)
//                .next();
//        logger.info("Added Bob: [Bob added], vertex id: {}", bob.id());
//
//        Vertex phone = g.addV("product")
//                .property("name", "手机")
//                .property("price", 2999)
//                .next();
//        logger.info("Added Phone: [Phone added], vertex id: {}", phone.id());
//
//        // 测试 2: 查询顶点
//        logger.info("\nTest 2: Querying vertices");
//        List<Map<Object, Object>> users = g.V().hasLabel("user")
//                .valueMap("name", "age")
//                .toList();
//        logger.info("All users: {}", users);
//
//        // 测试 3: 添加边
//        logger.info("\nTest 3: Adding edges");
//        // 方式1：直接使用已保存的顶点对象（更高效）
//        Edge friendEdge = g.addE("friend")
//                .from(alice)
//                .to(bob)
//                .property("since", 2020)
//                .next();
//
//        Edge buyEdge = g.addE("buy")
//                .from(alice)
//                .to(phone)
//                .property("time", 2025)
//                .next();
//
//        // 方式2：如果没有保存顶点对象，也可以通过查询获取（兼容原逻辑）
//        // Vertex aliceFromQuery = g.V().has("name", "Alice").next();
//
//        logger.info("Added edges: [Edges added], friend edge id: {}, buy edge id: {}",
//                friendEdge.id(), buyEdge.id());
//
//        // 测试 4: 遍历查询
//        logger.info("\nTest 4: Traversal queries");
//        List<Object> aliceFriends = g.V().has("name", "Alice")
//                .out("friend")
//                .values("name")
//                .toList();
//        logger.info("Alice's friends: {}", aliceFriends);
//
//        List<Map<Object, Object>> alicePurchases = g.V().has("name", "Alice")
//                .out("buy")
//                .valueMap("name", "price")
//                .toList();
//        logger.info("Alice's purchases: {}", alicePurchases);
//
//        // 测试 5: 统计查询
//        logger.info("\nTest 5: Count queries");
//        Long totalVertices = g.V().count().next();
//        logger.info("Total vertices: [{}]", totalVertices);
//
//        Long totalEdges = g.E().count().next();
//        logger.info("Total edges: [{}]", totalEdges);

        // 测试 6: 修改属性
        logger.info("\nTest 6: Update property");
        g.V().has("name", "Alice").property("age", 29).next();

        Object aliceNewAge = g.V().has("name", "Alice").values("age").next();
        logger.info("Alice's updated age: [{}]", aliceNewAge);

        logger.info("\n{}", "=".repeat(60));
        logger.info("All LsmGraph tests completed successfully!");
        logger.info("=".repeat(60));
    }

    // 主方法示例（供测试）
    public static void main(String[] args) {
        try {
            initializeSession();
            runTests();
        } catch (Exception e) {
            logger.error("Test failed", e);
            e.printStackTrace();
        }
    }
}