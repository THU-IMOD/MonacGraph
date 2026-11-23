
package db.monacgraph.jni;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class PCSTJNITest {

    @Test
    void testNativePCST() {

        System.out.println("\n========== PCST JNI TEST ==========");

        // 1. 检查动态库
        assertTrue(
            PCSTJNI.isAvailable(),
            "PCST native library failed to load"
        );

        System.out.println("[OK] Native library loaded");


        // 2. 创建 PCSTJNI
        PCSTJNI pcst = new PCSTJNI();


        // 3. 构造测试图
        //
        // 0 = 7001
        // 1 = 7002
        // 2 = 7003
        // 3 = 7004

        int vertexCount = 4;

        int[] edgeU = {
            0, 1, 1
        };

        int[] edgeV = {
            1, 2, 3
        };

        double[] prizes = {
            10.0,
            0.0,
            8.0,
            1.0
        };

        double[] costs = {
            2.0,
            2.0,
            5.0
        };


        // 4. 调用 JNI
        System.out.println("[RUN] Calling native PCST...");

        int[][] result = pcst.solve(
            vertexCount,
            edgeU,
            edgeV,
            prizes,
            costs
        );


        // 5. 检查结果
        assertNotNull(result);
        assertEquals(2, result.length);

        int[] selectedNodes = result[0];
        int[] selectedEdges = result[1];

        Arrays.sort(selectedNodes);
        Arrays.sort(selectedEdges);


        // 6. 检查节点
        assertArrayEquals(
            new int[]{0, 1, 2},
            selectedNodes
        );

        System.out.println("[OK] Selected nodes:");

        for (int index : selectedNodes) {
            System.out.println(
                "Vertex: " + (7001 + index)
            );
        }


        // 7. 检查边
        assertArrayEquals(
            new int[]{0, 1},
            selectedEdges
        );

        System.out.println("[OK] Selected edges:");

        for (int index : selectedEdges) {

            System.out.println(
                (7001 + edgeU[index])
                + " -- "
                + (7001 + edgeV[index])
            );
        }


        // 8. 验证目标函数
        double totalPrize = 0.0;
        double totalCost = 0.0;

        for (int index : selectedNodes) {
            totalPrize += prizes[index];
        }

        for (int index : selectedEdges) {
            totalCost += costs[index];
        }

        double objective = totalPrize - totalCost;

        assertEquals(18.0, totalPrize, 1e-9);
        assertEquals(4.0, totalCost, 1e-9);
        assertEquals(14.0, objective, 1e-9);


        System.out.println("-----------------------------------");

        System.out.println("Total Prize: " + totalPrize);
        System.out.println("Total Cost: " + totalCost);
        System.out.println("Objective: " + objective);

        System.out.println("PCST JNI TEST PASSED!");

        System.out.println("===================================\n");
    }
}
