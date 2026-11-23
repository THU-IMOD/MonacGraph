#include "pcst_fast.h"
#include <jni.h>
#include <algorithm>
#include <cmath>
#include <exception>
#include <string>
#include <utility>
#include <vector>

using namespace std;
using namespace cluster_approx;


// =====================================================
// 1. Java 异常辅助函数
// =====================================================

static void throwJavaException(
        JNIEnv* env,
        const char* className,
        const string& message) {

    if (env->ExceptionCheck()) {
        return;
    }

    jclass exceptionClass = env->FindClass(className);

    if (exceptionClass != nullptr) {
        env->ThrowNew(
            exceptionClass,
            message.c_str()
        );

        env->DeleteLocalRef(exceptionClass);
    }
}


// =====================================================
// 2. PCST 日志回调
// =====================================================

static void pcstLog(const char* message) {
    (void)message;
}


// =====================================================
// 3. JNI 主函数
//
// Java 类：db.monacgraph.jni.PCSTJNI
// Java 方法：nativeSolve
//
// Java 输入：
//   vertexCount
//   edgeU
//   edgeV
//   prizes
//   costs
//
// Java 输出：
//   result[0] = selected vertex indices
//   result[1] = selected edge indices
// =====================================================

extern "C" {

JNIEXPORT jobjectArray JNICALL
Java_db_monacgraph_jni_PCSTJNI_nativeSolve(

        JNIEnv* env,
        jobject /* thiz */,

        jint vertexCount,

        jintArray jEdgeU,
        jintArray jEdgeV,

        jdoubleArray jPrizes,
        jdoubleArray jCosts

) {

    // =================================================
    // Step 1. 检查输入参数
    // =================================================

    if (vertexCount <= 0) {

        throwJavaException(
            env,
            "java/lang/IllegalArgumentException",
            "Vertex count must be positive."
        );

        return nullptr;
    }

    if (jEdgeU == nullptr ||
        jEdgeV == nullptr ||
        jPrizes == nullptr ||
        jCosts == nullptr) {

        throwJavaException(
            env,
            "java/lang/IllegalArgumentException",
            "PCST input arrays must not be null."
        );

        return nullptr;
    }


    // =================================================
    // Step 2. 检查数组长度
    // =================================================

    jsize edgeCount = env->GetArrayLength(jEdgeU);

    if (env->GetArrayLength(jEdgeV) != edgeCount ||
        env->GetArrayLength(jCosts) != edgeCount ||
        env->GetArrayLength(jPrizes) != vertexCount) {

        throwJavaException(
            env,
            "java/lang/IllegalArgumentException",
            "PCST input array size mismatch."
        );

        return nullptr;
    }


    // =================================================
    // Step 3. 将 Java 数组读取到 C++ vector
    // =================================================

    vector<jint> edgeU(edgeCount);
    vector<jint> edgeV(edgeCount);

    vector<jdouble> prizes(vertexCount);
    vector<jdouble> costs(edgeCount);


    if (edgeCount > 0) {

        env->GetIntArrayRegion(
            jEdgeU,
            0,
            edgeCount,
            edgeU.data()
        );

        env->GetIntArrayRegion(
            jEdgeV,
            0,
            edgeCount,
            edgeV.data()
        );

        env->GetDoubleArrayRegion(
            jCosts,
            0,
            edgeCount,
            costs.data()
        );
    }


    env->GetDoubleArrayRegion(
        jPrizes,
        0,
        vertexCount,
        prizes.data()
    );


    if (env->ExceptionCheck()) {
        return nullptr;
    }


    // =================================================
    // Step 4. 构造 PCST 输入数据
    // =================================================

    vector<pair<int, int>> edges;

    edges.reserve(edgeCount);


    for (jsize i = 0; i < edgeCount; ++i) {

        int u = edgeU[i];
        int v = edgeV[i];

        // 检查端点范围
        if (u < 0 || u >= vertexCount ||
            v < 0 || v >= vertexCount) {

            throwJavaException(
                env,
                "java/lang/IllegalArgumentException",
                "Invalid edge endpoint index."
            );

            return nullptr;
        }

        // 检查边代价
        if (!std::isfinite(costs[i]) ||
            costs[i] < 0) {

            throwJavaException(
                env,
                "java/lang/IllegalArgumentException",
                "Edge costs must be finite and non-negative."
            );

            return nullptr;
        }

        edges.emplace_back(u, v);
    }


    // 检查节点奖励
    for (double prize : prizes) {

        if (!std::isfinite(prize) ||
            prize < 0) {

            throwJavaException(
                env,
                "java/lang/IllegalArgumentException",
                "Vertex prizes must be finite and non-negative."
            );

            return nullptr;
        }
    }


    // =================================================
    // Step 5. 准备 PCST 求解器输入
    // =================================================

    vector<double> prizeValues(
        prizes.begin(),
        prizes.end()
    );

    vector<double> costValues(
        costs.begin(),
        costs.end()
    );


    // =================================================
    // Step 6. 调用 PCSTFast
    // =================================================

    vector<int> selectedNodes;
    vector<int> selectedEdges;


    try {

        PCSTFast solver(

            edges,

            prizeValues,

            costValues,

            PCSTFast::kNoRoot,

            1,

            PCSTFast::kGWPruning,

            0,

            pcstLog

        );


        bool success = solver.run(
            &selectedNodes,
            &selectedEdges
        );


        if (!success) {

            throwJavaException(
                env,
                "java/lang/IllegalStateException",
                "PCST execution failed."
            );

            return nullptr;
        }


    } catch (const std::exception& e) {

        throwJavaException(
            env,
            "java/lang/IllegalStateException",
            e.what()
        );

        return nullptr;

    } catch (...) {

        throwJavaException(
            env,
            "java/lang/IllegalStateException",
            "Unknown error during PCST execution."
        );

        return nullptr;
    }


    // =================================================
    // Step 7. 排序结果
    // =================================================

    sort(
        selectedNodes.begin(),
        selectedNodes.end()
    );

    sort(
        selectedEdges.begin(),
        selectedEdges.end()
    );


    // =================================================
    // Step 8. 创建 Java int[][]
    //
    // result[0] = selectedNodes
    // result[1] = selectedEdges
    // =================================================

    jclass intArrayClass = env->FindClass("[I");

    if (intArrayClass == nullptr) {
        return nullptr;
    }


    jobjectArray result = env->NewObjectArray(
        2,
        intArrayClass,
        nullptr
    );

    if (result == nullptr) {

        env->DeleteLocalRef(intArrayClass);

        return nullptr;
    }


    // =================================================
    // Step 9. 创建节点索引数组
    // =================================================

    vector<jint> nodeValues(
        selectedNodes.begin(),
        selectedNodes.end()
    );


    jintArray nodesArray = env->NewIntArray(
        static_cast<jsize>(nodeValues.size())
    );

    if (nodesArray == nullptr) {

        env->DeleteLocalRef(result);
        env->DeleteLocalRef(intArrayClass);

        return nullptr;
    }


    if (!nodeValues.empty()) {

        env->SetIntArrayRegion(
            nodesArray,
            0,
            static_cast<jsize>(nodeValues.size()),
            nodeValues.data()
        );
    }


    // =================================================
    // Step 10. 创建边索引数组
    // =================================================

    vector<jint> edgeValues(
        selectedEdges.begin(),
        selectedEdges.end()
    );


    jintArray edgesArray = env->NewIntArray(
        static_cast<jsize>(edgeValues.size())
    );

    if (edgesArray == nullptr) {

        env->DeleteLocalRef(nodesArray);
        env->DeleteLocalRef(result);
        env->DeleteLocalRef(intArrayClass);

        return nullptr;
    }


    if (!edgeValues.empty()) {

        env->SetIntArrayRegion(
            edgesArray,
            0,
            static_cast<jsize>(edgeValues.size()),
            edgeValues.data()
        );
    }


    // =================================================
    // Step 11. 将两个数组放入 Java 二维数组
    // =================================================

    if (env->ExceptionCheck()) {

        env->DeleteLocalRef(nodesArray);
        env->DeleteLocalRef(edgesArray);
        env->DeleteLocalRef(result);
        env->DeleteLocalRef(intArrayClass);

        return nullptr;
    }


    env->SetObjectArrayElement(
        result,
        0,
        nodesArray
    );


    env->SetObjectArrayElement(
        result,
        1,
        edgesArray
    );


    // 释放 JNI 局部引用
    env->DeleteLocalRef(nodesArray);
    env->DeleteLocalRef(edgesArray);
    env->DeleteLocalRef(intArrayClass);


    if (env->ExceptionCheck()) {
        env->DeleteLocalRef(result);
        return nullptr;
    }


    // =================================================
    // Step 12. 返回 Java
    // =================================================

    return result;
}

} // extern "C"
