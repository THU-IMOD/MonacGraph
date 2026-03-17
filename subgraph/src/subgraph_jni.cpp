#include "graph.h"
#include "filter.h"
#include "enumerate.h"
#include <jni.h>
#include <iostream>

extern "C" {

/**
 * Java_com_graph_rocks_jni_SubgraphJNI_runSubgraphMatch
 *
 * Full pipeline: load → filter → order → backtrack (with Java phi callback).
 * Returns flat jlong[]: [matchCount, m0v0, m0v1, ..., m_{M-1}v_{qn-1}]
 */
JNIEXPORT jlongArray JNICALL
Java_db_monacgraph_jni_SubgraphJNI_nativeRunSubgraphMatch(
        JNIEnv*  env,
        jobject  /* thiz */,
        jstring  jDataPath,
        jstring  jQueryPath,
        jintArray jQuantifiers,
        jobject  jPhiCallback)
{
    /* ── 1. Read file paths ───────────────────────────────────── */
    const char* dataPath  = env->GetStringUTFChars(jDataPath,  nullptr);
    const char* queryPath = env->GetStringUTFChars(jQueryPath, nullptr);

    Graph      data;   data.load(dataPath);
    QueryGraph query;  query.load(queryPath);

    env->ReleaseStringUTFChars(jDataPath,  dataPath);
    env->ReleaseStringUTFChars(jQueryPath, queryPath);

    /* ── 2. Decode quantifiers ────────────────────────────────── */
    int   k    = env->GetArrayLength(jQuantifiers);
    jint* qArr = env->GetIntArrayElements(jQuantifiers, nullptr);
    std::vector<Quantifier> quantifiers(k);
    for (int i = 0; i < k; i++)
        quantifiers[i] = (qArr[i] == 1) ? EXISTS : FORALL;
    env->ReleaseIntArrayElements(jQuantifiers, qArr, JNI_ABORT);

    /* ── 3. Build phi closure that calls back to Java ─────────── *
     *                                                              *
     *  JNI method signature: evaluate([I)Z                        *
     *    [I = int array (vertex index per quantifier variable)     *
     *    Z  = boolean return                                       *
     *                                                              *
     *  We take a global ref to the callback object so it stays    *
     *  alive across the entire recursive backtrack call.          */
    jclass    callbackClass = env->GetObjectClass(jPhiCallback);
    jmethodID evaluateMid   = env->GetMethodID(callbackClass,
                                               "evaluate", "([I)Z");
    jobject   callbackRef   = env->NewGlobalRef(jPhiCallback);

    auto phi = [env, callbackRef, evaluateMid, k](const int* coords) -> bool {
        // Pack coords into a Java int[]
        jintArray jCoords = env->NewIntArray(k);
        env->SetIntArrayRegion(jCoords, 0, k, reinterpret_cast<const jint*>(coords));
        jboolean result = env->CallBooleanMethod(callbackRef, evaluateMid, jCoords);
        env->DeleteLocalRef(jCoords);
        return static_cast<bool>(result);
    };

    /* ── 4. Filter candidates ────────────────────────────────────── */
    CandidateSet candidates;
    filterByGQL(query, data, candidates);

    /* ── 5. Backtrack with result collection ─────────────────── */
    std::vector<std::vector<VertexID>> matchResults;

    EnumContext ctx(data, query, candidates, quantifiers, 1000);
    ctx.onMatch = [&](const std::vector<VertexID>& matched,
                  const std::vector<VertexID>& dynOrder) {
        std::vector<VertexID> canonical(ctx.qn);
        for (uint32_t d = 0; d < ctx.qn; ++d)
            canonical[dynOrder[d]] = matched[d];
        matchResults.push_back(canonical);
    };

    backtrack(ctx, 0, phi);
    env->DeleteGlobalRef(callbackRef);

    /* ── 6. Encode results as flat jlong[] ───────────────────── *
     *  Layout: [M, m0v0, m0v1, ..., m_{M-1}v_{qn-1}]           */
    const int      qn         = static_cast<int>(query.getNumVertices());
    const long     matchCount = static_cast<long>(matchResults.size());
    const jsize    arrayLen   = static_cast<jsize>(1 + matchCount * qn);

    jlongArray result = env->NewLongArray(arrayLen);
    std::vector<jlong> buf;
    buf.reserve(arrayLen);
    buf.push_back(static_cast<jlong>(matchCount));

    for (const auto& match : matchResults)
        for (VertexID vid : match)
            buf.push_back(static_cast<jlong>(vid));

    env->SetLongArrayRegion(result, 0, arrayLen, buf.data());
    return result;
}

} // extern "C"