#pragma once
#include "graph.h"
#include "filter.h"
#include "decision_tree.h"

/**
 * Dynamic fail-first subgraph enumeration with adaptive probe ordering.
 *
 * Adaptive probe ordering (per-depth)
 * =====================================
 * Each probe at depth d does two expensive operations:
 *   (A) narrowCands  — two-pointer intersections, O(|cands| + deg) per neighbour
 *   (B) update_for_depth — DT traversal, O(depth²) work
 *
 * The order matters because whichever runs first can prune the probe before
 * the other runs.  The optimal order depends on two rates and two costs:
 *
 *   f     = fwd_prune_rate  = fwdPruned / probed
 *   d'    = dt_prune_rate   = dtPruned  / (probed - fwdPruned)
 *   C_n   = avg narrow elements per probe  = narrowWork / probed
 *   C_d   = DT cost in equivalent narrow-element units  (constant DT_COST_ELEMS)
 *
 * Expected cost per probe:
 *   Order A (narrow → DT):  E_A = C_n + (1-f)  * C_d
 *   Order B (DT → narrow):  E_B = C_d + (1-d') * C_n
 *
 * B is cheaper when:  C_d * f  <  C_n * d'
 * i.e. DT is cheap AND its prune rate d' is high relative to fwd's rate f.
 *
 * Every ADAPT_INTERVAL probes at depth d, we recompute this inequality and
 * flip dtFirst[d] accordingly.  A warmup guard (ADAPT_WARMUP probes) prevents
 * premature decisions before enough data is collected.
 */

using Quantifiers = std::vector<Quantifier>;

inline uint32_t twoPointerIntersect(const VertexID* a, uint32_t na,
                                     const VertexID* b, uint32_t nb,
                                     VertexID*       out)
{
    uint32_t i = 0, j = 0, cnt = 0;
    while (i < na && j < nb) {
        if      (a[i] < b[j]) ++i;
        else if (a[i] > b[j]) ++j;
        else { out[cnt++] = a[i]; ++i; ++j; }
    }
    return cnt;
}

/* ── adaptive ordering constants ───────────────────────────────────── *
 * DT_COST_ELEMS  — estimated DT cost expressed in "equivalent number   *
 *   of narrow elements scanned".  This is the single tuning knob:      *
 *   higher = DT is considered more expensive → bias toward narrow-first *
 *   lower  = DT is considered cheap → bias toward DT-first             *
 *   Default 400 ≈ ~200 ns, roughly 1 DT call on a 16-vertex query.    *
 *                                                                       *
 * ADAPT_INTERVAL — re-evaluate ordering every N probes at each depth.  *
 * ADAPT_WARMUP   — minimum probes before the first evaluation.         */
static constexpr uint32_t DT_COST_ELEMS  = 400;
static constexpr uint32_t ADAPT_INTERVAL = 2000;
static constexpr uint32_t ADAPT_WARMUP   = 500;

struct EnumContext {
    const Graph&        data;
    const QueryGraph&   query;
    const CandidateSet& candidates;
    const Quantifiers&  quantifiers;

    /* ── flat candidate pool ──────────────────────────────────────── *
     *                                                                 *
     * Layout:  candPool[ u * (qn+1) * maxCands                       *
     *                    + level * maxCands ]                         *
     *   level 0   = GQL baseline (copied once in constructor)        *
     *   level k>0 = k-th narrowing of u's candidates                 *
     *                                                                 *
     * candTop[u]      : next free level index (0 after init → 1 after *
     *                   first narrowing pushed)                       *
     * candSz[u][lv]   : element count at level lv                    *
     *                                                                 *
     * getCands(u)  → pointer + size at candPool[ u ][ top-1 ]        *
     * narrowCands  → intersect into [ u ][ top ], increment top      *
     * undoCands    → decrement top for each u in updatedAtDepth[d]   */
    uint32_t qn;
    uint32_t maxCands;

    std::vector<VertexID>  candPool;    // flat: qn * (qn+1) * maxCands
    std::vector<uint32_t>  candTop;     // [qn]  current stack height
    std::vector<uint32_t>  candSzFlat;  // [qn * (qn+1)]  sizes per level

    /* per-depth undo list */
    std::vector<std::vector<VertexID>> updatedAtDepth;  // [depth] → query verts pushed

    /* ── matching state ──────────────────────────────────────────── */
    std::vector<VertexID> mapping;
    std::vector<bool>     inMapping;
    std::vector<bool>     placed;

    std::vector<VertexID> matched;   // matched[depth]  = data vertex
    std::vector<VertexID> dynOrder;  // dynOrder[depth] = query vertex

    std::vector<uint32_t> failPos;
    DecisionTree          decisionTree;
    uint64_t matchCount{0};
    uint64_t matchLimit{UINT64_MAX};

    std::function<void(const std::vector<VertexID>& matched,
                   const std::vector<VertexID>& dynOrder)> onMatch;

    /* ── profiling ────────────────────────────────────────────────── */
    std::vector<uint64_t> probeCount;
    std::vector<uint64_t> licmPruned;
    std::vector<uint64_t> dtPruned;
    std::vector<uint64_t> narrowWork;   // total elements scanned in twoPointerIntersect

    /* ── adaptive probe ordering ─────────────────────────────────── *
     * dtFirst[d]      : true = try DT check before narrowCands at d  *
     * lastAdaptAt[d]  : probeCount[d] value at last adaptation        */
    std::vector<bool>     dtFirst;
    std::vector<uint64_t> lastAdaptAt;

    /* ── constructor ─────────────────────────────────────────────── */
    EnumContext(const Graph&        data_,
                const QueryGraph&   query_,
                const CandidateSet& cands,
                const Quantifiers&  quants,
                uint64_t            limit = UINT64_MAX)
        : data(data_), query(query_), candidates(cands), quantifiers(quants),
          decisionTree((int)query_.getNumVertices(),
                       (int)quants.size(), quants),
          matchLimit(limit)
    {
        qn = query_.getNumVertices();
        const uint32_t dn = data_.getNumVertices();

        maxCands = 0;
        for (const auto& c : cands)
            maxCands = std::max(maxCands, (uint32_t)c.size());

        /* Flat pool allocation — one shot, no further heap traffic */
        const uint32_t levels = qn + 1;  // GQL + one push per depth at most
        candPool.resize((size_t)qn * levels * maxCands);
        candTop.assign(qn, 1);           // level 0 already filled below
        candSzFlat.assign((size_t)qn * levels, 0);

        /* Copy GQL candidates into level 0 of each query vertex */
        for (uint32_t u = 0; u < qn; ++u) {
            uint32_t sz = (uint32_t)cands[u].size();
            candSzFlat[u * levels + 0] = sz;
            std::copy(cands[u].begin(), cands[u].end(),
                      candPool.data() + (size_t)u * levels * maxCands);
        }

        updatedAtDepth.resize(qn + 1);

        mapping.assign(qn, UINT32_MAX);
        inMapping.assign(dn, false);
        placed.assign(qn, false);
        failPos.assign(qn + 1, UINT32_MAX);
        matched.resize(qn);
        dynOrder.resize(qn);

        probeCount.assign(qn, 0);
        licmPruned.assign(qn, 0);
        dtPruned.assign(qn, 0);
        narrowWork.assign(qn, 0);

        /* Start with narrow-first (conservative default) */
        dtFirst.assign(qn, false);
        lastAdaptAt.assign(qn, 0);
    }

    /* ── hot-path candidate accessors ────────────────────────────── */

    inline VertexID* candPtr(uint32_t u, uint32_t lv) {
        return candPool.data() + ((size_t)u * (qn + 1) + lv) * maxCands;
    }
    inline const VertexID* candPtr(uint32_t u, uint32_t lv) const {
        return candPool.data() + ((size_t)u * (qn + 1) + lv) * maxCands;
    }

    /* Returns pointer + size of current candidates for u */
    inline std::pair<const VertexID*, uint32_t> getCands(uint32_t u) const {
        uint32_t lv = candTop[u] - 1;
        return { candPtr(u, lv), candSzFlat[u * (qn + 1) + lv] };
    }

    /* Intersect current candidates of u with N(v); push result.
     * Returns false (and still pushes) if intersection is empty — caller
     * must still call undoCands() before returning.                   */
    inline bool narrowCands(uint32_t u, VertexID v, uint32_t depth) {
        uint32_t        dDeg;
        const VertexID* dNbrs = data.getNeighbors(v, dDeg);

        auto [src, srcSz] = getCands(u);
        narrowWork[depth] += srcSz + dDeg;   // elements touched by two-pointer
        uint32_t lv   = candTop[u];
        uint32_t newSz = twoPointerIntersect(src, srcSz, dNbrs, dDeg,
                                              candPtr(u, lv));
        candSzFlat[u * (qn + 1) + lv] = newSz;
        ++candTop[u];
        updatedAtDepth[depth].push_back(u);
        return newSz > 0;
    }

    /* Undo all narrowings recorded at `depth` */
    inline void undoCands(uint32_t depth) {
        for (uint32_t u : updatedAtDepth[depth])
            --candTop[u];
        updatedAtDepth[depth].clear();
    }

    void updateFail(uint32_t d, uint32_t pos) {
        if (failPos[d] == UINT32_MAX || pos < failPos[d])
            failPos[d] = pos;
    }

    /* ── adaptive ordering ───────────────────────────────────────── *
     *                                                                *
     * Recompute dtFirst[d] using the cost model:                    *
     *   E_A (narrow→DT) = C_n + (1-f)  * C_d                       *
     *   E_B (DT→narrow) = C_d + (1-d') * C_n                       *
     *   B < A  ⟺  C_d * f < C_n * d'                               *
     *                                                                *
     * where f  = fwd_prune_rate, d' = dt_prune_rate (cond. on fwd), *
     *       C_n = avg narrow elements/probe, C_d = DT_COST_ELEMS.   *
     *                                                                *
     * Guard: require ADAPT_WARMUP probes before first evaluation,   *
     * and at least 1 probe in each of the fwd and dt buckets.       */
    void adaptStrategy(uint32_t d) {
        uint64_t probes   = probeCount[d];
        if (probes < ADAPT_WARMUP) return;

        uint64_t fwd      = licmPruned[d];
        uint64_t dt_p     = dtPruned[d];
        uint64_t dt_calls = probes - fwd;   // probes that reached DT
        if (dt_calls == 0 || fwd == 0) return;

        /* C_n = narrowWork[d] / probes  (as a fixed-point ratio)     *
         * Condition B < A:  DT_COST_ELEMS * fwd * dt_calls           *
         *                <  narrowWork[d] * dt_p                     *
         * All quantities are uint64_t — no floating point needed.    */
        /* Scale lhs by dt_rate denominator to match units */
        /* Condition: DT_COST_ELEMS * (fwd/probes) < (narrowWork/probes) * (dt_p/dt_calls) *
         * Multiply both sides by probes * dt_calls to clear denominators:                  *
         *   DT_COST_ELEMS * fwd * dt_calls  <  narrowWork[d] * dt_p                       */
        bool should_dt_first =
            (DT_COST_ELEMS * fwd * dt_calls < narrowWork[d] * dt_p);

        dtFirst[d] = should_dt_first;
    }
};

/* ------------------------------------------------------------------ */

/** Print one match line.  Format: "u->v u->v ..."  (dynamic order). */
inline void printAnswer(const EnumContext& ctx)
{
    for (uint32_t d = 0; d < ctx.qn; ++d) {
        VertexID u = ctx.dynOrder[d];
        if (d) std::cout << ' ';
        std::cout << u << "->" << ctx.matched[d];
    }
    std::cout << '\n';
}

/* ================================================================== *
 *  backtrack — dynamic fail-first recursive search                     *
 *                                                                      *
 *  Two probe orderings, selected per-depth by adaptStrategy():         *
 *                                                                      *
 *  Order A  (narrow → DT, default):                                    *
 *    narrowCands for all unplaced neighbours → if empty, prune (fwd)  *
 *    update_for_depth → if NS_FALSE, prune (dt)                       *
 *    recurse                                                            *
 *                                                                      *
 *  Order B  (DT → narrow, when dtFirst[depth]):                        *
 *    update_for_depth → if NS_FALSE, skip narrow entirely (dt)         *
 *    narrowCands for all unplaced neighbours → if empty, prune (fwd)  *
 *    recurse                                                            *
 *    note: if narrow fails after DT passed, both are undone            *
 * ================================================================== */
template<typename Fn>
inline void backtrack(EnumContext& ctx, uint32_t depth, Fn&& phi)
{
    if (ctx.matchCount >= ctx.matchLimit) return;

    /* Base case */
    if (depth == ctx.qn) {
        if (ctx.decisionTree.nodes[0] == NS_TRUE) {
            ++ctx.matchCount;
            if (ctx.onMatch) ctx.onMatch(ctx.matched, ctx.dynOrder);
        }
        return;
    }

    /* Fail-first: pick unplaced query vertex with smallest candidates */
    uint32_t bestU  = UINT32_MAX;
    uint32_t bestSz = UINT32_MAX;
    for (uint32_t u = 0; u < ctx.qn; ++u) {
        if (ctx.placed[u]) continue;
        uint32_t sz = ctx.getCands(u).second;
        if (sz < bestSz) { bestSz = sz; bestU = u; }
    }

    if (bestSz == 0) {
        ctx.updateFail(depth, depth > 0 ? depth - 1 : 0);
        return;
    }

    ctx.dynOrder[depth] = bestU;

    /* Iterate directly over the pool slice — no snapshot copy needed.
     * Safety: narrowCands() only touches *neighbours* of bestU, never
     * bestU's own pool slot, so the pointer stays valid for the entire
     * loop even as other vertices' candidates are narrowed and restored. */
    const VertexID* localPtr = ctx.candPtr(bestU, ctx.candTop[bestU] - 1);
    const uint32_t  localSz  = ctx.candSzFlat[bestU * (ctx.qn + 1)
                                               + ctx.candTop[bestU] - 1];

    uint32_t        qDeg;
    const VertexID* qNbrs = ctx.query.getNeighbors(bestU, qDeg);

    /* Sort unplaced query neighbours by current candidate size ascending.
     * twoPointerIntersect is O(src + deg); processing the most-constrained
     * (smallest) neighbour first means we hit a zero-intersection and exit
     * the inner loop as early as possible, skipping larger neighbours.    */
    uint32_t nbrBuf[64];   // query graph is small; stack buffer is enough
    uint32_t nbrCnt = 0;
    for (uint32_t ni = 0; ni < qDeg; ++ni) {
        uint32_t w = qNbrs[ni];
        if (!ctx.placed[w]) nbrBuf[nbrCnt++] = w;
    }
    std::sort(nbrBuf, nbrBuf + nbrCnt, [&](uint32_t a, uint32_t b) {
        return ctx.getCands(a).second < ctx.getCands(b).second;
    });

    for (uint32_t ci = 0; ci < localSz; ++ci) {
        if (ctx.matchCount >= ctx.matchLimit) return;
        VertexID v = localPtr[ci];
        if (ctx.inMapping[v]) continue;
        ++ctx.probeCount[depth];

        /* Periodically re-evaluate the optimal ordering for this depth */
        if (ctx.probeCount[depth] - ctx.lastAdaptAt[depth] >= ADAPT_INTERVAL) {
            ctx.adaptStrategy(depth);
            ctx.lastAdaptAt[depth] = ctx.probeCount[depth];
        }

        if (ctx.dtFirst[depth]) {
            /* ── Order B: DT check first ──────────────────────────── *
             * Try the phi constraint before paying for narrowCands.   *
             * If DT prunes, we skip the intersection work entirely.   *
             *                                                          *
             * update_for_depth always modifies tree state (log entries *
             * are written for NS_FALSE, NS_TRUE, and NS_UNKNOWN alike).*
             * undo_to(depth) must therefore be called on every exit   *
             * path — whether DT pruned, narrow pruned, or recursion   *
             * returned — regardless of the returned NodeState.        */
            ctx.matched[depth] = v;
            bool dt_fail = false;

            dt_fail = (ctx.decisionTree.update_for_depth(
                           depth, ctx.matched, phi) == NS_FALSE);

            if (dt_fail) {
                /* DT pruned — narrowCands was never called.
                 * Undo the tree state written by update_for_depth.   */
                ctx.decisionTree.undo_to(depth);
                ++ctx.dtPruned[depth];
                continue;
            }

            /* DT passed — now pay for narrowCands */
            bool fwd_fail = false;
            for (uint32_t ni = 0; ni < nbrCnt && !fwd_fail; ++ni) {
                if (!ctx.narrowCands(nbrBuf[ni], v, depth)) {
                    fwd_fail = true;
                    ++ctx.licmPruned[depth];
                }
            }

            if (fwd_fail) {
                /* Narrow failed after DT passed — undo both */
                ctx.undoCands(depth);
                ctx.decisionTree.undo_to(depth);
                continue;
            }

            /* Both passed — recurse */
            ctx.failPos[depth + 1] = UINT32_MAX;
            ctx.mapping[bestU] = v;
            ctx.inMapping[v]   = true;
            ctx.placed[bestU]  = true;

            backtrack(ctx, depth + 1, phi);

            ctx.mapping[bestU] = UINT32_MAX;
            ctx.inMapping[v]   = false;
            ctx.placed[bestU]  = false;
            ctx.undoCands(depth);
            ctx.decisionTree.undo_to(depth);

            if (ctx.matchCount >= ctx.matchLimit) return;

            uint32_t childFail = ctx.failPos[depth + 1];
            if (childFail != UINT32_MAX) {
                ctx.updateFail(depth, childFail);
                if (childFail < depth) return;
            }

        } else {
            /* ── Order A: narrow first (default) ──────────────────── */
            bool fwd_fail = false;
            for (uint32_t ni = 0; ni < nbrCnt && !fwd_fail; ++ni) {
                if (!ctx.narrowCands(nbrBuf[ni], v, depth)) {
                    fwd_fail = true;
                    ++ctx.licmPruned[depth];
                }
            }

            if (!fwd_fail) {
                ctx.matched[depth] = v;
                bool dt_fail = false;

                if (ctx.decisionTree.update_for_depth(depth, ctx.matched, phi)
                        == NS_FALSE) {
                    ctx.decisionTree.undo_to(depth);
                    dt_fail = true;
                }

                if (dt_fail) {
                    ++ctx.dtPruned[depth];
                } else {
                    ctx.failPos[depth + 1] = UINT32_MAX;
                    ctx.mapping[bestU] = v;
                    ctx.inMapping[v]   = true;
                    ctx.placed[bestU]  = true;

                    backtrack(ctx, depth + 1, phi);

                    ctx.mapping[bestU] = UINT32_MAX;
                    ctx.inMapping[v]   = false;
                    ctx.placed[bestU]  = false;
                    ctx.decisionTree.undo_to(depth);

                    if (ctx.matchCount >= ctx.matchLimit) {
                        ctx.undoCands(depth);
                        return;
                    }

                    uint32_t childFail = ctx.failPos[depth + 1];
                    if (childFail != UINT32_MAX) {
                        ctx.updateFail(depth, childFail);
                        if (childFail < depth) {
                            ctx.undoCands(depth);
                            return;
                        }
                    }
                }
            }

            ctx.undoCands(depth);
        }
    }
}

/* ================================================================== *
 *  enumerate — public entry points (Order parameter removed)          *
 * ================================================================== */

inline uint64_t enumerate(const QueryGraph&   query,
                           const Graph&        data,
                           const CandidateSet& candidates,
                           const Quantifiers&  quantifiers,
                           uint64_t            limit = UINT64_MAX)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit);
    backtrack(ctx, 0, [](const int*) noexcept { return true; });
    return ctx.matchCount;
}

template<typename Fn>
inline uint64_t enumerate(const QueryGraph&   query,
                           const Graph&        data,
                           const CandidateSet& candidates,
                           const Quantifiers&  quantifiers,
                           uint64_t            limit,
                           Fn&&                phi)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit);
    backtrack(ctx, 0, std::forward<Fn>(phi));
    return ctx.matchCount;
}
