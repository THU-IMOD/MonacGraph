#pragma once
#include "graph.h"
#include "filter.h"
#include "order.h"
#include "decision_tree.h"
#include "trie.h"
#include "symmetry.h"
#include <queue>

/**
 * Dynamic fail-first subgraph enumeration with adaptive probe ordering.
 *
 * Adaptive probe ordering (per-depth)
 * =====================================
 * Each probe at depth d does two expensive operations:
 *   (A) narrowCands  — two-pointer intersections, O(|cands| + deg) per neighbour
 *   (B) update_for_depth — DT traversal
 *
 * The order matters because whichever runs first can prune the probe before
 * the other runs.  The optimal order depends on two rates and two costs:
 *
 *   f     = fwd_prune_rate  = fwdPruned / probed  (window)
 *   d'    = dt_prune_rate   = dtPruned  / (probed - fwdPruned)  (window)
 *   C_n   = avg narrow elements per probe  = narrowWork / probed  (window)
 *   C_d(d)= DT_NODE_COST * [(d+1)^k - d^k] * prod_ell(f_ell)
 *             where f_ell = fraction of UNKNOWN nodes at tree level ell
 *
 * Expected cost per probe:
 *   Order A (narrow → DT):  E_A = C_n + (1-f)  * C_d(d)
 *   Order B (DT → narrow):  E_B = C_d(d) + (1-d') * C_n
 *
 * B is cheaper when:  C_d(d) * f  <  C_n * d'
 *
 * Every ADAPT_WINDOW probes at depth d, adaptStrategy() recomputes this
 * inequality (using floating-point), flips dtFirst[d] accordingly, and
 * resets all per-depth window counters to start a fresh window.
 */

using Quantifiers = std::vector<Quantifier>;

/* ================================================================== *
 *  Set-intersection primitives                                         *
 *                                                                      *
 *  Three implementations, one adaptive dispatcher:                    *
 *                                                                      *
 *  twoPointerIntersect  — classic merge, O(na + nb).                 *
 *    Best when na ≈ nb (similar sizes).                               *
 *                                                                      *
 *  gallopingIntersect   — exponential search on the larger side,      *
 *    O(na × log(nb/na)) when na << nb.                                *
 *    Algorithm: for each element of the smaller array, locate it in   *
 *    the larger array via an exponential probe (1,2,4,8,...) that     *
 *    narrows the search window, then falls back to binary search.     *
 *    Break-even vs two-pointer is roughly na × log(nb/na) < na + nb, *
 *    i.e. when nb/na > GALLOP_THRESHOLD.                              *
 *                                                                      *
 *  intersectCands       — dispatcher: measures the size ratio and     *
 *    routes to gallopingIntersect when one side dominates, otherwise  *
 *    falls back to twoPointerIntersect.                               *
 *                                                                      *
 *  GALLOP_THRESHOLD — ratio at which galloping beats two-pointer.     *
 *    Empirically ~32 for 32-bit integer arrays on modern hardware     *
 *    (matches ASDMatch's count-only threshold).  Stored as constexpr  *
 *    so it can be tuned without rewriting the dispatch condition.      *
 * ================================================================== */

static constexpr uint32_t GALLOP_THRESHOLD = 32;

/* ------------------------------------------------------------------ *
 *  twoPointerIntersect                                                 *
 *  Classic O(na + nb) merge.  Always correct; best when na ≈ nb.     *
 * ------------------------------------------------------------------ */
inline uint32_t twoPointerIntersect(const VertexID* a, uint32_t na,
                                     const VertexID* b, uint32_t nb,
                                     VertexID*       out)
{
    uint32_t i = 0, j = 0, cnt = 0;
    while (i < na && j < nb) {
        if (a[i] < b[j]) {
            ++i;
        } else if (a[i] > b[j]) {
            ++j;
        } else {
            out[cnt++] = a[i];
            ++i;
            ++j;
        }
    }
    return cnt;
}

/* ------------------------------------------------------------------ *
 *  binarySearch                                                        *
 *  Find first index k in src[lo..hi) with src[k] >= val.             *
 * ------------------------------------------------------------------ */
inline uint32_t binarySearch(const VertexID* src,
                              uint32_t lo, uint32_t hi,
                              VertexID val)
{
    while (lo < hi) {
        uint32_t mid = lo + ((hi - lo) >> 1);
        if      (src[mid] < val) lo = mid + 1;
        else if (src[mid] > val) hi = mid;
        else return mid;
    }
    return lo;
}

/* ------------------------------------------------------------------ *
 *  gallopingSearch                                                     *
 *                                                                      *
 *  Find the first index k in src[begin..end) such that src[k] >= val.*
 *  Returns end if no such element exists.                             *
 *                                                                      *
 *  Algorithm (mirrors ASDMatch's GallopingSearch):                    *
 *    Phase 1 — exponential probe from begin:                          *
 *      Check src[begin + jump] for jump = 4, 8, 16, ...              *
 *      The key invariant is that offset_beg STAYS AT begin.           *
 *      When src[begin + jump] < val, just double jump.                *
 *      When src[begin + jump] >= val, we know the answer lies in      *
 *      [begin + jump/2 + 1, begin + jump], because:                  *
 *        • src[begin + jump/2] < val  (last confirmed miss)           *
 *        • src[begin + jump]   >= val (current probe)                 *
 *    Phase 2 — binary search within that window.                      *
 *                                                                      *
 *  IMPORTANT: offset_beg must NOT advance between probes.             *
 *    Moving it (as a "smarter" cursor) shifts the window incorrectly: *
 *    the lower bound cursor + jump/2 would skip [cursor+1, cursor+    *
 *    jump/2-1], which are provably reachable matches.  Keep it fixed. *
 * ------------------------------------------------------------------ */
inline uint32_t gallopingSearch(const VertexID* src,
                                 uint32_t begin, uint32_t end,
                                 VertexID val)
{
    /* Fast-exit if val exceeds the entire remaining range */
    if (src[end - 1] < val) return end;

    /* Short-circuit for the first few positions (cache-hot).
     * Each step is guarded so we never read past src[end-1].          */
    if (src[begin] >= val) return begin;
    if (begin + 1 >= end)  return end;
    if (src[begin + 1] >= val) return begin + 1;
    if (begin + 2 >= end)  return end;
    if (src[begin + 2] >= val) return begin + 2;

    /* Exponential probe — offset_beg stays fixed at begin */
    uint32_t jump       = 4;
    uint32_t offset_beg = begin;    // NEVER moves

    while (true) {
        uint32_t probe = offset_beg + jump;
        if (probe >= end) {
            /* Jumped past the end: answer in [offset_beg + jump/2 + 1, end) */
            return binarySearch(src, offset_beg + (jump >> 1) + 1, end, val);
        }
        if (src[probe] < val) {
            jump <<= 1;             // still below: double the jump
        } else {
            /* src[probe] >= val: answer in [offset_beg+jump/2+1, probe+1) *
             * because src[offset_beg + jump/2] < val (previous probe)     */
            if (src[probe] == val) return probe;
            return binarySearch(src, offset_beg + (jump >> 1) + 1, probe + 1, val);
        }
    }
}

/* ------------------------------------------------------------------ *
 *  gallopingIntersect                                                  *
 *                                                                      *
 *  O(na × log(nb/na)) intersection where na <= nb.                   *
 *                                                                      *
 *  For each element a[i] of the smaller array, use gallopingSearch   *
 *  to locate it in the larger array starting from the last position  *
 *  found (j).  The monotonicity of sorted arrays means j never goes  *
 *  backward, so the total work across all probes is bounded by        *
 *  O(na × log(nb/na)).                                               *
 *                                                                      *
 *  The caller is responsible for ensuring na <= nb; the function      *
 *  does NOT swap sides internally to keep the hot path branch-free.   *
 * ------------------------------------------------------------------ */
inline uint32_t gallopingIntersect(const VertexID* a, uint32_t na,
                                    const VertexID* b, uint32_t nb,
                                    VertexID*       out)
{
    uint32_t j = 0, cnt = 0;
    for (uint32_t i = 0; i < na; ++i) {
        j = gallopingSearch(b, j, nb, a[i]);
        if (j >= nb) break;            // a[i] > b[nb-1]: no more matches possible
        if (b[j] == a[i]) {
            out[cnt++] = a[i];
            ++j;                       // advance past the matched element
        }
        /* If b[j] > a[i], move to next a[i] (j stays for next iteration) */
    }
    return cnt;
}

/* ------------------------------------------------------------------ *
 *  intersectCands — adaptive dispatcher                               *
 *                                                                      *
 *  Routes to gallopingIntersect when one array is at least            *
 *  GALLOP_THRESHOLD times larger than the other; otherwise uses       *
 *  twoPointerIntersect.                                               *
 *                                                                      *
 *  Size swap note: gallopingIntersect expects (smaller, larger) order.*
 *  We swap here so the caller never needs to think about ordering.    *
 * ------------------------------------------------------------------ */
inline uint32_t intersectCands(const VertexID* a, uint32_t na,
                                const VertexID* b, uint32_t nb,
                                VertexID*       out)
{
    if (na == 0 || nb == 0) return 0;

    /* Choose algorithm based on size ratio */
    if (na * GALLOP_THRESHOLD < nb) {
        /* na << nb: gallop through b for each element of a */
        return gallopingIntersect(a, na, b, nb, out);
    }
    if (nb * GALLOP_THRESHOLD < na) {
        /* nb << na: gallop through a for each element of b */
        return gallopingIntersect(b, nb, a, na, out);
    }
    /* Sizes are comparable: two-pointer is optimal */
    return twoPointerIntersect(a, na, b, nb, out);
}

/* ── adaptive ordering constants ───────────────────────────────────── *
 * ADAPT_WINDOW — statistics window size: every W probes at depth d,   *
 *   adaptStrategy() is called and per-depth counters are reset.       *
 *                                                                       *
 * DT_NODE_COST — cost of visiting one decision-tree node, expressed   *
 *   in units of "narrow elements scanned".  A tree node access is     *
 *   ~1 cache-hot byte read + branch, roughly 1-2 two-pointer steps.   */
static constexpr uint32_t ADAPT_WINDOW   = 10000;
static constexpr double   DT_NODE_COST   = 1.5;

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
    uint32_t staticOrderLen;  // 0 → DYNAMIC; qn → static order active

    VertexID*  candPool{nullptr};    // flat: qn * (qn+1) * maxCands
    uint32_t*  candTop{nullptr};     // [qn]  current stack height
    uint32_t*  candSzFlat{nullptr};  // [qn * (qn+1)]  sizes per level

    VertexID*  updatedAtDepth{nullptr};   // flat: (qn+1) * qn
    uint32_t*  updatedAtDepthSz{nullptr}; // [qn+1]

    VertexID*  mapping{nullptr};   // [qn]
    bool*      inMapping{nullptr}; // [dn]
    bool*      placed{nullptr};    // [qn]

    VertexID*  matched{nullptr};   // [qn]  data vertex at each depth
    VertexID*  dynOrder{nullptr};  // [qn]  query vertex at each depth
    VertexID*  staticOrder{nullptr}; // [qn] or nullptr when DYNAMIC

    uint32_t*  failPos{nullptr};   // [qn+1]

    /* ── per-depth iterative search state ───────────────────────── *
     *                                                               *
     * These replace the call-stack frames of the recursive version.*
     *                                                               *
     * candIdx[d]          — next candidate index to try at depth d *
     * nbrBufs[d * 64 + i] — i-th unplaced neighbour of dynOrder[d]*
     * nbrCnts[d]          — number of unplaced neighbours at d     *
     *                                                               *
     * 64 is a safe upper bound: query graphs are ≤ DP_MAX_N = 20  *
     * vertices, so no vertex can have more than 19 neighbours.     */
    uint32_t*  candIdx{nullptr};   // [qn]
    uint32_t*  nbrBufs{nullptr};   // [qn * 64]  flat per-depth neighbour buffer
    uint32_t*  nbrCnts{nullptr};   // [qn]

    DecisionTree  decisionTree;
    uint64_t      matchCount{0};
    uint64_t      matchLimit{UINT64_MAX};

    std::function<void(const VertexID* matched,
                       const VertexID* dynOrder,
                       uint32_t        qn)> onMatch;

    MatchTrie* matchTrie{nullptr};

    uint64_t*  probeCount{nullptr};  // [qn]
    uint64_t*  licmPruned{nullptr};  // [qn]
    uint64_t*  dtPruned{nullptr};    // [qn]
    uint64_t*  narrowWork{nullptr};  // [qn]

    uint64_t*  snapProbe{nullptr};   // [qn]
    uint64_t*  snapFwd{nullptr};     // [qn]
    uint64_t*  snapDT{nullptr};      // [qn]
    uint64_t*  snapNarrow{nullptr};  // [qn]

    bool*      dtFirst{nullptr};     // [qn]

    /* ── symmetry-breaking constraints (from SymBreak) ───────────── *
     * symConstraints[u].first  = { w : mapping[w] < mapping[u] }    *
     * symConstraints[u].second = { w : mapping[u] < mapping[w] }    *
     * symEndArr[d]  — upper-bound index into candidate pool at d;    *
     *                 computed once on entry, stored per depth.      *
     * symPruned[d]  — candidates skipped by symmetry at depth d.    */
    uint32_t*  symEndArr{nullptr};   // [qn]
    std::unordered_map<VertexID,
        std::pair<std::set<VertexID>, std::set<VertexID>>> symConstraints;
    std::vector<uint64_t> symPruned;

    /* ── automorphism group (from SymBreak) ──────────────────────── *
     * autGroupSize   : |Aut(query)| — multiplier for match counting  *
     * autGenerators  : generator set; autGenerators[i][u] = σᵢ(u)  *
     * When empty, behaviour is identical to the non-symmetric case.  */
    uint64_t autGroupSize{1};
    std::vector<std::vector<VertexID>> autGenerators;

    /* ── constructor ─────────────────────────────────────────────── */
    EnumContext(const Graph&        data_,
                const QueryGraph&   query_,
                const CandidateSet& cands,
                const Quantifiers&  quants,
                uint64_t            limit = UINT64_MAX,
                const Order&        staticOrd = {},
                bool                sym = false,
                std::unordered_map<VertexID,
                    std::pair<std::set<VertexID>,
                              std::set<VertexID>>> symCons = {},
                uint64_t                           autSize  = 1,
                std::vector<std::vector<VertexID>> autGens  = {})
        : data(data_), query(query_), candidates(cands), quantifiers(quants),
          decisionTree((int)query_.getNumVertices(),
                       (int)quants.size(), quants, sym),
          matchLimit(limit),
          symConstraints(std::move(symCons)),
          autGroupSize(autSize),
          autGenerators(std::move(autGens))
    {
        qn = query_.getNumVertices();
        const uint32_t dn  = data_.getNumVertices();
        const uint32_t levels = qn + 1;

        maxCands = 0;
        for (const auto& c : cands)
            maxCands = std::max(maxCands, (uint32_t)c.size());

        /* Candidate pool: one flat block, no further allocation */
        candPool    = new VertexID[(size_t)qn * levels * maxCands]();
        candTop     = new uint32_t[qn];
        candSzFlat  = new uint32_t[(size_t)qn * levels]();

        for (uint32_t u = 0; u < qn; ++u) {
            candTop[u] = 1;
            uint32_t sz = (uint32_t)cands[u].size();
            candSzFlat[u * levels] = sz;
            std::memcpy(candPool + (size_t)u * levels * maxCands,
                        cands[u].data(), sz * sizeof(VertexID));
        }

        updatedAtDepth   = new VertexID[(size_t)(qn + 1) * qn]();
        updatedAtDepthSz = new uint32_t[qn + 1]();

        mapping   = new VertexID[qn];
        inMapping = new bool[dn]();
        placed    = new bool[qn]();
        matched   = new VertexID[qn];
        dynOrder  = new VertexID[qn];
        failPos   = new uint32_t[qn + 1];

        std::fill(mapping, mapping + qn, UINT32_MAX);
        std::fill(failPos, failPos + qn + 1, UINT32_MAX);

        /* Static order */
        staticOrderLen = (uint32_t)staticOrd.size();
        if (staticOrderLen > 0) {
            staticOrder = new VertexID[qn];
            std::memcpy(staticOrder, staticOrd.data(), qn * sizeof(VertexID));
            matchTrie = new MatchTrie(qn);
        }

        /* Profiling arrays — zero-initialised */
        probeCount  = new uint64_t[qn]();
        licmPruned  = new uint64_t[qn]();
        dtPruned    = new uint64_t[qn]();
        narrowWork  = new uint64_t[qn]();
        snapProbe   = new uint64_t[qn]();
        snapFwd     = new uint64_t[qn]();
        snapDT      = new uint64_t[qn]();
        snapNarrow  = new uint64_t[qn]();

        candIdx  = new uint32_t[qn]();
        nbrBufs  = new uint32_t[(size_t)qn * 64]();
        nbrCnts  = new uint32_t[qn]();

        dtFirst = new bool[qn]();   // false = narrow-first default

        symEndArr = new uint32_t[qn];
        std::fill(symEndArr, symEndArr + qn, UINT32_MAX);
        symPruned.assign(qn, 0);
    }

    ~EnumContext() {
        delete[] candPool;
        delete[] candTop;
        delete[] candSzFlat;
        delete[] updatedAtDepth;
        delete[] updatedAtDepthSz;
        delete[] mapping;
        delete[] inMapping;
        delete[] placed;
        delete[] matched;
        delete[] dynOrder;
        delete[] staticOrder;
        delete[] failPos;
        delete[] probeCount;
        delete[] licmPruned;
        delete[] dtPruned;
        delete[] narrowWork;
        delete[] snapProbe;
        delete[] snapFwd;
        delete[] snapDT;
        delete[] snapNarrow;
        delete[] candIdx;
        delete[] nbrBufs;
        delete[] nbrCnts;
        delete[] dtFirst;
        delete[] symEndArr;
        delete   matchTrie;
    }

    EnumContext(const EnumContext&)            = delete;
    EnumContext& operator=(const EnumContext&) = delete;

    /* ── hot-path candidate accessors ────────────────────────────── */

    inline VertexID* candPtr(uint32_t u, uint32_t lv) {
        return candPool + ((size_t)u * (qn + 1) + lv) * maxCands;
    }
    inline const VertexID* candPtr(uint32_t u, uint32_t lv) const {
        return candPool + ((size_t)u * (qn + 1) + lv) * maxCands;
    }

    inline std::pair<const VertexID*, uint32_t> getCands(uint32_t u) const {
        uint32_t lv = candTop[u] - 1;
        return { candPtr(u, lv), candSzFlat[u * (qn + 1) + lv] };
    }

    /* Intersect current candidates of u with N(v); push result.
     * Returns false (and still pushes) if intersection is empty — caller
     * must still call undoCands() before returning.
     *
     * Uses intersectCands() which dispatches to gallopingIntersect when
     * one side is >> the other (ratio >= GALLOP_THRESHOLD), and falls
     * back to twoPointerIntersect when sizes are comparable.
     *
     * narrowWork[depth] counts min(srcSz, dDeg) as a proxy for the
     * actual work done: two-pointer scans na+nb elements, galloping
     * scans ~na×log(nb/na) ≈ na elements on the smaller side.
     * Using min() keeps the profiling meaningful under both algorithms
     * without adding a branch.                                         */
    inline bool narrowCands(uint32_t u, VertexID v, uint32_t depth) {
        uint32_t        dDeg;
        const VertexID* dNbrs = data.getNeighbors(v, dDeg);

        auto [src, srcSz] = getCands(u);
        narrowWork[depth] += srcSz + dDeg;
        uint32_t lv    = candTop[u];
        uint32_t newSz = intersectCands(src, srcSz, dNbrs, dDeg,
                                         candPtr(u, lv));
        candSzFlat[u * (qn + 1) + lv] = newSz;
        ++candTop[u];
        /* Flat push: no heap allocation, just a counter increment */
        updatedAtDepth[(size_t)depth * qn + updatedAtDepthSz[depth]++] = u;
        return newSz > 0;
    }

    /* Undo all narrowings recorded at `depth` */
    inline void undoCands(uint32_t depth) {
        const uint32_t  sz  = updatedAtDepthSz[depth];
        const VertexID* row = updatedAtDepth + (size_t)depth * qn;
        for (uint32_t i = 0; i < sz; ++i)
            --candTop[row[i]];
        updatedAtDepthSz[depth] = 0;   // O(1) clear — no destructor calls
    }

    void updateFail(uint32_t d, uint32_t pos) {
        if (failPos[d] == UINT32_MAX || pos < failPos[d])
            failPos[d] = pos;
    }

    /* ── adaptive ordering ───────────────────────────────────────── *
     *                                                                *
     * Called every ADAPT_WINDOW probes at depth d.                  *
     * Window deltas are computed as current - snapshot.             *
     *                                                                *
     * Switching condition (both orders): C_d(d) * f < C_n * d'     *
     *                                                                *
     * Rate definitions depend on the current order, because each    *
     * order only directly observes its first operation on every     *
     * probe:                                                         *
     *                                                                *
     *   Order A (narrow first):                                      *
     *     f  = F / P        — structural prune rate over all P      *
     *     d' = D / (P - F)  — DT prune rate | structural passed     *
     *                                                                *
     *   Order B (DT first):                                          *
     *     d' = D / P        — DT prune rate over all P              *
     *     f  = F / (P - D)  — structural prune rate | DT passed     *
     *                                                                *
     * C_d(d) = DT_NODE_COST * delta(d) * prod_ell(f_ell)           *
     *   delta(d) = (d+1)^k - d^k                                    *
     *   f_ell    = fraction of UNKNOWN nodes at tree level ell      */
    void adaptStrategy(uint32_t d) {
        const uint64_t P = probeCount[d] - snapProbe[d];
        const uint64_t F = licmPruned[d] - snapFwd[d];
        const uint64_t D = dtPruned[d]   - snapDT[d];

        double f_rate, d_rate;

        if (!dtFirst[d]) {
            /* ── Order A: structural first ───────────────────────── */
            const uint64_t dt_calls = P - F;
            if (dt_calls == 0 || F == 0) goto snap;
            f_rate = (double)F / (double)P;
            d_rate = (double)D / (double)dt_calls;
        } else {
            /* ── Order B: DT first ──────────────────────────────── */
            const uint64_t fwd_calls = P - D;
            if (fwd_calls == 0 || D == 0) goto snap;
            d_rate = (double)D / (double)P;
            f_rate = (double)F / (double)fwd_calls;
        }

        {
            /* ── C_d(d) ─────────────────────────────────────────── */
            long long new1 = 1, old1 = 1;
            for (int j = 0; j < decisionTree.k; ++j) {
                new1 *= (long long)(d + 1);
                old1 *= (long long)d;
            }
            const double delta = (double)(new1 - old1);

            double f_prod = 1.0;
            for (int ell = 0; ell < decisionTree.k; ++ell)
                f_prod *= decisionTree.unknown_fraction_at_level(ell);

            const double Cd = DT_NODE_COST * delta * f_prod;
            const double Cn = (double)(narrowWork[d] - snapNarrow[d])
                              / (double)P;

            dtFirst[d] = (Cd * f_rate < Cn * d_rate);
        }

    snap:
        snapProbe[d]  = probeCount[d];
        snapFwd[d]    = licmPruned[d];
        snapDT[d]     = dtPruned[d];
        snapNarrow[d] = narrowWork[d];
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
 *  Orbit generation helpers for symmetric match enumeration           *
 *                                                                      *
 *  applyAut — apply generator sigma to match m (raw pointer inputs).  *
 *    new_matched[d] = m[ queryToDepth[ sigma_inv[ dynOrder[d] ] ] ]   *
 *                                                                      *
 *  generateMatchOrbit — BFS over the automorphism group: starts from  *
 *    base, applies each generator, collects all distinct matches.     *
 *    For each orbit element, increments matchCount and fires onMatch  *
 *    or inserts into matchTrie.                                        *
 * ================================================================== */

inline void applyAut(const VertexID*              m,
                     const VertexID*              dynOrder,
                     const std::vector<VertexID>& sigma,
                     uint32_t                     qn,
                     VertexID*                    out)
{
    std::vector<uint32_t> queryToDepth(qn);
    for (uint32_t d = 0; d < qn; ++d)
        queryToDepth[dynOrder[d]] = d;

    std::vector<VertexID> sigma_inv(qn);
    for (uint32_t u = 0; u < qn; ++u)
        sigma_inv[sigma[u]] = u;

    for (uint32_t d = 0; d < qn; ++d)
        out[d] = m[ queryToDepth[ sigma_inv[ dynOrder[d] ] ] ];
}

inline void generateMatchOrbit(
        EnumContext& ctx,
        const std::vector<std::vector<VertexID>>& generators)
{
    const uint32_t qn = ctx.qn;

    /* Seed the BFS with the canonical match currently in ctx.matched. */
    using Match = std::vector<VertexID>;
    std::set<Match>   seen;
    std::queue<Match> q;

    Match base(ctx.matched, ctx.matched + qn);
    seen.insert(base);
    q.push(base);

    std::vector<VertexID> buf(qn);   // reusable apply buffer

    while (!q.empty()) {
        Match cur = std::move(q.front()); q.pop();

        /* Count and report this orbit element. */
        ++ctx.matchCount;
        if (ctx.matchTrie) {
            ctx.matchTrie->insert(cur.data(), qn);
        } else if (ctx.onMatch) {
            ctx.onMatch(cur.data(), ctx.dynOrder, qn);
        }

        /* Apply every generator to discover new orbit elements. */
        for (const auto& gen : generators) {
            applyAut(cur.data(), ctx.dynOrder, gen, qn, buf.data());
            Match next(buf.begin(), buf.end());
            if (!seen.count(next)) {
                seen.insert(next);
                q.push(std::move(next));
            }
        }
    }
}

/* ================================================================== *
 *                                                                      *
 *  FULL       — full system: DT pruning + adaptive probe ordering    *
 *  NO_DT      — structural narrowing only; phi verified by brute      *
 *               force at the base case (O(n^k) per full match)       *
 *  NO_ADAPT   — DT pruning active, but probe order is fixed at        *
 *               narrow-first throughout (dtFirst never flipped)       *
 * ================================================================== */
enum class AblationMode { FULL, NO_DT, NO_ADAPT };

/* ── phi_brute_check ─────────────────────────────────────────────── *
 *                                                                      *
 * Used by NO_DT mode at the base case.  Enumerates all n^k tuples    *
 * from matched[0..n-1] and evaluates the quantified formula directly, *
 * without touching the DecisionTree.                                  *
 *                                                                      *
 *   Q_0 x_0 ∈ matched: Q_1 x_1 ∈ matched: ... phi(x_0,x_1,...)      *
 *                                                                      *
 * Time: O(n^k).  Space: O(k) stack.                                  */
template<typename Fn>
inline bool phi_brute_check(const DecisionTree& dt,
                             const VertexID*     matched,
                             uint32_t            n,
                             int*                coords,
                             int                 level,
                             Fn&&                phi)
{
    if (level == dt.k) {
        return phi(coords);
    }
    const bool is_exists = dt.quantifier_at(level) == EXISTS;
    for (uint32_t i = 0; i < n; ++i) {
        coords[level] = (int)matched[i];
        const bool sub = phi_brute_check(dt, matched, n, coords, level + 1, phi);
        if (is_exists && sub) {
            return true;
        }
        if (!is_exists && !sub) {
            return false;
        }
    }
    return !is_exists;
}

/* ================================================================== *
 *  backtrack_impl — iterative DFS with compile-time ablation mode    *
 * ================================================================== */
template<AblationMode Mode, typename Fn>
inline void backtrack_impl(EnumContext& ctx, Fn&& phi)
{
    int32_t depth = 0;
    bool    fresh = true;

    while (true) {
        if (ctx.matchCount >= ctx.matchLimit) return;

        /* ── ENTER ───────────────────────────────────────────────── */
        if (fresh) {
            uint32_t bestU;
            if (ctx.staticOrderLen > 0) {
                bestU = ctx.staticOrder[depth];
                if (ctx.placed[bestU]) {
                    bestU = UINT32_MAX;
                    for (uint32_t u = 0; u < ctx.qn; ++u) {
                        if (!ctx.placed[u]) {
                            bestU = u;
                            break;
                        }
                    }
                }
                if (bestU == UINT32_MAX) goto do_backtrack;
            } else {
                bestU = UINT32_MAX;
                uint32_t bestSz = UINT32_MAX;
                for (uint32_t u = 0; u < ctx.qn; ++u) {
                    if (ctx.placed[u]) continue;
                    uint32_t sz = ctx.getCands(u).second;
                    if (sz < bestSz) {
                        bestSz = sz;
                        bestU  = u;
                    }
                }
                if (bestSz == 0) {
                    ctx.updateFail(depth, depth > 0 ? depth - 1 : 0);
                    goto do_backtrack;
                }
            }

            ctx.dynOrder[depth]    = bestU;
            ctx.failPos[depth + 1] = UINT32_MAX;
            ctx.candIdx[depth]     = 0;

            {
                uint32_t        qDeg;
                const VertexID* qNbrs  = ctx.query.getNeighbors(bestU, qDeg);
                uint32_t*       nbrBuf = ctx.nbrBufs + (size_t)depth * 64;
                uint32_t        nbrCnt = 0;
                for (uint32_t ni = 0; ni < qDeg; ++ni) {
                    uint32_t w = qNbrs[ni];
                    if (!ctx.placed[w]) {
                        nbrBuf[nbrCnt++] = w;
                    }
                }
                std::sort(nbrBuf, nbrBuf + nbrCnt, [&](uint32_t a, uint32_t b) {
                    return ctx.getCands(a).second < ctx.getCands(b).second;
                });
                ctx.nbrCnts[depth] = nbrCnt;
            }

            /* ── Symmetry-breaking bounds ──────────────────────────── *
             * Compute [symLo, symHi) once on entry via binary search.  *
             * candIdx is advanced to startCi; symEndArr[depth] stores  *
             * endCi so the ITERATE while-condition enforces the upper  *
             * bound without any per-candidate check.                   *
             *                                                           *
             * symPruned accounts for candidates outside [lo, hi).     */
            {
                const VertexID* lPtrE = ctx.candPtr(bestU, ctx.candTop[bestU] - 1);
                const uint32_t  lSzE  = ctx.candSzFlat[bestU * (ctx.qn + 1)
                                                        + ctx.candTop[bestU] - 1];
                VertexID symLo = 0, symHi = UINT32_MAX;
                auto it = ctx.symConstraints.find(bestU);
                if (it != ctx.symConstraints.end()) {
                    for (VertexID w : it->second.first)
                        if (ctx.placed[w])
                            symLo = std::max(symLo, ctx.mapping[w] + 1);
                    for (VertexID w : it->second.second)
                        if (ctx.placed[w])
                            symHi = std::min(symHi, ctx.mapping[w]);
                }
                const uint32_t startCi = static_cast<uint32_t>(
                    std::lower_bound(lPtrE, lPtrE + lSzE, symLo) - lPtrE);
                const uint32_t endCi = static_cast<uint32_t>(
                    std::lower_bound(lPtrE + startCi, lPtrE + lSzE, symHi) - lPtrE);
                ctx.candIdx[depth]   = startCi;
                ctx.symEndArr[depth] = endCi;
                ctx.symPruned[depth] += startCi + (lSzE - endCi);
            }
            fresh = false;
        }

        /* ── ITERATE ─────────────────────────────────────────────── */
        {
            const uint32_t  bestU     = ctx.dynOrder[depth];
            const VertexID* lPtr      = ctx.candPtr(bestU, ctx.candTop[bestU] - 1);
            uint32_t* const nbrBuf    = ctx.nbrBufs + (size_t)depth * 64;
            const uint32_t  nbrCnt    = ctx.nbrCnts[depth];
            bool            descended = false;

            while (ctx.candIdx[depth] < ctx.symEndArr[depth]) {
                if (ctx.matchCount >= ctx.matchLimit) return;
                const VertexID v = lPtr[ctx.candIdx[depth]++];
                if (ctx.inMapping[v]) continue;
                ++ctx.probeCount[depth];

                if constexpr (Mode == AblationMode::FULL) {
                    if (ctx.probeCount[depth] - ctx.snapProbe[depth] >= ADAPT_WINDOW) {
                        ctx.adaptStrategy(depth);
                    }
                }

                /* ── probe ───────────────────────────────────────── */
                if constexpr (Mode == AblationMode::NO_DT) {
                    bool fwd_fail = false;
                    for (uint32_t ni = 0; ni < nbrCnt && !fwd_fail; ++ni) {
                        if (!ctx.narrowCands(nbrBuf[ni], v, depth)) {
                            fwd_fail = true;
                            ++ctx.licmPruned[depth];
                        }
                    }
                    if (fwd_fail) {
                        ctx.undoCands(depth);
                        continue;
                    }
                    ctx.matched[depth] = v;
                } else {
                    const bool use_dt_first =
                        (Mode == AblationMode::FULL) && ctx.dtFirst[depth];

                    if (use_dt_first) {
                        ctx.matched[depth] = v;
                        if (ctx.decisionTree.update_for_depth(
                                depth, ctx.matched, phi) == NS_FALSE) {
                            ctx.decisionTree.undo_to(depth);
                            ++ctx.dtPruned[depth];
                            continue;
                        }
                        bool fwd_fail = false;
                        for (uint32_t ni = 0; ni < nbrCnt && !fwd_fail; ++ni) {
                            if (!ctx.narrowCands(nbrBuf[ni], v, depth)) {
                                fwd_fail = true;
                                ++ctx.licmPruned[depth];
                            }
                        }
                        if (fwd_fail) {
                            ctx.undoCands(depth);
                            ctx.decisionTree.undo_to(depth);
                            continue;
                        }
                    } else {
                        bool fwd_fail = false;
                        for (uint32_t ni = 0; ni < nbrCnt && !fwd_fail; ++ni) {
                            if (!ctx.narrowCands(nbrBuf[ni], v, depth)) {
                                fwd_fail = true;
                                ++ctx.licmPruned[depth];
                            }
                        }
                        if (fwd_fail) {
                            ctx.undoCands(depth);
                            continue;
                        }
                        ctx.matched[depth] = v;
                        if (ctx.decisionTree.update_for_depth(
                                depth, ctx.matched, phi) == NS_FALSE) {
                            ctx.decisionTree.undo_to(depth);
                            ++ctx.dtPruned[depth];
                            ctx.undoCands(depth);
                            continue;
                        }
                    }
                }

                /* ── candidate passed — place and descend/record ─── */
                ctx.mapping[bestU] = v;
                ctx.inMapping[v]   = true;
                ctx.placed[bestU]  = true;

                if ((uint32_t)depth + 1 == ctx.qn) {
                    bool accepted;
                    if constexpr (Mode == AblationMode::NO_DT) {
                        int coords[32];
                        accepted = phi_brute_check(ctx.decisionTree,
                                                   ctx.matched, ctx.qn,
                                                   coords, 0, phi);
                    } else {
                        accepted = (ctx.decisionTree.nodes[0] == NS_TRUE);
                    }
                    if (accepted) {
                        if (ctx.autGenerators.empty()) {
                            /* No symmetry breaking — one canonical match = one result. */
                            ++ctx.matchCount;
                            if (ctx.matchTrie) {
                                ctx.matchTrie->insert(ctx.matched, ctx.qn);
                            } else if (ctx.onMatch) {
                                ctx.onMatch(ctx.matched, ctx.dynOrder, ctx.qn);
                            }
                        } else {
                            /* Symmetry breaking active — BFS-expand the canonical
                             * match to its full orbit under the automorphism group,
                             * counting and reporting every equivalent match.         */
                            generateMatchOrbit(ctx, ctx.autGenerators);
                        }
                    }
                    ctx.mapping[bestU] = UINT32_MAX;
                    ctx.inMapping[v]   = false;
                    ctx.placed[bestU]  = false;
                    ctx.undoCands(depth);
                    if constexpr (Mode != AblationMode::NO_DT) {
                        ctx.decisionTree.undo_to(depth);
                    }
                    if (ctx.matchCount >= ctx.matchLimit) return;
                } else {
                    ++depth;
                    fresh     = true;
                    descended = true;
                    break;
                }
            }

            if (!descended) goto do_backtrack;
        }
        continue;

    do_backtrack:
        if (depth == 0) return;
        --depth;
        {
            const uint32_t bu = ctx.dynOrder[depth];
            ctx.mapping[bu]                   = UINT32_MAX;
            ctx.inMapping[ctx.matched[depth]] = false;
            ctx.placed[bu]                    = false;
            ctx.undoCands(depth);
            if constexpr (Mode != AblationMode::NO_DT) {
                ctx.decisionTree.undo_to(depth);
            }
            if (ctx.matchCount >= ctx.matchLimit) return;

            const uint32_t childFail = ctx.failPos[depth + 1];
            if (childFail != UINT32_MAX) {
                ctx.updateFail(depth, childFail);
                while (childFail < (uint32_t)depth) {
                    --depth;
                    const uint32_t bu2 = ctx.dynOrder[depth];
                    ctx.mapping[bu2]                  = UINT32_MAX;
                    ctx.inMapping[ctx.matched[depth]] = false;
                    ctx.placed[bu2]                   = false;
                    ctx.undoCands(depth);
                    if constexpr (Mode != AblationMode::NO_DT) {
                        ctx.decisionTree.undo_to(depth);
                    }
                    if (ctx.matchCount >= ctx.matchLimit) return;
                }
            }
        }
        fresh = false;
    }
}

/* ── Named entry points ──────────────────────────────────────────── */

template<typename Fn>
inline void backtrack(EnumContext& ctx, Fn&& phi)
{
    backtrack_impl<AblationMode::FULL>(ctx, std::forward<Fn>(phi));
}

template<typename Fn>
inline void backtrack_no_dt(EnumContext& ctx, Fn&& phi)
{
    backtrack_impl<AblationMode::NO_DT>(ctx, std::forward<Fn>(phi));
}

template<typename Fn>
inline void backtrack_no_adapt(EnumContext& ctx, Fn&& phi)
{
    backtrack_impl<AblationMode::NO_ADAPT>(ctx, std::forward<Fn>(phi));
}

/* ================================================================== *
 *  enumerate — public entry points (Order parameter removed)          *
 * ================================================================== */

inline uint64_t enumerate(const QueryGraph&   query,
                           const Graph&        data,
                           const CandidateSet& candidates,
                           const Quantifiers&  quantifiers,
                           uint64_t            limit = UINT64_MAX,
                           const Order&        staticOrder = {},
                           bool                sym = false)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit, staticOrder, sym);
    backtrack(ctx, [](const int*) noexcept { return true; });
    return ctx.matchCount;
}

template<typename Fn>
inline uint64_t enumerate(const QueryGraph&   query,
                           const Graph&        data,
                           const CandidateSet& candidates,
                           const Quantifiers&  quantifiers,
                           uint64_t            limit,
                           Fn&&                phi,
                           const Order&        staticOrder = {},
                           bool                sym = false)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit, staticOrder, sym);
    backtrack(ctx, std::forward<Fn>(phi));
    return ctx.matchCount;
}

/* ── Ablation entry points ───────────────────────────────────────── *
 *                                                                      *
 * enumerate_no_dt    — structural matching only; phi checked by       *
 *                      brute force at every full match (O(n^k)).      *
 *                      Measures the contribution of DT pruning.       *
 *                                                                      *
 * enumerate_no_adapt — DT active but probe order is always            *
 *                      narrow-first.  Measures the contribution of    *
 *                      adaptive probe ordering.                       *
 *                                                                      *
 * Both ablations share the same EnumContext, filter, and candidate    *
 * set as the full system to ensure a fair comparison.                 */
template<typename Fn>
inline uint64_t enumerate_no_dt(const QueryGraph&   query,
                                  const Graph&        data,
                                  const CandidateSet& candidates,
                                  const Quantifiers&  quantifiers,
                                  uint64_t            limit,
                                  Fn&&                phi,
                                  const Order&        staticOrder = {},
                                  bool                sym = false)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit, staticOrder, sym);
    backtrack_no_dt(ctx, std::forward<Fn>(phi));
    return ctx.matchCount;
}

template<typename Fn>
inline uint64_t enumerate_no_adapt(const QueryGraph&   query,
                                     const Graph&        data,
                                     const CandidateSet& candidates,
                                     const Quantifiers&  quantifiers,
                                     uint64_t            limit,
                                     Fn&&                phi,
                                     const Order&        staticOrder = {},
                                     bool                sym = false)
{
    EnumContext ctx(data, query, candidates, quantifiers, limit, staticOrder, sym);
    backtrack_no_adapt(ctx, std::forward<Fn>(phi));
    return ctx.matchCount;
}
