#pragma once
#include "graph.h"
#include <vector>
#include <algorithm>
#include <string>
#include <stdexcept>
#include <limits>
#include <cstdint>

/* ── Compiler portability shims ─────────────────────────────────── *
 *                                                                    *
 * GCC/Clang expose __builtin_popcount and __builtin_ctz as          *
 * intrinsics.  MSVC does not; use the equivalent _BitScanForward    *
 * and __popcnt intrinsics instead.                                  */
#if defined(_MSC_VER)
#  include <intrin.h>
   inline int bit_popcount(uint32_t x) { return static_cast<int>(__popcnt(x)); }
   inline uint32_t bit_ctz(uint32_t x) {
       unsigned long idx;
       _BitScanForward(&idx, x);
       return static_cast<uint32_t>(idx);
   }
#else
   inline int      bit_popcount(uint32_t x) { return __builtin_popcount(x); }
   inline uint32_t bit_ctz(uint32_t x)      { return (uint32_t)__builtin_ctz(x); }
#endif

/**
 * Order = the sequence in which query vertices are matched during enumeration.
 * order[0] is matched first, order[n-1] last.
 */
using Order = std::vector<VertexID>;

/* ================================================================== *
 *  OrderStrategy                                                       *
 *                                                                      *
 *  DYNAMIC  — no static order is precomputed; at each backtrack depth *
 *             the unplaced vertex with the smallest CURRENT candidate  *
 *             set is chosen on-the-fly (original fail-first logic).   *
 *             Order vector passed to EnumContext must be empty.        *
 *                                                                      *
 *  GQL      — static order based on GraphQL's strategy:               *
 *             start from min-candidate vertex, then greedily extend   *
 *             by always picking the adjacent unvisited vertex with     *
 *             the fewest candidates.  Tie-break: highest degree.       *
 *                                                                      *
 *  RI       — static RI-DS order: three-priority greedy selection     *
 *             (max backward neighbours → max forward coverage →        *
 *              max new forward neighbours).  Does not use candidates.  *
 *                                                                      *
 *  DEGREE   — static order: highest-degree vertex first, then the     *
 *             adjacent unvisited vertex with the highest degree.       *
 *             Tie-break for start: most core-number.                  *
 *             Fastest to compute; useful as a sanity-check baseline.  *
 *                                                                      *
 *  DP       — bitmask DP over all 2^n query-vertex subsets.           *
 *             Minimises total estimated enumeration work by choosing  *
 *             the extension order with the smallest cumulative cost.  *
 *             Requires full CandidateSet and data Graph for edge      *
 *             cardinality estimates; use the overloaded computeOrder  *
 *             that accepts (strategy, query, data, candidates, order).*
 *             Falls back to GQL when n > DP_MAX_N (default 20).      *
 * ================================================================== */
enum class OrderStrategy {
    DYNAMIC,
    GQL,
    RI,
    DEGREE,
    DP,
};

/* Convert string to enum (convenience for CLI argument parsing) */
inline OrderStrategy orderStrategyFromString(const std::string& s)
{
    if (s == "dynamic" || s == "DYNAMIC") return OrderStrategy::DYNAMIC;
    if (s == "gql"     || s == "GQL")     return OrderStrategy::GQL;
    if (s == "ri"      || s == "RI")      return OrderStrategy::RI;
    if (s == "degree"  || s == "DEGREE")  return OrderStrategy::DEGREE;
    if (s == "dp"      || s == "DP")      return OrderStrategy::DP;
    throw std::invalid_argument("Unknown order strategy: " + s);
}

inline const char* orderStrategyName(OrderStrategy s)
{
    switch (s) {
        case OrderStrategy::DYNAMIC: return "DYNAMIC";
        case OrderStrategy::GQL:     return "GQL";
        case OrderStrategy::RI:      return "RI";
        case OrderStrategy::DEGREE:  return "DEGREE";
        case OrderStrategy::DP:      return "DP";
    }
    return "UNKNOWN";
}

/* ------------------------------------------------------------------ *
 *  intersect                                                           *
 *                                                                      *
 *  Two-pointer intersection of two sorted arrays.                     *
 *  Returns the number of common elements written into out[].          *
 * ------------------------------------------------------------------ */
inline uint32_t intersect(const VertexID* a, uint32_t sizeA,
                           const VertexID* b, uint32_t sizeB,
                           VertexID*       out)
{
    uint32_t i = 0, j = 0, cnt = 0;
    while (i < sizeA && j < sizeB) {
        if      (a[i] < b[j]) ++i;
        else if (a[i] > b[j]) ++j;
        else { out[cnt++] = a[i]; ++i; ++j; }
    }
    return cnt;
}

/* ------------------------------------------------------------------ *
 *  backwardNeighborCount                                               *
 *                                                                      *
 *  Returns the number of already-ordered vertices adjacent to u.      *
 *  Used by RI ordering and as a helper for GQL start-vertex choice.   *
 * ------------------------------------------------------------------ */
inline uint32_t backwardNeighborCount(VertexID u,
                                       const QueryGraph& query,
                                       const Order&      placed,
                                       uint32_t          placedCount)
{
    uint32_t bn = 0;
    for (uint32_t k = 0; k < placedCount; ++k)
        if (query.hasEdge(u, placed[k]))
            ++bn;
    return bn;
}

/* ================================================================== *
 *  orderByGQL                                                          *
 *                                                                      *
 *  Static order following GraphQL's matching-order heuristic.         *
 *                                                                      *
 *  Start vertex selection                                              *
 *  ─────────────────────                                              *
 *    The vertex with the minimum number of candidates is chosen.      *
 *    Ties broken by: (1) maximum degree, (2) maximum core number.     *
 *                                                                      *
 *  Subsequent vertex selection                                         *
 *  ──────────────────────────                                         *
 *    At each step, pick the unvisited vertex u such that:             *
 *      • u has at least one already-ordered neighbour (adjacent-first) *
 *      • among adjacent candidates, u has the fewest candidates        *
 *      • ties broken by maximum degree, then maximum core number       *
 *    If no adjacent unvisited vertex exists (disconnected query),      *
 *    fall back to global minimum-candidate selection.                  *
 *                                                                      *
 *  candidateCount[u] must hold the GQL-filtered candidate count for   *
 *  each query vertex u before this function is called.                *
 * ================================================================== */
inline void orderByGQL(const QueryGraph&              query,
                        const std::vector<uint32_t>&   candidateCount,
                        Order&                         order)
{
    const uint32_t n = query.getNumVertices();
    order.assign(n, UINT32_MAX);
    std::vector<bool> visited(n, false);
    std::vector<bool> adjacent(n, false);  // reachable from already-ordered set

    /* ── Start vertex: min candidates, tie-break degree then core ── */
    {
        uint32_t bestCands  = UINT32_MAX;
        uint32_t bestDeg    = 0;
        int      bestCore   = -1;
        VertexID start      = 0;

        for (VertexID v = 0; v < n; ++v) {
            uint32_t cands = candidateCount[v];
            uint32_t deg   = query.getDegree(v);
            int      core  = query.getCoreNum(v);

            bool better =
                (cands < bestCands) ||
                (cands == bestCands && deg   > bestDeg) ||
                (cands == bestCands && deg   == bestDeg && core > bestCore);

            if (better) {
                bestCands = cands;
                bestDeg   = deg;
                bestCore  = core;
                start     = v;
            }
        }

        order[0]        = start;
        visited[start]  = true;

        uint32_t sDeg;
        const VertexID* sNbrs = query.getNeighbors(start, sDeg);
        for (uint32_t i = 0; i < sDeg; ++i)
            adjacent[sNbrs[i]] = true;
    }

    /* ── Remaining vertices ──────────────────────────────────────── */
    for (uint32_t step = 1; step < n; ++step) {

        uint32_t bestCands  = UINT32_MAX;
        uint32_t bestDeg    = 0;
        int      bestCore   = -1;
        VertexID chosen     = UINT32_MAX;
        bool     foundAdj   = false;  // have we found an adjacent candidate yet?

        for (VertexID u = 0; u < n; ++u) {
            if (visited[u]) continue;

            bool isAdj = adjacent[u];

            /* Adjacent vertices are strictly preferred over non-adjacent.
             * Once an adjacent vertex is found, skip all non-adjacent. */
            if (foundAdj && !isAdj) continue;

            uint32_t cands = candidateCount[u];
            uint32_t deg   = query.getDegree(u);
            int      core  = query.getCoreNum(u);

            /* Switching from non-adjacent to adjacent: reset best */
            if (isAdj && !foundAdj) {
                foundAdj = true;
                bestCands = UINT32_MAX; bestDeg = 0; bestCore = -1;
            }

            bool better =
                (cands < bestCands) ||
                (cands == bestCands && deg   > bestDeg) ||
                (cands == bestCands && deg   == bestDeg && core > bestCore);

            if (better) {
                bestCands = cands;
                bestDeg   = deg;
                bestCore  = core;
                chosen    = u;
            }
        }

        order[step]         = chosen;
        visited[chosen]     = true;

        /* Expand adjacency frontier */
        uint32_t cDeg;
        const VertexID* cNbrs = query.getNeighbors(chosen, cDeg);
        for (uint32_t i = 0; i < cDeg; ++i)
            adjacent[cNbrs[i]] = true;
    }
}

/* ================================================================== *
 *  orderByRI                                                           *
 *                                                                      *
 *  Static RI-DS order.  Three-priority greedy selection:              *
 *                                                                      *
 *  Priority 1 — max backward neighbours (bn):                         *
 *    Pick the unvisited vertex sharing the most edges with the        *
 *    already-ordered set.  Maximises constraints applied early.       *
 *                                                                      *
 *  Priority 2 — max forward neighbourhood coverage (tie-break):      *
 *    Among equal-bn vertices, pick u whose unvisited neighbours are   *
 *    most reachable from already-ordered vertices.                    *
 *                                                                      *
 *  Priority 3 — max unconnected forward neighbours (tie-break):      *
 *    Among still-equal vertices, pick u with the most unvisited       *
 *    neighbours that have NO edge to any ordered vertex.              *
 *    Extends coverage into unexplored regions of the query.           *
 *                                                                      *
 *  Start vertex: highest degree (tie-break: highest core number).     *
 *  Does not use candidate counts.                                     *
 * ================================================================== */
inline void orderByRI(const QueryGraph& query, Order& order)
{
    const uint32_t n = query.getNumVertices();
    order.assign(n, 0);
    std::vector<bool> visited(n, false);

    /* ── Start vertex: max degree, tie-break: max core number ─────── */
    {
        uint32_t bestDeg  = 0;
        int      bestCore = -1;
        for (uint32_t v = 0; v < n; ++v) {
            uint32_t deg  = query.getDegree(v);
            int      core = query.getCoreNum(v);
            if (deg > bestDeg || (deg == bestDeg && core > bestCore)) {
                bestDeg  = deg;
                bestCore = core;
                order[0] = v;
            }
        }
    }
    visited[order[0]] = true;

    /* scratch buffers reused across iterations */
    std::vector<VertexID> cands, nextCands;
    std::vector<VertexID> unvisNbrs;
    std::vector<VertexID> tmpBuf(n);

    /* ── Remaining n-1 vertices ──────────────────────────────────── */
    for (uint32_t step = 1; step < n; ++step) {

        /* --- Priority 1: maximise backward-neighbour count --- */
        uint32_t bestBN = 0;
        cands.clear();

        for (uint32_t u = 0; u < n; ++u) {
            if (visited[u]) continue;
            uint32_t bn = 0;
            for (uint32_t k = 0; k < step; ++k)
                if (query.hasEdge(u, order[k]))
                    ++bn;

            if (bn > bestBN) {
                bestBN = bn;
                cands.clear();
                cands.push_back(u);
            } else if (bn == bestBN) {
                cands.push_back(u);
            }
        }

        /* --- Priority 2: maximise forward-coverage count --- */
        if (cands.size() > 1) {
            uint32_t bestCov = 0;
            nextCands.clear();

            for (VertexID u : cands) {
                unvisNbrs.clear();
                uint32_t uDeg;
                const VertexID* uNbrs = query.getNeighbors(u, uDeg);
                for (uint32_t i = 0; i < uDeg; ++i)
                    if (!visited[uNbrs[i]])
                        unvisNbrs.push_back(uNbrs[i]);

                uint32_t cov = 0;
                for (uint32_t k = 0; k < step; ++k) {
                    uint32_t oDeg;
                    const VertexID* oNbrs = query.getNeighbors(order[k], oDeg);
                    if (intersect(oNbrs, oDeg,
                                  unvisNbrs.data(), (uint32_t)unvisNbrs.size(),
                                  tmpBuf.data()) > 0)
                        ++cov;
                }

                if (cov > bestCov) {
                    bestCov = cov;
                    nextCands.clear();
                    nextCands.push_back(u);
                } else if (cov == bestCov) {
                    nextCands.push_back(u);
                }
            }
            cands.swap(nextCands);
        }

        /* --- Priority 3: maximise purely-new forward neighbours --- */
        if (cands.size() > 1) {
            uint32_t bestNew = 0;
            nextCands.clear();

            for (VertexID u : cands) {
                unvisNbrs.clear();
                uint32_t uDeg;
                const VertexID* uNbrs = query.getNeighbors(u, uDeg);
                for (uint32_t i = 0; i < uDeg; ++i)
                    if (!visited[uNbrs[i]])
                        unvisNbrs.push_back(uNbrs[i]);

                uint32_t newCount = 0;
                for (VertexID w : unvisNbrs) {
                    bool touched = false;
                    for (uint32_t k = 0; k < step && !touched; ++k)
                        touched = query.hasEdge(w, order[k]);
                    if (!touched) ++newCount;
                }

                if (newCount > bestNew) {
                    bestNew = newCount;
                    nextCands.clear();
                    nextCands.push_back(u);
                } else if (newCount == bestNew) {
                    nextCands.push_back(u);
                }
            }
            cands.swap(nextCands);
        }

        order[step] = cands[0];
        visited[order[step]] = true;
    }
}

/* ================================================================== *
 *  orderByDegree                                                       *
 *                                                                      *
 *  Static degree-greedy order.  Cheapest to compute; good baseline.  *
 *                                                                      *
 *  Start vertex: max degree, tie-break: max core number.             *
 *                                                                      *
 *  Subsequent vertices: among unvisited vertices adjacent to the      *
 *  already-ordered set, pick the one with the highest degree.         *
 *  Tie-break: highest core number.                                    *
 *  Fall back to global max-degree if the query is disconnected.       *
 * ================================================================== */
inline void orderByDegree(const QueryGraph& query, Order& order)
{
    const uint32_t n = query.getNumVertices();
    order.assign(n, UINT32_MAX);
    std::vector<bool> visited(n, false);
    std::vector<bool> adjacent(n, false);

    /* ── Start vertex ────────────────────────────────────────────── */
    {
        uint32_t bestDeg  = 0;
        int      bestCore = -1;
        VertexID start    = 0;
        for (VertexID v = 0; v < n; ++v) {
            uint32_t deg  = query.getDegree(v);
            int      core = query.getCoreNum(v);
            if (deg > bestDeg || (deg == bestDeg && core > bestCore)) {
                bestDeg  = deg;
                bestCore = core;
                start    = v;
            }
        }
        order[0]       = start;
        visited[start] = true;

        uint32_t sDeg;
        const VertexID* sNbrs = query.getNeighbors(start, sDeg);
        for (uint32_t i = 0; i < sDeg; ++i)
            adjacent[sNbrs[i]] = true;
    }

    /* ── Remaining vertices ──────────────────────────────────────── */
    for (uint32_t step = 1; step < n; ++step) {

        uint32_t bestDeg  = 0;
        int      bestCore = -1;
        VertexID chosen   = UINT32_MAX;
        bool     foundAdj = false;

        for (VertexID u = 0; u < n; ++u) {
            if (visited[u]) continue;

            bool isAdj = adjacent[u];
            if (foundAdj && !isAdj) continue;

            uint32_t deg  = query.getDegree(u);
            int      core = query.getCoreNum(u);

            if (isAdj && !foundAdj) {
                foundAdj = true;
                bestDeg = 0; bestCore = -1;
            }

            if (deg > bestDeg || (deg == bestDeg && core > bestCore)) {
                bestDeg  = deg;
                bestCore = core;
                chosen   = u;
            }
        }

        order[step]     = chosen;
        visited[chosen] = true;

        uint32_t cDeg;
        const VertexID* cNbrs = query.getNeighbors(chosen, cDeg);
        for (uint32_t i = 0; i < cDeg; ++i)
            adjacent[cNbrs[i]] = true;
    }
}

/* ================================================================== *
 *  computeOrder — unified dispatcher                                   *
 *                                                                      *
 *  Computes a static order into 'order' for the given strategy.       *
 *  For DYNAMIC, order is cleared (the caller should pass an empty     *
 *  Order to EnumContext, which then uses on-the-fly selection).        *
 *                                                                      *
 *  candidateCount is only used by GQL; pass an empty vector for       *
 *  RI / DEGREE / DYNAMIC.                                              *
 *                                                                      *
 *  For DP, use the overloaded version below that takes the data       *
 *  Graph and full CandidateSet.  Calling this overload with DP        *
 *  throws std::invalid_argument.                                      *
 * ================================================================== */
inline void computeOrder(OrderStrategy                  strategy,
                          const QueryGraph&              query,
                          const std::vector<uint32_t>&   candidateCount,
                          Order&                         order)
{
    switch (strategy) {
        case OrderStrategy::DYNAMIC:
            order.clear();   // signal to EnumContext: use dynamic selection
            break;
        case OrderStrategy::GQL:
            orderByGQL(query, candidateCount, order);
            break;
        case OrderStrategy::RI:
            orderByRI(query, order);
            break;
        case OrderStrategy::DEGREE:
            orderByDegree(query, order);
            break;
        case OrderStrategy::DP:
            throw std::invalid_argument(
                "OrderStrategy::DP requires the full CandidateSet and data "
                "Graph. Use the overloaded computeOrder(strategy, query, "
                "data, candidates, order) instead.");
    }
}

/* ================================================================== *
 *  orderByDP                                                           *
 *                                                                      *
 *  Bitmask DP over query-vertex subsets to find the matching order   *
 *  that minimises estimated total enumeration work.                   *
 *  Follows ASDMatch's optimalPlanDP cost model.                       *
 *                                                                      *
 *  Cost model                                                          *
 *  ──────────                                                          *
 *    card[S]  — estimated number of partial matches for subset S.     *
 *    dp[S]    — total estimated work:                                 *
 *               Σ_{each prefix P of S's optimal order} card[P].      *
 *                                                                      *
 *    Base case (|S|=1):                                               *
 *      card[{u}] = |cands(u)|,   dp[{u}] = |cands(u)|               *
 *                                                                      *
 *    Transition (|S|≥2): for each u∈S, let S'=S\{u}:                *
 *      listSize(u,S') — expected candidates for u per match of S':   *
 *        Σ_{v∈N_q(u)∩S'} edgeCard(u,v)/card(v)  if N_q(u)∩S' ≠ ∅  *
 *        card(u)                                  otherwise (Cartesian)*
 *      card(S) via split (S',u) = card(S') × listSize(u,S')         *
 *      dp(S)  via split (S',u) = dp(S') + card(S') × listSize(u,S') *
 *      Choose split minimising dp(S).                                 *
 *                                                                      *
 *  Edge cardinality precomputation                                     *
 *  ────────────────────────────────                                   *
 *    edgeCard(u,v) = Σ_{a∈cands(u)} |N_data(a) ∩ cands(v)|          *
 *    Computed once via two-pointer intersection.                       *
 *    O(|E_q| × |cands| × avg_data_deg) — runs in the filter phase.   *
 *                                                                      *
 *  Complexity: O(2^n × n) DP;  O(|E_q|×|C|×deg) setup.             *
 *  n > DP_MAX_N falls back to GQL automatically.                     *
 * ================================================================== */

static constexpr uint32_t DP_MAX_N = 20;   // 2^20 ≈ 1M states, ~24 MB

inline void orderByDP(const QueryGraph&                            query,
                       const Graph&                                data,
                       const std::vector<std::vector<VertexID>>&   candidates,
                       Order&                                      order)
{
    const uint32_t n = query.getNumVertices();

    /* Fallback: DP is infeasible for large query graphs */
    if (n > DP_MAX_N) {
        std::vector<uint32_t> cc(n);
        for (uint32_t u = 0; u < n; ++u)
            cc[u] = (uint32_t)candidates[u].size();
        orderByGQL(query, cc, order);
        return;
    }

    /* ── Step 1: vertex cardinalities ────────────────────────────── */
    std::vector<double> vertCard(n);
    for (uint32_t u = 0; u < n; ++u)
        vertCard[u] = (double)candidates[u].size();

    /* ── Step 2: edge cardinalities ──────────────────────────────── *
     *                                                                *
     * edgeCard[u*n+v] = Σ_{a∈cands(u)} |N_data(a) ∩ cands(v)|     *
     *                                                                *
     * The intersection reuses the `intersect` helper already in     *
     * this header.  tmpBuf must be large enough for any data-graph  *
     * neighbour list; data.getMaxDegree() is a safe upper bound.    */
    std::vector<double>   edgeCard(n * n, 0.0);
    {
        std::vector<VertexID> tmpBuf(data.getMaxDegree() + 1);

        for (uint32_t u = 0; u < n; ++u) {
            uint32_t        qDeg;
            const VertexID* qNbrs = query.getNeighbors(u, qDeg);

            for (uint32_t ni = 0; ni < qDeg; ++ni) {
                uint32_t v = qNbrs[ni];
                if (v <= u) continue;   // symmetric; compute once

                double card = 0.0;
                for (VertexID a : candidates[u]) {
                    uint32_t        aDeg;
                    const VertexID* aNbrs = data.getNeighbors(a, aDeg);
                    card += (double)intersect(
                                aNbrs, aDeg,
                                candidates[v].data(),
                                (uint32_t)candidates[v].size(),
                                tmpBuf.data());
                }
                edgeCard[u * n + v] = card;
                edgeCard[v * n + u] = card;   // symmetric
            }
        }
    }

    /* ── Step 3: bitmask DP ──────────────────────────────────────── *
     *                                                                *
     * dp[S]      — minimum cumulative work for subset S             *
     * subCard[S] — card(S) for the optimal order of S              *
     * prevU[S]   — last vertex added in the optimal split of S     *
     *              (uint8_t: vertex IDs ≤ 255 for n ≤ DP_MAX_N)   */
    const uint32_t FULL   = (1u << n) - 1;
    const uint32_t STATES = 1u << n;

    const double INF = std::numeric_limits<double>::max();
    std::vector<double>  dp(STATES, INF);
    std::vector<double>  subCard(STATES, 0.0);
    std::vector<uint8_t> prevU(STATES, 0xFF);

    /* Base cases: single-vertex subsets */
    for (uint32_t u = 0; u < n; ++u) {
        uint32_t bit   = 1u << u;
        dp[bit]        = vertCard[u];
        subCard[bit]   = vertCard[u];
        prevU[bit]     = (uint8_t)u;
    }

    /* Fill in order of increasing subset size                        *
     * (iterating S from 1 to FULL visits smaller subsets first      *
     * because smaller bitmasks have fewer set bits on average, but  *
     * this is NOT guaranteed.  We rely on dp[S']=INF as a guard:   *
     * if S' was not yet filled, the transition is skipped.          *
     * In practice the standard approach processes subsets by        *
     * popcount using a level-by-level BFS — but the simple loop    *
     * is correct here because S' = S\{u} < S bitwise, so S' is     *
     * always processed before S in the ascending loop.)             */
    for (uint32_t S = 1; S <= FULL; ++S) {
        if (bit_popcount(S) < 2) continue;

        /* Try every vertex u ∈ S as the last one added */
        uint32_t mask = S;
        while (mask) {
            uint32_t bit = mask & (uint32_t)(-(int32_t)mask);   // lowest set bit
            mask        &= mask - 1;
            uint32_t u   = bit_ctz(bit);
            uint32_t Sp  = S ^ bit;     // S' = S \ {u}

            if (dp[Sp] >= INF) continue;

            /* Compute listSize(u, S') */
            double   listSize    = 0.0;
            bool     hasNeighbor = false;
            uint32_t uqDeg;
            const VertexID* uqNbrs = query.getNeighbors(u, uqDeg);

            for (uint32_t ni = 0; ni < uqDeg; ++ni) {
                uint32_t v = uqNbrs[ni];
                if (!(Sp & (1u << v))) continue;    // v not in S'
                hasNeighbor = true;
                double cv = vertCard[v];
                if (cv > 0.0)
                    listSize += edgeCard[u * n + v] / cv;
            }

            if (!hasNeighbor)
                listSize = vertCard[u];    // Cartesian: u disconnected from S'

            double newCard = subCard[Sp] * listSize;
            double newCost = dp[Sp] + subCard[Sp] * listSize;

            if (newCost < dp[S]) {
                dp[S]      = newCost;
                subCard[S] = newCard;
                prevU[S]   = (uint8_t)u;
            }
        }
    }

    /* ── Step 4: reconstruct order ───────────────────────────────── *
     *                                                                *
     * prevU[S] = last vertex added to S in the optimal order.       *
     * Walk backwards from FULL to recover order[n-1]…order[0].     */
    order.resize(n);
    uint32_t S = FULL;
    for (int i = (int)n - 1; i >= 0; --i) {
        order[i] = (VertexID)prevU[S];
        S        ^= (1u << prevU[S]);
    }
}

/* ================================================================== *
 *  computeOrder overload — DP-capable version                         *
 *                                                                      *
 *  Accepts the full data Graph and CandidateSet so that OrderStrategy *
 *  ::DP can compute edge cardinalities.  For all other strategies,   *
 *  the candidate counts are extracted and the original dispatcher is *
 *  called, so callers can use this overload uniformly.               *
 * ================================================================== */
inline void computeOrder(OrderStrategy                            strategy,
                          const QueryGraph&                       query,
                          const Graph&                            data,
                          const std::vector<std::vector<VertexID>>& candidates,
                          Order&                                  order)
{
    if (strategy == OrderStrategy::DP) {
        orderByDP(query, data, candidates, order);
        return;
    }
    /* All other strategies only need candidate counts */
    std::vector<uint32_t> cc(query.getNumVertices());
    for (uint32_t u = 0; u < query.getNumVertices(); ++u)
        cc[u] = (uint32_t)candidates[u].size();
    computeOrder(strategy, query, cc, order);
}

