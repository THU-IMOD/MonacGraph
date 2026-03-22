#pragma once
#include "graph.h"
#include <vector>
#include <queue>
#include <algorithm>
#include <string>
#include <stdexcept>
#include <limits>

/**
 * CandidateSet[u] = all data vertices that may match query vertex u.
 * Built by the filter phase and consumed by enumeration.
 */
using CandidateSet = std::vector<std::vector<VertexID>>;

/* ================================================================== *
 *  FilterStrategy                                                      *
 *                                                                      *
 *  LDF   — Label + Degree Filter.                                     *
 *          v passes for u iff label(v)==label(u) and deg(v)>=deg(u).  *
 *          Cheapest; weakest pruning.                                 *
 *                                                                      *
 *  NLF   — Neighbourhood Label Frequency filter.                      *
 *          LDF + NLF containment: for every label l, v must have >=  *
 *          as many l-labelled neighbours as u. No structural pass.    *
 *                                                                      *
 *  GQL   — GraphQL filter.                                            *
 *          NLF + structural refinement to fixed point (Phase 2):     *
 *          repeatedly remove candidates lacking a support neighbour   *
 *          until convergence.                                          *
 *                                                                      *
 *  DPISO — DP-iso filter.                                             *
 *          NLF initialisation + BFS spanning tree of the query graph  *
 *          + 3 rounds of bidirectional structural pruning guided by   *
 *          the tree's forward / backward neighbour lists.             *
 *          More targeted than GQL's full scan; faster on dense        *
 *          queries while remaining bounded in cost.                   *
 *                                                                      *
 *  CFL   — CFL filter (default in FiPE).                             *
 *          Builds a BFS spanning tree, classifies each query edge     *
 *          into three roles: backward (bn), same-level-forward (fn), *
 *          and under-level (ul). Then runs:                           *
 *            Phase 1 (top-down):  generate candidates for each level  *
 *              using bn as pivots (forward gen), then prune within    *
 *              the same level using fn as pivots (backward prune).    *
 *            Phase 2 (bottom-up): prune each vertex using ul as       *
 *              pivots, propagating constraints upward.                *
 *          Avoids GQL's unbounded fixed-point iteration; cost is      *
 *          bounded by one top-down + one bottom-up pass.              *
 * ================================================================== */
enum class FilterStrategy { LDF, NLF, GQL, DPISO, CFL };

inline FilterStrategy filterStrategyFromString(const std::string& s)
{
    if (s == "ldf"   || s == "LDF")   return FilterStrategy::LDF;
    if (s == "nlf"   || s == "NLF")   return FilterStrategy::NLF;
    if (s == "gql"   || s == "GQL")   return FilterStrategy::GQL;
    if (s == "dpiso" || s == "DPISO") return FilterStrategy::DPISO;
    if (s == "cfl"   || s == "CFL")   return FilterStrategy::CFL;
    throw std::invalid_argument("Unknown filter strategy: " + s);
}

inline const char* filterStrategyName(FilterStrategy s)
{
    switch (s) {
        case FilterStrategy::LDF:   return "LDF";
        case FilterStrategy::NLF:   return "NLF";
        case FilterStrategy::GQL:   return "GQL";
        case FilterStrategy::DPISO: return "DPISO";
        case FilterStrategy::CFL:   return "CFL";
    }
    return "UNKNOWN";
}

/* ================================================================== *
 *  Internal helpers                                                    *
 * ================================================================== */

/* ------------------------------------------------------------------ *
 *  buildLabelIndex                                                     *
 *                                                                      *
 *  Groups data-graph vertices by label for O(1) label-based lookup.  *
 *  Returns labelIndex[l] = list of data vertices carrying label l.   *
 *                                                                      *
 *  Our Graph class does not expose getVerticesByLabel(), so we build  *
 *  this index locally at the start of every filter call.  The cost   *
 *  is O(dn), amortised over all query vertices.                       *
 * ------------------------------------------------------------------ */
inline std::vector<std::vector<VertexID>>
buildLabelIndex(const Graph& data)
{
    const uint32_t dn = data.getNumVertices();
    uint32_t maxLabel = 0;
    for (uint32_t v = 0; v < dn; ++v)
        maxLabel = std::max(maxLabel, data.getLabelOf(v));

    std::vector<std::vector<VertexID>> idx(maxLabel + 1);
    for (uint32_t v = 0; v < dn; ++v)
        idx[data.getLabelOf(v)].push_back(v);
    return idx;
}

/* ------------------------------------------------------------------ *
 *  passesNLF                                                           *
 *                                                                      *
 *  Returns true iff data vertex v satisfies the NLF check for query  *
 *  vertex u: for every label l, v has >= as many l-labelled           *
 *  neighbours as u does.                                              *
 * ------------------------------------------------------------------ */
inline bool passesNLF(const Graph& data, VertexID v,
                       const Graph& query, VertexID u)
{
    const auto* uNLF = query.getNbrLabelFreq(u);
    const auto* vNLF = data.getNbrLabelFreq(v);
    for (const auto& [lbl, uCnt] : *uNLF) {
        auto it = vNLF->find(lbl);
        if (it == vNLF->end() || it->second < uCnt)
            return false;
    }
    return true;
}

/* ------------------------------------------------------------------ *
 *  localFilterLDF / localFilterNLF                                    *
 *                                                                      *
 *  Phase-1 filters that fill candidates[] from scratch.              *
 *  Both require labelIdx pre-built by buildLabelIndex().             *
 * ------------------------------------------------------------------ */
inline void localFilterLDF(const QueryGraph& query,
                             const Graph&      data,
                             const std::vector<std::vector<VertexID>>& labelIdx,
                             CandidateSet& candidates)
{
    const uint32_t qn = query.getNumVertices();
    candidates.assign(qn, {});

    for (uint32_t u = 0; u < qn; ++u) {
        const LabelID  uLbl = query.getLabelOf(u);
        const uint32_t uDeg = query.getDegree(u);
        if (uLbl >= labelIdx.size()) continue;

        for (VertexID v : labelIdx[uLbl])
            if (data.getDegree(v) >= uDeg)
                candidates[u].push_back(v);
    }
}

inline void localFilterNLF(const QueryGraph& query,
                             const Graph&      data,
                             const std::vector<std::vector<VertexID>>& labelIdx,
                             CandidateSet& candidates)
{
    const uint32_t qn = query.getNumVertices();
    candidates.assign(qn, {});

    for (uint32_t u = 0; u < qn; ++u) {
        const LabelID  uLbl = query.getLabelOf(u);
        const uint32_t uDeg = query.getDegree(u);
        if (uLbl >= labelIdx.size()) continue;

        for (VertexID v : labelIdx[uLbl]) {
            if (data.getDegree(v) < uDeg) continue;
            if (passesNLF(data, v, query, u))
                candidates[u].push_back(v);
        }
    }
}

/* ------------------------------------------------------------------ *
 *  structuralRefinementGQL                                             *
 *                                                                      *
 *  Iterates to a fixed point: removes candidate v for u if, for some *
 *  query neighbour u' of u, v has no data neighbour in candidates[u'].*
 * ------------------------------------------------------------------ */
inline void structuralRefinementGQL(const QueryGraph& query,
                                     const Graph&      data,
                                     CandidateSet&     candidates)
{
    const uint32_t qn = query.getNumVertices();
    const uint32_t dn = data.getNumVertices();

    /* isCandidate[u][v]: flat membership test, updated incrementally */
    std::vector<std::vector<bool>> isCandidate(qn,
                                               std::vector<bool>(dn, false));
    for (uint32_t u = 0; u < qn; ++u)
        for (VertexID v : candidates[u])
            isCandidate[u][v] = true;

    bool changed = true;
    while (changed) {
        changed = false;
        for (uint32_t u = 0; u < qn; ++u) {
            uint32_t        uDeg;
            const VertexID* uNbrs = query.getNeighbors(u, uDeg);
            auto&           cands = candidates[u];
            uint32_t        writePos = 0;

            for (uint32_t ci = 0; ci < cands.size(); ++ci) {
                VertexID v     = cands[ci];
                bool     valid = true;

                for (uint32_t ni = 0; ni < uDeg && valid; ++ni) {
                    VertexID uPrime = uNbrs[ni];
                    uint32_t vDeg;
                    const VertexID* vNbrs = data.getNeighbors(v, vDeg);
                    bool found = false;
                    for (uint32_t vi = 0; vi < vDeg && !found; ++vi)
                        found = isCandidate[uPrime][vNbrs[vi]];
                    if (!found) valid = false;
                }

                if (valid) {
                    cands[writePos++] = v;
                } else {
                    isCandidate[u][v] = false;
                    changed = true;
                }
            }
            cands.resize(writePos);
        }
    }
}

/* ------------------------------------------------------------------ *
 *  SpanTree                                                            *
 *                                                                      *
 *  BFS spanning tree of the query graph.  Used by DPiso to guide     *
 *  structural pruning along query-tree edges.                         *
 *                                                                      *
 *  bfsOrder[i]  — query vertex at BFS position i                     *
 *  bfsPos[u]    — BFS position of query vertex u                     *
 *  bn[u]        — query neighbours of u with SMALLER bfsPos          *
 *                 (backward neighbours: already processed in fwd pass)*
 *  fn[u]        — query neighbours of u with LARGER bfsPos           *
 *                 (forward neighbours: already processed in bwd pass) *
 * ------------------------------------------------------------------ */
struct SpanTree {
    std::vector<VertexID>              bfsOrder;
    std::vector<uint32_t>              bfsPos;
    std::vector<std::vector<VertexID>> bn;
    std::vector<std::vector<VertexID>> fn;
};

inline SpanTree buildBFSTree(const QueryGraph& query, VertexID root)
{
    const uint32_t qn = query.getNumVertices();
    SpanTree t;
    t.bfsOrder.reserve(qn);
    t.bfsPos.assign(qn, UINT32_MAX);
    t.bn.resize(qn);
    t.fn.resize(qn);

    std::vector<bool>    visited(qn, false);
    std::queue<VertexID> bfsQ;
    bfsQ.push(root);
    visited[root] = true;

    while (!bfsQ.empty()) {
        VertexID u = bfsQ.front(); bfsQ.pop();
        t.bfsPos[u] = (uint32_t)t.bfsOrder.size();
        t.bfsOrder.push_back(u);

        uint32_t        uDeg;
        const VertexID* uNbrs = query.getNeighbors(u, uDeg);
        for (uint32_t i = 0; i < uDeg; ++i) {
            VertexID w = uNbrs[i];
            if (!visited[w]) { visited[w] = true; bfsQ.push(w); }
        }
    }

    /* Classify every query edge as forward or backward */
    for (uint32_t u = 0; u < qn; ++u) {
        uint32_t        uDeg;
        const VertexID* uNbrs = query.getNeighbors(u, uDeg);
        for (uint32_t i = 0; i < uDeg; ++i) {
            VertexID w = uNbrs[i];
            if (t.bfsPos[w] < t.bfsPos[u]) t.bn[u].push_back(w);
            else                            t.fn[u].push_back(w);
        }
    }

    return t;
}

/* ------------------------------------------------------------------ *
 *  pruneCandidatesByPivots                                             *
 *                                                                      *
 *  Removes from candidates[u] every vertex v that does NOT have a    *
 *  data-graph neighbour inside candidates[pi] for every pivot pi.    *
 *                                                                      *
 *  Algorithm (flag counter):                                           *
 *    For each pivot pi, walk candidates[pi] and for each pv, mark    *
 *    each data neighbour w of pv: flag[w]++ if flag[w]==pivotsSeen.  *
 *    After all pivots, v survives iff flag[v] == |pivots|.            *
 *    Reset all touched flags to 0 before returning.                  *
 *                                                                      *
 *  Complexity: O( Σ_{pi} |cands[pi]| × avg_data_deg + |cands[u]| )  *
 *  — no quadratic scan over dn, unlike the isCandidate matrix.        *
 * ------------------------------------------------------------------ */
inline void pruneCandidatesByPivots(VertexID                     u,
                                     const std::vector<VertexID>& pivots,
                                     const Graph&                 data,
                                     CandidateSet&                candidates,
                                     std::vector<uint32_t>&       flag,
                                     std::vector<VertexID>&       touched)
{
    if (pivots.empty()) return;

    uint32_t pivotsSeen  = 0;
    uint32_t touchedSize = 0;

    for (VertexID pivot : pivots) {
        for (VertexID pv : candidates[pivot]) {
            uint32_t        pvDeg;
            const VertexID* pvNbrs = data.getNeighbors(pv, pvDeg);
            for (uint32_t k = 0; k < pvDeg; ++k) {
                VertexID w = pvNbrs[k];
                if (flag[w] == pivotsSeen) {
                    flag[w]++;
                    /* Record w on the first pivot so we can reset later */
                    if (pivotsSeen == 0) {
                        if (touchedSize < touched.size())
                            touched[touchedSize] = w;
                        else
                            touched.push_back(w);
                        ++touchedSize;
                    }
                }
            }
        }
        ++pivotsSeen;
    }

    /* Compact candidates[u] in place */
    auto&    cands    = candidates[u];
    uint32_t writePos = 0;
    for (VertexID v : cands)
        if (flag[v] == pivotsSeen)
            cands[writePos++] = v;
    cands.resize(writePos);

    /* Reset flags (only the touched vertices) */
    for (uint32_t i = 0; i < touchedSize; ++i)
        flag[touched[i]] = 0;
}

/* ------------------------------------------------------------------ *
 *  CFLTree                                                             *
 *                                                                      *
 *  BFS spanning tree augmented with three-way edge classification     *
 *  required by CFL filtering.                                          *
 *                                                                      *
 *  For each query vertex u:                                            *
 *    bn[u]  — backward neighbours: level(w) < level(u), OR           *
 *             same level with smaller BFS position.                   *
 *             Used as pivots in the top-down generation step.         *
 *    fn[u]  — same-level forward neighbours: level(w) == level(u)    *
 *             and larger BFS position.                                *
 *             Used as pivots in the within-level backward prune step. *
 *    ul[u]  — under-level neighbours: level(w) > level(u).           *
 *             Used as pivots in the bottom-up refinement step.        *
 *                                                                      *
 *  levels[d] — query vertices at BFS depth d, in BFS order.          *
 *                                                                      *
 *  Difference from SpanTree                                            *
 *  ─────────────────────────                                          *
 *  SpanTree::fn merges fn_cfl and ul into one list (sufficient for   *
 *  DPiso).  CFL needs them split so each phase uses the right set.   *
 * ------------------------------------------------------------------ */
struct CFLTree {
    std::vector<VertexID>              bfsOrder;
    std::vector<uint32_t>              bfsPos;
    std::vector<uint32_t>              level;
    std::vector<std::vector<VertexID>> bn;
    std::vector<std::vector<VertexID>> fn;
    std::vector<std::vector<VertexID>> ul;
    std::vector<std::vector<VertexID>> levels;   // levels[d] = vertices at depth d
};

inline CFLTree buildCFLTree(const QueryGraph& query, VertexID root)
{
    const uint32_t qn = query.getNumVertices();
    CFLTree t;
    t.bfsOrder.reserve(qn);
    t.bfsPos.assign(qn, UINT32_MAX);
    t.level.assign(qn, UINT32_MAX);
    t.bn.resize(qn);
    t.fn.resize(qn);
    t.ul.resize(qn);

    std::vector<bool>    visited(qn, false);
    std::queue<VertexID> bfsQ;
    bfsQ.push(root);
    visited[root] = true;
    t.level[root] = 0;

    while (!bfsQ.empty()) {
        VertexID u = bfsQ.front(); bfsQ.pop();
        t.bfsPos[u] = (uint32_t)t.bfsOrder.size();
        t.bfsOrder.push_back(u);

        uint32_t        uDeg;
        const VertexID* uNbrs = query.getNeighbors(u, uDeg);
        for (uint32_t i = 0; i < uDeg; ++i) {
            VertexID w = uNbrs[i];
            if (!visited[w]) {
                visited[w] = true;
                t.level[w] = t.level[u] + 1;
                bfsQ.push(w);
            }
        }
    }

    /* Build per-level buckets */
    uint32_t maxLevel = *std::max_element(t.level.begin(), t.level.end());
    t.levels.resize(maxLevel + 1);
    for (VertexID u : t.bfsOrder)
        t.levels[t.level[u]].push_back(u);

    /* Classify each query edge into bn / fn / ul */
    for (uint32_t u = 0; u < qn; ++u) {
        uint32_t        uDeg;
        const VertexID* uNbrs = query.getNeighbors(u, uDeg);
        for (uint32_t i = 0; i < uDeg; ++i) {
            VertexID w = uNbrs[i];
            if (t.level[w] < t.level[u]) {
                t.bn[u].push_back(w);                          // shallower level
            } else if (t.level[w] == t.level[u]) {
                if (t.bfsPos[w] < t.bfsPos[u]) t.bn[u].push_back(w);  // same level, earlier
                else                            t.fn[u].push_back(w);  // same level, later
            } else {
                t.ul[u].push_back(w);                          // deeper level
            }
        }
    }

    return t;
}

/* ------------------------------------------------------------------ *
 *  selectCFLStartVertex                                                *
 *                                                                      *
 *  Mirrors FiPE's selectCFLFilterStartVertex:                         *
 *    1. Prefer 2-core vertices (coreNum > 1) if the 2-core exists.   *
 *    2. Score each candidate: labelFreq(label(u)) / deg(u).          *
 *       Lower score = more discriminating label + higher degree.      *
 *    3. Keep top-3 by score.                                          *
 *    4. Among top-3, pick the one with min NLF_count / deg.          *
 * ------------------------------------------------------------------ */
inline VertexID selectCFLStartVertex(
        const QueryGraph&                            query,
        const Graph&                                 data,
        const std::vector<std::vector<VertexID>>&    labelIdx)
{
    const uint32_t qn = query.getNumVertices();

    using Pair = std::pair<VertexID, double>;
    auto cmp = [](const Pair& a, const Pair& b){ return a.second < b.second; };
    std::priority_queue<Pair, std::vector<Pair>, decltype(cmp)> pq(cmp);

    bool has2Core = (query.getCoreSize() > 0);

    for (uint32_t u = 0; u < qn; ++u) {
        if (has2Core && query.getCoreNum(u) <= 1) continue;
        uint32_t uDeg = query.getDegree(u);
        if (uDeg == 0) continue;
        LabelID  uLbl    = query.getLabelOf(u);
        uint32_t lblFreq = (uLbl < labelIdx.size())
                           ? (uint32_t)labelIdx[uLbl].size() : 0;
        pq.push({u, lblFreq / (double)uDeg});
    }

    if (pq.empty()) return 0;

    /* Trim to top-3 (smallest scores) */
    while (pq.size() > 3) pq.pop();

    VertexID bestVertex = 0;
    double   bestScore  = std::numeric_limits<double>::max();
    while (!pq.empty()) {
        VertexID u    = pq.top().first; pq.pop();
        LabelID  uLbl = query.getLabelOf(u);
        uint32_t uDeg = query.getDegree(u);
        uint32_t count = 0;
        if (uLbl < labelIdx.size())
            for (VertexID v : labelIdx[uLbl])
                if (data.getDegree(v) >= uDeg && passesNLF(data, v, query, u))
                    ++count;
        double s = count / (double)uDeg;
        if (s < bestScore) { bestScore = s; bestVertex = u; }
    }
    return bestVertex;
}

/* ------------------------------------------------------------------ *
 *  generateCandidatesByPivots                                          *
 *                                                                      *
 *  Generates candidates for u by scanning data-graph neighbours of   *
 *  existing candidates in each pivot vertex.                          *
 *                                                                      *
 *  A data vertex w is added to candidates[u] iff:                    *
 *    (a) w is adjacent to at least one candidate for every pivot.    *
 *    (b) label(w) == label(u) and deg(w) >= deg(u).                  *
 *    (c) w passes the NLF check for u.                                *
 *                                                                      *
 *  This is the "forward generation" step of CFL's top-down phase.    *
 *  It adds new candidates rather than removing existing ones.         *
 * ------------------------------------------------------------------ */
inline void generateCandidatesByPivots(VertexID                     u,
                                        const std::vector<VertexID>& pivots,
                                        const Graph&                 data,
                                        const QueryGraph&            query,
                                        CandidateSet&                candidates,
                                        std::vector<uint32_t>&       flag,
                                        std::vector<VertexID>&       touched)
{
    if (pivots.empty()) return;

    const LabelID  uLbl = query.getLabelOf(u);
    const uint32_t uDeg = query.getDegree(u);

    uint32_t pivotsSeen  = 0;
    uint32_t touchedSize = 0;

    for (VertexID pivot : pivots) {
        for (VertexID pv : candidates[pivot]) {
            uint32_t        pvDeg;
            const VertexID* pvNbrs = data.getNeighbors(pv, pvDeg);
            for (uint32_t k = 0; k < pvDeg; ++k) {
                VertexID w = pvNbrs[k];
                if (data.getLabelOf(w) != uLbl) continue;
                if (data.getDegree(w)  <  uDeg) continue;
                if (flag[w] == pivotsSeen) {
                    flag[w]++;
                    if (pivotsSeen == 0) {
                        if (touchedSize < touched.size())
                            touched[touchedSize] = w;
                        else
                            touched.push_back(w);
                        ++touchedSize;
                    }
                }
            }
        }
        ++pivotsSeen;
    }

    auto& cands = candidates[u];
    for (uint32_t i = 0; i < touchedSize; ++i) {
        VertexID w = touched[i];
        if (flag[w] == pivotsSeen && passesNLF(data, w, query, u))
            cands.push_back(w);
        flag[w] = 0;   // reset inline — avoids a second pass
    }
}

/* ================================================================== *
 *  filterByCFL                                                         *
 *                                                                      *
 *  CFL filter — the default strategy used in FiPE.                   *
 *                                                                      *
 *  Phase 0  Build BFS tree + NLF candidates for root.                *
 *                                                                      *
 *  Phase 1  Top-down (level 1 → maxLevel):                           *
 *    For each level d:                                                 *
 *      (a) Forward generation (BFS order):                            *
 *          For each u at level d, generateCandidatesByPivots(u,bn[u])*
 *          — uses parent-level candidates to enumerate data vertices  *
 *            that could match u.                                       *
 *      (b) Backward prune (reverse BFS order within level):           *
 *          For each u at level d, pruneCandidatesByPivots(u,fn[u])   *
 *          — removes candidates of u that have no support in          *
 *            same-level-forward neighbours already generated.         *
 *                                                                      *
 *  Phase 2  Bottom-up (level maxLevel-1 → 0):                        *
 *    For each u, pruneCandidatesByPivots(u, ul[u])                   *
 *    — propagates constraints from deeper levels back upward.         *
 * ================================================================== */
inline void filterByCFL(const QueryGraph& query,
                         const Graph&      data,
                         CandidateSet&     candidates)
{
    const uint32_t qn = query.getNumVertices();
    const uint32_t dn = data.getNumVertices();

    auto labelIdx = buildLabelIndex(data);

    VertexID start = selectCFLStartVertex(query, data, labelIdx);
    CFLTree  tree  = buildCFLTree(query, start);

    candidates.assign(qn, {});

    /* Phase 0: NLF for start vertex */
    {
        LabelID  uLbl = query.getLabelOf(start);
        uint32_t uDeg = query.getDegree(start);
        if (uLbl < labelIdx.size())
            for (VertexID v : labelIdx[uLbl])
                if (data.getDegree(v) >= uDeg && passesNLF(data, v, query, start))
                    candidates[start].push_back(v);
    }

    std::vector<uint32_t> flag(dn, 0);
    std::vector<VertexID> touched;
    touched.reserve(dn);

    const uint32_t numLevels = (uint32_t)tree.levels.size();

    /* Phase 1: top-down */
    for (uint32_t d = 1; d < numLevels; ++d) {
        const auto& lv = tree.levels[d];

        /* (a) Forward generation */
        for (VertexID u : lv)
            if (!tree.bn[u].empty())
                generateCandidatesByPivots(u, tree.bn[u], data, query,
                                            candidates, flag, touched);

        /* (b) Backward prune (reverse order within level) */
        for (int i = (int)lv.size() - 1; i >= 0; --i) {
            VertexID u = lv[i];
            if (!tree.fn[u].empty())
                pruneCandidatesByPivots(u, tree.fn[u], data,
                                        candidates, flag, touched);
        }
    }

    /* Phase 2: bottom-up */
    for (int d = (int)numLevels - 2; d >= 0; --d)
        for (VertexID u : tree.levels[d])
            if (!tree.ul[u].empty())
                pruneCandidatesByPivots(u, tree.ul[u], data,
                                        candidates, flag, touched);

    /* Sort every candidate list.                                        *
     *                                                                   *
     * generateCandidatesByPivots adds vertices in neighbor-scan order, *
     * which is not necessarily sorted by vertex ID.  The enumerator's  *
     * narrowCands uses twoPointerIntersect which requires sorted input. *
     * pruneCandidatesByPivots also relies on sorted order to correctly  *
     * match flag counts. Sort once here before the structural pass.     */
    for (uint32_t u = 0; u < qn; ++u)
        std::sort(candidates[u].begin(), candidates[u].end());

    /* Phase 3: structural refinement to fixed point.                   *
     *                                                                   *
     * CFL's single-pass top-down + bottom-up does not reach the fixed  *
     * point that GQL's repeated refinement achieves.  False positives  *
     * left behind by the CFL pass mislead the fail-first backjumping   *
     * (failPos) mechanism in the enumerator: exploring a false-positive *
     * branch produces incorrect failPos values that cause valid paths   *
     * to be skipped, leading to missed matches.                         *
     *                                                                   *
     * Applying one final GQL-style structural refinement removes the   *
     * remaining false positives, restoring correct enumeration.        *
     * In practice, this pass converges in 1–2 iterations because CFL  *
     * already eliminated the vast majority of invalid candidates.      */
    // structuralRefinementGQL(query, data, candidates);
}

/* ------------------------------------------------------------------ *
 *  selectDPisoStartVertex                                              *
 *                                                                      *
 *  Picks the query vertex u with the lowest score:                   *
 *    score(u) = |NLF_candidates(u)| / deg(u)                         *
 *  Vertices with deg <= 1 are skipped to avoid trivial chain starts.  *
 * ------------------------------------------------------------------ */
inline VertexID selectDPisoStartVertex(
        const QueryGraph&                            query,
        const Graph&                                 data,
        const std::vector<std::vector<VertexID>>&    labelIdx)
{
    const uint32_t qn = query.getNumVertices();
    double   bestScore  = std::numeric_limits<double>::max();
    VertexID bestVertex = 0;

    for (uint32_t u = 0; u < qn; ++u) {
        uint32_t deg = query.getDegree(u);
        if (deg <= 1) continue;

        LabelID uLbl = query.getLabelOf(u);
        if (uLbl >= labelIdx.size()) continue;

        uint32_t count = 0;
        for (VertexID v : labelIdx[uLbl])
            if (data.getDegree(v) >= deg && passesNLF(data, v, query, u))
                ++count;

        double score = count / (double)deg;
        if (score < bestScore) { bestScore = score; bestVertex = u; }
    }
    return bestVertex;
}

/* ================================================================== *
 *  Public filter functions                                             *
 * ================================================================== */

/* ------------------------------------------------------------------ *
 *  filterByLDF — label + degree only                                  *
 * ------------------------------------------------------------------ */
inline void filterByLDF(const QueryGraph& query,
                         const Graph&      data,
                         CandidateSet&     candidates)
{
    auto labelIdx = buildLabelIndex(data);
    localFilterLDF(query, data, labelIdx, candidates);
}

/* ------------------------------------------------------------------ *
 *  filterByNLF — LDF + neighbour label frequency, no structural pass *
 * ------------------------------------------------------------------ */
inline void filterByNLF(const QueryGraph& query,
                         const Graph&      data,
                         CandidateSet&     candidates)
{
    auto labelIdx = buildLabelIndex(data);
    localFilterNLF(query, data, labelIdx, candidates);
}

/* ------------------------------------------------------------------ *
 *  filterByGQL — NLF + fixed-point structural refinement             *
 * ------------------------------------------------------------------ */
inline void filterByGQL(const QueryGraph& query,
                         const Graph&      data,
                         CandidateSet&     candidates)
{
    auto labelIdx = buildLabelIndex(data);
    localFilterNLF(query, data, labelIdx, candidates);
    structuralRefinementGQL(query, data, candidates);
}

/* ------------------------------------------------------------------ *
 *  filterByDPiso — NLF + BFS tree + 3-round bidirectional pruning    *
 *                                                                      *
 *  Round 0 (forward):  BFS order i=1..qn-1, prune u via bn[u]       *
 *  Round 1 (backward): reverse BFS order i=qn-2..0, prune u via fn[u]*
 *  Round 2 (forward):  same as round 0                               *
 *                                                                      *
 *  The flag array and touched buffer are shared across all pruning    *
 *  calls to avoid repeated allocation.                                *
 * ------------------------------------------------------------------ */
inline void filterByDPiso(const QueryGraph& query,
                            const Graph&      data,
                            CandidateSet&     candidates)
{
    const uint32_t qn = query.getNumVertices();
    const uint32_t dn = data.getNumVertices();

    auto labelIdx = buildLabelIndex(data);

    /* Step 1: NLF local filter */
    localFilterNLF(query, data, labelIdx, candidates);

    /* Step 2: select start vertex */
    VertexID start = selectDPisoStartVertex(query, data, labelIdx);

    /* Step 3: BFS spanning tree */
    SpanTree tree = buildBFSTree(query, start);

    /* Step 4: 3 rounds of bidirectional structural pruning */
    std::vector<uint32_t> flag(dn, 0);
    std::vector<VertexID> touched;
    touched.reserve(dn);

    for (int round = 0; round < 3; ++round) {
        if (round % 2 == 0) {
            /* Forward pass: increasing BFS order, use bn[u] as pivots */
            for (uint32_t i = 1; i < qn; ++i) {
                VertexID u = tree.bfsOrder[i];
                if (!tree.bn[u].empty())
                    pruneCandidatesByPivots(u, tree.bn[u], data,
                                            candidates, flag, touched);
            }
        } else {
            /* Backward pass: decreasing BFS order, use fn[u] as pivots */
            for (int i = (int)qn - 2; i >= 0; --i) {
                VertexID u = tree.bfsOrder[i];
                if (!tree.fn[u].empty())
                    pruneCandidatesByPivots(u, tree.fn[u], data,
                                            candidates, flag, touched);
            }
        }
    }
}

/* ================================================================== *
 *  computeFilter — unified dispatcher                                  *
 * ================================================================== */
inline void computeFilter(FilterStrategy    strategy,
                            const QueryGraph& query,
                            const Graph&      data,
                            CandidateSet&     candidates)
{
    switch (strategy) {
        case FilterStrategy::LDF:   filterByLDF  (query, data, candidates); break;
        case FilterStrategy::NLF:   filterByNLF  (query, data, candidates); break;
        case FilterStrategy::GQL:   filterByGQL  (query, data, candidates); break;
        case FilterStrategy::DPISO: filterByDPiso(query, data, candidates); break;
        case FilterStrategy::CFL:   filterByCFL  (query, data, candidates); break;
    }
}
