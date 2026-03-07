#pragma once
#include "graph.h"
#include "filter.h"
#include "order.h"
#include "decision_tree.h"

/**
 * Enumerate subgraph isomorphisms via backtracking.
 *
 * Optimizations
 * =============
 *
 * 1. LICM multi-level intersection cache
 *    ─────────────────────────────────────────────────────────────────
 *    For depth d with backward neighbors [p0, p1, p2, ...] (p0<p1<p2):
 *
 *      licmCache[d][0] = C(u_d) ∩ N(v_p0)            ← set when p0 is mapped
 *      licmCache[d][1] = licmCache[d][0] ∩ N(v_p1)   ← set when p1 is mapped
 *      licmCache[d][2] = licmCache[d][1] ∩ N(v_p2)   ← set when p2 is mapped
 *
 *    When entering depth d: localCands = licmCache[d][numBN-1].
 *    Zero intersection work at depth d — everything is pre-computed.
 *
 *    This is the generalization of CSE:
 *      CSE  = LICM level 0 only  (cache first BN, recompute rest at depth d)
 *      LICM = all levels cached  (nothing left to compute at depth d)
 *
 *    bnTargets[k] = list of (depth d, bn_index i) where
 *    backwardNbrs[d][i] == k. When position k is mapped to vertex v,
 *    updateLICM(k, v) recomputes all affected cache entries.
 *
 * 2. Forward neighbor pruning (unified with LICM)
 *    ─────────────────────────────────────────────────────────────────
 *    updateLICM(k, v) returns false as soon as any licmCacheSize[d][i]
 *    becomes 0. This means depth d is provably unsatisfiable under the
 *    current partial mapping → prune v before recursing.
 *    No separate fwdCSE / fwdCheck structures needed.
 *
 * 3. Failing Set pruning (FP:GuP)
 *    ─────────────────────────────────────────────────────────────────
 *    failPos[d] = shallowest ancestor position blocking depth d.
 *    childFail < depth → skip remaining candidates, propagate up.
 */

/* ------------------------------------------------------------------ */

using Quantifiers = std::vector<Quantifier>;

/** Two-pointer sorted-array intersection; same as intersect() in order.h. */
inline uint32_t twoPointerIntersect(const VertexID* a, uint32_t na,
                                     const VertexID* b, uint32_t nb,
                                     VertexID* out)
{
    uint32_t i = 0, j = 0, cnt = 0;
    while (i < na && j < nb) {
        if      (a[i] < b[j]) ++i;
        else if (a[i] > b[j]) ++j;
        else { out[cnt++] = a[i]; ++i; ++j; }
    }
    return cnt;
}

/** Identifies the source position and BN slot of a backward-neighbor edge. */
struct BNTarget {
    uint32_t depth;
    uint32_t bnIdx;   /* backwardNbrs[depth][bnIdx] == k */
};

/**
 * EnumContext — all mutable state shared across the recursive backtrack().
 *
 * Keeping everything in one struct avoids passing many arguments and
 * makes the undo / rollback logic straightforward.
 */
struct EnumContext {
    const Graph&        data;
    const Order&        order;
    const CandidateSet& candidates;
    const Quantifiers&  quantifiers;

    /** backwardNbrs[d] = list of position indices in order[] that are
     *  both before d and adjacent to order[d] in the query graph.    */
    std::vector<std::vector<uint32_t>> backwardNbrs;

    std::vector<VertexID> mapping;    // mapping[u] = data vertex matched to query vertex u
    std::vector<bool>     inMapping;  // inMapping[v] = true iff v is already used

    /** failPos[d]: shallowest ancestor position p such that the failure at
     *  depth d was caused (directly or transitively) by the choice at p.
     *  Used by Failing Set pruning to skip siblings that cannot fix it. */
    std::vector<uint32_t> failPos;

    /* LICM multi-level cache
     * licmCache[d][i]     : sorted vertex list after incorporating BNs 0..i
     * licmCacheSize[d][i] : valid element count                          */
    std::vector<std::vector<std::vector<VertexID>>> licmCache;
    std::vector<std::vector<uint32_t>>              licmCacheSize;

    /** bnTargets[k]: all (depth, bnIdx) pairs where backwardNbrs[depth][bnIdx]==k.
     *  Used by updateLICM() to find which cache entries to refresh when
     *  position k is mapped.                                             */
    std::vector<std::vector<BNTarget>> bnTargets;

    DecisionTree decisionTree;
    uint64_t matchCount{0};
    uint64_t matchLimit{UINT64_MAX};

    /** Current partial match: matched[i] = data vertex placed at position i. */
    Order matched;

    std::function<bool(const int*)> phi;
    std::function<void(const std::vector<VertexID>&)> onMatch;

    EnumContext(const Graph&        data_,
                const QueryGraph&   query,
                const Order&        order_,
                const CandidateSet& cands,
                const Quantifiers&  quants,
                uint64_t            limit = UINT64_MAX)
        : data(data_), order(order_), candidates(cands), quantifiers(quants),
          decisionTree(query.getNumVertices(), quantifiers.size(), quants),
          matchLimit(limit)
    {
        const uint32_t qn = query.getNumVertices();
        const uint32_t dn = data_.getNumVertices();
        mapping.assign(qn, UINT32_MAX);
        inMapping.assign(dn, false);
        failPos.assign(qn + 1, UINT32_MAX);

        /* max candidate set size (upper bound for all cache buffers) */
        uint32_t maxCands = 0;
        for (const auto& c : cands)
            maxCands = std::max(maxCands, (uint32_t)c.size());

        /* backward neighbor lists */
        backwardNbrs.resize(qn);
        for (uint32_t i = 1; i < qn; ++i) {
            VertexID u = order[i];
            for (uint32_t j = 0; j < i; ++j)
                if (query.hasEdge(u, order[j]))
                    backwardNbrs[i].push_back(j);
        }

        /* LICM cache: one buffer per (depth, bn_level) pair */
        licmCache.resize(qn);
        licmCacheSize.resize(qn);
        for (uint32_t d = 0; d < qn; ++d) {
            uint32_t numBN = (uint32_t)backwardNbrs[d].size();
            licmCache[d].resize(numBN);
            licmCacheSize[d].assign(numBN, 0);
            for (uint32_t i = 0; i < numBN; ++i)
                licmCache[d][i].resize(maxCands);
        }

        /* bnTargets index: maps each position k to the list of
         * (depth, bn_index) entries that need updating when k is mapped */
        bnTargets.resize(qn);
        for (uint32_t d = 1; d < qn; ++d) {
            for (uint32_t i = 0; i < (uint32_t)backwardNbrs[d].size(); ++i) {
                uint32_t k = backwardNbrs[d][i];
                bnTargets[k].push_back({d, i});
            }
        }

        matched.resize(qn);
    }

    /**
     * updateLICM — called immediately after mapping position k to vertex v.
     *
     * For every (depth d, bn_index i) in bnTargets[k], recomputes:
     *   licmCache[d][i] = (i==0 ? candidates[order[d]] : licmCache[d][i-1])
     *                     ∩ N(v)
     *
     * If any resulting cache becomes empty, depth d is provably
     * unsatisfiable → returns false so the caller can prune v immediately.
     */
    bool updateLICM(uint32_t k, VertexID v) {
        uint32_t        nbrDeg;
        const VertexID* nbrPtr = data.getNeighbors(v, nbrDeg);

        for (const auto& [d, i] : bnTargets[k]) {
            /* source = previous cache level, or static candidates for level 0 */
            const VertexID* src;
            uint32_t        srcSize;
            if (i == 0) {
                src     = candidates[order[d]].data();
                srcSize = (uint32_t)candidates[order[d]].size();
            } else {
                src     = licmCache[d][i - 1].data();
                srcSize = licmCacheSize[d][i - 1];
            }

            licmCacheSize[d][i] = twoPointerIntersect(
                src, srcSize, nbrPtr, nbrDeg, licmCache[d][i].data());

            /* Forward prune: depth d is already unsatisfiable */
            if (licmCacheSize[d][i] == 0) return false;
        }
        return true;
    }

    /**
     * updateFail — merges a newly discovered failing position into failPos[d].
     *
     * Records the shallowest ancestor position 'pos' known to be
     * responsible for a failure at depth d.  Only updates if pos is
     * shallower than the currently recorded value (min semantics).
     */
    void updateFail(uint32_t d, uint32_t pos) {
        if (failPos[d] == UINT32_MAX || pos < failPos[d])
            failPos[d] = pos;
    }
};

/* ------------------------------------------------------------------ *
 *  phi — second-order predicate                                        *
 *                                                                      *
 *  Defines the second-order constraint evaluated at each leaf of the  *
 *  decision tree.  It receives a coordinate tuple coords[0..k-1]      *
 *  where each coords[j] is a matched data vertex at some tree level.  *
 *                                                                      *
 *  Current semantics (example):                                        *
 *    Returns true iff the two matched vertices are "far apart"         *
 *    (absolute difference > 100) OR they are equal.                    *
 *    This enforces a constraint of the form:                           *
 *      |id(v0) - id(v1)| > 100  ∨  id(v0) == id(v1)                  *
 *                                                                      *
 *  Replace this function with the actual second-order predicate        *
 *  required by the query.  The signature must remain                  *
 *    bool phi(const int* coords)                                       *
 *  so it is compatible with DecisionTree::update_for_depth().         *
 * ------------------------------------------------------------------ */
// bool phi(const int* coords)
// {
//     return coords[0] - coords[1] > 100
//         || coords[1] - coords[0] > 100
//         || coords[0] == coords[1];
// }

/** Prints the current complete match (one line per result). */
inline void printAnswer(const EnumContext& ctx)
{
    for (size_t i = 0; i < ctx.order.size(); ++i)
        std::cout << ctx.matched[i] << " ";
    std::cout << "\n";
}

/* ================================================================== *
 *  backtrack                                                           *
 *                                                                      *
 *  Core recursive search procedure.                                   *
 *                                                                      *
 *  At each call, 'depth' is the index into order[] of the next query  *
 *  vertex to be matched.  The function tries every viable data         *
 *  vertex for order[depth] and recurses.                              *
 *                                                                      *
 *  Pruning applied at each candidate v:                               *
 *    (a) Injectivity     — skip v if already in mapping               *
 *    (b) LICM / forward  — updateLICM() returns false if any future   *
 *                          depth becomes empty after placing v         *
 *    (c) Decision tree   — decisionTree.update_for_depth() returns     *
 *                          NS_FALSE if the second-order formula is     *
 *                          already violated under the current match    *
 *    (d) Failing Set     — if childFail < depth, the failure cannot   *
 *                          be fixed by trying other candidates at      *
 *                          this depth → return early                  *
 * ================================================================== */
inline void backtrack(EnumContext& ctx, uint32_t depth)
{
    if (ctx.matchCount >= ctx.matchLimit) return;

    const uint32_t qn = (uint32_t)ctx.order.size();

    /* Base case: all query vertices matched */
    if (depth == qn) {
        // Accept only if the second-order formula evaluated to TRUE
        if (node::state(ctx.decisionTree.nodes[0]) == NS_TRUE) {
            ++ctx.matchCount;
            if (ctx.onMatch) ctx.onMatch(ctx.matched);
            // printAnswer(ctx);  // uncomment to print each match
        }
        return;
    }

    const VertexID  u     = ctx.order[depth];
    const auto&     bnPos = ctx.backwardNbrs[depth];
    const uint32_t  numBN = (uint32_t)bnPos.size();

    /* ── Get local candidates ───────────────────────────────────── *
     * With LICM, all BN intersections were precomputed when each BN *
     * position was mapped. Just read the final cache level.         *
     * No intersection loop needed here at all.                      */
    const VertexID* localPtr;
    uint32_t        localSize;

    if (numBN == 0) {
        /* depth 0 or no BNs: use static candidate set directly */
        localPtr  = ctx.candidates[u].data();
        localSize = (uint32_t)ctx.candidates[u].size();
    } else {
        /* All BNs pre-incorporated in the last LICM cache level */
        localPtr  = ctx.licmCache[depth][numBN - 1].data();
        localSize = ctx.licmCacheSize[depth][numBN - 1];
        if (localSize == 0) {
            /* Should have been caught by forward pruning, but be safe */
            ctx.updateFail(depth, bnPos[0]);
            if (ctx.onMatch) ctx.onMatch(ctx.matched);
            return;
        }
    }

    /* ── Iterate local candidates ───────────────────────────────── */
    for (uint32_t i = 0; i < localSize; ++i) {
        if (ctx.matchCount >= ctx.matchLimit) return;

        VertexID v = localPtr[i];

        /* (a) Injectivity: reject vertices already in the mapping */
        if (ctx.inMapping[v]) continue;

        /* (b) LICM + forward pruning:
         *     Recompute cache entries for all depths that have 'depth'
         *     as one of their BNs.  Returns false if any depth becomes
         *     unsatisfiable → skip v without recursing.              */
        if (!ctx.updateLICM(depth, v)) continue;

        ctx.matched[depth] = v;

        /* (c) Decision tree check:
         *     Insert newly evaluable leaf paths contributed by placing
         *     v at this depth.  NS_FALSE means the formula is already
         *     violated — prune and undo.                             */
        if (ctx.decisionTree.update_for_depth(depth, ctx.matched, ctx.phi) == NS_FALSE) {
            ctx.decisionTree.undo_to(depth);
            continue;
        }

        ctx.failPos[depth + 1] = UINT32_MAX;
        ctx.mapping[u]   = v;
        ctx.inMapping[v] = true;

        backtrack(ctx, depth + 1);

        /* Undo this choice */
        ctx.mapping[u]   = UINT32_MAX;
        ctx.inMapping[v] = false;
        ctx.decisionTree.undo_to(depth);

        if (ctx.matchCount >= ctx.matchLimit) return;

        /* (d) Failing Set propagation:
         *     If the subtree rooted at depth+1 failed because of a
         *     choice at an ancestor shallower than this depth, no
         *     sibling candidate can fix it → cut this branch early.  */
        uint32_t childFail = ctx.failPos[depth + 1];
        if (childFail != UINT32_MAX) {
            ctx.updateFail(depth, childFail);
            if (childFail < depth)
                return;
        }
    }
}

/**
 * enumerate — top-level entry point.
 *
 * Builds an EnumContext, runs the backtracking search, and returns the
 * total number of valid subgraph isomorphisms found (up to 'limit').
 */
inline uint64_t enumerate(const QueryGraph&   query,
                           const Graph&        data,
                           const CandidateSet& candidates,
                           const Order&        order,
                           const Quantifiers&  quantifiers,
                           uint64_t            limit = UINT64_MAX)
{
    EnumContext ctx(data, query, order, candidates, quantifiers, limit);
    backtrack(ctx, 0);
    return ctx.matchCount;
}
