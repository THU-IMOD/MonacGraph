#pragma once
#include "graph.h"
#include <vector>
#include <algorithm>

/**
 * Order = the sequence in which query vertices are matched during enumeration.
 * order[0] is matched first, order[n-1] last.
 */
using Order = std::vector<VertexID>;

/* ------------------------------------------------------------------ *
 *  intersect                                                           *
 *                                                                      *
 *  Computes the intersection of two sorted arrays a[] and b[],        *
 *  writes results into out[], returns the count.                      *
 *  Both input arrays must be sorted in ascending order.               *
 *  Uses a standard two-pointer merge — O(|a| + |b|).                 *
 * ------------------------------------------------------------------ */
inline uint32_t intersect(const VertexID* a, uint32_t sizeA,
                           const VertexID* b, uint32_t sizeB,
                           VertexID* out)
{
    uint32_t i = 0, j = 0, cnt = 0;
    while (i < sizeA && j < sizeB) {
        if      (a[i] < b[j]) ++i;
        else if (a[i] > b[j]) ++j;
        else { out[cnt++] = a[i]; ++i; ++j; }
    }
    return cnt;
}

/* ================================================================== *
 *  orderByRI                                                           *
 *                                                                      *
 *  Fills 'order' with a RI-DS vertex ordering of the query graph.    *
 *                                                                      *
 *  Three-priority greedy selection (RI-DS):                           *
 *                                                                      *
 *  Priority 1 — backward neighbors (bn)                               *
 *    Pick the unvisited vertex u that shares the most edges with      *
 *    already-ordered vertices.  Maximises constraints applied early.  *
 *                                                                      *
 *  Priority 2 — forward neighborhood coverage (tie-break)            *
 *    Among equal-bn vertices, pick u whose unvisited neighbours are   *
 *    most "reachable" from already-ordered vertices (share a common   *
 *    neighbour with some ordered vertex).  These future vertices will *
 *    have smaller candidate sets when their turn comes.               *
 *                                                                      *
 *  Priority 3 — unconnected forward neighbours (tie-break)           *
 *    Among still-equal vertices, pick u with the most unvisited       *
 *    neighbours that have NO edge to any ordered vertex yet.          *
 *    Extends coverage into unexplored regions of the query.           *
 * ================================================================== */
inline void orderByRI(const Graph& query, Order& order)
{
    const uint32_t n = query.getNumVertices();
    order.assign(n, 0);
    std::vector<bool> visited(n, false);

    /* ── Step 1: start from the highest-degree vertex ───────────── */
    for (uint32_t v = 1; v < n; ++v)
        if (query.getDegree(v) > query.getDegree(order[0]))
            order[0] = v;
    visited[order[0]] = true;

    /* scratch buffers reused across iterations */
    std::vector<VertexID> candidates;    // current tie set
    std::vector<VertexID> nextCandidates;
    std::vector<VertexID> unvisitedNbrs; // unvisited neighbours of a vertex
    std::vector<VertexID> tmpBuf;        // intersection scratch

    /* ── Step 2: greedily place the remaining n-1 vertices ──────── */
    for (uint32_t step = 1; step < n; ++step) {

        /* --- Priority 1: maximise backward-neighbour count --- */
        uint32_t bestBN = 0;
        candidates.clear();

        for (uint32_t u = 0; u < n; ++u) {
            if (visited[u]) continue;

            // Count how many already-ordered vertices are adjacent to u
            uint32_t bn = 0;
            for (uint32_t k = 0; k < step; ++k)
                if (query.hasEdge(u, order[k]))
                    ++bn;

            if (bn > bestBN) {
                bestBN = bn;
                candidates.clear();
                candidates.push_back(u);
            } else if (bn == bestBN) {
                candidates.push_back(u);
            }
        }

        /* --- Priority 2: maximise forward-coverage count --- */
        if (candidates.size() > 1) {
            uint32_t bestCov = 0;
            nextCandidates.clear();
            tmpBuf.resize(n);   // worst-case intersection output

            for (VertexID u : candidates) {
                /* collect unvisited neighbours of u */
                unvisitedNbrs.clear();
                uint32_t uDeg;
                const VertexID* uNbrs = query.getNeighbors(u, uDeg);
                for (uint32_t i = 0; i < uDeg; ++i)
                    if (!visited[uNbrs[i]])
                        unvisitedNbrs.push_back(uNbrs[i]);

                /* count how many ordered vertices share a neighbour
                   with at least one element of unvisitedNbrs */
                uint32_t cov = 0;
                for (uint32_t k = 0; k < step; ++k) {
                    uint32_t ordDeg;
                    const VertexID* ordNbrs =
                        query.getNeighbors(order[k], ordDeg);

                    uint32_t common = intersect(
                        ordNbrs,              ordDeg,
                        unvisitedNbrs.data(), (uint32_t)unvisitedNbrs.size(),
                        tmpBuf.data());
                    if (common > 0) ++cov;
                }

                if (cov > bestCov) {
                    bestCov = cov;
                    nextCandidates.clear();
                    nextCandidates.push_back(u);
                } else if (cov == bestCov) {
                    nextCandidates.push_back(u);
                }
            }
            candidates.swap(nextCandidates);
        }

        /* --- Priority 3: maximise purely-new forward neighbours --- */
        if (candidates.size() > 1) {
            uint32_t bestNew = 0;
            nextCandidates.clear();

            for (VertexID u : candidates) {
                /* collect unvisited neighbours of u */
                unvisitedNbrs.clear();
                uint32_t uDeg;
                const VertexID* uNbrs = query.getNeighbors(u, uDeg);
                for (uint32_t i = 0; i < uDeg; ++i)
                    if (!visited[uNbrs[i]])
                        unvisitedNbrs.push_back(uNbrs[i]);

                /* count neighbours not adjacent to ANY ordered vertex
                   (completely "new" territory, not yet constrained) */
                uint32_t newCount = 0;
                for (VertexID w : unvisitedNbrs) {
                    bool touched = false;
                    for (uint32_t k = 0; k < step; ++k) {
                        if (query.hasEdge(w, order[k])) {
                            touched = true;
                            break;
                        }
                    }
                    if (!touched) ++newCount;
                }

                if (newCount > bestNew) {
                    bestNew = newCount;
                    nextCandidates.clear();
                    nextCandidates.push_back(u);
                } else if (newCount == bestNew) {
                    nextCandidates.push_back(u);
                }
            }
            candidates.swap(nextCandidates);
        }

        /* place the winner (first element if still tied) */
        order[step] = candidates[0];
        visited[order[step]] = true;
    }
}
