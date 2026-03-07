#pragma once
#include "graph.h"
#include <vector>
#include <unordered_map>

/**
 * CandidateSet[u] = all data vertices that may match query vertex u.
 * Built by filterByGQL() and consumed by the enumeration phase.
 */
using CandidateSet = std::vector<std::vector<VertexID>>;

/* ================================================================== *
 *  filterByGQL                                                         *
 *                                                                      *
 *  Two-phase candidate generation following GraphQL's strategy.       *
 *                                                                      *
 *  Phase 1 — local filter (per-vertex)                                *
 *  ------------------------------------                                *
 *  A data vertex v passes for query vertex u when all three hold:     *
 *    (a) deg(v)   >= deg(u)          degree lower-bound               *
 *    (b) label(v) == label(u)        label equality                   *
 *    (c) NLF(v)  ⊇ NLF(u)           Neighbourhood Label Frequency:   *
 *        for every label l, the count of v's l-labelled neighbours    *
 *        must be >= the count of u's l-labelled neighbours.           *
 *                                                                      *
 *  Phase 2 — structural refinement (iterative)                        *
 *  -----------------------------------------------                    *
 *  Repeatedly scan every (u, v) pair in the current candidate sets.  *
 *  For each neighbour u' of u in the query, check that v has at       *
 *  least one neighbour v' that is still a candidate for u'.           *
 *  If not, v cannot be part of any valid match → remove it.           *
 *  Repeat until no candidate is removed (fixed point).                *
 * ================================================================== */
inline void filterByGQL(const QueryGraph& query,
                         const Graph&      data,
                         CandidateSet&     candidates)
{
    const uint32_t qn = query.getNumVertices();
    const uint32_t dn = data.getNumVertices();
    candidates.assign(qn, {});

    /* ---------------------------------------------------------- *
     *  Phase 1: local filter                                      *
     * ---------------------------------------------------------- */
    for (uint32_t u = 0; u < qn; ++u) {
        const uint32_t uDeg = query.getDegree(u);
        const LabelID  uLbl = query.getLabelOf(u);
        const auto*    uNLF = query.getNbrLabelFreq(u);

        for (uint32_t v = 0; v < dn; ++v) {
            /* (a) degree lower-bound */
            if (data.getDegree(v) < uDeg)   continue;

            /* (b) label equality */
            if (data.getLabelOf(v) != uLbl) continue;

            /* (c) NLF containment:
             *     for every (label, count) in u's NLF,
             *     v must have at least as many neighbours with that label */
            const auto* vNLF = data.getNbrLabelFreq(v);
            bool nlfOk = true;
            for (const auto& [lbl, uCnt] : *uNLF) {
                auto it = vNLF->find(lbl);
                if (it == vNLF->end() || it->second < uCnt) {
                    nlfOk = false;
                    break;
                }
            }
            if (!nlfOk) continue;

            candidates[u].push_back(v);
        }
    }

    /* ---------------------------------------------------------- *
     *  Phase 2: structural refinement — iterate to fixed point   *
     * ---------------------------------------------------------- */

    // isCandidate[u][v] = true means v is currently a candidate for u.
    // We use a flat boolean array indexed by u * dn + v.
    // For large data graphs this can be a bitmap; here a bool array suffices.
    std::vector<std::vector<bool>> isCandidate(qn,
                                               std::vector<bool>(dn, false));
    for (uint32_t u = 0; u < qn; ++u)
        for (VertexID v : candidates[u])
            isCandidate[u][v] = true;

    bool changed = true;
    while (changed) {
        changed = false;

        for (uint32_t u = 0; u < qn; ++u) {
            /* collect query neighbours of u */
            uint32_t uDeg;
            const VertexID* uNbrs = query.getNeighbors(u, uDeg);

            /* scan every current candidate v for u */
            auto& cands = candidates[u];
            uint32_t writePos = 0;

            for (uint32_t ci = 0; ci < cands.size(); ++ci) {
                VertexID v = cands[ci];
                bool valid = true;

                /* for every query neighbour u', v must have at least
                   one data neighbour v' that is still a candidate for u' */
                for (uint32_t ni = 0; ni < uDeg && valid; ++ni) {
                    VertexID uPrime = uNbrs[ni];

                    uint32_t vDeg;
                    const VertexID* vNbrs = data.getNeighbors(v, vDeg);

                    bool found = false;
                    for (uint32_t vi = 0; vi < vDeg && !found; ++vi)
                        if (isCandidate[uPrime][vNbrs[vi]])
                            found = true;

                    if (!found) valid = false;
                }

                if (valid) {
                    // v survives: keep it in place (compact in-place removal)
                    cands[writePos++] = v;
                } else {
                    // v is invalidated: mark it removed and flag another pass
                    isCandidate[u][v] = false;
                    changed = true;
                }
            }
            cands.resize(writePos);
        }
    }
}
