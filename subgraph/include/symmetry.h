#pragma once

#include "graph.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include <set>
#include <unordered_map>

/**
 * SymBreak — query-graph automorphism analysis and symmetry breaking.
 *
 * Header-only: all implementations are inline, no .cpp needed.
 *
 * Pipeline
 * --------
 *   computeOrbits()
 *     └─ initPartition()       — one cell per distinct vertex label
 *     └─ refinePartitions()    — Weisfeiler-Lehman color refinement
 *     └─ processPartitions()   — individualization + orbit tracking
 *          └─ individualizeCell()  — fix one vertex pair, re-refine
 *          └─ readPermutation()    — read off leaf permutation
 *          └─ mergeOrbits()        — union-find over orbit classes
 *
 *   makeConstraints()         — orbit → ordered pairs  (u < v in orbit)
 *   makeFullConstraints()     — pair list → per-vertex predecessor/successor sets
 *   makeOrderedConstraints()  — filter to constraints active under a given order
 *   computeAutGroupSize()     — |Aut(query)| = product of orbit sizes
 *
 * Constraint semantics
 * --------------------
 *   fullConstraints[u].first  = { w : mapping[w] < mapping[u] must hold }
 *   fullConstraints[u].second = { w : mapping[u] < mapping[w] must hold }
 */

using Partition = std::vector<std::set<VertexID>>;

class SymBreak {

public:
    /* ── main entry point ─────────────────────────────────────────── */

    static std::unordered_map<VertexID, std::set<VertexID>>
    computeOrbits(const QueryGraph& query,
                  std::vector<std::vector<VertexID>>& permutations);

    /* ── constraint builders ──────────────────────────────────────── */

    static void makeConstraints(
        std::unordered_map<VertexID, std::set<VertexID>>& cosets,
        std::vector<std::pair<VertexID, VertexID>>&        constraints);

    static void makeFullConstraints(
        std::vector<std::pair<VertexID, VertexID>>&                                      constraints,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>, std::set<VertexID>>>& fullConstraints);

    static void makeOrderedConstraints(
        const uint32_t* order,
        uint32_t        numVertices,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>, std::set<VertexID>>>& fullConstraints,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>, std::set<VertexID>>>& orderedConstraints);

    static uint64_t computeAutGroupSize(
        const std::unordered_map<VertexID, std::set<VertexID>>& cosets);

    /* ── debug printers ───────────────────────────────────────────── */

    static void printPermutations(std::vector<std::vector<VertexID>>& permutations);
    static void printCosets(std::unordered_map<VertexID, std::set<VertexID>>& cosets);
    static void printConstraints(std::vector<std::pair<VertexID, VertexID>>& constraints);
    static void printConstraints(std::unordered_map<VertexID, std::pair<std::set<VertexID>, std::set<VertexID>>>& constraints);
    static void printPartition(Partition& partition);

private:
    static void initPartition(const QueryGraph& query, Partition& partition);
    static void refinePartitions(const QueryGraph& query, Partition& partition);

    static std::unordered_map<VertexID, std::set<VertexID>>
    processPartitions(const QueryGraph& query,
                      Partition& top, Partition& bottom, Partition& orbits,
                      std::unordered_map<VertexID, std::set<VertexID>> coset,
                      std::vector<std::vector<VertexID>>& permutations);

    static void partitionToColor(Partition& partition, std::vector<int>& colors);

    static void updateEdgeColors(
        const QueryGraph& query, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors);

    static bool isAllEqual(
        Partition& partition, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors);

    static bool isCellEqual(
        int cellIdx, Partition& partition, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors);

    static void splitCell(
        std::set<VertexID>& cell, Partition& out, int partitionSize,
        const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors);

    static bool equalLength(const Partition& top, const Partition& bottom);
    static bool isDiscrete(const Partition& partition);

    static std::vector<VertexID>
    readPermutation(const Partition& top, const Partition& bottom);

    static void mergeOrbits(Partition& orbits, const std::vector<VertexID>& perm);
    static bool sameOrbit(const Partition& orbits, VertexID u, VertexID v);

    static std::pair<Partition, Partition>
    individualizeCell(const QueryGraph& query,
                      Partition& top, Partition& bottom,
                      int cellIdx, VertexID tNode, VertexID bNode);
};

/* ================================================================== *
 *  Implementations (inline — header-only, no .cpp needed)            *
 * ================================================================== */

inline std::unordered_map<VertexID, std::set<VertexID>>
SymBreak::computeOrbits(
        const QueryGraph& query,
        std::vector<std::vector<VertexID>>& permutations)
{
    Partition partition;
    initPartition(query, partition);
    refinePartitions(query, partition);

    Partition orbits;
    std::unordered_map<VertexID, std::set<VertexID>> coset;
    return processPartitions(query, partition, partition, orbits, coset, permutations);
}

inline void
SymBreak::makeConstraints(
        std::unordered_map<VertexID, std::set<VertexID>>& cosets,
        std::vector<std::pair<VertexID, VertexID>>&        constraints)
{
    for (auto& [u, orbit] : cosets)
        for (VertexID v : orbit)
            if (u != v) constraints.emplace_back(u, v);
}

inline void
SymBreak::makeFullConstraints(
        std::vector<std::pair<VertexID, VertexID>>& constraints,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>,
                                               std::set<VertexID>>>& fullConstraints)
{
    for (auto& [u, v] : constraints) {
        fullConstraints.try_emplace(u);
        fullConstraints.try_emplace(v);
        fullConstraints[u].second.insert(v);
        fullConstraints[v].first.insert(u);
    }
}

inline void
SymBreak::makeOrderedConstraints(
        const uint32_t* order, uint32_t numVertices,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>,
                                               std::set<VertexID>>>& fullConstraints,
        std::unordered_map<VertexID, std::pair<std::set<VertexID>,
                                               std::set<VertexID>>>& orderedConstraints)
{
    std::vector<bool> visited(numVertices, false);
    for (uint32_t i = 0; i < numVertices; ++i) {
        VertexID u = order[i];
        visited[u] = true;
        auto it = fullConstraints.find(u);
        if (it == fullConstraints.end()) continue;
        auto& [prior, inferior] = it->second;
        auto& [op, oi]          = orderedConstraints[u];
        for (VertexID w : prior)    if (visited[w]) op.insert(w);
        for (VertexID w : inferior) if (visited[w]) oi.insert(w);
    }
}

inline uint64_t
SymBreak::computeAutGroupSize(
        const std::unordered_map<VertexID, std::set<VertexID>>& cosets)
{
    uint64_t size = 1;
    for (auto& [u, orbit] : cosets)
        size *= static_cast<uint64_t>(orbit.size());
    return size;
}

inline void
SymBreak::initPartition(const QueryGraph& query, Partition& partition)
{
    std::unordered_map<LabelID, int> labelToCell;
    for (VertexID v = 0; v < query.getNumVertices(); ++v) {
        LabelID label = query.getLabelOf(v);
        auto it = labelToCell.find(label);
        if (it == labelToCell.end()) {
            labelToCell[label] = static_cast<int>(partition.size());
            partition.push_back({});
        }
        partition[labelToCell[label]].insert(v);
    }
}

inline void
SymBreak::refinePartitions(const QueryGraph& query, Partition& partition)
{
    std::vector<int> colors(query.getNumVertices());
    std::unordered_map<VertexID, std::unordered_map<int, int>> edgeColors;
    Partition next, splitOut;

    while (true) {
        partitionToColor(partition, colors);
        updateEdgeColors(query, colors, edgeColors);

        bool stable = true;
        for (int i = 0; i < static_cast<int>(partition.size()); ++i) {
            if (partition[i].size() > 1 &&
                !isCellEqual(i, partition, colors, edgeColors))
            {
                stable = false;
                splitCell(partition[i], splitOut,
                          static_cast<int>(partition.size()), colors, edgeColors);
                for (auto& cell : splitOut) next.push_back(std::move(cell));
                splitOut.clear();
            } else {
                next.push_back(partition[i]);
            }
        }
        std::swap(partition, next);
        next.clear();
        if (stable) break;
    }

    /* Canonical sort: first by cell size, then by edge-color profile.
     *
     * Using edge-color profile as the tiebreaker (instead of vertex ID)
     * ensures that cells in different branches of the individualization
     * tree end up in different orders when their structural context
     * differs.  This is essential for readPermutation to correctly
     * reconstruct the automorphism across top and bottom partitions.
     *
     * Example: after individualizing v1 vs v2, the cells {3} and {4}
     * have swapped profiles in the two branches (v3 connects to the
     * "top" position in one branch and "bottom" in the other).  Profile-
     * based sort puts {4} before {3} in one branch, {3} before {4} in
     * the other, so readPermutation sees sigma[3]=4 and sigma[4]=3.    */
    std::sort(partition.begin(), partition.end(),
              [&](const std::set<VertexID>& a, const std::set<VertexID>& b) {
                  if (a.size() != b.size()) return a.size() < b.size();
                  VertexID ra = *a.begin(), rb = *b.begin();
                  auto it_a = edgeColors.find(ra);
                  auto it_b = edgeColors.find(rb);
                  std::vector<std::pair<int,int>> va, vb;
                  if (it_a != edgeColors.end())
                      va.assign(it_a->second.begin(), it_a->second.end());
                  if (it_b != edgeColors.end())
                      vb.assign(it_b->second.begin(), it_b->second.end());
                  std::sort(va.begin(), va.end());
                  std::sort(vb.begin(), vb.end());
                  if (va != vb) return va < vb;
                  return ra < rb;   // final fallback: vertex ID
              });
}

inline std::unordered_map<VertexID, std::set<VertexID>>
SymBreak::processPartitions(
        const QueryGraph& query,
        Partition& top, Partition& bottom, Partition& orbits,
        std::unordered_map<VertexID, std::set<VertexID>> coset,
        std::vector<std::vector<VertexID>>& permutations)
{
    if (orbits.empty()) {
        orbits.resize(query.getNumVertices());
        for (VertexID v = 0; v < query.getNumVertices(); ++v)
            orbits[v].insert(v);
    }

    assert(equalLength(top, bottom));

    if (isDiscrete(top)) {
        auto perm = readPermutation(top, bottom);
        mergeOrbits(orbits, perm);
        bool is_identity = true;
        for (uint32_t u = 0; u < static_cast<uint32_t>(perm.size()); ++u)
            if (perm[u] != u) { is_identity = false; break; }
        if (!is_identity) permutations.push_back(std::move(perm));
        return coset;
    }

    VertexID minNode = static_cast<VertexID>(-1);
    int      cellIdx = -1;
    for (int i = 0; i < static_cast<int>(top.size()); ++i) {
        if (top[i].size() > 1) {
            for (VertexID u : top[i])
                if (minNode == static_cast<VertexID>(-1) || u < minNode) {
                    minNode = u; cellIdx = i;
                }
            break;
        }
    }

    assert(bottom[cellIdx].size() > 1);

    for (VertexID v : bottom[cellIdx]) {
        if (minNode != v && sameOrbit(orbits, minNode, v)) continue;
        auto [newTop, newBot] =
            individualizeCell(query, top, bottom, cellIdx, minNode, v);
        auto subCoset =
            processPartitions(query, newTop, newBot, orbits, coset, permutations);
        for (auto& [key, val] : subCoset)
            coset[key] = std::move(val);
    }

    int fixedBefore = 0;
    for (int i = 0; i < static_cast<int>(top.size()); ++i)
        if (top[i].size() == 1 && bottom[i].size() == 1 &&
            *top[i].begin() == *bottom[i].begin() &&
            *top[i].begin() < minNode)
            ++fixedBefore;

    if (static_cast<VertexID>(fixedBefore) == minNode &&
        coset.find(minNode) == coset.end())
    {
        for (const auto& orbit : orbits)
            if (orbit.count(minNode)) { coset[minNode] = orbit; break; }
    }

    return coset;
}

inline void
SymBreak::partitionToColor(Partition& partition, std::vector<int>& colors)
{
    for (int i = 0; i < static_cast<int>(partition.size()); ++i)
        for (VertexID v : partition[i])
            colors[v] = i;
}

inline void
SymBreak::updateEdgeColors(
        const QueryGraph& query, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors)
{
    edgeColors.clear();
    for (VertexID v = 0; v < query.getNumVertices(); ++v) {
        uint32_t deg;
        const VertexID* nb = query.getNeighbors(v, deg);
        for (uint32_t i = 0; i < deg; ++i)
            edgeColors[v][colors[nb[i]]]++;
    }
}

inline bool
SymBreak::isAllEqual(
        Partition& partition, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors)
{
    int np = static_cast<int>(partition.size());
    for (auto& cell : partition)
        for (auto i = cell.begin(); i != cell.end(); ++i)
            for (auto j = std::next(i); j != cell.end(); ++j) {
                if (colors[*i] != colors[*j]) return false;
                for (int p = 0; p < np; ++p) {
                    auto ci = edgeColors[*i].find(p);
                    auto cj = edgeColors[*j].find(p);
                    if ((ci != edgeColors[*i].end() ? ci->second : 0) !=
                        (cj != edgeColors[*j].end() ? cj->second : 0)) return false;
                }
            }
    return true;
}

inline bool
SymBreak::isCellEqual(
        int cellIdx, Partition& partition, const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors)
{
    int np = static_cast<int>(partition.size());
    auto& cell = partition[cellIdx];
    for (auto i = cell.begin(); i != cell.end(); ++i)
        for (auto j = std::next(i); j != cell.end(); ++j) {
            if (colors[*i] != colors[*j]) return false;
            for (int p = 0; p < np; ++p) {
                auto ci = edgeColors[*i].find(p);
                auto cj = edgeColors[*j].find(p);
                if ((ci != edgeColors[*i].end() ? ci->second : 0) !=
                    (cj != edgeColors[*j].end() ? cj->second : 0)) return false;
            }
        }
    return true;
}

inline void
SymBreak::splitCell(
        std::set<VertexID>& cell, Partition& out, int partitionSize,
        const std::vector<int>& colors,
        std::unordered_map<VertexID, std::unordered_map<int, int>>& edgeColors)
{
    out.clear();
    for (VertexID u : cell) {
        bool placed = false;
        for (auto& bucket : out) {
            VertexID rep = *bucket.begin();
            if (colors[u] != colors[rep]) continue;
            bool same = true;
            for (int p = 0; p < partitionSize && same; ++p) {
                auto cu = edgeColors[u].find(p);
                auto cr = edgeColors[rep].find(p);
                if ((cu != edgeColors[u].end()   ? cu->second : 0) !=
                    (cr != edgeColors[rep].end() ? cr->second : 0)) same = false;
            }
            if (same) { bucket.insert(u); placed = true; break; }
        }
        if (!placed) out.push_back({u});
    }
}

inline bool
SymBreak::equalLength(const Partition& top, const Partition& bottom)
{
    if (top.size() != bottom.size()) return false;
    for (size_t i = 0; i < top.size(); ++i)
        if (top[i].size() != bottom[i].size()) return false;
    return true;
}

inline bool
SymBreak::isDiscrete(const Partition& partition)
{
    for (const auto& cell : partition)
        if (cell.size() != 1) return false;
    return true;
}

inline std::vector<VertexID>
SymBreak::readPermutation(const Partition& top, const Partition& bottom)
{
    std::vector<VertexID> sigma(top.size());
    for (size_t i = 0; i < top.size(); ++i) {
        assert(top[i].size() == 1 && bottom[i].size() == 1);
        sigma[*top[i].begin()] = *bottom[i].begin();
    }
    return sigma;
}

inline void
SymBreak::mergeOrbits(Partition& orbits, const std::vector<VertexID>& perm)
{
    for (uint32_t u = 0; u < static_cast<uint32_t>(perm.size()); ++u) {
        VertexID v = perm[u];
        if (u == v) continue;
        int uIdx = -1, vIdx = -1;
        for (int i = 0; i < static_cast<int>(orbits.size()); ++i) {
            if (orbits[i].count(u)) uIdx = i;
            if (orbits[i].count(v)) vIdx = i;
            if (uIdx != -1 && vIdx != -1) break;
        }
        assert(uIdx != -1 && vIdx != -1);
        if (uIdx != vIdx) {
            orbits[uIdx].insert(orbits[vIdx].begin(), orbits[vIdx].end());
            orbits.erase(orbits.begin() + vIdx);
        }
    }
}

inline bool
SymBreak::sameOrbit(const Partition& orbits, VertexID u, VertexID v)
{
    for (const auto& orbit : orbits)
        if (orbit.count(u) && orbit.count(v)) return true;
    return false;
}

inline std::pair<Partition, Partition>
SymBreak::individualizeCell(
        const QueryGraph& query,
        Partition& top, Partition& bottom,
        int cellIdx, VertexID tNode, VertexID bNode)
{
    Partition newTop = top;
    Partition newBot = bottom;

    auto& tCell = newTop[cellIdx];
    auto& bCell = newBot[cellIdx];
    assert(tCell.count(tNode) && bCell.count(bNode));

    tCell.erase(tNode);
    bCell.erase(bNode);
    newTop.insert(newTop.begin() + cellIdx, {tNode});
    newBot.insert(newBot.begin() + cellIdx, {bNode});

    refinePartitions(query, newTop);
    refinePartitions(query, newBot);

    return { std::move(newTop), std::move(newBot) };
}

inline void
SymBreak::printPermutations(std::vector<std::vector<VertexID>>& permutations)
{
    std::cout << "Permutations:\n";
    for (auto& perm : permutations) {
        for (uint32_t u = 0; u < static_cast<uint32_t>(perm.size()); ++u)
            if (perm[u] != u) std::cout << u << "→" << perm[u] << ' ';
        std::cout << '\n';
    }
}

inline void
SymBreak::printCosets(std::unordered_map<VertexID, std::set<VertexID>>& cosets)
{
    std::cout << "Cosets:\n";
    for (auto& [u, orbit] : cosets) {
        std::cout << u << ": ";
        for (VertexID v : orbit) std::cout << v << ' ';
        std::cout << '\n';
    }
}

inline void
SymBreak::printConstraints(std::vector<std::pair<VertexID, VertexID>>& constraints)
{
    std::cout << "Constraints:\n";
    for (auto& [u, v] : constraints)
        std::cout << '(' << u << ' ' << v << ")\n";
}

inline void
SymBreak::printConstraints(
        std::unordered_map<VertexID,
            std::pair<std::set<VertexID>, std::set<VertexID>>>& constraints)
{
    std::cout << "Full constraints:\n";
    for (auto& [u, ps] : constraints) {
        std::cout << u << ": ";
        for (VertexID w : ps.first)  std::cout << w << ' ';
        std::cout << "| ";
        for (VertexID w : ps.second) std::cout << w << ' ';
        std::cout << '\n';
    }
}

inline void
SymBreak::printPartition(Partition& partition)
{
    std::cout << "partition:\t";
    for (auto& cell : partition) {
        for (VertexID v : cell) std::cout << v << ' ';
        std::cout << '|';
    }
    std::cout << '\n';
}
