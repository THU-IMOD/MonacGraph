#pragma once
#include <vector>
#include <cstdint>
#include <cassert>
#include <cstdio>
#include <functional>
#include "graph.h"   // VertexID

/* ================================================================== *
 *  DecisionTree                                                        *
 *                                                                      *
 *  k-level n-ary decision tree, flat array storage.                   *
 *                                                                      *
 *  Node encoding — one uint8_t per node:                             *
 *    0 = NS_FALSE, 1 = NS_TRUE, 2 = NS_UNKNOWN                       *
 *    (count removed; nodes store state only)                          *
 *                                                                      *
 *  Quantifier encoding — one uint32_t bitmask (k ≤ 32):              *
 *    quant_mask bit j = 1 → EXISTS at level j                        *
 *    quant_mask bit j = 0 → FORALL at level j                        *
 *                                                                      *
 *  Symmetry:                                                           *
 *    EXISTS = 1 = NS_TRUE,  FORALL = 0 = NS_FALSE                    *
 *    decisive_state   = is_exists                                     *
 *    completion_state = 1 - is_exists                                 *
 *                                                                      *
 *  Count removal:                                                      *
 *    When new_depth == n-1, all children have been evaluated.         *
 *    If the node is still UNKNOWN, all children are non-decisive,     *
 *    so completion state (compl_s) is assigned directly.             *
 *                                                                      *
 *  Undo log — stores node id only; undo_to() restores to NS_UNKNOWN. *
 * ================================================================== */

enum NodeState  : uint8_t { NS_FALSE = 0, NS_TRUE = 1, NS_UNKNOWN = 2 };
enum Quantifier : uint8_t { FORALL   = 0, EXISTS  = 1 };

class DecisionTree {
public:
    int      n;           // branching factor (= matched subgraph size)
    int      k;           // depth            (= number of quantifiers, k ≤ 32)
    uint32_t quant_mask;

    std::vector<uint8_t> nodes;   // one byte per node, value in {0,1,2}

    std::vector<int> offset;      // offset[j] = first index of level j
    std::vector<int> child_start; // [0, offset[k])
    std::vector<int> parent_arr;  // [0, offset[k+1])
    std::vector<int> path;        // reusable, size = k+1

    /* ── undo log ────────────────────────────────────────────────── */
    struct LogEntry { int id; };
    std::vector<LogEntry> log;
    std::vector<int>      checkpoints;

    /* ── state for incremental update_tree ───────────────────────── */
    std::vector<int>       enum_coords;
    int                    new_depth;
    std::vector<VertexID>* matched = nullptr;

    /* ── constructor ─────────────────────────────────────────────── */
    DecisionTree(int n_, int k_, std::vector<Quantifier> quants)
        : n(n_), k(k_), quant_mask(0)
    {
        assert(k >= 1 && k <= 32 && n >= 1);
        assert(static_cast<int>(quants.size()) == k);

        for (int j = 0; j < k; ++j)
            if (quants[j] == EXISTS)
                quant_mask |= (1u << j);

        offset.resize(k + 2);
        offset[0] = 0;
        long long pow_n = 1;
        for (int j = 0; j <= k; ++j) {
            offset[j + 1] = static_cast<int>(offset[j] + pow_n);
            pow_n *= n;
        }
        const int total = offset[k + 1];

        nodes.assign(total, NS_UNKNOWN);

        child_start.resize(offset[k]);
        for (int lvl = 0; lvl < k; ++lvl) {
            const int base_next = offset[lvl + 1];
            const int lvl_start = offset[lvl];
            const int lvl_end   = offset[lvl + 1];
            for (int f = lvl_start; f < lvl_end; ++f)
                child_start[f] = base_next + (f - lvl_start) * n;
        }

        parent_arr.resize(total);
        for (int f = 0; f < offset[k]; ++f) {
            const int cb = child_start[f];
            for (int c = 0; c < n; ++c)
                parent_arr[cb + c] = f;
        }

        path.resize(k + 1);
        enum_coords.resize(k);
        log.reserve((size_t)n * total);
        checkpoints.resize(n + 1);
    }

    /* ── checkpoint ──────────────────────────────────────────────── */
    void save_checkpoint(int depth) {
        checkpoints[depth] = static_cast<int>(log.size());
    }

    /* ── insert ──────────────────────────────────────────────────── */
    NodeState insert(const int* coords, bool phi_val) {

        path[0] = 0;
        for (int lvl = 0; lvl < k; ++lvl) {
            if (nodes[path[lvl]] != NS_UNKNOWN)
                return static_cast<NodeState>(nodes[0]);
            path[lvl + 1] = child_start[path[lvl]] + coords[lvl];
        }
        if (nodes[path[k]] != NS_UNKNOWN)
            return static_cast<NodeState>(nodes[0]);

        uint8_t child_s = phi_val ? NS_TRUE : NS_FALSE;
        log_write(path[k], child_s);

        for (int lvl = k; lvl >= 1; --lvl) {
            const int     p         = path[lvl - 1];
            const uint8_t is_exists = (quant_mask >> (lvl - 1)) & 1u;

            if (child_s == is_exists) {
                log_write(p, is_exists);
            } else {
                break;
            }
        }

        return static_cast<NodeState>(nodes[0]);
    }

    /* ── update_tree ─────────────────────────────────────────────── */
    template<typename Fn>
    bool update_tree(int depth, int p, bool has_new, Fn&& phi)
    {
        if (nodes[p] != NS_UNKNOWN)
            return false;

        if (depth == k + 1) {
            log.push_back({p});
            nodes[p] = phi(enum_coords.data()) ? NS_TRUE : NS_FALSE;
            return true;
        }

        const uint8_t is_exists = (quant_mask >> (depth - 1)) & 1u;
        const uint8_t compl_s   = 1u - is_exists;

        if (depth != k || has_new) {
            for (int i = 0; i < new_depth; i++) {
                enum_coords[depth - 1] = (*matched)[i];
                if (update_tree(depth + 1, child_start[p] + i, has_new, phi)) {
                    int q = child_start[p] + i;
                    if (nodes[q] == is_exists) {
                        log.push_back({p});
                        nodes[p] = is_exists;
                        return true;
                    }
                }
            }
        }

        enum_coords[depth - 1] = (*matched)[new_depth];
        if (update_tree(depth + 1, child_start[p] + new_depth, true, phi)) {
            int q = child_start[p] + new_depth;
            if (nodes[q] == is_exists) {
                log.push_back({p});
                nodes[p] = is_exists;
                return true;
            }
        }

        if (new_depth == n - 1) {
            log.push_back({p});
            nodes[p] = compl_s;
            return true;
        }

        return false;
    }

    /* ── update_for_depth ────────────────────────────────────────── */
    template<typename Fn>
    NodeState update_for_depth(
        int bt_depth,
        std::vector<VertexID>& matched_,
        Fn&& phi)
    {
        save_checkpoint(bt_depth);

        if (nodes[0] != NS_UNKNOWN)
            return static_cast<NodeState>(nodes[0]);

        new_depth     = bt_depth;
        this->matched = &matched_;
        update_tree(1, 0, false, phi);
        return static_cast<NodeState>(nodes[0]);
    }

    /* ── undo ────────────────────────────────────────────────────── */
    void undo_to(int depth) {
        const int target = checkpoints[depth];
        while (static_cast<int>(log.size()) > target) {
            nodes[log.back().id] = NS_UNKNOWN;
            log.pop_back();
        }
    }

    /* ── convenience ─────────────────────────────────────────────── */
    NodeState root_state() const { return static_cast<NodeState>(nodes[0]); }

    void print() const {
        static const char* sname[] = { "FALSE  ", "TRUE   ", "UNKNOWN" };
        for (int lvl = 0; lvl <= k; ++lvl) {
            printf("  lvl %d [%d..%d]: ", lvl, offset[lvl], offset[lvl+1]-1);
            for (int f = offset[lvl]; f < offset[lvl + 1]; ++f)
                printf("%s ", sname[nodes[f]]);
            printf("\n");
        }
        printf("  root = %s\n", sname[nodes[0]]);
    }

private:
    inline void log_write(int id, uint8_t new_state) {
        log.push_back({id});
        nodes[id] = new_state;
    }
};
