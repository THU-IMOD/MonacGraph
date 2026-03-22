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
    std::vector<int>    enum_coords;
    int                 new_depth;
    const VertexID*     matched = nullptr;

    /* ── rotational-symmetry compression ─────────────────────────── *
     *                                                                 *
     * When phi is symmetric under permutation of variables within a  *
     * run of consecutive same-type quantifiers (∀∀...∀ or ∃∃...∃),  *
     * we only need to enumerate canonical (non-decreasing index)      *
     * tuples for those variables.  For a run of size g, this reduces *
     * the g innermost levels from n^1 + n^2 + ... + n^g nodes to     *
     * C(n,1) + C(n+1,2) + ... + C(n+g-1,g) ≈ n^g / g! nodes.       *
     *                                                                 *
     * Enabled by passing rotational_symmetry=true to the constructor.*
     * Mixed quantifier sequences (e.g. ∀∀∃) are supported: each     *
     * maximal same-type run is compressed independently.             *
     *                                                                 *
     * Data layout for a sym run [l_s, l_e] of size g:               *
     *   level l_s + d  holds  entry_factor * C(n+d, d+1) nodes.     *
     *   entry_factor = # nodes at level l_s - 1 (context entering    *
     *                  the run from the non-sym part of the tree).   *
     *                                                                 *
     *   Node at (entry_rank, sym_rank):                              *
     *     absolute index = offset[l] + entry_rank * C(n+d,d+1)      *
     *                                + sym_rank                      *
     *   where sym_rank = lex_rank of the non-decreasing path inside  *
     *   the run (see lex_rank_sym below).                            *
     *                                                                 *
     * min_child[p]:  minimum allowed matched-array index for the     *
     *   children of node p.  For sym nodes this equals the last      *
     *   index chosen within the run; for non-sym nodes it is 0.      *
     *   Child j (0-indexed) of p corresponds to matched index        *
     *   min_child[p] + j, stored at child_start[p] + j.             */
    bool sym_active{false};

    /* binom_coeff[a][b] = C(a,b), precomputed up to a = n+k+2.      */
    std::vector<std::vector<int>> binom_coeff;

    /* per tree-level (indices 1..k): which sym run (-1 = none)
     * and depth-within-run (0-indexed).                              */
    std::vector<int> lev_run_id;
    std::vector<int> lev_sdepth;

    /* min_child[p] for all non-leaf nodes (offset[k] entries total). *
     * Initialized to 0; only sym-run nodes get a value > 0.         */
    std::vector<int> min_child;

    /* Each run of consecutive same-type quantifiers that is ≥2 long. */
    struct SymRun {
        int qs;   // 0-indexed starting quantifier position
        int g;    // run length (number of levels in the run)
    };
    std::vector<SymRun> sym_runs;

    /* son[p * n + v] = child node index when choosing matched-array      *
     * index v at non-leaf node p.  -1 for non-canonical sym entries.    */
    std::vector<int> son;

    /* visit_flat / visit_base — precomputed child lists per (p, new_depth)
     *                                                                     *
     * For node p and new_depth d, the children to visit are:            *
     *   visit_flat[ visit_base[p] + 0 ]                                 *
     *   visit_flat[ visit_base[p] + 1 ]                                 *
     *   ...                                                              *
     *   visit_flat[ visit_base[p] + (d - min_child[p]) ]               *
     *                                                                    *
     * i.e. a prefix of length (d - min_child[p] + 1) of p's contiguous  *
     * slice in visit_flat.  The last element in the prefix is the "new" *
     * child (has_new=true); all preceding elements are "old".           *
     *                                                                    *
     * visit_flat is laid out as the concatenation of each node's valid  *
     * child list in node-index order:                                   *
     *   p=0: son[0*n + min_child[0]], ..., son[0*n + n-1]              *
     *   p=1: son[1*n + min_child[1]], ..., son[1*n + n-1]              *
     *   ...                                                              *
     *                                                                    *
     * visit_base[p] = start index of p's slice in visit_flat.          *
     * Total visit_flat size = Σ_p (n - min_child[p]).                  *
     *                                                                    *
     * For non-sym trees: min_child[p] = 0 for all p, so visit_flat     *
     * equals son and visit_base[p] = p * n.                            */
    std::vector<int> visit_flat;
    std::vector<int> visit_base;   // size = offset[k] + 1 (sentinel at end)

    /* ── constructor ─────────────────────────────────────────────── */
    DecisionTree(int n_, int k_, std::vector<Quantifier> quants,
                 bool rotational_symmetry = false)
        : n(n_), k(k_), quant_mask(0)
    {
        assert(k >= 1 && k <= 32 && n >= 1);
        assert(static_cast<int>(quants.size()) == k);

        for (int j = 0; j < k; ++j)
            if (quants[j] == EXISTS)
                quant_mask |= (1u << j);

        if (rotational_symmetry) {
            setup_sym(quants);
        }

        if (!sym_active) {
            /* Standard n-ary tree setup */
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
                for (int f = lvl_start; f < lvl_end; ++f) {
                    child_start[f] = base_next + (f - lvl_start) * n;
                }
            }

            parent_arr.resize(total);
            for (int f = 0; f < offset[k]; ++f) {
                const int cb = child_start[f];
                for (int c = 0; c < n; ++c) {
                    parent_arr[cb + c] = f;
                }
            }

            min_child.assign(offset[k], 0);

            /* Build son[p*n+v] = child_start[p] + v for all non-leaf p. */
            son.resize((size_t)offset[k] * n);
            for (int f = 0; f < offset[k]; ++f) {
                for (int v = 0; v < n; ++v) {
                    son[(size_t)f * n + v] = child_start[f] + v;
                }
            }
        }

        /* Build visit_flat / visit_base from son and min_child.
         * Shared by both sym and non-sym paths.                    */
        build_visit_table();

        path.resize(k + 1);
        enum_coords.resize(k);
        log.reserve((size_t)n * (int)nodes.size());
        checkpoints.resize(n + 1);
    }

    /* ── checkpoint ──────────────────────────────────────────────── */
    void save_checkpoint(int depth) {
        checkpoints[depth] = static_cast<int>(log.size());
    }

    /* ── insert ──────────────────────────────────────────────────── *
     *                                                                 *
     * coords[j] = matched-array index (0..n-1) assigned to quant j. *
     * In sym mode, non-canonical (non-decreasing) tuples are silently*
     * ignored — the caller must ensure phi is indeed symmetric.      */
    NodeState insert(const int* coords, bool phi_val) {
        if (sym_active) {
            return insert_sym(coords, phi_val);
        }

        path[0] = 0;
        for (int lvl = 0; lvl < k; ++lvl) {
            if (nodes[path[lvl]] != NS_UNKNOWN) {
                return static_cast<NodeState>(nodes[0]);
            }
            path[lvl + 1] = son[(size_t)path[lvl] * n + coords[lvl]];
        }
        if (nodes[path[k]] != NS_UNKNOWN) {
            return static_cast<NodeState>(nodes[0]);
        }

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

    /* ── update_tree ─────────────────────────────────────────────── *
     *                                                                 *
     * cl[0..new_depth] is the complete child list for node p.       *
     * cl[0..new_depth-1] are "old" children (may already be decided)*
     * cl[new_depth]      is the "new" child (always UNKNOWN on      *
     *                    first visit).                               *
     *                                                                *
     * Old children that were evaluated in a previous call return    *
     * immediately via the nodes[p] != NS_UNKNOWN guard at the top.  *
     * No has_new flag is needed.                                     */
    template<typename Fn>
    bool update_tree(int depth, int p, Fn&& phi)
    {
        if (nodes[p] != NS_UNKNOWN) {
            return false;
        }

        if (depth == k + 1) {
            log.push_back({p});
            nodes[p] = phi(enum_coords.data()) ? NS_TRUE : NS_FALSE;
            return true;
        }

        const uint8_t is_exists = (quant_mask >> (depth - 1)) & 1u;
        const uint8_t compl_s   = 1u - is_exists;

        const int* cl = visit_flat.data() + visit_base[p];

        for (int i = 0; i <= new_depth; ++i) {
            enum_coords[depth - 1] = matched[i];
            int q = cl[i];
            if (update_tree(depth + 1, q, phi)) {
                if (nodes[q] == is_exists) {
                    log.push_back({p});
                    nodes[p] = is_exists;
                    return true;
                }
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
        int             bt_depth,
        const VertexID* matched_,
        Fn&&            phi)
    {
        save_checkpoint(bt_depth);
        if (nodes[0] != NS_UNKNOWN) {
            return static_cast<NodeState>(nodes[0]);
        }
        new_depth     = bt_depth;
        this->matched = matched_;
        if (sym_active) {
            update_tree_sym(1, 0, phi);
        } else {
            update_tree(1, 0, phi);
        }
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

    /* ── convenience queries ─────────────────────────────────────── */

    /* Current state of the root node.  NS_TRUE / NS_FALSE means the
     * constraint is fully resolved; NS_UNKNOWN means more matching
     * is needed before a verdict is possible.                       */
    NodeState root_state() const {
        return static_cast<NodeState>(nodes[0]);
    }

    /* True iff the root is already resolved (not NS_UNKNOWN).
     * Callers can use this to skip update_for_depth entirely when the
     * tree has already settled — e.g. in DT-first probe ordering.   */
    bool is_decided() const { return nodes[0] != NS_UNKNOWN; }

    /* Quantifier at tree level lvl (0-indexed).
     * Saves callers from manually decoding quant_mask.              */
    Quantifier quantifier_at(int lvl) const {
        assert(lvl >= 0 && lvl < k);
        return ((quant_mask >> lvl) & 1u) ? EXISTS : FORALL;
    }

    /* Number of nodes at BFS level lvl (0 = root level).           */
    int level_size(int lvl) const {
        assert(lvl >= 0 && lvl <= k);
        return offset[lvl + 1] - offset[lvl];
    }

    /* Total number of nodes in the tree (all levels combined).      */
    int total_nodes() const { return offset[k + 1]; }

    /* Number of UNKNOWN nodes at level lvl.
     * Used by adaptStrategy to estimate the remaining DT work.
     * Calling this avoids duplicating the inline loop in enumerate.h */
    int unknown_count_at_level(int lvl) const {
        assert(lvl >= 0 && lvl <= k);
        int cnt = 0;
        for (int i = offset[lvl]; i < offset[lvl + 1]; ++i)
            cnt += (nodes[i] == NS_UNKNOWN);
        return cnt;
    }

    /* Fraction of UNKNOWN nodes at level lvl  ∈ [0.0, 1.0].
     * Convenience wrapper used in the adaptive probe-order model.  */
    double unknown_fraction_at_level(int lvl) const {
        int sz = level_size(lvl);
        return sz > 0 ? unknown_count_at_level(lvl) / (double)sz : 0.0;
    }

    /* Number of log entries written since the checkpoint at `depth`.
     * Gives the number of node state changes in the current subtree.
     * Useful for profiling DT work per probe.                       */
    int log_entries_since(int depth) const {
        return static_cast<int>(log.size()) - checkpoints[depth];
    }

    /* ── reset ───────────────────────────────────────────────────── *
     *                                                                *
     * Resets all nodes to NS_UNKNOWN and clears the log.  Use this  *
     * to reuse the same DecisionTree across multiple top-level       *
     * matches without reallocating.  Cheaper than constructing a     *
     * new tree: the structural arrays (offset, child_start,          *
     * parent_arr) are unchanged.                                     */
    void reset() {
        std::fill(nodes.begin(), nodes.end(), NS_UNKNOWN);
        log.clear();
        std::fill(checkpoints.begin(), checkpoints.end(), 0);
    }

    /* ── evaluate ────────────────────────────────────────────────── *
     *                                                                *
     * Pure, stateless evaluation of a full coordinate tuple against  *
     * phi.  Does NOT modify the tree and does NOT write to the log.  *
     *                                                                *
     * Use case: verify a complete match (depth == n) without         *
     * committing the result to the tree.  Also useful for unit       *
     * testing phi independently of the incremental update path.      *
     *                                                                *
     * coords[lvl] = the matched data vertex at tree level lvl.      *
     * Returns true iff phi(coords) is satisfied.                    */
    template<typename Fn>
    bool evaluate(const int* coords, Fn&& phi) const {
        return phi(coords);
    }

    /* ── clone ───────────────────────────────────────────────────── *
     *                                                                *
     * Deep-copies the current tree state into `dst`.  `dst` must    *
     * have been constructed with the same (n, k, quants) parameters.*
     * Useful for parallel or speculative search: take a snapshot     *
     * before a branching point, restore it on backtrack.            *
     *                                                                *
     * Only the mutable state is copied (nodes, log, checkpoints).   *
     * Structural arrays are identical by construction so we skip     *
     * copying them.                                                  */
    void clone_state_into(DecisionTree& dst) const {
        assert(dst.n == n && dst.k == k && dst.quant_mask == quant_mask);
        dst.nodes       = nodes;
        dst.log         = log;
        dst.checkpoints = checkpoints;
    }

    /* ── print ───────────────────────────────────────────────────── */
    void print() const {
        static const char* sname[] = { "F", "T", "?" };
        printf("DecisionTree  n=%d  k=%d  quant=", n, k);
        for (int j = 0; j < k; ++j)
            printf("%c", quantifier_at(j) == EXISTS ? 'E' : 'A');
        printf("  total_nodes=%d\n", total_nodes());
        for (int lvl = 0; lvl <= k; ++lvl) {
            printf("  lvl %d [%3d..%3d]  unk=%d/%d : ",
                   lvl, offset[lvl], offset[lvl + 1] - 1,
                   unknown_count_at_level(lvl), level_size(lvl));
            for (int f = offset[lvl]; f < offset[lvl + 1]; ++f)
                printf("%s", sname[nodes[f]]);
            printf("\n");
        }
        printf("  root = %s  log_entries=%d\n",
               sname[nodes[0]], static_cast<int>(log.size()));
    }

private:
    /* ── log_write ───────────────────────────────────────────────── */
    inline void log_write(int id, uint8_t new_state) {
        log.push_back({id});
        nodes[id] = new_state;
    }

    /* ── binom ───────────────────────────────────────────────────── *
     *                                                                 *
     * C(a, b) from precomputed table.  Returns 0 for out-of-range   *
     * arguments.                                                      */
    int binom(int a, int b) const {
        if (b < 0 || a < 0 || b > a) {
            return (b == 0) ? 1 : 0;
        }
        if (a >= (int)binom_coeff.size()) {
            return 0;
        }
        if (b >= (int)binom_coeff[a].size()) {
            return 0;
        }
        return binom_coeff[a][b];
    }

    /* ── sym_child_of ────────────────────────────────────────────── *
     *                                                                 *
     * Navigate from sym node p (at 0-indexed tree level j) to its   *
     * child for choosing value v in the run.                         *
     *                                                                 *
     * Precondition: the edge from level j to j+1 is a sym edge,     *
     * i.e. lev_run_id[j] >= 0 && lev_sdepth[j] >= 1.               *
     *                                                                 *
     * Rank formula (co-lex on multisets):                            *
     *   rank(a_0 ≤ a_1 ≤ ... ≤ a_{d-1}) = Σ_{i=0}^{d-1} C(a_i+i, i+1)
     *                                                                 *
     * Extending by one new element v at position s_child gives:      *
     *   new_rank = old_rank + C(v + s_child, s_child + 1)            *
     *                                                                 *
     * where s_child = lev_sdepth[j] (0-indexed position in run of   *
     * the new element being chosen at the edge j→j+1).              */
    int sym_child_of(int p, int v, int j) const {
        /* s_parent = number of run elements already chosen at level j,
         * minus 1 (0-indexed position of the last chosen element).    */
        int s_parent = lev_sdepth[j - 1];            // j >= 1 always (see precond)
        int w_parent = binom(n + s_parent, s_parent + 1);
        int c        = (p - offset[j]) / w_parent;   // context rank
        int r        = (p - offset[j]) % w_parent;   // partial-tuple rank

        int s_child  = lev_sdepth[j];                // = s_parent + 1
        int w_child  = binom(n + s_child, s_child + 1);
        return offset[j + 1] + c * w_child + r + binom(v + s_child, s_child + 1);
    }

    /* ── setup_sym ───────────────────────────────────────────────── *
     *                                                                 *
     * Called from the constructor when rotational_symmetry=true.     *
     * Builds the compressed tree layout and sets sym_active.         *
     *                                                                 *
     * Layout overview:                                               *
     *   Non-sym level lev: level_size = level_size[lev-1] * n       *
     *   Sym level lev (s+1 elements chosen in run, s=sdepth[lev-1])  *
     *     level_size = context_count * C(n+s, s+1)                  *
     *     where context_count = level_size[qs] (nodes before run)   *
     *                                                                 *
     * Sets: binom_coeff, sym_runs, lev_run_id, lev_sdepth, sym_active,
     *       offset, nodes, child_start, min_child.                   */
    void setup_sym(const std::vector<Quantifier>& quants) {
        /* Build Pascal's triangle up to C(n+k+3, k+3). */
        int bmax = n + k + 4;
        binom_coeff.assign(bmax + 1, std::vector<int>(k + 5, 0));
        for (int a = 0; a <= bmax; ++a) {
            binom_coeff[a][0] = 1;
            for (int b = 1; b <= std::min(a, k + 4); ++b) {
                binom_coeff[a][b] = binom_coeff[a - 1][b - 1] + binom_coeff[a - 1][b];
            }
        }

        /* Identify maximal same-type runs of length ≥ 2.
         * lev_run_id[qidx] = run index (-1 if not in a run).
         * lev_sdepth[qidx] = 0-indexed position within the run.     */
        lev_run_id.assign(k, -1);
        lev_sdepth.assign(k, 0);

        int qi = 0;
        while (qi < k) {
            Quantifier qt = quants[qi];
            int start = qi;
            while (qi < k && quants[qi] == qt) {
                ++qi;
            }
            int g = qi - start;
            if (g >= 2) {
                int rid = static_cast<int>(sym_runs.size());
                sym_runs.push_back({start, g});
                for (int d = 0; d < g; ++d) {
                    lev_run_id[start + d] = rid;
                    lev_sdepth[start + d] = d;
                }
            }
        }

        if (sym_runs.empty()) {
            return;   /* no compression possible; caller falls through to standard setup */
        }
        sym_active = true;

        /* Compute offset[] (tree level sizes).
         *
         * At tree level lev (lev quantifiers chosen, 0-indexed):
         *   Non-sym edge (arriving at lev via qidx=lev-1 outside a run):
         *     level_size[lev] = level_size[lev-1] * n
         *
         *   Sym edge (arriving at lev via qidx=lev-1, sdepth[lev-1]=s):
         *     context_count = level_size[qs]   (nodes at level qs)
         *     level_size[lev] = context_count * C(n+s, s+1)
         *     where s = lev_sdepth[lev-1] and qs = run start.           */
        offset.resize(k + 2, 0);
        offset[0] = 0;
        offset[1] = 1;   // root node
        for (int lev = 1; lev <= k; ++lev) {
            int qidx = lev - 1;
            int rid  = lev_run_id[qidx];
            if (rid < 0) {
                /* Standard: fanout n */
                int prev = offset[lev] - offset[lev - 1];
                offset[lev + 1] = offset[lev] + prev * n;
            } else {
                int s  = lev_sdepth[qidx];              // 0-indexed depth (= s+1 elements at lev)
                int qs = sym_runs[rid].qs;               // tree level before run
                int cc = offset[qs + 1] - offset[qs];   // context count
                offset[lev + 1] = offset[lev] + cc * binom(n + s, s + 1);
            }
        }

        const int total = offset[k + 1];
        nodes.assign(total, NS_UNKNOWN);

        /* child_start:
         *   Non-sym edges and s_edge=0 (first sym element, C(n,1)=n):
         *     child_start[f] = offset[lev+1] + (f-offset[lev]) * n  (standard)
         *   Sym edges with s_edge >= 1:
         *     child_start[f] = -1  (invalid; sym_child_of is used instead)         */
        child_start.resize(offset[k], -1);
        for (int lev = 0; lev < k; ++lev) {
            /* Edge lev→lev+1 corresponds to quantifier qidx=lev. */
            int rid    = lev_run_id[lev];
            int s_edge = (rid >= 0) ? lev_sdepth[lev] : -1;
            bool sym_edge = (rid >= 0 && s_edge >= 1);

            if (!sym_edge) {
                int base  = offset[lev + 1];
                int start = offset[lev];
                int end   = offset[lev + 1];
                for (int f = start; f < end; ++f) {
                    child_start[f] = base + (f - start) * n;
                }
            }
            /* Sym edges: child_start stays -1 — sym_child_of used at runtime. */
        }

        /* min_child: for each sym node p, the minimum valid value for its
         * children (= last element chosen within the run at that node).
         * Fill by enumerating all non-decreasing tuples for each run.    */
        min_child.assign(offset[k], 0);
        for (int rid = 0; rid < (int)sym_runs.size(); ++rid) {
            fill_run_min_child(rid);
        }

        /* parent_arr: non-trivial to compute for compressed sym layout
         * (non-uniform fanout); left empty in sym mode.                  */
        parent_arr.clear();

        /* ── Precompute son[p * n + v] ─────────────────────────────── *
         *                                                                *
         * For each non-leaf node p at tree level lev and each value     *
         * v in [0, n):                                                  *
         *   • Non-sym edge (s_edge < 1):  son = child_start[p] + v     *
         *   • Sym edge, v >= min_child[p]: son = sym_child_of(p,v,lev) *
         *   • Sym edge, v <  min_child[p]: son = -1  (non-canonical)   *
         *                                                               *
         * sym_child_of is called only here during construction; the     *
         * hot paths (insert_sym, update_tree_sym) use son directly.    */
        son.assign((size_t)offset[k] * n, -1);
        for (int lev = 0; lev < k; ++lev) {
            int  rid      = lev_run_id[lev];
            bool sym_edge = (rid >= 0 && lev_sdepth[lev] >= 1);

            for (int p = offset[lev]; p < offset[lev + 1]; ++p) {
                int vm = sym_edge ? min_child[p] : 0;
                for (int v = vm; v < n; ++v) {
                    if (sym_edge) {
                        son[(size_t)p * n + v] = sym_child_of(p, v, lev);
                    } else {
                        son[(size_t)p * n + v] = child_start[p] + v;
                    }
                }
            }
        }
    }

    /* ── build_visit_table ───────────────────────────────────────── *
     *                                                                 *
     * Called once after son[] and min_child[] are fully populated.  *
     *                                                                 *
     * For each non-leaf node p, its valid children are:             *
     *   son[p*n + min_child[p]],  son[p*n + min_child[p]+1],  ...  *
     *   son[p*n + n-1]                                              *
     *                                                                *
     * These are packed contiguously into visit_flat[], and          *
     * visit_base[p] records where p's slice begins.                 *
     *                                                                *
     * At runtime, when update_tree(_sym) processes node p with      *
     * new_depth = d:                                                 *
     *   const int* cl  = visit_flat.data() + visit_base[p];        *
     *   int        cnt = d - min_child[p] + 1;   // if d >= v_min  *
     *   cl[0..cnt-2]  → old children  (has_new inherited)          *
     *   cl[cnt-1]     → new child     (has_new = true)             */
    void build_visit_table() {
        const int np = offset[k];   // number of non-leaf nodes
        visit_base.resize(np + 1);
        int total = 0;
        for (int p = 0; p < np; ++p) {
            visit_base[p] = total;
            total += n - min_child[p];   // valid children count for p
        }
        visit_base[np] = total;

        visit_flat.resize(total);
        for (int p = 0; p < np; ++p) {
            int vm  = min_child[p];
            int dst = visit_base[p];
            for (int v = vm; v < n; ++v) {
                visit_flat[dst++] = son[(size_t)p * n + v];
            }
        }
    }

    /* ── fill_run_min_child ──────────────────────────────────────── *
     *                                                                 *
     * For sym run `rid`, enumerate every non-decreasing tuple of     *
     * every length 1..g and write the last chosen value (a_{s}) into *
     * min_child for the corresponding node.                           */
    void fill_run_min_child(int rid) {
        int qs = sym_runs[rid].qs;
        int g  = sym_runs[rid].g;
        /* context_count = level_size[qs] = number of nodes before run */
        int cc = offset[qs + 1] - offset[qs];
        fill_run_min_child_rec(qs, g, cc, 0, 0, 0);
    }

    /* Recursively enumerate all non-decreasing tuples within a sym run.
     *
     * Parameters:
     *   qs           — 0-indexed quantifier at the start of the run
     *   g            — run length
     *   cc           — context count (nodes at tree level qs)
     *   s            — 0-indexed position within run (element being placed)
     *   min_v        — minimum allowed value for position s (= a_{s-1}, or 0)
     *   prefix_rank  — cumulative sym rank of a[0..s-1]                      */
    void fill_run_min_child_rec(int qs, int g, int cc,
                                int s, int min_v, int prefix_rank) {
        /* Tree level for nodes where s+1 run elements have been chosen:
         * quantifier qidx = qs+s, so tree level = qs+s+1.               */
        int lev   = qs + s + 1;
        int width = binom(n + s, s + 1);   // per-context width at lev

        for (int v = min_v; v < n; ++v) {
            int r = prefix_rank + binom(v + s, s + 1);   // sym rank including a[s]=v
            if (lev < k) {
                /* Only fill min_child for non-leaf nodes (level < k). */
                for (int c = 0; c < cc; ++c) {
                    int p = offset[lev] + c * width + r;
                    min_child[p] = v;   // a[s] = v = minimum for next run element
                }
            }
            if (s < g - 1) {
                fill_run_min_child_rec(qs, g, cc, s + 1, v, r);
            }
        }
    }

    /* ── insert_sym ──────────────────────────────────────────────── *
     *                                                                 *
     * Insert a full coordinate tuple into the compressed tree.       *
     * The caller must pass coords[] already sorted (non-decreasing)  *
     * within each sym run.  Non-canonical tuples (those that violate *
     * the non-decreasing constraint within a run) produce an invalid *
     * node index and are silently ignored.                           */
    NodeState insert_sym(const int* coords, bool phi_val) {
        path[0] = 0;
        for (int lvl = 0; lvl < k; ++lvl) {
            if (nodes[path[lvl]] != NS_UNKNOWN) {
                return static_cast<NodeState>(nodes[0]);
            }
            int next = son[(size_t)path[lvl] * n + coords[lvl]];
            if (next < 0) {
                /* Non-canonical tuple (violates non-decreasing within run).
                 * phi symmetry guarantees its value equals the canonical
                 * permutation, which is already or will be recorded.     */
                return static_cast<NodeState>(nodes[0]);
            }
            path[lvl + 1] = next;
        }
        if (nodes[path[k]] != NS_UNKNOWN) {
            return static_cast<NodeState>(nodes[0]);
        }

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

    /* ── update_tree_sym ─────────────────────────────────────────── *
     *                                                                 *
     * Sym-compressed version of update_tree.  has_new is gone:      *
     * cl[0..new_slot] is the precomputed canonical child list and    *
     * already-decided nodes self-prune via nodes[p] != NS_UNKNOWN.  */
    template<typename Fn>
    bool update_tree_sym(int depth, int p, Fn&& phi)
    {
        if (nodes[p] != NS_UNKNOWN) {
            return false;
        }

        if (depth == k + 1) {
            log.push_back({p});
            nodes[p] = phi(enum_coords.data()) ? NS_TRUE : NS_FALSE;
            return true;
        }

        int qidx = depth - 1;
        const uint8_t is_exists = (quant_mask >> qidx) & 1u;
        const uint8_t compl_s   = 1u - is_exists;

        const int v_min = min_child[p];
        if (new_depth < v_min) {
            if (new_depth == n - 1) {
                log.push_back({p});
                nodes[p] = compl_s;
                return true;
            }
            return false;
        }

        const int* cl       = visit_flat.data() + visit_base[p];
        const int  new_slot = new_depth - v_min;

        for (int i = 0; i <= new_slot; ++i) {
            enum_coords[qidx] = matched[v_min + i];
            int q = cl[i];
            if (update_tree_sym(depth + 1, q, phi)) {
                if (nodes[q] == is_exists) {
                    log.push_back({p});
                    nodes[p] = is_exists;
                    return true;
                }
            }
        }

        if (new_depth == n - 1) {
            log.push_back({p});
            nodes[p] = compl_s;
            return true;
        }

        return false;
    }
};
