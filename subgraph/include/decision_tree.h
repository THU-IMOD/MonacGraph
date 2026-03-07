#pragma once
#include <vector>
#include <cstdint>
#include <cassert>
#include <cstdio>
#include <functional>

/* ================================================================== *
 *  DecisionTree                                                        *
 *                                                                      *
 *  k-level n-ary decision tree, flat array storage.                   *
 *                                                                      *
 *  Node encoding — one uint32_t per node:                             *
 *    bits[ 1: 0] = state  (0=FALSE, 1=TRUE, 2=UNKNOWN)               *
 *    bits[31: 2] = count  (non-decisive child count)                  *
 *                                                                      *
 *  count semantics:                                                    *
 *    ∃-node : # FALSE children so far;  count==n → node=FALSE        *
 *    ∀-node : # TRUE  children so far;  count==n → node=TRUE         *
 *                                                                      *
 *  Quantifier encoding — one uint32_t bitmask (k ≤ 32):              *
 *    quant_mask bit j = 1 → EXISTS at level j                        *
 *    quant_mask bit j = 0 → FORALL at level j                        *
 *    Entire mask lives in a register; no vector indirection.          *
 *                                                                      *
 *  Symmetry used to eliminate the EXISTS/FORALL branch in walk-UP:   *
 *    EXISTS = 1 = NS_TRUE,  FORALL = 0 = NS_FALSE                    *
 *    decisive_state   = static_cast<NodeState>(is_exists)            *
 *    completion_state = static_cast<NodeState>(1 - is_exists)        *
 *    One branch covers both quantifiers.                              *
 *                                                                      *
 *  Precomputed navigation (built once, O(total)):                     *
 *    child_start[f] — first child's global index, no multiply        *
 *    parent_arr[f]  — parent's global index,      no divide          *
 *                                                                      *
 *  path[] — member buffer, reused every insert(), no stack alloc.    *
 *                                                                      *
 *  Undo log — (id, old_word) pairs; undo_to(depth) replays in O(k). *
 * ================================================================== */

enum NodeState  : uint8_t { NS_FALSE = 0, NS_TRUE = 1, NS_UNKNOWN = 2 };
enum Quantifier : uint8_t { FORALL   = 0, EXISTS  = 1 };

/* ── node word helpers ───────────────────────────────────────────── */
namespace node {
    inline uint32_t make(NodeState s, uint32_t cnt) {
        return (cnt << 2) | static_cast<uint32_t>(s);
    }
    inline NodeState state(uint32_t w)               { return static_cast<NodeState>(w & 0x3u); }
    inline uint32_t  count(uint32_t w)               { return w >> 2; }
    inline uint32_t  set_state(uint32_t w, NodeState s) {
        return (w & ~0x3u) | static_cast<uint32_t>(s);
    }
    inline uint32_t  inc_count(uint32_t w)           { return w + 0x4u; }
}

/* ================================================================== */

class DecisionTree {
public:
    int      n;           // branching factor (= matched subgraph size)
    int      k;           // depth            (= number of quantifiers, k ≤ 32)

    /* ── optimization 1: quantifiers as a bitmask ────────────────── *
     * bit j = 1 → EXISTS at level j, bit j = 0 → FORALL at level j  *
     * Replaces std::vector<Quantifier>: entire mask in one register, *
     * no heap pointer, no cache miss on every loop iteration.        */
    uint32_t quant_mask;

    /* flat node storage */
    std::vector<uint32_t> nodes;

    /* offset[j] = first index of level j (geometric prefix sums)    *
     * offset[0]=0, offset[j+1]=offset[j]+n^j, offset[k+1]=total    */
    std::vector<int> offset;

    /* precomputed child / parent tables — no arithmetic at insert time */
    std::vector<int> child_start;   /* [0, offset[k])  — internal nodes */
    std::vector<int> parent_arr;    /* [0, offset[k+1]) — all nodes     */

    /* reusable path buffer — allocated once, no per-call stack alloc */
    std::vector<int> path;          /* size = k + 1 */

    /* ── undo log ────────────────────────────────────────────────── */
    struct LogEntry { int id; uint32_t old_word; };
    std::vector<LogEntry> log;
    std::vector<int>      checkpoints;  // checkpoints[bt_depth] = log.size() before that depth

    /* ── state for incremental update_tree traversal ─────────────── */
    std::vector<int> enum_coords;   /* size = k — current branch indices being evaluated */
    int new_depth;                  /* backtracking depth at which a new vertex was just placed */
    Order* matched = nullptr;       /* pointer to the current partial match (borrowed) */

    /* ── constructor ─────────────────────────────────────────────── */
    DecisionTree(int n_, int k_, std::vector<Quantifier> quants)
        : n(n_), k(k_), quant_mask(0)
    {
        assert(k >= 1 && k <= 32 && n >= 1);
        assert(static_cast<int>(quants.size()) == k);

        /* pack quantifier vector into bitmask */
        for (int j = 0; j < k; ++j)
            if (quants[j] == EXISTS)
                quant_mask |= (1u << j);

        /* offset table: offset[j] = start index of tree level j */
        offset.resize(k + 2);
        offset[0] = 0;
        long long pow_n = 1;
        for (int j = 0; j <= k; ++j) {
            offset[j + 1] = static_cast<int>(offset[j] + pow_n);
            pow_n *= n;
        }
        const int total = offset[k + 1];

        nodes.assign(total, node::make(NS_UNKNOWN, 0));

        /* child_start[f] = global index of the first child of node f */
        child_start.resize(offset[k]);
        for (int lvl = 0; lvl < k; ++lvl) {
            const int base_next = offset[lvl + 1];
            const int lvl_start = offset[lvl];
            const int lvl_end   = offset[lvl + 1];
            for (int f = lvl_start; f < lvl_end; ++f)
                child_start[f] = base_next + (f - lvl_start) * n;
        }

        /* parent_arr[c] = global index of the parent of node c */
        parent_arr.resize(total);
        for (int f = 0; f < offset[k]; ++f) {
            const int cb = child_start[f];
            for (int c = 0; c < n; ++c)
                parent_arr[cb + c] = f;
        }

        path.resize(k + 1);
        enum_coords.resize(k);
        log.reserve(total << 1);
        checkpoints.resize(1000);
    }

    /* ── checkpoint ──────────────────────────────────────────────── *
     * Records the current log size before processing backtrack       *
     * depth 'depth', so undo_to(depth) can revert all changes made  *
     * at that depth and below.                                       */
    void save_checkpoint(int depth) {
        checkpoints[depth] = static_cast<int>(log.size());
    }

    /* ── insert ──────────────────────────────────────────────────── *
     *                                                                *
     *  coords[0..k-1] : branch index at each level                  *
     *  phi_val        : predicate value for this leaf                *
     *                                                                *
     *  Walks DOWN from root to leaf, sets the leaf to phi_val, then *
     *  walks UP propagating state changes to ancestors.              *
     *                                                                *
     *  Returns root state after insertion.                           *
     * ─────────────────────────────────────────────────────────────  */
    NodeState insert(const int* coords, bool phi_val) {

        /* ── Step 1: walk DOWN ─────────────────────────────────── */
        path[0] = 0;
        for (int lvl = 0; lvl < k; ++lvl) {
            if (node::state(nodes[path[lvl]]) != NS_UNKNOWN)
                return node::state(nodes[0]);
            path[lvl + 1] = child_start[path[lvl]] + coords[lvl];
        }
        if (node::state(nodes[path[k]]) != NS_UNKNOWN)
            return node::state(nodes[0]);

        /* ── Step 2: set leaf ─────────────────────────────────── */
        NodeState child_state = phi_val ? NS_TRUE : NS_FALSE;
        log_write(path[k], node::make(child_state, 0));

        /* ── Step 3: walk UP ──────────────────────────────────── *
         *                                                          *
         *  Optimization 2: exploit EXISTS=1=NS_TRUE,              *
         *                           FORALL=0=NS_FALSE symmetry.   *
         *                                                          *
         *  For level lvl-1 (parent level):                        *
         *    is_exists      = (quant_mask >> (lvl-1)) & 1         *
         *    decisive_state = static_cast<NodeState>(is_exists)   *
         *      → EXISTS: decisive = TRUE  (any TRUE  → parent TRUE)  *
         *      → FORALL: decisive = FALSE (any FALSE → parent FALSE) *
         *    completion_state = static_cast<NodeState>(1-is_exists)  *
         *      → EXISTS: completion = FALSE (all FALSE → parent FALSE)*
         *      → FORALL: completion = TRUE  (all TRUE  → parent TRUE) *
         *                                                          *
         *  Both quantifiers handled in one unified branch.        */
        for (int lvl = k; lvl >= 1; --lvl) {
            const int p = path[lvl - 1];

            const uint32_t  is_exists  = (quant_mask >> (lvl - 1)) & 1u;
            const NodeState decisive   = static_cast<NodeState>(is_exists);
            const NodeState completion = static_cast<NodeState>(1u - is_exists);

            if (child_state == decisive) {
                /* decisive child: parent is immediately determined */
                log_write(p, node::set_state(nodes[p], decisive));
                /* child_state already == decisive, propagate upward */

            } else {
                /* non-decisive child: increment count */
                const uint32_t nw = node::inc_count(nodes[p]);
                if (node::count(nw) == static_cast<uint32_t>(n)) {
                    /* all n children non-decisive → parent = completion */
                    log_write(p, node::make(completion, n));
                    child_state = completion;   /* propagate upward */
                } else {
                    /* parent still UNKNOWN, stop propagation */
                    log_write(p, nw);
                    break;
                }
            }
        }

        return node::state(nodes[0]);
    }

    /* ================================================================ *
     *  update_tree  (recursive incremental update)                     *
     *                                                                   *
     *  Lazily inserts all newly-evaluable leaf paths that became       *
     *  available when the backtracker placed its (new_depth)-th        *
     *  vertex, i.e., when matched[new_depth] was just assigned.        *
     *                                                                   *
     *  Parameters:                                                      *
     *    depth    — current tree level being processed (1-based)       *
     *    p        — global index of the current tree node              *
     *    has_new  — true once we are on a path that has already        *
     *               descended through the new_depth branch             *
     *    phi      — second-order predicate evaluated at leaves         *
     *                                                                   *
     *  Strategy:                                                        *
     *    At each internal level, iterate over children indexed by      *
     *    matched[0..new_depth-1] (previously seen branches, only when  *
     *    has_new=false) and matched[new_depth] (the newly added one).  *
     *    A child is only visited if its subtree could possibly contain  *
     *    new, unevaluated leaves (i.e., the child node is UNKNOWN).    *
     *    When the leaf level (depth == k+1) is reached, phi is         *
     *    evaluated on the accumulated enum_coords and the leaf is set. *
     *    After each child update, the parent's state is propagated      *
     *    upward using the same decisive/completion logic as insert().  *
     *                                                                   *
     *  Returns true if the current node's state changed (so the        *
     *  caller can propagate upward).                                   *
     * ================================================================ */
    bool update_tree(int depth, int p, bool has_new,
                     const std::function<bool(const int*)>& phi)
    {
        // If this node is already resolved, no need to descend further
        if (node::state(nodes[p]) != NS_UNKNOWN) {
            return false;
        }

        // Leaf level: evaluate phi on the current coordinate tuple
        if (depth == k + 1) {
            log_write(p, node::make(phi(enum_coords.data()) ? NS_TRUE : NS_FALSE, 0));
            return true;
        }

        const uint32_t  is_exists  = (quant_mask >> (depth - 1)) & 1u;
        const NodeState decisive   = static_cast<NodeState>(is_exists);
        const NodeState completion = static_cast<NodeState>(1u - is_exists);

        // Visit previously-seen branches (indices 0..new_depth-1) only when
        // we haven't descended through the new dimension yet (has_new=false).
        // Once has_new=true, all paths below already include matched[new_depth]
        // as one of their coordinates and may contain unvisited leaves.
        if (depth != k || has_new) {
            for (int i = 0; i < new_depth; i++) {
                enum_coords[depth - 1] = (*matched)[i];
                if (update_tree(depth + 1, child_start[p] + i, has_new, phi)) {
                    int q = child_start[p] + i;
                    // Propagate child state upward using decisive/completion logic
                    if (node::state(nodes[q]) == decisive) {
                        log_write(p, node::set_state(nodes[p], decisive));
                        return true;
                    } else {
                        const uint32_t nw = node::inc_count(nodes[p]);
                        if (node::count(nw) == static_cast<uint32_t>(n)) {
                            log_write(p, node::make(completion, n));
                            return true;
                        } else {
                            log_write(p, nw);
                        }
                    }
                }
            }
        }

        // Visit the newly-added branch: index = new_depth, has_new becomes true
        enum_coords[depth - 1] = (*matched)[new_depth];
        if (update_tree(depth + 1, child_start[p] + new_depth, true, phi)) {
            int q = child_start[p] + new_depth;
            if (node::state(nodes[q]) == decisive) {
                log_write(p, node::set_state(nodes[p], decisive));
                return true;
            } else {
                const uint32_t nw = node::inc_count(nodes[p]);
                if (node::count(nw) == static_cast<uint32_t>(n)) {
                    log_write(p, node::make(completion, n));
                    return true;
                } else {
                    log_write(p, nw);
                }
            }
        }
        return false;
    }

    /* ================================================================ *
     *  update_for_depth  (entry point for backtracker)                 *
     *                                                                   *
     *  Called by the backtracking engine each time a new vertex is     *
     *  placed at backtrack depth bt_depth (i.e., matched[bt_depth]    *
     *  has just been assigned).                                         *
     *                                                                   *
     *  Steps:                                                           *
     *    1. Save the undo checkpoint for this depth so undo_to() can   *
     *       later revert all tree mutations made here.                  *
     *    2. Short-circuit if the root is already resolved — no tree    *
     *       work needed.                                                *
     *    3. Delegate to update_tree() which does an incremental DFS    *
     *       over tree nodes that have newly-evaluable leaves.          *
     *                                                                   *
     *  Returns the root state after the update (NS_FALSE means the     *
     *  current partial mapping is already inconsistent and the          *
     *  backtracker should prune this branch immediately).              *
     * ================================================================ */
    NodeState update_for_depth(
        int bt_depth,
        Order& matched_,
        const std::function<bool(const int*)>& phi)
    {
        save_checkpoint(bt_depth);

        // Quick exit: root already determined, no need to traverse
        if (node::state(nodes[0]) != NS_UNKNOWN) {
            return node::state(nodes[0]);
        }

        new_depth    = bt_depth;
        this->matched = &matched_;
        update_tree(1, 0, false, phi);
        return node::state(nodes[0]);
    }

    /* ── undo ────────────────────────────────────────────────────── *
     * Reverts all log entries recorded after checkpoints[depth].     *
     * Called by the backtracker when it backtracks past 'depth'.     */
    void undo_to(int depth) {
        const int target = checkpoints[depth];
        while (static_cast<int>(log.size()) > target) {
            nodes[log.back().id] = log.back().old_word;
            log.pop_back();
        }
    }

    /* ── convenience ─────────────────────────────────────────────── */
    NodeState root_state() const { return node::state(nodes[0]); }

    /** Dumps all tree nodes level-by-level with their state and count. */
    void print() const {
        static const char* sname[] = { "FALSE  ", "TRUE   ", "UNKNOWN" };
        for (int lvl = 0; lvl <= k; ++lvl) {
            printf("  lvl %d [%d..%d]: ", lvl, offset[lvl], offset[lvl+1]-1);
            for (int f = offset[lvl]; f < offset[lvl + 1]; ++f)
                printf("%s(cnt=%u) ", sname[node::state(nodes[f])],
                                      node::count(nodes[f]));
            printf("\n");
        }
        printf("  root = %s\n", sname[root_state()]);
    }

private:
    /**
     * Appends an undo entry for node 'id', then overwrites it with new_word.
     * All mutations to nodes[] go through this function to maintain the log.
     */
    inline void log_write(int id, uint32_t new_word) {
        log.push_back({id, nodes[id]});
        nodes[id] = new_word;
    }
};
