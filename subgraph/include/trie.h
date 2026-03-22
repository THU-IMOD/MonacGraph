#pragma once
#include "graph.h"
#include <vector>
#include <functional>
#include <cassert>

/* ================================================================== *
 *  MatchTrie                                                           *
 *                                                                      *
 *  A prefix tree (trie) that stores subgraph match results when the  *
 *  enumeration uses a static matching order.                          *
 *                                                                      *
 *  Why a trie is correct and efficient under static order             *
 *  ─────────────────────────────────────────────────────             *
 *  With static order, the query vertex matched at each depth d is     *
 *  always staticOrder[d] — it never changes between calls.  The      *
 *  candidate pool for each vertex is sorted by data vertex ID, and   *
 *  the outer loop at depth d iterates localPtr in ascending order.   *
 *  Therefore the complete match sequences are produced in strict      *
 *  lexicographic order of (matched[0], matched[1], ..., matched[qn-1]*
 *  This guarantees that each new match either:                        *
 *    (a) shares a prefix with the last match and appends a new leaf,  *
 *    (b) diverges at some depth and appends a new branch node.        *
 *  In both cases, the new value at the divergence depth is strictly   *
 *  larger than the last child at that node (because we are advancing  *
 *  in sorted order).  This makes the "append-only" optimisation       *
 *  safe: we only ever look at the LAST child, never need to search.   *
 *                                                                      *
 *  Why a trie is NOT safe under dynamic order                         *
 *  ─────────────────────────────────────────                          *
 *  With dynamic order (fail-first), the query vertex chosen at depth  *
 *  d changes between different branches of the search tree.  The      *
 *  resulting match sequences are NOT lexicographically ordered, so   *
 *  the append-only assumption breaks.  Attempting to insert would    *
 *  silently merge distinct matches that happen to share a prefix.    *
 *  The trie is therefore disabled (nullptr) when no static order is  *
 *  provided to EnumContext.                                            *
 *                                                                      *
 *  Memory layout                                                       *
 *  ─────────────                                                       *
 *  Each TrieNode stores:                                               *
 *    value    — the data vertex ID matched at this level              *
 *    children — sorted vector of child pointers (ascending by value)  *
 *  The root has value = UINT32_MAX (sentinel, never used as a data    *
 *  vertex ID) and its children are the depth-0 matches.               *
 *  Leaf nodes (at depth qn-1) have empty children vectors.            *
 *                                                                      *
 *  API                                                                 *
 *  ───                                                                 *
 *    insert(matched)         — record one match (O(qn))               *
 *    count()                 → total number of stored matches         *
 *    traverse(fn)            — visit every match: fn(vector<VertexID>)*
 *    traverseFrom(node, depth, buf, fn) — internal DFS helper         *
 * ================================================================== */

struct TrieNode {
    VertexID              value{UINT32_MAX};   // sentinel for root
    std::vector<TrieNode*> children;           // sorted ascending by value

    TrieNode() = default;
    explicit TrieNode(VertexID v) : value(v) {}

    ~TrieNode() {
        for (TrieNode* c : children) delete c;
    }

    /* Disabled copy; allow move */
    TrieNode(const TrieNode&)            = delete;
    TrieNode& operator=(const TrieNode&) = delete;
};

/* ------------------------------------------------------------------ *
 *  MatchTrie                                                           *
 * ------------------------------------------------------------------ */
class MatchTrie {
public:
    TrieNode  root;     // sentinel root; depth-0 values are root.children
    uint32_t  qn;       // number of query vertices (= tree depth)
    uint64_t  stored{0};

    explicit MatchTrie(uint32_t queryVertices)
        : qn(queryVertices) {}

    ~MatchTrie() = default;   // TrieNode destructor handles the tree

    /* ---------------------------------------------------------------- *
     *  insert                                                           *
     *                                                                   *
     *  Record one match: matched[d] is the data vertex at depth d.    *
     *                                                                   *
     *  Append-only fast path: at each node, check only the LAST child. *
     *  If the new value equals the last child's value, descend into   *
     *  it (shared prefix).  If the new value is strictly greater,     *
     *  create a new child and append it (new branch).                  *
     *  Any other case should never occur under static order (would     *
     *  indicate out-of-order insertion).                               *
     * ---------------------------------------------------------------- */
    /* Primary insert — takes raw array (used by backtrack hot path) */
    inline void insert(const VertexID* matched, uint32_t n) {
        TrieNode* cur = &root;
        for (uint32_t d = 0; d < n; ++d) {
            VertexID v = matched[d];
            if (cur->children.empty() || cur->children.back()->value < v) {
                TrieNode* child = new TrieNode(v);
                cur->children.push_back(child);
                cur = child;
            } else {
                assert(cur->children.back()->value == v &&
                       "MatchTrie::insert: out-of-order insertion");
                cur = cur->children.back();
            }
        }
        ++stored;
    }

    /* Convenience overload for vector callers */
    inline void insert(const std::vector<VertexID>& matched) {
        insert(matched.data(), (uint32_t)matched.size());
    }

    /* ---------------------------------------------------------------- *
     *  count                                                            *
     *  Returns the total number of stored matches.                     *
     *  O(1) — maintained incrementally by insert().                   *
     * ---------------------------------------------------------------- */
    uint64_t count() const { return stored; }

    /* ---------------------------------------------------------------- *
     *  traverse                                                         *
     *  Calls fn(match) for every stored match in lexicographic order. *
     *  match is a vector of length qn: match[d] = data vertex at d.  *
     * ---------------------------------------------------------------- */
    void traverse(const std::function<void(const std::vector<VertexID>&)>& fn) const {
        std::vector<VertexID> buf(qn);
        traverseFrom(&root, 0, buf, fn);
    }

    /* ---------------------------------------------------------------- *
     *  memoryCost                                                       *
     *  Approximate memory usage in bytes.                              *
     * ---------------------------------------------------------------- */
    size_t memoryCost() const {
        return nodeCost(&root);
    }

private:
    void traverseFrom(const TrieNode*    node,
                      uint32_t           depth,
                      std::vector<VertexID>& buf,
                      const std::function<void(const std::vector<VertexID>&)>& fn) const
    {
        if (depth == qn) {
            fn(buf);
            return;
        }
        for (const TrieNode* child : node->children) {
            buf[depth] = child->value;
            traverseFrom(child, depth + 1, buf, fn);
        }
    }

    static size_t nodeCost(const TrieNode* node) {
        size_t mem = sizeof(TrieNode)
                   + node->children.capacity() * sizeof(TrieNode*);
        for (const TrieNode* c : node->children)
            mem += nodeCost(c);
        return mem;
    }
};
