#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>

using VertexID = uint32_t;
using LabelID  = uint32_t;

/**
 * Undirected graph stored in CSR (Compressed Sparse Row) format.
 *
 * Memory layout
 * -------------
 *   rowPtr[v] .. rowPtr[v+1]  →  contiguous slice of colIdx[]
 *   colIdx[]                  →  sorted neighbor list for every vertex
 *
 * This is identical in layout to the reference implementation but uses
 * descriptive names chosen independently.
 *
 * Key design decisions
 * --------------------
 *  1. Raw arrays (new[]) for rowPtr/colIdx/labels so that getNeighbors()
 *     can return a raw pointer into the middle of the array with zero
 *     indirection — the hot-path inner loop requires this.
 *  2. rowPtr is built in one pass using the degree declared in each 'v' line,
 *     avoiding a separate edge-counting pass.
 *  3. hasEdge() uses binary_search on the sorted colIdx slice — O(log d),
 *     no adjacency matrix needed regardless of graph size.
 *  4. NLF (Neighbor Label Frequency) index is precomputed for label-aware
 *     candidate filtering.
 */
class Graph {
protected:
    /* ── graph dimensions ─────────────────────────────────── */
    uint32_t numVertices{0};
    uint32_t numEdges{0};
    uint32_t maxDegree{0};

    /* ── CSR core ─────────────────────────────────────────── */
    uint32_t* rowPtr{nullptr};    // size: numVertices + 1
                                  // rowPtr[v+1] - rowPtr[v] = degree(v)
    VertexID* colIdx{nullptr};    // size: 2 * numEdges
                                  // sorted within each row
    LabelID*  vertLabel{nullptr}; // size: numVertices

    /* ── NLF index ────────────────────────────────────────── */
    // nbrLabelFreq[v][l] = number of neighbors of v that carry label l
    // Used in GraphQL-style candidate filtering (one map per vertex).
    std::unordered_map<LabelID, uint32_t>* nbrLabelFreq{nullptr};

    /* ── internal builders ────────────────────────────────── */

    /**
     * Populates nbrLabelFreq[] by scanning the CSR adjacency lists.
     * Called once at the end of load() after colIdx is fully filled.
     */
    void buildNLF();

public:
    Graph() = default;

    virtual ~Graph() {
        delete[] rowPtr;       rowPtr       = nullptr;
        delete[] colIdx;       colIdx       = nullptr;
        delete[] vertLabel;    vertLabel    = nullptr;
        delete[] nbrLabelFreq; nbrLabelFreq = nullptr;
    }

    /* Owns raw memory — disable copy, allow move if needed later. */
    Graph(const Graph&)            = delete;
    Graph& operator=(const Graph&) = delete;

    virtual void load(const std::string& filePath);

    /* ── hot-path accessors ───────────────────────────────── */

    /**
     * Returns a raw pointer to the first neighbor of v,
     * and writes the degree of v into 'deg'.
     *
     * Callers iterate as:
     *   uint32_t deg;
     *   const VertexID* nb = g.getNeighbors(v, deg);
     *   for (uint32_t i = 0; i < deg; ++i) use(nb[i]);
     */
    const VertexID* getNeighbors(VertexID v, uint32_t& deg) const {
        deg = rowPtr[v + 1] - rowPtr[v];
        return colIdx + rowPtr[v];
    }

    uint32_t getDegree(VertexID v) const {
        return rowPtr[v + 1] - rowPtr[v];
    }

    /**
     * Edge existence test via binary search on the sorted neighbor slice.
     * O(log d) — no adjacency matrix, scales to arbitrarily large graphs.
     */
    bool hasEdge(VertexID u, VertexID v) const {
        return std::binary_search(colIdx + rowPtr[u],
                                  colIdx + rowPtr[u + 1], v);
    }

    /* ── metadata ─────────────────────────────────────────── */
    uint32_t getNumVertices() const { return numVertices; }
    uint32_t getNumEdges()    const { return numEdges; }
    uint32_t getMaxDegree()   const { return maxDegree; }

    LabelID  getLabelOf(VertexID v) const { return vertLabel[v]; }

    const std::unordered_map<LabelID, uint32_t>*
    getNbrLabelFreq(VertexID v) const { return nbrLabelFreq + v; }

    void print() const;
};


/**
 * Query graph — extends Graph with k-core decomposition.
 *
 * The k-core number of each query vertex guides ordering heuristics:
 * vertices with higher core number belong to the structurally dense
 * "core" region and are typically matched first.
 */
class QueryGraph : public Graph {
    int*     coreNum{nullptr};  // coreNum[v] = k-core number of v
    uint32_t coreSize{0};       // # vertices with coreNum > 1

    /**
     * Computes k-core numbers for all vertices using O(V + E) bucket-sort
     * peeling.  Fills coreNum[] and coreSize.
     * Called automatically by load() after the base graph is constructed.
     */
    void computeKCore();

public:
    QueryGraph() = default;

    ~QueryGraph() override {
        delete[] coreNum;
        coreNum = nullptr;
    }

    void load(const std::string& filePath) override {
        Graph::load(filePath);
        computeKCore();
    }

    int      getCoreNum(VertexID v) const { return coreNum[v]; }
    uint32_t getCoreSize()          const { return coreSize; }
};


/* ================================================================== *
 *  Graph::load                                                         *
 *                                                                      *
 *  File format:                                                        *
 *    t  N  M          — graph header (N vertices, M edges)            *
 *    v  id  label  d  — vertex with declared degree d                 *
 *    e  u   v         — undirected edge                               *
 *                                                                      *
 *  Construction strategy (single pass):                               *
 *    'v' lines → use declared degree to fill rowPtr[id+1] directly,   *
 *                no separate degree-counting pass needed.             *
 *    'e' lines → scatter into colIdx using a write-cursor per vertex. *
 *    Finish    → sort each row for binary_search compatibility.       *
 * ================================================================== */
inline void Graph::load(const std::string& filePath) {
    std::ifstream fin(filePath);
    if (!fin.is_open())
        throw std::runtime_error("Cannot open file: " + filePath);

    char lineTag;
    fin >> lineTag >> numVertices >> numEdges;   // 't N M'

    rowPtr    = new uint32_t[numVertices + 1];
    colIdx    = new VertexID[numEdges * 2];
    vertLabel = new LabelID[numVertices];
    memset(rowPtr, 0, sizeof(uint32_t) * (numVertices + 1));

    // writeCursor[v] = next free position in colIdx for vertex v's row
    std::vector<uint32_t> writeCursor(numVertices, 0);

    while (fin >> lineTag) {
        if (lineTag == 'v') {
            VertexID id;
            LabelID  label;
            uint32_t degree;
            fin >> id >> label >> degree;

            vertLabel[id] = label;
            maxDegree     = std::max(maxDegree, degree);

            // rowPtr[id+1] marks the exclusive end of id's neighbor slice
            rowPtr[id + 1] = rowPtr[id] + degree;

        } else {   /* lineTag == 'e' */
            VertexID u, v;
            fin >> u >> v;

            // Insert the edge in both directions (undirected graph)
            colIdx[rowPtr[u] + writeCursor[u]++] = v;
            colIdx[rowPtr[v] + writeCursor[v]++] = u;
        }
    }

    // Sort each row so binary_search and set_intersection work correctly
    for (uint32_t v = 0; v < numVertices; ++v)
        std::sort(colIdx + rowPtr[v], colIdx + rowPtr[v + 1]);

    buildNLF();
}

/* ------------------------------------------------------------------ *
 *  Graph::buildNLF                                                     *
 *                                                                      *
 *  nbrLabelFreq[v][l] = number of neighbors of v carrying label l.   *
 *  Precomputed once; used by label-aware candidate filtering.         *
 * ------------------------------------------------------------------ */
inline void Graph::buildNLF() {
    nbrLabelFreq =
        new std::unordered_map<LabelID, uint32_t>[numVertices];

    for (uint32_t v = 0; v < numVertices; ++v) {
        uint32_t deg;
        const VertexID* nb = getNeighbors(v, deg);
        for (uint32_t i = 0; i < deg; ++i)
            nbrLabelFreq[v][vertLabel[nb[i]]]++;
    }
}

/* ------------------------------------------------------------------ *
 *  Graph::print                                                        *
 *                                                                      *
 *  Dumps every vertex with its label, degree, and adjacency list.     *
 *  Intended for debugging small graphs only.                          *
 * ------------------------------------------------------------------ */
inline void Graph::print() const {
    std::cout << "Graph  n=" << numVertices
              << "  m=" << numEdges
              << "  maxDeg=" << maxDegree << "\n";

    for (uint32_t v = 0; v < numVertices; ++v) {
        uint32_t deg;
        const VertexID* nb = getNeighbors(v, deg);

        std::cout << "  v" << std::setw(4) << v
                  << "  label=" << vertLabel[v]
                  << "  deg="   << deg
                  << "  adj: [";
        for (uint32_t i = 0; i < deg; ++i) {
            if (i) std::cout << ", ";
            std::cout << nb[i];
        }
        std::cout << "]\n";
    }
}

/* ================================================================== *
 *  QueryGraph::computeKCore                                            *
 *                                                                      *
 *  O(V + E) bin-sort peeling algorithm.                               *
 *                                                                      *
 *  Algorithm sketch:                                                   *
 *    1. Initialize coreNum[v] = degree(v) for all v.                  *
 *    2. Build a bucket-sort ordering of vertices by degree.           *
 *    3. Peel in degree order: for the current minimum-degree vertex   *
 *       v, each unprocessed neighbor u has its effective degree       *
 *       decremented.  If coreNum[u] > coreNum[v], decrement          *
 *       coreNum[u] and re-bucket u (swap to front of its bucket).    *
 *    4. After peeling, coreNum[v] holds the k-core number of v.       *
 *                                                                      *
 *  Local variables:                                                    *
 *    sortedVerts  — vertices ordered by increasing degree             *
 *    slotOf       — position of each vertex in sortedVerts            *
 *    degBucket    — histogram of current degrees                      *
 *    bucketStart  — start index of each degree bucket in sortedVerts  *
 * ================================================================== */
inline void QueryGraph::computeKCore() {
    const int total   = static_cast<int>(numVertices);
    const int maxDeg  = static_cast<int>(maxDegree);

    coreNum = new int[total];

    int* sortedVerts = new int[total];
    int* slotOf      = new int[total];
    int* degBucket   = new int[maxDeg + 1];
    int* bucketStart = new int[maxDeg + 1];

    // Step 1: initialize coreNum to degree; count per-degree bucket sizes
    std::fill(degBucket, degBucket + maxDeg + 1, 0);
    for (int v = 0; v < total; ++v) {
        coreNum[v] = static_cast<int>(getDegree(v));
        degBucket[coreNum[v]]++;
    }

    // Step 2: compute prefix sums → bucketStart[d] = first slot for degree d
    int pos = 0;
    for (int d = 0; d <= maxDeg; ++d) {
        bucketStart[d] = pos;
        pos += degBucket[d];
    }

    // Place each vertex into its degree bucket and record its slot
    for (int v = 0; v < total; ++v) {
        slotOf[v]              = bucketStart[coreNum[v]];
        sortedVerts[slotOf[v]] = v;
        bucketStart[coreNum[v]]++;
    }

    // Restore bucketStart to actual starts (was incremented in place above)
    for (int d = maxDeg; d > 0; --d)
        bucketStart[d] = bucketStart[d - 1];
    bucketStart[0] = 0;

    // Step 3: Peeling — process vertices in degree order, reduce neighbors' core
    for (int i = 0; i < total; ++i) {
        int v = sortedVerts[i];

        uint32_t deg;
        const VertexID* nb = getNeighbors(v, deg);

        for (uint32_t j = 0; j < deg; ++j) {
            int u = static_cast<int>(nb[j]);
            // Only process neighbors not yet peeled (coreNum > coreNum[v])
            if (coreNum[u] <= coreNum[v]) continue;

            // Swap u to the front of its bucket, then shrink the bucket.
            // This logically moves u into the next-lower degree bucket.
            int bucketFront = bucketStart[coreNum[u]];
            int frontVert   = sortedVerts[bucketFront];

            if (u != frontVert) {
                sortedVerts[slotOf[u]]   = frontVert;
                sortedVerts[bucketFront] = u;
                slotOf[frontVert]        = slotOf[u];
                slotOf[u]                = bucketFront;
            }
            bucketStart[coreNum[u]]++;
            coreNum[u]--;
        }
    }

    // Step 4: count vertices belonging to a non-trivial core (coreNum > 1)
    coreSize = 0;
    for (int v = 0; v < total; ++v)
        if (coreNum[v] > 1) coreSize++;

    delete[] sortedVerts;
    delete[] slotOf;
    delete[] degBucket;
    delete[] bucketStart;
}
