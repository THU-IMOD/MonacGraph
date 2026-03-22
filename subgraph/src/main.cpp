#include "graph.h"
#include "order.h"
#include "filter.h"
#include "enumerate.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <cstring>

using Clock = std::chrono::steady_clock;
using Ms    = std::chrono::duration<double, std::milli>;

/* ── printUsage ─────────────────────────────────────────────────────── */
static void printUsage(const char* prog) {
    std::cerr
        << "Usage: " << prog
        << " <query_graph> <data_graph> [options]\n\n"
        << "Options:\n"
        << "  --sym          Enable rotational-symmetry compression in the\n"
        << "                 decision tree.  Only valid when phi is symmetric\n"
        << "                 under permutation of variables within each\n"
        << "                 consecutive same-type quantifier block.\n"
        << "  --limit <N>    Stop after N matches (default: unlimited).\n"
        << "  --help         Show this message.\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        printUsage(argv[0]);
        return 1;
    }

    /* ── Parse positional and optional arguments ─────────────────────── */
    const char* queryFile = argv[1];
    const char* dataFile  = argv[2];
    bool     sym          = false;
    uint64_t limit        = UINT64_MAX;

    for (int i = 3; i < argc; ++i) {
        if (std::strcmp(argv[i], "--sym") == 0) {
            sym = true;
        } else if (std::strcmp(argv[i], "--limit") == 0) {
            if (i + 1 >= argc) {
                std::cerr << "Error: --limit requires a value.\n";
                return 1;
            }
            ++i;
            limit = static_cast<uint64_t>(std::strtoull(argv[i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            printUsage(argv[0]);
            return 1;
        }
    }

    /* ── Load graphs ─────────────────────────────────────────────────── */
    auto t0 = Clock::now();
    QueryGraph query;
    query.load(queryFile);
    Graph data;
    data.load(dataFile);
    auto t1 = Clock::now();

    /* ── Filter ──────────────────────────────────────────────────────── */
    CandidateSet candidates;
    computeFilter(FilterStrategy::GQL, query, data, candidates);
    auto t2 = Clock::now();

    std::cout << "Load         : " << Ms(t1 - t0).count() << " ms\n";
    std::cout << "Filter (GQL) : " << Ms(t2 - t1).count() << " ms\n";
    std::cout << "query n=" << query.getNumVertices()
              << "  data n=" << data.getNumVertices()
              << "  m=" << data.getNumEdges() << "\n";
    std::cout << "Symmetry     : " << (sym ? "ON" : "OFF") << "\n\n";

    /* ── Define quantifiers and phi ──────────────────────────────────── *
     *                                                                     *
     * Edit this block to change the second-order constraint.             *
     * When sym=true, phi MUST be symmetric under permutation of         *
     * variables within each consecutive same-type quantifier block.     */
    const Quantifiers quants = { FORALL, FORALL, FORALL};

    auto phi = [](const int* a) {
        // return a[0] == a[1] || std::abs(a[0] - a[1]) > 2000;
        return a[0] == a[1] || a[0] == a[2] || a[1] == a[2] || (std::abs(a[0] - a[1]) > 200 && std::abs(a[1] - a[2]) > 200 && std::abs(a[0] - a[2]) > 200);
    };

    /* ── Build context and run ───────────────────────────────────────── */
    const uint32_t qn = query.getNumVertices();
    EnumContext ctx(data, query, candidates, quants, limit, /*staticOrd=*/{}, sym);

    if (sym) {
        std::cout << "DT nodes (compressed) : "
                  << ctx.decisionTree.total_nodes() << "\n\n";
    }

    auto t3 = Clock::now();
    backtrack(ctx, phi);
    auto t4 = Clock::now();

    std::cout << "Backtrack : " << Ms(t4 - t3).count() << " ms"
              << "  (" << ctx.matchCount << " matches)\n\n";

    /* ── Per-depth statistics table ──────────────────────────────────── */
    std::cout << std::setw(5)  << "depth"
              << std::setw(12) << "cands@start"
              << std::setw(12) << "probed"
              << std::setw(12) << "fwd_pruned"
              << std::setw(10) << "dt_pruned"
              << std::setw(12) << "recurse_in"
              << std::setw(14) << "narrow_Melems"
              << std::setw(10) << "order"
              << "\n" << std::string(87, '-') << "\n";

    for (uint32_t d = 0; d < qn; ++d) {
        VertexID u   = ctx.dynOrder[d];
        uint64_t rec = ctx.probeCount[d] - ctx.licmPruned[d] - ctx.dtPruned[d];
        std::cout << std::setw(5)  << d
                  << std::setw(12) << candidates[u].size()
                  << std::setw(12) << ctx.probeCount[d]
                  << std::setw(12) << ctx.licmPruned[d]
                  << std::setw(10) << ctx.dtPruned[d]
                  << std::setw(12) << rec
                  << std::setw(14) << std::fixed << std::setprecision(1)
                  << ctx.narrowWork[d] / 1e6
                  << std::setw(10) << (ctx.dtFirst[d] ? "DT→narr" : "narr→DT")
                  << "\n";
    }

    return 0;
}
