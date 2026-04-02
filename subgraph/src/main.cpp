#include "graph.h"
#include "order.h"
#include "filter.h"
#include "enumerate.h"
#include "symmetry.h"
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
        << "  --filter  LDF|NLF|GQL|DPISO|CFL\n"
        << "                 Candidate filter strategy (default: GQL)\n"
        << "  --order   DYNAMIC|GQL|RI|DEGREE|DP\n"
        << "                 Vertex ordering strategy (default: DYNAMIC)\n"
        << "  --ablation FULL|NO_DT|NO_ADAPT\n"
        << "                 Ablation mode (default: FULL)\n"
        << "  --dt-sym       Enable rotational-symmetry compression in the\n"
        << "                 decision tree.  Only valid when phi is symmetric\n"
        << "                 under permutation of variables within each\n"
        << "                 consecutive same-type quantifier block.\n"
        << "  --sym-break    Enable automorphism-based symmetry breaking.\n"
        << "  --limit <N>    Stop after N matches (default: unlimited).\n"
        << "  --print        Print each match to stdout.\n"
        << "  --help         Show this message.\n";
}

/* ================================================================== *
 *  USER-DEFINED SECTION                                                *
 *                                                                      *
 *  Edit ONLY this section to change the logical formula.             *
 *                                                                      *
 *  USER_QUANTIFIERS  — one entry per quantifier variable (k entries) *
 *  userPhi           — the second-order constraint                    *
 *                      coords[j] = data vertex ID for variable j     *
 *  USE_DT_SYM        — set true when phi is invariant under any      *
 *                      permutation of its quantifier variables AND    *
 *                      all quantifiers share the same type.           *
 *                      Enables compressed decision-tree layout.       *
 * ================================================================== */

static const Quantifiers USER_QUANTIFIERS = { FORALL, FORALL, FORALL };

static bool userPhi(const int* a) {
    return true;
    return a[0] == a[1] || a[0] == a[2] || a[1] == a[2]
        || (std::abs(a[0] - a[1]) > 200
         && std::abs(a[1] - a[2]) > 200
         && std::abs(a[0] - a[2]) > 200);
    // return true;  // pure structural subgraph isomorphism
}

/* ================================================================== *
 *  END OF USER-DEFINED SECTION                                        *
 * ================================================================== */

int main(int argc, char* argv[]) {
    if (argc < 3) {
        printUsage(argv[0]);
        return 1;
    }

    /* ── Parse positional and optional arguments ─────────────────────── */
    const char*     queryFile  = argv[1];
    const char*     dataFile   = argv[2];
    FilterStrategy  filterStrat = FilterStrategy::GQL;
    OrderStrategy   orderStrat  = OrderStrategy::DYNAMIC;
    AblationMode    ablation    = AblationMode::FULL;
    bool            sym         = false;   // DT rotational-symmetry compression
    bool            symBreak    = false;   // automorphism symmetry breaking
    uint64_t        limit       = UINT64_MAX;
    bool            printMatch  = false;

    for (int i = 3; i < argc; ++i) {
        if (std::strcmp(argv[i], "--filter") == 0) {
            if (i + 1 >= argc) { std::cerr << "--filter requires a value.\n"; return 1; }
            filterStrat = filterStrategyFromString(argv[++i]);

        } else if (std::strcmp(argv[i], "--order") == 0) {
            if (i + 1 >= argc) { std::cerr << "--order requires a value.\n"; return 1; }
            orderStrat = orderStrategyFromString(argv[++i]);

        } else if (std::strcmp(argv[i], "--ablation") == 0) {
            if (i + 1 >= argc) { std::cerr << "--ablation requires a value.\n"; return 1; }
            std::string s = argv[++i];
            if      (s == "FULL")     ablation = AblationMode::FULL;
            else if (s == "NO_DT")    ablation = AblationMode::NO_DT;
            else if (s == "NO_ADAPT") ablation = AblationMode::NO_ADAPT;
            else { std::cerr << "Unknown ablation mode: " << s << "\n"; return 1; }

        } else if (std::strcmp(argv[i], "--dt-sym") == 0) {
            sym = true;

        } else if (std::strcmp(argv[i], "--sym-break") == 0) {
            symBreak = true;

        } else if (std::strcmp(argv[i], "--limit") == 0) {
            if (i + 1 >= argc) { std::cerr << "--limit requires a value.\n"; return 1; }
            limit = static_cast<uint64_t>(std::strtoull(argv[++i], nullptr, 10));

        } else if (std::strcmp(argv[i], "--print") == 0) {
            printMatch = true;

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
    QueryGraph query;  query.load(queryFile);
    Graph      data;   data.load(dataFile);
    auto t1 = Clock::now();

    /* ── Filter ──────────────────────────────────────────────────────── */
    CandidateSet candidates;
    computeFilter(filterStrat, query, data, candidates);
    auto t2 = Clock::now();

    /* ── Order ───────────────────────────────────────────────────────── */
    Order order;
    {
        std::vector<uint32_t> candCount(query.getNumVertices());
        for (uint32_t u = 0; u < query.getNumVertices(); ++u)
            candCount[u] = static_cast<uint32_t>(candidates[u].size());

        if (orderStrat == OrderStrategy::DP)
            computeOrder(orderStrat, query, data, candidates, order);
        else
            computeOrder(orderStrat, query, candCount, order);
    }
    auto t3 = Clock::now();

    /* ── Symmetry breaking ───────────────────────────────────────────── */
    std::unordered_map<VertexID,
        std::pair<std::set<VertexID>, std::set<VertexID>>> symCons;
    uint64_t                           autGroupSize = 1;
    std::vector<std::vector<VertexID>> autGens;

    if (symBreak) {
        std::vector<std::vector<VertexID>> perms;
        auto cosets = SymBreak::computeOrbits(query, perms);
        std::vector<std::pair<VertexID, VertexID>> cons;
        SymBreak::makeConstraints(cosets, cons);
        SymBreak::makeFullConstraints(cons, symCons);
        autGroupSize = SymBreak::computeAutGroupSize(cosets);
        autGens      = std::move(perms);
    }
    auto t4 = Clock::now();

    /* ── Print configuration ─────────────────────────────────────────── */
    std::cout << "Load         : " << Ms(t1 - t0).count() << " ms\n"
              << "Filter (" << filterStrategyName(filterStrat) << ")  : "
              << Ms(t2 - t1).count() << " ms\n"
              << "Order (" << orderStrategyName(orderStrat) << ")   : "
              << Ms(t3 - t2).count() << " ms\n";
    if (symBreak)
        std::cout << "SymBreak     : " << Ms(t4 - t3).count() << " ms"
                  << "  |Aut|=" << autGroupSize
                  << "  gens=" << autGens.size() << "\n";
    std::cout << "query n=" << query.getNumVertices()
              << "  data n=" << data.getNumVertices()
              << "  m=" << data.getNumEdges() << "\n"
              << "DT-sym       : " << (sym      ? "ON" : "OFF") << "\n"
              << "Sym-break    : " << (symBreak ? "ON" : "OFF") << "\n"
              << "Ablation     : " << (ablation == AblationMode::FULL     ? "FULL"
                                     : ablation == AblationMode::NO_DT    ? "NO_DT"
                                                                          : "NO_ADAPT")
              << "\n\n";

    /* ── Build context ───────────────────────────────────────────────── */
    const uint32_t qn = query.getNumVertices();
    EnumContext ctx(data, query, candidates, USER_QUANTIFIERS, limit,
                    order, sym,
                    std::move(symCons), autGroupSize, std::move(autGens));

    if (sym)
        std::cout << "DT nodes (compressed) : "
                  << ctx.decisionTree.total_nodes() << "\n\n";

    if (printMatch) {
        ctx.onMatch = [](const VertexID* matched,
                         const VertexID* dynOrder,
                         uint32_t n) {
            for (uint32_t d = 0; d < n; ++d) {
                if (d) std::cout << ' ';
                std::cout << dynOrder[d] << "->" << matched[d];
            }
            std::cout << '\n';
        };
    }

    /* ── Wrap userPhi and dispatch to backtrack variant ──────────────── */
    auto phi = [](const int* a) noexcept -> bool { return userPhi(a); };

    auto t5 = Clock::now();
    switch (ablation) {
        case AblationMode::FULL:     backtrack(         ctx, phi); break;
        case AblationMode::NO_DT:    backtrack_no_dt(   ctx, phi); break;
        case AblationMode::NO_ADAPT: backtrack_no_adapt(ctx, phi); break;
    }
    auto t6 = Clock::now();

    std::cout << "Backtrack : " << Ms(t6 - t5).count() << " ms"
              << "  (" << ctx.matchCount << " matches)\n\n";

    /* ── Per-depth statistics table ──────────────────────────────────── */
    std::cout << std::setw(5)  << "depth"
              << std::setw(12) << "cands@start"
              << std::setw(12) << "probed"
              << std::setw(12) << "fwd_pruned"
              << std::setw(10) << "dt_pruned"
              << std::setw(10) << "sym_pruned"
              << std::setw(12) << "recurse_in"
              << std::setw(14) << "narrow_Melems"
              << std::setw(10) << "order"
              << "\n" << std::string(97, '-') << "\n";

    for (uint32_t d = 0; d < qn; ++d) {
        VertexID u   = ctx.dynOrder[d];
        uint64_t rec = ctx.probeCount[d] - ctx.licmPruned[d] - ctx.dtPruned[d];
        std::cout << std::setw(5)  << d
                  << std::setw(12) << candidates[u].size()
                  << std::setw(12) << ctx.probeCount[d]
                  << std::setw(12) << ctx.licmPruned[d]
                  << std::setw(10) << ctx.dtPruned[d]
                  << std::setw(10) << ctx.symPruned[d]
                  << std::setw(12) << rec
                  << std::setw(14) << std::fixed << std::setprecision(1)
                  << ctx.narrowWork[d] / 1e6
                  << std::setw(10) << (ctx.dtFirst[d] ? "DT→narr" : "narr→DT")
                  << "\n";
    }

    return 0;
}
