#include "graph.h"
#include "filter.h"
#include "enumerate.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <cstdlib>

using Clock = std::chrono::steady_clock;
using Ms    = std::chrono::duration<double, std::milli>;

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <query_graph> <data_graph>\n";
        return 1;
    }

    auto t0 = Clock::now();
    QueryGraph query;  query.load(argv[1]);
    Graph      data;   data.load(argv[2]);
    auto t1 = Clock::now();

    CandidateSet candidates;
    filterByGQL(query, data, candidates);
    auto t2 = Clock::now();

    std::cout << "Load         : " << Ms(t1-t0).count() << " ms\n";
    std::cout << "Filter (GQL) : " << Ms(t2-t1).count() << " ms\n";
    std::cout << "query n=" << query.getNumVertices()
              << "  data n=" << data.getNumVertices()
              << "  m=" << data.getNumEdges() << "\n\n";

    const Quantifiers quants = { FORALL, FORALL };
    auto phi = [](const int* a) {
        return a[0] == a[1] || std::abs(a[0] - a[1]) > 100;
        // return true;
    };

    const uint32_t qn = query.getNumVertices();
    EnumContext ctx(data, query, candidates, quants, 100000);

    auto t3 = Clock::now();
    backtrack(ctx, 0, phi);
    auto t4 = Clock::now();

    std::cout << "Backtrack : " << Ms(t4-t3).count() << " ms"
              << "  (" << ctx.matchCount << " matches)\n\n";

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
