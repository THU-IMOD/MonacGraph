#include "graph.h"
#include "filter.h"
#include "order.h"
#include "enumerate.h"
#include <iostream>
#include <chrono>

using Clock = std::chrono::steady_clock;

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <query_graph> <data_graph>\n";
        return 1;
    }

    QueryGraph query;
    query.load(argv[1]);
    Graph data;
    data.load(argv[2]);

    CandidateSet candidates;
    filterByGQL(query, data, candidates);
    Order order;
    orderByRI(query, order);

    const uint64_t TARGET = 1000;
    auto t0 = Clock::now();
    uint64_t found = enumerate(query, data, candidates, order, {FORALL, FORALL}, TARGET);
    auto t1 = Clock::now();
    double ms_limited = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::cout << "=== First " << TARGET << " matches ===\n";
    if (found < TARGET)
        std::cout << "  (graph only has " << found << " matches total)\n";
    std::cout << "  Time : " << ms_limited << " ms\n\n";

    auto t2 = Clock::now();
    uint64_t total = enumerate(query, data, candidates, order, {FORALL, FORALL});
    auto t3 = Clock::now();
    double ms_all = std::chrono::duration<double, std::milli>(t3 - t2).count();

    std::cout << "=== All matches ===\n";
    std::cout << "  Count : " << total  << "\n";
    std::cout << "  Time  : " << ms_all << " ms\n";

    return 0;
}
