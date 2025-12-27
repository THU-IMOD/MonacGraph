# LSM Community

A high-performance graph storage system based on community structures, applying LSM-tree (Log-Structured Merge-tree) concepts to graph data storage. The system is designed to handle massive graph datasets efficiently using a Paged CSR (Compressed Sparse Row) format with support for fast queries, insertions, and graph algorithm execution.

## Getting Started

### Building the Project

Build the benchmark tools:
```bash
cargo build -p benchmark --release
```

Build the entire project:
```bash
cargo build --release
```

### Running Benchmarks

Run BFS (Breadth-First Search) benchmark:
```bash
./target/release/bfs --start-vertex 0 --graph example
```

Run BFS with random sampling:
```bash
./target/release/bfs --graph example --samples 100
```

Run WCC (Weakly Connected Components) benchmark:
```bash
./target/release/wcc --graph example
```

Run edge insertion benchmark:
```bash
./target/release/edge_insertion -n 10 --graph example
```

### Running Tests

Run all tests:
```bash
cargo test
```

Run specific test:
```bash
cargo test test_edge_insertion_with_delta
```

Run tests with output:
```bash
cargo test -- --nocapture
```

### Cleanup

Clean up temporary files and build artifacts:
```bash
bash ./tools/clean.sh
```

## Current Version

This is a simplified version focusing on core storage and basic graph algorithms. Subgraph matching algorithms and dynamic community maintenance mechanisms are under development as part of ongoing research work and will be integrated in future releases.

## Roadmap

In recent, the system will be extended to support Gremlin query language with second-order logic capabilities, enabling more expressive graph traversal and pattern queries.