# COSMO: A Community Structure Aware Graph Processing System

This repository contains the source code implementation of the paper "COSMO: A Community Structure Aware Graph Processing System".

## Overview

COSMO is a graph processing system that leverages community structure awareness to optimize graph computations. The system is designed to efficiently handle large-scale graph processing tasks by utilizing the inherent community structures present in real-world graphs.

## Building the System

### Prerequisites

- Rust (1.91.1 version recommended)
- Cargo (comes with Rust installation)

### Build Instructions

To build COSMO from source:

```bash
# Build the release version
cargo build --release
```

### Testing

To run the test suite:

```bash
cargo test
```

This will execute all unit tests and integration tests to ensure the system is working correctly.

## Usage

After building the system, you can run COSMO using the following command structure:

```bash
./target/release/cosmo --task <algorithm> --dataset <dataset_name>
```

### Example

To run the Breadth-First Search (BFS) algorithm on the example dataset:

```bash
./target/release/cosmo --task bfs --dataset example
```

### Available Options

- `--task`: Specifies the graph algorithm to run (e.g., `bfs`)
- `--dataset`: Specifies the dataset to process (e.g., `example`)