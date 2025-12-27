use anyhow::Result;
use clap::Parser;
use lsm_storage::{LsmCommunity, LsmCommunityStorageOptions};
use rand::prelude::*;
use std::time::Instant;

/// BFS (Breadth-First Search) benchmark for LSM graph storage
#[derive(Parser, Debug)]
#[command(name = "bfs")]
#[command(about = "Run BFS algorithm and measure performance", long_about = None)]
struct Args {
    /// Starting vertex ID for BFS traversal
    #[arg(short, long)]
    start_vertex: u32,

    /// Graph name to load
    #[arg(short, long, default_value = "example")]
    graph: String,

    /// Number of random samples to test (if > 1, start_vertex is ignored)
    #[arg(short = 'n', long, default_value_t = 1)]
    samples: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Setup storage options
    let mut options = LsmCommunityStorageOptions::default();
    options.graph_name = args.graph.clone();

    let lsm_community = LsmCommunity::open(options)?;
    lsm_community.warm_up()?;

    let vertex_count = lsm_community.vertex_count() as u32;

    if args.samples == 1 {
        // Single BFS run
        println!("Starting BFS from vertex {}...", args.start_vertex);
        let bfs_start = Instant::now();
        let bfs_result = lsm_community.bfs(args.start_vertex);
        let bfs_time = bfs_start.elapsed();
        println!("Starting BFS from vertex {} - [OK]", args.start_vertex);

        // Report results
        println!("\n=== BFS Results ===");
        println!("Start vertex: {}", args.start_vertex);
        println!("Execution time: {:.2} ms", bfs_time.as_secs_f64() * 1000.0);
        println!("Vertices visited: {}", bfs_result.len());
        println!(
            "Throughput: {:.2} vertices/ms",
            bfs_result.len() as f64 / (bfs_time.as_secs_f64() * 1000.0)
        );
    } else {
        // Multiple BFS runs with random sampling
        println!("Running BFS with {} random samples...", args.samples);

        let mut rng = ThreadRng::default();
        let mut total_time = 0.0;
        let mut total_vertices = 0;
        let mut sample_results = Vec::new();

        for i in 0..args.samples {
            let sample_vertex = rng.random_range(0..vertex_count);

            let bfs_start = Instant::now();
            let bfs_result = lsm_community.bfs(sample_vertex);
            let bfs_time = bfs_start.elapsed();

            let time_ms = bfs_time.as_secs_f64() * 1000.0;
            let visited = bfs_result.len();

            total_time += time_ms;
            total_vertices += visited;

            sample_results.push((sample_vertex, visited, time_ms));

            // Print individual results if samples < 10
            if args.samples < 10 {
                println!(
                    "Sample {}: vertex {} -> {} vertices visited in {:.2} ms",
                    i + 1,
                    sample_vertex,
                    visited,
                    time_ms
                );
            }
        }

        println!("\n=== BFS Sampling Results ===");
        println!("Number of samples: {}", args.samples);
        println!(
            "Average execution time: {:.2} ms",
            total_time / args.samples as f64
        );
        println!(
            "Average vertices visited: {:.2}",
            total_vertices as f64 / args.samples as f64
        );
        println!(
            "Average throughput: {:.2} vertices/ms",
            total_vertices as f64 / total_time
        );

        // Additional statistics
        let min_time = sample_results
            .iter()
            .map(|(_, _, t)| t)
            .fold(f64::INFINITY, |a, &b| a.min(b));
        let max_time = sample_results
            .iter()
            .map(|(_, _, t)| t)
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let min_visited = sample_results.iter().map(|(_, v, _)| v).min().unwrap();
        let max_visited = sample_results.iter().map(|(_, v, _)| v).max().unwrap();

        println!(
            "\nExecution time range: {:.2} ms - {:.2} ms",
            min_time, max_time
        );
        println!("Vertices visited range: {} - {}", min_visited, max_visited);
    }

    Ok(())
}
