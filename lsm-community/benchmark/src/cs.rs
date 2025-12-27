// src/cs.rs
use anyhow::Result;
use clap::Parser;
use lsm_storage::{LsmCommunity, LsmCommunityStorageOptions};
use rand::prelude::*;
use std::time::Instant;

/// CS (Community Search) benchmark for LSM graph storage
#[derive(Parser, Debug)]
#[command(name = "cs")]
#[command(about = "Search for vertex community membership and measure performance", long_about = None)]
struct Args {
    /// Vertex ID to search
    #[arg(short, long)]
    vertex_id: u32,

    /// Graph name to load
    #[arg(short, long, default_value = "example")]
    graph: String,

    /// Number of random samples to test (if > 1, vertex_id is ignored)
    #[arg(short = 'n', long, default_value_t = 1)]
    samples: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Setup storage options
    let mut options = LsmCommunityStorageOptions::default();
    options.graph_name = args.graph.clone();

    let lsm_community = LsmCommunity::open(options)?;

    let vertex_count = lsm_community.vertex_count() as u32;

    if args.samples == 1 {
        // Single community search
        println!("Searching community for vertex {}...", args.vertex_id);
        let cs_start = Instant::now();
        let community = lsm_community.community_search(args.vertex_id);
        let cs_time = cs_start.elapsed();

        match community {
            Some(members) => {
                println!("Searching community for vertex {} - [OK]", args.vertex_id);

                println!("\n=== Community Search Results ===");
                println!("Query vertex: {}", args.vertex_id);
                println!(
                    "Execution time: {:.2} μs",
                    cs_time.as_secs_f64() * 1_000_000.0
                );
                println!("Community size: {} vertices", members.len());
            }
            None => {
                println!(
                    "Searching community for vertex {} - [FAILED]",
                    args.vertex_id
                );
                println!("Error: Vertex {} not found (invalid ID)", args.vertex_id);
            }
        }
    } else {
        // Multiple community searches with random sampling
        println!(
            "Running Community Search with {} random samples...",
            args.samples
        );

        let mut rng = ThreadRng::default();
        let mut total_time = 0.0;
        let mut total_community_size = 0;
        let mut success_count = 0;
        let mut sample_results = Vec::new();

        for i in 0..args.samples {
            let sample_vertex = rng.random_range(0..vertex_count);

            let cs_start = Instant::now();
            let community = lsm_community.community_search(sample_vertex);
            let cs_time = cs_start.elapsed();

            let time_us = cs_time.as_secs_f64() * 1_000_000.0;

            match community {
                Some(members) => {
                    let community_size = members.len();
                    total_time += time_us;
                    total_community_size += community_size;
                    success_count += 1;

                    sample_results.push((sample_vertex, Some(community_size), time_us));

                    // Print individual results if samples < 10
                    if args.samples < 10 {
                        println!(
                            "Sample {}: vertex {} -> community size {} in {:.2} μs",
                            i + 1,
                            sample_vertex,
                            community_size,
                            time_us
                        );
                    }
                }
                None => {
                    sample_results.push((sample_vertex, None, time_us));

                    if args.samples < 10 {
                        println!(
                            "Sample {}: vertex {} -> NOT FOUND in {:.2} μs",
                            i + 1,
                            sample_vertex,
                            time_us
                        );
                    }
                }
            }
        }

        println!("\n=== Community Search Sampling Results ===");
        println!("Number of samples: {}", args.samples);
        println!(
            "Successful queries: {} ({:.2}%)",
            success_count,
            success_count as f64 / args.samples as f64 * 100.0
        );

        if success_count > 0 {
            println!(
                "Average execution time: {:.2} μs",
                total_time / success_count as f64
            );
            println!(
                "Average community size: {:.2}",
                total_community_size as f64 / success_count as f64
            );

            // Additional statistics
            let successful_times: Vec<f64> = sample_results
                .iter()
                .filter_map(|(_, opt_size, time)| {
                    if opt_size.is_some() {
                        Some(*time)
                    } else {
                        None
                    }
                })
                .collect();

            let successful_sizes: Vec<usize> = sample_results
                .iter()
                .filter_map(|(_, opt_size, _)| *opt_size)
                .collect();

            if !successful_times.is_empty() {
                let min_time = successful_times
                    .iter()
                    .fold(f64::INFINITY, |a, &b| a.min(b));
                let max_time = successful_times
                    .iter()
                    .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                let min_size = successful_sizes.iter().min().unwrap();
                let max_size = successful_sizes.iter().max().unwrap();

                println!(
                    "\nExecution time range: {:.2} μs - {:.2} μs",
                    min_time, max_time
                );
                println!("Community size range: {} - {}", min_size, max_size);
            }
        } else {
            println!("No successful queries found!");
        }
    }

    Ok(())
}
