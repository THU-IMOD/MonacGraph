// src/cd.rs
use anyhow::Result;
use clap::Parser;
use lsm_storage::{LsmCommunity, LsmCommunityStorageOptions};
use std::time::Instant;

/// CD (Community Detection) benchmark for LSM graph storage
#[derive(Parser, Debug)]
#[command(name = "cd")]
#[command(about = "Run community detection and measure performance", long_about = None)]
struct Args {
    /// Graph name to load
    #[arg(short, long, default_value = "example")]
    graph: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Setup storage options
    let mut options = LsmCommunityStorageOptions::default();
    options.graph_name = args.graph.clone();

    let lsm_community = LsmCommunity::open(options)?;

    // Run Community Detection
    println!("Running Community Detection...");
    let cd_start = Instant::now();
    let communities = lsm_community.community_detection();
    let cd_time = cd_start.elapsed();
    println!("Running Community Detection - [OK]");

    // Compute statistics
    let total_vertices: usize = communities.iter().map(|c| c.len()).sum();
    let num_communities = communities.len();
    let largest_community = communities.iter().map(|c| c.len()).max().unwrap_or(0);
    let smallest_community = communities.iter().map(|c| c.len()).min().unwrap_or(0);
    let avg_community_size = if num_communities > 0 {
        total_vertices as f64 / num_communities as f64
    } else {
        0.0
    };

    // Report results
    println!("\n=== Community Detection Results ===");
    println!("Execution time: {:.2} ms", cd_time.as_secs_f64() * 1000.0);
    println!("Total vertices: {}", total_vertices);
    println!("Number of communities: {}", num_communities);
    println!("Largest community: {} vertices", largest_community);
    println!("Smallest community: {} vertices", smallest_community);
    println!("Average community size: {:.2} vertices", avg_community_size);

    Ok(())
}
