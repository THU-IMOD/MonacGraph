// src/wcc.rs
use anyhow::Result;
use clap::Parser;
use lsm_storage::{LsmCommunity, LsmCommunityStorageOptions};
use std::time::Instant;

/// WCC (Weakly Connected Components) benchmark for LSM graph storage
#[derive(Parser, Debug)]
#[command(name = "wcc")]
#[command(about = "Compute weakly connected components and measure performance", long_about = None)]
struct Args {
    /// Graph name to load
    #[arg(short, long, default_value = "sd")]
    graph: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Setup storage options
    let mut options = LsmCommunityStorageOptions::default();
    options.graph_name = args.graph.clone();

    let lsm_community = LsmCommunity::open(options)?;
    lsm_community.warm_up()?;

    // Run WCC
    println!("Computing Weakly Connected Components...");
    let wcc_start = Instant::now();
    let wcc_result = lsm_community.wcc();
    let wcc_time = wcc_start.elapsed();
    println!("Computing Weakly Connected Components - [OK]");

    // Compute statistics
    let mut component_sizes = std::collections::HashMap::new();
    for &comp_id in &wcc_result {
        *component_sizes.entry(comp_id).or_insert(0) += 1;
    }

    let num_components = component_sizes.len();

    // Report results
    println!("\n=== WCC Results ===");
    println!("Execution time: {:.2} ms", wcc_time.as_secs_f64() * 1000.0);
    println!("Total vertices: {}", wcc_result.len());
    println!("Number of components: {}", num_components);

    Ok(())
}
