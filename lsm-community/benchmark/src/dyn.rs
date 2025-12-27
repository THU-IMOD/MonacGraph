use anyhow::Result;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use lsm_storage::{LsmCommunity, LsmCommunityStorageOptions};
use rand::prelude::*;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// Edge insertion benchmark for LSM graph storage
#[derive(Parser, Debug)]
#[command(name = "edge_insertion")]
#[command(about = "Benchmark edge insertion performance with multi-threading", long_about = None)]
struct Args {
    /// Number of edges to insert
    #[arg(short = 'n', long)]
    num_edges: usize,

    /// Graph name to load
    #[arg(short, long, default_value = "example")]
    graph: String,

    /// Number of threads to use (default: use all available cores)
    #[arg(short, long)]
    threads: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Setup storage options
    let mut options = LsmCommunityStorageOptions::default();
    options.graph_name = args.graph.clone();

    let lsm_community = Arc::new(LsmCommunity::open(options)?);

    let vertex_count = lsm_community.vertex_count() as u32;
    println!("Graph loaded: {} vertices", vertex_count);

    // Determine number of threads
    let num_threads = args.threads.unwrap_or_else(|| {
        let cores = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        cores
    });

    println!("Using {} threads", num_threads);
    println!("Inserting {} edges...\n", args.num_edges);

    // Calculate edges per thread
    let edges_per_thread = args.num_edges / num_threads;
    let remainder = args.num_edges % num_threads;

    // Setup multi-progress bar
    let multi_progress = Arc::new(MultiProgress::new());

    // Create overall progress bar
    let overall_pb = multi_progress.add(ProgressBar::new(args.num_edges as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.set_message("Overall progress");

    let start_time = Instant::now();

    // Spawn threads
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let lsm = Arc::clone(&lsm_community);
            let overall_pb = overall_pb.clone();
            let mut edges_to_insert = edges_per_thread;

            // First thread handles remainder
            if thread_id == 0 {
                edges_to_insert += remainder;
            }

            thread::spawn(move || {
                let mut rng = ThreadRng::default();
                let mut success_count = 0;
                let mut error_count = 0;

                for _ in 0..edges_to_insert {
                    let src = rng.random_range(0..vertex_count);
                    let dst = rng.random_range(0..vertex_count);

                    match lsm.insert_edge(src, dst) {
                        Ok(_) => success_count += 1,
                        Err(_) => error_count += 1,
                    }

                    // Update progress bar
                    overall_pb.inc(1);
                }

                (success_count, error_count)
            })
        })
        .collect();

    // Wait for all threads to complete
    let mut total_success = 0;
    let mut total_errors = 0;

    for handle in handles.into_iter() {
        let (success, errors) = handle.join().expect("Thread panicked");
        total_success += success;
        total_errors += errors;
    }

    overall_pb.finish_with_message("Completed!");

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    // Print results
    println!("\n=== Edge Insertion Benchmark Results ===");
    println!("Total edges attempted: {}", args.num_edges);
    println!("Successfully inserted: {}", total_success);
    println!("Errors: {}", total_errors);
    println!("Total time: {:.2} seconds", elapsed_secs);
    println!(
        "Throughput: {:.2} edges/second",
        total_success as f64 / elapsed_secs
    );
    println!(
        "Average latency: {:.2} µs/edge",
        (elapsed_secs * 1_000_000.0) / total_success as f64
    );

    Ok(())
}
