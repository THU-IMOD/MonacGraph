use clap::Parser;
use cosmo::algorithms::analysis::boundary_analysis;
use cosmo::algorithms::bfs::{BFSConfig, BFSController, BFS};
use cosmo::algorithms::path::{PathConfig, PathController};
use cosmo::algorithms::scc::{SCCConfig, SCCController};
use cosmo::algorithms::wcc::{WCCConfig, WCCController, WCC};
use cosmo::comm_io::CommunityStorage;
use cosmo::io_status::IOStatsCollector;
use cosmo::measure_io;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug, Serialize)]
#[command(author, version, about)]
struct Args {
    /// Base path of data graph files (.edges and .labels)
    #[arg(short, long, default_value_t = String::from("example"))]
    dataset: String,

    /// The task to be performed.
    #[arg(short, long, default_value_t = String::from("bfs"))]
    task: String,

    /// Approach to be tested.
    #[arg(short, long, default_value_t = String::from("community"))]
    storage: String,

    /// Repeat count.
    #[arg(short, long, default_value_t = 4)]
    num_threads: usize,

    /// Source vertex id.
    #[arg(short, long, default_value_t = 0)]
    source_vertex: u64,

    /// Dest vertex id.
    #[arg(short, long, default_value_t = 10000)]
    destination_vertex: u64,

    /// Giant community theta value.
    #[arg(short, long, default_value_t = 0.0001)]
    giant_theta: f64,
}

fn main() {
    let args: Args = Args::parse();
    let graph_name = args.dataset;
    let giant_theta = args.giant_theta;
    let thread_num = args.num_threads;
    let task = args.task;
    let start_vertex = args.source_vertex;

    // Step 1: Build the community storage from the graph file
    // The community storage is constructed using a threshold value of 0.5 for community size
    let comm_storage = match CommunityStorage::build_from_index_file(&graph_name) {
        None => {
            println!("Build Storage Engine from the sketch for {}.", graph_name);
            CommunityStorage::build_from_graph_file_opt_par(
                &format!("data/{}.graph", graph_name),
                &graph_name,
                giant_theta,
            )
        }
        Some(storage_engine) => {
            println!("Rebuild Index for {}.", graph_name);
            storage_engine
        }
    };

    // Step 2. Perform each task, and report the time.
    if task == "bfs" {
        let bfs_controller = BFSController::new(Arc::new(comm_storage));
        // Start a timer to measure execution time
        let start = Instant::now();

        // Configure BFS with a single thread and no external visit tracking
        let bfs_config = BFSConfig {
            thread_num,
            global_visit: None,
        };

        // Perform BFS starting from vertex `start vertex`.
        let _visited_vertex_list = bfs_controller.bfs(&start_vertex, bfs_config);

        // Calculate the elapsed time
        let duration = start.elapsed();

        // Output results: number of vertices visited and execution time
        println!("BFS Elapsed Time: {:?} us", duration.as_micros());
    } else if task == "wcc" {
        // Create a BFS controller with the community storage
        let wcc_controller = WCCController::new(Arc::new(comm_storage));

        // Start a timer to measure execution time
        let start = Instant::now();

        // Configure BFS with a single thread and no external visit tracking
        let wcc_config = WCCConfig { thread_num };

        let _wcc = wcc_controller.wcc(wcc_config);

        // Calculate the elapsed time
        let duration = start.elapsed();
        println!("WCC Elapsed Time: {:?} us", duration.as_micros());
    } else if task == "wccn" {
        // Create a BFS controller with the community storage
        let wcc_controller = WCCController::new(Arc::new(comm_storage));

        // Start a timer to measure execution time
        let start = Instant::now();

        let _wcc = wcc_controller.count_wcc();

        // Calculate the elapsed time
        let duration = start.elapsed();
        println!("WCC Elapsed Time: {:?} us", duration.as_micros());
    } else if task == "scc" {
        let scc_controller = SCCController::new(Arc::new(comm_storage));
        let start_time = Instant::now();
        let scc_config = SCCConfig { thread_num };
        let _result_scc = scc_controller.scc(scc_config);
        let duration = start_time.elapsed();
        println!("SCC Elapsed Time: {:?} us", duration.as_micros());
    } else if task == "analysis" {
        println!(
            "Dataset: {}, Vertex Count: {}, boundary count: {}",
            graph_name,
            comm_storage.vertex_count,
            boundary_analysis(&comm_storage)
        );
    } else if task == "path" {
        let path_controller = PathController::new(Arc::new(comm_storage));
        let start_time = Instant::now();
        let path_config = PathConfig { thread_num };
        let dst_vertex = args.destination_vertex;
        let _result_path = measure_io!("PATH_IO", {
            path_controller.path(&start_vertex, &dst_vertex, path_config)
        });
        let duration = start_time.elapsed();
        println!("Path Elapsed Time: {:?} us", duration.as_micros());
    } else {
        println!("Task {} not supported in COSMO.", task);
    }
}
