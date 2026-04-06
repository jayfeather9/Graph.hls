#include "common.h"
#include "fpga_executor.h"
#include "graph_loader.h"
#include "graph_preprocess/graph_preprocess.h"
#include "host_verifier.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstring>

void analyze_performance(const GraphCSR &graph) {
    const double FREQUENCY_MHZ = 220.0;
    const double FREQUENCY_HZ = FREQUENCY_MHZ * 1e6;
    
    std::cout << "\n=== Performance Analysis Mode ===" << std::endl;
    std::cout << "Frequency: " << FREQUENCY_MHZ << " MHz" << std::endl;
    
    // Partition the graph
    PartitionContainer partition_container = partitionGraph(&graph);
    
    double max_little_time_sec = 0.0;
    double max_big_time_sec = 0.0;
    
    // Analyze Little Pipelines
    std::cout << "\n--- Little Pipeline Analysis ---" << std::endl;
    for (int i = 0; i < partition_container.num_dense_partitions; ++i) {
        const auto &little_partition = partition_container.DPs[i];
        std::cout << "\nLittle Partition " << i << ":" << std::endl;
        std::cout << "  Total vertices: " << little_partition.num_vertices << std::endl;
        std::cout << "  Total edges: " << little_partition.num_edges << std::endl;
        std::cout << "  Destination nodes: " << little_partition.num_dsts << std::endl;
        
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            const auto &pipeline_edges = little_partition.pipeline_edges[pip];
            int E = pipeline_edges.num_edges;
            int N = little_partition.num_dsts;
            
            // Cycle count: E/8 + N/16
            double cycles = (double)E / 8.0 + (double)N / 16.0;
            double time_sec = cycles / FREQUENCY_HZ;
            double time_ms = time_sec * 1000.0;
            
            max_little_time_sec = std::max(max_little_time_sec, time_sec);
            
            std::cout << "  Pipeline " << pip << ":" << std::endl;
            std::cout << "    Edges: " << E << std::endl;
            std::cout << "    Cycles: " << cycles << std::endl;
            std::cout << "    Time: " << time_ms << " ms" << std::endl;
            if (E > 0) {
                double mteps = (double)E / time_sec / 1.0e6;
                std::cout << "    Throughput: " << mteps << " MTEPS" << std::endl;
            }
        }
    }
    
    // Analyze Big Pipelines
    std::cout << "\n--- Big Pipeline Analysis ---" << std::endl;
    for (int i = 0; i < partition_container.num_sparse_partitions; ++i) {
        const auto &big_partition = partition_container.SPs[i];
        std::cout << "\nBig Partition " << i << ":" << std::endl;
        std::cout << "  Total vertices: " << big_partition.num_vertices << std::endl;
        std::cout << "  Total edges: " << big_partition.num_edges << std::endl;
        std::cout << "  Destination nodes: " << big_partition.num_dsts << std::endl;
        
        // Analyze each pipeline separately
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            const auto &pipeline_edges = big_partition.pipeline_edges[pip];
            
            std::cout << "\n  Pipeline " << pip << ":" << std::endl;
            std::cout << "    Total edges: " << pipeline_edges.num_edges << std::endl;
            
            // For this pipeline, analyze PE distribution using omega network
            // Last 3 bits of dst_id determine which PE (0-7)
            std::vector<int> pe_edge_counts(PE_NUM, 0);
            
            // Count edges per PE based on destination's last 3 bits
            for (int edge_idx = 0; edge_idx < pipeline_edges.columns.size(); ++edge_idx) {
                int dst_id = pipeline_edges.columns[edge_idx];
                if (dst_id != 0x7FFFFFFF) { // Skip dummy edges
                    int pe_id = dst_id & 0x7; // Last 3 bits
                    pe_edge_counts[pe_id]++;
                }
            }
            
            // Find max and min edges per PE for this pipeline
            int max_edges_per_pe = 0;
            int min_edges_per_pe = INT_MAX;
            for (int pe = 0; pe < PE_NUM; ++pe) {
                std::cout << "      PE " << pe << ": " << pe_edge_counts[pe] << " edges" << std::endl;
                max_edges_per_pe = std::max(max_edges_per_pe, pe_edge_counts[pe]);
                min_edges_per_pe = std::min(min_edges_per_pe, pe_edge_counts[pe]);
            }
            
            // Output ratio for each PE compared to min PE
            if (min_edges_per_pe > 0) {
                std::cout << "    PE Load Ratios (compared to min PE):" << std::endl;
                for (int pe = 0; pe < PE_NUM; ++pe) {
                    double ratio = (double)pe_edge_counts[pe] / (double)min_edges_per_pe;
                    std::cout << "      PE " << pe << ": " << ratio << "x" << std::endl;
                }
            }
            
            int N = big_partition.num_dsts;
            
            // Cycle count: ME + N/16
            double cycles = (double)max_edges_per_pe + (double)N / 16.0;
            double time_sec = cycles / FREQUENCY_HZ;
            double time_ms = time_sec * 1000.0;
            
            max_big_time_sec = std::max(max_big_time_sec, time_sec);
            
            std::cout << "    Max edges per PE: " << max_edges_per_pe << std::endl;
            std::cout << "    Min edges per PE: " << min_edges_per_pe << std::endl;
            std::cout << "    Cycles: " << cycles << std::endl;
            std::cout << "    Time: " << time_ms << " ms" << std::endl;
            if (pipeline_edges.num_edges > 0) {
                double mteps = (double)pipeline_edges.num_edges / time_sec / 1.0e6;
                std::cout << "    Throughput: " << mteps << " MTEPS" << std::endl;
            }
        }
    }
    
    // Calculate total estimated time and MTEPS
    double total_estimated_time_sec = std::max(max_little_time_sec, max_big_time_sec);
    double total_estimated_time_ms = total_estimated_time_sec * 1000.0;
    double total_mteps = 0.0;
    if (total_estimated_time_sec > 0) {
        total_mteps = (double)graph.num_edges / total_estimated_time_sec / 1.0e6;
    }
    
    std::cout << "\n=== Overall Performance Summary ===" << std::endl;
    std::cout << "Max Little Pipeline Time: " << (max_little_time_sec * 1000.0) << " ms" << std::endl;
    std::cout << "Max Big Pipeline Time: " << (max_big_time_sec * 1000.0) << " ms" << std::endl;
    std::cout << "Total Estimated Time: " << total_estimated_time_ms << " ms" << std::endl;
    std::cout << "Total Estimated Throughput: " << total_mteps << " MTEPS" << std::endl;
    
    std::cout << "\n=== Analysis Complete ===" << std::endl;
}


int main(int argc, char **argv) {
    bool analyze_mode = false;
    std::string xclbin_file;
    std::string graph_file;
    
    // Parse command line arguments
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <xclbin_file> <graph_data_file> [--analyze] [--repartition-by-largest-partition]" << std::endl;
        return EXIT_FAILURE;
    }
    
    xclbin_file = argv[1];
    graph_file = argv[2];
    
    // Check for flags
    FpgaRunOptions run_options;
    for (int i = 3; i < argc; ++i) {
        if (strcmp(argv[i], "--analyze") == 0) {
            analyze_mode = true;
        } else if (strcmp(argv[i], "--repartition-by-largest-partition") == 0) {
            run_options.repartition_with_iter0_profile = true;
        }
    }

    int start_node = 0;

    // 1. Load graph data
    std::cout << "--- Step 1: Loading Graph Data ---" << std::endl;
    GraphCSR graph = load_graph_from_file(graph_file);
    if (graph.num_vertices == 0) {
        return EXIT_FAILURE;
    }

    if (analyze_mode) {
        // Analysis mode: only partition and analyze performance
        analyze_performance(graph);
        return EXIT_SUCCESS;
    }

    // Normal FPGA execution mode
    // 2. Run on FPGA
    std::cout << "\n--- Step 2: Running on FPGA ---" << std::endl;
    double total_kernel_time_sec = 0;
    int iter_count = 0;
    std::vector<int> fpga_distances = run_fpga_kernel(
        xclbin_file, graph, start_node, total_kernel_time_sec, iter_count,
        run_options);

    // 3. Verify on Host CPU
    std::cout << "\n--- Step 3: Verifying on Host CPU ---" << std::endl;
    std::vector<int> host_distances =
        verify_on_host(graph, start_node);

    // 4. Compare results
    std::cout << "\n--- Step 4: Comparing Results ---" << std::endl;
    int error_count = 0;
    for (int i = 0; i < graph.num_vertices; ++i) {
        if (fpga_distances[i] != host_distances[i]) {
            if (error_count < 10) {
                std::cout << "Mismatch at vertex " << i << ": "
                          << "FPGA_Result = " << fpga_distances[i] << ", "
                          << "Host_Result = " << host_distances[i] << std::endl;
            }
            error_count++;
        }
    }

    // 5. Final report
    std::cout << "\n--- Final Report ---" << std::endl;
    if (error_count == 0) {
        std::cout << "SUCCESS: Results match!" << std::endl;
    } else {
        std::cout << "FAILURE: Found " << error_count << " mismatches."
                  << std::endl;
    }

    std::cout << "Total FPGA Kernel Execution Time: "
              << total_kernel_time_sec * 1000.0 << " ms" << std::endl;
    std::cout << "Total MTEPS (Edges / Total Time): "
              << ((double)graph.num_edges * iter_count) /
                     total_kernel_time_sec / 1.0e6
              << " MTEPS" << std::endl;

    return (error_count == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
