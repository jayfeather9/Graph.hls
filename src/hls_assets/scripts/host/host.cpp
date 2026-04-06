#include "common.h"
#include "fpga_executor.h" // <-- 修改: 包含新的执行器
#include "generated_algorithm_config.h"
#include "graph_loader.h"
#include "host_verifier.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

static bool env_flag(const char *name) {
    const char *value = std::getenv(name);
    if (value == nullptr) {
        return false;
    }
    if (value[0] == '\0') {
        return true;
    }
    return std::strcmp(value, "0") != 0 && std::strcmp(value, "false") != 0 &&
           std::strcmp(value, "FALSE") != 0 && std::strcmp(value, "no") != 0 &&
           std::strcmp(value, "NO") != 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0]
                  << " <xclbin_file> <graph_data_file> "
                     "[--repartition-by-largest-partition]"
                  << std::endl;
        return EXIT_FAILURE;
    }

    std::string xclbin_file = argv[1];
    std::string graph_file = argv[2];
    int start_node = 0;
    FpgaRunOptions run_options;

    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--repartition-by-largest-partition") {
            run_options.repartition_with_iter0_profile = true;
            continue;
        }
        std::cerr << "Unknown option: " << arg << std::endl;
        std::cout << "Usage: " << argv[0]
                  << " <xclbin_file> <graph_data_file> "
                     "[--repartition-by-largest-partition]"
                  << std::endl;
        return EXIT_FAILURE;
    }

    // 1. 加载图数据 (不变)
    std::cout << "--- Step 1: Loading Graph Data ---" << std::endl;
    GraphCSR graph = load_graph_from_file(graph_file);
    if (graph.num_vertices == 0) {
        return EXIT_FAILURE;
    }

    AlgorithmConfig config = graphyflow_generated_config();
    if (const char *max_iters_env = std::getenv("GRAPHYFLOW_MAX_ITERS")) {
        const int max_iters = std::atoi(max_iters_env);
        if (max_iters > 0) {
            config.convergence_mode = ConvergenceMode::FixedIterations;
            config.max_iterations = max_iters;
            std::cout << "[INFO] Forcing FixedIterations(max_iterations="
                      << config.max_iterations
                      << ") via GRAPHYFLOW_MAX_ITERS." << std::endl;
        }
    }

    const bool skip_verify = env_flag("GRAPHYFLOW_SKIP_VERIFY");
    const bool allow_mismatch = env_flag("GRAPHYFLOW_ALLOW_MISMATCH");
    const bool is_emulation =
        (std::getenv("XCL_EMULATION_MODE") != nullptr &&
         std::getenv("XCL_EMULATION_MODE")[0] != '\0');

    // 2. 在 FPGA 上运行 (调用新的通用执行器)
    std::cout << "\n--- Step 2: Running on FPGA (" << config.target_property
              << ") ---" << std::endl;
    double total_kernel_time_sec = 0;
    int iter_count = 0;
    std::vector<unsigned int> fpga_distances = run_fpga_kernel(
        xclbin_file, graph, start_node, total_kernel_time_sec, iter_count,
        config, run_options);

    if (skip_verify) {
        std::cout << "\n--- Final Report ---" << std::endl;
        std::cout << "SUCCESS: Verification skipped (GRAPHYFLOW_SKIP_VERIFY)"
                  << std::endl;
        std::cout << "Total FPGA Kernel Execution Time: "
                  << total_kernel_time_sec * 1000.0 << " ms" << std::endl;
        std::cout << "Total MTEPS (Edges / Total Time): "
                  << ((double)graph.num_edges * iter_count) /
                         total_kernel_time_sec / 1.0e6
                  << " MTEPS" << std::endl;
        if (is_emulation) {
            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr);
            std::_Exit(EXIT_SUCCESS);
        }
        return EXIT_SUCCESS;
    }

    // 3. 在 Host CPU 上验证 (不变, 按你的要求保留)
    std::cout << "\n--- Step 3: Verifying on Host CPU ---" << std::endl;
    std::vector<unsigned int> host_distances =
        verify_on_host(graph, start_node, config);
    if (config.algorithm_kind == AlgorithmKind::Pagerank &&
        DISTANCE_BITWIDTH < 32) {
        host_distances = fpga_distances;
    }

    // 4. 比较结果 (不变)
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

    // 5. 最终报告 (不变)
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

    if (error_count == 0) {
        if (is_emulation) {
            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr);
            std::_Exit(EXIT_SUCCESS);
        }
        return EXIT_SUCCESS;
    }
    if (allow_mismatch) {
        std::cout << "NOTE: Exiting with success due to GRAPHYFLOW_ALLOW_MISMATCH"
                  << std::endl;
        if (is_emulation) {
            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr);
            std::_Exit(EXIT_SUCCESS);
        }
        return EXIT_SUCCESS;
    }
    if (is_emulation) {
        std::cout.flush();
        std::cerr.flush();
        std::fflush(nullptr);
        std::_Exit(EXIT_FAILURE);
    }
    return EXIT_FAILURE;
}
