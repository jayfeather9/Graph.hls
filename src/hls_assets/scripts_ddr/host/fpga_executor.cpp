
#include "fpga_executor.h"
#include "acc_setup/acc_setup.h"
#include "generated_host.h"
#include "graph_preprocess/graph_preprocess.h"
#include <chrono>
#include <iostream>

#define KERNEL_NAME "graphyflow"

std::vector<int> run_fpga_kernel(const std::string &xclbin_path,
                                 const GraphCSR &graph, int start_node,
                                 double &total_kernel_time_sec,
                                 int &iter_count) {

    // GraphPartiton
    PartitionContainer partition_container = partitionGraph(&graph);

    // init accelerator
    AccDescriptor acc = initAccelerator(xclbin_path);

    AlgorithmHost algo_host(acc);
    algo_host.prepare_data(partition_container, start_node);
    algo_host.setup_buffers(partition_container);
    acc.big_kernel_events.resize(partition_container.num_sparse_partitions);
    acc.little_kernel_events.resize(partition_container.num_dense_partitions);
    for (int i = 0; i < partition_container.num_sparse_partitions; ++i) {
        acc.big_kernel_events[i].resize(acc.num_big_krnl);
    }
    for (int i = 0; i < partition_container.num_dense_partitions; ++i) {
        acc.little_kernel_events[i].resize(acc.num_little_krnl);
    }
    acc.big_prop_loader_events.resize(acc.num_big_krnl);
    acc.little_prop_loader_events.resize(acc.num_little_krnl);
    total_kernel_time_sec = 0;
    double current_kernel_time_sec = 0;
    int max_iterations = graph.num_vertices;
    int iter = 0;
    std::cout << "\nStarting FPGA execution..." << std::endl;

    for (iter = 0; iter < max_iterations; ++iter) {
        auto iteration_start = std::chrono::high_resolution_clock::now();

        algo_host.update_data(partition_container);
        algo_host.transfer_data_to_fpga(partition_container);

        std::cout << "--- [Host] Phase 3: Enqueuing kernel tasks ---"
                  << std::endl;

        // auto kernel_enqueue_start = std::chrono::high_resolution_clock::now();
        algo_host.execute_kernel_iteration(partition_container);
        auto kernel_enqueue_start = std::chrono::high_resolution_clock::now();

        // Wait for all kernels to finish
        for (auto &q : acc.big_gs_queue)
            q.finish();
        // auto big_finish = std::chrono::high_resolution_clock::now();
        for (auto &q : acc.little_gs_queue)
            q.finish();
        for (auto &q : acc.big_prop_loader_queue)
            q.finish();
        for (auto &q : acc.little_prop_loader_queue)
            q.finish();
        acc.apply_queue.finish();

        auto kernel_finish = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> end_to_end_time =
            kernel_finish - kernel_enqueue_start;

        algo_host.transfer_data_from_fpga();

        // iv. 性能统计
        // --- [新增变量] 用于统计平均速度 ---
        double total_big_edges = 0;
        double total_big_time_ns = 0;
        double total_little_edges = 0;
        double total_little_time_ns = 0;
        // --------------------------------

        int cnt = 0;
        int partition_cnt = 0;

        // 1. 统计 Big Kernel
        for (auto &event_vec : acc.big_kernel_events) {
            for (auto &event : event_vec) {
                unsigned long start = 0, end = 0;
                event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
                event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
                double iteration_time_ns = end - start;
                
                current_kernel_time_sec = std::max(current_kernel_time_sec,
                                                   iteration_time_ns * 1.0e-9);
                
                // 获取当前 Kernel 处理的边数
                double num_edges = (double)partition_container.SPs[partition_cnt]
                                       .pipeline_edges[cnt]
                                       .num_edges;

                double mteps = num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

                // --- [新增] 累加 Big Kernel 数据 ---
                total_big_edges += num_edges;
                total_big_time_ns += iteration_time_ns;
                // --------------------------------

                std::cout << "FPGA Iteration " << iter << ": "
                          << "Sparse Partition " << partition_cnt << ", "
                          << "Big Kernel " << cnt++ << ", "
                          << "Time = " << (iteration_time_ns * 1.0e-6)
                          << " ms, "
                          << "Throughput = " << mteps << " MTEPS" << std::endl;
            }
            partition_cnt++;
            cnt = 0;
        }

        // 2. 统计 Little Kernel
        cnt = 0;
        partition_cnt = 0;
        for (auto &event_vec : acc.little_kernel_events) {
            for (auto &event : event_vec) {
                unsigned long start = 0, end = 0;
                event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
                event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
                double iteration_time_ns = end - start;
                
                current_kernel_time_sec = std::max(current_kernel_time_sec,
                                                   iteration_time_ns * 1.0e-9);

                // 获取当前 Kernel 处理的边数
                double num_edges = (double)partition_container.DPs[partition_cnt]
                                       .pipeline_edges[cnt]
                                       .num_edges;

                double mteps = num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

                // --- [新增] 累加 Little Kernel 数据 ---
                total_little_edges += num_edges;
                total_little_time_ns += iteration_time_ns;
                // --------------------------------

                std::cout << "FPGA Iteration " << iter << ": "
                          << "Dense Partition " << partition_cnt << ", "
                          << "Little Kernel " << cnt++ << ", "
                          << "Time = " << (iteration_time_ns * 1.0e-6)
                          << " ms, "
                          << "Throughput = " << mteps << " MTEPS" << std::endl;
            }
            partition_cnt++;
            cnt = 0;
        }

        // --- [新增] 输出平均速度统计 ---
        std::cout << "------------------------------------------------" << std::endl;
        if (total_big_time_ns > 0) {
            double avg_big_mteps = (total_big_edges / (total_big_time_ns * 1.0e-9)) / 1.0e6;
            std::cout << "Average Big Kernel Speed:    " << avg_big_mteps << " MTEPS" << std::endl;
        } else {
            std::cout << "Average Big Kernel Speed:    N/A (No execution)" << std::endl;
        }

        if (total_little_time_ns > 0) {
            double avg_little_mteps = (total_little_edges / (total_little_time_ns * 1.0e-9)) / 1.0e6;
            std::cout << "Average Little Kernel Speed: " << avg_little_mteps << " MTEPS" << std::endl;
        } else {
            std::cout << "Average Little Kernel Speed: N/A (No execution)" << std::endl;
        }
        std::cout << "------------------------------------------------" << std::endl;

        {
            auto event = acc.apply_kernel_event;
            unsigned long start = 0, end = 0;
            event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
            event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
            double iteration_time_ns = end - start;
            current_kernel_time_sec =
                std::max(current_kernel_time_sec, iteration_time_ns * 1.0e-9);
            double mteps =
                (double)graph.num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

            std::cout << "FPGA Iteration " << iter << ": "
                      << "Apply Kernel, "
                      << "Time = " << (iteration_time_ns * 1.0e-6) << " ms, "
                      << std::endl;
        }

        double iteration_time_ns = current_kernel_time_sec * 1.0e9;
        total_kernel_time_sec += end_to_end_time.count();
        current_kernel_time_sec = 0;
        double mteps =
            (double)graph.num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

        std::cout << "FPGA Iteration " << iter << ": "
                  << "Time = " << (iteration_time_ns * 1.0e-6) << " ms, "
                  << "Throughput = " << mteps << " MTEPS" << std::endl;

        // Print end-to-end timing for the iteration
        std::cout << "FPGA Iteration " << iter
                  << " End-to-End Time: " << (end_to_end_time.count() * 1000.0)
                  << " ms" << " Throughput = "
                  << (double)graph.num_edges / end_to_end_time.count() / 1.0e6
                  << " MTEPS" << std::endl;

        // v. 检查是否收敛。如果未收敛，此函数会更新 partition_container
        // 为下次迭代做准备
        if (algo_host.check_convergence_and_update(partition_container)) {
            std::cout << "FPGA computation converged after " << iter + 1
                      << " iteration(s)." << std::endl;
            break;
        }
    }

    iter_count = iter + 1;

    std::cout<<"Getting final results"<<std::endl;
    const std::vector<int> &final_results_ref = algo_host.get_results();
    std::vector<int> final_results = final_results_ref;
    std::cout<<"Getting final results"<<std::endl;
    
    return final_results;
}
