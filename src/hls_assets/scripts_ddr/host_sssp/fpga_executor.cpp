
#include "fpga_executor.h"
#include "acc_setup/acc_setup.h"
#include "generated_host.h"
#include "graph_preprocess/graph_preprocess.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>

#define KERNEL_NAME "graphyflow"

namespace {

constexpr double kDefaultBigEdgePerMsPerPipe = BIG_EDGE_PER_MS;
constexpr double kDefaultLittleEdgePerMsPerPipe = LITTLE_EDGE_PER_MS;

struct LargestPartitionInfo {
    bool valid = false;
    size_t partition_idx = 0;
    unsigned int num_edges = 0;
    unsigned int num_pipelines = 0;
};

struct ProfileAccumulator {
    double sum_edges_per_ms = 0.0;
    int sample_count = 0;
};

struct ProfileResult {
    bool has_big = false;
    bool has_little = false;
    double tuned_big_edge_per_ms_per_pipe = 0.0;
    double tuned_little_edge_per_ms_per_pipe = 0.0;
};

struct KernelRunResult {
    std::vector<int> final_results;
    double total_kernel_time_sec = 0.0;
    int iter_count = 0;
    ProfileResult profile;
};

LargestPartitionInfo
find_largest_partition(const std::vector<PartitionDescriptor> &partitions) {
    LargestPartitionInfo info;
    for (size_t i = 0; i < partitions.size(); ++i) {
        if (!info.valid || partitions[i].num_edges > info.num_edges) {
            info.valid = true;
            info.partition_idx = i;
            info.num_edges = partitions[i].num_edges;
            info.num_pipelines = partitions[i].num_pipelines;
        }
    }
    return info;
}

KernelRunResult run_single_fpga_pass(const std::string &xclbin_path,
                                     const GraphCSR &graph, int start_node,
                                     double big_edge_per_ms_per_pipe,
                                     double little_edge_per_ms_per_pipe,
                                     bool profile_only_one_iter) {
    PartitionContainer partition_container =
        partitionGraph(&graph, big_edge_per_ms_per_pipe,
                       little_edge_per_ms_per_pipe);

    LargestPartitionInfo largest_sparse =
        find_largest_partition(partition_container.SPs);
    LargestPartitionInfo largest_dense =
        find_largest_partition(partition_container.DPs);

    if (profile_only_one_iter) {
        std::cout << "[AUTO-PROFILE] Sparse largest partition: index="
                  << largest_sparse.partition_idx
                  << ", edges=" << largest_sparse.num_edges
                  << ", pipelines=" << largest_sparse.num_pipelines
                  << std::endl;
        std::cout << "[AUTO-PROFILE] Dense largest partition: index="
                  << largest_dense.partition_idx
                  << ", edges=" << largest_dense.num_edges
                  << ", pipelines=" << largest_dense.num_pipelines
                  << std::endl;
    }

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

    KernelRunResult result;
    double current_kernel_time_sec = 0;
    int max_iterations = graph.num_vertices;
    if (profile_only_one_iter) {
        max_iterations = 1;
    }

    ProfileAccumulator big_profile_acc;
    ProfileAccumulator little_profile_acc;

    std::cout << "\nStarting FPGA execution..." << std::endl;

    int iter = 0;
    for (iter = 0; iter < max_iterations; ++iter) {
        auto iteration_start = std::chrono::high_resolution_clock::now();

        algo_host.update_data(partition_container);
        algo_host.transfer_data_to_fpga(partition_container);

        std::cout << "--- [Host] Phase 3: Enqueuing kernel tasks ---"
                  << std::endl;

        algo_host.execute_kernel_iteration(partition_container);
        auto kernel_enqueue_start = std::chrono::high_resolution_clock::now();

        // Wait for all kernels to finish
        for (auto &q : acc.big_gs_queue)
            q.finish();
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

        // iv. Performance statistics
        double total_big_edges = 0;
        double total_big_time_ns = 0;
        double total_little_edges = 0;
        double total_little_time_ns = 0;

        int cnt = 0;
        int partition_cnt = 0;

        // 1. Big Kernel statistics
        for (auto &event_vec : acc.big_kernel_events) {
            for (auto &event : event_vec) {
                unsigned long start = 0, end = 0;
                event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
                event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
                double iteration_time_ns = end - start;

                current_kernel_time_sec = std::max(current_kernel_time_sec,
                                                   iteration_time_ns * 1.0e-9);

                double num_edges = (double)partition_container.SPs[partition_cnt]
                                       .pipeline_edges[cnt]
                                       .num_edges;

                double mteps = num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

                total_big_edges += num_edges;
                total_big_time_ns += iteration_time_ns;

                std::cout << "FPGA Iteration " << iter << ": "
                          << "Sparse Partition " << partition_cnt << ", "
                          << "Big Kernel " << cnt++ << ", "
                          << "Time = " << (iteration_time_ns * 1.0e-6)
                          << " ms, "
                          << "Throughput = " << mteps << " MTEPS" << std::endl;

                double iteration_time_ms = iteration_time_ns * 1.0e-6;
                if (profile_only_one_iter && iter == 0 &&
                    largest_sparse.valid &&
                    (size_t)partition_cnt == largest_sparse.partition_idx &&
                    iteration_time_ms > 0.0) {
                    big_profile_acc.sum_edges_per_ms +=
                        num_edges / iteration_time_ms;
                    big_profile_acc.sample_count++;
                }
            }
            partition_cnt++;
            cnt = 0;
        }

        // 2. Little Kernel statistics
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

                double num_edges = (double)partition_container.DPs[partition_cnt]
                                       .pipeline_edges[cnt]
                                       .num_edges;

                double mteps = num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

                total_little_edges += num_edges;
                total_little_time_ns += iteration_time_ns;

                std::cout << "FPGA Iteration " << iter << ": "
                          << "Dense Partition " << partition_cnt << ", "
                          << "Little Kernel " << cnt++ << ", "
                          << "Time = " << (iteration_time_ns * 1.0e-6)
                          << " ms, "
                          << "Throughput = " << mteps << " MTEPS" << std::endl;

                double iteration_time_ms = iteration_time_ns * 1.0e-6;
                if (profile_only_one_iter && iter == 0 &&
                    largest_dense.valid &&
                    (size_t)partition_cnt == largest_dense.partition_idx &&
                    iteration_time_ms > 0.0) {
                    little_profile_acc.sum_edges_per_ms +=
                        num_edges / iteration_time_ms;
                    little_profile_acc.sample_count++;
                }
            }
            partition_cnt++;
            cnt = 0;
        }

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

            std::cout << "FPGA Iteration " << iter << ": "
                      << "Apply Kernel, "
                      << "Time = " << (iteration_time_ns * 1.0e-6) << " ms, "
                      << std::endl;
        }

        double iteration_time_ns = current_kernel_time_sec * 1.0e9;
        result.total_kernel_time_sec += end_to_end_time.count();
        current_kernel_time_sec = 0;
        double mteps =
            (double)graph.num_edges / (iteration_time_ns * 1.0e-9) / 1.0e6;

        std::cout << "FPGA Iteration " << iter << ": "
                  << "Time = " << (iteration_time_ns * 1.0e-6) << " ms, "
                  << "Throughput = " << mteps << " MTEPS" << std::endl;

        std::cout << "FPGA Iteration " << iter
                  << " End-to-End Time: " << (end_to_end_time.count() * 1000.0)
                  << " ms" << " Throughput = "
                  << (double)graph.num_edges / end_to_end_time.count() / 1.0e6
                  << " MTEPS" << std::endl;

        if (profile_only_one_iter) {
            break;
        }

        if (algo_host.check_convergence_and_update(partition_container)) {
            std::cout << "FPGA computation converged after " << iter + 1
                      << " iteration(s)." << std::endl;
            break;
        }
    }

    result.iter_count = iter + 1;

    std::cout<<"Getting final results"<<std::endl;
    const std::vector<int> &final_results_ref = algo_host.get_results();
    result.final_results = final_results_ref;
    std::cout<<"Getting final results"<<std::endl;

    if (profile_only_one_iter) {
        if (big_profile_acc.sample_count > 0) {
            result.profile.has_big = true;
            result.profile.tuned_big_edge_per_ms_per_pipe =
                big_profile_acc.sum_edges_per_ms /
                static_cast<double>(big_profile_acc.sample_count);
        }
        if (little_profile_acc.sample_count > 0) {
            result.profile.has_little = true;
            result.profile.tuned_little_edge_per_ms_per_pipe =
                little_profile_acc.sum_edges_per_ms /
                static_cast<double>(little_profile_acc.sample_count);
        }
    }

    return result;
}

} // namespace

std::vector<int> run_fpga_kernel(const std::string &xclbin_path,
                                 const GraphCSR &graph, int start_node,
                                 double &total_kernel_time_sec,
                                 int &iter_count,
                                 const FpgaRunOptions &options) {
    if (!options.repartition_with_iter0_profile) {
        KernelRunResult result =
            run_single_fpga_pass(xclbin_path, graph, start_node,
                                 kDefaultBigEdgePerMsPerPipe,
                                 kDefaultLittleEdgePerMsPerPipe, false);
        total_kernel_time_sec = result.total_kernel_time_sec;
        iter_count = result.iter_count;
        return result.final_results;
    }

    std::cout << "[AUTO-PROFILE] Mode enabled: first run 1 iteration, "
                 "derive throughputs from largest partitions, then repartition "
                 "and rerun."
              << std::endl;

    KernelRunResult profile_run =
        run_single_fpga_pass(xclbin_path, graph, start_node,
                             kDefaultBigEdgePerMsPerPipe,
                             kDefaultLittleEdgePerMsPerPipe, true);

    double tuned_big = kDefaultBigEdgePerMsPerPipe;
    double tuned_little = kDefaultLittleEdgePerMsPerPipe;

    if (profile_run.profile.has_big) {
        tuned_big = profile_run.profile.tuned_big_edge_per_ms_per_pipe;
    }
    if (profile_run.profile.has_little) {
        tuned_little = profile_run.profile.tuned_little_edge_per_ms_per_pipe;
    }

    std::cout << "[AUTO-PROFILE] Derived big throughput (edge/ms/pipeline) = "
              << tuned_big << std::endl;
    std::cout
        << "[AUTO-PROFILE] Derived little throughput (edge/ms/pipeline) = "
        << tuned_little << std::endl;
    std::cout << "[AUTO-PROFILE] Profiling pass end-to-end kernel time (not "
                 "included in final report): "
              << (profile_run.total_kernel_time_sec * 1000.0) << " ms"
              << std::endl;

    KernelRunResult tuned_run =
        run_single_fpga_pass(xclbin_path, graph, start_node, tuned_big,
                             tuned_little, false);

    total_kernel_time_sec = tuned_run.total_kernel_time_sec;
    iter_count = tuned_run.iter_count;
    return tuned_run.final_results;
}
