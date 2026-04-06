
#include "fpga_executor.h"
#include "acc_setup/acc_setup.h"
#include "generated_host.h"
#include "graph_preprocess/graph_preprocess.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <sys/file.h>
#include <unistd.h>

#define KERNEL_NAME "graphyflow"

namespace {

constexpr double kDefaultBigEdgePerMsPerPipe = 280000.0;
constexpr double kDefaultLittleEdgePerMsPerPipe = 1040000.0;

struct LargestPartitionInfo {
    bool valid = false;
    size_t group_idx = 0;
    size_t part_idx = 0;
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
    std::vector<unsigned int> final_results;
    double total_kernel_time_sec = 0.0;
    int iter_count = 0;
    ProfileResult profile;
};

void release_device_lock_if_held(AccDescriptor &acc) {
    if (acc.device_lock_fd >= 0) {
        flock(acc.device_lock_fd, LOCK_UN);
        close(acc.device_lock_fd);
        acc.device_lock_fd = -1;
    }
}

LargestPartitionInfo
find_largest_partition(const std::vector<PartitionGroup> &groups) {
    LargestPartitionInfo info;
    for (size_t g = 0; g < groups.size(); ++g) {
        const auto &group = groups[g];
        for (size_t p = 0; p < group.partitions.size(); ++p) {
            const auto &part = group.partitions[p];
            if (!info.valid || part.num_edges > info.num_edges) {
                info.valid = true;
                info.group_idx = g;
                info.part_idx = p;
                info.num_edges = part.num_edges;
                info.num_pipelines = group.num_pipelines;
            }
        }
    }
    return info;
}

KernelRunResult run_single_fpga_pass(const std::string &xclbin_path,
                                     const GraphCSR &graph, int start_node,
                                     AlgorithmConfig config,
                                     double big_edge_per_ms_per_pipe,
                                     double little_edge_per_ms_per_pipe,
                                     bool profile_only_one_iter) {
    PartitionContainer partition_container =
        partitionGraph(&graph, big_edge_per_ms_per_pipe,
                       little_edge_per_ms_per_pipe);

    LargestPartitionInfo largest_sparse =
        find_largest_partition(partition_container.sparse_groups);
    LargestPartitionInfo largest_dense =
        find_largest_partition(partition_container.dense_groups);

    if (profile_only_one_iter) {
        std::cout << "[AUTO-PROFILE] Sparse largest partition: group="
                  << largest_sparse.group_idx
                  << ", partition=" << largest_sparse.part_idx
                  << ", edges=" << largest_sparse.num_edges
                  << ", pipelines=" << largest_sparse.num_pipelines
                  << std::endl;
        std::cout << "[AUTO-PROFILE] Dense largest partition: group="
                  << largest_dense.group_idx
                  << ", partition=" << largest_dense.part_idx
                  << ", edges=" << largest_dense.num_edges
                  << ", pipelines=" << largest_dense.num_pipelines
                  << std::endl;
    }

    AccDescriptor acc = initAccelerator(xclbin_path);

    AlgorithmHost algo_host(acc, config);
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

    KernelRunResult result;
    double current_kernel_time_sec = 0.0;
    int max_iterations = graph.num_vertices;
    if (profile_only_one_iter) {
        max_iterations = 1;
    } else if (config.convergence_mode == ConvergenceMode::FixedIterations &&
               config.max_iterations > 0) {
        max_iterations = config.max_iterations;
    }

    ProfileAccumulator big_profile_acc;
    ProfileAccumulator little_profile_acc;

    std::cout << "\nStarting FPGA execution..." << std::endl;

    int iter = 0;
    for (iter = 0; iter < max_iterations; ++iter) {
        algo_host.update_data(partition_container);
        algo_host.transfer_data_to_fpga(partition_container);

        std::cout << "--- [Host] Phase 3: Enqueuing kernel tasks ---"
                  << std::endl;

        auto kernel_enqueue_start = std::chrono::high_resolution_clock::now();
        algo_host.execute_kernel_iteration(partition_container);

        // Submit every dependent queue before waiting. Without the explicit
        // flushes below, a finish() on an upstream queue can block while
        // downstream service kernels are still buffered in the host runtime.
        for (auto &q : acc.big_gs_queue)
            q.flush();
        for (auto &q : acc.little_gs_queue)
            q.flush();
        acc.apply_queue.flush();
        acc.hbm_writer_queue.flush();

        // Hardware runs were previously stabilized by draining every command
        // queue before the Phase-4 readback. Keep the barriers, but drain the
        // downstream queues first after all work has been submitted.
        acc.hbm_writer_queue.finish();
        acc.apply_queue.finish();
        for (auto &q : acc.little_gs_queue)
            q.finish();
        for (auto &q : acc.big_gs_queue)
            q.finish();

        auto kernel_finish = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> end_to_end_time =
            kernel_finish - kernel_enqueue_start;

        algo_host.transfer_data_from_fpga();

        // Collect per-pipeline timings and derive profile data from the
        // largest sparse/dense partitions during the profiling pass.
        size_t sparse_flat_idx = 0;
        for (size_t group_idx = 0;
             group_idx < partition_container.num_sparse_groups; ++group_idx) {
            const auto &group = partition_container.sparse_groups[group_idx];
            for (size_t part_idx = 0; part_idx < group.partitions.size();
                 ++part_idx, ++sparse_flat_idx) {
                const auto &partition = group.partitions[part_idx];
                for (unsigned int local_pip = 0;
                     local_pip < group.num_pipelines; ++local_pip) {
                    unsigned int global_pip = group.pipeline_offset + local_pip;
                    auto &event =
                        acc.big_kernel_events[sparse_flat_idx][global_pip];
                    if (event() == nullptr) {
                        continue;
                    }
                    unsigned long start = 0, end = 0;
                    event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
                    event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
                    double iteration_time_ns = end - start;
                    current_kernel_time_sec = std::max(
                        current_kernel_time_sec, iteration_time_ns * 1.0e-9);
                    double iteration_time_ms = iteration_time_ns * 1.0e-6;
                    double mteps =
                        static_cast<double>(
                            partition.pipeline_edges[local_pip].num_edges) /
                        (iteration_time_ns * 1.0e-9) / 1.0e6;

                    std::cout << "FPGA Iteration " << iter << ": "
                              << "Sparse Group " << group_idx
                              << ", Partition " << part_idx << ", Big Kernel "
                              << global_pip << ", Time = "
                              << iteration_time_ms
                              << " ms, Throughput = " << mteps << " MTEPS"
                              << std::endl;

                    if (profile_only_one_iter && iter == 0 &&
                        largest_sparse.valid &&
                        group_idx == largest_sparse.group_idx &&
                        part_idx == largest_sparse.part_idx &&
                        iteration_time_ms > 0.0) {
                        const double edges = static_cast<double>(
                            partition.pipeline_edges[local_pip].num_edges);
                        big_profile_acc.sum_edges_per_ms +=
                            edges / iteration_time_ms;
                        big_profile_acc.sample_count++;
                    }
                }
            }
        }

        size_t dense_flat_idx = 0;
        for (size_t group_idx = 0;
             group_idx < partition_container.num_dense_groups; ++group_idx) {
            const auto &group = partition_container.dense_groups[group_idx];
            for (size_t part_idx = 0; part_idx < group.partitions.size();
                 ++part_idx, ++dense_flat_idx) {
                const auto &partition = group.partitions[part_idx];
                for (unsigned int local_pip = 0;
                     local_pip < group.num_pipelines; ++local_pip) {
                    unsigned int global_pip = group.pipeline_offset + local_pip;
                    auto &event =
                        acc.little_kernel_events[dense_flat_idx][global_pip];
                    if (event() == nullptr) {
                        continue;
                    }
                    unsigned long start = 0, end = 0;
                    event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
                    event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
                    double iteration_time_ns = end - start;
                    current_kernel_time_sec = std::max(
                        current_kernel_time_sec, iteration_time_ns * 1.0e-9);
                    double iteration_time_ms = iteration_time_ns * 1.0e-6;
                    double mteps =
                        static_cast<double>(
                            partition.pipeline_edges[local_pip].num_edges) /
                        (iteration_time_ns * 1.0e-9) / 1.0e6;

                    std::cout << "FPGA Iteration " << iter << ": "
                              << "Dense Group " << group_idx
                              << ", Partition " << part_idx
                              << ", Little Kernel " << global_pip
                              << ", Time = " << iteration_time_ms
                              << " ms, Throughput = " << mteps << " MTEPS"
                              << std::endl;

                    if (profile_only_one_iter && iter == 0 &&
                        largest_dense.valid &&
                        group_idx == largest_dense.group_idx &&
                        part_idx == largest_dense.part_idx &&
                        iteration_time_ms > 0.0) {
                        const double edges = static_cast<double>(
                            partition.pipeline_edges[local_pip].num_edges);
                        little_profile_acc.sum_edges_per_ms +=
                            edges / iteration_time_ms;
                        little_profile_acc.sample_count++;
                    }
                }
            }
        }

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

        {
            auto event = acc.hbm_writer_event;
            unsigned long start = 0, end = 0;
            event.getProfilingInfo(CL_PROFILING_COMMAND_START, &start);
            event.getProfilingInfo(CL_PROFILING_COMMAND_END, &end);
            double iteration_time_ns = end - start;
            current_kernel_time_sec =
                std::max(current_kernel_time_sec, iteration_time_ns * 1.0e-9);

            std::cout << "FPGA Iteration " << iter << ": "
                      << "HBM Writer Kernel Time = "
                      << (iteration_time_ns * 1.0e-6) << " ms" << std::endl;
        }

        double iteration_time_ns = current_kernel_time_sec * 1.0e9;
        result.total_kernel_time_sec += end_to_end_time.count();
        current_kernel_time_sec = 0;
        double mteps =
            static_cast<double>(graph.num_edges) / (iteration_time_ns * 1.0e-9) /
            1.0e6;

        std::cout << "FPGA Iteration " << iter << ": "
                  << "Time = " << (iteration_time_ns * 1.0e-6)
                  << " ms, "
                  << "Throughput = " << mteps << " MTEPS" << std::endl;
        std::cout << "FPGA Iteration " << iter
                  << " End-to-End Time: " << (end_to_end_time.count() * 1000.0)
                  << " ms"
                  << " Throughput = "
                  << static_cast<double>(graph.num_edges) /
                         end_to_end_time.count() / 1.0e6
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

    const std::vector<unsigned int> &final_results_ref = algo_host.get_results();
    result.final_results = final_results_ref;

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

    release_device_lock_if_held(acc);

    return result;
}

} // namespace

std::vector<unsigned int> run_fpga_kernel(const std::string &xclbin_path,
                                          const GraphCSR &graph,
                                          int start_node,
                                          double &total_kernel_time_sec,
                                          int &iter_count,
                                          AlgorithmConfig config,
                                          const FpgaRunOptions &options) {
    if (!options.repartition_with_iter0_profile) {
        KernelRunResult result =
            run_single_fpga_pass(xclbin_path, graph, start_node, config,
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
        run_single_fpga_pass(xclbin_path, graph, start_node, config,
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
        run_single_fpga_pass(xclbin_path, graph, start_node, config, tuned_big,
                             tuned_little, false);

    total_kernel_time_sec = tuned_run.total_kernel_time_sec;
    iter_count = tuned_run.iter_count;
    return tuned_run.final_results;
}
