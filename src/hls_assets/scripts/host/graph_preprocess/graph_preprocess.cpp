#include "graph_preprocess.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
#include <chrono>
#endif
#include <iostream>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#ifndef DST_SHUFFLE_MODE
#define DST_SHUFFLE_MODE 0
#endif

#ifndef DST_SHUFFLE_BLOCK_SIZE
#define DST_SHUFFLE_BLOCK_SIZE 1024
#endif

#ifndef GRAPH_PREPROCESS_DENSE_PARTITIONS_PER_GROUP
#define GRAPH_PREPROCESS_DENSE_PARTITIONS_PER_GROUP 0
#endif

#ifndef GRAPH_PREPROCESS_SPARSE_PARTITIONS_PER_GROUP
#define GRAPH_PREPROCESS_SPARSE_PARTITIONS_PER_GROUP 0
#endif

#ifndef GRAPH_PREPROCESS_DENSE_BALANCE_WINDOW
#define GRAPH_PREPROCESS_DENSE_BALANCE_WINDOW 0
#endif

#ifndef GRAPH_PREPROCESS_SPARSE_BALANCE_WINDOW
#define GRAPH_PREPROCESS_SPARSE_BALANCE_WINDOW 0
#endif

#ifndef GRAPH_PREPROCESS_DENSE_THROUGHPUT_SCALE_PCT
#define GRAPH_PREPROCESS_DENSE_THROUGHPUT_SCALE_PCT 100
#endif

#ifndef GRAPH_PREPROCESS_SPARSE_THROUGHPUT_SCALE_PCT
#define GRAPH_PREPROCESS_SPARSE_THROUGHPUT_SCALE_PCT 100
#endif

#if EDGES_PER_WORD <= 0
#error "EDGES_PER_WORD must be positive"
#endif

struct Edge {
    int src;
    int dest;
    int weight;
    std::vector<uint64_t> props;
};

static inline size_t pad_to_word(size_t n) {
    const size_t mod = n % EDGES_PER_WORD;
    return mod == 0 ? 0 : (EDGES_PER_WORD - mod);
}

struct DestinationAssignment {
    bool assigned = false;
    bool is_dense = false;
    size_t group_idx = 0;
    size_t partition_idx = 0;
};

static inline size_t ceil_div_size_t(size_t n, size_t d) {
    return (n + d - 1) / d;
}

static inline size_t effective_balance_window(size_t configured, size_t size) {
    if (configured == 0) {
        return 0;
    }
    if (configured >= size) {
        return size;
    }
    return configured;
}

static void shuffle_dsts(std::vector<int> &dsts, size_t group_idx,
                         size_t part_idx, bool is_dense) {
    if (dsts.empty()) {
        return;
    }
#if DST_SHUFFLE_MODE == 0
    std::srand(42);
    std::random_shuffle(dsts.begin(), dsts.end());
#elif DST_SHUFFLE_MODE == 1
    (void)group_idx;
    (void)part_idx;
    (void)is_dense;
#elif DST_SHUFFLE_MODE == 2
    {
        const size_t n = dsts.size();
        for (size_t i = 0; i < n;
             i += static_cast<size_t>(DST_SHUFFLE_BLOCK_SIZE)) {
            const size_t j =
                std::min(n, i + static_cast<size_t>(DST_SHUFFLE_BLOCK_SIZE));
            const int salt = is_dense ? 1315423911u : 2654435761u;
            std::srand(42 + static_cast<int>((group_idx * 97 + part_idx) * salt + i));
            std::random_shuffle(dsts.begin() + i, dsts.begin() + j);
        }
    }
#elif DST_SHUFFLE_MODE == 3
    {
        const int salt = is_dense ? 1315423911u : 2654435761u;
        std::srand(42 + static_cast<int>((group_idx * 97 + part_idx) * salt));
        std::random_shuffle(dsts.begin(), dsts.end());
    }
#elif DST_SHUFFLE_MODE == 6
    std::srand(42);
    std::random_shuffle(dsts.begin(), dsts.end());
#else
#error "Unsupported DST_SHUFFLE_MODE"
#endif
}

PartitionContainer partitionGraph(const GraphCSR *graph,
                                  double big_edge_per_ms_per_pipe,
                                  double little_edge_per_ms_per_pipe) {
    std::cout << "--- Starting Graph Partitioning and Preprocessing ---"
              << std::endl;
#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    using gp_clock = std::chrono::steady_clock;
    auto gp_ms_since = [](const gp_clock::time_point &start,
                          const gp_clock::time_point &end) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    };
    const auto t_total0 = gp_clock::now();
#endif

    PartitionContainer container;
    container.num_graph_vertices = graph->num_vertices;
    container.num_graph_edges = graph->num_edges;
    container.graph_out_degrees.assign(static_cast<size_t>(graph->num_vertices), 0u);
    for (int u = 0; u < graph->num_vertices; ++u) {
        container.graph_out_degrees[static_cast<size_t>(u)] =
            static_cast<uint32_t>(graph->offsets[static_cast<size_t>(u + 1)] -
                                  graph->offsets[static_cast<size_t>(u)]);
    }
    container.num_dense_groups = NUM_LITTLE_MERGERS;
    container.num_sparse_groups = NUM_BIG_MERGERS;
    container.num_dense_partitions = 0;
    container.num_sparse_partitions = 0;

    container.dense_groups.resize(container.num_dense_groups);
    container.sparse_groups.resize(container.num_sparse_groups);
    container.dense_partition_indices.resize(container.num_dense_groups);
    container.sparse_partition_indices.resize(container.num_sparse_groups);

    constexpr size_t little_pipeline_len =
        sizeof(LITTLE_MERGER_PIPELINE_LENGTHS) / sizeof(uint32_t);
    constexpr size_t little_offset_len =
        sizeof(LITTLE_MERGER_KERNEL_OFFSETS) / sizeof(uint32_t);
    constexpr size_t big_pipeline_len =
        sizeof(BIG_MERGER_PIPELINE_LENGTHS) / sizeof(uint32_t);
    constexpr size_t big_offset_len =
        sizeof(BIG_MERGER_KERNEL_OFFSETS) / sizeof(uint32_t);

    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        auto &group = container.dense_groups[g];
        group.group_id = static_cast<unsigned int>(g);
        if (g >= little_offset_len || g >= little_pipeline_len) {
            std::cerr
                << "[ERROR] Mismatch in little merger group configuration!"
                << std::endl;
            std::exit(EXIT_FAILURE);
        }
        group.pipeline_offset = LITTLE_MERGER_KERNEL_OFFSETS[g];
        group.num_pipelines = LITTLE_MERGER_PIPELINE_LENGTHS[g];
    }

    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        auto &group = container.sparse_groups[g];
        group.group_id = static_cast<unsigned int>(g);
        if (g >= big_offset_len || g >= big_pipeline_len) {
            std::cerr << "[ERROR] Mismatch in big merger group configuration!"
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }
        group.pipeline_offset = BIG_MERGER_KERNEL_OFFSETS[g];
        group.num_pipelines = BIG_MERGER_PIPELINE_LENGTHS[g];
    }

    printf("Global graph has %d vertices and %d edges.\n", graph->num_vertices,
           graph->num_edges);
    std::cout << "[INFO] Dense groups: " << container.num_dense_groups
              << ", Sparse groups: " << container.num_sparse_groups
              << std::endl;

    if (container.num_dense_groups == 0 && container.num_sparse_groups == 0) {
        std::cerr << "[ERROR] No kernel groups configured." << std::endl;
        std::exit(EXIT_FAILURE);
    }

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase10 = gp_clock::now();
#endif

    // --- PHASE 1: Identify and collect unique destination vertices ---
    std::vector<int> node_indegrees(static_cast<size_t>(graph->num_vertices), 0);
    for (int i = 0; i < graph->num_edges; ++i) {
        const int dst = graph->columns[static_cast<size_t>(i)];
        node_indegrees[static_cast<size_t>(dst)]++;
    }

    std::vector<int> unique_dst_vertices;
    unique_dst_vertices.reserve(static_cast<size_t>(graph->num_vertices));
    for (int v = 0; v < graph->num_vertices; ++v) {
        if (node_indegrees[static_cast<size_t>(v)] > 0) {
            unique_dst_vertices.push_back(v);
        }
    }

    std::sort(unique_dst_vertices.begin(), unique_dst_vertices.end(),
              [&node_indegrees](int a, int b) {
                  return node_indegrees[static_cast<size_t>(a)] >
                         node_indegrees[static_cast<size_t>(b)];
              });

    std::cout << "[PHASE 1] Found " << unique_dst_vertices.size()
              << " unique destination vertices (sorted by indegree)."
              << std::endl;

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase11 = gp_clock::now();
    std::cout << "[PROFILE] Phase1_ms=" << gp_ms_since(t_phase10, t_phase11)
              << std::endl;
#endif

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase20 = gp_clock::now();
#endif

    // --- PHASE 2: Distribute destination vertices among groups and partitions ---
    const size_t total_dst_vertices = unique_dst_vertices.size();

    std::cout << "[PHASE 2] Total dst vertices: " << total_dst_vertices
              << std::endl;

    const size_t one_partition_capacity =
        static_cast<size_t>(container.num_dense_groups) * LITTLE_MAX_DST +
        static_cast<size_t>(container.num_sparse_groups) * BIG_MAX_DST;

    if (one_partition_capacity == 0) {
        std::cerr << "[ERROR] Invalid configuration: one_partition_capacity is "
                     "zero."
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    const size_t partition_number =
        ceil_div_size_t(total_dst_vertices, one_partition_capacity);

    std::cout << "[PHASE 2] Calculated partition number needed: "
              << partition_number << std::endl;

    std::vector<DestinationAssignment> dst_assignment(
        static_cast<size_t>(graph->num_vertices));

    std::vector<std::vector<std::vector<int>>> little_dst_lists(
        container.num_dense_groups);
    std::vector<std::vector<std::vector<int>>> big_dst_lists(
        container.num_sparse_groups);

    // Profile-based assignment: balance estimated processing time across groups
    // within each partition, while respecting per-group destination caps.
    std::vector<double> dense_throughput(container.num_dense_groups, 1.0);
    std::vector<double> sparse_throughput(container.num_sparse_groups, 1.0);
    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        dense_throughput[g] = std::max(
            1.0, little_edge_per_ms_per_pipe *
                     static_cast<double>(container.dense_groups[g].num_pipelines));
    }
    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        sparse_throughput[g] =
            std::max(1.0, big_edge_per_ms_per_pipe *
                              static_cast<double>(container.sparse_groups[g]
                                                      .num_pipelines));
    }

    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        little_dst_lists[g].resize(partition_number);
    }
    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        big_dst_lists[g].resize(partition_number);
    }

    std::vector<size_t> dense_remaining(
        container.num_dense_groups,
        static_cast<size_t>(LITTLE_MAX_DST) * partition_number);
    std::vector<size_t> sparse_remaining(
        container.num_sparse_groups,
        static_cast<size_t>(BIG_MAX_DST) * partition_number);

    size_t dense_remaining_total = std::accumulate(dense_remaining.begin(),
                                                   dense_remaining.end(), 0UL);
    size_t sparse_remaining_total = std::accumulate(sparse_remaining.begin(),
                                                    sparse_remaining.end(), 0UL);

    std::vector<double> dense_assigned_edges(container.num_dense_groups, 0.0);
    std::vector<double> sparse_assigned_edges(container.num_sparse_groups, 0.0);
    double dense_assigned_edges_total = 0.0;
    double sparse_assigned_edges_total = 0.0;

    const double dense_total_throughput = std::accumulate(
        dense_throughput.begin(), dense_throughput.end(), 0.0);
    const double sparse_total_throughput = std::accumulate(
        sparse_throughput.begin(), sparse_throughput.end(), 0.0);

    auto choose_group_by_projected_ms =
        [](const std::vector<size_t> &remaining,
           const std::vector<double> &assigned_edges,
           const std::vector<double> &throughput, double edge_cost) -> size_t {
        size_t best_group = std::numeric_limits<size_t>::max();
        double best_score = std::numeric_limits<double>::infinity();
        for (size_t g = 0; g < remaining.size(); ++g) {
            if (remaining[g] == 0) {
                continue;
            }
            const double projected_ms =
                (assigned_edges[g] + edge_cost) / throughput[g];
            if (best_group == std::numeric_limits<size_t>::max() ||
                projected_ms < best_score - 1e-12 ||
                (std::abs(projected_ms - best_score) <= 1e-12 &&
                 remaining[g] > remaining[best_group])) {
                best_group = g;
                best_score = projected_ms;
            }
        }
        return best_group;
    };

    auto assign_vertex_to_slot = [&](bool is_dense, size_t group_idx,
                                     int vertex_id) -> bool {
        auto &dst_lists = is_dense ? little_dst_lists : big_dst_lists;
        const size_t cap = is_dense ? static_cast<size_t>(LITTLE_MAX_DST)
                                    : static_cast<size_t>(BIG_MAX_DST);
        for (size_t part = 0; part < partition_number; ++part) {
            if (dst_lists[group_idx][part].size() < cap) {
                dst_lists[group_idx][part].push_back(vertex_id);
                dst_assignment[static_cast<size_t>(vertex_id)] = {
                    true, is_dense, group_idx, part};
                return true;
            }
        }
        return false;
    };

    for (int vertex_id : unique_dst_vertices) {
        const double edge_cost =
            static_cast<double>(node_indegrees[static_cast<size_t>(vertex_id)]);

        const bool can_dense = (dense_remaining_total > 0);
        const bool can_sparse = (sparse_remaining_total > 0);
        if (!can_dense && !can_sparse) {
            std::cerr << "[ERROR] Destination capacity exhausted unexpectedly."
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }

        bool choose_dense_class = false;
        if (can_dense && !can_sparse) {
            choose_dense_class = true;
        } else if (!can_dense && can_sparse) {
            choose_dense_class = false;
        } else {
            // Global class balancing objective: keep dense-vs-sparse estimated
            // processing times close, where each class time is
            // edges / (sum(pipeline_throughput)).
            const double dense_ms_now =
                dense_assigned_edges_total / dense_total_throughput;
            const double sparse_ms_now =
                sparse_assigned_edges_total / sparse_total_throughput;
            const double dense_ms_if =
                (dense_assigned_edges_total + edge_cost) / dense_total_throughput;
            const double sparse_ms_if =
                (sparse_assigned_edges_total + edge_cost) / sparse_total_throughput;
            const double diff_if_dense = std::abs(dense_ms_if - sparse_ms_now);
            const double diff_if_sparse = std::abs(dense_ms_now - sparse_ms_if);
            choose_dense_class = (diff_if_dense <= diff_if_sparse);
        }

        bool choose_dense = choose_dense_class;
        size_t choose_group = std::numeric_limits<size_t>::max();

        if (choose_dense) {
            choose_group = choose_group_by_projected_ms(
                dense_remaining, dense_assigned_edges, dense_throughput, edge_cost);
            if (choose_group == std::numeric_limits<size_t>::max()) {
                choose_dense = false;
            }
        }
        if (!choose_dense) {
            choose_group =
                choose_group_by_projected_ms(sparse_remaining,
                                             sparse_assigned_edges,
                                             sparse_throughput, edge_cost);
            if (choose_group == std::numeric_limits<size_t>::max()) {
                choose_dense = true;
                choose_group = choose_group_by_projected_ms(
                    dense_remaining, dense_assigned_edges, dense_throughput,
                    edge_cost);
            }
        }

        if (choose_group == std::numeric_limits<size_t>::max()) {
            std::cerr << "[ERROR] Cannot assign dst vertex " << vertex_id
                      << " due to exhausted group capacities." << std::endl;
            std::exit(EXIT_FAILURE);
        }

        if (!assign_vertex_to_slot(choose_dense, choose_group, vertex_id)) {
            std::cerr << "[ERROR] Failed to place dst vertex " << vertex_id
                      << " into a concrete partition slot." << std::endl;
            std::exit(EXIT_FAILURE);
        }

        if (choose_dense) {
            dense_remaining[choose_group]--;
            dense_remaining_total--;
            dense_assigned_edges[choose_group] += edge_cost;
            dense_assigned_edges_total += edge_cost;
        } else {
            sparse_remaining[choose_group]--;
            sparse_remaining_total--;
            sparse_assigned_edges[choose_group] += edge_cost;
            sparse_assigned_edges_total += edge_cost;
        }
    }

    for (size_t part = 0; part < partition_number; ++part) {
        size_t vertices_in_part = 0;
        for (size_t g = 0; g < container.num_dense_groups; ++g) {
            vertices_in_part += little_dst_lists[g][part].size();
        }
        for (size_t g = 0; g < container.num_sparse_groups; ++g) {
            vertices_in_part += big_dst_lists[g][part].size();
        }
        std::cout << "[PHASE 2]   Partition " << part << ": assigning "
                  << vertices_in_part << " vertices" << std::endl;
    }

    size_t total_dense_assigned = 0;
    size_t total_sparse_assigned = 0;
    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        for (size_t p = 0; p < little_dst_lists[g].size(); ++p) {
            total_dense_assigned += little_dst_lists[g][p].size();
        }
    }
    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        for (size_t p = 0; p < big_dst_lists[g].size(); ++p) {
            total_sparse_assigned += big_dst_lists[g][p].size();
        }
    }

    std::cout << "[PHASE 2] Dense groups assigned " << total_dense_assigned
              << " dst vertices across " << container.num_dense_groups
              << " groups." << std::endl;
    std::cout << "[PHASE 2] Sparse groups assigned " << total_sparse_assigned
              << " dst vertices across " << container.num_sparse_groups
              << " groups." << std::endl;

    const double dense_est_ms =
        dense_assigned_edges_total / std::max(1.0, dense_total_throughput);
    const double sparse_est_ms =
        sparse_assigned_edges_total / std::max(1.0, sparse_total_throughput);
    std::cout << "[PHASE 2] Estimated dense time = " << dense_est_ms
              << " ms, sparse time = " << sparse_est_ms
              << " ms (ratio dense/sparse = "
              << (dense_est_ms / std::max(1e-9, sparse_est_ms)) << ")"
              << std::endl;

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase21 = gp_clock::now();
    std::cout << "[PROFILE] Phase2_ms=" << gp_ms_since(t_phase20, t_phase21)
              << std::endl;
#endif

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase30 = gp_clock::now();
#endif

    // --- PHASE 3: Assign edges based on destination ownership ---
    std::vector<std::vector<std::vector<Edge>>> dense_edges_lists(
        container.num_dense_groups);
    std::vector<std::vector<std::vector<Edge>>> sparse_edges_lists(
        container.num_sparse_groups);

    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        dense_edges_lists[g].resize(little_dst_lists[g].size());
        for (size_t p = 0; p < little_dst_lists[g].size(); ++p) {
            size_t edge_cap = 0;
            for (int dst : little_dst_lists[g][p]) {
                edge_cap +=
                    static_cast<size_t>(node_indegrees[static_cast<size_t>(dst)]);
            }
            dense_edges_lists[g][p].reserve(edge_cap);
        }
    }
    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        sparse_edges_lists[g].resize(big_dst_lists[g].size());
        for (size_t p = 0; p < big_dst_lists[g].size(); ++p) {
            size_t edge_cap = 0;
            for (int dst : big_dst_lists[g][p]) {
                edge_cap +=
                    static_cast<size_t>(node_indegrees[static_cast<size_t>(dst)]);
            }
            sparse_edges_lists[g][p].reserve(edge_cap);
        }
    }

    size_t little_edge_num = 0;
    size_t big_edge_num = 0;

    for (int u = 0; u < graph->num_vertices; ++u) {
        for (int i = graph->offsets[static_cast<size_t>(u)];
             i < graph->offsets[static_cast<size_t>(u + 1)]; ++i) {
            const int v = graph->columns[static_cast<size_t>(i)];
            const int w = graph->weights[static_cast<size_t>(i)];

            const auto &assign = dst_assignment[static_cast<size_t>(v)];
            if (!assign.assigned) {
                std::cerr << "[ERROR] Destination vertex " << v
                          << " not assigned to any group/partition."
                          << std::endl;
                std::exit(EXIT_FAILURE);
            }

            Edge edge{u, v, w, {}};
            if (EDGE_PROP_COUNT > 0) {
                edge.props.resize(EDGE_PROP_COUNT, 0);
                const size_t base = static_cast<size_t>(i) * EDGE_PROP_COUNT;
                for (size_t p_idx = 0; p_idx < EDGE_PROP_COUNT; ++p_idx) {
                    edge.props[p_idx] = graph->edge_props[base + p_idx];
                }
            }

            if (assign.is_dense) {
                dense_edges_lists[assign.group_idx][assign.partition_idx].push_back(
                    std::move(edge));
                little_edge_num++;
            } else {
                sparse_edges_lists[assign.group_idx][assign.partition_idx].push_back(
                    std::move(edge));
                big_edge_num++;
            }
        }
    }

    std::cout << "[PHASE 3] Assigned " << little_edge_num
              << " edges to dense (little) groups." << std::endl;
    std::cout << "[PHASE 3] Assigned " << big_edge_num
              << " edges to sparse (big) groups." << std::endl;

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase31 = gp_clock::now();
    std::cout << "[PROFILE] Phase3_ms=" << gp_ms_since(t_phase30, t_phase31)
              << std::endl;
#endif

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase40 = gp_clock::now();
#endif

    // --- PHASE 4: process each group partition ---
    std::cout << "[PHASE 4] Processing partitions..." << std::endl;

    std::vector<int> global_to_local(static_cast<size_t>(graph->num_vertices),
                                     -1);

    auto process_partition = [&](const std::vector<Edge> &partition_edges,
                                 const std::vector<int> &partition_dst_nodes,
                                 bool is_dense,
                                 unsigned int num_pipelines) {
        PartitionDescriptor pd;
        pd.is_dense = is_dense;
        pd.num_pipelines = num_pipelines;

        if (partition_edges.empty()) {
            pd.num_edges = 0;
            pd.num_vertices = 0;
            pd.num_dsts = static_cast<unsigned int>(partition_dst_nodes.size());
            pd.pipeline_edges.resize(num_pipelines);
            for (unsigned int pip = 0; pip < num_pipelines; ++pip) {
                pd.pipeline_edges[pip].pipeline_id = pip;
                pd.pipeline_edges[pip].num_edges = 0;
                pd.pipeline_edges[pip].offsets.assign(1, 0);
                pd.pipeline_edges[pip].columns.clear();
                pd.pipeline_edges[pip].weights.clear();
                pd.pipeline_edges[pip].edge_props.clear();
            }
            return pd;
        }

        std::vector<int> ordered_dst_vertices = partition_dst_nodes;
        std::srand(42);
        std::random_shuffle(ordered_dst_vertices.begin(),
                            ordered_dst_vertices.end());

        std::vector<int> touched_global_ids;
        touched_global_ids.reserve(ordered_dst_vertices.size() +
                                   partition_edges.size() / 2 + 16);

        int local_id_counter = 0;
        auto map_vertex = [&](int global_id) {
            if (global_to_local[static_cast<size_t>(global_id)] != -1) {
                return;
            }
            global_to_local[static_cast<size_t>(global_id)] = local_id_counter;
            pd.vtx_map[global_id] = local_id_counter;
            pd.vtx_map_rev[local_id_counter] = global_id;
            touched_global_ids.push_back(global_id);
            local_id_counter++;
        };

        for (int global_id : ordered_dst_vertices) {
            map_vertex(global_id);
        }
        pd.num_dsts = static_cast<unsigned int>(ordered_dst_vertices.size());

        for (const auto &edge : partition_edges) {
            map_vertex(edge.src);
            map_vertex(edge.dest);
        }

        pd.num_vertices = static_cast<unsigned int>(local_id_counter);

        std::vector<Edge> local_edges;
        local_edges.reserve(partition_edges.size());
        for (const auto &global_edge : partition_edges) {
            const uint32_t src_id = static_cast<uint32_t>(
                global_to_local[static_cast<size_t>(global_edge.src)]);
            const uint32_t dest_id = static_cast<uint32_t>(
                global_to_local[static_cast<size_t>(global_edge.dest)]);
            const uint32_t weight = static_cast<uint32_t>(global_edge.weight);
            local_edges.push_back({static_cast<int>(src_id),
                                   static_cast<int>(dest_id),
                                   static_cast<int>(weight),
                                   global_edge.props});
        }

        std::sort(local_edges.begin(), local_edges.end(),
                  [](const Edge &a, const Edge &b) { return a.src < b.src; });

        pd.num_edges = static_cast<unsigned int>(local_edges.size());
        const int invalid_dst_id =
            is_dense ? static_cast<int>(INVALID_LOCAL_ID_LITTLE)
                     : static_cast<int>(INVALID_LOCAL_ID_BIG);
        pd.pipeline_edges.resize(num_pipelines);

        const unsigned int edges_per_pipeline =
            (num_pipelines == 0)
                ? 0
                : static_cast<unsigned int>(
                      ceil_div_size_t(pd.num_edges, num_pipelines));

        for (unsigned int pip = 0; pip < num_pipelines; ++pip) {
            std::vector<Edge> cur_pip_edges;
            pd.pipeline_edges[pip].pipeline_id = pip;

            const int start_idx =
                std::min(static_cast<int>(pip * edges_per_pipeline),
                         static_cast<int>(pd.num_edges));
            const int end_idx =
                std::min(start_idx + static_cast<int>(edges_per_pipeline),
                         static_cast<int>(pd.num_edges));

            uint32_t last_src_buffer = 0;
            uint32_t last_src_id = 0;

            for (int edge_idx = start_idx; edge_idx < end_idx; ++edge_idx) {
                auto local_edge = local_edges[edge_idx];
                const uint32_t src_id = static_cast<uint32_t>(local_edge.src);
                const uint32_t dest_id = static_cast<uint32_t>(local_edge.dest);
                const uint32_t weight = static_cast<uint32_t>(local_edge.weight);
                const uint32_t cur_src_buffer = static_cast<uint32_t>(
                    std::floor(static_cast<double>(src_id) / SRC_BUFFER_SIZE));
                if (is_dense && cur_src_buffer != last_src_buffer) {
                    const uint32_t mod8 =
                        static_cast<unsigned int>(cur_pip_edges.size() % 8);
                    if (mod8 != 0) {
                        for (uint32_t pad = 0; pad < (8 - mod8); ++pad) {
                            cur_pip_edges.push_back(
                                {static_cast<int>(last_src_id), invalid_dst_id, 1, {}});
                        }
                    }
                    last_src_buffer = cur_src_buffer;
                }
                last_src_id = src_id;
                cur_pip_edges.push_back({static_cast<int>(src_id),
                                         static_cast<int>(dest_id),
                                         static_cast<int>(weight),
                                         local_edge.props});
            }

            pd.pipeline_edges[pip].num_edges =
                static_cast<unsigned int>(cur_pip_edges.size());

            const int padding_size =
                (8 - (pd.pipeline_edges[pip].num_edges % 8)) % 8;
            pd.pipeline_edges[pip].num_edges +=
                static_cast<unsigned int>(padding_size);

            pd.pipeline_edges[pip].offsets.resize(
                static_cast<size_t>(pd.num_vertices) + 1, 0);
            pd.pipeline_edges[pip].columns.reserve(
                pd.pipeline_edges[pip].num_edges);
            pd.pipeline_edges[pip].weights.reserve(
                pd.pipeline_edges[pip].num_edges);
            if (EDGE_PROP_COUNT > 0) {
                pd.pipeline_edges[pip].edge_props.reserve(
                    static_cast<size_t>(pd.pipeline_edges[pip].num_edges) *
                    EDGE_PROP_COUNT);
            }

            std::vector<int> out_degree(pd.num_vertices, 0);
            for (size_t j = 0; j < cur_pip_edges.size(); ++j) {
                out_degree[static_cast<size_t>(cur_pip_edges[j].src)]++;
                if (j == cur_pip_edges.size() - 1) {
                    for (int p = 0; p < padding_size; ++p) {
                        out_degree[static_cast<size_t>(cur_pip_edges[j].src)]++;
                    }
                }
            }

            for (size_t v = 0; v < static_cast<size_t>(pd.num_vertices); ++v) {
                pd.pipeline_edges[pip].offsets[v + 1] =
                    pd.pipeline_edges[pip].offsets[v] + out_degree[v];
            }

            std::vector<int> current_offset = pd.pipeline_edges[pip].offsets;
            for (size_t j = 0; j < cur_pip_edges.size(); ++j) {
                const int src = cur_pip_edges[j].src;
                int idx = current_offset[static_cast<size_t>(src)]++;
                (void)idx;
                pd.pipeline_edges[pip].columns.push_back(cur_pip_edges[j].dest);
                pd.pipeline_edges[pip].weights.push_back(cur_pip_edges[j].weight);
                if (EDGE_PROP_COUNT > 0) {
                    pd.pipeline_edges[pip].edge_props.insert(
                        pd.pipeline_edges[pip].edge_props.end(),
                        cur_pip_edges[j].props.begin(),
                        cur_pip_edges[j].props.end());
                }
                if (j == cur_pip_edges.size() - 1) {
                    for (int p = 0; p < padding_size; ++p) {
                        idx = current_offset[static_cast<size_t>(src)]++;
                        (void)idx;
                        pd.pipeline_edges[pip].columns.push_back(invalid_dst_id);
                        pd.pipeline_edges[pip].weights.push_back(1);
                        if (EDGE_PROP_COUNT > 0) {
                            pd.pipeline_edges[pip].edge_props.insert(
                                pd.pipeline_edges[pip].edge_props.end(),
                                static_cast<size_t>(EDGE_PROP_COUNT), 0);
                        }
                    }
                }
            }
        }

        for (int global_id : touched_global_ids) {
            global_to_local[static_cast<size_t>(global_id)] = -1;
        }

        return pd;
    };

    for (size_t g = 0; g < container.num_dense_groups; ++g) {
        for (size_t p = 0; p < little_dst_lists[g].size(); ++p) {
            const auto &dst_nodes = little_dst_lists[g][p];
            const auto &partition_edges = dense_edges_lists[g][p];

            PartitionDescriptor pd =
                process_partition(partition_edges, dst_nodes, true,
                                  container.dense_groups[g].num_pipelines);

            container.dense_groups[g].partitions.push_back(pd);
            const size_t part_idx = container.dense_groups[g].partitions.size() -
                                    static_cast<size_t>(1);
            const size_t flat_idx = container.dense_partition_order.size();
            container.dense_partition_order.emplace_back(g, part_idx);
            container.dense_partition_indices[g].push_back(flat_idx);

            std::cout << "  - Dense group " << g << ": partition " << part_idx
                      << " | vertices: " << pd.num_vertices
                      << ", dsts: " << pd.num_dsts
                      << ", edges: " << pd.num_edges
                      << ", pipelines: " << pd.num_pipelines << std::endl;

            for (unsigned int pip = 0; pip < pd.num_pipelines; ++pip) {
                std::cout << "      pipeline " << pip << ": "
                          << pd.pipeline_edges[pip].num_edges << " edges"
                          << std::endl;
            }
        }
    }

    for (size_t g = 0; g < container.num_sparse_groups; ++g) {
        for (size_t p = 0; p < big_dst_lists[g].size(); ++p) {
            const auto &dst_nodes = big_dst_lists[g][p];
            const auto &partition_edges = sparse_edges_lists[g][p];

            PartitionDescriptor pd =
                process_partition(partition_edges, dst_nodes, false,
                                  container.sparse_groups[g].num_pipelines);

            container.sparse_groups[g].partitions.push_back(pd);
            const size_t part_idx = container.sparse_groups[g].partitions.size() -
                                    static_cast<size_t>(1);
            const size_t flat_idx = container.sparse_partition_order.size();
            container.sparse_partition_order.emplace_back(g, part_idx);
            container.sparse_partition_indices[g].push_back(flat_idx);

            std::cout << "  - Sparse group " << g << ": partition " << part_idx
                      << " | vertices: " << pd.num_vertices
                      << ", dsts: " << pd.num_dsts
                      << ", edges: " << pd.num_edges
                      << ", pipelines: " << pd.num_pipelines << std::endl;

            for (unsigned int pip = 0; pip < pd.num_pipelines; ++pip) {
                std::cout << "      pipeline " << pip << ": "
                          << pd.pipeline_edges[pip].num_edges << " edges"
                          << std::endl;
            }
        }
    }

    container.num_dense_partitions =
        static_cast<unsigned int>(container.dense_partition_order.size());
    container.num_sparse_partitions =
        static_cast<unsigned int>(container.sparse_partition_order.size());

#ifdef ENABLE_GRAPH_PREPROCESS_PROFILE
    const auto t_phase41 = gp_clock::now();
    std::cout << "[PROFILE] Phase4_ms=" << gp_ms_since(t_phase40, t_phase41)
              << std::endl;
    std::cout << "[PROFILE] Total_partitionGraph_ms="
              << gp_ms_since(t_total0, t_phase41) << std::endl;
#endif

    std::cout << "[SUCCESS] Graph partitioning and preprocessing complete."
              << std::endl;
    std::cout << "  Dense partitions: " << container.num_dense_partitions
              << " across " << container.num_dense_groups << " groups."
              << std::endl;
    std::cout << "  Sparse partitions: " << container.num_sparse_partitions
              << " across " << container.num_sparse_groups << " groups."
              << std::endl;

    return container;
}
