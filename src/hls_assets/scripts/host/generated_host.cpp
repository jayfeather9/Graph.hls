#include "generated_host.h"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cmath>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <thread>
#include <vector>

static inline uint64_t edge_prop_mask(uint32_t width) {
    return width >= 64 ? ~0ULL : ((1ULL << width) - 1ULL);
}

static inline size_t round_up(size_t value, size_t multiple) {
    if (multiple == 0) {
        return value;
    }
    return ((value + multiple - 1) / multiple) * multiple;
}

static inline void set_distance_slot(bus_word_t &word, size_t slot,
                                     ap_fixed_pod_t pod) {
    const size_t bit_low = slot * DISTANCE_BITWIDTH;
    word.range(bit_low + DISTANCE_BITWIDTH - 1, bit_low) = pod;
}

static inline ap_fixed_pod_t get_distance_slot(const bus_word_t &word,
                                               size_t slot) {
    const size_t bit_low = slot * DISTANCE_BITWIDTH;
    return word.range(bit_low + DISTANCE_BITWIDTH - 1, bit_low);
}

static inline bus_word_t make_identity_word() {
    bus_word_t word = 0;
    for (size_t slot = 0; slot < DIST_PER_WORD; ++slot) {
        set_distance_slot(word, slot, INFINITY_POD);
    }
    return word;
}

static inline ap_uint<EDGE_PAYLOAD_BITS> build_edge_payload(
    uint32_t src_id, uint32_t dst_id, const uint64_t *props) {
    ap_uint<EDGE_PAYLOAD_BITS> payload = 0;
    payload.range(NODE_ID_BITWIDTH - 1, 0) = dst_id;
    payload.range(2 * NODE_ID_BITWIDTH - 1, NODE_ID_BITWIDTH) = src_id;
#if EDGE_PROP_COUNT > 0
    uint32_t bit_offset = 2 * NODE_ID_BITWIDTH;
    for (size_t i = 0; i < EDGE_PROP_COUNT; ++i) {
        const uint32_t width = EDGE_PROP_WIDTHS[i];
        const uint64_t mask = edge_prop_mask(width);
        const uint64_t value = props ? (props[i] & mask) : 0;
        payload.range(bit_offset + width - 1, bit_offset) = value;
        bit_offset += width;
    }
#endif
    return payload;
}

AlgorithmHost::AlgorithmHost(AccDescriptor &acc, AlgorithmConfig config)
    : acc(acc), config(config) {}

void AlgorithmHost::prepare_data(const PartitionContainer &container,
                                 int start_node) {
    std::cout << "--- [Host] Phase 0: Preparing data structures ---"
              << std::endl;

    auto start_time = std::chrono::system_clock::now();
    auto current_time = start_time;

    // 1. Initialize algorithm state
    m_num_vertices = container.num_graph_vertices;
    const char *debug_edges_env = std::getenv("GRAPHYFLOW_DEBUG_EDGES");
    const bool debug_edges =
        (debug_edges_env != nullptr && debug_edges_env[0] != '\0');

    h_distances.assign(m_num_vertices, distance_t(0));
    switch (config.algorithm_kind) {
    case AlgorithmKind::Sssp:
        h_distances.assign(
            m_num_vertices,
            (!DISTANCE_SIGNED && DISTANCE_BITWIDTH == 8) ? distance_t(126)
                                                         : INFINITY_DIST_VAL);
        if (start_node >= 0 && start_node < m_num_vertices) {
            h_distances[start_node] = distance_t(0);
        }
        break;
    case AlgorithmKind::ConnectedComponents:
        if (DISTANCE_BITWIDTH < 32) {
            const uint64_t max_label = DISTANCE_SIGNED
                                           ? ((uint64_t(1) << (DISTANCE_BITWIDTH - 1)) - 1)
                                           : ((uint64_t(1) << DISTANCE_BITWIDTH) - 1);
            if (static_cast<uint64_t>(m_num_vertices) > max_label) {
                std::cout << "Warning: CC labels exceed "
                          << DISTANCE_BITWIDTH
                          << "-bit range; values will wrap." << std::endl;
            }
        }
        for (int u = 0; u < m_num_vertices; ++u) {
            h_distances[u] = distance_t(u);
        }
        break;
    case AlgorithmKind::Bfs: {
        const ap_fixed_pod_t inf =
            static_cast<ap_fixed_pod_t>(config.inf_value != 0u ? config.inf_value
                                                               : 0x7ffffffeu);
        h_distances.assign(m_num_vertices, static_cast<distance_t>(inf));
        if (start_node >= 0 && start_node < m_num_vertices) {
            const ap_fixed_pod_t seed =
                static_cast<ap_fixed_pod_t>(config.active_mask | 1u);
            h_distances[start_node] = static_cast<distance_t>(seed);
        }
        break;
    }
    case AlgorithmKind::Pagerank:
        for (int u = 0; u < m_num_vertices; ++u) {
            h_distances[u] = distance_t(1);
        }
        break;
    case AlgorithmKind::ArticleRank:
        h_distances.assign(m_num_vertices, distance_t(0));
        break;
    case AlgorithmKind::Wcc:
        if (DISTANCE_BITWIDTH < 32) {
            const uint64_t max_label =
                DISTANCE_SIGNED
                    ? ((uint64_t(1) << (DISTANCE_BITWIDTH - 1)) - 1)
                    : ((uint64_t(1) << DISTANCE_BITWIDTH) - 1);
            if (static_cast<uint64_t>(m_num_vertices) > max_label) {
                std::cout << "Warning: WCC labels exceed "
                          << DISTANCE_BITWIDTH
                          << "-bit range; values will wrap." << std::endl;
            }
        }
        for (int u = 0; u < m_num_vertices; ++u) {
            h_distances[u] = distance_t(u);
        }
        break;
    }

    h_aux_props.assign(m_num_vertices, distance_t(0));
    if (config.needs_out_degree) {
        std::vector<uint32_t> outdeg = container.graph_out_degrees;
        if (outdeg.size() != static_cast<size_t>(m_num_vertices)) {
            outdeg.assign(static_cast<size_t>(m_num_vertices), 0u);
        }

        if (config.algorithm_kind == AlgorithmKind::ArticleRank) {
            uint64_t total = 0;
            for (uint32_t deg : outdeg) {
                total += static_cast<uint64_t>(deg);
            }
            const uint32_t avg =
                m_num_vertices > 0
                    ? static_cast<uint32_t>(
                          total / static_cast<uint64_t>(m_num_vertices))
                    : 0u;
            for (size_t v = 0; v < outdeg.size(); ++v) {
                uint32_t denom = outdeg[v] + avg;
                if (denom == 0u) {
                    denom = 1u;
                }
                h_aux_props[v] = distance_t(denom);
            }
        } else {
            for (size_t v = 0; v < outdeg.size(); ++v) {
                uint32_t denom = outdeg[v];
                if (denom == 0u) {
                    denom = 1u;
                }
                h_aux_props[v] = distance_t(denom);
            }
        }
    }

    h_pr_contrib.clear();
    h_pr_out_degree.clear();
    h_pr_has_incoming.clear();
    pr_base_arg = 0;
    if (config.algorithm_kind == AlgorithmKind::Pagerank) {
        h_pr_out_degree = container.graph_out_degrees;
        if (h_pr_out_degree.size() != static_cast<size_t>(m_num_vertices)) {
            h_pr_out_degree.assign(static_cast<size_t>(m_num_vertices), 0u);
        }

        h_pr_has_incoming.assign(static_cast<size_t>(m_num_vertices), 0u);
        auto mark_partition_dsts = [&](const PartitionDescriptor &partition) {
            for (int local_id = 0; local_id < partition.num_dsts; ++local_id) {
                auto it = partition.vtx_map_rev.find(local_id);
                if (it == partition.vtx_map_rev.end()) {
                    continue;
                }
                const int global_id = it->second;
                if (global_id >= 0 && global_id < m_num_vertices) {
                    h_pr_has_incoming[static_cast<size_t>(global_id)] = 1u;
                }
            }
        };
        for (const auto &group : container.dense_groups) {
            for (const auto &partition : group.partitions) {
                mark_partition_dsts(partition);
            }
        }
        for (const auto &group : container.sparse_groups) {
            for (const auto &partition : group.partitions) {
                mark_partition_dsts(partition);
            }
        }

        const uint64_t scale = (1ull << 30);
        const uint32_t inv_n =
            (m_num_vertices > 0)
                ? static_cast<uint32_t>(scale / static_cast<uint64_t>(m_num_vertices))
                : 0u;
        pr_base_arg =
            (m_num_vertices > 0)
                ? static_cast<uint32_t>(
                      (scale * 15ull) /
                      (100ull * static_cast<uint64_t>(m_num_vertices)))
                : 0u;

        h_pr_contrib.assign(static_cast<size_t>(m_num_vertices), 0);
        for (int u = 0; u < m_num_vertices; ++u) {
            const uint32_t od = h_pr_out_degree[static_cast<size_t>(u)];
            h_pr_contrib[static_cast<size_t>(u)] =
                (od != 0u) ? static_cast<int32_t>(inv_n / od) : 0;
        }
    }

    auto node_prop_pod_for = [&](int global_id) -> ap_fixed_pod_t {
        if (config.algorithm_kind == AlgorithmKind::Pagerank) {
            return static_cast<ap_fixed_pod_t>(
                static_cast<uint32_t>(h_pr_contrib[static_cast<size_t>(global_id)]));
        }
        return distance_to_pod(h_distances[global_id]);
    };

    auto apply_prop_pod_for = [&](int global_id) -> ap_fixed_pod_t {
        if (config.algorithm_kind == AlgorithmKind::Pagerank) {
            return static_cast<ap_fixed_pod_t>(
                h_pr_out_degree[static_cast<size_t>(global_id)]);
        }
        return distance_to_pod(h_distances[global_id]);
    };
    // 2. Prepare host-side input buffers for each pipeline
    const size_t bytes_per_word = AXI_BUS_WIDTH / 8;
    dense_buffers.resize(container.num_dense_partitions);
    sparse_buffers.resize(container.num_sparse_partitions);

    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        dense_buffers[flat_idx].pipelines.resize(group.num_pipelines);
        dense_buffers[flat_idx].node_prop_offset = 0;
        dense_buffers[flat_idx].dst_prop_offset = 0;
        dense_buffers[flat_idx].src_buf_offset = 0;
    }
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        sparse_buffers[flat_idx].pipelines.resize(group.num_pipelines);
        sparse_buffers[flat_idx].node_prop_offset = 0;
        sparse_buffers[flat_idx].dst_prop_offset = 0;
        sparse_buffers[flat_idx].src_prop_offset = 0;
    }

    size_t sparse_src_prop_offset = 0;
    size_t sparse_node_offset = 0;
    size_t sparse_dst_offset = 0;
    size_t dense_node_offset = 0;
    size_t dense_src_buf_offset = 0;
    size_t dense_dst_offset = 0;
    int32_t previous_group = -1;

    // --- 2.1: Prepare BIG partition data (shared node props, separate edge
    // props per pipeline) ---
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        const auto &big_partition = group.partitions[group_part.second];

        if (previous_group != static_cast<int32_t>(group_part.first)) {
            sparse_src_prop_offset = 0;
            sparse_node_offset = 0;
            sparse_dst_offset = 0;
            previous_group = static_cast<int32_t>(group_part.first);
        }

        // Pack node distances ONCE for the big partition (shared)
        {
            const size_t dist_per_word = DIST_PER_WORD;
            const size_t word_number =
                (big_partition.num_vertices + dist_per_word - 1) /
                dist_per_word;
            const size_t node_words = (word_number == 0) ? 1 : word_number;
            sparse_buffers[flat_idx].packed_node_props.assign(node_words,
                                                              make_identity_word());

            for (int j = 0; j < big_partition.num_vertices; ++j) {
                const int global_id = big_partition.vtx_map_rev.at(j);
                const ap_fixed_pod_t pod = node_prop_pod_for(global_id);
                const size_t word_idx = j / dist_per_word;
                const size_t slot = j % dist_per_word;
                set_distance_slot(
                    sparse_buffers[flat_idx].packed_node_props[word_idx], slot,
                    pod);
            }
            sparse_buffers[flat_idx].src_prop_offset =
                static_cast<uint32_t>(sparse_src_prop_offset);
            sparse_src_prop_offset += node_words;
            sparse_buffers[flat_idx].node_prop_offset =
                static_cast<uint32_t>(sparse_node_offset);
            sparse_node_offset += node_words;

            const size_t dst_word_number =
                (big_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            sparse_buffers[flat_idx].packed_dst_props.assign(dst_word_number,
                                                             make_identity_word());
            for (int j = 0; j < big_partition.num_dsts; ++j) {
                const int global_id = big_partition.vtx_map_rev.at(j);
                const ap_fixed_pod_t pod = apply_prop_pod_for(global_id);
                const size_t word_idx = j / dist_per_word;
                const size_t slot = j % dist_per_word;
                set_distance_slot(
                    sparse_buffers[flat_idx].packed_dst_props[word_idx], slot,
                    pod);
            }
            sparse_buffers[flat_idx].dst_prop_offset =
                static_cast<uint32_t>(sparse_dst_offset);
            sparse_dst_offset += dst_word_number;
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared shared big node props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;

        // Pack edge properties for EACH big pipeline
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const auto &pipeline_edges = big_partition.pipeline_edges[pip];
            // Pack this pipeline's edge properties
            const size_t edges_per_word = EDGES_PER_WORD;

            const size_t actual_edges = pipeline_edges.num_edges;
            const size_t padded_edges =
                (actual_edges == 0)
                    ? edges_per_word
                    : ((actual_edges + edges_per_word - 1) / edges_per_word) *
                          edges_per_word;
            const size_t word_number = padded_edges / edges_per_word;
            sparse_buffers[flat_idx].pipelines[pip].packed_edge_props.assign(
                word_number, 0);

            std::vector<uint64_t> dummy_props;
            if (EDGE_PROP_COUNT > 0) {
                dummy_props.assign(EDGE_PROP_COUNT, 0);
                dummy_props[0] = 1;
            }

            if (actual_edges == 0) {
                printf(
                    "Padding SP flat_idx=%zu (group=%zu, part=%zu), big pipe "
                    "%zu with %zu dummy edges.\n",
                    flat_idx, group_part.first, group_part.second, pip,
                    padded_edges);
                const uint32_t dummy_dst_id = INVALID_LOCAL_ID_BIG;
                const uint32_t dummy_src_id = 0;

                for (size_t dummy_idx = 0; dummy_idx < padded_edges;
                     ++dummy_idx) {
                    const size_t word_idx = dummy_idx / edges_per_word;
                    const size_t slot = dummy_idx % edges_per_word;
                    auto payload = build_edge_payload(
                        dummy_src_id, dummy_dst_id,
                        EDGE_PROP_COUNT > 0 ? dummy_props.data() : nullptr);
                    sparse_buffers[flat_idx].pipelines[pip]
                        .packed_edge_props[word_idx]
                        .range(slot * EDGE_PAYLOAD_BITS + EDGE_PAYLOAD_BITS - 1,
                               slot * EDGE_PAYLOAD_BITS) = payload;
                }
            } else {
                for (int v = 0; v < big_partition.num_vertices; ++v) {
                    node_id_t src_id = v;
                    for (int edge_idx = pipeline_edges.offsets[v];
                         edge_idx < pipeline_edges.offsets[v + 1];
                         ++edge_idx) {
                        uint32_t dest_id = pipeline_edges.columns[edge_idx];
                        const uint64_t *props = nullptr;
                        if (EDGE_PROP_COUNT > 0) {
                            props = pipeline_edges.edge_props.data() +
                                    static_cast<size_t>(edge_idx) *
                                        EDGE_PROP_COUNT;
                        }
                        const size_t word_idx =
                            static_cast<size_t>(edge_idx) / edges_per_word;
                        const size_t slot =
                            static_cast<size_t>(edge_idx) % edges_per_word;
                        auto payload =
                            build_edge_payload(src_id, dest_id, props);
                        sparse_buffers[flat_idx].pipelines[pip]
                            .packed_edge_props[word_idx]
                            .range(slot * EDGE_PAYLOAD_BITS +
                                       EDGE_PAYLOAD_BITS - 1,
                                   slot * EDGE_PAYLOAD_BITS) = payload;
                    }
                }
                const size_t padding_edges = padded_edges - actual_edges;
                if (padding_edges > 0) {
                    const uint32_t dummy_dst_id = INVALID_LOCAL_ID_BIG;
                    const uint32_t dummy_src_id = 0;
                    for (size_t dummy_idx = 0; dummy_idx < padding_edges;
                         ++dummy_idx) {
                        const size_t edge_idx = actual_edges + dummy_idx;
                        const size_t word_idx = edge_idx / edges_per_word;
                        const size_t slot = edge_idx % edges_per_word;
                        auto payload = build_edge_payload(
                            dummy_src_id, dummy_dst_id,
                            EDGE_PROP_COUNT > 0 ? dummy_props.data() : nullptr);
                        sparse_buffers[flat_idx].pipelines[pip]
                            .packed_edge_props[word_idx]
                            .range(slot * EDGE_PAYLOAD_BITS +
                                       EDGE_PAYLOAD_BITS - 1,
                                   slot * EDGE_PAYLOAD_BITS) = payload;
                    }
                }
            }
            if (debug_edges && m_num_vertices <= 16) {
                const auto &edge_words =
                    sparse_buffers[flat_idx].pipelines[pip].packed_edge_props;
                std::cout << "[DEBUG] big partition flat_idx=" << flat_idx
                          << " (group=" << group_part.first
                          << ", part=" << group_part.second
                          << ") pipeline " << pip << " edges:" << std::endl;
                for (size_t w = 0; w < edge_words.size(); ++w) {
                    bus_word_t word = edge_words[w];
                    for (size_t e = 0; e < edges_per_word; ++e) {
                        ap_uint<EDGE_PAYLOAD_BITS> packed_edge =
                            word.range((EDGE_PAYLOAD_BITS - 1) +
                                           (e * EDGE_PAYLOAD_BITS),
                                       e * EDGE_PAYLOAD_BITS);
                        uint32_t dst_id =
                            packed_edge.range(NODE_ID_BITWIDTH - 1, 0)
                                .to_uint();
                        uint32_t src_id =
                            packed_edge
                                .range(2 * NODE_ID_BITWIDTH - 1,
                                       NODE_ID_BITWIDTH)
                                .to_uint();
                        std::cout << "  src=" << src_id << " dst=" << dst_id
                                  << std::endl;
                    }
                }
            }
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared " << group.num_pipelines
            << " big pipeline edge props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;
    }

    // --- 2.2: Prepare LITTLE partition data (shared node props, separate edge
    // props per pipeline) ---
    previous_group = -1;
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        const auto &little_partition = group.partitions[group_part.second];

        if (previous_group != static_cast<int32_t>(group_part.first)) {
            dense_node_offset = 0;
            dense_dst_offset = 0;
            dense_src_buf_offset = 0;
            previous_group = static_cast<int32_t>(group_part.first);
        }

        // Pack node distances ONCE for the little partition (shared)
        {
            const size_t dist_per_word = DIST_PER_WORD;
            size_t padded_vertices =
                round_up(little_partition.num_vertices, SRC_BUFFER_SIZE);
            if (padded_vertices == 0) {
                // Keep little-kernel src-buffer addressing well-defined even when a
                // partition is empty. The little kernel can prefetch round+1, so
                // we need at least one full SRC_BUFFER block.
                padded_vertices = SRC_BUFFER_SIZE;
            }
            const size_t word_number = padded_vertices / dist_per_word;
            const size_t node_words = (word_number == 0) ? 1 : word_number;
            dense_buffers[flat_idx].packed_node_props.assign(node_words,
                                                             make_identity_word());

            for (int j = 0; j < little_partition.num_vertices; ++j) {
                const int global_id = little_partition.vtx_map_rev.at(j);
                const ap_fixed_pod_t pod = node_prop_pod_for(global_id);
                const size_t word_idx = j / dist_per_word;
                const size_t slot = j % dist_per_word;
                set_distance_slot(
                    dense_buffers[flat_idx].packed_node_props[word_idx], slot,
                    pod);
            }
            const size_t src_buf_cnt = padded_vertices / SRC_BUFFER_SIZE;

            if (debug_edges && m_num_vertices <= 16 &&
                little_partition.num_vertices > 0) {
                std::cout << "[DEBUG] little partition flat_idx=" << flat_idx
                          << " (group=" << group_part.first
                          << ", part=" << group_part.second
                          << ") node props (first word):" << std::endl;
                bus_word_t word = dense_buffers[flat_idx].packed_node_props[0];
                for (size_t d = 0;
                     d < std::min<size_t>(dist_per_word,
                                          little_partition.num_vertices);
                     ++d) {
                    ap_fixed_pod_t dist_pod = get_distance_slot(word, d);
                    distance_t dist_val = pod_to_distance(dist_pod);
                    std::cout << "  local_id=" << d
                              << " dist=" << dist_val.to_int() << std::endl;
                }
            }
            dense_buffers[flat_idx].node_prop_offset =
                static_cast<uint32_t>(dense_node_offset);
            dense_node_offset += node_words;
            dense_buffers[flat_idx].src_buf_offset =
                static_cast<uint32_t>(dense_src_buf_offset);
            dense_src_buf_offset += src_buf_cnt;

            const size_t dst_word_number =
                (little_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            dense_buffers[flat_idx].packed_dst_props.assign(dst_word_number,
                                                            make_identity_word());
            for (int j = 0; j < little_partition.num_dsts; ++j) {
                const int global_id = little_partition.vtx_map_rev.at(j);
                const ap_fixed_pod_t pod = apply_prop_pod_for(global_id);
                const size_t word_idx = j / dist_per_word;
                const size_t slot = j % dist_per_word;
                set_distance_slot(
                    dense_buffers[flat_idx].packed_dst_props[word_idx], slot,
                    pod);
            }
            dense_buffers[flat_idx].dst_prop_offset =
                static_cast<uint32_t>(dense_dst_offset);
            dense_dst_offset += dst_word_number;
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared shared little node props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;

        // Pack edge properties for EACH little pipeline
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const auto &pipeline_edges = little_partition.pipeline_edges[pip];

            // Pack this pipeline's edge properties
            const size_t edges_per_word = EDGES_PER_WORD;

            const size_t actual_edges = pipeline_edges.num_edges;
            const size_t padded_edges =
                (actual_edges == 0)
                    ? edges_per_word
                    : ((actual_edges + edges_per_word - 1) / edges_per_word) *
                          edges_per_word;
            const size_t word_number = padded_edges / edges_per_word;
            dense_buffers[flat_idx].pipelines[pip].packed_edge_props.assign(
                word_number, 0);

            std::vector<uint64_t> dummy_props;
            if (EDGE_PROP_COUNT > 0) {
                dummy_props.assign(EDGE_PROP_COUNT, 0);
                dummy_props[0] = 1;
            }

            if (actual_edges == 0) {
                printf(
                    "Padding DP flat_idx=%zu (group=%zu, part=%zu), little "
                    "pipe %zu with %zu dummy edges.\n",
                    flat_idx, group_part.first, group_part.second, pip,
                    padded_edges);
                const uint32_t dummy_dst_id = INVALID_LOCAL_ID_LITTLE;
                const uint32_t dummy_src_id = 0;

                for (size_t dummy_idx = 0; dummy_idx < padded_edges;
                     ++dummy_idx) {
                    const size_t word_idx = dummy_idx / edges_per_word;
                    const size_t slot = dummy_idx % edges_per_word;
                    auto payload = build_edge_payload(
                        dummy_src_id, dummy_dst_id,
                        EDGE_PROP_COUNT > 0 ? dummy_props.data() : nullptr);
                    dense_buffers[flat_idx].pipelines[pip]
                        .packed_edge_props[word_idx]
                        .range(slot * EDGE_PAYLOAD_BITS + EDGE_PAYLOAD_BITS - 1,
                               slot * EDGE_PAYLOAD_BITS) = payload;
                }
            } else {
                for (int v = 0; v < little_partition.num_vertices; ++v) {
                    node_id_t src_id = v;
                    for (int edge_idx = pipeline_edges.offsets[v];
                         edge_idx < pipeline_edges.offsets[v + 1];
                         ++edge_idx) {
                        uint32_t dest_id = pipeline_edges.columns[edge_idx];
                        const uint64_t *props = nullptr;
                        if (EDGE_PROP_COUNT > 0) {
                            props = pipeline_edges.edge_props.data() +
                                    static_cast<size_t>(edge_idx) *
                                        EDGE_PROP_COUNT;
                        }
                        const size_t word_idx =
                            static_cast<size_t>(edge_idx) / edges_per_word;
                        const size_t slot =
                            static_cast<size_t>(edge_idx) % edges_per_word;
                        auto payload =
                            build_edge_payload(src_id, dest_id, props);
                        dense_buffers[flat_idx].pipelines[pip]
                            .packed_edge_props[word_idx]
                            .range(slot * EDGE_PAYLOAD_BITS +
                                       EDGE_PAYLOAD_BITS - 1,
                                   slot * EDGE_PAYLOAD_BITS) = payload;
                    }
                }
                const size_t padding_edges = padded_edges - actual_edges;
                if (padding_edges > 0) {
                    const uint32_t dummy_dst_id = INVALID_LOCAL_ID_LITTLE;
                    const uint32_t dummy_src_id = 0;
                    for (size_t dummy_idx = 0; dummy_idx < padding_edges;
                         ++dummy_idx) {
                        const size_t edge_idx = actual_edges + dummy_idx;
                        const size_t word_idx = edge_idx / edges_per_word;
                        const size_t slot = edge_idx % edges_per_word;
                        auto payload = build_edge_payload(
                            dummy_src_id, dummy_dst_id,
                            EDGE_PROP_COUNT > 0 ? dummy_props.data() : nullptr);
                        dense_buffers[flat_idx].pipelines[pip]
                            .packed_edge_props[word_idx]
                            .range(slot * EDGE_PAYLOAD_BITS +
                                       EDGE_PAYLOAD_BITS - 1,
                                   slot * EDGE_PAYLOAD_BITS) = payload;
                    }
                }
            }
            if (debug_edges && m_num_vertices <= 16) {
                const auto &edge_words =
                    dense_buffers[flat_idx].pipelines[pip].packed_edge_props;
                std::cout << "[DEBUG] little partition flat_idx=" << flat_idx
                          << " (group=" << group_part.first
                          << ", part=" << group_part.second
                          << ") pipeline " << pip << " edges:" << std::endl;
                for (size_t w = 0; w < edge_words.size(); ++w) {
                    bus_word_t word = edge_words[w];
                    for (size_t e = 0; e < edges_per_word; ++e) {
                        ap_uint<EDGE_PAYLOAD_BITS> packed_edge =
                            word.range((EDGE_PAYLOAD_BITS - 1) +
                                           (e * EDGE_PAYLOAD_BITS),
                                       e * EDGE_PAYLOAD_BITS);
                        uint32_t dst_id =
                            packed_edge.range(NODE_ID_BITWIDTH - 1, 0)
                                .to_uint();
                        uint32_t src_id =
                            packed_edge
                                .range(2 * NODE_ID_BITWIDTH - 1,
                                       NODE_ID_BITWIDTH)
                                .to_uint();
                        std::cout << "  src=" << src_id << " dst=" << dst_id
                                  << std::endl;
                    }
                }
            }
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared " << group.num_pipelines
            << " little pipeline edge props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;
    }

    // apply kernel node props layout:
    // [ all little groups concatenated by partition ][ all big groups concatenated by partition ]
    size_t total_little_dst_words = 0;
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        total_little_dst_words += dense_buffers[flat_idx].packed_dst_props.size();
    }
    size_t total_big_dst_words = 0;
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        total_big_dst_words += sparse_buffers[flat_idx].packed_dst_props.size();
    }

    apply_kernel_node_props.assign(total_little_dst_words + total_big_dst_words,
                                   0);
    if (apply_kernel_node_props.empty()) {
        apply_kernel_node_props.assign(1, 0);
    }
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
    apply_kernel_aux_node_props.assign(apply_kernel_node_props.size(), 0);
#endif

    little_merger_lengths.assign(container.num_dense_groups, 0);
    little_merger_offsets.assign(container.num_dense_groups, 0);
    big_merger_lengths.assign(container.num_sparse_groups, 0);
    big_merger_offsets.assign(container.num_sparse_groups, 0);

    size_t apply_offset = 0;
    for (size_t group_idx = 0; group_idx < container.num_dense_groups;
         ++group_idx) {
        little_merger_offsets[group_idx] = static_cast<uint32_t>(apply_offset);
        for (size_t part_idx = 0;
             part_idx < container.dense_groups[group_idx].partitions.size();
             ++part_idx) {
            const size_t flat_idx =
                container.dense_partition_indices[group_idx][part_idx];
            const auto &dst_props = dense_buffers[flat_idx].packed_dst_props;
            if (!dst_props.empty()) {
                std::memcpy(apply_kernel_node_props.data() + apply_offset,
                            dst_props.data(),
                            dst_props.size() * sizeof(bus_word_t));
                if (config.needs_out_degree) {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                    const auto &part =
                        container.dense_groups[group_idx].partitions[part_idx];
                    for (int j = 0; j < part.num_dsts; ++j) {
                        const int global_id = part.vtx_map_rev.at(j);
                        const ap_fixed_pod_t pod =
                            distance_to_pod(h_aux_props[global_id]);
                        const size_t word_idx =
                            static_cast<size_t>(j) / DIST_PER_WORD;
                        const size_t slot =
                            static_cast<size_t>(j) % DIST_PER_WORD;
                        set_distance_slot(
                            apply_kernel_aux_node_props[apply_offset + word_idx],
                            slot, pod);
                    }
#endif
                } else {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                    std::memcpy(apply_kernel_aux_node_props.data() + apply_offset,
                                dst_props.data(),
                                dst_props.size() * sizeof(bus_word_t));
#endif
                }
            }
            apply_offset += dst_props.size();
        }
        little_merger_lengths[group_idx] = static_cast<uint32_t>(
            apply_offset - static_cast<size_t>(little_merger_offsets[group_idx]));
    }

    big_dst_offset = static_cast<uint32_t>(apply_offset);
    for (size_t group_idx = 0; group_idx < container.num_sparse_groups;
         ++group_idx) {
        big_merger_offsets[group_idx] = static_cast<uint32_t>(apply_offset);
        for (size_t part_idx = 0;
             part_idx < container.sparse_groups[group_idx].partitions.size();
             ++part_idx) {
            const size_t flat_idx =
                container.sparse_partition_indices[group_idx][part_idx];
            const auto &dst_props = sparse_buffers[flat_idx].packed_dst_props;
            if (!dst_props.empty()) {
                std::memcpy(apply_kernel_node_props.data() + apply_offset,
                            dst_props.data(),
                            dst_props.size() * sizeof(bus_word_t));
                if (config.needs_out_degree) {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                    const auto &part =
                        container.sparse_groups[group_idx].partitions[part_idx];
                    for (int j = 0; j < part.num_dsts; ++j) {
                        const int global_id = part.vtx_map_rev.at(j);
                        const ap_fixed_pod_t pod =
                            distance_to_pod(h_aux_props[global_id]);
                        const size_t word_idx =
                            static_cast<size_t>(j) / DIST_PER_WORD;
                        const size_t slot =
                            static_cast<size_t>(j) % DIST_PER_WORD;
                        set_distance_slot(
                            apply_kernel_aux_node_props[apply_offset + word_idx],
                            slot, pod);
                    }
#endif
                } else {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                    std::memcpy(apply_kernel_aux_node_props.data() + apply_offset,
                                dst_props.data(),
                                dst_props.size() * sizeof(bus_word_t));
#endif
                }
            }
            apply_offset += dst_props.size();
        }
        big_merger_lengths[group_idx] = static_cast<uint32_t>(
            apply_offset - static_cast<size_t>(big_merger_offsets[group_idx]));
    }

    const size_t little_guard_words = SRC_BUFFER_SIZE / DIST_PER_WORD;
    // `graphyflow_little::request_manager` may prefetch up to one extra
    // SRC_BUFFER_SIZE "round" (pp_read_round + 1). For tiny graphs where the
    // real node-prop payload is much smaller than a round, we still need enough
    // identity padding so that those prefetched reads remain in-bounds.
    const size_t little_prefetch_guard_words = little_guard_words;
    const bus_word_t identity_word = make_identity_word();

    std::vector<size_t> dense_group_node_words(container.num_dense_groups, 1);
    std::vector<size_t> sparse_group_node_words(container.num_sparse_groups, 1);

    for (size_t group_idx = 0; group_idx < container.num_dense_groups;
         ++group_idx) {
        size_t total_words = 0;
        for (size_t part_idx = 0;
             part_idx < container.dense_groups[group_idx].partitions.size();
             ++part_idx) {
            const size_t flat_idx =
                container.dense_partition_indices[group_idx][part_idx];
            const size_t end =
                static_cast<size_t>(dense_buffers[flat_idx].node_prop_offset) +
                dense_buffers[flat_idx].packed_node_props.size();
            total_words = std::max(total_words, end);
        }
        dense_group_node_words[group_idx] =
            std::max<size_t>(static_cast<size_t>(1), total_words);
    }
    for (size_t group_idx = 0; group_idx < container.num_sparse_groups;
         ++group_idx) {
        size_t total_words = 0;
        for (size_t part_idx = 0;
             part_idx < container.sparse_groups[group_idx].partitions.size();
             ++part_idx) {
            const size_t flat_idx =
                container.sparse_partition_indices[group_idx][part_idx];
            const size_t end =
                static_cast<size_t>(sparse_buffers[flat_idx].node_prop_offset) +
                sparse_buffers[flat_idx].packed_node_props.size();
            total_words = std::max(total_words, end);
        }
        sparse_group_node_words[group_idx] =
            std::max<size_t>(static_cast<size_t>(1), total_words);
    }

    writer_kernel_node_props.resize(LITTLE_KERNEL_NUM + BIG_KERNEL_NUM);
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        const uint32_t group_id = LITTLE_KERNEL_GROUP_ID[pip];
        const size_t group_words = (group_id < dense_group_node_words.size())
                                       ? dense_group_node_words[group_id]
                                       : static_cast<size_t>(1);
        writer_kernel_node_props[pip].assign(
            group_words + little_guard_words + little_prefetch_guard_words,
            identity_word);
    }
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        const uint32_t group_id = BIG_KERNEL_GROUP_ID[pip];
        const size_t group_words = (group_id < sparse_group_node_words.size())
                                       ? sparse_group_node_words[group_id]
                                       : static_cast<size_t>(1);
        writer_kernel_node_props[pip + LITTLE_KERNEL_NUM].assign(group_words,
                                                                 identity_word);
    }

    // Fill per-pipeline src_prop buffers with partition node props (group-local offsets).
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip = static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= static_cast<size_t>(LITTLE_KERNEL_NUM)) {
                continue;
            }
            std::copy(dense_buffers[flat_idx].packed_node_props.begin(),
                      dense_buffers[flat_idx].packed_node_props.end(),
                      writer_kernel_node_props[global_pip].begin() +
                          dense_buffers[flat_idx].node_prop_offset);
        }
    }
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip = static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= static_cast<size_t>(BIG_KERNEL_NUM)) {
                continue;
            }
            std::copy(sparse_buffers[flat_idx].packed_node_props.begin(),
                      sparse_buffers[flat_idx].packed_node_props.end(),
                      writer_kernel_node_props[static_cast<size_t>(LITTLE_KERNEL_NUM) +
                                               global_pip]
                              .begin() +
                          sparse_buffers[flat_idx].node_prop_offset);
        }
    }
    current_time = std::chrono::system_clock::now();
    std::cout
        << "--- [Host] Phase 0: Prepared apply kernel node props ("
        << std::chrono::duration<double>(current_time - start_time).count()
        << " sec) ---" << std::endl;
}

// --- PHASE 1: BUFFER SETUP ---
// MODIFIED: Create separate edge buffers for each pipeline, but share node
// buffers within partition
void AlgorithmHost::setup_buffers(const PartitionContainer &container) {
    cl_int err;
    std::cout
        << "--- [Host] Phase 1: Setting up HBM buffers for all pipelines ---"
        << std::endl;

    // 1.1: Clear old buffer handles and resize host-side result vectors
    writer_kernel_node_prop_buffers.clear();

    const size_t bytes_per_word = AXI_BUS_WIDTH / 8;
    size_t total_output_words = apply_kernel_node_props.size();

    // --- 1.2: Setup buffers for LITTLE pipelines ---
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];

        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= acc.little_kernel_hbm_edge_id.size()) {
                std::cerr << "[ERROR] little global_pip out of range: "
                          << global_pip << std::endl;
                std::exit(EXIT_FAILURE);
            }

            cl::Buffer edge_props_buf;

            cl_mem_ext_ptr_t hbm_ext_edge;
            hbm_ext_edge.flags =
                XCL_MEM_TOPOLOGY | acc.little_kernel_hbm_edge_id[global_pip];
            hbm_ext_edge.obj =
                dense_buffers[flat_idx].pipelines[pip].packed_edge_props.data();
            hbm_ext_edge.param = 0;

            const size_t num_edge_words =
                dense_buffers[flat_idx].pipelines[pip].packed_edge_props.size();
            printf(
                "LITTLE DP flat_idx=%zu (group=%zu, part=%zu), global_pip=%zu, "
                "edge words %d\n",
                flat_idx, group_part.first, group_part.second, global_pip,
                (int)num_edge_words);
            OCL_CHECK(err, edge_props_buf = cl::Buffer(
                               acc.context,
                               CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                   CL_MEM_USE_HOST_PTR,
                               num_edge_words * bytes_per_word, &hbm_ext_edge,
                               &err));

            dense_buffers[flat_idx].pipelines[pip].edge_props_buffer =
                edge_props_buf;
        }
    }

    // --- 1.3: Setup buffers for BIG pipelines ---
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];

        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= acc.big_kernel_hbm_edge_id.size()) {
                std::cerr << "[ERROR] big global_pip out of range: " << global_pip
                          << std::endl;
                std::exit(EXIT_FAILURE);
            }

            cl::Buffer edge_props_buf;

            cl_mem_ext_ptr_t hbm_ext_edge;
            hbm_ext_edge.flags =
                XCL_MEM_TOPOLOGY | acc.big_kernel_hbm_edge_id[global_pip];
            hbm_ext_edge.obj =
                sparse_buffers[flat_idx].pipelines[pip].packed_edge_props.data();
            hbm_ext_edge.param = 0;

            const size_t num_edge_words =
                sparse_buffers[flat_idx].pipelines[pip].packed_edge_props.size();
            printf(
                "BIG SP flat_idx=%zu (group=%zu, part=%zu), global_pip=%zu, "
                "edge words %d\n",
                flat_idx, group_part.first, group_part.second, global_pip,
                (int)num_edge_words);
            OCL_CHECK(err, edge_props_buf = cl::Buffer(
                               acc.context,
                               CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                   CL_MEM_USE_HOST_PTR,
                               num_edge_words * bytes_per_word, &hbm_ext_edge,
                               &err));

            sparse_buffers[flat_idx].pipelines[pip].edge_props_buffer =
                edge_props_buf;
        }
    }

    // --- 1.4: Setup shared node property buffers for hbm_writer (14 total: 11
    // little + 3 big) --- Create 11 little node prop buffers for hbm_writer
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        cl_mem_ext_ptr_t hbm_ext_node;
        hbm_ext_node.flags =
            XCL_MEM_TOPOLOGY | acc.little_kernel_hbm_node_id[pip];
        hbm_ext_node.obj = writer_kernel_node_props[pip].data();
        hbm_ext_node.param = 0;

        size_t num_node_words = writer_kernel_node_props[pip].size();
        cl::Buffer node_buf;
        OCL_CHECK(err, node_buf =
                           cl::Buffer(acc.context,
                                      CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                          CL_MEM_USE_HOST_PTR,
                                      num_node_words * bytes_per_word,
                                      &hbm_ext_node, &err));
        writer_kernel_node_prop_buffers.push_back(node_buf);
    }

    // Create 3 big node prop buffers for hbm_writer
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        cl_mem_ext_ptr_t hbm_ext_node;
        hbm_ext_node.flags = XCL_MEM_TOPOLOGY | acc.big_kernel_hbm_node_id[pip];
        hbm_ext_node.obj =
            writer_kernel_node_props[pip + LITTLE_KERNEL_NUM].data();
        hbm_ext_node.param = 0;

        size_t num_node_words =
            writer_kernel_node_props[pip + LITTLE_KERNEL_NUM].size();
        cl::Buffer node_buf;
        OCL_CHECK(err, node_buf =
                           cl::Buffer(acc.context,
                                      CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                          CL_MEM_USE_HOST_PTR,
                                      num_node_words * bytes_per_word,
                                      &hbm_ext_node, &err));
        writer_kernel_node_prop_buffers.push_back(node_buf);
    }

    // --- 1.5: Setup unified output buffer ---
    cl_mem_ext_ptr_t hbm_ext_output;
    hbm_ext_output.flags = XCL_MEM_TOPOLOGY | WRITER_OUTPUT_HBM_ID;
    hbm_ext_output.obj = nullptr;
    hbm_ext_output.param = 0;

    writer_kernel_host_outputs.resize(total_output_words, 0);
    OCL_CHECK(err,
              writer_kernel_output_buffer = cl::Buffer(
                  acc.context, CL_MEM_WRITE_ONLY | CL_MEM_EXT_PTR_XILINX,
                  total_output_words * bytes_per_word, &hbm_ext_output, &err));

    // --- 1.6: Setup apply_kernel node prop buffer ---
    cl_mem_ext_ptr_t hbm_ext_apply;
    hbm_ext_apply.flags = XCL_MEM_TOPOLOGY | APPLY_KERNEL_NODE_HBM_ID;
    hbm_ext_apply.obj = apply_kernel_node_props.data();
    hbm_ext_apply.param = 0;

    size_t apply_node_words = apply_kernel_node_props.size();
    OCL_CHECK(err, apply_kernel_node_prop_buffer =
                       cl::Buffer(acc.context,
                                  CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX |
                                      CL_MEM_USE_HOST_PTR,
                                  apply_node_words * bytes_per_word,
                                  &hbm_ext_apply, &err));
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
    cl_mem_ext_ptr_t hbm_ext_apply_aux;
    hbm_ext_apply_aux.flags =
        XCL_MEM_TOPOLOGY | APPLY_KERNEL_AUX_NODE_HBM_ID;
    hbm_ext_apply_aux.obj = apply_kernel_aux_node_props.data();
    hbm_ext_apply_aux.param = 0;
    OCL_CHECK(err, apply_kernel_aux_node_prop_buffer =
                       cl::Buffer(acc.context,
                                  CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                      CL_MEM_USE_HOST_PTR,
                                  apply_node_words * bytes_per_word,
                                  &hbm_ext_apply_aux, &err));
#endif

    std::cout << "[SUCCESS] HBM buffers created: " << LITTLE_KERNEL_NUM
              << " little + " << BIG_KERNEL_NUM << " big pipelines, "
              << "total output size: " << total_output_words << " words."
              << std::endl;
}

void AlgorithmHost::update_data(const PartitionContainer &container) {
    std::cout
        << "--- [Host] Phase 2.1: Updating host-side data for new iteration ---"
        << std::endl;

    auto node_prop_pod_for = [&](int global_id) -> ap_fixed_pod_t {
        if (config.algorithm_kind == AlgorithmKind::Pagerank) {
            return static_cast<ap_fixed_pod_t>(
                static_cast<uint32_t>(h_pr_contrib[static_cast<size_t>(global_id)]));
        }
        return distance_to_pod(h_distances[global_id]);
    };

    auto apply_prop_pod_for = [&](int global_id) -> ap_fixed_pod_t {
        if (config.algorithm_kind == AlgorithmKind::Pagerank) {
            return static_cast<ap_fixed_pod_t>(
                h_pr_out_degree[static_cast<size_t>(global_id)]);
        }
        return distance_to_pod(h_distances[global_id]);
    };

    // Update BIG partition node props + apply dst props
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        const auto &big_partition = group.partitions[group_part.second];

        const size_t dist_per_word = DIST_PER_WORD;
        const size_t word_number =
            (big_partition.num_vertices + dist_per_word - 1) / dist_per_word;
        const size_t node_words = (word_number == 0) ? 1 : word_number;
        sparse_buffers[flat_idx].packed_node_props.assign(node_words,
                                                          make_identity_word());
        for (int j = 0; j < big_partition.num_vertices; ++j) {
            const int global_id = big_partition.vtx_map_rev.at(j);
            const ap_fixed_pod_t pod = node_prop_pod_for(global_id);
            const size_t word_idx = j / dist_per_word;
            const size_t slot = j % dist_per_word;
            set_distance_slot(sparse_buffers[flat_idx].packed_node_props[word_idx],
                              slot, pod);
        }
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= static_cast<size_t>(BIG_KERNEL_NUM)) {
                continue;
            }
            std::copy(
                sparse_buffers[flat_idx].packed_node_props.begin(),
                sparse_buffers[flat_idx].packed_node_props.end(),
                writer_kernel_node_props[static_cast<size_t>(LITTLE_KERNEL_NUM) +
                                         global_pip]
                        .begin() +
                    sparse_buffers[flat_idx].node_prop_offset);
        }

        const size_t dst_word_number =
            (big_partition.num_dsts + dist_per_word - 1) / dist_per_word;
        sparse_buffers[flat_idx].packed_dst_props.assign(dst_word_number,
                                                         make_identity_word());
        for (int j = 0; j < big_partition.num_dsts; ++j) {
            const int global_id = big_partition.vtx_map_rev.at(j);
            const ap_fixed_pod_t pod = apply_prop_pod_for(global_id);
            const size_t word_idx = j / dist_per_word;
            const size_t slot = j % dist_per_word;
            set_distance_slot(sparse_buffers[flat_idx].packed_dst_props[word_idx],
                              slot, pod);
        }

        const size_t apply_base =
            static_cast<size_t>(big_merger_offsets[group_part.first]) +
            static_cast<size_t>(sparse_buffers[flat_idx].dst_prop_offset);
        if (!sparse_buffers[flat_idx].packed_dst_props.empty()) {
            std::copy(sparse_buffers[flat_idx].packed_dst_props.begin(),
                      sparse_buffers[flat_idx].packed_dst_props.end(),
                      apply_kernel_node_props.begin() + apply_base);
            if (!config.needs_out_degree) {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                std::copy(sparse_buffers[flat_idx].packed_dst_props.begin(),
                          sparse_buffers[flat_idx].packed_dst_props.end(),
                          apply_kernel_aux_node_props.begin() + apply_base);
#endif
            }
        }
    }

    // Update LITTLE partition node props + apply dst props
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        const auto &little_partition = group.partitions[group_part.second];

        const size_t dist_per_word = DIST_PER_WORD;
        size_t padded_vertices =
            round_up(little_partition.num_vertices, SRC_BUFFER_SIZE);
        if (padded_vertices == 0) {
            padded_vertices = SRC_BUFFER_SIZE;
        }
        const size_t word_number = padded_vertices / dist_per_word;
        const size_t node_words = (word_number == 0) ? 1 : word_number;
        dense_buffers[flat_idx].packed_node_props.assign(node_words,
                                                         make_identity_word());
        for (int j = 0; j < little_partition.num_vertices; ++j) {
            const int global_id = little_partition.vtx_map_rev.at(j);
            const ap_fixed_pod_t pod = node_prop_pod_for(global_id);
            const size_t word_idx = j / dist_per_word;
            const size_t slot = j % dist_per_word;
            set_distance_slot(dense_buffers[flat_idx].packed_node_props[word_idx],
                              slot, pod);
        }
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            if (global_pip >= static_cast<size_t>(LITTLE_KERNEL_NUM)) {
                continue;
            }
            std::copy(dense_buffers[flat_idx].packed_node_props.begin(),
                      dense_buffers[flat_idx].packed_node_props.end(),
                      writer_kernel_node_props[global_pip].begin() +
                          dense_buffers[flat_idx].node_prop_offset);
        }

        const size_t dst_word_number =
            (little_partition.num_dsts + dist_per_word - 1) / dist_per_word;
        dense_buffers[flat_idx].packed_dst_props.assign(dst_word_number,
                                                        make_identity_word());
        for (int j = 0; j < little_partition.num_dsts; ++j) {
            const int global_id = little_partition.vtx_map_rev.at(j);
            const ap_fixed_pod_t pod = apply_prop_pod_for(global_id);
            const size_t word_idx = j / dist_per_word;
            const size_t slot = j % dist_per_word;
            set_distance_slot(dense_buffers[flat_idx].packed_dst_props[word_idx],
                              slot, pod);
        }

        const size_t apply_base =
            static_cast<size_t>(little_merger_offsets[group_part.first]) +
            static_cast<size_t>(dense_buffers[flat_idx].dst_prop_offset);
        if (!dense_buffers[flat_idx].packed_dst_props.empty()) {
            std::copy(dense_buffers[flat_idx].packed_dst_props.begin(),
                      dense_buffers[flat_idx].packed_dst_props.end(),
                      apply_kernel_node_props.begin() + apply_base);
            if (!config.needs_out_degree) {
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
                std::copy(dense_buffers[flat_idx].packed_dst_props.begin(),
                          dense_buffers[flat_idx].packed_dst_props.end(),
                          apply_kernel_aux_node_props.begin() + apply_base);
#endif
            }
        }
    }

    // Print writer kernel node prop data
    // for (size_t pip = 0; pip < 20; pip += 12) {
    //     const auto &node_props = writer_kernel_node_props[pip];
    //     bool is_dense = (pip < LITTLE_KERNEL_NUM);
    //     std::cout << "Pipeline " << pip << std::endl;
    //     if (is_dense) {
    //         std::cout << "  (Dense partition)" << std::endl;
    //     } else {
    //         std::cout << "  (Sparse partition)" << std::endl;
    //     }
    //     size_t partition_cnt = 0;
    //     size_t nxt_offset = 0;
    //     size_t cur_valid_node_num =
    //         is_dense ? dense_buffers[partition_cnt].node_prop_offset +
    //                        container.DPs[partition_cnt].num_vertices
    //                  : sparse_buffers[partition_cnt].node_prop_offset +
    //                        container.SPs[partition_cnt].num_vertices;

    //     for (size_t word_idx = 0; word_idx < node_props.size(); ++word_idx) {
    //         const bus_word_t &word = node_props[word_idx];
    //         std::cout << "Pipeline " << pip << ", Word " << word_idx << ": ";

    //         for (size_t data_idx = 0; data_idx < dists_per_word; ++data_idx)
    //         {
    //             distance_t dist =
    //                 reinterpret_cast<const distance_t *>(&word)[data_idx];
    //             float dist_float = static_cast<float>(dist);
    //             std::cout << dist_float;
    //             size_t cur_idx = word_idx * dists_per_word + data_idx;

    //             if (cur_idx >= nxt_offset && cur_idx < cur_valid_node_num) {
    //                 std::cout << " (valid) ";
    //             } else {
    //                 if (cur_idx == cur_valid_node_num) {
    //                     partition_cnt++;
    //                     if (is_dense) {
    //                         nxt_offset =
    //                             dense_buffers[partition_cnt].node_prop_offset
    //                             * dists_per_word;
    //                         cur_valid_node_num =
    //                             nxt_offset +
    //                             container.DPs[partition_cnt].num_vertices;
    //                     } else {
    //                         nxt_offset =
    //                             sparse_buffers[partition_cnt].node_prop_offset
    //                             * dists_per_word;
    //                         cur_valid_node_num =
    //                             nxt_offset +
    //                             container.SPs[partition_cnt].num_vertices;
    //                     }
    //                 }
    //                 std::cout << " (invalid) ";
    //             }
    //         }
    //         std::cout << std::endl;
    //     }
    // }

    std::cout << "[SUCCESS] Host-side data updated for new iteration."
              << std::endl;
}

void AlgorithmHost::transfer_data_to_fpga(const PartitionContainer &container) {
    cl_int err;
    std::cout << "--- [Host] Phase 2.2: Transferring data to FPGA HBM ---"
              << std::endl;

    // Transfer edge buffers for all big pipelines across all sparse partitions
    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            OCL_CHECK(err,
                      err = acc.big_gs_queue[global_pip].enqueueMigrateMemObjects(
                          {sparse_buffers[flat_idx].pipelines[pip].edge_props_buffer},
                          0 /* 0 means from host*/));
        }
    }

    // Transfer edge buffers for all little pipelines across all dense
    // partitions
    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            OCL_CHECK(err,
                      err = acc.little_gs_queue[global_pip]
                                .enqueueMigrateMemObjects(
                                    {dense_buffers[flat_idx]
                                         .pipelines[pip]
                                         .edge_props_buffer},
                                    0 /* 0 means from host*/));
        }
    }

    // Transfer apply_kernel node buffer
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
    OCL_CHECK(err,
              err = acc.apply_queue.enqueueMigrateMemObjects(
                  {apply_kernel_node_prop_buffer, apply_kernel_aux_node_prop_buffer},
                  0 /* 0 means from host*/));
#else
    OCL_CHECK(err,
              err = acc.apply_queue.enqueueMigrateMemObjects(
                  {apply_kernel_node_prop_buffer},
                  0 /* 0 means from host*/));
#endif

    // Transfer hbm_writer node prop buffers
    for (size_t i = 0; i < writer_kernel_node_prop_buffers.size(); ++i) {
        OCL_CHECK(err, err = acc.hbm_writer_queue.enqueueMigrateMemObjects(
                           {writer_kernel_node_prop_buffers[i]},
                           0 /* 0 means from host*/));
    }

    // Wait for all transfers to complete
    for (auto &q : acc.big_gs_queue)
        q.finish();
    for (auto &q : acc.little_gs_queue)
        q.finish();
    acc.apply_queue.finish();
    acc.hbm_writer_queue.finish();

    std::cout
        << "[SUCCESS] All data packed and transferred for current iteration."
        << std::endl;
}

// --- PHASE 3: KERNEL EXECUTION ---
// MODIFIED: Kernel arguments are updated to match the new kernel signature.
void AlgorithmHost::execute_kernel_iteration(
    const PartitionContainer &container) {
    cl_int err;

    auto enqueue_start = std::chrono::high_resolution_clock::now();

    const uint32_t edges_per_word = EDGES_PER_WORD;

    for (size_t flat_idx = 0; flat_idx < container.num_sparse_partitions;
         ++flat_idx) {
        auto group_part = container.sparse_partition_order[flat_idx];
        const auto &group = container.sparse_groups[group_part.first];
        const auto &big_partition = group.partitions[group_part.second];

        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            auto &kernel = acc.big_gs_krnls[global_pip];
            auto &buffer =
                sparse_buffers[flat_idx].pipelines[pip].edge_props_buffer;
            // The kernel reads edge_props by wide words, so the edge-count
            // argument must reflect the packed buffer capacity rather than the
            // PE-padded logical edge list length.
            const uint32_t pip_num_edges = static_cast<uint32_t>(
                sparse_buffers[flat_idx].pipelines[pip].packed_edge_props.size() *
                static_cast<size_t>(edges_per_word));

            const uint32_t big_num_vertices = big_partition.num_vertices;
            const uint32_t big_num_dsts = big_partition.num_dsts;
            const uint32_t memory_offset =
                sparse_buffers[flat_idx].src_prop_offset;

            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, big_num_vertices));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, pip_num_edges));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, big_num_dsts));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, memory_offset));

            OCL_CHECK(err,
                      err = acc.big_gs_queue[global_pip].enqueueTask(
                          kernel, nullptr,
                          &acc.big_kernel_events[flat_idx][global_pip]));
        }
    }

    for (size_t flat_idx = 0; flat_idx < container.num_dense_partitions;
         ++flat_idx) {
        auto group_part = container.dense_partition_order[flat_idx];
        const auto &group = container.dense_groups[group_part.first];
        const auto &little_partition = group.partitions[group_part.second];

        for (size_t pip = 0; pip < group.num_pipelines; ++pip) {
            const size_t global_pip =
                static_cast<size_t>(group.pipeline_offset) + pip;
            auto &kernel = acc.little_gs_krnls[global_pip];
            auto &buffer =
                dense_buffers[flat_idx].pipelines[pip].edge_props_buffer;
            const uint32_t pip_num_edges = static_cast<uint32_t>(
                dense_buffers[flat_idx].pipelines[pip].packed_edge_props.size() *
                static_cast<size_t>(edges_per_word));

            const uint32_t little_num_vertices = little_partition.num_vertices;
            const uint32_t little_num_dsts = little_partition.num_dsts;
            const uint32_t memory_offset =
                dense_buffers[flat_idx].src_buf_offset;

            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, little_num_vertices));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, pip_num_edges));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, little_num_dsts));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, memory_offset));

            OCL_CHECK(err,
                      err = acc.little_gs_queue[global_pip].enqueueTask(
                          kernel, nullptr,
                          &acc.little_kernel_events[flat_idx][global_pip]));
        }
    }

    {
        auto &apply_kernel = acc.apply_krnl;

        int arg_idx = 0;
        OCL_CHECK(err, err = apply_kernel.setArg(
                               arg_idx++, apply_kernel_node_prop_buffer));
#if APPLY_KERNEL_HAS_AUX_NODE_PROPS
        OCL_CHECK(err, err = apply_kernel.setArg(
                               arg_idx++, apply_kernel_aux_node_prop_buffer));
#endif
        OCL_CHECK(
            err,
            err = apply_kernel.setArg(
                arg_idx++,
                static_cast<uint32_t>(little_merger_lengths.size())));
        OCL_CHECK(
            err,
            err = apply_kernel.setArg(
                arg_idx++,
                static_cast<uint32_t>(big_merger_lengths.size())));
        for (size_t g = 0; g < little_merger_lengths.size(); ++g) {
            OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++,
                                                     little_merger_lengths[g]));
        }
        for (size_t g = 0; g < big_merger_lengths.size(); ++g) {
            OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++,
                                                     big_merger_lengths[g]));
        }
        for (size_t g = 0; g < little_merger_offsets.size(); ++g) {
            OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++,
                                                     little_merger_offsets[g]));
        }
        for (size_t g = 0; g < big_merger_offsets.size(); ++g) {
            OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++,
                                                     big_merger_offsets[g]));
        }
        if (config.algorithm_kind == AlgorithmKind::Pagerank) {
            OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++, pr_base_arg));
        }

        OCL_CHECK(err, err = acc.apply_queue.enqueueTask(
                           apply_kernel, nullptr, &acc.apply_kernel_event));
    }

    {
        auto &writer_kernel = acc.hbm_writer_krnl;
        int arg_idx = 0;
        for (const auto &buffer : writer_kernel_node_prop_buffers) {
            OCL_CHECK(err, err = writer_kernel.setArg(arg_idx++, buffer));
        }
        OCL_CHECK(err, err = writer_kernel.setArg(arg_idx++,
                                                  writer_kernel_output_buffer));
        std::vector<uint32_t> little_group_partition_counts(
            container.num_dense_groups, 0u);
        for (size_t group_idx = 0; group_idx < container.num_dense_groups;
             ++group_idx) {
            if (group_idx < container.dense_partition_indices.size()) {
                little_group_partition_counts[group_idx] =
                    static_cast<uint32_t>(
                        container.dense_partition_indices[group_idx].size());
            }
        }

        std::vector<uint32_t> big_group_partition_counts(
            container.num_sparse_groups, 0u);
        for (size_t group_idx = 0; group_idx < container.num_sparse_groups;
             ++group_idx) {
            if (group_idx < container.sparse_partition_indices.size()) {
                big_group_partition_counts[group_idx] = static_cast<uint32_t>(
                    container.sparse_partition_indices[group_idx].size());
            }
        }

        for (uint32_t count : little_group_partition_counts) {
            OCL_CHECK(err, err = writer_kernel.setArg(arg_idx++, count));
        }
        for (uint32_t count : big_group_partition_counts) {
            OCL_CHECK(err, err = writer_kernel.setArg(arg_idx++, count));
        }

        OCL_CHECK(err, err = acc.hbm_writer_queue.enqueueTask(
                           writer_kernel, nullptr, &acc.hbm_writer_event));
    }

    auto enqueue_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> enqueue_time = enqueue_end - enqueue_start;
    std::cout << "[SUCCESS] All kernel tasks enqueued for one iteration (Time: "
              << enqueue_time.count() << " seconds)" << std::endl;
}

// --- PHASE 4: DATA TRANSFER FROM FPGA ---
void AlgorithmHost::transfer_data_from_fpga() {
    cl_int err;
    std::cout << "--- [Host] Phase 4: Transferring results from FPGA ---"
              << std::endl;

    auto transfer_start = std::chrono::high_resolution_clock::now();

    cl::Event read_event;

    // Read from the single unified output buffer
    OCL_CHECK(err,
              err = acc.hbm_writer_queue.enqueueReadBuffer(
                  writer_kernel_output_buffer, CL_FALSE, 0,
                  writer_kernel_host_outputs.size() * sizeof(bus_word_t),
                  writer_kernel_host_outputs.data(), nullptr, &read_event));

    const char *watchdog_env = std::getenv("GRAPHYFLOW_EVENT_WATCHDOG_SECONDS");
    const int watchdog_seconds =
        (watchdog_env && watchdog_env[0]) ? std::atoi(watchdog_env) : 0;

    if (watchdog_seconds > 0) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(watchdog_seconds);
        auto next_print = std::chrono::steady_clock::now();

        auto status_to_string = [](cl_int status) -> const char * {
            switch (status) {
            case CL_QUEUED:
                return "QUEUED";
            case CL_SUBMITTED:
                return "SUBMITTED";
            case CL_RUNNING:
                return "RUNNING";
            case CL_COMPLETE:
                return "COMPLETE";
            default:
                return "UNKNOWN";
            }
        };

        auto dump_event = [&](const char *name, const cl::Event &evt) {
            if (evt() == nullptr) {
                std::cout << "  " << name << ": (unset)" << std::endl;
                return;
            }
            cl_int st = CL_COMPLETE;
            evt.getInfo(CL_EVENT_COMMAND_EXECUTION_STATUS, &st);
            std::cout << "  " << name << ": " << status_to_string(st) << " (" << st
                      << ")" << std::endl;
        };

        auto dump_all = [&]() {
            std::cout << "[watchdog] Event status dump:" << std::endl;
            dump_event("read_output", read_event);
            dump_event("apply", acc.apply_kernel_event);
            dump_event("hbm_writer", acc.hbm_writer_event);

            for (size_t flat = 0; flat < acc.big_kernel_events.size(); ++flat) {
                for (size_t k = 0; k < acc.big_kernel_events[flat].size(); ++k) {
                    std::string name = "big[" + std::to_string(flat) + "][" +
                                       std::to_string(k) + "]";
                    dump_event(name.c_str(), acc.big_kernel_events[flat][k]);
                }
            }
            for (size_t flat = 0; flat < acc.little_kernel_events.size(); ++flat) {
                for (size_t k = 0; k < acc.little_kernel_events[flat].size(); ++k) {
                    std::string name = "little[" + std::to_string(flat) + "][" +
                                       std::to_string(k) + "]";
                    dump_event(name.c_str(), acc.little_kernel_events[flat][k]);
                }
            }
        };

        while (true) {
            bool all_complete = true;

            auto check_event = [&](const cl::Event &evt) {
                if (evt() == nullptr) {
                    return;
                }
                cl_int st = CL_COMPLETE;
                evt.getInfo(CL_EVENT_COMMAND_EXECUTION_STATUS, &st);
                if (st != CL_COMPLETE) {
                    all_complete = false;
                }
            };

            check_event(read_event);
            check_event(acc.apply_kernel_event);
            check_event(acc.hbm_writer_event);

            for (size_t flat = 0; flat < acc.big_kernel_events.size(); ++flat) {
                for (size_t k = 0; k < acc.big_kernel_events[flat].size(); ++k) {
                    check_event(acc.big_kernel_events[flat][k]);
                }
            }
            for (size_t flat = 0; flat < acc.little_kernel_events.size(); ++flat) {
                for (size_t k = 0; k < acc.little_kernel_events[flat].size(); ++k) {
                    check_event(acc.little_kernel_events[flat][k]);
                }
            }

            if (all_complete) {
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= next_print) {
                dump_all();
                next_print = now + std::chrono::seconds(10);
            }

            if (now >= deadline) {
                std::cout << "[watchdog] Phase 4 timeout after " << watchdog_seconds
                          << " seconds." << std::endl;
                dump_all();
                std::cout.flush();
                std::cerr.flush();
                std::fflush(nullptr);
                std::_Exit(EXIT_FAILURE);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        // Wait for all transfers to complete (including the read) and ensure all
        // compute kernels have completed before the next iteration starts.
        //
        // Use event-based waits instead of queue.finish() so hangs can be diagnosed
        // via GRAPHYFLOW_EVENT_WATCHDOG_SECONDS.
        read_event.wait();
        if (acc.hbm_writer_event() != nullptr) {
            acc.hbm_writer_event.wait();
        }
        if (acc.apply_kernel_event() != nullptr) {
            acc.apply_kernel_event.wait();
        }
        for (size_t flat = 0; flat < acc.big_kernel_events.size(); ++flat) {
            for (size_t k = 0; k < acc.big_kernel_events[flat].size(); ++k) {
                if (acc.big_kernel_events[flat][k]() != nullptr) {
                    acc.big_kernel_events[flat][k].wait();
                }
            }
        }
        for (size_t flat = 0; flat < acc.little_kernel_events.size(); ++flat) {
            for (size_t k = 0; k < acc.little_kernel_events[flat].size(); ++k) {
                if (acc.little_kernel_events[flat][k]() != nullptr) {
                    acc.little_kernel_events[flat][k].wait();
                }
            }
        }
    } else {
        // Match the known-good reference host: finish the writer queue, which
        // sequences writer-kernel execution and the output readback.
        acc.hbm_writer_queue.finish();
    }

    auto transfer_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> transfer_time = transfer_end - transfer_start;
    std::cout << "[SUCCESS] All results transferred from HBM (Time: "
              << transfer_time.count() << " seconds)" << std::endl;
}

// --- PHASE 5: CONVERGENCE CHECK AND GLOBAL STATE UPDATE ---
// REWRITTEN: Implements unpacking logic to parse results from 512-bit words.

/*
bool AlgorithmHost::check_convergence_and_update(
    const PartitionContainer &container) {
    if (config.algorithm_kind == AlgorithmKind::Pagerank) {
        constexpr uint32_t k_max_abs_delta_eps = 1u;

        uint32_t max_abs_delta = 0u;
        std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                     "convergence ---"
                  << std::endl;

        std::map<int, int32_t> merged_contrib;
        const int props_per_word = AXI_BUS_WIDTH / 32;

        int word_idx = 0;
        int prop_in_word = 0;

        auto consume_partition_dsts = [&](const PartitionDescriptor &partition) {
            for (int local_id = 0; local_id < partition.num_dsts; ++local_id) {
                const int bit_offset = prop_in_word * 32;
                const uint32_t out_prop_u =
                    static_cast<uint32_t>(writer_kernel_host_outputs[word_idx]
                                              .range(bit_offset + 31, bit_offset));

                auto it = partition.vtx_map_rev.find(local_id);
                if (it != partition.vtx_map_rev.end()) {
                    const int global_id = it->second;
                    if (global_id >= 0 && global_id < m_num_vertices) {
                        merged_contrib[global_id] = static_cast<int32_t>(out_prop_u);
                    }
                }

                prop_in_word++;
                if (prop_in_word >= props_per_word) {
                    prop_in_word = 0;
                    word_idx++;
                }
            }

            if (prop_in_word != 0) {
                prop_in_word = 0;
                word_idx++;
            }
        };

        for (const auto &group : container.dense_groups) {
            for (const auto &partition : group.partitions) {
                consume_partition_dsts(partition);
            }
        }
        for (const auto &group : container.sparse_groups) {
            for (const auto &partition : group.partitions) {
                consume_partition_dsts(partition);
            }
        }

        for (const auto &[global_id, new_val] : merged_contrib) {
            if (global_id < 0 || global_id >= m_num_vertices) {
                continue;
            }
            const int32_t old_val = h_pr_contrib[static_cast<size_t>(global_id)];
            const int32_t delta = new_val - old_val;
            const uint32_t abs_delta =
                (delta < 0) ? static_cast<uint32_t>(-(int64_t)delta)
                            : static_cast<uint32_t>(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
            h_pr_contrib[static_cast<size_t>(global_id)] = new_val;
        }

        for (int v = 0; v < m_num_vertices; ++v) {
            if (h_pr_has_incoming[static_cast<size_t>(v)] != 0u) {
                continue;
            }
            const uint32_t od = h_pr_out_degree[static_cast<size_t>(v)];
            int32_t new_contrib = 0;
            if (od != 0u) {
                const int32_t recip = static_cast<int32_t>((1u << 16) / od);
                new_contrib = static_cast<int32_t>(
                    (static_cast<int64_t>(static_cast<int32_t>(pr_base_arg)) *
                     static_cast<int64_t>(recip)) >>
                    16);
            }
            const int32_t old_val = h_pr_contrib[static_cast<size_t>(v)];
            const int32_t delta = new_contrib - old_val;
            const uint32_t abs_delta =
                (delta < 0) ? static_cast<uint32_t>(-(int64_t)delta)
                            : static_cast<uint32_t>(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
            h_pr_contrib[static_cast<size_t>(v)] = new_contrib;
        }

        const bool converged = (max_abs_delta <= k_max_abs_delta_eps);
        if (!converged) {
            std::cout << "[INFO] Properties updated. Preparing for next iteration."
                      << std::endl;
        } else {
            std::cout << "[INFO] No property updates. Algorithm has converged."
                      << std::endl;
        }
        return converged;
    }

    bool changed = false;
    std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                 "convergence ---"
              << std::endl;

    std::map<int, distance_t> min_distances;
    const int dists_per_word = DIST_PER_WORD;

    // Output layout: [little_dsts][big_dsts]
    // First little_dst_num entries are little partition destinations
    // Next big_dst_num entries are big partition destinations

    int word_idx = 0;
    int dist_in_word = 0;

    // Process little partition destinations [0:little_dst_num]
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        const auto &little_partition = container.DPs[i];
        for (int local_id = 0; local_id < little_partition.num_dsts;
             ++local_id) {
            ap_fixed_pod_t dist_pod =
                get_distance_slot(writer_kernel_host_outputs[word_idx],
                                  dist_in_word);

            if (little_partition.vtx_map_rev.count(local_id)) {
                int global_id = little_partition.vtx_map_rev.at(local_id);
                if (global_id < m_num_vertices) {
                    distance_t new_dist =
                        *reinterpret_cast<distance_t *>(&dist_pod);
                    printf("DP No.%d, little pipe No.%d, local_id %d, "
                           "global_id %d, new_dist %f\n",
                           i, 0, local_id, global_id, (float)new_dist);

                    if (min_distances.find(global_id) == min_distances.end() ||
                        new_dist < min_distances[global_id]) {
                        min_distances[global_id] = new_dist;
                    }
                }
            }

            dist_in_word++;
            if (dist_in_word >= dists_per_word) {
                dist_in_word = 0;
                word_idx++;
            }
        }

        // print other node's local & global id
        for (int local_id = little_partition.num_dsts;
             local_id < little_partition.num_vertices; ++local_id) {
            if (little_partition.vtx_map_rev.count(local_id)) {
                int global_id = little_partition.vtx_map_rev.at(local_id);
                printf("DP No.%d, little pipe No.%d, local_id %d, global_id "
                       "%d, not dst node\n",
                       i, 0, local_id, global_id);
            }
        }
    }

    if (dist_in_word != 0) {
        dist_in_word = 0;
        word_idx++;
    }

    // Process big partition destinations
    // [little_dst_num:little_dst_num+big_dst_num]
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        const auto &big_partition = container.SPs[i];
        for (int local_id = 0; local_id < big_partition.num_dsts; ++local_id) {
            int bit_offset = dist_in_word * DISTANCE_BITWIDTH;
            ap_fixed_pod_t dist_pod =
                writer_kernel_host_outputs[word_idx].range(
                    bit_offset + DISTANCE_BITWIDTH - 1, bit_offset);

            if (big_partition.vtx_map_rev.count(local_id)) {
                int global_id = big_partition.vtx_map_rev.at(local_id);
                if (global_id < m_num_vertices) {
                    distance_t new_dist =
                        *reinterpret_cast<distance_t *>(&dist_pod);
                    printf("SP No.%d, big pipe No.%d, local_id %d, global_id "
                           "%d, new_dist %f\n",
                           i, 0, local_id, global_id, (float)new_dist);

                    if (min_distances.find(global_id) == min_distances.end() ||
                        new_dist < min_distances[global_id]) {
                        min_distances[global_id] = new_dist;
                    }
                }
            }

            dist_in_word++;
            if (dist_in_word >= dists_per_word) {
                dist_in_word = 0;
                word_idx++;
            }
        }
        // print other node's local & global id
        for (int local_id = big_partition.num_dsts;
             local_id < big_partition.num_vertices; ++local_id) {
            if (big_partition.vtx_map_rev.count(local_id)) {
                int global_id = big_partition.vtx_map_rev.at(local_id);
                printf("SP No.%d, big pipe No.%d, local_id %d, global_id %d, "
                       "not dst node\n",
                       i, 0, local_id, global_id);
            }
        }
    }

    // Update global distance vector and check for changes
    for (auto const &[global_id, new_dist] : min_distances) {
        if (global_id < m_num_vertices && new_dist < h_distances[global_id]) {
            h_distances[global_id] = new_dist;
            changed = true;
        }
    }

    if (changed) {
        std::cout << "[INFO] Distances updated. Preparing for next iteration."
                  << std::endl;
    } else {
        std::cout << "[INFO] No distance updates. Algorithm has converged."
                  << std::endl;
    }

    return !changed;
}

// --- FINALIZATION ---
const std::vector<int> &AlgorithmHost::get_results() const {
    static std::vector<int> final_distances;
    final_distances.clear();
    final_distances.reserve(h_distances.size());

    for (const auto &dist : h_distances) {
        if (dist >= INFINITY_DIST) {
            final_distances.push_back(INFINITY_DIST);
        } else {
            final_distances.push_back(dist.to_int());
        }
    }
    return final_distances;
}
*/
bool AlgorithmHost::check_convergence_and_update(
    const PartitionContainer &container) {
    if (config.algorithm_kind == AlgorithmKind::Pagerank) {
        constexpr uint32_t k_max_abs_delta_eps = 1u;

        uint32_t max_abs_delta = 0u;
        std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                     "convergence ---"
                  << std::endl;

        std::map<int, int32_t> merged_contrib;
        const int props_per_word = AXI_BUS_WIDTH / 32;

        int word_idx = 0;
        int prop_in_word = 0;

        auto consume_partition_dsts = [&](const PartitionDescriptor &partition) {
            for (int local_id = 0; local_id < partition.num_dsts; ++local_id) {
                const int bit_offset = prop_in_word * 32;
                const uint32_t out_prop_u =
                    static_cast<uint32_t>(writer_kernel_host_outputs[word_idx]
                                              .range(bit_offset + 31, bit_offset));

                auto it = partition.vtx_map_rev.find(local_id);
                if (it != partition.vtx_map_rev.end()) {
                    const int global_id = it->second;
                    if (global_id >= 0 && global_id < m_num_vertices) {
                        merged_contrib[global_id] = static_cast<int32_t>(out_prop_u);
                    }
                }

                prop_in_word++;
                if (prop_in_word >= props_per_word) {
                    prop_in_word = 0;
                    word_idx++;
                }
            }

            if (prop_in_word != 0) {
                prop_in_word = 0;
                word_idx++;
            }
        };

        for (const auto &group : container.dense_groups) {
            for (const auto &partition : group.partitions) {
                consume_partition_dsts(partition);
            }
        }
        for (const auto &group : container.sparse_groups) {
            for (const auto &partition : group.partitions) {
                consume_partition_dsts(partition);
            }
        }

        for (const auto &[global_id, new_val] : merged_contrib) {
            if (global_id < 0 || global_id >= m_num_vertices) {
                continue;
            }
            const int32_t old_val = h_pr_contrib[static_cast<size_t>(global_id)];
            const int32_t delta = new_val - old_val;
            const uint32_t abs_delta =
                (delta < 0) ? static_cast<uint32_t>(-(int64_t)delta)
                            : static_cast<uint32_t>(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
            h_pr_contrib[static_cast<size_t>(global_id)] = new_val;
        }

        for (int v = 0; v < m_num_vertices; ++v) {
            if (h_pr_has_incoming[static_cast<size_t>(v)] != 0u) {
                continue;
            }
            const uint32_t od = h_pr_out_degree[static_cast<size_t>(v)];
            int32_t new_contrib = 0;
            if (od != 0u) {
                const int32_t recip = static_cast<int32_t>((1u << 16) / od);
                new_contrib = static_cast<int32_t>(
                    (static_cast<int64_t>(static_cast<int32_t>(pr_base_arg)) *
                     static_cast<int64_t>(recip)) >>
                    16);
            }
            const int32_t old_val = h_pr_contrib[static_cast<size_t>(v)];
            const int32_t delta = new_contrib - old_val;
            const uint32_t abs_delta =
                (delta < 0) ? static_cast<uint32_t>(-(int64_t)delta)
                            : static_cast<uint32_t>(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
            h_pr_contrib[static_cast<size_t>(v)] = new_contrib;
        }

        const bool converged = (max_abs_delta <= k_max_abs_delta_eps);
        if (!converged) {
            std::cout << "[INFO] Properties updated. Preparing for next iteration."
                      << std::endl;
        } else {
            std::cout << "[INFO] No property updates. Algorithm has converged."
                      << std::endl;
        }
        return converged;
    }

    bool changed = false;
    std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                 "convergence ---"
              << std::endl;

    const char *debug_env = std::getenv("GRAPHYFLOW_DEBUG_MAP");
    const bool debug_map = (debug_env != nullptr && debug_env[0] != '\0');

    const int dists_per_word = DIST_PER_WORD;
    std::vector<ap_fixed_pod_t> new_values(m_num_vertices);
    std::vector<uint8_t> has_value(m_num_vertices, 0);

    auto store_value = [&](int global_id, ap_fixed_pod_t value) {
        if (global_id < 0 || global_id >= m_num_vertices) {
            return;
        }
        if (!has_value[global_id]) {
            new_values[global_id] = value;
            has_value[global_id] = 1;
            return;
        }
        switch (config.update_mode) {
        case UpdateMode::Overwrite:
            new_values[global_id] = value;
            break;
        case UpdateMode::Max:
            if (value > new_values[global_id]) {
                new_values[global_id] = value;
            }
            break;
        case UpdateMode::Min:
        default:
            if (value < new_values[global_id]) {
                new_values[global_id] = value;
            }
            break;
        }
    };

    int word_idx = 0;
    int dist_in_word = 0;

    for (size_t group_idx = 0; group_idx < container.num_dense_groups;
         ++group_idx) {
        const auto &group = container.dense_groups[group_idx];
        for (size_t part_idx = 0; part_idx < group.partitions.size();
             ++part_idx) {
            const auto &little_partition = group.partitions[part_idx];
            for (int local_id = 0; local_id < little_partition.num_dsts;
                 ++local_id) {
                ap_fixed_pod_t dist_pod =
                    get_distance_slot(writer_kernel_host_outputs[word_idx],
                                      dist_in_word);

                if (little_partition.vtx_map_rev.count(local_id)) {
                    int global_id = little_partition.vtx_map_rev.at(local_id);
                    if (debug_map && m_num_vertices <= 16) {
                        distance_t dist_val = pod_to_distance(dist_pod);
                        std::cout << "[DEBUG] little group " << group_idx
                                  << " part " << part_idx
                                  << " local_id=" << local_id
                                  << " global_id=" << global_id
                                  << " dist=" << dist_val.to_int() << std::endl;
                    }
                    store_value(global_id, dist_pod);
                }

                dist_in_word++;
                if (dist_in_word >= dists_per_word) {
                    dist_in_word = 0;
                    word_idx++;
                }
            }

            if (dist_in_word != 0) {
                dist_in_word = 0;
                word_idx++;
            }
        }
    }

    for (size_t group_idx = 0; group_idx < container.num_sparse_groups;
         ++group_idx) {
        const auto &group = container.sparse_groups[group_idx];
        for (size_t part_idx = 0; part_idx < group.partitions.size();
             ++part_idx) {
            const auto &big_partition = group.partitions[part_idx];
            for (int local_id = 0; local_id < big_partition.num_dsts; ++local_id) {
                ap_fixed_pod_t dist_pod =
                    get_distance_slot(writer_kernel_host_outputs[word_idx],
                                      dist_in_word);

                if (big_partition.vtx_map_rev.count(local_id)) {
                    int global_id = big_partition.vtx_map_rev.at(local_id);
                    if (debug_map && m_num_vertices <= 16) {
                        distance_t dist_val = pod_to_distance(dist_pod);
                        std::cout << "[DEBUG] big group " << group_idx
                                  << " part " << part_idx
                                  << " local_id=" << local_id
                                  << " global_id=" << global_id
                                  << " dist=" << dist_val.to_int() << std::endl;
                    }
                    store_value(global_id, dist_pod);
                }

                dist_in_word++;
                if (dist_in_word >= dists_per_word) {
                    dist_in_word = 0;
                    word_idx++;
                }
            }

            if (dist_in_word != 0) {
                dist_in_word = 0;
                word_idx++;
            }
        }
    }

    float max_delta = 0.0f;
    unsigned int newly_discovered = 0u;

    for (int global_id = 0; global_id < m_num_vertices; ++global_id) {
        if (!has_value[global_id]) {
            continue;
        }

        ap_fixed_pod_t new_val_pod = new_values[global_id];
        ap_fixed_pod_t old_val_pod = distance_to_pod(h_distances[global_id]);
        distance_t new_val = pod_to_distance(new_val_pod);
        distance_t old_val = h_distances[global_id];

        if (config.convergence_mode == ConvergenceMode::DeltaThreshold) {
            float delta =
                std::fabs(new_val.to_float() - old_val.to_float());
            if (delta > max_delta) {
                max_delta = delta;
            }
        }

        if (config.convergence_mode == ConvergenceMode::NewlyDiscoveredZero &&
            config.active_mask != 0u) {
            const unsigned int active =
                (static_cast<unsigned int>(new_val_pod) & config.active_mask);
            if (active != 0u && static_cast<unsigned int>(old_val_pod) == config.inf_value) {
                newly_discovered++;
            }
        }

        switch (config.update_mode) {
        case UpdateMode::Overwrite:
            if (new_val_pod != old_val_pod) {
                changed = true;
            }
            h_distances[global_id] = new_val;
            break;
        case UpdateMode::Max:
            if (new_val_pod > old_val_pod) {
                h_distances[global_id] = new_val;
                changed = true;
            }
            break;
        case UpdateMode::Min:
        default:
            if (new_val_pod < old_val_pod) {
                h_distances[global_id] = new_val;
                changed = true;
            }
            break;
        }
    }

    switch (config.convergence_mode) {
    case ConvergenceMode::FixedIterations:
        std::cout << "[INFO] Fixed-iteration mode." << std::endl;
        return false;
    case ConvergenceMode::DeltaThreshold:
        changed = max_delta >= config.delta_threshold;
        if (changed) {
            std::cout << "[INFO] Delta threshold not met. Preparing for next iteration."
                      << std::endl;
        } else {
            std::cout << "[INFO] Delta threshold met. Algorithm has converged."
                      << std::endl;
        }
        return !changed;
    case ConvergenceMode::NewlyDiscoveredZero:
        if (newly_discovered == 0u) {
            std::cout << "[INFO] No newly discovered vertices. Algorithm has converged."
                      << std::endl;
            return true;
        }
        std::cout << "[INFO] Newly discovered vertices: " << newly_discovered
                  << ". Preparing for next iteration." << std::endl;
        return false;
    case ConvergenceMode::MinImprove:
    case ConvergenceMode::EqualityStable:
    default:
        if (changed) {
            std::cout << "[INFO] Values updated. Preparing for next iteration."
                      << std::endl;
        } else {
            std::cout << "[INFO] No value updates. Algorithm has converged."
                      << std::endl;
        }
        return !changed;
    }
}

const std::vector<unsigned int> &AlgorithmHost::get_results() const {
    static std::vector<unsigned int> final_values;
    final_values.clear();
    if (config.algorithm_kind == AlgorithmKind::Pagerank) {
        final_values.reserve(h_pr_contrib.size());
        for (int32_t value : h_pr_contrib) {
            final_values.push_back(static_cast<uint32_t>(value));
        }
        return final_values;
    }

    final_values.reserve(h_distances.size());

    for (const auto &dist : h_distances) {
        ap_fixed_pod_t pod = distance_to_pod(dist);
        final_values.push_back(static_cast<unsigned int>(pod));
    }
    return final_values;
}
