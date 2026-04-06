#include "generated_host.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <vector>

AlgorithmHost::AlgorithmHost(AccDescriptor &acc) : acc(acc) {}

void AlgorithmHost::prepare_data(const PartitionContainer &container,
                                 int start_node) {
    std::cout << "--- [Host] Phase 0: Preparing data structures ---"
              << std::endl;

    auto start_time = std::chrono::system_clock::now();
    auto current_time = start_time;

    // 1. Initialize algorithm state
    m_num_vertices = container.num_graph_vertices;

    // Initialize CC bitmasks (ReGraph-style): seed first 32 vertices with 1<<i
    h_masks.assign(m_num_vertices, 0);
    int seed_count = std::min(m_num_vertices, 32);
    for (int i = 0; i < seed_count; ++i) {
        h_masks[i] = (uint32_t(1) << i);
    }
    // 2. Prepare host-side input buffers for each pipeline
    const size_t bytes_per_word = AXI_BUS_WIDTH / 8;
    dense_buffers.resize(container.num_dense_partitions);
    sparse_buffers.resize(container.num_sparse_partitions);

    for (int i = 0; i < container.num_dense_partitions; ++i) {
        dense_buffers[i].pipelines.resize(LITTLE_KERNEL_NUM);
        dense_buffers[i].node_prop_offset = 0;
        dense_buffers[i].dst_prop_offset = 0;
    }
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        sparse_buffers[i].pipelines.resize(BIG_KERNEL_NUM);
        sparse_buffers[i].node_prop_offset = 0;
        sparse_buffers[i].dst_prop_offset = 0;
    }

    size_t sparse_node_offset = 0;
    size_t sparse_dst_offset = 0;
    size_t dense_node_offset = 0;
    size_t dense_src_buf_offset = 0;
    size_t dense_dst_offset = 0;

    // --- 2.1: Prepare BIG partition data (shared node props, separate edge
    // props per pipeline) ---
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        const auto &big_partition = container.SPs[i];

        // Pack node distances ONCE for the big partition (shared)
        {
            const size_t bytes_per_dist = DISTANCE_BITWIDTH / 8;
            const size_t dist_per_word = bytes_per_word / bytes_per_dist;
            const size_t word_number =
                (big_partition.num_vertices + dist_per_word - 1) /
                dist_per_word;
            std::vector<char> temp_byte_buffer;
            temp_byte_buffer.reserve(word_number * bytes_per_word);

            for (int j = 0; j < big_partition.num_vertices; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }

                int global_id = big_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                // printf("Putting Data: SP No.%d, big pipe No.%d, local_id %d,
                // global_id %d, dist %.3f\n",
                //        i, 0,  j, global_id, (float)dist_val);

                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            size_t node_word_count =
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;
            sparse_buffers[i].packed_node_props.resize(node_word_count, 0);
            std::memcpy(sparse_buffers[i].packed_node_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            sparse_buffers[i].node_prop_offset = sparse_node_offset;
            sparse_node_offset += node_word_count;

            const size_t dst_word_number =
                (big_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            temp_byte_buffer.clear();
            temp_byte_buffer.reserve(dst_word_number * bytes_per_word);
            for (int j = 0; j < big_partition.num_dsts; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }
                int global_id = big_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            size_t dst_word_count =
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;
            sparse_buffers[i].packed_dst_props.resize(dst_word_count, 0);
            std::memcpy(sparse_buffers[i].packed_dst_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            sparse_buffers[i].dst_prop_offset = sparse_dst_offset;
            sparse_dst_offset += dst_word_count;
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared shared big node props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;

        // Pack edge properties for EACH big pipeline
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            const auto &pipeline_edges = big_partition.pipeline_edges[pip];
            // Pack this pipeline's edge properties
            const size_t bytes_per_edge =
                (NODE_ID_BITWIDTH + NODE_ID_BITWIDTH) / 8;
            const size_t edges_per_word = bytes_per_word / bytes_per_edge;

            // Check if this pipeline has no edges - if so, add 8 dummy edges
            const size_t actual_edges =
                (pipeline_edges.num_edges == 0) ? 8 : pipeline_edges.num_edges;
            const size_t word_number =
                (actual_edges + edges_per_word - 1) / edges_per_word;
            std::vector<char> temp_byte_buffer;
            temp_byte_buffer.reserve(word_number * bytes_per_word);

            if (pipeline_edges.num_edges == 0) {
                printf("Padding SP No.%d, big pipe No.%d with 8 dummy edges.\n",
                       i, pip);
                // Add 8 dummy edges with dst_id = 0x7FFFFFFF and src_id = 0
                const uint32_t dummy_dst_id = 0x7FFFFFFF;
                const uint32_t dummy_src_id = 0;

                for (int dummy_idx = 0; dummy_idx < 8; ++dummy_idx) {
                    if ((temp_byte_buffer.size() % bytes_per_word) +
                            bytes_per_edge >
                        bytes_per_word) {
                        size_t padding_needed =
                            bytes_per_word -
                            (temp_byte_buffer.size() % bytes_per_word);
                        temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                padding_needed, 0);
                    }

                    char edge_bytes[bytes_per_edge];

                    // Pack dst_id (first NODE_ID_BITWIDTH bits)
                    for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                        edge_bytes[b] = (dummy_dst_id >> (8 * b)) & 0xFF;
                    }

                    // Pack src_id (next NODE_ID_BITWIDTH bits)
                    for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                        edge_bytes[(NODE_ID_BITWIDTH / 8) + b] =
                            (dummy_src_id >> (8 * b)) & 0xFF;
                    }

                    temp_byte_buffer.insert(temp_byte_buffer.end(), edge_bytes,
                                            edge_bytes + bytes_per_edge);
                }
            } else {
                // Iterate through vertices, then their edges
                for (int v = 0; v < big_partition.num_vertices; ++v) {
                    node_id_t src_id = v;
                    for (int edge_idx = pipeline_edges.offsets[v];
                         edge_idx < pipeline_edges.offsets[v + 1]; ++edge_idx) {
                        if ((temp_byte_buffer.size() % bytes_per_word) +
                                bytes_per_edge >
                            bytes_per_word) {
                            size_t padding_needed =
                                bytes_per_word -
                                (temp_byte_buffer.size() % bytes_per_word);
                            temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                    padding_needed, 0);
                        }

                        char edge_bytes[bytes_per_edge];
                        uint32_t dest_id = pipeline_edges.columns[edge_idx];

                        // Pack dst_id (first NODE_ID_BITWIDTH bits)
                        for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                            edge_bytes[b] = (dest_id >> (8 * b)) & 0xFF;
                        }

                        // Pack src_id (next NODE_ID_BITWIDTH bits)
                        for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                            edge_bytes[(NODE_ID_BITWIDTH / 8) + b] =
                                (src_id >> (8 * b)) & 0xFF;
                        }

                        temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                edge_bytes,
                                                edge_bytes + bytes_per_edge);
                    }
                }
            }
            sparse_buffers[i].pipelines[pip].packed_edge_props.resize(
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word,
                0);
            std::memcpy(
                sparse_buffers[i].pipelines[pip].packed_edge_props.data(),
                temp_byte_buffer.data(), temp_byte_buffer.size());
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared " << BIG_KERNEL_NUM
            << " big pipeline edge props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;
    }

    // --- 2.2: Prepare LITTLE partition data (shared node props, separate edge
    // props per pipeline) ---
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        const auto &little_partition = container.DPs[i];

        // Pack node distances ONCE for the little partition (shared)
        {
            const size_t bytes_per_dist = DISTANCE_BITWIDTH / 8;
            const size_t dist_per_word = bytes_per_word / bytes_per_dist;
            const size_t word_number =
                (little_partition.num_vertices + dist_per_word - 1) /
                dist_per_word;
            std::vector<char> temp_byte_buffer;
            temp_byte_buffer.reserve(word_number * bytes_per_word);

            for (int j = 0; j < little_partition.num_vertices; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }
                int global_id = little_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                // printf("Putting Data: DP No.%d, little pipe No.%d, local_id
                // %d, global_id %d, dist %.3f\n",
                //        i, 0,  j, global_id, (float)dist_val);
                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            // pack temp_byte_buffer to SRC_BUFFER_SIZE nodes
            size_t buffer_size = SRC_BUFFER_SIZE * bytes_per_dist;
            if (temp_byte_buffer.size() % buffer_size != 0) {
                size_t padding_needed =
                    buffer_size - (temp_byte_buffer.size() % buffer_size);
                temp_byte_buffer.insert(temp_byte_buffer.end(), padding_needed,
                                        0);
            }
            size_t src_buf_cnt = temp_byte_buffer.size() / buffer_size;

            size_t node_word_count =
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;
            dense_buffers[i].packed_node_props.resize(node_word_count, 0);
            std::memcpy(dense_buffers[i].packed_node_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            dense_buffers[i].node_prop_offset = dense_node_offset;
            dense_node_offset += node_word_count;
            dense_buffers[i].src_buf_offset = dense_src_buf_offset;
            dense_src_buf_offset += src_buf_cnt;

            const size_t dst_word_number =
                (little_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            temp_byte_buffer.clear();
            temp_byte_buffer.reserve(dst_word_number * bytes_per_word);
            for (int j = 0; j < little_partition.num_dsts; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }
                int global_id = little_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            size_t dst_word_count =
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;
            dense_buffers[i].packed_dst_props.resize(dst_word_count, 0);
            std::memcpy(dense_buffers[i].packed_dst_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            dense_buffers[i].dst_prop_offset = dense_dst_offset;
            dense_dst_offset += dst_word_count;
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared shared little node props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;

        // Pack edge properties for EACH little pipeline
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            const auto &pipeline_edges = little_partition.pipeline_edges[pip];

            // Pack this pipeline's edge properties
            const size_t bytes_per_edge =
                (NODE_ID_BITWIDTH + NODE_ID_BITWIDTH) / 8;
            const size_t edges_per_word = bytes_per_word / bytes_per_edge;

            // Check if this pipeline has no edges - if so, add 8 dummy edges
            const size_t actual_edges =
                (pipeline_edges.num_edges == 0) ? 8 : pipeline_edges.num_edges;
            const size_t word_number =
                (actual_edges + edges_per_word - 1) / edges_per_word;
            std::vector<char> temp_byte_buffer;
            temp_byte_buffer.reserve(word_number * bytes_per_word);

            if (pipeline_edges.num_edges == 0) {
                printf(
                    "Padding DP No.%d, little pipe No.%d with 8 dummy edges.\n",
                    i, pip);
                // Add 8 dummy edges with dst_id = 0x7FFFFFFF and src_id = 0
                const uint32_t dummy_dst_id = 0x7FFFFFFF;
                const uint32_t dummy_src_id = 0;

                for (int dummy_idx = 0; dummy_idx < 8; ++dummy_idx) {
                    if ((temp_byte_buffer.size() % bytes_per_word) +
                            bytes_per_edge >
                        bytes_per_word) {
                        size_t padding_needed =
                            bytes_per_word -
                            (temp_byte_buffer.size() % bytes_per_word);
                        temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                padding_needed, 0);
                    }

                    char edge_bytes[bytes_per_edge];

                    // Pack dst_id (first NODE_ID_BITWIDTH bits)
                    for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                        edge_bytes[b] = (dummy_dst_id >> (8 * b)) & 0xFF;
                    }

                    // Pack src_id (next NODE_ID_BITWIDTH bits)
                    for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                        edge_bytes[(NODE_ID_BITWIDTH / 8) + b] =
                            (dummy_src_id >> (8 * b)) & 0xFF;
                    }

                    temp_byte_buffer.insert(temp_byte_buffer.end(), edge_bytes,
                                            edge_bytes + bytes_per_edge);
                }
            } else {
                // Iterate through vertices, then their edges
                for (int v = 0; v < little_partition.num_vertices; ++v) {
                    node_id_t src_id = v;
                    for (int edge_idx = pipeline_edges.offsets[v];
                         edge_idx < pipeline_edges.offsets[v + 1]; ++edge_idx) {
                        if ((temp_byte_buffer.size() % bytes_per_word) +
                                bytes_per_edge >
                            bytes_per_word) {
                            size_t padding_needed =
                                bytes_per_word -
                                (temp_byte_buffer.size() % bytes_per_word);
                            temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                    padding_needed, 0);
                        }

                        char edge_bytes[bytes_per_edge];
                        uint32_t dest_id = pipeline_edges.columns[edge_idx];

                        // Pack dst_id (first NODE_ID_BITWIDTH bits)
                        for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                            edge_bytes[b] = (dest_id >> (8 * b)) & 0xFF;
                        }

                        // Pack src_id (next NODE_ID_BITWIDTH bits)
                        for (int b = 0; b < NODE_ID_BITWIDTH / 8; ++b) {
                            edge_bytes[(NODE_ID_BITWIDTH / 8) + b] =
                                (src_id >> (8 * b)) & 0xFF;
                        }

                        temp_byte_buffer.insert(temp_byte_buffer.end(),
                                                edge_bytes,
                                                edge_bytes + bytes_per_edge);
                    }
                }
            }
            dense_buffers[i].pipelines[pip].packed_edge_props.resize(
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word,
                0);
            std::memcpy(
                dense_buffers[i].pipelines[pip].packed_edge_props.data(),
                temp_byte_buffer.data(), temp_byte_buffer.size());
        }

        current_time = std::chrono::system_clock::now();
        std::cout
            << "--- [Host] Phase 0: Prepared " << LITTLE_KERNEL_NUM
            << " little pipeline edge props ("
            << std::chrono::duration<double>(current_time - start_time).count()
            << " sec) ---" << std::endl;
        start_time = current_time;
    }

    // apply kernel node props should be first each little partition's dst
    // props, then each big partition's dst props
    size_t total_little_dst_words = dense_dst_offset;
    size_t total_big_dst_words = sparse_dst_offset;
    size_t total_little_node_words = dense_node_offset;
    size_t total_big_node_words = sparse_node_offset;

    apply_kernel_node_props.assign(total_little_dst_words + total_big_dst_words,
                                   0);
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        std::copy(dense_buffers[i].packed_dst_props.begin(),
                  dense_buffers[i].packed_dst_props.end(),
                  apply_kernel_node_props.begin() +
                      dense_buffers[i].dst_prop_offset);
    }
    big_dst_offset = total_little_dst_words;
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        std::copy(sparse_buffers[i].packed_dst_props.begin(),
                  sparse_buffers[i].packed_dst_props.end(),
                  apply_kernel_node_props.begin() + big_dst_offset +
                      sparse_buffers[i].dst_prop_offset);
    }

    prop_loader_node_props.resize(LITTLE_KERNEL_NUM + BIG_KERNEL_NUM);
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        prop_loader_node_props[pip].assign(total_little_node_words, 0);
    }
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        prop_loader_node_props[pip + LITTLE_KERNEL_NUM].assign(
            total_big_node_words, 0);
    }

    for (int i = 0; i < container.num_dense_partitions; ++i) {
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            std::copy(dense_buffers[i].packed_node_props.begin(),
                      dense_buffers[i].packed_node_props.end(),
                      prop_loader_node_props[pip].begin() +
                          dense_buffers[i].node_prop_offset);
        }
    }
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            std::copy(
                sparse_buffers[i].packed_node_props.begin(),
                sparse_buffers[i].packed_node_props.end(),
                prop_loader_node_props[pip + LITTLE_KERNEL_NUM].begin() +
                    sparse_buffers[i].node_prop_offset);
            printf("SP No.%d, big pipe No.%d, node_prop_offset %d\n", i, pip,
                   sparse_buffers[i].node_prop_offset);
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
    prop_loader_node_prop_buffers.clear();

    const size_t bytes_per_word = AXI_BUS_WIDTH / 8;
    size_t dists_per_word = AXI_BUS_WIDTH / DISTANCE_BITWIDTH;
    size_t total_output_words = apply_kernel_node_props.size();

    // --- 1.2: Setup buffers for LITTLE pipelines ---
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        const auto &little_partition = container.DPs[i];

        // Create edge buffers for each little pipeline
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            cl::Buffer edge_props_buf;

            cl_mem_ext_ptr_t hbm_ext_edge;
            hbm_ext_edge.flags =
                XCL_MEM_TOPOLOGY | acc.little_kernel_hbm_edge_id[pip];
            hbm_ext_edge.obj =
                dense_buffers[i].pipelines[pip].packed_edge_props.data();
            hbm_ext_edge.param = 0;

            size_t num_edge_words =
                dense_buffers[i].pipelines[pip].packed_edge_props.size();
            printf("LITTLE SP No.%d, pipe No.%d, edge words %d\n", i, pip,
                   (int)num_edge_words);
            OCL_CHECK(err, edge_props_buf = cl::Buffer(
                               acc.context,
                               CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                   CL_MEM_USE_HOST_PTR,
                               num_edge_words * bytes_per_word, &hbm_ext_edge,
                               &err));

            dense_buffers[i].pipelines[pip].edge_props_buffer = edge_props_buf;
        }

        std::cout << "  Created " << LITTLE_KERNEL_NUM
                  << " little pipeline edge buffers." << std::endl;
    }

    // --- 1.3: Setup buffers for BIG pipelines ---
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        const auto &big_partition = container.SPs[i];

        // Create edge buffers for each big pipeline
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            cl::Buffer edge_props_buf;

            cl_mem_ext_ptr_t hbm_ext_edge;
            hbm_ext_edge.flags =
                XCL_MEM_TOPOLOGY | acc.big_kernel_hbm_edge_id[pip];
            hbm_ext_edge.obj =
                sparse_buffers[i].pipelines[pip].packed_edge_props.data();
            hbm_ext_edge.param = 0;

            size_t num_edge_words =
                sparse_buffers[i].pipelines[pip].packed_edge_props.size();
            printf("BIG SP No.%d, pipe No.%d, edge words %d\n", i, pip,
                   (int)num_edge_words);
            OCL_CHECK(err, edge_props_buf = cl::Buffer(
                               acc.context,
                               CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                   CL_MEM_USE_HOST_PTR,
                               num_edge_words * bytes_per_word, &hbm_ext_edge,
                               &err));

            sparse_buffers[i].pipelines[pip].edge_props_buffer = edge_props_buf;
        }

        std::cout << "  Created " << BIG_KERNEL_NUM
                  << " big pipeline edge buffers." << std::endl;
    }

    // --- 1.4: Setup shared node property buffers for prop loaders ---
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        cl_mem_ext_ptr_t hbm_ext_node;
        hbm_ext_node.flags =
            XCL_MEM_TOPOLOGY | acc.little_kernel_hbm_node_id[pip];
        hbm_ext_node.obj = prop_loader_node_props[pip].data();
        hbm_ext_node.param = 0;

        size_t num_node_words = prop_loader_node_props[pip].size();
        cl::Buffer node_buf;
        OCL_CHECK(err, node_buf =
                           cl::Buffer(acc.context,
                                      CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                          CL_MEM_USE_HOST_PTR,
                                      num_node_words * bytes_per_word,
                                      &hbm_ext_node, &err));
        prop_loader_node_prop_buffers.push_back(node_buf);
    }

    // Create big node prop buffers for prop loaders
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        cl_mem_ext_ptr_t hbm_ext_node;
        hbm_ext_node.flags = XCL_MEM_TOPOLOGY | acc.big_kernel_hbm_node_id[pip];
        hbm_ext_node.obj =
            prop_loader_node_props[pip + LITTLE_KERNEL_NUM].data();
        hbm_ext_node.param = 0;

        size_t num_node_words =
            prop_loader_node_props[pip + LITTLE_KERNEL_NUM].size();
        cl::Buffer node_buf;
        OCL_CHECK(err, node_buf =
                           cl::Buffer(acc.context,
                                      CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX |
                                          CL_MEM_USE_HOST_PTR,
                                      num_node_words * bytes_per_word,
                                      &hbm_ext_node, &err));
        prop_loader_node_prop_buffers.push_back(node_buf);
    }

    // --- 1.5: Setup apply_kernel output buffer ---
    cl_mem_ext_ptr_t hbm_ext_output;
    hbm_ext_output.flags =
        XCL_MEM_TOPOLOGY |
        1; // Use first HBM bank
    hbm_ext_output.obj = nullptr;
    hbm_ext_output.param = 0;

    apply_kernel_host_outputs.resize(total_output_words, 0);
    OCL_CHECK(err,
              apply_kernel_output_buffer = cl::Buffer(
                  acc.context, CL_MEM_WRITE_ONLY | CL_MEM_EXT_PTR_XILINX,
                  total_output_words * bytes_per_word, &hbm_ext_output, &err));

    // --- 1.6: Setup apply_kernel node prop buffer ---
    cl_mem_ext_ptr_t hbm_ext_apply;
    hbm_ext_apply.flags = XCL_MEM_TOPOLOGY | 1;
    hbm_ext_apply.obj = apply_kernel_node_props.data();
    hbm_ext_apply.param = 0;

    size_t apply_node_words = apply_kernel_node_props.size();
    OCL_CHECK(err, apply_kernel_node_prop_buffer =
                       cl::Buffer(acc.context,
                                  CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX |
                                      CL_MEM_USE_HOST_PTR,
                                  apply_node_words * bytes_per_word,
                                  &hbm_ext_apply, &err));

    std::cout << "[SUCCESS] HBM buffers created: " << LITTLE_KERNEL_NUM
              << " little + " << BIG_KERNEL_NUM << " big pipelines, "
              << "total output size: " << total_output_words << " words."
              << std::endl;
}

void AlgorithmHost::update_data(const PartitionContainer &container) {
    std::cout
        << "--- [Host] Phase 2.1: Updating host-side data for new iteration ---"
        << std::endl;

    const size_t bytes_per_word = AXI_BUS_WIDTH / 8;
    const size_t dists_per_word = AXI_BUS_WIDTH / DISTANCE_BITWIDTH;

    // Update BIG partition node distances (shared across all big pipelines)
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        const auto &big_partition = container.SPs[i];
        const size_t bytes_per_dist = DISTANCE_BITWIDTH / 8;
        std::vector<char> temp_byte_buffer;
        temp_byte_buffer.clear();

        for (int j = 0; j < big_partition.num_vertices; ++j) {
            if ((temp_byte_buffer.size() % bytes_per_word) + bytes_per_dist >
                bytes_per_word) {
                size_t padding_needed =
                    bytes_per_word - (temp_byte_buffer.size() % bytes_per_word);
                temp_byte_buffer.insert(temp_byte_buffer.end(), padding_needed,
                                        0);
            }

            int global_id = big_partition.vtx_map_rev.at(j);
            uint32_t dist_val = h_masks[global_id];

            // printf("Putting Data: SP No.%d, local_id %d, global_id %d, dist
            // %.3f\n",
            //        i,  j, global_id, (float)dist_val);

            const char *data_ptr = reinterpret_cast<const char *>(&dist_val);
            temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                    data_ptr + bytes_per_dist);
        }

        // Update all big pipeline buffers with same node data
        size_t num_words =
            (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;

        sparse_buffers[i].packed_node_props.resize(num_words, 0);
        std::memcpy(sparse_buffers[i].packed_node_props.data(),
                    temp_byte_buffer.data(), temp_byte_buffer.size());
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            std::copy(
                sparse_buffers[i].packed_node_props.begin(),
                sparse_buffers[i].packed_node_props.end(),
                prop_loader_node_props[pip + LITTLE_KERNEL_NUM].begin() +
                    sparse_buffers[i].node_prop_offset);
        }

        // update big_dst_node_props for apply kernel
        {
            const size_t dist_per_word = bytes_per_word / bytes_per_dist;
            const size_t dst_word_number =
                (big_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            temp_byte_buffer.clear();
            temp_byte_buffer.reserve(dst_word_number * bytes_per_word);
            for (int j = 0; j < big_partition.num_dsts; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }
                int global_id = big_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            sparse_buffers[i].packed_dst_props.resize(
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word,
                0);
            std::memcpy(sparse_buffers[i].packed_dst_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            std::copy(sparse_buffers[i].packed_dst_props.begin(),
                      sparse_buffers[i].packed_dst_props.end(),
                      apply_kernel_node_props.begin() + big_dst_offset +
                          sparse_buffers[i].dst_prop_offset);
        }
    }

    // Update LITTLE partition node distances (shared across all little
    // pipelines)
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        const auto &little_partition = container.DPs[i];
        const size_t bytes_per_dist = DISTANCE_BITWIDTH / 8;
        std::vector<char> temp_byte_buffer;
        temp_byte_buffer.clear();

        for (int j = 0; j < little_partition.num_vertices; ++j) {
            if ((temp_byte_buffer.size() % bytes_per_word) + bytes_per_dist >
                bytes_per_word) {
                size_t padding_needed =
                    bytes_per_word - (temp_byte_buffer.size() % bytes_per_word);
                temp_byte_buffer.insert(temp_byte_buffer.end(), padding_needed,
                                        0);
            }
            int global_id = little_partition.vtx_map_rev.at(j);
            uint32_t dist_val = h_masks[global_id];
            // printf("Putting Data: DP No.%d, local_id %d, global_id %d, dist
            // %.3f\n",
            //        i,  j, global_id, (float)dist_val);
            const char *data_ptr = reinterpret_cast<const char *>(&dist_val);
            temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                    data_ptr + bytes_per_dist);
        }

        // Update all little pipeline buffers with same node data
        size_t num_words =
            (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word;

        dense_buffers[i].packed_node_props.resize(num_words, 0);
        std::memcpy(dense_buffers[i].packed_node_props.data(),
                    temp_byte_buffer.data(), temp_byte_buffer.size());
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            std::copy(dense_buffers[i].packed_node_props.begin(),
                      dense_buffers[i].packed_node_props.end(),
                      prop_loader_node_props[pip].begin() +
                          dense_buffers[i].node_prop_offset);
        }

        // update little_dst_node_props for apply kernel
        {
            const size_t dist_per_word = bytes_per_word / bytes_per_dist;
            const size_t dst_word_number =
                (little_partition.num_dsts + dist_per_word - 1) / dist_per_word;
            temp_byte_buffer.clear();
            temp_byte_buffer.reserve(dst_word_number * bytes_per_word);
            for (int j = 0; j < little_partition.num_dsts; ++j) {
                if ((temp_byte_buffer.size() % bytes_per_word) +
                        bytes_per_dist >
                    bytes_per_word) {
                    size_t padding_needed =
                        bytes_per_word -
                        (temp_byte_buffer.size() % bytes_per_word);
                    temp_byte_buffer.insert(temp_byte_buffer.end(),
                                            padding_needed, 0);
                }
                int global_id = little_partition.vtx_map_rev.at(j);
                uint32_t dist_val = h_masks[global_id];
                const char *data_ptr =
                    reinterpret_cast<const char *>(&dist_val);
                temp_byte_buffer.insert(temp_byte_buffer.end(), data_ptr,
                                        data_ptr + bytes_per_dist);
            }
            dense_buffers[i].packed_dst_props.resize(
                (temp_byte_buffer.size() + bytes_per_word - 1) / bytes_per_word,
                0);
            std::memcpy(dense_buffers[i].packed_dst_props.data(),
                        temp_byte_buffer.data(), temp_byte_buffer.size());
            std::copy(dense_buffers[i].packed_dst_props.begin(),
                      dense_buffers[i].packed_dst_props.end(),
                      apply_kernel_node_props.begin() +
                          dense_buffers[i].dst_prop_offset);
        }
    }

    // Print prop loader node prop data
    // for (size_t pip = 0; pip < 20; pip += 12) {
    //     const auto &node_props = prop_loader_node_props[pip];
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
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        for (int part = 0; part < container.num_sparse_partitions; ++part) {
            OCL_CHECK(
                err,
                err = acc.big_gs_queue[pip].enqueueMigrateMemObjects(
                    {sparse_buffers[part].pipelines[pip].edge_props_buffer},
                    0 /* 0 means from host*/));
        }
    }

    // Transfer edge buffers for all little pipelines across all dense
    // partitions
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        for (int part = 0; part < container.num_dense_partitions; ++part) {
            OCL_CHECK(
                err, err = acc.little_gs_queue[pip].enqueueMigrateMemObjects(
                         {dense_buffers[part].pipelines[pip].edge_props_buffer},
                         0 /* 0 means from host*/));
        }
    }

    // Transfer apply_kernel node buffer
    OCL_CHECK(err,
              err = acc.apply_queue.enqueueMigrateMemObjects(
                  {apply_kernel_node_prop_buffer}, 0 /* 0 means from host*/));

    // Transfer prop loader node prop buffers
    for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
        OCL_CHECK(err,
                  err = acc.little_prop_loader_queue[pip]
                            .enqueueMigrateMemObjects(
                                {prop_loader_node_prop_buffers[pip]},
                                0 /* 0 means from host*/));
    }
    for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
        OCL_CHECK(err,
                  err = acc.big_prop_loader_queue[pip]
                            .enqueueMigrateMemObjects(
                                {prop_loader_node_prop_buffers[pip +
                                                               LITTLE_KERNEL_NUM]},
                                0 /* 0 means from host*/));
    }

    // Wait for all transfers to complete
    for (auto &q : acc.big_gs_queue)
        q.finish();
    for (auto &q : acc.little_gs_queue)
        q.finish();
    for (auto &q : acc.big_prop_loader_queue)
        q.finish();
    for (auto &q : acc.little_prop_loader_queue)
        q.finish();
    acc.apply_queue.finish();

    std::cout
        << "[SUCCESS] All data packed and transferred for current iteration."
        << std::endl;
}

// --- PHASE 3: KERNEL EXECUTION ---
// MODIFIED: Kernel arguments are updated to match the new kernel signature.
void AlgorithmHost::execute_kernel_iteration(
    const PartitionContainer &container) {
    cl_int err;
    // std::cout << "--- [Host] Phase 3: Enqueuing kernel tasks ---" <<
    // std::endl;

    auto enqueue_start = std::chrono::high_resolution_clock::now();

    // uint32_t byte_per_word = AXI_BUS_WIDTH / 8;
    // uint32_t dists_per_word = AXI_BUS_WIDTH / DISTANCE_BITWIDTH;
    uint32_t little_dst_word_num = 0, big_dst_word_num = 0;

    for (int i = 0; i < container.num_dense_partitions; ++i) {
        little_dst_word_num += dense_buffers[i].packed_dst_props.size();
    }
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        big_dst_word_num += sparse_buffers[i].packed_dst_props.size();
    }

    // 3.0: Enqueue prop loader kernels (one per pipeline)
    {
        uint32_t num_little_partitions = container.num_dense_partitions;
        uint32_t num_big_partitions = container.num_sparse_partitions;

        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            auto &kernel = acc.little_prop_loader_krnls[pip];
            auto &buffer = prop_loader_node_prop_buffers[pip];
            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err,
                      err = kernel.setArg(arg_idx++, num_little_partitions));
            OCL_CHECK(err,
                      err = acc.little_prop_loader_queue[pip].enqueueTask(
                          kernel, nullptr,
                          (acc.little_prop_loader_events.empty()
                               ? nullptr
                               : &acc.little_prop_loader_events[pip])));
        }

        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            auto &kernel = acc.big_prop_loader_krnls[pip];
            auto &buffer =
                prop_loader_node_prop_buffers[pip + LITTLE_KERNEL_NUM];
            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err,
                      err = kernel.setArg(arg_idx++, num_big_partitions));
            OCL_CHECK(err,
                      err = acc.big_prop_loader_queue[pip].enqueueTask(
                          kernel, nullptr,
                          (acc.big_prop_loader_events.empty()
                               ? nullptr
                               : &acc.big_prop_loader_events[pip])));
        }
    }

    // 3.1: Enqueue BIG gs kernels (one per pipeline, each with different edges,
    // same nodes)
    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        for (int pip = 0; pip < BIG_KERNEL_NUM; ++pip) {
            auto &kernel = acc.big_gs_krnls[pip];
            auto &buffer = sparse_buffers[i].pipelines[pip].edge_props_buffer;
            uint32_t original_num_edges =
                container.SPs[i].pipeline_edges[pip].num_edges;
            // Use 8 dummy edges if the pipeline has no edges
            uint32_t pip_num_edges =
                (original_num_edges == 0) ? 8 : original_num_edges;
            uint32_t big_num_vertices = container.SPs[i].num_vertices;
            uint32_t big_num_dsts = container.SPs[i].num_dsts;
            uint32_t memory_offset = sparse_buffers[i].node_prop_offset;

            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, big_num_vertices));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, pip_num_edges));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, big_num_dsts));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, memory_offset));

            printf(
                "Enqueue Big GS Kernel: SP No.%d, big pipe No.%d, num_vertices "
                "%d, num_edges %d, num_dsts %d, memory_offset %d\n",
                i, pip, big_num_vertices, pip_num_edges, big_num_dsts,
                memory_offset);

            OCL_CHECK(err,
                      err = acc.big_gs_queue[pip].enqueueTask(
                          kernel, nullptr, &acc.big_kernel_events[i][pip]));
        }
    }

    // 3.2: Enqueue LITTLE gs kernels (one per pipeline, each with different
    // edges, same nodes)
    for (int i = 0; i < container.num_dense_partitions; ++i) {
        for (int pip = 0; pip < LITTLE_KERNEL_NUM; ++pip) {
            auto &kernel = acc.little_gs_krnls[pip];
            auto &buffer = dense_buffers[i].pipelines[pip].edge_props_buffer;
            uint32_t original_num_edges =
                container.DPs[i].pipeline_edges[pip].num_edges;
            // Use 8 dummy edges if the pipeline has no edges
            uint32_t pip_num_edges =
                (original_num_edges == 0) ? 8 : original_num_edges;
            uint32_t little_num_vertices = container.DPs[i].num_vertices;
            uint32_t little_num_dsts = container.DPs[i].num_dsts;
            uint32_t memory_offset = dense_buffers[i].src_buf_offset;

            int arg_idx = 0;
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, buffer));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, little_num_vertices));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, pip_num_edges));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, little_num_dsts));
            OCL_CHECK(err, err = kernel.setArg(arg_idx++, memory_offset));

            printf("Enqueue Little GS Kernel: DP No.%d, little pipe No.%d, "
                   "num_vertices %d, num_edges %d, num_dsts %d, memory_offset "
                   "%d\n",
                   i, pip, little_num_vertices, pip_num_edges, little_num_dsts,
                   memory_offset);

            OCL_CHECK(err,
                      err = acc.little_gs_queue[pip].enqueueTask(
                          kernel, nullptr, &acc.little_kernel_events[i][pip]));
        }
    }

    // 3.3: Enqueue apply_kernel (receives merged streams from little_merger and
    // big_merger)
    {
        auto &apply_kernel = acc.apply_krnl;

        
        int arg_idx = 0;
        OCL_CHECK(err, err = apply_kernel.setArg(
                               arg_idx++, apply_kernel_node_prop_buffer));
        OCL_CHECK(err, err = apply_kernel.setArg(
                               arg_idx++, apply_kernel_output_buffer));
        OCL_CHECK(err,
                  err = apply_kernel.setArg(arg_idx++, little_dst_word_num));
        OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++, big_dst_word_num));
        OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++, (uint32_t)0));
        OCL_CHECK(err, err = apply_kernel.setArg(arg_idx++, big_dst_offset));


        OCL_CHECK(err, err = acc.apply_queue.enqueueTask(
                           apply_kernel, nullptr, &acc.apply_kernel_event));
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

    // Read from the apply_kernel output buffer
    OCL_CHECK(err, err = acc.apply_queue.enqueueReadBuffer(
                       apply_kernel_output_buffer, CL_FALSE, 0,
                       apply_kernel_host_outputs.size() * sizeof(bus_word_t),
                       apply_kernel_host_outputs.data()));

    // Wait for all transfers to complete
    acc.apply_queue.finish();

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
    bool changed = false;
    std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                 "convergence ---"
              << std::endl;

    std::map<int, distance_t> min_distances;
    const int dists_per_word = AXI_BUS_WIDTH / DISTANCE_BITWIDTH;

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
            int bit_offset = dist_in_word * DISTANCE_BITWIDTH;
            ap_fixed_pod_t dist_pod =
                apply_kernel_host_outputs[word_idx].range(
                    bit_offset + DISTANCE_BITWIDTH - 1, bit_offset);

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
                apply_kernel_host_outputs[word_idx].range(
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

    bool changed = false;
    std::cout << "--- [Host] Phase 5: Unpacking results and checking for "
                 "convergence ---"
              << std::endl;

    std::map<int, uint32_t> merged_masks;
    const int props_per_word = AXI_BUS_WIDTH / 32;

    int word_idx = 0;
    int prop_in_word = 0;

    auto consume_partition_dsts = [&](const PartitionDescriptor &partition) {
        for (int local_id = 0; local_id < partition.num_dsts; ++local_id) {
            int bit_offset = prop_in_word * 32;
            uint32_t out_mask = (uint32_t)apply_kernel_host_outputs[word_idx]
                                    .range(bit_offset + 31, bit_offset);

            auto it = partition.vtx_map_rev.find(local_id);
            if (it != partition.vtx_map_rev.end()) {
                int global_id = it->second;
                if (global_id < m_num_vertices) {
                    merged_masks[global_id] |= out_mask;
                }
            }

            prop_in_word++;
            if (prop_in_word >= props_per_word) {
                prop_in_word = 0;
                word_idx++;
            }
        }
    };

    for (int i = 0; i < container.num_dense_partitions; ++i) {
        consume_partition_dsts(container.DPs[i]);
    }

    if (prop_in_word != 0) {
        prop_in_word = 0;
        word_idx++;
    }

    for (int i = 0; i < container.num_sparse_partitions; ++i) {
        consume_partition_dsts(container.SPs[i]);
    }

    for (const auto &[global_id, new_mask] : merged_masks) {
        if (global_id < m_num_vertices && new_mask != h_masks[global_id]) {
            h_masks[global_id] = new_mask;
            changed = true;
        }
    }

    if (changed) {
        std::cout << "[INFO] Masks updated. Preparing for next iteration."
                  << std::endl;
    } else {
        std::cout << "[INFO] No mask updates. Algorithm has converged."
                  << std::endl;
    }

    return !changed;
}

const std::vector<unsigned int> &AlgorithmHost::get_results() const {
    static std::vector<unsigned int> final_masks;
    final_masks.clear();
    final_masks.reserve(h_masks.size());
    for (uint32_t mask : h_masks) {
        final_masks.push_back(mask);
    }
    return final_masks;
}
