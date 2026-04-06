#ifndef __GENERATED_HOST_H__
#define __GENERATED_HOST_H__

#include "acc_setup/acc_setup.h"
#include "common.h"
#include "graph_preprocess/graph_preprocess.h"
#include <vector>

// Buffer for one big or little pipeline, host + device side
struct PipelineBuffer {
    std::vector<bus_word_t, aligned_allocator<bus_word_t>> packed_edge_props;
    cl::Buffer edge_props_buffer;
};

// One dense / sparse partition buffer including multiple pipelines
struct PartitionBuffer {
    std::vector<PipelineBuffer> pipelines;
    std::vector<bus_word_t, aligned_allocator<bus_word_t>> packed_dst_props;
    std::vector<bus_word_t, aligned_allocator<bus_word_t>> packed_node_props;
    uint32_t node_prop_offset;
    uint32_t dst_prop_offset;
    uint32_t src_buf_offset;
};

class AlgorithmHost {
  public:
    AlgorithmHost(AccDescriptor &acc);

    // --- MODIFICATION: Updated function signatures to use new data structures
    // ---
    void prepare_data(const PartitionContainer &container, int start_node);
    void setup_buffers(const PartitionContainer &container);
    void update_data(const PartitionContainer &container);
    void transfer_data_to_fpga(const PartitionContainer &container);
    void execute_kernel_iteration(const PartitionContainer &container);
    void transfer_data_from_fpga();
    bool check_convergence_and_update(const PartitionContainer &container);
    const std::vector<unsigned int> &get_results() const;

  private:
    AccDescriptor &acc;

    // Algorithm state
    int m_num_vertices;

    // Host-side master CC bitmask vector using original (global) vertex IDs
    std::vector<uint32_t> h_masks;

    // Buffer containers for kernels (big + little)
    std::vector<PartitionBuffer> dense_buffers, sparse_buffers;

    // Buffer containers for prop loader kernels (one entry per kernel instance)
    std::vector<cl::Buffer> prop_loader_node_prop_buffers;
    cl::Buffer apply_kernel_output_buffer;
    std::vector<bus_word_t, aligned_allocator<bus_word_t>>
        apply_kernel_node_props;
    cl::Buffer apply_kernel_node_prop_buffer;
    std::vector<std::vector<bus_word_t, aligned_allocator<bus_word_t>>>
        prop_loader_node_props;
    std::vector<bus_word_t, aligned_allocator<bus_word_t>>
        apply_kernel_host_outputs;
    uint32_t big_dst_offset = 0;
};

#endif // __GENERATED_HOST_H__
