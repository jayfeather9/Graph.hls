#ifndef GRAPH_PREPROCESS_H
#define GRAPH_PREPROCESS_H

#include "common.h"
#include "host_config.h"

#include <algorithm> // std::swap
#include <iomanip>
#include <numeric> // std::iota
#include <vector>

/**
 * @struct PipelineEdges
 * @brief Holds edge data for a single pipeline instance.
 */
typedef struct PipelineEdges {
    unsigned int pipeline_id;
    unsigned int num_edges;
    std::vector<int> offsets; // Per-vertex offsets for this pipeline's edges
    std::vector<int> columns; // Destination IDs
    std::vector<int> weights; // Edge weights
} PipelineEdges;

/**
 * @struct PartitionDescriptor
 * @brief Describes a single graph partition (either big or little).
 * * This structure holds a self-contained CSR representation of a graph
 * partition, including the mapping between its local, compressed vertex IDs and
 * the original global vertex IDs. Edges are distributed among multiple
 * pipelines.
 */
typedef struct PartitionDescriptor {
    // Metadata about the partition
    unsigned int num_edges;    // Total edges across all pipelines
    unsigned int num_vertices; // Number of vertices *within this partition*
    unsigned int num_dsts;     // Number of destination vertices
    bool is_dense;             // True for little kernel, false for big kernel
    unsigned int
        num_pipelines; // Number of pipeline instances for this partition

    // The core graph data for this partition
    // Vertex mappings are shared across all pipelines
    std::unordered_map<int, int> vtx_map;     // Global ID -> Local ID
    std::unordered_map<int, int> vtx_map_rev; // Local ID -> Global ID

    // Edge data distributed across pipelines
    std::vector<PipelineEdges> pipeline_edges; // One entry per pipeline

} PartitionDescriptor;

/**
 * @struct PartitionContainer
 * @brief A container holding all graph partitions.
 * * This top-level structure contains metadata for the entire graph and holds
 * separate vectors for partitions assigned to sparse (big) and dense (little)
 * kernels.
 */
typedef struct PartitionContainer {
    // Global graph metadata
    unsigned int num_graph_vertices;
    unsigned int num_graph_edges;

    // Partition collections
    unsigned int num_dense_partitions;
    unsigned int num_sparse_partitions;

    std::vector<PartitionDescriptor>
        DPs; // Partitions for Dense (little) kernels
    std::vector<PartitionDescriptor> SPs; // Partitions for Sparse (big) kernels

} PartitionContainer;

/**
 * @brief Partitions the global graph and preprocesses each partition into a CSR
 * format.
 * * This function implements the new partitioning strategy based on destination
 * vertices and creates a container with all the partitioned graph data.
 * @param graph The input global graph in CSR format.
 * @return A PartitionContainer object containing all processed partitions.
 */
PartitionContainer partitionGraph(const GraphCSR *graph,
                                  double big_edge_per_ms_per_pipe = BIG_EDGE_PER_MS,
                                  double little_edge_per_ms_per_pipe = LITTLE_EDGE_PER_MS);

#endif