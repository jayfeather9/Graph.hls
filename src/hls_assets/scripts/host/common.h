#ifndef __COMMON_H__
#define __COMMON_H__

#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <ap_fixed.h>
#include <ap_int.h>
#include <cstdint>

#ifndef __SYNTHESIS__
#include "xcl2.h"
#endif

#include "algorithm_config.h"
#include "host_config.h"

// Bitwidth macros come from host_config.h
#ifndef DISTANCE_BITWIDTH
#error "DISTANCE_BITWIDTH must be defined in host_config.h"
#endif
#ifndef DISTANCE_INTEGER_PART
#error "DISTANCE_INTEGER_PART must be defined in host_config.h"
#endif
#ifndef DISTANCE_SIGNED
#error "DISTANCE_SIGNED must be defined in host_config.h"
#endif
#ifndef DIST_PER_WORD
#error "DIST_PER_WORD must be defined in host_config.h"
#endif
#ifndef NODE_ID_BITWIDTH
#define NODE_ID_BITWIDTH 32
#endif
#ifndef OUT_END_MARKER_BITWIDTH
#define OUT_END_MARKER_BITWIDTH 4
#endif
#ifndef SRC_BUFFER_SIZE
#define SRC_BUFFER_SIZE 4096
#endif

#ifdef GRAPHYFLOW_HW_EMU_LIMIT_MAX_DST
const int LITTLE_MAX_DST = (MAX_DST_LITTLE < 512 ? MAX_DST_LITTLE : 512);
const int BIG_MAX_DST = (MAX_DST_BIG < 512 ? MAX_DST_BIG : 512);
#else
const int LITTLE_MAX_DST = MAX_DST_LITTLE;
const int BIG_MAX_DST = MAX_DST_BIG;
#endif

// --- Host-side definition for the AXI bus word ---
#define AXI_BUS_WIDTH 512
typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;
#if DISTANCE_SIGNED
typedef ap_int<DISTANCE_BITWIDTH> ap_fixed_pod_t;
#else
typedef ap_uint<DISTANCE_BITWIDTH> ap_fixed_pod_t;
#endif
#if DISTANCE_SIGNED
typedef ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;
#else
typedef ap_ufixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;
#endif
typedef distance_t weight_t;
typedef ap_uint<OUT_END_MARKER_BITWIDTH> out_end_marker_t;

// A constant representing infinity for distance initialization
const ap_fixed_pod_t INFINITY_POD =
#if DISTANCE_SIGNED
    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1)) - 1;
#else
    ~ap_fixed_pod_t(0);
#endif
const ap_fixed_pod_t NEG_INFINITY_POD =
#if DISTANCE_SIGNED
    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1));
#else
    ap_fixed_pod_t(0);
#endif
const distance_t INFINITY_DIST_VAL = static_cast<distance_t>(INFINITY_POD);

// Default algorithm configuration (SSSP-compatible)
static const AlgorithmConfig kDefaultAlgorithmConfig{
    /*target_property=*/"dist",
    /*numeric_kind=*/NumericKind::Int,
    /*bitwidth=*/32,
    /*int_width=*/32,
    /*convergence_mode=*/ConvergenceMode::MinImprove,
    /*delta_threshold=*/0.0f,
    /*max_iterations=*/0,
    /*needs_edge_weight=*/true,
    /*needs_out_degree=*/false,
    /*update_mode=*/UpdateMode::Min,
    /*active_mask=*/0u,
    /*inf_value=*/0u,
    /*algorithm_kind=*/AlgorithmKind::Sssp,
};

// --- Graph Type Definitions ---
typedef uint32_t edge_id_t;
typedef uint32_t node_id_t;
// typedef uint32_t ap_fixed_pod_t;

// Structure to hold the graph in Compressed Sparse Row (CSR) format
struct GraphCSR {
    int num_vertices;
    int num_edges;
    int num_dsts;
    std::vector<int> offsets;
    std::vector<int> columns;
    std::vector<int> weights;
    std::vector<uint64_t> edge_props;
    // Map from original global vertex ID to compressed local ID
    std::unordered_map<int, int> vtx_map;
    // Map from compressed local ID to original global vertex ID
    std::unordered_map<int, int> vtx_map_rev;
};

#ifndef EDGE_PROP_BITS
#define EDGE_PROP_BITS 0
#endif

#define EDGE_PAYLOAD_BITS                                                   \
    (NODE_ID_BITWIDTH + NODE_ID_BITWIDTH + EDGE_PROP_BITS)
#define EDGES_PER_WORD (AXI_BUS_WIDTH / EDGE_PAYLOAD_BITS)

#define KERNEL_OUTPUT_BATCH_TYPE KernelOutputBatch
#define BATCH_TYPE edge_des_burst_t
#define EDGE_TYPE edge_t
#define NODE_TYPE node_t

#define PE_NUM 8

inline ap_fixed_pod_t distance_to_pod(distance_t value) {
    return *reinterpret_cast<ap_fixed_pod_t *>(&value);
}

inline distance_t pod_to_distance(ap_fixed_pod_t value) {
    return *reinterpret_cast<distance_t *>(&value);
}

#endif // __COMMON_H__
