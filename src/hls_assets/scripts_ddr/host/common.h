#ifndef __COMMON_H__
#define __COMMON_H__

#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <ap_fixed.h>
#include <ap_int.h>
#include <stdint.h>

#ifndef __SYNTHESIS__
#include "xcl2.h"
#endif

// --- Customizable Bitwidth Macros ---
// These macros define the bitwidths for core data types.
// They are used by the host for data packing and by the kernel for synthesis.
#define NODE_ID_BITWIDTH 32
#define DISTANCE_BITWIDTH 32
#define DISTANCE_INTEGER_PART                                                  \
    16 // Number of bits for the integer part of distance
#define WEIGHT_BITWIDTH DISTANCE_BITWIDTH
#define WEIGHT_INTEGER_PART                                                    \
    DISTANCE_INTEGER_PART // Number of bits for the integer part of weight
#define OUT_END_MARKER_BITWIDTH 4
#define SRC_BUFFER_SIZE 4096

#ifdef EMULATION
const int LITTLE_MAX_DST = 512;
const int BIG_MAX_DST = 512;
#else
const int LITTLE_MAX_DST = 122880; // 65536
const int BIG_MAX_DST = 655360;   // 524288
#endif

// --- Host-side definition for the AXI bus word ---
#define AXI_BUS_WIDTH 512
typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;
typedef ap_uint<DISTANCE_BITWIDTH> ap_fixed_pod_t;
typedef ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;
typedef ap_fixed<WEIGHT_BITWIDTH, WEIGHT_INTEGER_PART> weight_t;
typedef ap_uint<OUT_END_MARKER_BITWIDTH> out_end_marker_t;

// A constant representing infinity for distance initialization
const int INFINITY_DIST = 16384;

// Default throughput estimates for graph partitioning (edges per ms per pipeline).
#ifndef BIG_EDGE_PER_MS
#define BIG_EDGE_PER_MS 280000
#endif
#ifndef LITTLE_EDGE_PER_MS
#define LITTLE_EDGE_PER_MS 1040000
#endif

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
    // Map from original global vertex ID to compressed local ID
    std::unordered_map<int, int> vtx_map;
    // Map from compressed local ID to original global vertex ID
    std::unordered_map<int, int> vtx_map_rev;
};

#define KERNEL_OUTPUT_BATCH_TYPE KernelOutputBatch
#define BATCH_TYPE edge_des_burst_t
#define EDGE_TYPE edge_t
#define NODE_TYPE node_t

#define PE_NUM 8

#endif // __COMMON_H__