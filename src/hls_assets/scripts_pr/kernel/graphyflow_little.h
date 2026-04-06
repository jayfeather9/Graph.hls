#ifndef __GRAPHYFLOW_GRAPHYFLOW_LITTLE_H__
#define __GRAPHYFLOW_GRAPHYFLOW_LITTLE_H__

#include <ap_axi_sdata.h>
#include <ap_fixed.h>
#include <ap_int.h>
#include <hls_stream.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define PE_NUM 8
#define DBL_PE_NUM 16
#define LOG_PE_NUM 3
#ifdef GRAPHYFLOW_HW_EMU_LIMIT_MAX_DST
#define MAX_DST_LITTLE 512
#else
#define MAX_DST_LITTLE 65536
#endif
#define MAX_NUM MAX_DST_LITTLE

#define L 3
#define SRC_BUFFER_SIZE 4096
#define LOG_SRC_BUFFER_SIZE 12

#define LOCAL_ID_BITWIDTH 32
#define LOCAL_ID_MSB (LOCAL_ID_BITWIDTH - 1)
typedef ap_uint<LOCAL_ID_BITWIDTH> local_id_t;
#define INVALID_LOCAL_ID_LITTLE (local_id_t(1) << LOCAL_ID_MSB)

// --- New Bitwidth Definitions for HLS Synthesis ---
#define NODE_ID_BITWIDTH 32
#define DISTANCE_BITWIDTH 32
#define DISTANCE_INTEGER_PART 16
#define DISTANCE_SIGNED 0
#define WEIGHT_BITWIDTH DISTANCE_BITWIDTH
#define WEIGHT_INTEGER_PART DISTANCE_INTEGER_PART
#define OUT_END_MARKER_BITWIDTH 4
#define DIST_PER_WORD 16
#define LOG_DIST_PER_WORD 4

// --- New Memory Word and Bus Definitions ---
#define AXI_BUS_WIDTH 512
#define EDGE_PROP_BITS 0
#define EDGE_PROP_STORAGE_BITS ((EDGE_PROP_BITS == 0) ? 1 : EDGE_PROP_BITS)
#define EDGE_WEIGHT_BITS 0
#define EDGE_WEIGHT_LSB 0
#define EDGE_SRC_PAYLOAD_LSB NODE_ID_BITWIDTH
#define EDGE_SRC_PAYLOAD_MSB ((2 * NODE_ID_BITWIDTH) - 1)
#define EDGE_PROP_PAYLOAD_LSB 64
#define EDGE_PROP_PAYLOAD_MSB (EDGE_PROP_PAYLOAD_LSB + EDGE_PROP_BITS - 1)
#define EDGE_PAYLOAD_BITS 64
#define EDGES_PER_WORD 8

#define REDUCE_MEM_WIDTH 64
typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;
typedef ap_uint<REDUCE_MEM_WIDTH> reduce_word_t;

// --- New Packing-related Constants ---
// Number of distances that can be packed into a single reduce memory word.
#define DISTANCES_PER_REDUCE_WORD 2
#define LOG_DISTANCES_PER_REDUCE_WORD                                      \
    ((LOG_DIST_PER_WORD > 3) ? (LOG_DIST_PER_WORD - 3) : 0)

// --- Redefinition of Core Graph Types for HLS ---
// These typedefs override the standard integer types from common.h for
// synthesis.
typedef ap_uint<NODE_ID_BITWIDTH> node_id_t;
typedef ap_uint<32> edge_id_t; // edge_id_t is not customized yet, keep as is.
#if DISTANCE_SIGNED
typedef ap_int<DISTANCE_BITWIDTH>
    ap_fixed_pod_t; // Used to hold bit representation of ap_fixed types
#else
typedef ap_uint<DISTANCE_BITWIDTH>
    ap_fixed_pod_t; // Used to hold bit representation of ap_fixed types
#endif
typedef ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;
typedef ap_uint<OUT_END_MARKER_BITWIDTH> out_end_marker_t;
typedef ap_uint<EDGE_PROP_STORAGE_BITS> edge_prop_t;
typedef ap_axiu<256, 0, 0, 0> node_dist_pkt_t;
typedef ap_axiu<512, 0, 0, 0> write_burst_pkt_t;
typedef ap_axiu<64, 0, 0, 0> little_out_pkt_t;
typedef ap_axiu<32, 0, 0, 0> ppb_request_pkt_t;
typedef ap_axiu<512, 0, 0, 32> ppb_response_pkt_t;
typedef ap_axiu<512, 0, 0, 0> cacheline_data_pkt_t;

#if DISTANCE_SIGNED
const ap_fixed_pod_t INFINITY_POD =
    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1)) - 1;
const ap_fixed_pod_t NEG_INFINITY_POD =
    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1));
#else
const ap_fixed_pod_t INFINITY_POD = ~ap_fixed_pod_t(0);
const ap_fixed_pod_t NEG_INFINITY_POD = 0;
#endif
const distance_t INFINITY_DIST_VAL = static_cast<distance_t>(INFINITY_POD);
// --- Struct Type Definitions ---
struct __attribute__((packed)) edge_t {
    node_id_t src_id;
    local_id_t dst_id;
#if EDGE_PROP_BITS > 0
    edge_prop_t edge_prop;
#endif
};

struct __attribute__((packed)) update_t_little {
    local_id_t node_id;
    ap_fixed_pod_t prop;
};

struct __attribute__((packed)) ppb_request_t {
    ap_uint<32> request_round;
    bool end_flag;
};

struct __attribute__((packed)) ppb_response_t {
    bus_word_t data;
    ap_uint<32> addr;
    bool end_flag;
};

struct __attribute__((packed)) edge_descriptor_batch_t {
    edge_t edges[PE_NUM];
};

struct __attribute__((packed)) update_tuple_t_little {
    update_t_little data[PE_NUM];
};

extern "C" void
 graphyflow_little(const bus_word_t* edge_props,
 int32_t num_nodes,
 int32_t num_edges,
 int32_t dst_num,
 int32_t memory_offset,
 hls::stream<ppb_request_pkt_t> &ppb_req_stream,
 hls::stream<ppb_response_pkt_t> &ppb_resp_stream,
 hls::stream<little_out_pkt_t> &kernel_out_stream 
);

#endif // __GRAPHYFLOW_GRAPHYFLOW_LITTLE_H__
