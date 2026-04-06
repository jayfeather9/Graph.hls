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
#ifdef EMULATION
#define MAX_NUM 512
#else
#define MAX_NUM 122880
#endif
#define L 3
#define SRC_BUFFER_SIZE 4096
#define LOG_SRC_BUFFER_SIZE 12

// --- New Bitwidth Definitions for HLS Synthesis ---
#define NODE_ID_BITWIDTH 32
#define DISTANCE_BITWIDTH 32
#define DISTANCE_INTEGER_PART 16
#define WEIGHT_BITWIDTH DISTANCE_BITWIDTH
#define WEIGHT_INTEGER_PART DISTANCE_INTEGER_PART
#define OUT_END_MARKER_BITWIDTH 4
#define DIST_PER_WORD 16 // AXI_BUS_WIDTH / DISTANCE_BITWIDTH = 512 / 32 = 16
#define LOG_DIST_PER_WORD                                            \
    4 // log2(AXI_BUS_WIDTH / DISTANCE_BITWIDTH) = log2(512 / 32) = log2(16) = 4

// --- New Memory Word and Bus Definitions ---
#define AXI_BUS_WIDTH 512

#define REDUCE_MEM_WIDTH 64
typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;
typedef ap_uint<REDUCE_MEM_WIDTH> reduce_word_t;

const int INFINITY_DIST = 16384;

// --- New Packing-related Constants ---
// Number of distances that can be packed into a single reduce memory word.
#define DISTANCES_PER_REDUCE_WORD (REDUCE_MEM_WIDTH / DISTANCE_BITWIDTH)

// --- Redefinition of Core Graph Types for HLS ---
// These typedefs override the standard integer types from common.h for
// synthesis.
typedef ap_uint<NODE_ID_BITWIDTH> node_id_t;
typedef ap_uint<32> edge_id_t; // edge_id_t is not customized yet, keep as is.
typedef ap_uint<DISTANCE_BITWIDTH>
    ap_fixed_pod_t; // Used to hold bit representation of ap_fixed types
typedef ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;
typedef ap_uint<OUT_END_MARKER_BITWIDTH> out_end_marker_t;
typedef ap_axiu<256, 0, 0, 0> node_dist_pkt_t;
typedef ap_axiu<512, 0, 0, 0> write_burst_pkt_t;
typedef ap_axiu<64, 0, 0, 0> little_out_pkt_t;
typedef ap_axiu<32, 0, 0, 0> ppb_request_pkt_t;
typedef ap_axiu<512, 0, 0, 32> ppb_response_pkt_t;
typedef ap_axiu<512, 0, 0, 0> cacheline_data_pkt_t;
// --- Struct Type Definitions ---
struct __attribute__((packed)) edge_t {
    node_id_t src_id;
    ap_uint<20> dst_id;
};

struct __attribute__((packed)) update_t_little {
    ap_uint<20> node_id;
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
