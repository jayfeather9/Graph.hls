#ifndef __SHARED_KERNEL_PARAMS_H__
#define __SHARED_KERNEL_PARAMS_H__

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
#define L 4
#define SRC_BUFFER_SIZE 4096
#define LOG_SRC_BUFFER_SIZE 12

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

#define BIG_MERGER_LENGTH 4
#define LITTLE_MERGER_LENGTH 4

#define REDUCE_MEM_WIDTH 64
typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;
typedef ap_uint<REDUCE_MEM_WIDTH> reduce_word_t;

const int INFINITY_DIST = 16384;

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
typedef ap_axiu<512, 0, 0, 32> write_burst_w_dst_pkt_t;
typedef ap_axiu<32, 0, 0, 8> cacheline_request_pkt_t;
typedef ap_axiu<512, 0, 0, 8> cacheline_response_pkt_t;
typedef ap_axiu<32, 0, 0, 0> ppb_request_pkt_t;
typedef ap_axiu<512, 0, 0, 32> ppb_response_pkt_t;
typedef ap_axiu<512, 0, 0, 0> cacheline_data_pkt_t;
// --- Struct Type Definitions ---
struct __attribute__((packed)) in_write_burst_w_dst_pkt_t {
    bus_word_t data;
    uint32_t dest_addr;
    bool end_flag;
};

extern "C" void
 graphyflow_big(const bus_word_t* edge_props,
 int32_t num_nodes,
 int32_t num_edges,
 int32_t dst_num,
 int32_t memory_offset,
 hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
 hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream,
 hls::stream<write_burst_pkt_t> &kernel_out_stream 
);

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


extern "C" void
apply_kernel(bus_word_t *node_props,
             bus_word_t *output,
             uint32_t little_kernel_length,
             uint32_t big_kernel_length,
             uint32_t little_kernel_st_offset,
             uint32_t big_kernel_st_offset,
             hls::stream<write_burst_pkt_t> &little_kernel_out_stream,
             hls::stream<write_burst_pkt_t> &big_kernel_out_stream);

extern "C" void
little_prop_loader(bus_word_t *src_prop,
                   uint32_t num_partitions_little,
                   hls::stream<ppb_request_pkt_t> &ppb_req_stream,
                   hls::stream<ppb_response_pkt_t> &ppb_resp_stream);

extern "C" void
big_prop_loader(bus_word_t *src_prop,
                uint32_t num_partitions_big,
                hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
                hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream);

extern "C" void
big_merger(hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream,
hls::stream<write_burst_pkt_t> &kernel_out_stream);

extern "C" void
little_merger(hls::stream<little_out_pkt_t> &little_kernel_1_out_stream,
hls::stream<little_out_pkt_t> &little_kernel_2_out_stream,
hls::stream<little_out_pkt_t> &little_kernel_3_out_stream,
hls::stream<little_out_pkt_t> &little_kernel_4_out_stream,
hls::stream<write_burst_pkt_t> &kernel_out_stream);

#endif // SHARED_KERNEL_PARAMS_H
