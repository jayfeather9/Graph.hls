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
#define L 3
#define SRC_BUFFER_SIZE 4096
#define LOG_SRC_BUFFER_SIZE 12
#define NODE_ID_BITWIDTH 32
#define DISTANCE_BITWIDTH 32
#define DISTANCE_INTEGER_PART 16
#define WEIGHT_BITWIDTH DISTANCE_BITWIDTH
#define WEIGHT_INTEGER_PART DISTANCE_INTEGER_PART
#define OUT_END_MARKER_BITWIDTH 4
#define DIST_PER_WORD 16
#define LOG_DIST_PER_WORD 4
#define AXI_BUS_WIDTH 512
#define SRC_BUFFER_WORDS (SRC_BUFFER_SIZE / DIST_PER_WORD)
#define LOG_SRC_BUFFER_WORDS (LOG_SRC_BUFFER_SIZE - LOG_DIST_PER_WORD)
#define BIG_MERGER_LENGTH 4
#define LITTLE_MERGER_LENGTH 10
#define REDUCE_MEM_WIDTH 64
#define REDUCE_WORDS_PER_BUS (AXI_BUS_WIDTH / REDUCE_MEM_WIDTH)
#define DISTANCES_PER_REDUCE_WORD 2
#define LOG_DISTANCES_PER_REDUCE_WORD ((LOG_DIST_PER_WORD > 3) ? (LOG_DIST_PER_WORD - 3) : 0)
#define DISTANCE_SIGNED 1

// Constants

using bus_word_t = ap_uint<AXI_BUS_WIDTH>;
using reduce_word_t = ap_uint<REDUCE_MEM_WIDTH>;
using node_id_t = ap_uint<NODE_ID_BITWIDTH>;
using edge_id_t = ap_uint<32>;
using ap_fixed_pod_t = ap_int<DISTANCE_BITWIDTH>;
using distance_t = ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART>;
using out_end_marker_t = ap_uint<OUT_END_MARKER_BITWIDTH>;
using node_dist_pkt_t = ap_axiu<256, 0, 0, 0>;
using write_burst_pkt_t = ap_axiu<512, 0, 0, 0>;
using little_out_pkt_t = ap_axiu<64, 0, 0, 0>;
using write_burst_w_dst_pkt_t = ap_axiu<512, 0, 0, 32>;
using cacheline_request_pkt_t = ap_axiu<32, 0, 0, 8>;
using cacheline_response_pkt_t = ap_axiu<512, 0, 0, 8>;
using ppb_request_pkt_t = ap_axiu<32, 0, 0, 0>;
using ppb_response_pkt_t = ap_axiu<512, 0, 0, 32>;
using cacheline_data_pkt_t = ap_axiu<512, 0, 0, 0>;

// Derived constants
const ap_fixed_pod_t INFINITY_POD = 2147483647u;
const ap_fixed_pod_t NEG_INFINITY_POD = 2147483648u;
const distance_t INFINITY_DIST_VAL = static_cast<distance_t>(INFINITY_POD);

struct __attribute__((packed)) in_write_burst_w_dst_pkt_t {
    bus_word_t data;
    uint32_t dest_addr;
    bool end_flag;
};

// Kernel prototypes
extern "C" void graphyflow_big(const bus_word_t* edge_props, int32_t num_nodes, int32_t num_edges, int32_t dst_num, int32_t memory_offset, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream);
extern "C" void graphyflow_little(const bus_word_t* edge_props, int32_t num_nodes, int32_t num_edges, int32_t dst_num, int32_t memory_offset, hls::stream<ppb_request_pkt_t> &ppb_req_stream, hls::stream<ppb_response_pkt_t> &ppb_resp_stream, hls::stream<little_out_pkt_t> &kernel_out_stream);
extern "C" void apply_kernel(bus_word_t* node_props, uint32_t little_kernel_length, uint32_t big_kernel_length, uint32_t little_kernel_st_offset, uint32_t big_kernel_st_offset, hls::stream<write_burst_pkt_t> &little_kernel_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_out_stream, hls::stream<write_burst_w_dst_pkt_t> &kernel_out_stream);
extern "C" void big_merger(uint32_t num_words, hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream);
extern "C" void little_merger(uint32_t num_words, hls::stream<little_out_pkt_t> &little_kernel_1_out_stream, hls::stream<little_out_pkt_t> &little_kernel_2_out_stream, hls::stream<little_out_pkt_t> &little_kernel_3_out_stream, hls::stream<little_out_pkt_t> &little_kernel_4_out_stream, hls::stream<little_out_pkt_t> &little_kernel_5_out_stream, hls::stream<little_out_pkt_t> &little_kernel_6_out_stream, hls::stream<little_out_pkt_t> &little_kernel_7_out_stream, hls::stream<little_out_pkt_t> &little_kernel_8_out_stream, hls::stream<little_out_pkt_t> &little_kernel_9_out_stream, hls::stream<little_out_pkt_t> &little_kernel_10_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream);
extern "C" void hbm_writer(bus_word_t* src_props_1, bus_word_t* src_props_2, bus_word_t* src_props_3, bus_word_t* src_props_4, bus_word_t* src_props_5, bus_word_t* src_props_6, bus_word_t* src_props_7, bus_word_t* src_props_8, bus_word_t* src_props_9, bus_word_t* src_props_10, bus_word_t* src_props_11, bus_word_t* src_props_12, bus_word_t* src_props_13, bus_word_t* src_props_14, bus_word_t* output, uint32_t num_partitions_little, uint32_t num_partitions_big, hls::stream<ppb_request_pkt_t> &ppb_req_stream_1, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_1, hls::stream<ppb_request_pkt_t> &ppb_req_stream_2, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_2, hls::stream<ppb_request_pkt_t> &ppb_req_stream_3, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_3, hls::stream<ppb_request_pkt_t> &ppb_req_stream_4, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_4, hls::stream<ppb_request_pkt_t> &ppb_req_stream_5, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_5, hls::stream<ppb_request_pkt_t> &ppb_req_stream_6, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_6, hls::stream<ppb_request_pkt_t> &ppb_req_stream_7, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_7, hls::stream<ppb_request_pkt_t> &ppb_req_stream_8, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_8, hls::stream<ppb_request_pkt_t> &ppb_req_stream_9, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_9, hls::stream<ppb_request_pkt_t> &ppb_req_stream_10, hls::stream<ppb_response_pkt_t> &ppb_resp_stream_10, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_1, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_1, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_2, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_2, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_3, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_3, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_4, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_4, hls::stream<write_burst_w_dst_pkt_t> &write_burst_stream);

#endif // __SHARED_KERNEL_PARAMS_H__
