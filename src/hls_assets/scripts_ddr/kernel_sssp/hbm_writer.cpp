#include "shared_kernel_params.h"

struct little_ppb_resp_t {
    bus_word_t data;
    uint32_t dest;
    bool last;
};

static void
little_response_packer(int i, hls::stream<little_ppb_resp_t> &prop_loader_out,
                       hls::stream<ppb_response_pkt_t> &ppb_response_stm,
                       uint32_t num_partitions) {

#pragma HLS function_instantiate variable = i

    uint32_t left_partitions = num_partitions;
LOOP_PACK_RESPONSES:
    while (true) {
#pragma HLS PIPELINE II = 1
        little_ppb_resp_t prop_data = prop_loader_out.read();
        ppb_response_pkt_t ppb_response;
        ppb_response.data = prop_data.data;
        ppb_response.dest = prop_data.dest;
        ppb_response.last = prop_data.last;
        ppb_response_stm.write(ppb_response);
        if (prop_data.last) {
            left_partitions--;
        }
        if (left_partitions == 0) {
            break;
        }
    }
}

static void little_node_prop_loader(
    int i, const bus_word_t *node_distances_ddr, uint32_t num_partitions,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream,
    hls::stream<little_ppb_resp_t> &ppb_resp_stream
    // hls::stream<cacheline_data_pkt_t> &cacheline_data_stream
) {
#pragma HLS function_instantiate variable = i

    uint32_t left_partitions = num_partitions;

littleKernelReadMemory:
    while (true) {
#pragma HLS PIPELINE
        ppb_request_pkt_t one_ppb_request_pkg;
        if (ppb_req_stream.read_nb(one_ppb_request_pkg)) {
            uint32_t request_round = one_ppb_request_pkg.data;
            bool end_flag = one_ppb_request_pkg.last;

            uint32_t base_addr = request_round << LOG_SRC_BUFFER_SIZE >> 4;

            if (end_flag) {
                little_ppb_resp_t one_ppb_response_pkg;
                one_ppb_response_pkg.last = end_flag;
                ppb_resp_stream.write(one_ppb_response_pkg);
                left_partitions--;
                if (left_partitions == 0) {
                    break;
                }
            } else {
                for (int i = 0; i < (SRC_BUFFER_SIZE >> 4); i++) {
                    uint32_t addr = base_addr + i;
                    little_ppb_resp_t one_ppb_response_pkg;
                    one_ppb_response_pkg.data = node_distances_ddr[addr];
                    one_ppb_response_pkg.dest = addr;
                    one_ppb_response_pkg.last = false;
                    ppb_resp_stream.write(one_ppb_response_pkg);
                }
            }
        }
    }
}

static void big_node_prop_loader(
    int i, const bus_word_t *node_distances_ddr, uint32_t num_partitions,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream
    // hls::stream<cacheline_data_pkt_t> &cacheline_data_stream
) {
#pragma HLS function_instantiate variable = i

    cacheline_request_pkt_t cache_req;
    cacheline_response_pkt_t cache_resp;

    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> last_cache_idx = -1;
    bus_word_t last_cacheline;

    uint32_t left_partitions = num_partitions;

LOOP_BIG_KRL_READ_MEMORY:
    while (true) {
#pragma HLS PIPELINE II = 1
        bool process_flag = cacheline_req_stream.read_nb(cache_req);

        ap_uint<26> idx = cache_req.data;
        ap_uint<8> target_pe = cache_req.dest;
        bool end_flag = cache_req.last;

        ap_uint<8> dst_pe;
        bus_word_t out_data;
        bool out_end_flag;

        if (process_flag) {
            if (end_flag) {
                out_data = 0;
                last_cache_idx = -1;
                left_partitions--;
            } else {
                if (idx == last_cache_idx) {
                    out_data = last_cacheline;
                } else {
                    out_data = node_distances_ddr[idx];
                    last_cache_idx = idx;
                    last_cacheline = out_data;
                }
            }

            out_end_flag = end_flag;
            dst_pe = target_pe;

            cache_resp.data = out_data;
            cache_resp.dest = dst_pe;
            cache_resp.last = out_end_flag;
            cacheline_resp_stream.write(cache_resp);
        }
        if (left_partitions == 0) {
            break;
        }
    }
}

void write_out(bus_word_t *output,
               hls::stream<write_burst_w_dst_pkt_t> &write_burst_stream) {
LOOP_WRITE_OUT:
    while (true) {
#pragma HLS PIPELINE II = 1

        write_burst_w_dst_pkt_t one_write_burst;

        if (write_burst_stream.read_nb(one_write_burst)) {
            uint32_t dest_addr = one_write_burst.dest;
            bus_word_t data = one_write_burst.data;
            bool end_flag = one_write_burst.last;

            if (end_flag) {
                break;
            }

            output[dest_addr] = data;
        }
    }
}
extern "C" void hbm_writer(
    bus_word_t *src_prop_1,
    bus_word_t *src_prop_2,
    bus_word_t *src_prop_3,
    bus_word_t *src_prop_4,
    bus_word_t *src_prop_5,
    bus_word_t *src_prop_6,
    bus_word_t *src_prop_7,
    bus_word_t *src_prop_8,
    bus_word_t *output,
    uint32_t num_partitions_little,
    uint32_t num_partitions_big,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream_1,
    hls::stream<ppb_response_pkt_t> &ppb_resp_stream_1,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream_2,
    hls::stream<ppb_response_pkt_t> &ppb_resp_stream_2,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream_3,
    hls::stream<ppb_response_pkt_t> &ppb_resp_stream_3,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream_4,
    hls::stream<ppb_response_pkt_t> &ppb_resp_stream_4,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_1,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_1,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_2,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_2,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_3,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_3,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream_4,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream_4,
hls::stream<write_burst_w_dst_pkt_t> &write_burst_stream) {
#pragma HLS INTERFACE m_axi port = src_prop_1 offset = slave bundle = gmem0
#pragma HLS INTERFACE m_axi port = src_prop_2 offset = slave bundle = gmem1
#pragma HLS INTERFACE m_axi port = src_prop_3 offset = slave bundle = gmem2
#pragma HLS INTERFACE m_axi port = src_prop_4 offset = slave bundle = gmem3
#pragma HLS INTERFACE m_axi port = src_prop_5 offset = slave bundle = gmem4
#pragma HLS INTERFACE m_axi port = src_prop_6 offset = slave bundle = gmem5
#pragma HLS INTERFACE m_axi port = src_prop_7 offset = slave bundle = gmem6
#pragma HLS INTERFACE m_axi port = src_prop_8 offset = slave bundle = gmem7
#pragma HLS INTERFACE m_axi port = output offset = slave bundle = gmem1
#pragma HLS INTERFACE s_axilite port = src_prop_1 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_2 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_3 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_4 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_5 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_6 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_7 bundle = control
#pragma HLS INTERFACE s_axilite port = src_prop_8 bundle = control
#pragma HLS INTERFACE s_axilite port = output bundle = control
 #pragma HLS INTERFACE s_axilite port = num_partitions_little bundle = control
#pragma HLS INTERFACE s_axilite port = num_partitions_big bundle = control
#pragma HLS INTERFACE s_axilite port = return bundle = control
#pragma HLS DATAFLOW
hls::stream<little_ppb_resp_t> little_prop_loader_out_1;
#pragma HLS STREAM variable = little_prop_loader_out_1 depth = 16
hls::stream<little_ppb_resp_t> little_prop_loader_out_2;
#pragma HLS STREAM variable = little_prop_loader_out_2 depth = 16
hls::stream<little_ppb_resp_t> little_prop_loader_out_3;
#pragma HLS STREAM variable = little_prop_loader_out_3 depth = 16
hls::stream<little_ppb_resp_t> little_prop_loader_out_4;
#pragma HLS STREAM variable = little_prop_loader_out_4 depth = 16
    little_node_prop_loader(0, src_prop_1, num_partitions_little,ppb_req_stream_1, little_prop_loader_out_1);
    little_response_packer(0, little_prop_loader_out_1, ppb_resp_stream_1,num_partitions_little);
    little_node_prop_loader(1, src_prop_2, num_partitions_little,ppb_req_stream_2, little_prop_loader_out_2);
    little_response_packer(1, little_prop_loader_out_2, ppb_resp_stream_2,num_partitions_little);
    little_node_prop_loader(2, src_prop_3, num_partitions_little,ppb_req_stream_3, little_prop_loader_out_3);
    little_response_packer(2, little_prop_loader_out_3, ppb_resp_stream_3,num_partitions_little);
    little_node_prop_loader(3, src_prop_4, num_partitions_little,ppb_req_stream_4, little_prop_loader_out_4);
    little_response_packer(3, little_prop_loader_out_4, ppb_resp_stream_4,num_partitions_little);
    big_node_prop_loader(0, src_prop_5, num_partitions_big,cacheline_req_stream_1, cacheline_resp_stream_1);
    big_node_prop_loader(1, src_prop_6, num_partitions_big,cacheline_req_stream_2, cacheline_resp_stream_2);
    big_node_prop_loader(2, src_prop_7, num_partitions_big,cacheline_req_stream_3, cacheline_resp_stream_3);
    big_node_prop_loader(3, src_prop_8, num_partitions_big,cacheline_req_stream_4, cacheline_resp_stream_4);
    write_out(output, write_burst_stream);
}
