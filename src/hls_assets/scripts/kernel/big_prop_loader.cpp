#include "shared_kernel_params.h"

static void big_node_prop_loader(
    int i, const bus_word_t *node_distances_ddr, uint32_t num_partitions,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream
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

extern "C" void big_prop_loader(
    bus_word_t *src_prop,
    uint32_t num_partitions_big,
    hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
    hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream) {
#pragma HLS INTERFACE m_axi port = src_prop offset = slave bundle = gmem0
#pragma HLS INTERFACE s_axilite port = src_prop bundle = control
#pragma HLS INTERFACE s_axilite port = num_partitions_big bundle = control
#pragma HLS INTERFACE s_axilite port = return bundle = control
#pragma HLS DATAFLOW

    big_node_prop_loader(0, src_prop, num_partitions_big, cacheline_req_stream, cacheline_resp_stream);
}
