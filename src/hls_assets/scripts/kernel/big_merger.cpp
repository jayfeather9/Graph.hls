#include "shared_kernel_params.h"

void merge_big_kernels(hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    write_burst_pkt_t tmp_prop_pkt[BIG_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete
    bool process_flag[BIG_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = process_flag dim = 0 complete
    init_process_flag: for (int32_t i = 0; (i < BIG_MERGER_LENGTH); ++i) {
        #pragma HLS UNROLL
        process_flag[i] = false;
    }
    bus_word_t merged_write_burst;
    write_burst_pkt_t one_write_burst;
    ap_fixed_pod_t tmp_prop_arrary[DIST_PER_WORD];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_arrary dim = 0 complete
    ap_fixed_pod_t identity_pod = INFINITY_POD;
    merge_tmp_prop_big_krnls: while (true) {
        #pragma HLS pipeline style = flp
        if ((!process_flag[0u])) {
            process_flag[0u] = big_kernel_1_out_stream.read_nb(tmp_prop_pkt[0u]);
        }
        if ((!process_flag[1u])) {
            process_flag[1u] = big_kernel_2_out_stream.read_nb(tmp_prop_pkt[1u]);
        }
        if ((!process_flag[2u])) {
            process_flag[2u] = big_kernel_3_out_stream.read_nb(tmp_prop_pkt[2u]);
        }
        if ((!process_flag[3u])) {
            process_flag[3u] = big_kernel_4_out_stream.read_nb(tmp_prop_pkt[3u]);
        }
        bool merge_flag = ((((process_flag[0u] & process_flag[1u]) & process_flag[2u]) & process_flag[3u]) & 1u);
        if (merge_flag) {
            init_tmp_prop: for (int32_t i = 0; (i < DIST_PER_WORD); ++i) {
                #pragma HLS UNROLL
                tmp_prop_arrary[i] = identity_pod;
            }
            merge_outer: for (int32_t i = 0; (i < BIG_MERGER_LENGTH); ++i) {
                #pragma HLS UNROLL
                merge_inner: for (int32_t j = 0; (j < DIST_PER_WORD); ++j) {
                    #pragma HLS UNROLL
                    tmp_prop_arrary[j] = ((tmp_prop_arrary[j] == identity_pod) ? static_cast<ap_fixed_pod_t>(tmp_prop_pkt[i].data.range(((j * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (j * DISTANCE_BITWIDTH))) : ((tmp_prop_pkt[i].data.range(((j * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (j * DISTANCE_BITWIDTH)) < tmp_prop_arrary[j]) ? static_cast<ap_fixed_pod_t>(tmp_prop_pkt[i].data.range(((j * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (j * DISTANCE_BITWIDTH))) : static_cast<ap_fixed_pod_t>(tmp_prop_arrary[j])));
                }
            }
            pack_output: for (int32_t i = 0; (i < DIST_PER_WORD); ++i) {
                #pragma HLS UNROLL
                merged_write_burst.range(((i * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (i * DISTANCE_BITWIDTH)) = tmp_prop_arrary[i];
            }
            one_write_burst.data = merged_write_burst;
            kernel_out_stream.write(one_write_burst);
            reset_process_flag: for (int32_t i = 0; (i < BIG_MERGER_LENGTH); ++i) {
                #pragma HLS UNROLL
                process_flag[i] = false;
            }
        }
    }
}

extern "C" void big_merger(hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    #pragma HLS interface ap_ctrl_none port = return
    #pragma HLS DATAFLOW
    merge_big_kernels(big_kernel_1_out_stream, big_kernel_2_out_stream, big_kernel_3_out_stream, big_kernel_4_out_stream, kernel_out_stream);
}