#include "shared_kernel_params.h"

void merge_little_kernels(hls::stream<little_out_pkt_t> &little_kernel_1_out_stream, hls::stream<little_out_pkt_t> &little_kernel_2_out_stream, hls::stream<little_out_pkt_t> &little_kernel_3_out_stream, hls::stream<little_out_pkt_t> &little_kernel_4_out_stream, hls::stream<little_out_pkt_t> &little_kernel_5_out_stream, hls::stream<little_out_pkt_t> &little_kernel_6_out_stream, hls::stream<little_out_pkt_t> &little_kernel_7_out_stream, hls::stream<little_out_pkt_t> &little_kernel_8_out_stream, hls::stream<little_out_pkt_t> &little_kernel_9_out_stream, hls::stream<little_out_pkt_t> &little_kernel_10_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    little_out_pkt_t tmp_prop_pkt[LITTLE_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete
    bool process_flag[LITTLE_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = process_flag dim = 0 complete
    init_process_flag: for (int32_t i = 0; (i < LITTLE_MERGER_LENGTH); ++i) {
        #pragma HLS UNROLL
        process_flag[i] = false;
    }
    bus_word_t one_write_burst = 0u;
    uint32_t inner_idx = 0u;
    ap_fixed_pod_t identity_pod = INFINITY_POD;
    merge_tmp_prop_little_krnls: while (true) {
        #pragma HLS pipeline style = flp
        if ((!process_flag[0u])) {
            process_flag[0u] = little_kernel_1_out_stream.read_nb(tmp_prop_pkt[0u]);
        }
        if ((!process_flag[1u])) {
            process_flag[1u] = little_kernel_2_out_stream.read_nb(tmp_prop_pkt[1u]);
        }
        if ((!process_flag[2u])) {
            process_flag[2u] = little_kernel_3_out_stream.read_nb(tmp_prop_pkt[2u]);
        }
        if ((!process_flag[3u])) {
            process_flag[3u] = little_kernel_4_out_stream.read_nb(tmp_prop_pkt[3u]);
        }
        if ((!process_flag[4u])) {
            process_flag[4u] = little_kernel_5_out_stream.read_nb(tmp_prop_pkt[4u]);
        }
        if ((!process_flag[5u])) {
            process_flag[5u] = little_kernel_6_out_stream.read_nb(tmp_prop_pkt[5u]);
        }
        if ((!process_flag[6u])) {
            process_flag[6u] = little_kernel_7_out_stream.read_nb(tmp_prop_pkt[6u]);
        }
        if ((!process_flag[7u])) {
            process_flag[7u] = little_kernel_8_out_stream.read_nb(tmp_prop_pkt[7u]);
        }
        if ((!process_flag[8u])) {
            process_flag[8u] = little_kernel_9_out_stream.read_nb(tmp_prop_pkt[8u]);
        }
        if ((!process_flag[9u])) {
            process_flag[9u] = little_kernel_10_out_stream.read_nb(tmp_prop_pkt[9u]);
        }
        bool merge_flag = ((((((((((process_flag[0u] & process_flag[1u]) & process_flag[2u]) & process_flag[3u]) & process_flag[4u]) & process_flag[5u]) & process_flag[6u]) & process_flag[7u]) & process_flag[8u]) & process_flag[9u]) & 1u);
        if (merge_flag) {
            ap_fixed_pod_t uram_vals[DISTANCES_PER_REDUCE_WORD];
            #pragma HLS ARRAY_PARTITION variable = uram_vals dim = 0 complete
            init_uram_vals: for (int32_t s = 0; (s < DISTANCES_PER_REDUCE_WORD); ++s) {
                #pragma HLS UNROLL
                uram_vals[s] = identity_pod;
            }
            merge_reduction: for (int32_t i = 0; (i < LITTLE_MERGER_LENGTH); ++i) {
                #pragma HLS UNROLL
                merge_slots: for (int32_t s = 0; (s < DISTANCES_PER_REDUCE_WORD); ++s) {
                    #pragma HLS UNROLL
                    uram_vals[s] = ((uram_vals[s] == identity_pod) ? static_cast<ap_fixed_pod_t>(tmp_prop_pkt[i].data.range(((s * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (s * DISTANCE_BITWIDTH))) : ((tmp_prop_pkt[i].data.range(((s * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (s * DISTANCE_BITWIDTH)) < uram_vals[s]) ? static_cast<ap_fixed_pod_t>(tmp_prop_pkt[i].data.range(((s * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (s * DISTANCE_BITWIDTH))) : static_cast<ap_fixed_pod_t>(uram_vals[s])));
                }
            }
            pack_reduce_word: for (int32_t s = 0; (s < DISTANCES_PER_REDUCE_WORD); ++s) {
                #pragma HLS UNROLL
                one_write_burst.range(((((inner_idx * DISTANCES_PER_REDUCE_WORD) + s) * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (((inner_idx * DISTANCES_PER_REDUCE_WORD) + s) * DISTANCE_BITWIDTH)) = uram_vals[s];
            }
            inner_idx = (inner_idx + 1u);
            if ((inner_idx == REDUCE_WORDS_PER_BUS)) {
                write_burst_pkt_t out_pkt;
                out_pkt.data = one_write_burst;
                out_pkt.last = false;
                kernel_out_stream.write(out_pkt);
                inner_idx = 0u;
                one_write_burst = 0u;
            }
            reset_process_flag: for (int32_t i = 0; (i < LITTLE_MERGER_LENGTH); ++i) {
                #pragma HLS UNROLL
                process_flag[i] = false;
            }
        }
    }
}

extern "C" void little_merger(hls::stream<little_out_pkt_t> &little_kernel_1_out_stream, hls::stream<little_out_pkt_t> &little_kernel_2_out_stream, hls::stream<little_out_pkt_t> &little_kernel_3_out_stream, hls::stream<little_out_pkt_t> &little_kernel_4_out_stream, hls::stream<little_out_pkt_t> &little_kernel_5_out_stream, hls::stream<little_out_pkt_t> &little_kernel_6_out_stream, hls::stream<little_out_pkt_t> &little_kernel_7_out_stream, hls::stream<little_out_pkt_t> &little_kernel_8_out_stream, hls::stream<little_out_pkt_t> &little_kernel_9_out_stream, hls::stream<little_out_pkt_t> &little_kernel_10_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    #pragma HLS interface ap_ctrl_none port = return
    #pragma HLS DATAFLOW
    merge_little_kernels(little_kernel_1_out_stream, little_kernel_2_out_stream, little_kernel_3_out_stream, little_kernel_4_out_stream, little_kernel_5_out_stream, little_kernel_6_out_stream, little_kernel_7_out_stream, little_kernel_8_out_stream, little_kernel_9_out_stream, little_kernel_10_out_stream, kernel_out_stream);
}