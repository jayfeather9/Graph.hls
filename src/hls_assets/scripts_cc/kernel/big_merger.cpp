#include "shared_kernel_params.h"

void merge_big_kernels(uint32_t num_words, hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    write_burst_pkt_t tmp_prop_pkt[BIG_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete
    ap_fixed_pod_t tmp_prop_arrary[DIST_PER_WORD];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_arrary dim = 0 complete
    ap_fixed_pod_t identity_pod = INFINITY_POD;
    merge_words: for (uint32_t word_idx = 0; word_idx < num_words; ++word_idx) {
        #pragma HLS PIPELINE II = 1

        tmp_prop_pkt[0u] = big_kernel_1_out_stream.read();
        tmp_prop_pkt[1u] = big_kernel_2_out_stream.read();
        tmp_prop_pkt[2u] = big_kernel_3_out_stream.read();
        tmp_prop_pkt[3u] = big_kernel_4_out_stream.read();

        init_tmp_prop: for (int32_t i = 0; i < DIST_PER_WORD; ++i) {
            #pragma HLS UNROLL
            tmp_prop_arrary[i] = identity_pod;
        }
        merge_outer: for (int32_t i = 0; i < BIG_MERGER_LENGTH; ++i) {
            #pragma HLS UNROLL
            merge_inner: for (int32_t j = 0; j < DIST_PER_WORD; ++j) {
                #pragma HLS UNROLL
                const ap_fixed_pod_t slot_val = static_cast<ap_fixed_pod_t>(
                    tmp_prop_pkt[i].data.range((j * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1),
                                               (j * DISTANCE_BITWIDTH)));
                tmp_prop_arrary[j] = (tmp_prop_arrary[j] == identity_pod)
                                         ? slot_val
                                         : ((slot_val < tmp_prop_arrary[j])
                                                ? slot_val
                                                : tmp_prop_arrary[j]);
            }
        }
        bus_word_t merged_write_burst;
        pack_output: for (int32_t i = 0; i < DIST_PER_WORD; ++i) {
            #pragma HLS UNROLL
            merged_write_burst.range((i * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1),
                                     (i * DISTANCE_BITWIDTH)) = tmp_prop_arrary[i];
        }
        write_burst_pkt_t one_write_burst;
        one_write_burst.data = merged_write_burst;
        one_write_burst.last = false;
        kernel_out_stream.write(one_write_burst);
    }
}

extern "C" void big_merger(uint32_t num_words, hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    #pragma HLS INTERFACE s_axilite port = num_words bundle = control
    #pragma HLS INTERFACE s_axilite port = return bundle = control
    #pragma HLS DATAFLOW
    merge_big_kernels(num_words, big_kernel_1_out_stream, big_kernel_2_out_stream, big_kernel_3_out_stream, big_kernel_4_out_stream, kernel_out_stream);
}
