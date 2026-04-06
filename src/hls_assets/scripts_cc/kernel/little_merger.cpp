#include "shared_kernel_params.h"

void merge_little_kernels(uint32_t num_words, hls::stream<little_out_pkt_t> &little_kernel_1_out_stream, hls::stream<little_out_pkt_t> &little_kernel_2_out_stream, hls::stream<little_out_pkt_t> &little_kernel_3_out_stream, hls::stream<little_out_pkt_t> &little_kernel_4_out_stream, hls::stream<little_out_pkt_t> &little_kernel_5_out_stream, hls::stream<little_out_pkt_t> &little_kernel_6_out_stream, hls::stream<little_out_pkt_t> &little_kernel_7_out_stream, hls::stream<little_out_pkt_t> &little_kernel_8_out_stream, hls::stream<little_out_pkt_t> &little_kernel_9_out_stream, hls::stream<little_out_pkt_t> &little_kernel_10_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    little_out_pkt_t tmp_prop_pkt[LITTLE_MERGER_LENGTH];
    #pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete
    bus_word_t one_write_burst = 0u;
    uint32_t inner_idx = 0u;
    ap_fixed_pod_t identity_pod = INFINITY_POD;

    merge_reduce_words: for (uint32_t word_idx = 0; word_idx < (num_words * REDUCE_WORDS_PER_BUS); ++word_idx) {
        #pragma HLS PIPELINE II = 1

        tmp_prop_pkt[0u] = little_kernel_1_out_stream.read();
        tmp_prop_pkt[1u] = little_kernel_2_out_stream.read();
        tmp_prop_pkt[2u] = little_kernel_3_out_stream.read();
        tmp_prop_pkt[3u] = little_kernel_4_out_stream.read();
        tmp_prop_pkt[4u] = little_kernel_5_out_stream.read();
        tmp_prop_pkt[5u] = little_kernel_6_out_stream.read();
        tmp_prop_pkt[6u] = little_kernel_7_out_stream.read();
        tmp_prop_pkt[7u] = little_kernel_8_out_stream.read();
        tmp_prop_pkt[8u] = little_kernel_9_out_stream.read();
        tmp_prop_pkt[9u] = little_kernel_10_out_stream.read();

        ap_fixed_pod_t uram_vals[DISTANCES_PER_REDUCE_WORD];
        #pragma HLS ARRAY_PARTITION variable = uram_vals dim = 0 complete
        init_uram_vals: for (int32_t s = 0; s < DISTANCES_PER_REDUCE_WORD; ++s) {
            #pragma HLS UNROLL
            uram_vals[s] = identity_pod;
        }

        merge_reduction: for (int32_t i = 0; i < LITTLE_MERGER_LENGTH; ++i) {
            #pragma HLS UNROLL
            merge_slots: for (int32_t s = 0; s < DISTANCES_PER_REDUCE_WORD; ++s) {
                #pragma HLS UNROLL
                const ap_fixed_pod_t slot_val = static_cast<ap_fixed_pod_t>(
                    tmp_prop_pkt[i].data.range((s * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1),
                                               (s * DISTANCE_BITWIDTH)));
                uram_vals[s] = (uram_vals[s] == identity_pod)
                                   ? slot_val
                                   : ((slot_val < uram_vals[s]) ? slot_val
                                                                : uram_vals[s]);
            }
        }

        pack_reduce_word: for (int32_t s = 0; s < DISTANCES_PER_REDUCE_WORD; ++s) {
            #pragma HLS UNROLL
            one_write_burst.range((((inner_idx * DISTANCES_PER_REDUCE_WORD) + s) * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1),
                                  (((inner_idx * DISTANCES_PER_REDUCE_WORD) + s) * DISTANCE_BITWIDTH)) = uram_vals[s];
        }
        inner_idx = inner_idx + 1u;
        if (inner_idx == REDUCE_WORDS_PER_BUS) {
            write_burst_pkt_t out_pkt;
            out_pkt.data = one_write_burst;
            out_pkt.last = false;
            kernel_out_stream.write(out_pkt);
            inner_idx = 0u;
            one_write_burst = 0u;
        }
    }
}

extern "C" void little_merger(uint32_t num_words, hls::stream<little_out_pkt_t> &little_kernel_1_out_stream, hls::stream<little_out_pkt_t> &little_kernel_2_out_stream, hls::stream<little_out_pkt_t> &little_kernel_3_out_stream, hls::stream<little_out_pkt_t> &little_kernel_4_out_stream, hls::stream<little_out_pkt_t> &little_kernel_5_out_stream, hls::stream<little_out_pkt_t> &little_kernel_6_out_stream, hls::stream<little_out_pkt_t> &little_kernel_7_out_stream, hls::stream<little_out_pkt_t> &little_kernel_8_out_stream, hls::stream<little_out_pkt_t> &little_kernel_9_out_stream, hls::stream<little_out_pkt_t> &little_kernel_10_out_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    #pragma HLS INTERFACE s_axilite port = num_words bundle = control
    #pragma HLS INTERFACE s_axilite port = return bundle = control
    #pragma HLS DATAFLOW
    merge_little_kernels(num_words, little_kernel_1_out_stream, little_kernel_2_out_stream, little_kernel_3_out_stream, little_kernel_4_out_stream, little_kernel_5_out_stream, little_kernel_6_out_stream, little_kernel_7_out_stream, little_kernel_8_out_stream, little_kernel_9_out_stream, little_kernel_10_out_stream, kernel_out_stream);
}
