#include "shared_kernel_params.h"

void merge_little_kernels(
    hls::stream<little_out_pkt_t> &little_kernel_1_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_2_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_3_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_4_out_stream,
hls::stream<write_burst_pkt_t> &kernel_out_stream){

    little_out_pkt_t tmp_prop_pkt[LITTLE_MERGER_LENGTH];
#pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete

    bool process_flag[LITTLE_MERGER_LENGTH];
#pragma HLS ARRAY_PARTITION variable = process_flag dim = 0 complete

    for (int i = 0; i < LITTLE_MERGER_LENGTH; i++) {
#pragma HLS unroll
        process_flag[i] = 0;
    }

    reduce_word_t merged_write_burst;

    bus_word_t one_write_burst;

    uint32_t inner_idx = 0;

    // WCC: max reduction, initialize to 0

merge_tmp_prop_big_krnls:
    while (true) {
#pragma HLS pipeline style = flp

        if (!process_flag[0])
            process_flag[0] =
                little_kernel_1_out_stream.read_nb(tmp_prop_pkt[0]);
        if (!process_flag[1])
            process_flag[1] =
                little_kernel_2_out_stream.read_nb(tmp_prop_pkt[1]);
        if (!process_flag[2])
            process_flag[2] =
                little_kernel_3_out_stream.read_nb(tmp_prop_pkt[2]);
        if (!process_flag[3])
            process_flag[3] =
                little_kernel_4_out_stream.read_nb(tmp_prop_pkt[3]);
        bool merge_flag = 
        process_flag[0] & 
        process_flag[1] & 
        process_flag[2] & 
        process_flag[3] & 
        1;

if (merge_flag) {
            ap_fixed_pod_t uram_high = 0;
            ap_fixed_pod_t uram_low = 0;

            for (int i = 0; i < LITTLE_MERGER_LENGTH; i++) {
#pragma HLS UNROLL
                ap_fixed_pod_t update_low = tmp_prop_pkt[i].data.range(31, 0);
                ap_fixed_pod_t update_high = tmp_prop_pkt[i].data.range(63, 32);

                    // =======  begin inline reduce logic (max) ====
                    uram_low = (update_low > uram_low) ? update_low : uram_low;
                    uram_high = (update_high > uram_high) ? update_high : uram_high;
                    // =======  end inline reduce logic ====

            }

            merged_write_burst.range(31, 0) = uram_low;
            merged_write_burst.range(63, 32) = uram_high;

            one_write_burst.range(63 + (inner_idx << 6), (inner_idx << 6)) =
                merged_write_burst;
            inner_idx++;

            if (inner_idx == 8) {
                write_burst_pkt_t out_pkt;
                out_pkt.data = one_write_burst;
                out_pkt.last = 0;
                kernel_out_stream.write(out_pkt);
                inner_idx = 0;
                one_write_burst = 0;
            }

            for (int i = 0; i < LITTLE_MERGER_LENGTH; i++) {
#pragma HLS unroll
                process_flag[i] = 0;
            }
        }
    }
}


extern "C" void
little_merger(
    hls::stream<little_out_pkt_t> &little_kernel_1_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_2_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_3_out_stream,
    hls::stream<little_out_pkt_t> &little_kernel_4_out_stream,
    hls::stream<write_burst_pkt_t> &kernel_out_stream) {
#pragma HLS interface ap_ctrl_none port = return
#pragma HLS DATAFLOW
    merge_little_kernels(
        little_kernel_1_out_stream,
        little_kernel_2_out_stream,
        little_kernel_3_out_stream,
        little_kernel_4_out_stream,
        kernel_out_stream);
}
