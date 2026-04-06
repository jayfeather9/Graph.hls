#include "shared_kernel_params.h"

void merge_big_kernels(hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream,
hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream,
hls::stream<write_burst_pkt_t> &kernel_out_stream) {

 write_burst_pkt_t tmp_prop_pkt[BIG_MERGER_LENGTH];
#pragma HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete

    bool process_flag[BIG_MERGER_LENGTH];
#pragma HLS ARRAY_PARTITION variable = process_flag dim = 0 complete

    for (int i = 0; i < BIG_MERGER_LENGTH; i++) {
#pragma HLS unroll
        process_flag[i] = 0;
    }

    bus_word_t merged_write_burst;

    write_burst_pkt_t one_write_burst;

    uint32_t outer_idx = 0;

    ap_fixed_pod_t tmp_prop_arrary[16];
#pragma HLS ARRAY_PARTITION variable = tmp_prop_arrary dim = 0 complete

    // WCC: max reduction, initialize to 0

merge_tmp_prop_big_krnls:
    while (true) {
#pragma HLS pipeline style = flp

        if (!process_flag[0])
                    process_flag[0] = big_kernel_1_out_stream.read_nb(tmp_prop_pkt[0]);
        if (!process_flag[1])
                    process_flag[1] = big_kernel_2_out_stream.read_nb(tmp_prop_pkt[1]);
        if (!process_flag[2])
                    process_flag[2] = big_kernel_3_out_stream.read_nb(tmp_prop_pkt[2]);
        if (!process_flag[3])
                    process_flag[3] = big_kernel_4_out_stream.read_nb(tmp_prop_pkt[3]);
    bool merge_flag = 
        process_flag[0] & 
        process_flag[1] & 
        process_flag[2] & 
        process_flag[3] & 
                        1;

if (merge_flag) {
            for (int i = 0; i < 16; i++) {
#pragma HLS UNROLL
                tmp_prop_arrary[i] = 0;
                
            }

            for (int i = 0; i < BIG_MERGER_LENGTH; i++) {
#pragma HLS UNROLL
                for (int j = 0; j < 16; j++) {
#pragma HLS UNROLL
                    ap_fixed_pod_t update =
                        tmp_prop_pkt[i].data.range(31 + (j << 5), (j << 5));
                        // =======  begin inline reduce logic (max) ====
                        tmp_prop_arrary[j] =
                            (update > tmp_prop_arrary[j]) ? update : tmp_prop_arrary[j];
                        // =======  end inline reduce logic ====

                }
            }

            for (int i = 0; i < 16; i++) {
#pragma HLS UNROLL
                merged_write_burst.range(31 + (i << 5), (i << 5)) =
                    tmp_prop_arrary[i];
            }

            one_write_burst.data = merged_write_burst;
            kernel_out_stream.write(one_write_burst);

            for (int i = 0; i < BIG_MERGER_LENGTH; i++) {
#pragma HLS unroll
                process_flag[i] = 0;
            }
        }
    }
}


extern "C" void
big_merger(
    hls::stream<write_burst_pkt_t> &big_kernel_1_out_stream,
    hls::stream<write_burst_pkt_t> &big_kernel_2_out_stream,
    hls::stream<write_burst_pkt_t> &big_kernel_3_out_stream,
    hls::stream<write_burst_pkt_t> &big_kernel_4_out_stream,
    hls::stream<write_burst_pkt_t> &kernel_out_stream) {
#pragma HLS interface ap_ctrl_none port = return
#pragma HLS DATAFLOW
    merge_big_kernels(
        big_kernel_1_out_stream,
        big_kernel_2_out_stream,
        big_kernel_3_out_stream,
        big_kernel_4_out_stream,
        kernel_out_stream);
}
