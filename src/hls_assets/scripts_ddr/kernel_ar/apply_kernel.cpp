#include "shared_kernel_params.h"


void merge_big_little_writes(
    hls::stream<write_burst_pkt_t> &little_kernel_out_stream,
    hls::stream<write_burst_pkt_t> &big_kernel_out_stream,
    hls::stream<in_write_burst_w_dst_pkt_t> &kernel_out_stream,
    uint32_t little_kernel_length, uint32_t big_kernel_length,
    uint32_t little_kernel_st_offset, uint32_t big_kernel_st_offset) {
    write_burst_pkt_t big_tmp_prop_pkt;
    write_burst_pkt_t little_tmp_prop_pkt;

    uint32_t little_idx = little_kernel_st_offset;
    uint32_t big_idx = big_kernel_st_offset;
    uint32_t total_length = little_kernel_length + big_kernel_length;

LOOP_MERGE_WRITES:
    while (true) {
        if (total_length == 0) {
            in_write_burst_w_dst_pkt_t end_pkt;
            end_pkt.end_flag = true;
            kernel_out_stream.write(end_pkt);
            break;
        }

        if (little_kernel_out_stream.read_nb(little_tmp_prop_pkt)) {
            in_write_burst_w_dst_pkt_t little_write_burst;
            little_write_burst.data = little_tmp_prop_pkt.data;
            little_write_burst.dest_addr = little_idx;
            little_write_burst.end_flag = false;
            kernel_out_stream.write(little_write_burst);
            little_idx++;
            total_length--;
        } else if (big_kernel_out_stream.read_nb(big_tmp_prop_pkt)) {
            in_write_burst_w_dst_pkt_t big_write_burst;
            big_write_burst.data = big_tmp_prop_pkt.data;
            big_write_burst.dest_addr = big_idx;
            big_write_burst.end_flag = false;
            kernel_out_stream.write(big_write_burst);
            big_idx++;
            total_length--;
        }
    }
}

struct write_burst_w_dst_t {
    bus_word_t data;
    uint32_t dest;
    bool last;
};

void write_out(bus_word_t *output,
               hls::stream<write_burst_w_dst_t> &write_burst_stream) {
LOOP_WRITE_OUT:
    while (true) {
#pragma HLS PIPELINE II = 1

        write_burst_w_dst_t one_write_burst;

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


static void
apply_func(bus_word_t* node_props,
 uint32_t arg_reg,
 hls::stream<in_write_burst_w_dst_pkt_t> &write_burst_stream,
 hls::stream<write_burst_w_dst_t> &kernel_out_stream) {
    const ap_int<32> kDampFixPoint = 108; // (0.85 * 128)
    const ap_uint<32> kScaleDegree = 16;
    const ap_uint<32> kScaleDamping = 7;
    const ap_int<32> kBase =
        (ap_int<32>)(((ap_int<64>)1 << (kScaleDegree + kScaleDamping)) * 15 / 100);
    LOOP_WHILE_44:
    while (true) {
        in_write_burst_w_dst_pkt_t in_pkt = write_burst_stream.read();
        if (in_pkt.end_flag) {
            write_burst_w_dst_t end_pkt;
            end_pkt.last = true;
            kernel_out_stream.write(end_pkt);
            break;
        }
        uint32_t dest_addr = in_pkt.dest_addr;
        // For PR, node_props stores outdegree (uint32 per lane).
        bus_word_t outdeg_word = node_props[dest_addr];
        bus_word_t new_props;
        write_burst_w_dst_t out_pkt;
        out_pkt.dest = dest_addr;
        out_pkt.last = false;
        LOOP_FOR_43:
        for (int32_t i = 0; i < 16; i++) {
#pragma HLS UNROLL
            ap_int<32> sum_in = (ap_int<32>)in_pkt.data.range(31 + (i << 5), (i << 5));
            ap_uint<32> outDeg = (ap_uint<32>)outdeg_word.range(31 + (i << 5), (i << 5));

            ap_uint<32> denom = outDeg + (ap_uint<32>)arg_reg;
            ap_int<32> new_score =
                (ap_int<32>)((ap_int<64>)kDampFixPoint * (ap_int<64>)sum_in) +
                kBase;

            ap_int<32> new_contrib = 0;
            if (denom != 0) {
                ap_uint<32> tmp = ((ap_uint<32>)1 << kScaleDegree) / denom;
                new_contrib =
                    (ap_int<32>)((ap_int<64>)new_score * (ap_int<64>)tmp);
            }

            new_props.range(31 + (i << 5), (i << 5)) = (ap_uint<32>)new_contrib;
        }
        out_pkt.data = new_props;
        kernel_out_stream.write(out_pkt);
    }
}



extern "C" void
apply_kernel(bus_word_t *node_props,
             bus_word_t *output,
             uint32_t little_kernel_length,
             uint32_t big_kernel_length,
             uint32_t little_kernel_st_offset,
             uint32_t big_kernel_st_offset,
             uint32_t arg_reg,
             hls::stream<write_burst_pkt_t> &little_kernel_out_stream,
             hls::stream<write_burst_pkt_t> &big_kernel_out_stream) {
#pragma HLS INTERFACE m_axi port = node_props offset = slave bundle = gmem0
#pragma HLS INTERFACE m_axi port = output offset = slave bundle = gmem1
#pragma HLS INTERFACE s_axilite port = node_props bundle = control
#pragma HLS INTERFACE s_axilite port = output bundle = control
#pragma HLS INTERFACE s_axilite port = little_kernel_length bundle = control
#pragma HLS INTERFACE s_axilite port = big_kernel_length bundle = control
#pragma HLS INTERFACE s_axilite port = little_kernel_st_offset bundle = control
#pragma HLS INTERFACE s_axilite port = big_kernel_st_offset bundle = control
#pragma HLS INTERFACE s_axilite port = arg_reg bundle = control
#pragma HLS INTERFACE s_axilite port = return bundle = control
#pragma HLS DATAFLOW

    hls::stream<in_write_burst_w_dst_pkt_t> write_burst_stream;
#pragma HLS STREAM variable = write_burst_stream depth = 16
    hls::stream<write_burst_w_dst_t> kernel_out_stream;
#pragma HLS STREAM variable = kernel_out_stream depth = 16

    merge_big_little_writes(little_kernel_out_stream, big_kernel_out_stream, write_burst_stream, little_kernel_length, big_kernel_length, little_kernel_st_offset, big_kernel_st_offset);
    apply_func(node_props, arg_reg, write_burst_stream, kernel_out_stream);
    write_out(output, kernel_out_stream);
}
