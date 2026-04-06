#include "shared_kernel_params.h"

void attach_dest_to_stream(hls::stream<write_burst_pkt_t> &in_stream, hls::stream<in_write_burst_w_dst_pkt_t> &out_stream, uint32_t st_offset, uint32_t length) {
    
#pragma HLS INLINE off
  uint32_t idx = st_offset;
ATTACH_DEST: for (uint32_t i = 0; i < length; ++i) {
#pragma HLS PIPELINE II = 1
    write_burst_pkt_t in_pkt = in_stream.read();
    in_write_burst_w_dst_pkt_t out_pkt;
    out_pkt.data = in_pkt.data;
    out_pkt.dest_addr = idx;
    out_pkt.end_flag = false;
    out_stream.write(out_pkt);
    idx++;
}
  in_write_burst_w_dst_pkt_t end_pkt;
  end_pkt.end_flag = true;
  out_stream.write(end_pkt);

}

void merge_two_endflag_streams(hls::stream<in_write_burst_w_dst_pkt_t> &a_stream, hls::stream<in_write_burst_w_dst_pkt_t> &b_stream, hls::stream<in_write_burst_w_dst_pkt_t> &out_stream) {
    
#pragma HLS INLINE off
  bool a_done = false;
  bool b_done = false;
  bool prefer_a = true;
MERGE_TWO: while (true) {
#pragma HLS PIPELINE II = 1
    if (a_done && b_done) {
      break;
    }
    bool took_a = false;
    bool took_b = false;
    if (prefer_a) {
      if (!a_done && (!a_stream.empty())) {
        in_write_burst_w_dst_pkt_t pkt = a_stream.read();
        if (pkt.end_flag) {
          a_done = true;
        } else {
          out_stream.write(pkt);
        }
        took_a = true;
      } else if (!b_done && (!b_stream.empty())) {
        in_write_burst_w_dst_pkt_t pkt = b_stream.read();
        if (pkt.end_flag) {
          b_done = true;
        } else {
          out_stream.write(pkt);
        }
        took_b = true;
      }
    } else {
      if (!b_done && (!b_stream.empty())) {
        in_write_burst_w_dst_pkt_t pkt = b_stream.read();
        if (pkt.end_flag) {
          b_done = true;
        } else {
          out_stream.write(pkt);
        }
        took_b = true;
      } else if (!a_done && (!a_stream.empty())) {
        in_write_burst_w_dst_pkt_t pkt = a_stream.read();
        if (pkt.end_flag) {
          a_done = true;
        } else {
          out_stream.write(pkt);
        }
        took_a = true;
      }
    }
    if (took_a) {
      prefer_a = false;
    } else if (took_b) {
      prefer_a = true;
    }
  }
  in_write_burst_w_dst_pkt_t end_pkt;
  end_pkt.end_flag = true;
  out_stream.write(end_pkt);

}

void forward_endflag_stream_to_out(hls::stream<in_write_burst_w_dst_pkt_t> &in_stream, hls::stream<in_write_burst_w_dst_pkt_t> &out_stream) {
    
#pragma HLS INLINE off
FORWARD_END: while (true) {
#pragma HLS PIPELINE II = 1
  in_write_burst_w_dst_pkt_t pkt = in_stream.read();
  out_stream.write(pkt);
  if (pkt.end_flag) {
    break;
  }
}

}

void merge_big_little_writes(hls::stream<write_burst_pkt_t> &little_kernel_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_out_stream, hls::stream<in_write_burst_w_dst_pkt_t> &kernel_out_stream, uint32_t little_kernel_length, uint32_t big_kernel_length, uint32_t little_kernel_st_offset, uint32_t big_kernel_st_offset) {
    
#pragma HLS INLINE off
#pragma HLS DATAFLOW
  hls::stream<in_write_burst_w_dst_pkt_t> little_with_dst;
  hls::stream<in_write_burst_w_dst_pkt_t> big_with_dst;
#pragma HLS STREAM variable = little_with_dst depth = 16
#pragma HLS STREAM variable = big_with_dst depth = 16

  attach_dest_to_stream(little_kernel_out_stream, little_with_dst, little_kernel_st_offset, little_kernel_length);
  attach_dest_to_stream(big_kernel_out_stream, big_with_dst, big_kernel_st_offset, big_kernel_length);
  merge_two_endflag_streams(little_with_dst, big_with_dst, kernel_out_stream);

}

void apply_func(bus_word_t *node_props, hls::stream<in_write_burst_w_dst_pkt_t> &write_burst_stream, hls::stream<write_burst_w_dst_pkt_t> &kernel_out_stream) {
    LOOP_WHILE_44: while (true) {
        in_write_burst_w_dst_pkt_t in_pkt = write_burst_stream.read();
        if (in_pkt.end_flag) {
            write_burst_w_dst_pkt_t end_pkt;
            end_pkt.last = true;
            kernel_out_stream.write(end_pkt);
            break;
        }
        else {
            uint32_t dest_addr = in_pkt.dest_addr;
            bus_word_t ori_props = node_props[dest_addr];
            bus_word_t new_props;
            write_burst_w_dst_pkt_t out_pkt;
            out_pkt.dest = dest_addr;
            out_pkt.last = false;
            LOOP_FOR_43: for (int32_t i = 0; (i < DIST_PER_WORD); ++i) {
                #pragma HLS UNROLL
                ap_fixed_pod_t update = in_pkt.data.range(((i * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (i * DISTANCE_BITWIDTH));
                ap_fixed_pod_t old = ori_props.range(((i * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (i * DISTANCE_BITWIDTH));
                distance_t update_val;
                distance_t old_val;
                ap_fixed_pod_t new_prop;
                distance_t BinOp_128_res;
                update_val.range((DISTANCE_BITWIDTH - 1), 0) = update;
                old_val.range((DISTANCE_BITWIDTH - 1), 0) = old;
                BinOp_128_res = ((old_val < update_val) ? old_val : update_val);
                new_prop = BinOp_128_res.range((DISTANCE_BITWIDTH - 1), 0);
                new_props.range(((i * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1)), (i * DISTANCE_BITWIDTH)) = new_prop;
            }
            out_pkt.data = new_props;
            kernel_out_stream.write(out_pkt);
        }
    }
}

extern "C" void apply_kernel(bus_word_t *node_props, uint32_t little_kernel_length, uint32_t big_kernel_length, uint32_t little_kernel_st_offset, uint32_t big_kernel_st_offset, hls::stream<write_burst_pkt_t> &little_kernel_out_stream, hls::stream<write_burst_pkt_t> &big_kernel_out_stream, hls::stream<write_burst_w_dst_pkt_t> &kernel_out_stream) {
    #pragma HLS INTERFACE m_axi port = node_props offset = slave bundle = gmem0
    #pragma HLS INTERFACE s_axilite port = node_props bundle = control
    #pragma HLS INTERFACE s_axilite port = little_kernel_length bundle = control
    #pragma HLS INTERFACE s_axilite port = big_kernel_length bundle = control
    #pragma HLS INTERFACE s_axilite port = little_kernel_st_offset bundle = control
    #pragma HLS INTERFACE s_axilite port = big_kernel_st_offset bundle = control
    #pragma HLS INTERFACE s_axilite port = return bundle = control
    #pragma HLS DATAFLOW
    hls::stream<in_write_burst_w_dst_pkt_t> write_burst_stream;
    #pragma HLS STREAM variable = write_burst_stream depth = 16
    merge_big_little_writes(little_kernel_out_stream, big_kernel_out_stream, write_burst_stream, little_kernel_length, big_kernel_length, little_kernel_st_offset, big_kernel_st_offset);
    apply_func(node_props, write_burst_stream, kernel_out_stream);
}