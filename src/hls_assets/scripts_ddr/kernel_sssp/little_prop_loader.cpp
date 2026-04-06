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

extern "C" void little_prop_loader(
    bus_word_t *src_prop,
    uint32_t num_partitions_little,
    hls::stream<ppb_request_pkt_t> &ppb_req_stream,
    hls::stream<ppb_response_pkt_t> &ppb_resp_stream){

#pragma HLS INTERFACE m_axi port = src_prop offset = slave bundle = gmem0
#pragma HLS INTERFACE s_axilite port = src_prop bundle = control
#pragma HLS INTERFACE s_axilite port = num_partitions_little bundle = control
#pragma HLS INTERFACE s_axilite port = return bundle = control
#pragma HLS DATAFLOW

hls::stream<little_ppb_resp_t> little_prop_loader_out;
#pragma HLS STREAM variable = little_prop_loader_out depth = 16


little_node_prop_loader(0, src_prop, num_partitions_little,ppb_req_stream, little_prop_loader_out);
little_response_packer(0, little_prop_loader_out, ppb_resp_stream,num_partitions_little);


}
