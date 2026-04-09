#include "graphyflow_big.h"

void stream2axistream(hls::stream<cacheline_req_t> &stream, hls::stream<cacheline_request_pkt_t> &axi_stream) {
    stream2axistream: while (true) {
        cacheline_req_t tmp_t1;
        tmp_t1 = stream.read();
        cacheline_request_pkt_t tmp_t2;
        tmp_t2.data = tmp_t1.idx;
        tmp_t2.dest = tmp_t1.dst;
        tmp_t2.last = tmp_t1.end_flag;
        axi_stream.write(tmp_t2);
        if (tmp_t1.end_flag) {
            break;
        }
    }
}

void axistream2stream(hls::stream<cacheline_response_pkt_t> &axi_stream, hls::stream<cacheline_resp_t> &stream) {
    axistream2stream: while (true) {
        cacheline_response_pkt_t tmp_t1;
        tmp_t1 = axi_stream.read();
        cacheline_resp_t tmp_t2;
        tmp_t2.data = tmp_t1.data;
        tmp_t2.dst = tmp_t1.dest;
        tmp_t2.end_flag = tmp_t1.last;
        stream.write(tmp_t2);
        if (tmp_t2.end_flag) {
            break;
        }
    }
}

ap_uint<4> count_end_ones(ap_uint<PE_NUM> valid_mask) {
    #pragma HLS INLINE
    ap_uint<4> count = 0u;
    if ((valid_mask == 0u)) {
        count = 0u;
    }
    else {
        if ((valid_mask == 1u)) {
            count = 1u;
        }
        else {
            if ((valid_mask == 3u)) {
                count = 2u;
            }
            else {
                if ((valid_mask == 7u)) {
                    count = 3u;
                }
                else {
                    if ((valid_mask == 15u)) {
                        count = 4u;
                    }
                    else {
                        if ((valid_mask == 31u)) {
                            count = 5u;
                        }
                        else {
                            if ((valid_mask == 63u)) {
                                count = 6u;
                            }
                            else {
                                if ((valid_mask == 127u)) {
                                    count = 7u;
                                }
                                else {
                                    if ((valid_mask == 255u)) {
                                        count = 8u;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return count;
}

void dist_req_packer(hls::stream<node_id_burst_t> &src_id_burst_stream, hls::stream<distance_req_pack_t> &distance_req_pack_stream, uint32_t total_edge_sets) {
    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> last_idx_max = 0u;
    LOOP_FOR_4: for (uint32_t edge_burst_idx = 0u; (edge_burst_idx < total_edge_sets); ++edge_burst_idx) {
        #pragma HLS PIPELINE II = 1
        node_id_burst_t node_id_burst;
        node_id_burst = src_id_burst_stream.read();
        ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> cache_idx[PE_NUM];
        #pragma HLS ARRAY_PARTITION variable = cache_idx complete dim = 0
        LOOP_FOR_0: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
            #pragma HLS UNROLL
            cache_idx[pe_idx] = (node_id_burst.data[pe_idx].range(30u, 0u) >> LOG_DIST_PER_WORD);
        }
        ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> cache_idx_diffs[PE_NUM];
        #pragma HLS ARRAY_PARTITION variable = cache_idx_diffs complete dim = 0
        LOOP_FOR_1: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
            #pragma HLS UNROLL
            cache_idx_diffs[pe_idx] = (cache_idx[pe_idx] - last_idx_max);
        }
        if (cache_idx_diffs[(PE_NUM - 1u)]) {
            ap_uint<PE_NUM> valid_mask;
            LOOP_FOR_2: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
                #pragma HLS UNROLL
                if ((cache_idx_diffs[pe_idx] == 0u)) {
                    valid_mask.range(pe_idx, pe_idx) = 1u;
                }
                else {
                    valid_mask.range(pe_idx, pe_idx) = 0u;
                }
            }
            ap_uint<4> num_unread = count_end_ones(valid_mask);
            distance_req_pack_t req_pack;
            req_pack.offset = num_unread;
            req_pack.end_flag = false;
            LOOP_FOR_3: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
                #pragma HLS UNROLL
                req_pack.idx[pe_idx] = cache_idx[pe_idx];
            }
            distance_req_pack_stream.write(req_pack);
        }
        last_idx_max = cache_idx[(PE_NUM - 1u)];
    }
    {
        distance_req_pack_t end_req_pack;
        end_req_pack.end_flag = true;
        end_req_pack.offset = 7u;
        distance_req_pack_stream.write(end_req_pack);
    }
}

void cacheline_req_sender(hls::stream<distance_req_pack_t> &distance_req_pack_stream, hls::stream<cacheline_req_t> &cacheline_req_stream, int32_t memory_offset) {
    {
        cacheline_req_t cache_req;
        cache_req.end_flag = false;
        cache_req.idx = memory_offset;
        cache_req.dst = 0u;
        cacheline_req_stream.write(cache_req);
    }
    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> cacheline_idx[PE_NUM];
    #pragma HLS ARRAY_PARTITION variable = cacheline_idx complete dim = 0
    LOOP_WHILE_7: while (true) {
        #pragma HLS PIPELINE II = 1
        #pragma HLS dependence variable = cacheline_idx inter false
        distance_req_pack_t req_pack;
        req_pack = distance_req_pack_stream.read();
        LOOP_FOR_5: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
            #pragma HLS UNROLL
            cacheline_idx[pe_idx] = req_pack.idx[pe_idx];
        }
        LOOP_FOR_6: for (ap_uint<4> i = req_pack.offset; (i < PE_NUM); ++i) {
            #pragma HLS PIPELINE II = 1 rewind
            #pragma HLS unroll factor = 1
            cacheline_req_t cache_req;
            cache_req.idx = (cacheline_idx[i] + memory_offset);
            cache_req.dst = i;
            cache_req.end_flag = req_pack.end_flag;
            cacheline_req_stream.write(cache_req);
        }
        if (req_pack.end_flag) {
            break;
        }
    }
}

void node_prop_resp_receiver(hls::stream<cacheline_resp_t> &cacheline_resp_stream, hls::stream<bus_word_t> (&cacheline_streams)[PE_NUM]) {
    cacheline_resp_t cache_resp;
    cache_resp = cacheline_resp_stream.read();
    bus_word_t first_line = cache_resp.data;
    LOOP_FOR_8: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
        #pragma HLS UNROLL
        cacheline_streams[pe_idx].write(first_line);
    }
    LOOP_WHILE_9: while (true) {
        #pragma HLS PIPELINE II = 1
        if (cacheline_resp_stream.read_nb(cache_resp)) {
            if (cache_resp.end_flag) {
                break;
            }
            bus_word_t resp_line = cache_resp.data;
            ap_uint<8> target_pe = cache_resp.dst;
            cacheline_streams[target_pe].write(resp_line);
        }
    }
}

void merge_node_props(hls::stream<bus_word_t> (&cacheline_streams)[PE_NUM], hls::stream<edge_descriptor_batch_t> &edge_stream, hls::stream<update_tuple_t_big> &edge_batch_stream, uint32_t total_edge_sets) {
    bus_word_t last_cacheline[PE_NUM];
    #pragma HLS ARRAY_PARTITION variable = last_cacheline complete dim = 0
    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> last_cache_idx[PE_NUM];
    #pragma HLS ARRAY_PARTITION variable = last_cache_idx complete dim = 0
    LOOP_FOR_10: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
        #pragma HLS UNROLL
        last_cacheline[pe_idx] = cacheline_streams[pe_idx].read();
        last_cache_idx[pe_idx] = 0u;
    }
    ap_fixed_pod_t edge_weight = (1u << (DISTANCE_BITWIDTH - DISTANCE_INTEGER_PART));
    LOOP_FOR_13: for (int32_t edge_batch_idx = 0; (edge_batch_idx < total_edge_sets); ++edge_batch_idx) {
        #pragma HLS PIPELINE II = 1
        edge_descriptor_batch_t edge_batch;
        edge_batch = edge_stream.read();
        update_tuple_t_big out_batch;
        LOOP_FOR_11: for (int32_t pe_idx = 0; (pe_idx < PE_NUM); ++pe_idx) {
            #pragma HLS UNROLL
            ap_uint<NODE_ID_BITWIDTH - 1> node_id_low = edge_batch.edges[pe_idx].src_id.range(30u, 0u);
            ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> cacheline_idx = (node_id_low >> LOG_DIST_PER_WORD);
            ap_uint<LOG_DIST_PER_WORD> offset = (node_id_low & (DIST_PER_WORD - 1u));
            bus_word_t cacheline;
            if ((cacheline_idx == last_cache_idx[pe_idx])) {
                cacheline = last_cacheline[pe_idx];
            }
            else {
                cacheline = cacheline_streams[pe_idx].read();
            }
            ap_fixed_pod_t prop = cacheline.range(((static_cast<ap_uint<9>>(offset) * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (static_cast<ap_uint<9>>(offset) * DISTANCE_BITWIDTH));
            ap_fixed_pod_t BinOp_68_res = ((prop == INFINITY_POD) ? INFINITY_POD : static_cast<ap_fixed_pod_t>(prop));
            out_batch.data[pe_idx].prop = BinOp_68_res;
            out_batch.data[pe_idx].node_id = edge_batch.edges[pe_idx].dst_id;
            out_batch.data[pe_idx].end_flag = false;
            if ((pe_idx == (PE_NUM - 1u))) {
                last_cacheline[pe_idx] = cacheline;
                last_cache_idx[pe_idx] = cacheline_idx;
            }
        }
        edge_batch_stream.write(out_batch);
        LOOP_FOR_12: for (int32_t pe_idx = 0; (pe_idx < (PE_NUM - 1u)); ++pe_idx) {
            #pragma HLS UNROLL
            last_cacheline[pe_idx] = last_cacheline[(PE_NUM - 1u)];
            last_cache_idx[pe_idx] = last_cache_idx[(PE_NUM - 1u)];
        }
    }
}

void demux_1(hls::stream<update_tuple_t_big> &in_batch_stream, hls::stream<update_t_big> (&out_streams)[PE_NUM], uint32_t total_edge_sets) {
    LOOP_FOR_19: for (uint32_t batch_idx = 0u; (batch_idx < total_edge_sets); ++batch_idx) {
        #pragma HLS PIPELINE II = 1
        update_tuple_t_big in_batch;
        in_batch = in_batch_stream.read();
        LOOP_FOR_18: for (uint32_t i = 0u; (i < PE_NUM); ++i) {
            #pragma HLS UNROLL
            if ((in_batch.data[i].node_id.range(LOCAL_ID_MSB, LOCAL_ID_MSB) == 0u)) {
                out_streams[i].write(in_batch.data[i]);
            }
        }
    }
    LOOP_FOR_20: for (uint32_t i_tail = 0u; (i_tail < PE_NUM); ++i_tail) {
        #pragma HLS UNROLL
        update_t_big end_wrapper;
        end_wrapper.end_flag = true;
        out_streams[i_tail].write(end_wrapper);
    }
}

void sender_2(int32_t i, hls::stream<update_t_big> &in1, hls::stream<update_t_big> &in2, hls::stream<update_t_big> &out1, hls::stream<update_t_big> &out2, hls::stream<update_t_big> &out3, hls::stream<update_t_big> &out4) {
    #pragma HLS function_instantiate variable = i
    bool in1_end_flag = false;
    bool in2_end_flag = false;
    LOOP_WHILE_21: while (true) {
        #pragma HLS PIPELINE II = 1
        if ((!in1.empty())) {
            update_t_big data1;
            data1 = in1.read();
            if ((!data1.end_flag)) {
                if ((((data1.node_id >> i) & 1) != 0)) {
                    out2.write(data1);
                }
                else {
                    out1.write(data1);
                }
            }
            else {
                in1_end_flag = true;
            }
        }
        if ((!in2.empty())) {
            update_t_big data2;
            data2 = in2.read();
            if ((!data2.end_flag)) {
                if ((((data2.node_id >> i) & 1) != 0)) {
                    out4.write(data2);
                }
                else {
                    out3.write(data2);
                }
            }
            else {
                in2_end_flag = true;
            }
        }
        if ((in1_end_flag && in2_end_flag)) {
            update_t_big data;
            data.end_flag = true;
            out1.write(data);
            out2.write(data);
            out3.write(data);
            out4.write(data);
            in1_end_flag = false;
            in2_end_flag = false;
            break;
        }
    }
}

void receiver_2(int32_t i, hls::stream<update_t_big> &out1, hls::stream<update_t_big> &out2, hls::stream<update_t_big> &in1, hls::stream<update_t_big> &in2, hls::stream<update_t_big> &in3, hls::stream<update_t_big> &in4) {
    #pragma HLS function_instantiate variable = i
    bool in1_end_flag = false;
    bool in2_end_flag = false;
    bool in3_end_flag = false;
    bool in4_end_flag = false;
    LOOP_WHILE_22: while (true) {
        #pragma HLS PIPELINE II = 1
        if ((!in1.empty())) {
            update_t_big data;
            data = in1.read();
            if ((!data.end_flag)) {
                out1.write(data);
            }
            else {
                in1_end_flag = true;
            }
        }
        else {
            if ((!in3.empty())) {
                update_t_big data;
                data = in3.read();
                if ((!data.end_flag)) {
                    out1.write(data);
                }
                else {
                    in3_end_flag = true;
                }
            }
        }
        if ((!in2.empty())) {
            update_t_big data;
            data = in2.read();
            if ((!data.end_flag)) {
                out2.write(data);
            }
            else {
                in2_end_flag = true;
            }
        }
        else {
            if ((!in4.empty())) {
                update_t_big data;
                data = in4.read();
                if ((!data.end_flag)) {
                    out2.write(data);
                }
                else {
                    in4_end_flag = true;
                }
            }
        }
        if (((in1_end_flag && in2_end_flag) && (in3_end_flag && in4_end_flag))) {
            update_t_big data;
            data.end_flag = true;
            out1.write(data);
            out2.write(data);
            break;
        }
    }
}

void switch2x2_2(int32_t i, hls::stream<update_t_big> &in1, hls::stream<update_t_big> &in2, hls::stream<update_t_big> &out1, hls::stream<update_t_big> &out2) {
    #pragma HLS DATAFLOW
    hls::stream<update_t_big> l1_1;
    #pragma HLS STREAM variable = l1_1 depth = 2
    hls::stream<update_t_big> l1_2;
    #pragma HLS STREAM variable = l1_2 depth = 2
    hls::stream<update_t_big> l1_3;
    #pragma HLS STREAM variable = l1_3 depth = 2
    hls::stream<update_t_big> l1_4;
    #pragma HLS STREAM variable = l1_4 depth = 2
    sender_2(i, in1, in2, l1_1, l1_2, l1_3, l1_4);
    receiver_2(i, out1, out2, l1_1, l1_2, l1_3, l1_4);
}

void Reduc_105_unit_reduce_single_pe(hls::stream<update_t_big> &kt_wrap_item_single, hls::stream<reduce_word_t> &pe_mem_out, uint32_t num_word_per_pe) {
    const int32_t MEM_SIZE = ((MAX_NUM >> LOG_PE_NUM) / DISTANCES_PER_REDUCE_WORD);
    reduce_word_t prop_mem[MEM_SIZE];
    #pragma HLS BIND_STORAGE variable = prop_mem type = RAM_2P impl = URAM
    #pragma HLS dependence variable = prop_mem inter false
    reduce_word_t cache_data_buffer[(L + 1)];
    #pragma HLS ARRAY_PARTITION variable = cache_data_buffer complete dim = 0
    local_id_t cache_addr_buffer[(L + 1)];
    #pragma HLS ARRAY_PARTITION variable = cache_addr_buffer complete dim = 0
    ap_fixed_pod_t identity_val = INFINITY_POD;
    reduce_word_t identity_word;
    INIT_IDENTITY_WORD: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
        #pragma HLS UNROLL
        identity_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH)) = identity_val;
    }
    INIT_REDUCE_MEM: for (int32_t init_idx = 0; (init_idx < num_word_per_pe); ++init_idx) {
        #pragma HLS PIPELINE II = 1
        prop_mem[init_idx] = identity_word;
    }
    LOOP_FOR_23: for (int32_t i = 0; (i < (L + 1)); ++i) {
        #pragma HLS UNROLL
        cache_addr_buffer[i] = 0u;
        cache_data_buffer[i] = identity_word;
    }
    LOOP_WHILE_26: while (true) {
        #pragma HLS PIPELINE II = 1
        update_t_big kt_elem;
        kt_elem = kt_wrap_item_single.read();
        if (kt_elem.end_flag) {
            break;
        }
        local_id_t key = (kt_elem.node_id >> LOG_PE_NUM);
        ap_fixed_pod_t incoming_dist_pod = kt_elem.prop;
        local_id_t word_addr = (key >> LOG_DISTANCES_PER_REDUCE_WORD);
        reduce_word_t current_word = prop_mem[word_addr];
        LOOP_FOR_24: for (int32_t i = 0; (i < (L + 1)); ++i) {
            #pragma HLS UNROLL
            if ((cache_addr_buffer[i] == word_addr)) {
                current_word = cache_data_buffer[i];
            }
        }
        LOOP_FOR_25: for (int32_t i = 0; (i < L); ++i) {
            #pragma HLS UNROLL
            cache_addr_buffer[i] = cache_addr_buffer[(i + 1)];
            cache_data_buffer[i] = cache_data_buffer[(i + 1)];
        }
        uint32_t slot = (key & (DISTANCES_PER_REDUCE_WORD - 1u));
        uint32_t bit_low = (slot * DISTANCE_BITWIDTH);
        uint32_t bit_high = (bit_low + (DISTANCE_BITWIDTH - 1u));
        ap_fixed_pod_t current_val = current_word.range(bit_high, bit_low);
        ap_fixed_pod_t updated_val = ((current_val == identity_val) ? static_cast<ap_fixed_pod_t>(incoming_dist_pod) : ((incoming_dist_pod < current_val) ? static_cast<ap_fixed_pod_t>(incoming_dist_pod) : static_cast<ap_fixed_pod_t>(current_val)));
        current_word.range(bit_high, bit_low) = updated_val;
        prop_mem[word_addr] = current_word;
        cache_data_buffer[L] = current_word;
        cache_addr_buffer[L] = word_addr;
    }
    LOOP_FOR_27: for (int32_t i = 0; (i < num_word_per_pe); ++i) {
        #pragma HLS UNROLL factor = 1
        reduce_word_t tmp_word = prop_mem[i];
        prop_mem[i] = identity_word;
        pe_mem_out.write(tmp_word);
    }
}

void Reduc_105_partial_drain_four(hls::stream<reduce_word_t> (&pe_mem_in)[PE_NUM], uint32_t base_idx, uint32_t num_word_per_pe, hls::stream<ap_uint<256>> &partial_out_stream) {
    #pragma HLS function_instantiate variable = base_idx
    LOOP_FOR_29: for (uint32_t word_idx = 0u; (word_idx < num_word_per_pe); ++word_idx) {
        #pragma HLS PIPELINE II = 1
        ap_uint<256> packed_out = 0u;
        LOOP_FOR_28: for (uint32_t pe_offset = 0u; (pe_offset < 4u); ++pe_offset) {
            #pragma HLS UNROLL
            reduce_word_t tmp_word = pe_mem_in[(base_idx + pe_offset)].read();
            PACK_DISTANCES_4: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
                #pragma HLS UNROLL
                uint32_t bit_low = (((dist_idx * 4u) + pe_offset) * DISTANCE_BITWIDTH);
                packed_out.range((bit_low + (DISTANCE_BITWIDTH - 1u)), bit_low) = tmp_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH));
            }
        }
        partial_out_stream.write(packed_out);
    }
}

void Reduc_105_finalize_drain(hls::stream<ap_uint<256>> &lower_pe_pack_stream, hls::stream<ap_uint<256>> &upper_pe_pack_stream, uint32_t num_word_per_pe, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    LOOP_FOR_30: for (uint32_t word_idx = 0u; (word_idx < num_word_per_pe); ++word_idx) {
        #pragma HLS PIPELINE II = 1
        ap_uint<256> lower_pe_pack;
        lower_pe_pack = lower_pe_pack_stream.read();
        ap_uint<256> upper_pe_pack;
        upper_pe_pack = upper_pe_pack_stream.read();
        write_burst_pkt_t one_write_burst;
        one_write_burst.data.range(127u, 0u) = lower_pe_pack.range(127u, 0u);
        one_write_burst.data.range(255u, 128u) = upper_pe_pack.range(127u, 0u);
        one_write_burst.data.range(383u, 256u) = lower_pe_pack.range(255u, 128u);
        one_write_burst.data.range(511u, 384u) = upper_pe_pack.range(255u, 128u);
        kernel_out_stream.write(one_write_burst);
    }
}

void Reduc_105_drain_variable(hls::stream<reduce_word_t> (&pe_mem_in)[PE_NUM], uint32_t num_word_per_pe, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    ap_fixed_pod_t identity_val = INFINITY_POD;
    reduce_word_t identity_word;
    DRAIN_INIT_IDENTITY_WORD: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
        #pragma HLS UNROLL
        identity_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH)) = identity_val;
    }
    uint32_t reduce_words_per_bus = (DIST_PER_WORD / DISTANCES_PER_REDUCE_WORD);
    uint32_t reduce_words_per_pe = (reduce_words_per_bus / PE_NUM);
    uint32_t num_bus_words = ((num_word_per_pe + (reduce_words_per_pe - 1u)) / reduce_words_per_pe);
    LOOP_DRAIN_WORD: for (uint32_t word_idx = 0u; (word_idx < num_bus_words); ++word_idx) {
        write_burst_pkt_t one_write_burst;
        LOOP_DRAIN_SUBWORD: for (uint32_t sub_idx = 0u; (sub_idx < reduce_words_per_pe); ++sub_idx) {
            #pragma HLS PIPELINE II = 1
            uint32_t word_base = ((word_idx * reduce_words_per_pe) + sub_idx);
            LOOP_DRAIN_PE: for (uint32_t pe_idx = 0u; (pe_idx < PE_NUM); ++pe_idx) {
                #pragma HLS UNROLL
                reduce_word_t tmp_word = identity_word;
                if ((word_base < num_word_per_pe)) {
                    tmp_word = pe_mem_in[pe_idx].read();
                }
                PACK_DISTANCES: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
                    #pragma HLS UNROLL
                    uint32_t slot_idx = ((((sub_idx * DISTANCES_PER_REDUCE_WORD) + dist_idx) * PE_NUM) + pe_idx);
                    uint32_t bit_low = (slot_idx * DISTANCE_BITWIDTH);
                    one_write_burst.data.range((bit_low + (DISTANCE_BITWIDTH - 1u)), bit_low) = tmp_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH));
                }
            }
        }
        kernel_out_stream.write(one_write_burst);
    }
}

extern "C" void graphyflow_big(const bus_word_t *edge_props, int32_t num_nodes, int32_t num_edges, int32_t dst_num, int32_t memory_offset, hls::stream<cacheline_request_pkt_t> &cacheline_req_stream, hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream, hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    #pragma HLS INTERFACE m_axi port = edge_props offset = slave bundle = gmem0
    #pragma HLS INTERFACE s_axilite port = edge_props
    #pragma HLS INTERFACE s_axilite port = num_nodes
    #pragma HLS INTERFACE s_axilite port = num_edges
    #pragma HLS INTERFACE s_axilite port = dst_num
    #pragma HLS INTERFACE s_axilite port = memory_offset
    #pragma HLS INTERFACE s_axilite port = return
    #pragma HLS DATAFLOW
    hls::stream<node_id_burst_t> stream_src_ids;
    #pragma HLS STREAM variable = stream_src_ids depth = 16
    hls::stream<distance_req_pack_t> stream_dist_req;
    #pragma HLS STREAM variable = stream_dist_req depth = 16
    hls::stream<bus_word_t> stream_cachelines[PE_NUM];
    #pragma HLS STREAM variable = stream_cachelines depth = 16
    hls::stream<edge_descriptor_batch_t> edge_stream;
    #pragma HLS STREAM variable = edge_stream depth = 16
    hls::stream<update_tuple_t_big> stream_edge_data;
    #pragma HLS STREAM variable = stream_edge_data depth = 16
    hls::stream<cacheline_req_t> cacheline_req;
    #pragma HLS STREAM variable = cacheline_req depth = 32
    hls::stream<cacheline_resp_t> cacheline_resp;
    #pragma HLS STREAM variable = cacheline_resp depth = 32
    hls::stream<update_t_big> reduce_105_d2o_pair[PE_NUM];
    #pragma HLS STREAM variable = reduce_105_d2o_pair depth = 8
    #pragma HLS ARRAY_PARTITION variable = reduce_105_d2o_pair complete dim = 0
    hls::stream<update_t_big> reduce_105_o2u_pair[PE_NUM];
    #pragma HLS STREAM variable = reduce_105_o2u_pair depth = 2
    #pragma HLS ARRAY_PARTITION variable = reduce_105_o2u_pair complete dim = 0
    uint32_t num_words = ((dst_num + (DISTANCES_PER_REDUCE_WORD - 1u)) >> LOG_DISTANCES_PER_REDUCE_WORD);
    uint32_t num_word_per_pe = ((num_words + (PE_NUM - 1u)) >> LOG_PE_NUM);
    int32_t edges_per_word = EDGES_PER_WORD;
    int32_t num_wide_reads = (num_edges / edges_per_word);
    uint32_t total_edge_sets = static_cast<uint32_t>(num_wide_reads);
    LOOP_FOR_47: for (int32_t i = 0; (i < num_wide_reads); ++i) {
        #pragma HLS PIPELINE II = 1
        bus_word_t wide_word = edge_props[i];
        edge_descriptor_batch_t edge_batch;
        LOOP_FOR_45: for (int32_t j = 0; (j < edges_per_word); ++j) {
            #pragma HLS UNROLL
            ap_uint<EDGE_PAYLOAD_BITS> packed_edge = wide_word.range(((j * EDGE_PAYLOAD_BITS) + (EDGE_PAYLOAD_BITS - 1u)), (j * EDGE_PAYLOAD_BITS));
            edge_batch.edges[j].dst_id = packed_edge.range(LOCAL_ID_MSB, 0u);
            edge_batch.edges[j].src_id = packed_edge.range(EDGE_SRC_PAYLOAD_MSB, EDGE_SRC_PAYLOAD_LSB);
        }
        LOOP_FOR_45_PAD: for (int32_t j = edges_per_word; (j < PE_NUM); ++j) {
            #pragma HLS UNROLL
            edge_batch.edges[j].dst_id = INVALID_LOCAL_ID_BIG;
            edge_batch.edges[j].src_id = edge_batch.edges[(edges_per_word - 1)].src_id;
        }
        edge_stream.write(edge_batch);
        node_id_burst_t src_id_burst;
        LOOP_FOR_46: for (int32_t j = 0; (j < PE_NUM); ++j) {
            #pragma HLS UNROLL
            node_id_t src_id = edge_batch.edges[j].src_id;
            src_id_burst.data[j] = src_id;
        }
        stream_src_ids.write(src_id_burst);
    }
    dist_req_packer(stream_src_ids, stream_dist_req, total_edge_sets);
    cacheline_req_sender(stream_dist_req, cacheline_req, memory_offset);
    stream2axistream(cacheline_req, cacheline_req_stream);
    axistream2stream(cacheline_resp_stream, cacheline_resp);
    node_prop_resp_receiver(cacheline_resp, stream_cachelines);
    merge_node_props(stream_cachelines, edge_stream, stream_edge_data, total_edge_sets);
    demux_1(stream_edge_data, reduce_105_d2o_pair, total_edge_sets);
    hls::stream<update_t_big> stream_stage_0[PE_NUM];
    #pragma HLS STREAM variable = stream_stage_0 depth = 2
    #pragma HLS ARRAY_PARTITION variable = stream_stage_0 complete dim = 0
    hls::stream<update_t_big> stream_stage_1[PE_NUM];
    #pragma HLS STREAM variable = stream_stage_1 depth = 2
    #pragma HLS ARRAY_PARTITION variable = stream_stage_1 complete dim = 0
    switch2x2_2(2, reduce_105_d2o_pair[0], reduce_105_d2o_pair[1], stream_stage_0[0], stream_stage_0[1]);
    switch2x2_2(2, reduce_105_d2o_pair[2], reduce_105_d2o_pair[3], stream_stage_0[2], stream_stage_0[3]);
    switch2x2_2(2, reduce_105_d2o_pair[4], reduce_105_d2o_pair[5], stream_stage_0[4], stream_stage_0[5]);
    switch2x2_2(2, reduce_105_d2o_pair[6], reduce_105_d2o_pair[7], stream_stage_0[6], stream_stage_0[7]);
    switch2x2_2(1, stream_stage_0[0], stream_stage_0[4], stream_stage_1[0], stream_stage_1[1]);
    switch2x2_2(1, stream_stage_0[1], stream_stage_0[5], stream_stage_1[2], stream_stage_1[3]);
    switch2x2_2(1, stream_stage_0[2], stream_stage_0[6], stream_stage_1[4], stream_stage_1[5]);
    switch2x2_2(1, stream_stage_0[3], stream_stage_0[7], stream_stage_1[6], stream_stage_1[7]);
    switch2x2_2(0, stream_stage_1[0], stream_stage_1[4], reduce_105_o2u_pair[0], reduce_105_o2u_pair[1]);
    switch2x2_2(0, stream_stage_1[1], stream_stage_1[5], reduce_105_o2u_pair[2], reduce_105_o2u_pair[3]);
    switch2x2_2(0, stream_stage_1[2], stream_stage_1[6], reduce_105_o2u_pair[4], reduce_105_o2u_pair[5]);
    switch2x2_2(0, stream_stage_1[3], stream_stage_1[7], reduce_105_o2u_pair[6], reduce_105_o2u_pair[7]);
    hls::stream<reduce_word_t> pe_mem_out_streams[PE_NUM];
    #pragma HLS STREAM variable = pe_mem_out_streams depth = 4
    LOOP_FOR_48: for (int32_t i = 0; (i < PE_NUM); ++i) {
        #pragma HLS UNROLL
        Reduc_105_unit_reduce_single_pe(reduce_105_o2u_pair[i], pe_mem_out_streams[i], num_word_per_pe);
    }
    Reduc_105_drain_variable(pe_mem_out_streams, num_word_per_pe, kernel_out_stream);
}