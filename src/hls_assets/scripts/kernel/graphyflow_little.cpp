#include "graphyflow_little.h"

void load_edges(const bus_word_t *edge_props, int32_t num_edges, hls::stream<edge_descriptor_batch_t> &edge_stream) {
    int32_t edges_per_word = EDGES_PER_WORD;
    int32_t num_wide_reads = (num_edges / edges_per_word);
    LOAD_EDGES_OUTER: for (int32_t i = 0; (i < num_wide_reads); ++i) {
        #pragma HLS PIPELINE II = 1
        bus_word_t wide_word = edge_props[i];
        edge_descriptor_batch_t edge_batch;
        LOAD_EDGES_INNER: for (int32_t j = 0; (j < edges_per_word); ++j) {
            #pragma HLS UNROLL
            ap_uint<EDGE_PAYLOAD_BITS> packed_edge = wide_word.range(((j * EDGE_PAYLOAD_BITS) + (EDGE_PAYLOAD_BITS - 1u)), (j * EDGE_PAYLOAD_BITS));
            edge_batch.edges[j].dst_id = packed_edge.range(31, 0);
            edge_batch.edges[j].src_id = packed_edge.range(EDGE_SRC_PAYLOAD_MSB, EDGE_SRC_PAYLOAD_LSB);
            edge_batch.edges[j].edge_prop = packed_edge.range(EDGE_PROP_PAYLOAD_MSB, EDGE_PROP_PAYLOAD_LSB);
        }
        LOAD_EDGES_PAD: for (int32_t j_pad = edges_per_word; (j_pad < PE_NUM); ++j_pad) {
            #pragma HLS UNROLL
            edge_batch.edges[j_pad].dst_id = INVALID_LOCAL_ID_LITTLE;
            edge_batch.edges[j_pad].src_id = edge_batch.edges[(edges_per_word - 1)].src_id;
            edge_batch.edges[j_pad].edge_prop = 0u;
        }
        edge_stream.write(edge_batch);
    }
}

void stream2axistream(hls::stream<ppb_request_t> &stream, hls::stream<ppb_request_pkt_t> &axi_stream) {
    stream2axistream: while (true) {
        ppb_request_t tmp_t1;
        tmp_t1 = stream.read();
        ppb_request_pkt_t tmp_t2;
        tmp_t2.data = tmp_t1.request_round;
        tmp_t2.last = tmp_t1.end_flag;
        axi_stream.write(tmp_t2);
        if (tmp_t1.end_flag) {
            break;
        }
    }
}

void axistream2stream(hls::stream<ppb_response_pkt_t> &axi_stream, hls::stream<ppb_response_t> &stream) {
    axistream2stream: while (true) {
        ppb_response_pkt_t tmp_t1;
        tmp_t1 = axi_stream.read();
        ppb_response_t tmp_t2;
        tmp_t2.data = tmp_t1.data;
        tmp_t2.addr = tmp_t1.dest;
        tmp_t2.end_flag = tmp_t1.last;
        stream.write(tmp_t2);
        if (tmp_t2.end_flag) {
            break;
        }
    }
}

ap_fixed_pod_t extract_src_prop_from_bus_word(bus_word_t word, ap_uint<LOG_DIST_PER_WORD> offset) {
    #pragma HLS INLINE
    ap_uint<9> bit_low = (offset * DISTANCE_BITWIDTH);
    bus_word_t shifted = (word >> bit_low);
    ap_fixed_pod_t out = shifted.range((DISTANCE_BITWIDTH - 1u), 0u);
    return out;
}

void request_manager(hls::stream<edge_descriptor_batch_t> &edge_burst_stm, hls::stream<ppb_request_t> &ppb_request_stm, hls::stream<ppb_response_t> &ppb_response_stm, hls::stream<update_tuple_t_little> &update_set_stm, uint32_t memory_offset, uint32_t total_edge_sets) {
    bus_word_t src_prop_buffer[PE_NUM][2][(SRC_BUFFER_SIZE / DIST_PER_WORD)];
    #pragma HLS ARRAY_PARTITION variable = src_prop_buffer dim = 1 complete
    #pragma HLS BIND_STORAGE variable = src_prop_buffer type = RAM_S2P impl = BRAM
    #pragma HLS dependence variable = src_prop_buffer inter false
    ap_uint<22> pp_read_round = 0u;
    ap_uint<22> pp_write_round = 0u;
    ap_uint<22> pp_request_round = 0u;
    int32_t edge_set_cnt = 0;
    bool wait_flag = false;
    edge_descriptor_batch_t an_edge_burst;
    #pragma HLS ARRAY_PARTITION variable = an_edge_burst.edges dim = 1 complete
    ap_fixed_pod_t identity_pod = INFINITY_POD;
    LOOP_WHILE_17: while (true) {
        #pragma HLS PIPELINE II = 1
        if ((pp_request_round < pp_read_round)) {
            pp_request_round = pp_read_round;
        }
        // Prefetch one round ahead. We treat "seeing any response for round R"
        // as evidence that the previous round (R-1) has completed, because
        // `hbm_writer::little_node_prop_loader` services one request at a time
        // (emits a full SRC_BUFFER_WORDS burst per request).
        if (((pp_request_round - pp_read_round) <= 1u)) {
            ppb_request_t one_ppb_request;
            one_ppb_request.request_round = (pp_request_round + memory_offset);
            one_ppb_request.end_flag = 0u;
            ppb_request_stm.write(one_ppb_request);
            pp_request_round = (pp_request_round + 1u);
        }
        ppb_response_t one_ppb_response;
        bool got_ppb_response = ppb_response_stm.read_nb(one_ppb_response);
        if (got_ppb_response) {
            ap_uint<22> pp_write_round_resp = ((one_ppb_response.addr >> (LOG_SRC_BUFFER_SIZE - LOG_DIST_PER_WORD)) - memory_offset);
            bool write_buffer = pp_write_round_resp.range(0, 0);
            uint32_t write_idx = (one_ppb_response.addr & ((SRC_BUFFER_SIZE / DIST_PER_WORD) - 1u));
            bus_word_t one_read_burst = one_ppb_response.data;
            LOOP_FOR_14: for (int32_t u = 0; (u < PE_NUM); ++u) {
                #pragma HLS UNROLL
                src_prop_buffer[u][write_buffer][write_idx] = one_read_burst;
            }
            pp_write_round = pp_write_round_resp;
        }
        if ((!wait_flag)) {
            an_edge_burst = edge_burst_stm.read();
        }
        pp_read_round = (an_edge_burst.edges[0].src_id / SRC_BUFFER_SIZE);
        wait_flag = (pp_read_round >= pp_write_round);
        bool exit_flag = ((wait_flag == false) ? ((edge_set_cnt + 1) >= total_edge_sets) : (edge_set_cnt >= total_edge_sets));
        if ((!wait_flag)) {
            bool read_buffer = pp_read_round.range(0, 0);
            update_tuple_t_little an_update_set;
            #pragma HLS ARRAY_PARTITION variable = an_update_set.data dim = 1 complete
            LOOP_FOR_15: for (int32_t u = 0; (u < PE_NUM); ++u) {
                #pragma HLS UNROLL
                ap_uint<31> idx = (an_edge_burst.edges[u].src_id % SRC_BUFFER_SIZE);
                ap_uint<30> uram_row_idx = (idx >> LOG_DIST_PER_WORD);
                ap_uint<30> uram_row_offset = (idx & (DIST_PER_WORD - 1u));
                bus_word_t uram_row = src_prop_buffer[u][read_buffer][uram_row_idx];
                ap_fixed_pod_t src_prop = extract_src_prop_from_bus_word(uram_row, uram_row_offset);
                ap_fixed_pod_t edge_weight = (static_cast<ap_fixed_pod_t>(an_edge_burst.edges[u].edge_prop.range(31u, 0u)) << 16u);
                ap_fixed_pod_t BinOp_68_res = ((src_prop == INFINITY_POD) ? INFINITY_POD : static_cast<ap_fixed_pod_t>((src_prop + edge_weight)));
                an_update_set.data[u].prop = BinOp_68_res;
                an_update_set.data[u].node_id = an_edge_burst.edges[u].dst_id;
            }
            update_set_stm.write(an_update_set);
            edge_set_cnt = (edge_set_cnt + 1);
        }
        if (exit_flag) {
            ppb_request_t one_ppb_request;
            one_ppb_request.request_round = 0u;
            one_ppb_request.end_flag = 1u;
            ppb_request_stm.write(one_ppb_request);
            LOOP_WHILE_16: while (true) {
                ppb_response_t one_ppb_response;
                one_ppb_response = ppb_response_stm.read();
                if (one_ppb_response.end_flag) {
                    break;
                }
            }
            break;
        }
    }
}

void Reduc_105_unit_reduce(hls::stream<update_tuple_t_little> &update_set_stm, hls::stream<reduce_word_t> (&pe_mem_outs_1)[4], hls::stream<reduce_word_t> (&pe_mem_outs_2)[4], uint32_t total_edge_sets, uint32_t rounded_num_words) {
    const int32_t MEM_SIZE = (MAX_NUM / DISTANCES_PER_REDUCE_WORD);
    reduce_word_t prop_mem[PE_NUM][MEM_SIZE];
    #pragma HLS ARRAY_PARTITION variable = prop_mem complete dim = 1
    #pragma HLS BIND_STORAGE variable = prop_mem type = RAM_S2P impl = URAM
    #pragma HLS dependence variable = prop_mem inter false
    reduce_word_t cache_data_buffer[PE_NUM][(L + 1)];
    #pragma HLS ARRAY_PARTITION variable = cache_data_buffer complete dim = 0
    local_id_t cache_addr_buffer[PE_NUM][(L + 1)];
    #pragma HLS ARRAY_PARTITION variable = cache_addr_buffer complete dim = 0
    ap_fixed_pod_t identity_val = 0u;
    reduce_word_t identity_word;
    INIT_IDENTITY_WORD: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
        #pragma HLS UNROLL
        identity_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH)) = identity_val;
    }
    #ifdef EMULATION
    INIT_PROP_MEM: for (int32_t init_idx = 0; (init_idx < rounded_num_words); ++init_idx) {
        #pragma HLS PIPELINE II = 1
        INIT_PROP_MEM_PE: for (int32_t init_pe = 0; (init_pe < PE_NUM); ++init_pe) {
            #pragma HLS UNROLL
            prop_mem[init_pe][init_idx] = identity_word;
        }
    }
    #endif
    LOOP_FOR_32: for (int32_t i = 0; (i < (L + 1)); ++i) {
        #pragma HLS UNROLL
        LOOP_FOR_31: for (int32_t pe = 0; (pe < PE_NUM); ++pe) {
            #pragma HLS UNROLL
            cache_addr_buffer[pe][i] = 0u;
            cache_data_buffer[pe][i] = 0u;
        }
    }
    LOOP_FOR_36: for (int32_t update_idx = 0; (update_idx < total_edge_sets); ++update_idx) {
        #pragma HLS PIPELINE II = 1
        update_tuple_t_little one_update;
        one_update = update_set_stm.read();
        LOOP_FOR_35: for (int32_t pe = 0; (pe < PE_NUM); ++pe) {
            #pragma HLS UNROLL
            if ((one_update.data[pe].node_id.range(LOCAL_ID_MSB, LOCAL_ID_MSB) == 0)) {
                local_id_t key = one_update.data[pe].node_id;
                ap_fixed_pod_t incoming_dist_pod = one_update.data[pe].prop;
                local_id_t word_addr = (key >> LOG_DISTANCES_PER_REDUCE_WORD);
                reduce_word_t current_word = prop_mem[pe][word_addr];
                LOOP_FOR_33: for (int32_t i = L; (i >= 0); --i) {
                    #pragma HLS UNROLL
                    if ((cache_addr_buffer[pe][i] == word_addr)) {
                        current_word = cache_data_buffer[pe][i];
                        break;
                    }
                }
                LOOP_FOR_34: for (int32_t i = 0; (i < L); ++i) {
                    #pragma HLS UNROLL
                    cache_addr_buffer[pe][i] = cache_addr_buffer[pe][(i + 1)];
                    cache_data_buffer[pe][i] = cache_data_buffer[pe][(i + 1)];
                }
                uint32_t slot = (key & (DISTANCES_PER_REDUCE_WORD - 1u));
                uint32_t bit_low = (slot * DISTANCE_BITWIDTH);
                uint32_t bit_high = (bit_low + (DISTANCE_BITWIDTH - 1u));
                ap_fixed_pod_t current_val = current_word.range(bit_high, bit_low);
                ap_fixed_pod_t updated_val = ((current_val != 0u) ? static_cast<ap_fixed_pod_t>(((current_val < incoming_dist_pod) ? current_val : incoming_dist_pod)) : static_cast<ap_fixed_pod_t>(incoming_dist_pod));
                current_word.range(bit_high, bit_low) = updated_val;
                prop_mem[pe][word_addr] = current_word;
                cache_data_buffer[pe][L] = current_word;
                cache_addr_buffer[pe][L] = word_addr;
            }
        }
    }
    LOOP_FOR_39: for (int32_t i = 0; (i < rounded_num_words); ++i) {
        #pragma HLS PIPELINE
        LOOP_FOR_37: for (int32_t pe = 0; (pe < 4); ++pe) {
            #pragma HLS UNROLL
            reduce_word_t word = identity_word;
            if ((pe < PE_NUM)) {
                word = prop_mem[pe][i];
                #ifndef EMULATION
                prop_mem[pe][i] = 0u;
                #endif
            }
            pe_mem_outs_1[pe].write(word);
        }
        LOOP_FOR_38: for (int32_t pe = 0; (pe < 4); ++pe) {
            #pragma HLS UNROLL
            reduce_word_t word = identity_word;
            if (((pe + 4) < PE_NUM)) {
                word = prop_mem[(pe + 4)][i];
                #ifndef EMULATION
                prop_mem[(pe + 4)][i] = 0u;
                #endif
            }
            pe_mem_outs_2[pe].write(word);
        }
    }
}

void Reduc_105_partial_drain_impl(int32_t i, hls::stream<reduce_word_t> (&pe_mem_in)[4], hls::stream<reduce_word_t> &partial_out_stream, uint32_t rounded_num_words, ap_fixed_pod_t identity_pod) {
    #pragma HLS function_instantiate variable = i
    LOOP_FOR_41: for (int32_t i = 0; (i < rounded_num_words); ++i) {
        #pragma HLS PIPELINE II = 1
        ap_fixed_pod_t uram_res[DISTANCES_PER_REDUCE_WORD];
        #pragma HLS ARRAY_PARTITION variable = uram_res complete dim = 0
        INIT_URAM_RES: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
            #pragma HLS UNROLL
            uram_res[dist_idx] = identity_pod;
        }
        LOOP_FOR_40: for (int32_t pe_idx = 0; (pe_idx < 4); ++pe_idx) {
            #pragma HLS UNROLL
            reduce_word_t word;
            word = pe_mem_in[pe_idx].read();
            REDUCE_DISTANCES: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
                #pragma HLS UNROLL
                ap_fixed_pod_t incoming_dist_pod = word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH));
                uram_res[dist_idx] = ((incoming_dist_pod != 0u) ? static_cast<ap_fixed_pod_t>(((uram_res[dist_idx] < incoming_dist_pod) ? uram_res[dist_idx] : incoming_dist_pod)) : static_cast<ap_fixed_pod_t>(uram_res[dist_idx]));
            }
        }
        reduce_word_t merged_word;
        PACK_MERGED_WORD: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
            #pragma HLS UNROLL
            merged_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH)) = uram_res[dist_idx];
        }
        partial_out_stream.write(merged_word);
    }
}

void Reduc_105_finalize_drain_single(hls::stream<reduce_word_t> &partial_in, hls::stream<little_out_pkt_t> &kernel_out_stream, uint32_t rounded_num_words) {
    little_out_pkt_t one_write_burst;
    one_write_burst.last = false;
    LOOP_FOR_42_SINGLE: for (int32_t i = 0; (i < rounded_num_words); ++i) {
        #pragma HLS PIPELINE II = 1
        reduce_word_t word;
        word = partial_in.read();
        one_write_burst.data = word;
        kernel_out_stream.write(one_write_burst);
    }
}

void Reduc_105_finalize_drain(hls::stream<reduce_word_t> &partial_in_first, hls::stream<reduce_word_t> &partial_in_second, hls::stream<little_out_pkt_t> &kernel_out_stream, uint32_t rounded_num_words, ap_fixed_pod_t identity_pod) {
    little_out_pkt_t one_write_burst;
    one_write_burst.last = false;
    LOOP_FOR_42: for (int32_t i = 0; (i < rounded_num_words); ++i) {
        #pragma HLS PIPELINE II = 1
        reduce_word_t first_word;
        first_word = partial_in_first.read();
        reduce_word_t second_word;
        second_word = partial_in_second.read();
        reduce_word_t merged_word;
        MERGE_DISTANCES: for (int32_t dist_idx = 0; (dist_idx < DISTANCES_PER_REDUCE_WORD); ++dist_idx) {
            #pragma HLS UNROLL
            ap_fixed_pod_t first_val = first_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH));
            ap_fixed_pod_t second_val = second_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH));
            ap_fixed_pod_t merged_val = ((second_val != 0u) ? static_cast<ap_fixed_pod_t>(((first_val < second_val) ? first_val : second_val)) : static_cast<ap_fixed_pod_t>(first_val));
            merged_word.range(((dist_idx * DISTANCE_BITWIDTH) + (DISTANCE_BITWIDTH - 1u)), (dist_idx * DISTANCE_BITWIDTH)) = merged_val;
        }
        one_write_burst.data = merged_word;
        kernel_out_stream.write(one_write_burst);
    }
}

extern "C" void graphyflow_little(const bus_word_t *edge_props, int32_t num_nodes, int32_t num_edges, int32_t dst_num, int32_t memory_offset, hls::stream<ppb_request_pkt_t> &ppb_req_stream, hls::stream<ppb_response_pkt_t> &ppb_resp_stream, hls::stream<little_out_pkt_t> &kernel_out_stream) {
    #pragma HLS INTERFACE m_axi port = edge_props offset = slave bundle = gmem0
    #pragma HLS INTERFACE s_axilite port = edge_props
    #pragma HLS INTERFACE s_axilite port = num_nodes
    #pragma HLS INTERFACE s_axilite port = num_edges
    #pragma HLS INTERFACE s_axilite port = dst_num
    #pragma HLS INTERFACE s_axilite port = return
    #pragma HLS DATAFLOW
    hls::stream<edge_descriptor_batch_t> edge_stream;
    #pragma HLS STREAM variable = edge_stream depth = 8
    hls::stream<update_tuple_t_little> stream_edge_data;
    #pragma HLS STREAM variable = stream_edge_data depth = 8
    hls::stream<reduce_word_t> pe_mem_outs_1[4];
    #pragma HLS STREAM variable = pe_mem_outs_1 depth = 8
    hls::stream<reduce_word_t> pe_mem_outs_2[4];
    #pragma HLS STREAM variable = pe_mem_outs_2 depth = 8
    hls::stream<reduce_word_t> pe_mem_out_partial_1;
    #pragma HLS STREAM variable = pe_mem_out_partial_1 depth = 8
    hls::stream<reduce_word_t> pe_mem_out_partial_2;
    #pragma HLS STREAM variable = pe_mem_out_partial_2 depth = 8
    hls::stream<ppb_request_t> ppb_req_stream_internal;
    #pragma HLS STREAM variable = ppb_req_stream_internal depth = 8
    hls::stream<ppb_response_t> ppb_resp_stream_internal;
    // Prefetch can cover two rounds (R and R+1). Each round returns
    // SRC_BUFFER_WORDS responses, so size the FIFO to avoid backpressure
    // deadlocks when request_manager temporarily stalls.
    #pragma HLS STREAM variable = ppb_resp_stream_internal depth = 8
    int32_t edges_per_word = EDGES_PER_WORD;
    int32_t num_wide_reads = (num_edges / edges_per_word);
    uint32_t total_edge_sets = static_cast<uint32_t>(num_wide_reads);
    uint32_t num_words = ((dst_num + (DISTANCES_PER_REDUCE_WORD - 1)) >> LOG_DISTANCES_PER_REDUCE_WORD);
    uint32_t rounded_num_words = ((num_words + 7) & (~7));
    load_edges(edge_props, num_edges, edge_stream);
    stream2axistream(ppb_req_stream_internal, ppb_req_stream);
    axistream2stream(ppb_resp_stream, ppb_resp_stream_internal);
    request_manager(edge_stream, ppb_req_stream_internal, ppb_resp_stream_internal, stream_edge_data, memory_offset, total_edge_sets);
    Reduc_105_unit_reduce(stream_edge_data, pe_mem_outs_1, pe_mem_outs_2, total_edge_sets, rounded_num_words);
    ap_fixed_pod_t identity_pod = INFINITY_POD;
    Reduc_105_partial_drain_impl(0, pe_mem_outs_1, pe_mem_out_partial_1, rounded_num_words, identity_pod);
    Reduc_105_partial_drain_impl(1, pe_mem_outs_2, pe_mem_out_partial_2, rounded_num_words, identity_pod);
    Reduc_105_finalize_drain(pe_mem_out_partial_1, pe_mem_out_partial_2, kernel_out_stream, rounded_num_words, identity_pod);
}