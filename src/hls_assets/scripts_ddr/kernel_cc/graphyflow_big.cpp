#include "graphyflow_big.h"


template <typename T1, typename T2>
void stream2axistream(hls::stream<T1> &stream, hls::stream<T2> &axi_stream) {

stream2axistream:
    while (true) {

        T1 tmp_t1 = stream.read();

        T2 tmp_t2;
        tmp_t2.data = tmp_t1.idx;
        tmp_t2.dest = tmp_t1.dst;
        tmp_t2.last = tmp_t1.end_flag;

        axi_stream.write(tmp_t2);

        if (tmp_t1.end_flag)
            break;
    }
}



template <typename T1, typename T2>
void axistream2stream(hls::stream<T1> &axi_stream, hls::stream<T2> &stream) {

axistream2stream:
    while (true) {

        T1 tmp_t1 = axi_stream.read();

        T2 tmp_t2;
        tmp_t2.data = tmp_t1.data;
        tmp_t2.dst = tmp_t1.dest;
        tmp_t2.end_flag = tmp_t1.last;

        stream.write(tmp_t2);
        if (tmp_t2.end_flag)
            break;
    }
}



static ap_uint<4> count_end_ones(ap_uint<PE_NUM> valid_mask) {
#pragma HLS INLINE
    ap_uint<4> count = 0;
    switch (valid_mask) {
    case 0:
        count = 0;
        break;
    case 1:
        count = 1;
        break;
    case 3:
        count = 2;
        break;
    case 7:
        count = 3;
        break;
    case 15:
        count = 4;
        break;
    case 31:
        count = 5;
        break;
    case 63:
        count = 6;
        break;
    case 127:
        count = 7;
        break;
    case 255:
        count = 8;
        break;
    default:
        break;
    }
    return count;
}


// --- 1. scatter_funcs ---
static void 
dist_req_packer(hls::stream<node_id_burst_t> &src_id_burst_stream,
 hls::stream<distance_req_pack_t> &distance_req_pack_stream,
 uint32_t total_edge_sets) {
    ap_uint<26> last_idx_max = 0;
    
    LOOP_FOR_4:
    for (uint32_t edge_burst_idx = 0; edge_burst_idx < total_edge_sets; edge_burst_idx++) {
#pragma HLS PIPELINE II = 1
        node_id_burst_t node_id_burst = src_id_burst_stream.read();
        
        ap_uint<26> cache_idx[PE_NUM];
#pragma HLS ARRAY_PARTITION variable = cache_idx complete dim = 0
        
        
        LOOP_FOR_0:
        for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
            cache_idx[pe_idx] = node_id_burst.data[pe_idx].range(30,0) >> LOG_DIST_PER_WORD;
        }
        
        ap_uint<26> cache_idx_diffs[PE_NUM];
#pragma HLS ARRAY_PARTITION variable = cache_idx_diffs complete dim = 0
        
        LOOP_FOR_1:
        for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
            cache_idx_diffs[pe_idx] = cache_idx[pe_idx] - last_idx_max;
        }
        
        if (cache_idx_diffs[PE_NUM - 1]) {
            // if not all diffs are zero, send a req_pack
            ap_uint<PE_NUM> valid_mask;
            LOOP_FOR_2:
            for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
                if (cache_idx_diffs[pe_idx] == 0) {
                    valid_mask.range(pe_idx, pe_idx) = 1;
                } else {
                    valid_mask.range(pe_idx, pe_idx) = 0;
                }
            }
            
            ap_uint<4> num_unread = count_end_ones(valid_mask);
            
            distance_req_pack_t req_pack;
            req_pack.offset = num_unread;
            req_pack.end_flag = false;
            
            LOOP_FOR_3:
            for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
                req_pack.idx[pe_idx] = cache_idx[pe_idx];
            }
            distance_req_pack_stream.write(req_pack);
        }
        last_idx_max = cache_idx[PE_NUM - 1];
    }
    {
    distance_req_pack_t end_req_pack;
    end_req_pack.end_flag = true;
    end_req_pack.offset = 7;
    distance_req_pack_stream.write(end_req_pack);
    }
}

static void 
cacheline_req_sender(hls::stream<distance_req_pack_t> &distance_req_pack_stream,
 hls::stream<cacheline_req_t> &cacheline_req_stream,
 int32_t memory_offset) {
    {
        cacheline_req_t cache_req;
        cache_req.end_flag = false;
        cache_req.idx = memory_offset;
        cacheline_req_stream.write(cache_req);
    }
    
    ap_uint<26> cacheline_idx[PE_NUM];
#pragma HLS ARRAY_PARTITION variable = cacheline_idx complete dim = 0
    
    LOOP_SEND_CACHE_REQ:
    LOOP_WHILE_7:
    while (true) {
#pragma HLS PIPELINE II = 1
#pragma HLS dependence variable = cacheline_idx inter false
        
        distance_req_pack_t req_pack;
        req_pack = distance_req_pack_stream.read();
        // #pragma HLS ARRAY_PARTITION variable = req_pack.idx complete dim = 0
        LOOP_FOR_5:
        for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
            cacheline_idx[pe_idx] = req_pack.idx[pe_idx];
        }
        
        {
            LOOP_SEND_CACHE_REQ_INNER:
            LOOP_FOR_6:
            for (ap_uint<4> i = req_pack.offset; i < PE_NUM; i++) {
#pragma HLS PIPELINE II = 1 rewind
#pragma HLS unroll factor = 1
                cacheline_req_t cache_req;
                cache_req.idx = (cacheline_idx[i] + memory_offset);
                cache_req.dst = i;
                cache_req.end_flag = req_pack.end_flag;
                cacheline_req_stream.write(cache_req);
                // printf("Sent cacheline req for idx %d to PE %d\n",
                // // (int)cache_req.idx, (int)cache_req.target_pe); fflush(NULL);
            }
        }
        
        if (req_pack.end_flag) {
            break;
        }
    }
    
    // cache_req.last = true;
    // cacheline_req_stream.write(cache_req);
}

static void 
node_prop_resp_receiver(hls::stream<cacheline_resp_t> &cacheline_resp_stream,
 hls::stream<bus_word_t> (&cacheline_streams)[PE_NUM]) {
    cacheline_resp_t cache_resp = cacheline_resp_stream.read();
    bus_word_t first_line = cache_resp.data;
    LOOP_FOR_8:
    for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
        cacheline_streams[pe_idx].write(first_line);
    }
    
    // LOOP_RECEIVE_CACHE_RESP:
    LOOP_WHILE_9:
    while (true) {
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
    // End of node_prop_resp_receiver Function
}

static void 
merge_node_props(hls::stream<bus_word_t> (&cacheline_streams)[PE_NUM],
 hls::stream<edge_descriptor_batch_t> &edge_stream,
 hls::stream<update_tuple_t_big> &edge_batch_stream,
 uint32_t total_edge_sets) {
    bus_word_t last_cacheline[PE_NUM] = {0};
#pragma HLS ARRAY_PARTITION variable = last_cacheline complete dim = 0
    ap_uint<26> last_cache_idx[PE_NUM] = {0};
#pragma HLS ARRAY_PARTITION variable = last_cache_idx complete dim = 0
    
    // Init first cacheline for each PE
    LOOP_INIT_CACHELINE:
    LOOP_FOR_10:
    for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
        last_cacheline[pe_idx] = cacheline_streams[pe_idx].read();
        last_cache_idx[pe_idx] = 0x0;
    }
    
    // CC: edge weight unused (bitmask propagation)
    
    LOOP_SCATTER_EDGES:
    LOOP_FOR_13:
    for (int32_t edge_batch_idx = 0; edge_batch_idx < total_edge_sets; edge_batch_idx++) {
#pragma HLS PIPELINE II = 1
        edge_descriptor_batch_t edge_batch;
        edge_batch = edge_stream.read();
        
        update_tuple_t_big out_batch;
        
        LOOP_FOR_11:
        for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
            ap_uint<26> cacheline_idx = edge_batch.edges[pe_idx].src_id.range(29, 4);
            ap_uint<4> offset = edge_batch.edges[pe_idx].src_id.range(3, 0);
            bus_word_t cacheline;
            if (cacheline_idx == last_cache_idx[pe_idx]) {
                cacheline = last_cacheline[pe_idx];
            } else {
                cacheline = cacheline_streams[pe_idx].read();
            }
            
            ap_fixed_pod_t prop = cacheline.range(31 + ((ap_uint<9>)offset << 5), ((ap_uint<9>)offset << 5));
            
            // CC: propagate bitmask
            out_batch.data[pe_idx].prop = prop;
            out_batch.data[pe_idx].node_id = edge_batch.edges[pe_idx].dst_id;
            out_batch.data[pe_idx].end_flag = 0;
            
            if (pe_idx == (PE_NUM - 1)) {
                last_cacheline[pe_idx] = cacheline;
                last_cache_idx[pe_idx] = cacheline_idx;
            }
        }
        edge_batch_stream.write(out_batch);
        
        LOOP_FOR_12:
        for (int32_t pe_idx = 0; pe_idx < (PE_NUM - 1); pe_idx++) {
#pragma HLS UNROLL
            last_cacheline[pe_idx] = last_cacheline[PE_NUM - 1];
            last_cache_idx[pe_idx] = last_cache_idx[PE_NUM - 1];
        }
    }
}

// --- 2. gather_funcs ---
static void 
demux_1(hls::stream<update_tuple_t_big> &in_batch_stream,
 hls::stream<update_t_big> (&out_streams)[8],
 uint32_t total_edge_sets) {
    LOOP_FOR_19:
    for (uint32_t batch_idx = 0; batch_idx < total_edge_sets; batch_idx++) {
#pragma HLS PIPELINE II = 1
        update_tuple_t_big in_batch;
        in_batch = in_batch_stream.read();
        
        LOOP_FOR_18:
        for (uint32_t i = 0; i < PE_NUM; i++) {
#pragma HLS UNROLL
            if (in_batch.data[i].node_id.range(LOCAL_ID_MSB, LOCAL_ID_MSB) == 0) {
                out_streams[i].write(in_batch.data[i]);
            }
        }
    }
    // Propagate end_flag to all output streams
    LOOP_FOR_20:
    for (uint32_t i = 0; i < 8; i++) {
#pragma HLS UNROLL
        update_t_big end_wrapper;
        end_wrapper.end_flag = true;
        out_streams[i].write(end_wrapper);
    }
}

static void 
sender_2(int32_t i,
 hls::stream<update_t_big> &in1,
 hls::stream<update_t_big> &in2,
 hls::stream<update_t_big> &out1,
 hls::stream<update_t_big> &out2,
 hls::stream<update_t_big> &out3,
 hls::stream<update_t_big> &out4) {
#pragma HLS function_instantiate variable = i
    bool in1_end_flag = false;
    bool in2_end_flag = false;
    LOOP_WHILE_21:
    while (true) {
#pragma HLS PIPELINE II = 1
        if ((!in1.empty())) {
            update_t_big data1;
            data1 = in1.read();
            if ((!data1.end_flag)) {
                if (((data1.node_id >> i) & 1)) {
                    out2.write(data1);
                } else {
                    out1.write(data1);
                }
            } else {
                in1_end_flag = true;
            }
        }
        if ((!in2.empty())) {
            update_t_big data2;
            data2 = in2.read();
            if ((!data2.end_flag)) {
                if (((data2.node_id >> i) & 1)) {
                    out4.write(data2);
                } else {
                    out3.write(data2);
                }
            } else {
                in2_end_flag = true;
            }
        }
        if ((in1_end_flag & in2_end_flag)) {
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

static void 
receiver_2(int32_t i,
 hls::stream<update_t_big> &out1,
 hls::stream<update_t_big> &out2,
 hls::stream<update_t_big> &in1,
 hls::stream<update_t_big> &in2,
 hls::stream<update_t_big> &in3,
 hls::stream<update_t_big> &in4) {
#pragma HLS function_instantiate variable = i
    bool in1_end_flag = false;
    bool in2_end_flag = false;
    bool in3_end_flag = false;
    bool in4_end_flag = false;
    LOOP_WHILE_22:
    while (true) {
#pragma HLS PIPELINE II = 1
        if ((!in1.empty())) {
            update_t_big data;
            data = in1.read();
            if ((!data.end_flag)) {
                out1.write(data);
            } else {
                in1_end_flag = true;
            }
        } else if ((!in3.empty())) {
            update_t_big data;
            data = in3.read();
            if ((!data.end_flag)) {
                out1.write(data);
            } else {
                in3_end_flag = true;
            }
        }
        if ((!in2.empty())) {
            update_t_big data;
            data = in2.read();
            if ((!data.end_flag)) {
                out2.write(data);
            } else {
                in2_end_flag = true;
            }
        } else if ((!in4.empty())) {
            update_t_big data;
            data = in4.read();
            if ((!data.end_flag)) {
                out2.write(data);
            } else {
                in4_end_flag = true;
            }
        }
        if ((((in1_end_flag & in2_end_flag) & in3_end_flag) & in4_end_flag)) {
            update_t_big data;
            data.end_flag = true;
            out1.write(data);
            out2.write(data);
            break;
        }
    }
}

static void 
switch2x2_2(int32_t i,
 hls::stream<update_t_big> &in1,
 hls::stream<update_t_big> &in2,
 hls::stream<update_t_big> &out1,
 hls::stream<update_t_big> &out2) {
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

static void 
Reduc_105_unit_reduce_single_pe(hls::stream<update_t_big> &kt_wrap_item_single,
 hls::stream<reduce_word_t> &pe_mem_out,
 uint32_t num_word_per_pe) {
    // --- Phase 1: Memory Declaration ---
    const int32_t MEM_SIZE = ((MAX_NUM >> LOG_PE_NUM) / DISTANCES_PER_REDUCE_WORD);
    reduce_word_t prop_mem[MEM_SIZE];
#pragma HLS BIND_STORAGE variable = prop_mem type = RAM_2P impl = URAM
#pragma HLS dependence variable = prop_mem inter false
    
    // Latency-hiding cache for recently accessed URAM words
    reduce_word_t cache_data_buffer[(L+1 )];
#pragma HLS ARRAY_PARTITION variable = cache_data_buffer complete dim = 0
    local_id_t cache_addr_buffer[(L+1)];
#pragma HLS ARRAY_PARTITION variable = cache_addr_buffer complete dim = 0
    
    #ifdef EMULATION
        memset(prop_mem, 0, sizeof(reduce_word_t) * MEM_SIZE);
    #endif
    
    LOOP_INIT_CACHE_ADDR:
    LOOP_FOR_23:
    for (int32_t i = 0; i < (L + 1); i++) {
#pragma HLS UNROLL
        cache_addr_buffer[i] = 0x0;
        // Invalidate cache
        cache_data_buffer[i] = 0;
    }
    
    // --- Phase 3: Aggregation Loop ---
    LOOP_AGGREGATE:
    LOOP_WHILE_26:
    while (true) {
#pragma HLS PIPELINE II = 1
        update_t_big kt_elem;
        kt_elem = kt_wrap_item_single.read();
        if (kt_elem.end_flag) {
            break;
        }
        local_id_t key = (kt_elem.node_id >> LOG_PE_NUM);
        ap_fixed_pod_t incoming_dist_pod = kt_elem.prop;
        
        local_id_t word_addr = (key >> 1);
        
        reduce_word_t current_word = prop_mem[word_addr];
        
        // Check cache first
        // for (int i = L; i >= 0; --i) {
        LOOP_FOR_24:
        for (int32_t i = 0; i < (L + 1); i++) {
#pragma HLS UNROLL
            if (cache_addr_buffer[i] == word_addr) {
                current_word = cache_data_buffer[i];
                // break;
            }
        }
        
        // Shift cache
        LOOP_FOR_25:
        for (int32_t i = 0; i < L; i++) {
#pragma HLS UNROLL
            cache_addr_buffer[i] = cache_addr_buffer[i + 1];
            cache_data_buffer[i] = cache_data_buffer[i + 1];
        }
        
        reduce_word_t tmp_cur_word = current_word;
        
        ap_fixed_pod_t msb = tmp_cur_word.range(63, 32);
        ap_fixed_pod_t lsb = tmp_cur_word.range(31, 0);
        
        ap_fixed_pod_t msb_out;
        ap_fixed_pod_t lsb_out;
        
        // CC: OR-reduction
        msb_out = (msb | incoming_dist_pod);
        lsb_out = (lsb | incoming_dist_pod);
        reduce_word_t accumulated_msb;
        reduce_word_t accumulated_lsb;
        
        accumulated_msb.range(63, 32) = msb_out;
        accumulated_msb.range(31, 0) = tmp_cur_word.range(31, 0);
        
        accumulated_lsb.range(63, 32) = tmp_cur_word.range(63, 32);
        accumulated_lsb.range(31, 0) = lsb_out;
        
        if ((key & 0x01)) {
            prop_mem[word_addr] = accumulated_msb;
            cache_data_buffer[L] = accumulated_msb;
        } else {
            prop_mem[word_addr] = accumulated_lsb;
            cache_data_buffer[L] = accumulated_lsb;
        }
        cache_addr_buffer[L] = word_addr;
    }
    
    // --- Phase 4: Stream out aggregated memory ---
    LOOP_STREAM_OUT:
    LOOP_FOR_27:
    for (int32_t i = 0; i < num_word_per_pe; i++) {
#pragma HLS UNROLL factor = 1
        reduce_word_t tmp_word = prop_mem[i];
        prop_mem[i] = 0;
        pe_mem_out.write(tmp_word);
    }
}

static void 
Reduc_105_partial_drain_four(hls::stream<reduce_word_t> (&pe_mem_in)[PE_NUM],
 uint32_t base_idx,
 uint32_t num_word_per_pe,
 hls::stream<ap_uint<256>> &partial_out_stream) {
#pragma HLS function_instantiate variable = base_idx
    LOOP_PARTIAL_DRAIN:
    LOOP_FOR_29:
    for (uint32_t word_idx = 0; word_idx < num_word_per_pe; word_idx++) {
#pragma HLS PIPELINE II = 1
        ap_uint<256> packed_out = 0;
        LOOP_FOR_28:
        for (uint32_t pe_offset = 0; pe_offset < 4; pe_offset++) {
#pragma HLS UNROLL
            reduce_word_t tmp_word = pe_mem_in[base_idx + pe_offset].read();
            uint32_t bit_low = (pe_offset << 5);
            packed_out.range(31 + bit_low, bit_low) = tmp_word.range(31, 0);
            packed_out.range(31 + bit_low + 128, bit_low + 128) = tmp_word.range(63, 32);
        }
        partial_out_stream.write(packed_out);
    }
}

static void 
Reduc_105_finalize_drain(hls::stream<ap_uint<256>> &lower_pe_pack_stream,
 hls::stream<ap_uint<256>> &upper_pe_pack_stream,
 uint32_t num_word_per_pe,
 hls::stream<write_burst_pkt_t> &kernel_out_stream) {
    LOOP_FINALIZE_DRAIN:
    LOOP_FOR_30:
    for (uint32_t word_idx = 0; word_idx < num_word_per_pe; word_idx++) {
#pragma HLS PIPELINE II = 1
        ap_uint<256> lower_pe_pack = lower_pe_pack_stream.read();
        ap_uint<256> upper_pe_pack = upper_pe_pack_stream.read();
        write_burst_pkt_t one_write_burst;
        one_write_burst.data.range(127, 0) = lower_pe_pack.range(127, 0);
        one_write_burst.data.range(255, 128) = upper_pe_pack.range(127, 0);
        one_write_burst.data.range(383, 256) = lower_pe_pack.range(255, 128);
        one_write_burst.data.range(511, 384) = upper_pe_pack.range(255, 128);
        kernel_out_stream.write(one_write_burst);
    }
}

// --- 4. top func ---
extern "C" void
 graphyflow_big(const bus_word_t* edge_props,
 int32_t num_nodes,
 int32_t num_edges,
 int32_t dst_num,
 int32_t memory_offset,
 hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,
 hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream,
 hls::stream<write_burst_pkt_t> &kernel_out_stream) {
#pragma HLS INTERFACE m_axi port = edge_props offset = slave bundle = gmem0
#pragma HLS INTERFACE s_axilite port = edge_props
#pragma HLS INTERFACE s_axilite port = num_nodes
#pragma HLS INTERFACE s_axilite port = num_edges
#pragma HLS INTERFACE s_axilite port = dst_num
#pragma HLS INTERFACE s_axilite port = memory_offset
#pragma HLS INTERFACE s_axilite port = return
#pragma HLS DATAFLOW
    
    // Streams for the new COO-style property loading
    hls::stream<node_id_burst_t> stream_src_ids;
#pragma HLS STREAM variable = stream_src_ids depth = 16
    hls::stream<distance_req_pack_t> stream_dist_req;
#pragma HLS STREAM variable = stream_dist_req depth = 16
    hls::stream<bus_word_t> stream_cachelines[PE_NUM];
#pragma HLS STREAM variable = stream_cachelines depth = 8
    hls::stream<edge_descriptor_batch_t> edge_stream;
#pragma HLS STREAM variable = edge_stream depth = 16
    hls::stream<update_tuple_t_big> stream_edge_data;
#pragma HLS STREAM variable = stream_edge_data depth = 16
    hls::stream<cacheline_req_t> cacheline_req;
#pragma HLS STREAM variable = cacheline_req depth = 32
    hls::stream<cacheline_resp_t> cacheline_resp;
#pragma HLS STREAM variable = cacheline_resp depth = 32
    hls::stream<update_t_big> reduce_105_d2o_pair[8];
#pragma HLS STREAM variable = reduce_105_d2o_pair depth = 8
#pragma HLS ARRAY_PARTITION variable = reduce_105_d2o_pair complete dim = 0
    hls::stream<update_t_big> reduce_105_o2u_pair[8];
#pragma HLS STREAM variable = reduce_105_o2u_pair depth = 2
#pragma HLS ARRAY_PARTITION variable = reduce_105_o2u_pair complete dim = 0
    
    const uint32_t num_words = ((dst_num + 1) / DISTANCES_PER_REDUCE_WORD);
    const uint32_t num_word_per_pe = ((num_words + PE_NUM - 1) >> LOG_PE_NUM);
    
    // --- Data Loading ---
    const int32_t edges_per_word = (AXI_BUS_WIDTH / (NODE_ID_BITWIDTH + NODE_ID_BITWIDTH));
    const int32_t num_wide_reads = (num_edges / edges_per_word);
    const uint32_t total_edge_sets = (num_edges >> LOG_PE_NUM);
    
    LOOP_EDL_READ:
    LOOP_FOR_47:
    for (int32_t i = 0; i < num_wide_reads; i++) {
#pragma HLS PIPELINE II = 1
        bus_word_t wide_word = edge_props[i];
        edge_descriptor_batch_t edge_batch;
        
        LOOP_EDL_UNPACK:
        LOOP_FOR_45:
        for (int32_t j = 0; j < edges_per_word; j++) {
#pragma HLS UNROLL
            ap_uint<64> packed_edge = wide_word.range(63 + (j << 6), (j << 6));
            edge_t edge;
            edge.dst_id = packed_edge.range(19, 0);
            edge.src_id = packed_edge.range(63, 32);
            edge_batch.edges[j] = edge;
        }
        edge_stream.write(edge_batch);
        
        node_id_burst_t src_id_burst;
        LOOP_FOR_46:
        for (int32_t j = 0; j < edges_per_word; j++) {
#pragma HLS UNROLL
            node_id_t src_id;
            src_id = edge_batch.edges[j].src_id;
            src_id_burst.data[j] = src_id;
        }
        stream_src_ids.write(src_id_burst);
    }
    
    // --- New COO-style Source Property Loading Pipeline ---
    dist_req_packer(stream_src_ids, stream_dist_req, total_edge_sets);
    cacheline_req_sender(stream_dist_req, cacheline_req, memory_offset);
    stream2axistream(cacheline_req, cacheline_req_stream);
    axistream2stream(cacheline_resp_stream, cacheline_resp);
    node_prop_resp_receiver(cacheline_resp, stream_cachelines);
    merge_node_props(stream_cachelines, edge_stream, stream_edge_data, total_edge_sets);
    
    demux_1(stream_edge_data, reduce_105_d2o_pair, total_edge_sets);
    
    hls::stream<update_t_big> stream_stage_0[8];
#pragma HLS STREAM variable = stream_stage_0 depth = 2
#pragma HLS ARRAY_PARTITION variable = stream_stage_0 complete dim = 0
    hls::stream<update_t_big> stream_stage_1[8];
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
    // Reduc_105_unit_reduce(reduce_105_o2u_pair, stream_o_0_107, dst_num);
    hls::stream<reduce_word_t> pe_mem_out_streams[PE_NUM];
#pragma HLS STREAM variable = pe_mem_out_streams depth = 4
    hls::stream<ap_uint<256>> drain_lower_stream;
#pragma HLS STREAM variable = drain_lower_stream depth = 4
    hls::stream<ap_uint<256>> drain_upper_stream;
#pragma HLS STREAM variable = drain_upper_stream depth = 4
    LOOP_FOR_48:
    for (int32_t pe_idx = 0; pe_idx < PE_NUM; pe_idx++) {
#pragma HLS UNROLL
        Reduc_105_unit_reduce_single_pe(reduce_105_o2u_pair[pe_idx], pe_mem_out_streams[pe_idx], num_word_per_pe);
    }
    Reduc_105_partial_drain_four(pe_mem_out_streams, 0, num_word_per_pe, drain_lower_stream);
    Reduc_105_partial_drain_four(pe_mem_out_streams, 4, num_word_per_pe, drain_upper_stream);
    Reduc_105_finalize_drain(drain_lower_stream, drain_upper_stream, num_word_per_pe, kernel_out_stream);
}
