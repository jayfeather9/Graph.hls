use std::fmt::Write;

use super::{HlsEdgeConfig, HlsNodeConfig};

const AXI_BUS_WIDTH: u32 = 512;
const REDUCE_MEM_WIDTH: u32 = 64;
const BIG_HEADER_GUARD: &str = "__GRAPHYFLOW_GRAPHYFLOW_BIG_H__";
const LITTLE_HEADER_GUARD: &str = "__GRAPHYFLOW_GRAPHYFLOW_LITTLE_H__";

pub fn render_graphyflow_big_header(
    edge: &HlsEdgeConfig,
    node: &HlsNodeConfig,
    max_dst_big: u32,
) -> String {
    let mut out = String::new();
    write_header_prelude(
        &mut out,
        BIG_HEADER_GUARD,
        edge.big_pe,
        edge.big_log_pe,
        Some(("MAX_DST_BIG", max_dst_big)),
    );
    writeln!(out, "#define L 3").unwrap();
    out.push('\n');

    write_bitwidth_section(&mut out, edge, node, "INVALID_LOCAL_ID_BIG");
    write_big_packet_types(&mut out);
    write_fixed_point_section(&mut out);
    write_big_structs(&mut out);
    write_big_prototype(&mut out);

    writeln!(out, "#endif // {BIG_HEADER_GUARD}").unwrap();
    out
}

pub fn render_graphyflow_little_header(
    edge: &HlsEdgeConfig,
    node: &HlsNodeConfig,
    max_dst_little: u32,
) -> String {
    let mut out = String::new();
    write_header_prelude(
        &mut out,
        LITTLE_HEADER_GUARD,
        edge.little_pe,
        3,
        Some(("MAX_DST_LITTLE", max_dst_little)),
    );
    writeln!(out, "#define L 3").unwrap();
    writeln!(out, "#define SRC_BUFFER_SIZE 4096").unwrap();
    writeln!(out, "#define LOG_SRC_BUFFER_SIZE 12").unwrap();
    out.push('\n');

    write_bitwidth_section(&mut out, edge, node, "INVALID_LOCAL_ID_LITTLE");
    write_little_packet_types(&mut out);
    write_fixed_point_section(&mut out);
    write_little_structs(&mut out);
    write_little_prototype(&mut out);

    writeln!(out, "#endif // {LITTLE_HEADER_GUARD}").unwrap();
    out
}

fn write_header_prelude(
    out: &mut String,
    guard: &str,
    pe_num: u32,
    log_pe_num: u32,
    max_dst: Option<(&str, u32)>,
) {
    writeln!(out, "#ifndef {guard}").unwrap();
    writeln!(out, "#define {guard}").unwrap();
    out.push('\n');

    for header in [
        "ap_axi_sdata.h",
        "ap_fixed.h",
        "ap_int.h",
        "hls_stream.h",
        "stdint.h",
        "stdio.h",
        "string.h",
    ] {
        writeln!(out, "#include <{header}>").unwrap();
    }
    out.push('\n');

    writeln!(out, "#define PE_NUM {pe_num}").unwrap();
    writeln!(out, "#define DBL_PE_NUM 16").unwrap();
    writeln!(out, "#define LOG_PE_NUM {log_pe_num}").unwrap();
    if let Some((macro_name, max_dst_value)) = max_dst {
        writeln!(out, "#ifdef GRAPHYFLOW_HW_EMU_LIMIT_MAX_DST").unwrap();
        writeln!(out, "#define {macro_name} 512").unwrap();
        writeln!(out, "#else").unwrap();
        writeln!(out, "#define {macro_name} {max_dst_value}").unwrap();
        writeln!(out, "#endif").unwrap();
        writeln!(out, "#define MAX_NUM {macro_name}").unwrap();
    }
    out.push('\n');
}

fn write_bitwidth_section(
    out: &mut String,
    edge: &HlsEdgeConfig,
    node: &HlsNodeConfig,
    invalid_macro_name: &str,
) {
    writeln!(out, "#define LOCAL_ID_BITWIDTH {}", edge.local_id_bits).unwrap();
    writeln!(out, "#define LOCAL_ID_MSB (LOCAL_ID_BITWIDTH - 1)").unwrap();
    writeln!(out, "typedef ap_uint<LOCAL_ID_BITWIDTH> local_id_t;").unwrap();
    writeln!(
        out,
        "#define {invalid_macro_name} (local_id_t(1) << LOCAL_ID_MSB)"
    )
    .unwrap();
    out.push('\n');

    writeln!(out, "// --- New Bitwidth Definitions for HLS Synthesis ---").unwrap();
    writeln!(out, "#define NODE_ID_BITWIDTH 32").unwrap();
    writeln!(out, "#define DISTANCE_BITWIDTH {}", node.node_prop_bits).unwrap();
    writeln!(
        out,
        "#define DISTANCE_INTEGER_PART {}",
        node.node_prop_int_bits
    )
    .unwrap();
    writeln!(
        out,
        "#define DISTANCE_SIGNED {}",
        if node.node_prop_signed { 1 } else { 0 }
    )
    .unwrap();
    writeln!(out, "#define WEIGHT_BITWIDTH DISTANCE_BITWIDTH").unwrap();
    writeln!(out, "#define WEIGHT_INTEGER_PART DISTANCE_INTEGER_PART").unwrap();
    writeln!(out, "#define OUT_END_MARKER_BITWIDTH 4").unwrap();
    writeln!(out, "#define DIST_PER_WORD {}", node.dist_per_word).unwrap();
    writeln!(out, "#define LOG_DIST_PER_WORD {}", node.log_dist_per_word).unwrap();
    out.push('\n');

    writeln!(out, "// --- New Memory Word and Bus Definitions ---").unwrap();
    writeln!(out, "#define AXI_BUS_WIDTH {AXI_BUS_WIDTH}").unwrap();
    writeln!(out, "#define EDGE_PROP_BITS {}", edge.edge_prop_bits).unwrap();
    writeln!(
        out,
        "#define EDGE_PROP_STORAGE_BITS ((EDGE_PROP_BITS == 0) ? 1 : EDGE_PROP_BITS)"
    )
    .unwrap();
    writeln!(out, "#define EDGE_WEIGHT_BITS {}", edge.edge_weight_bits).unwrap();
    writeln!(out, "#define EDGE_WEIGHT_LSB {}", edge.edge_weight_lsb).unwrap();
    writeln!(out, "#define EDGE_SRC_PAYLOAD_LSB NODE_ID_BITWIDTH").unwrap();
    writeln!(
        out,
        "#define EDGE_SRC_PAYLOAD_MSB ((2 * NODE_ID_BITWIDTH) - 1)"
    )
    .unwrap();
    writeln!(
        out,
        "#define EDGE_PROP_PAYLOAD_LSB {}",
        edge.edge_prop_payload_lsb()
    )
    .unwrap();
    writeln!(
        out,
        "#define EDGE_PROP_PAYLOAD_MSB (EDGE_PROP_PAYLOAD_LSB + EDGE_PROP_BITS - 1)"
    )
    .unwrap();
    writeln!(out, "#define EDGE_PAYLOAD_BITS {}", edge.payload_bits()).unwrap();
    writeln!(out, "#define EDGES_PER_WORD {}", edge.edges_per_word).unwrap();
    out.push('\n');

    writeln!(out, "#define REDUCE_MEM_WIDTH {REDUCE_MEM_WIDTH}").unwrap();
    writeln!(out, "typedef ap_uint<AXI_BUS_WIDTH> bus_word_t;").unwrap();
    writeln!(out, "typedef ap_uint<REDUCE_MEM_WIDTH> reduce_word_t;").unwrap();
    out.push('\n');

    writeln!(out, "// --- New Packing-related Constants ---").unwrap();
    writeln!(
        out,
        "// Number of distances that can be packed into a single reduce memory word."
    )
    .unwrap();
    writeln!(
        out,
        "#define DISTANCES_PER_REDUCE_WORD {}",
        node.distances_per_reduce_word
    )
    .unwrap();
    writeln!(
        out,
        "#define LOG_DISTANCES_PER_REDUCE_WORD                                      \\"
    )
    .unwrap();
    writeln!(
        out,
        "    ((LOG_DIST_PER_WORD > 3) ? (LOG_DIST_PER_WORD - 3) : 0)"
    )
    .unwrap();
    out.push('\n');

    writeln!(out, "// --- Redefinition of Core Graph Types for HLS ---").unwrap();
    writeln!(
        out,
        "// These typedefs override the standard integer types from common.h for"
    )
    .unwrap();
    writeln!(out, "// synthesis.").unwrap();
    writeln!(out, "typedef ap_uint<NODE_ID_BITWIDTH> node_id_t;").unwrap();
    writeln!(
        out,
        "typedef ap_uint<32> edge_id_t; // edge_id_t is not customized yet, keep as is."
    )
    .unwrap();
    writeln!(out, "#if DISTANCE_SIGNED").unwrap();
    writeln!(out, "typedef ap_int<DISTANCE_BITWIDTH>").unwrap();
    writeln!(
        out,
        "    ap_fixed_pod_t; // Used to hold bit representation of ap_fixed types"
    )
    .unwrap();
    writeln!(out, "#else").unwrap();
    writeln!(out, "typedef ap_uint<DISTANCE_BITWIDTH>").unwrap();
    writeln!(
        out,
        "    ap_fixed_pod_t; // Used to hold bit representation of ap_fixed types"
    )
    .unwrap();
    writeln!(out, "#endif").unwrap();
    writeln!(
        out,
        "typedef ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART> distance_t;"
    )
    .unwrap();
    writeln!(
        out,
        "typedef ap_uint<OUT_END_MARKER_BITWIDTH> out_end_marker_t;"
    )
    .unwrap();
    writeln!(out, "typedef ap_uint<EDGE_PROP_STORAGE_BITS> edge_prop_t;").unwrap();
}

fn write_big_packet_types(out: &mut String) {
    writeln!(out, "typedef ap_axiu<256, 0, 0, 0> node_dist_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<512, 0, 0, 0> write_burst_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<32, 0, 0, 8> cacheline_request_pkt_t;").unwrap();
    writeln!(
        out,
        "typedef ap_axiu<512, 0, 0, 8> cacheline_response_pkt_t;"
    )
    .unwrap();
    writeln!(out, "typedef ap_axiu<512, 0, 0, 0> cacheline_data_pkt_t;").unwrap();
    out.push('\n');
}

fn write_little_packet_types(out: &mut String) {
    writeln!(out, "typedef ap_axiu<256, 0, 0, 0> node_dist_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<512, 0, 0, 0> write_burst_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<64, 0, 0, 0> little_out_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<32, 0, 0, 0> ppb_request_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<512, 0, 0, 32> ppb_response_pkt_t;").unwrap();
    writeln!(out, "typedef ap_axiu<512, 0, 0, 0> cacheline_data_pkt_t;").unwrap();
    out.push('\n');
}

fn write_fixed_point_section(out: &mut String) {
    writeln!(out, "#if DISTANCE_SIGNED").unwrap();
    writeln!(out, "const ap_fixed_pod_t INFINITY_POD =").unwrap();
    writeln!(
        out,
        "    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1)) - 1;"
    )
    .unwrap();
    writeln!(out, "const ap_fixed_pod_t NEG_INFINITY_POD =").unwrap();
    writeln!(out, "    (ap_fixed_pod_t(1) << (DISTANCE_BITWIDTH - 1));").unwrap();
    writeln!(out, "#else").unwrap();
    writeln!(
        out,
        "const ap_fixed_pod_t INFINITY_POD = ~ap_fixed_pod_t(0);"
    )
    .unwrap();
    writeln!(out, "const ap_fixed_pod_t NEG_INFINITY_POD = 0;").unwrap();
    writeln!(out, "#endif").unwrap();
    writeln!(
        out,
        "const distance_t INFINITY_DIST_VAL = static_cast<distance_t>(INFINITY_POD);"
    )
    .unwrap();
    writeln!(out, "// --- Struct Type Definitions ---").unwrap();
}

fn write_big_structs(out: &mut String) {
    writeln!(out, "struct __attribute__((packed)) node_id_burst_t {{").unwrap();
    writeln!(out, "    node_id_t data[PE_NUM];").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) distance_req_pack_t {{").unwrap();
    writeln!(
        out,
        "    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> idx[PE_NUM];"
    )
    .unwrap();
    writeln!(out, "    ap_uint<4> offset;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) cacheline_resp_t {{").unwrap();
    writeln!(out, "    bus_word_t data;").unwrap();
    writeln!(out, "    ap_uint<8> dst;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) edge_t {{").unwrap();
    writeln!(out, "    node_id_t src_id;").unwrap();
    writeln!(out, "    local_id_t dst_id;").unwrap();
    writeln!(out, "#if EDGE_PROP_BITS > 0").unwrap();
    writeln!(out, "    edge_prop_t edge_prop;").unwrap();
    writeln!(out, "#endif").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) update_t_big {{").unwrap();
    writeln!(out, "    local_id_t node_id;").unwrap();
    writeln!(out, "    ap_fixed_pod_t prop;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) cacheline_req_t {{").unwrap();
    writeln!(
        out,
        "    ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD> idx;"
    )
    .unwrap();
    writeln!(out, "    ap_uint<8> dst;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(
        out,
        "struct __attribute__((packed)) edge_descriptor_batch_t {{"
    )
    .unwrap();
    writeln!(out, "    edge_t edges[PE_NUM];").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) update_tuple_t_big {{").unwrap();
    writeln!(out, "    update_t_big data[PE_NUM];").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');
}

fn write_little_structs(out: &mut String) {
    writeln!(out, "struct __attribute__((packed)) edge_t {{").unwrap();
    writeln!(out, "    node_id_t src_id;").unwrap();
    writeln!(out, "    local_id_t dst_id;").unwrap();
    writeln!(out, "#if EDGE_PROP_BITS > 0").unwrap();
    writeln!(out, "    edge_prop_t edge_prop;").unwrap();
    writeln!(out, "#endif").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) update_t_little {{").unwrap();
    writeln!(out, "    local_id_t node_id;").unwrap();
    writeln!(out, "    ap_fixed_pod_t prop;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) ppb_request_t {{").unwrap();
    writeln!(out, "    ap_uint<32> request_round;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(out, "struct __attribute__((packed)) ppb_response_t {{").unwrap();
    writeln!(out, "    bus_word_t data;").unwrap();
    writeln!(out, "    ap_uint<32> addr;").unwrap();
    writeln!(out, "    bool end_flag;").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(
        out,
        "struct __attribute__((packed)) edge_descriptor_batch_t {{"
    )
    .unwrap();
    writeln!(out, "    edge_t edges[PE_NUM];").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');

    writeln!(
        out,
        "struct __attribute__((packed)) update_tuple_t_little {{"
    )
    .unwrap();
    writeln!(out, "    update_t_little data[PE_NUM];").unwrap();
    writeln!(out, "}};").unwrap();
    out.push('\n');
}

fn write_big_prototype(out: &mut String) {
    writeln!(out, "// --- Top-Level Function Prototypes ---").unwrap();
    writeln!(out, "extern \"C\" void").unwrap();
    writeln!(out, " graphyflow_big(const bus_word_t* edge_props,").unwrap();
    writeln!(out, " int32_t num_nodes,").unwrap();
    writeln!(out, " int32_t num_edges,").unwrap();
    writeln!(out, " int32_t dst_num,").unwrap();
    writeln!(out, " int32_t memory_offset,").unwrap();
    writeln!(
        out,
        " hls::stream<cacheline_request_pkt_t> &cacheline_req_stream,"
    )
    .unwrap();
    writeln!(
        out,
        " hls::stream<cacheline_response_pkt_t> &cacheline_resp_stream,"
    )
    .unwrap();
    writeln!(out, " hls::stream<write_burst_pkt_t> &kernel_out_stream ").unwrap();
    writeln!(out, ");").unwrap();
    out.push('\n');
}

fn write_little_prototype(out: &mut String) {
    writeln!(out, "extern \"C\" void").unwrap();
    writeln!(out, " graphyflow_little(const bus_word_t* edge_props,").unwrap();
    writeln!(out, " int32_t num_nodes,").unwrap();
    writeln!(out, " int32_t num_edges,").unwrap();
    writeln!(out, " int32_t dst_num,").unwrap();
    writeln!(out, " int32_t memory_offset,").unwrap();
    writeln!(out, " hls::stream<ppb_request_pkt_t> &ppb_req_stream,").unwrap();
    writeln!(out, " hls::stream<ppb_response_pkt_t> &ppb_resp_stream,").unwrap();
    writeln!(out, " hls::stream<little_out_pkt_t> &kernel_out_stream ").unwrap();
    writeln!(out, ");").unwrap();
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_edge_config() -> HlsEdgeConfig {
        HlsEdgeConfig {
            edge_prop_bits: 32,
            edge_prop_widths: vec![32],
            edge_weight_bits: 32,
            edge_weight_lsb: 0,
            edge_weight_shift: 0,
            edges_per_word: 5,
            big_pe: 8,
            big_log_pe: 3,
            little_pe: 5,
            local_id_bits: 32,
            compact_edge_payload: false,
            zero_sentinel: true,
            allow_scatter_inf_overflow_to_zero: false,
        }
    }

    fn sample_node_config() -> HlsNodeConfig {
        HlsNodeConfig {
            node_prop_bits: 32,
            node_prop_int_bits: 32,
            node_prop_signed: true,
            dist_per_word: 16,
            log_dist_per_word: 4,
            distances_per_reduce_word: 2,
        }
    }

    #[test]
    fn big_header_uses_local_id_t_for_struct_fields() {
        let rendered =
            render_graphyflow_big_header(&sample_edge_config(), &sample_node_config(), 524_288);
        assert!(rendered.contains("typedef ap_uint<LOCAL_ID_BITWIDTH> local_id_t;"));
        assert!(rendered.contains("#define INVALID_LOCAL_ID_BIG (local_id_t(1) << LOCAL_ID_MSB)"));
        assert!(rendered.contains("local_id_t dst_id;"));
        assert!(rendered.contains("local_id_t node_id;"));
        assert!(!rendered.contains("ap_uint<20> dst_id"));
        assert!(!rendered.contains("ap_uint<20> node_id"));
    }

    #[test]
    fn little_header_emits_configured_local_id_width() {
        let mut edge = sample_edge_config();
        edge.local_id_bits = 22;
        let rendered = render_graphyflow_little_header(&edge, &sample_node_config(), 65_536);
        assert!(rendered.contains("#define LOCAL_ID_BITWIDTH 22"));
        assert!(
            rendered.contains("#define INVALID_LOCAL_ID_LITTLE (local_id_t(1) << LOCAL_ID_MSB)")
        );
        assert!(rendered.contains("local_id_t dst_id;"));
        assert!(rendered.contains("local_id_t node_id;"));
    }

    #[test]
    fn ddr_header_emits_compact_edge_payload_layout() {
        let mut edge = sample_edge_config();
        edge.local_id_bits = 22;
        edge.edge_prop_bits = 10;
        edge.edge_prop_widths = vec![10];
        edge.edge_weight_bits = 10;
        edge.edges_per_word = 8;
        edge.compact_edge_payload = true;
        let rendered = render_graphyflow_big_header(&edge, &sample_node_config(), 655_360);
        assert!(rendered.contains("#define EDGE_PROP_PAYLOAD_LSB 22"));
        assert!(rendered.contains("#define EDGE_PAYLOAD_BITS 64"));
        assert!(rendered.contains("#define EDGES_PER_WORD 8"));
    }
}
