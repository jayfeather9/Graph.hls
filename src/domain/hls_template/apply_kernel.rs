use crate::domain::{
    hls::{
        HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsField, HlsFunction, HlsIdentifier, HlsInclude,
        HlsLiteral, HlsParameter, HlsPragma, HlsStatement, HlsStruct, HlsType, HlsUnaryOp,
        HlsVarDecl, LoopIncrement, LoopInitializer, LoopLabel, PassingStyle,
    },
    hls_ops::{KernelOpBundle, OperatorExpr, OperatorOperand, ReducerKind},
};

use super::HlsTemplateError;
use super::utils::{
    assignment, binary, cast_ternary_branches, custom, expr_uses_operand, ident, literal_bool,
    literal_int, member_expr, method_call, range_method, render_operator_expr,
};

/// Builds the structured representation of `apply_kernel.cpp`.
pub fn apply_kernel_unit(ops: &KernelOpBundle) -> Result<HlsCompilationUnit, HlsTemplateError> {
    let is_pr = is_pagerank_apply(ops);
    // For PageRank, out-degree is read from node_props directly; no aux port needed.
    let needs_aux = !is_pr && expr_uses_operand(&ops.apply.expr, &OperatorOperand::OldAux);
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            attach_dest_to_stream()?,
            merge_two_endflag_streams()?,
            forward_endflag_stream_to_out()?,
            merge_big_little_writes()?,
            apply_func(ops, needs_aux, is_pr)?,
            apply_kernel_top(needs_aux, is_pr)?,
        ],
    })
}

/// Builds an `apply_kernel.cpp` that merges outputs from multiple little/big mergers.
pub fn apply_kernel_multi_merger_unit(
    ops: &KernelOpBundle,
    little_mergers: usize,
    big_mergers: usize,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    let is_pr = is_pagerank_apply(ops);
    let needs_aux = !is_pr && expr_uses_operand(&ops.apply.expr, &OperatorOperand::OldAux);
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            attach_dest_to_stream()?,
            merge_two_endflag_streams()?,
            forward_endflag_stream_to_out()?,
            merge_multi_merger_writes(little_mergers, big_mergers)?,
            apply_func(ops, needs_aux, is_pr)?,
            apply_kernel_top_multi(little_mergers, big_mergers, needs_aux, is_pr)?,
        ],
    })
}

/// Builds a DDR-mode `apply_kernel.cpp` that writes results directly to DDR
/// via an `output` m_axi port instead of streaming to hbm_writer.
pub fn apply_kernel_ddr_unit(ops: &KernelOpBundle) -> Result<HlsCompilationUnit, HlsTemplateError> {
    let is_pr = is_pagerank_apply(ops);
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: vec![
            HlsStatement::FunctionDef(merge_big_little_writes_ddr_fn()?),
            HlsStatement::Raw(String::new()),
            write_burst_w_dst_t_struct(),
        ],
        functions: vec![
            write_out_ddr_fn()?,
            apply_func_ddr_fn(ops, is_pr)?,
            apply_kernel_top_ddr_fn(is_pr)?,
        ],
    })
}

/// DDR merge_big_little_writes: polling loop that merges little/big write
/// streams into a single stream with destination addresses attached.
fn merge_big_little_writes_ddr_fn() -> Result<HlsFunction, HlsTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_big_little_writes")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("little_kernel_out_stream", "write_burst_pkt_t")?,
            stream_param("big_kernel_out_stream", "write_burst_pkt_t")?,
            HlsParameter {
                name: ident("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            scalar_param("little_kernel_length", HlsType::UInt32)?,
            scalar_param("big_kernel_length", HlsType::UInt32)?,
            scalar_param("little_kernel_st_offset", HlsType::UInt32)?,
            scalar_param("big_kernel_st_offset", HlsType::UInt32)?,
        ],
        body: vec![HlsStatement::Raw(
            r#"write_burst_pkt_t big_tmp_prop_pkt;
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
    }"#
            .to_string(),
        )],
    })
}

/// DDR struct: write_burst_w_dst_t { data, dest, last }.
fn write_burst_w_dst_t_struct() -> HlsStatement {
    HlsStatement::Struct(HlsStruct {
        name: ident("write_burst_w_dst_t").expect("valid identifier"),
        fields: vec![
            HlsField {
                name: ident("data").expect("valid identifier"),
                ty: custom("bus_word_t"),
            },
            HlsField {
                name: ident("dest").expect("valid identifier"),
                ty: HlsType::UInt32,
            },
            HlsField {
                name: ident("last").expect("valid identifier"),
                ty: HlsType::Bool,
            },
        ],
        attributes: Vec::new(),
    })
}

/// DDR write_out: reads from the write_burst_stream and writes to DDR output.
fn write_out_ddr_fn() -> Result<HlsFunction, HlsTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: ident("write_out")?,
        return_type: HlsType::Void,
        params: vec![
            HlsParameter {
                name: ident("output")?,
                ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: ident("write_burst_stream")?,
                ty: HlsType::Stream(Box::new(custom("write_burst_w_dst_t"))),
                passing: PassingStyle::Reference,
            },
        ],
        body: vec![HlsStatement::Raw(
            r#"LOOP_WRITE_OUT:
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
    }"#
            .to_string(),
        )],
    })
}

/// DDR apply_func: reads from write_burst_stream, applies the operator, writes
/// to kernel_out_stream.
fn apply_func_ddr_fn(ops: &KernelOpBundle, is_pr: bool) -> Result<HlsFunction, HlsTemplateError> {
    let apply_loop = render_ddr_apply_func_loop_raw(ops, is_pr)?;

    let mut params = vec![HlsParameter {
        name: ident("node_props")?,
        ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
        passing: PassingStyle::Value,
    }];
    if is_pr {
        params.push(scalar_param("arg_reg", HlsType::UInt32)?);
    }
    params.push(HlsParameter {
        name: ident("write_burst_stream")?,
        ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
        passing: PassingStyle::Reference,
    });
    params.push(HlsParameter {
        name: ident("kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(custom("write_burst_w_dst_t"))),
        passing: PassingStyle::Reference,
    });

    Ok(HlsFunction {
        linkage: Some("static"),
        name: ident("apply_func")?,
        return_type: HlsType::Void,
        params,
        body: vec![HlsStatement::Raw(format!(
            r#"LOOP_WHILE_44:
    while (true) {{
        in_write_burst_w_dst_pkt_t in_pkt = write_burst_stream.read();
        if (in_pkt.end_flag) {{
            write_burst_w_dst_t end_pkt;
            end_pkt.last = true;
            kernel_out_stream.write(end_pkt);
            break;
        }}
{apply_loop}
    }}"#,
            apply_loop = apply_loop,
        ))],
    })
}

/// Renders the per-lane apply logic for the DDR apply_func as a raw string.
fn render_ddr_apply_func_loop_raw(
    ops: &KernelOpBundle,
    is_pr: bool,
) -> Result<String, HlsTemplateError> {
    if is_pr {
        return Ok(r#"        uint32_t dest_addr = in_pkt.dest_addr;
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
            const ap_int<32> kDampFixPoint = 108;
            ap_int<32> new_score = (ap_int<32>)arg_reg +
                                   (ap_int<32>)(((ap_int<64>)kDampFixPoint * (ap_int<64>)sum_in) >> 7);
            ap_int<32> new_contrib = 0;
            if (outDeg != 0) {
                ap_uint<32> tmp = ((ap_uint<32>)1 << 16) / outDeg;
                new_contrib = (ap_int<32>)(((ap_int<64>)new_score * (ap_int<64>)tmp) >> 16);
            }
            new_props.range(31 + (i << 5), (i << 5)) = (ap_uint<32>)new_contrib;
        }
        out_pkt.data = new_props;
        kernel_out_stream.write(out_pkt);"#.to_string());
    }

    // Generic apply: render the operator expression as a string
    let apply_expr_str = render_apply_expr_string(&ops.apply.expr);

    Ok(format!(
        r#"        uint32_t dest_addr = in_pkt.dest_addr;
        bus_word_t ori_props = node_props[dest_addr];
        bus_word_t new_props;
        write_burst_w_dst_t out_pkt;
        out_pkt.dest = dest_addr;
        out_pkt.last = false;
        LOOP_FOR_43:
        for (int32_t i = 0; i < 16; i++) {{
#pragma HLS UNROLL
            ap_fixed_pod_t update = in_pkt.data.range(31 + (i << 5), (i << 5));
            ap_fixed_pod_t old = ori_props.range(31 + (i << 5), (i << 5));
            ap_fixed_pod_t new_prop;
            // Begin inline fused op
            ap_fixed_pod_t BinOp_128_res;
            BinOp_128_res = {apply_expr};
            new_prop = BinOp_128_res;
            // End inline fused op
            new_props.range(31 + (i << 5), (i << 5)) = new_prop;
        }}
        out_pkt.data = new_props;
        kernel_out_stream.write(out_pkt);"#,
        apply_expr = apply_expr_str,
    ))
}

/// DDR apply_kernel top-level extern "C" function.
fn apply_kernel_top_ddr_fn(is_pr: bool) -> Result<HlsFunction, HlsTemplateError> {
    let mut params = vec![
        HlsParameter {
            name: ident("node_props")?,
            ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: ident("output")?,
            ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        scalar_param("little_kernel_length", HlsType::UInt32)?,
        scalar_param("big_kernel_length", HlsType::UInt32)?,
        scalar_param("little_kernel_st_offset", HlsType::UInt32)?,
        scalar_param("big_kernel_st_offset", HlsType::UInt32)?,
    ];
    if is_pr {
        params.push(scalar_param("arg_reg", HlsType::UInt32)?);
    }
    params.push(stream_param(
        "little_kernel_out_stream",
        "write_burst_pkt_t",
    )?);
    params.push(stream_param("big_kernel_out_stream", "write_burst_pkt_t")?);

    let arg_reg_pragma = if is_pr {
        "\n#pragma HLS INTERFACE s_axilite port = arg_reg bundle = control"
    } else {
        ""
    };
    let arg_reg_fwd = if is_pr { ", arg_reg" } else { "" };

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("apply_kernel")?,
        return_type: HlsType::Void,
        params,
        body: vec![HlsStatement::Raw(format!(
            r#"#pragma HLS INTERFACE m_axi port = node_props offset = slave bundle = gmem0
#pragma HLS INTERFACE m_axi port = output offset = slave bundle = gmem1
#pragma HLS INTERFACE s_axilite port = node_props bundle = control
#pragma HLS INTERFACE s_axilite port = output bundle = control
#pragma HLS INTERFACE s_axilite port = little_kernel_length bundle = control
#pragma HLS INTERFACE s_axilite port = big_kernel_length bundle = control
#pragma HLS INTERFACE s_axilite port = little_kernel_st_offset bundle = control
#pragma HLS INTERFACE s_axilite port = big_kernel_st_offset bundle = control{arg_reg_pragma}
#pragma HLS INTERFACE s_axilite port = return bundle = control
#pragma HLS DATAFLOW

    hls::stream<in_write_burst_w_dst_pkt_t> write_burst_stream;
#pragma HLS STREAM variable = write_burst_stream depth = 16
    hls::stream<write_burst_w_dst_t> kernel_out_stream;
#pragma HLS STREAM variable = kernel_out_stream depth = 16

    merge_big_little_writes(little_kernel_out_stream, big_kernel_out_stream, write_burst_stream, little_kernel_length, big_kernel_length, little_kernel_st_offset, big_kernel_st_offset);
    apply_func(node_props{arg_reg_fwd}, write_burst_stream, kernel_out_stream);
    write_out(output, kernel_out_stream);"#,
            arg_reg_pragma = arg_reg_pragma,
            arg_reg_fwd = arg_reg_fwd,
        ))],
    })
}

fn attach_dest_to_stream() -> Result<HlsFunction, HlsTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: ident("attach_dest_to_stream")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("in_stream", "write_burst_pkt_t")?,
            HlsParameter {
                name: ident("out_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            scalar_param("st_offset", HlsType::UInt32)?,
            scalar_param("length", HlsType::UInt32)?,
        ],
        body: vec![HlsStatement::Raw(
            r#"
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
"#
            .to_string(),
        )],
    })
}

fn merge_two_endflag_streams() -> Result<HlsFunction, HlsTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_two_endflag_streams")?,
        return_type: HlsType::Void,
        params: vec![
            HlsParameter {
                name: ident("a_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: ident("b_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: ident("out_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
        ],
        body: vec![HlsStatement::Raw(
            r#"
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
"#
            .to_string(),
        )],
    })
}

fn forward_endflag_stream_to_out() -> Result<HlsFunction, HlsTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: ident("forward_endflag_stream_to_out")?,
        return_type: HlsType::Void,
        params: vec![
            HlsParameter {
                name: ident("in_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: ident("out_stream")?,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
        ],
        body: vec![HlsStatement::Raw(
            r#"
#pragma HLS INLINE off
FORWARD_END: while (true) {
#pragma HLS PIPELINE II = 1
  in_write_burst_w_dst_pkt_t pkt = in_stream.read();
  out_stream.write(pkt);
  if (pkt.end_flag) {
    break;
  }
}
"#
            .to_string(),
        )],
    })
}

fn merge_big_little_writes() -> Result<HlsFunction, HlsTemplateError> {
    let kernel_stream = ident("kernel_out_stream")?;
    let body = vec![HlsStatement::Raw(
        r#"
#pragma HLS INLINE off
#pragma HLS DATAFLOW
  hls::stream<in_write_burst_w_dst_pkt_t> little_with_dst;
  hls::stream<in_write_burst_w_dst_pkt_t> big_with_dst;
#pragma HLS STREAM variable = little_with_dst depth = 16
#pragma HLS STREAM variable = big_with_dst depth = 16

  attach_dest_to_stream(little_kernel_out_stream, little_with_dst, little_kernel_st_offset, little_kernel_length);
  attach_dest_to_stream(big_kernel_out_stream, big_with_dst, big_kernel_st_offset, big_kernel_length);
  merge_two_endflag_streams(little_with_dst, big_with_dst, kernel_out_stream);
"#
        .to_string(),
    )];

    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_big_little_writes")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("little_kernel_out_stream", "write_burst_pkt_t")?,
            stream_param("big_kernel_out_stream", "write_burst_pkt_t")?,
            HlsParameter {
                name: kernel_stream,
                ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            },
            scalar_param("little_kernel_length", HlsType::UInt32)?,
            scalar_param("big_kernel_length", HlsType::UInt32)?,
            scalar_param("little_kernel_st_offset", HlsType::UInt32)?,
            scalar_param("big_kernel_st_offset", HlsType::UInt32)?,
        ],
        body,
    })
}

fn merge_multi_merger_writes(
    little_mergers: usize,
    big_mergers: usize,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut params = Vec::new();
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(stream_param(
            &format!("little_merger_{kernel_id}_out_stream"),
            "write_burst_pkt_t",
        )?);
    }
    for gid in 0..big_mergers {
        params.push(stream_param(
            &format!("big_merger_{gid}_out_stream"),
            "write_burst_pkt_t",
        )?);
    }
    params.push(HlsParameter {
        name: ident("kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
        passing: PassingStyle::Reference,
    });
    params.push(scalar_param("num_little_mergers", HlsType::UInt32)?);
    params.push(scalar_param("num_big_mergers", HlsType::UInt32)?);
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(scalar_param(
            &format!("little_merger_{kernel_id}_length"),
            HlsType::UInt32,
        )?);
    }
    for gid in 0..big_mergers {
        params.push(scalar_param(
            &format!("big_merger_{gid}_length"),
            HlsType::UInt32,
        )?);
    }
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(scalar_param(
            &format!("little_merger_{kernel_id}_st_offset"),
            HlsType::UInt32,
        )?);
    }
    for gid in 0..big_mergers {
        params.push(scalar_param(
            &format!("big_merger_{gid}_st_offset"),
            HlsType::UInt32,
        )?);
    }

    let mut code = String::new();
    code.push_str("#pragma HLS INLINE off\n");
    code.push_str("#pragma HLS DATAFLOW\n\n");

    let mut source_streams: Vec<String> = Vec::new();
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        let with_dst = format!("little_{kernel_id}_with_dst");
        code.push_str(&format!(
            "hls::stream<in_write_burst_w_dst_pkt_t> {with_dst};\n"
        ));
        code.push_str(&format!(
            "#pragma HLS STREAM variable = {with_dst} depth = 16\n"
        ));
        code.push_str(&format!(
            "uint32_t little_{kernel_id}_len = (num_little_mergers >= {}u) ? little_merger_{kernel_id}_length : 0u;\n",
            gid + 1
        ));
        code.push_str(&format!(
            "attach_dest_to_stream(little_merger_{kernel_id}_out_stream, {with_dst}, little_merger_{kernel_id}_st_offset, little_{kernel_id}_len);\n"
        ));
        source_streams.push(with_dst);
    }
    for gid in 0..big_mergers {
        let with_dst = format!("big_{gid}_with_dst");
        code.push_str(&format!(
            "hls::stream<in_write_burst_w_dst_pkt_t> {with_dst};\n"
        ));
        code.push_str(&format!(
            "#pragma HLS STREAM variable = {with_dst} depth = 16\n"
        ));
        code.push_str(&format!(
            "uint32_t big_{gid}_len = (num_big_mergers > {}u) ? big_merger_{gid}_length : 0u;\n",
            gid
        ));
        code.push_str(&format!(
            "attach_dest_to_stream(big_merger_{gid}_out_stream, {with_dst}, big_merger_{gid}_st_offset, big_{gid}_len);\n"
        ));
        source_streams.push(with_dst);
    }

    match source_streams.len() {
        0 => {
            code.push_str(
                "in_write_burst_w_dst_pkt_t end_pkt;\nend_pkt.end_flag = true;\nkernel_out_stream.write(end_pkt);\n",
            );
        }
        1 => {
            code.push_str(&format!(
                "forward_endflag_stream_to_out({}, kernel_out_stream);\n",
                source_streams[0]
            ));
        }
        _ => {
            let mut current = source_streams[0].clone();
            for (idx, next) in source_streams.iter().enumerate().skip(1) {
                let stage_name = format!("merge_stage_{}", idx);
                let out_name = if idx == source_streams.len() - 1 {
                    "kernel_out_stream".to_string()
                } else {
                    code.push_str(&format!(
                        "hls::stream<in_write_burst_w_dst_pkt_t> {stage_name};\n"
                    ));
                    code.push_str(&format!(
                        "#pragma HLS STREAM variable = {stage_name} depth = 16\n"
                    ));
                    stage_name.clone()
                };
                code.push_str(&format!(
                    "merge_two_endflag_streams({}, {}, {});\n",
                    current, next, out_name
                ));
                current = stage_name;
            }
        }
    }

    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_multi_merger_writes")?,
        return_type: HlsType::Void,
        params,
        body: vec![HlsStatement::Raw(code)],
    })
}

fn apply_func(
    ops: &KernelOpBundle,
    needs_aux: bool,
    is_pr: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let node_props = ident("node_props")?;
    let write_stream = ident("write_burst_stream")?;
    let kernel_stream = ident("kernel_out_stream")?;
    let aux_node_props = ident("aux_node_props")?;

    let mut body = Vec::new();
    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_44")?,
        condition: literal_bool(true),
        body: apply_func_loop(
            write_stream.clone(),
            kernel_stream.clone(),
            node_props.clone(),
            aux_node_props.clone(),
            needs_aux,
            ops,
        )?,
    }));

    let mut params = vec![HlsParameter {
        name: node_props,
        ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
        passing: PassingStyle::Value,
    }];
    if needs_aux {
        params.push(HlsParameter {
            name: aux_node_props,
            ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        });
    }
    if is_pr {
        params.push(HlsParameter {
            name: ident("arg_reg")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }
    params.push(HlsParameter {
        name: write_stream,
        ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
        passing: PassingStyle::Reference,
    });
    params.push(HlsParameter {
        name: kernel_stream,
        ty: HlsType::Stream(Box::new(custom("write_burst_w_dst_pkt_t"))),
        passing: PassingStyle::Reference,
    });

    Ok(HlsFunction {
        linkage: None,
        name: ident("apply_func")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn apply_func_loop(
    write_stream: HlsIdentifier,
    kernel_stream: HlsIdentifier,
    node_props: HlsIdentifier,
    aux_node_props: HlsIdentifier,
    needs_aux: bool,
    ops: &KernelOpBundle,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut stmts = Vec::new();
    stmts.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("in_pkt")?,
        ty: custom("in_write_burst_w_dst_pkt_t"),
        init: Some(method_call(
            HlsExpr::Identifier(write_stream),
            "read",
            Vec::new(),
        )?),
    }));
    stmts.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: member_expr_var("in_pkt", "end_flag")?,
        then_body: end_packet_branch(kernel_stream.clone())?,
        else_body: relax_branch(kernel_stream, node_props, aux_node_props, needs_aux, ops)?,
    }));
    Ok(stmts)
}

fn end_packet_branch(stream: HlsIdentifier) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![
        HlsStatement::Declaration(HlsVarDecl {
            name: ident("end_pkt")?,
            ty: custom("write_burst_w_dst_pkt_t"),
            init: None,
        }),
        assignment(member_expr_var("end_pkt", "last")?, literal_bool(true)),
        HlsStatement::Expr(method_call(
            HlsExpr::Identifier(stream),
            "write",
            vec![HlsExpr::Identifier(ident("end_pkt")?)],
        )?),
        HlsStatement::Break,
    ])
}

fn relax_branch(
    stream: HlsIdentifier,
    node_props: HlsIdentifier,
    aux_node_props: HlsIdentifier,
    needs_aux: bool,
    ops: &KernelOpBundle,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let dest_addr = ident("dest_addr")?;
    let mut stmts = Vec::new();
    stmts.push(HlsStatement::Declaration(HlsVarDecl {
        name: dest_addr.clone(),
        ty: HlsType::UInt32,
        init: Some(member_expr_var("in_pkt", "dest_addr")?),
    }));
    stmts.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("ori_props")?,
        ty: custom("bus_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(node_props)),
            index: Box::new(HlsExpr::Identifier(dest_addr.clone())),
        }),
    }));
    if needs_aux {
        stmts.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("aux_props")?,
            ty: custom("bus_word_t"),
            init: Some(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(aux_node_props)),
                index: Box::new(HlsExpr::Identifier(dest_addr.clone())),
            }),
        }));
    }
    stmts.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("new_props")?,
        ty: custom("bus_word_t"),
        init: None,
    }));
    stmts.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("out_pkt")?,
        ty: custom("write_burst_w_dst_pkt_t"),
        init: None,
    }));
    stmts.push(assignment(
        member_expr_var("out_pkt", "dest")?,
        HlsExpr::Identifier(dest_addr.clone()),
    ));
    stmts.push(assignment(
        member_expr_var("out_pkt", "last")?,
        literal_bool(false),
    ));

    stmts.push(HlsStatement::ForLoop(crate::domain::hls::HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_43")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: ident("i")?,
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(ident("i")?),
            HlsExpr::Identifier(ident("DIST_PER_WORD")?),
        ),
        increment: LoopIncrement::Unary(HlsUnaryOp::PreIncrement, HlsExpr::Identifier(ident("i")?)),
        body: per_lane_body(ops, needs_aux)?,
    }));

    stmts.push(assignment(
        member_expr_var("out_pkt", "data")?,
        HlsExpr::Identifier(ident("new_props")?),
    ));
    stmts.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(stream),
        "write",
        vec![HlsExpr::Identifier(ident("out_pkt")?)],
    )?));
    Ok(stmts)
}

fn per_lane_body(
    ops: &KernelOpBundle,
    needs_aux: bool,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    if is_pagerank_apply(ops) {
        return per_lane_body_pagerank();
    }

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let shift = lane_shift_expr()?;
    let high = binary(
        HlsBinaryOp::Add,
        shift.clone(),
        binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            literal_int(1),
        ),
    );
    let low = shift;

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("update")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            member_expr_var("in_pkt", "data")?,
            high.clone(),
            low.clone(),
        )?),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("old")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("ori_props")?),
            high.clone(),
            low.clone(),
        )?),
    }));
    if needs_aux {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("aux")?,
            ty: custom("ap_fixed_pod_t"),
            init: Some(range_method(
                HlsExpr::Identifier(ident("aux_props")?),
                high.clone(),
                low.clone(),
            )?),
        }));
    }
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("update_val")?,
        ty: custom("distance_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("old_val")?,
        ty: custom("distance_t"),
        init: None,
    }));
    if needs_aux {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("aux_val")?,
            ty: custom("distance_t"),
            init: None,
        }));
    }
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("new_prop")?,
        ty: custom("ap_fixed_pod_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("BinOp_128_res")?,
        ty: custom("distance_t"),
        init: None,
    }));

    let full_high = binary(
        HlsBinaryOp::Sub,
        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        literal_int(1),
    );
    body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("update_val")?),
            full_high.clone(),
            literal_int(0),
        )?,
        HlsExpr::Identifier(ident("update")?),
    ));
    body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("old_val")?),
            full_high.clone(),
            literal_int(0),
        )?,
        HlsExpr::Identifier(ident("old")?),
    ));
    if needs_aux {
        body.push(assignment(
            range_method(
                HlsExpr::Identifier(ident("aux_val")?),
                full_high.clone(),
                literal_int(0),
            )?,
            HlsExpr::Identifier(ident("aux")?),
        ));
    }

    let old_ident = ident("old_val")?;
    let update_ident = ident("update_val")?;
    let aux_ident = ident("aux_val")?;

    let mut apply_mapper = |opnd: &OperatorOperand| match opnd {
        OperatorOperand::OldProp => Some(HlsExpr::Identifier(old_ident.clone())),
        OperatorOperand::OldAux => needs_aux.then(|| HlsExpr::Identifier(aux_ident.clone())),
        OperatorOperand::GatherValue => Some(HlsExpr::Identifier(update_ident.clone())),
        OperatorOperand::ConstInt(v) => Some(HlsExpr::Cast {
            target_type: custom("distance_t"),
            expr: Box::new(HlsExpr::Literal(HlsLiteral::Int(*v))),
        }),
        OperatorOperand::ConstFloat(v) => Some(HlsExpr::Cast {
            target_type: custom("distance_t"),
            expr: Box::new(HlsExpr::Literal(HlsLiteral::Float(*v))),
        }),
        _ => None,
    };

    let rendered_apply = render_operator_expr(&ops.apply.expr, &mut apply_mapper)?;
    body.push(assignment(
        HlsExpr::Identifier(ident("BinOp_128_res")?),
        if operator_expr_contains_const_int(&ops.apply.expr) {
            cast_ternary_branches(rendered_apply, custom("distance_t"))
        } else {
            rendered_apply
        },
    ));
    body.push(assignment(
        HlsExpr::Identifier(ident("new_prop")?),
        range_method(
            HlsExpr::Identifier(ident("BinOp_128_res")?),
            full_high,
            literal_int(0),
        )?,
    ));
    body.push(assignment(
        range_method(HlsExpr::Identifier(ident("new_props")?), high, low)?,
        HlsExpr::Identifier(ident("new_prop")?),
    ));
    Ok(body)
}

/// Reference-style PageRank per-lane apply body.
///
/// For each distance lane in the bus word the computation is:
///
/// ```c
/// ap_int<32> sum_in = (ap_int<32>)in_pkt.data.range(...);
/// ap_uint<32> outDeg = (ap_uint<32>)ori_props.range(...);
/// const ap_int<32> kDampFixPoint = 108; // 0.85 * 128
/// ap_int<32> new_score = (ap_int<32>)arg_reg +
///     (ap_int<32>)(((ap_int<64>)kDampFixPoint * (ap_int<64>)sum_in) >> 7);
/// ap_int<32> new_contrib = 0;
/// if (outDeg != 0) {
///     ap_uint<32> tmp = ((ap_uint<32>)1 << 16) / outDeg;
///     new_contrib = (ap_int<32>)(((ap_int<64>)new_score * (ap_int<64>)tmp) >> 16);
/// }
/// new_props.range(...) = (ap_uint<32>)new_contrib;
/// ```
///
/// `ori_props` holds the per-vertex out-degree (NOT the old rank),
/// and `arg_reg` is `(1-d)/N` precomputed on the host.
fn per_lane_body_pagerank() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    // Emit the whole per-lane body as raw C++ so it matches the reference
    // exactly without encoding every sub-expression in the HLS AST.
    body.push(HlsStatement::Raw(
        "ap_int<32> sum_in = (ap_int<32>)in_pkt.data.range(31 + (i << 5), (i << 5));".to_string(),
    ));
    body.push(HlsStatement::Raw(
        "ap_uint<32> outDeg = (ap_uint<32>)ori_props.range(31 + (i << 5), (i << 5));".to_string(),
    ));
    body.push(HlsStatement::Raw(
        "const ap_int<32> kDampFixPoint = 108;".to_string(),
    ));
    body.push(HlsStatement::Raw(
        "ap_int<32> new_score = (ap_int<32>)arg_reg + \
         (ap_int<32>)(((ap_int<64>)kDampFixPoint * (ap_int<64>)sum_in) >> 7);"
            .to_string(),
    ));
    body.push(HlsStatement::Raw("ap_int<32> new_contrib = 0;".to_string()));
    body.push(HlsStatement::Raw(
        "if (outDeg != 0) { \
         ap_uint<32> tmp = ((ap_uint<32>)1 << 16) / outDeg; \
         new_contrib = (ap_int<32>)(((ap_int<64>)new_score * (ap_int<64>)tmp) >> 16); \
         }"
        .to_string(),
    ));
    body.push(HlsStatement::Raw(
        "new_props.range(31 + (i << 5), (i << 5)) = (ap_uint<32>)new_contrib;".to_string(),
    ));
    Ok(body)
}

fn operator_expr_contains_const_int(expr: &OperatorExpr) -> bool {
    match expr {
        OperatorExpr::Operand(OperatorOperand::ConstInt(_)) => true,
        OperatorExpr::Operand(_) => false,
        OperatorExpr::Unary { expr, .. } => operator_expr_contains_const_int(expr),
        OperatorExpr::Binary { left, right, .. } => {
            operator_expr_contains_const_int(left) || operator_expr_contains_const_int(right)
        }
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            operator_expr_contains_const_int(condition)
                || operator_expr_contains_const_int(then_expr)
                || operator_expr_contains_const_int(else_expr)
        }
    }
}

/// Detect whether the apply should use the reference-style PageRank computation:
/// Sum reducer + apply uses OldAux (out-degree) and a float constant (damping).
fn is_pagerank_apply(ops: &KernelOpBundle) -> bool {
    matches!(ops.gather.kind, ReducerKind::Sum)
        && expr_uses_operand(&ops.apply.expr, &OperatorOperand::OldAux)
}

fn apply_kernel_top(needs_aux: bool, is_pr: bool) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    for pragma in [
        "HLS INTERFACE m_axi port = node_props offset = slave bundle = gmem0",
        "HLS INTERFACE s_axilite port = node_props bundle = control",
        "HLS INTERFACE s_axilite port = little_kernel_length bundle = control",
        "HLS INTERFACE s_axilite port = big_kernel_length bundle = control",
        "HLS INTERFACE s_axilite port = little_kernel_st_offset bundle = control",
        "HLS INTERFACE s_axilite port = big_kernel_st_offset bundle = control",
        "HLS INTERFACE s_axilite port = return bundle = control",
        "HLS DATAFLOW",
    ] {
        body.push(HlsStatement::Pragma(HlsPragma::new(pragma)?));
    }
    if needs_aux {
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE m_axi port = aux_node_props offset = slave bundle = gmem1",
        )?));
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE s_axilite port = aux_node_props bundle = control",
        )?));
    }
    if is_pr {
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE s_axilite port = arg_reg bundle = control",
        )?));
    }

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("write_burst_stream")?,
        ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = write_burst_stream depth = 16",
    )?));

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("merge_big_little_writes")?,
        args: vec![
            HlsExpr::Identifier(ident("little_kernel_out_stream")?),
            HlsExpr::Identifier(ident("big_kernel_out_stream")?),
            HlsExpr::Identifier(ident("write_burst_stream")?),
            HlsExpr::Identifier(ident("little_kernel_length")?),
            HlsExpr::Identifier(ident("big_kernel_length")?),
            HlsExpr::Identifier(ident("little_kernel_st_offset")?),
            HlsExpr::Identifier(ident("big_kernel_st_offset")?),
        ],
    }));

    let mut apply_args = vec![HlsExpr::Identifier(ident("node_props")?)];
    if needs_aux {
        apply_args.push(HlsExpr::Identifier(ident("aux_node_props")?));
    }
    if is_pr {
        apply_args.push(HlsExpr::Identifier(ident("arg_reg")?));
    }
    apply_args.push(HlsExpr::Identifier(ident("write_burst_stream")?));
    apply_args.push(HlsExpr::Identifier(ident("kernel_out_stream")?));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("apply_func")?,
        args: apply_args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("apply_kernel")?,
        return_type: HlsType::Void,
        params: {
            let mut params = Vec::new();
            params.push(HlsParameter {
                name: ident("node_props")?,
                ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
                passing: PassingStyle::Value,
            });
            if needs_aux {
                params.push(HlsParameter {
                    name: ident("aux_node_props")?,
                    ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
                    passing: PassingStyle::Value,
                });
            }
            let mut scalars: Vec<(&str, HlsType)> = vec![
                ("little_kernel_length", HlsType::UInt32),
                ("big_kernel_length", HlsType::UInt32),
                ("little_kernel_st_offset", HlsType::UInt32),
                ("big_kernel_st_offset", HlsType::UInt32),
            ];
            if is_pr {
                scalars.push(("arg_reg", HlsType::UInt32));
            }
            for scalar in scalars {
                params.push(HlsParameter {
                    name: ident(scalar.0)?,
                    ty: scalar.1,
                    passing: PassingStyle::Value,
                });
            }
            for (name, ty) in [
                ("little_kernel_out_stream", "write_burst_pkt_t"),
                ("big_kernel_out_stream", "write_burst_pkt_t"),
            ] {
                params.push(stream_param(name, ty)?);
            }
            params.push(HlsParameter {
                name: ident("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(custom("write_burst_w_dst_pkt_t"))),
                passing: PassingStyle::Reference,
            });
            params
        },
        body,
    })
}

fn apply_kernel_top_multi(
    little_mergers: usize,
    big_mergers: usize,
    needs_aux: bool,
    is_pr: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut params = Vec::new();
    params.push(HlsParameter {
        name: ident("node_props")?,
        ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
        passing: PassingStyle::Value,
    });
    if needs_aux {
        params.push(HlsParameter {
            name: ident("aux_node_props")?,
            ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        });
    }
    params.push(HlsParameter {
        name: ident("num_little_mergers")?,
        ty: HlsType::UInt32,
        passing: PassingStyle::Value,
    });
    params.push(HlsParameter {
        name: ident("num_big_mergers")?,
        ty: HlsType::UInt32,
        passing: PassingStyle::Value,
    });
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(HlsParameter {
            name: ident(&format!("little_merger_{kernel_id}_length"))?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }
    for gid in 0..big_mergers {
        params.push(HlsParameter {
            name: ident(&format!("big_merger_{gid}_length"))?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(HlsParameter {
            name: ident(&format!("little_merger_{kernel_id}_st_offset"))?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }
    for gid in 0..big_mergers {
        params.push(HlsParameter {
            name: ident(&format!("big_merger_{gid}_st_offset"))?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }

    if is_pr {
        params.push(HlsParameter {
            name: ident("arg_reg")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }

    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        params.push(stream_param(
            &format!("little_merger_{kernel_id}_out_stream"),
            "write_burst_pkt_t",
        )?);
    }
    for gid in 0..big_mergers {
        params.push(stream_param(
            &format!("big_merger_{gid}_out_stream"),
            "write_burst_pkt_t",
        )?);
    }
    params.push(HlsParameter {
        name: ident("kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(custom("write_burst_w_dst_pkt_t"))),
        passing: PassingStyle::Reference,
    });

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE m_axi port = node_props offset = slave bundle = gmem0",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = node_props bundle = control",
    )?));
    if needs_aux {
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE m_axi port = aux_node_props offset = slave bundle = gmem1",
        )?));
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE s_axilite port = aux_node_props bundle = control",
        )?));
    }
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = num_little_mergers bundle = control",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = num_big_mergers bundle = control",
    )?));
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS INTERFACE s_axilite port = little_merger_{kernel_id}_length bundle = control"
        ))?));
    }
    for gid in 0..big_mergers {
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS INTERFACE s_axilite port = big_merger_{gid}_length bundle = control"
        ))?));
    }
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS INTERFACE s_axilite port = little_merger_{kernel_id}_st_offset bundle = control"
        ))?));
    }
    for gid in 0..big_mergers {
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS INTERFACE s_axilite port = big_merger_{gid}_st_offset bundle = control"
        ))?));
    }
    if is_pr {
        body.push(HlsStatement::Pragma(HlsPragma::new(
            "HLS INTERFACE s_axilite port = arg_reg bundle = control",
        )?));
    }
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = return bundle = control",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("write_burst_stream")?,
        ty: HlsType::Stream(Box::new(custom("in_write_burst_w_dst_pkt_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = write_burst_stream depth = 16",
    )?));

    // Merge and attach destination addresses in a single pipelined loop.
    // This avoids deep DATAFLOW control chains (attach+merge tree+forward) that can depress Fmax.
    let mut merge_args = Vec::new();
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "little_merger_{kernel_id}_out_stream"
        ))?));
    }
    for gid in 0..big_mergers {
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "big_merger_{gid}_out_stream"
        ))?));
    }
    merge_args.push(HlsExpr::Identifier(ident("write_burst_stream")?));
    merge_args.push(HlsExpr::Identifier(ident("num_little_mergers")?));
    merge_args.push(HlsExpr::Identifier(ident("num_big_mergers")?));
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "little_merger_{kernel_id}_length"
        ))?));
    }
    for gid in 0..big_mergers {
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "big_merger_{gid}_length"
        ))?));
    }
    for gid in 0..little_mergers {
        let kernel_id = big_mergers + gid;
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "little_merger_{kernel_id}_st_offset"
        ))?));
    }
    for gid in 0..big_mergers {
        merge_args.push(HlsExpr::Identifier(ident(&format!(
            "big_merger_{gid}_st_offset"
        ))?));
    }
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("merge_multi_merger_writes")?,
        args: merge_args,
    }));

    let mut apply_args = vec![HlsExpr::Identifier(ident("node_props")?)];
    if needs_aux {
        apply_args.push(HlsExpr::Identifier(ident("aux_node_props")?));
    }
    if is_pr {
        apply_args.push(HlsExpr::Identifier(ident("arg_reg")?));
    }
    apply_args.push(HlsExpr::Identifier(ident("write_burst_stream")?));
    apply_args.push(HlsExpr::Identifier(ident("kernel_out_stream")?));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("apply_func")?,
        args: apply_args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("apply_kernel")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn stream_param(name: &str, ty: &str) -> Result<HlsParameter, HlsTemplateError> {
    Ok(HlsParameter {
        name: ident(name)?,
        ty: HlsType::Stream(Box::new(custom(ty))),
        passing: PassingStyle::Reference,
    })
}

fn scalar_param(name: &str, ty: HlsType) -> Result<HlsParameter, HlsTemplateError> {
    Ok(HlsParameter {
        name: ident(name)?,
        ty,
        passing: PassingStyle::Value,
    })
}

/// Render an apply expression to a C++ string for DDR raw emission.
fn render_apply_expr_string(expr: &OperatorExpr) -> String {
    match expr {
        OperatorExpr::Operand(op) => match op {
            OperatorOperand::OldProp => "old".to_string(),
            OperatorOperand::GatherValue => "update".to_string(),
            OperatorOperand::OldAux => "aux".to_string(),
            OperatorOperand::ConstInt(v) => format!("{v}"),
            OperatorOperand::ConstFloat(v) => format!("{v}"),
            _ => "/*unknown*/".to_string(),
        },
        OperatorExpr::Binary { op, left, right } => {
            let l = render_apply_expr_string(left);
            let r = render_apply_expr_string(right);
            let op_str = match op {
                crate::domain::hls_ops::OperatorBinary::Add => "+",
                crate::domain::hls_ops::OperatorBinary::Sub => "-",
                crate::domain::hls_ops::OperatorBinary::Mul => "*",
                crate::domain::hls_ops::OperatorBinary::Div => "/",
                crate::domain::hls_ops::OperatorBinary::Lt => "<",
                crate::domain::hls_ops::OperatorBinary::Gt => ">",
                crate::domain::hls_ops::OperatorBinary::BitOr => "|",
                _ => "??",
            };
            format!("(({l}) {op_str} ({r}))")
        }
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            let c = render_apply_expr_string(condition);
            let t = render_apply_expr_string(then_expr);
            let e = render_apply_expr_string(else_expr);
            format!("(({c}) ? ({t}) : ({e}))")
        }
        OperatorExpr::Unary { op, expr } => {
            let e = render_apply_expr_string(expr);
            let op_str = match op {
                crate::domain::hls_ops::OperatorUnary::LogicalNot => "!",
                crate::domain::hls_ops::OperatorUnary::BitNot => "~",
            };
            format!("{op_str}({e})")
        }
    }
}

fn member_expr_var(var: &str, field: &str) -> Result<HlsExpr, HlsTemplateError> {
    member_expr(HlsExpr::Identifier(ident(var)?), field)
}

fn lane_shift_expr() -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Mul,
        HlsExpr::Identifier(ident("i")?),
        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
    ))
}
