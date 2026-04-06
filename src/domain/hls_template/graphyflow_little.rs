use crate::domain::{
    hls::{
        HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsForLoop, HlsFunction, HlsIfElse, HlsInclude,
        HlsLiteral, HlsParameter, HlsPragma, HlsStatement, HlsType, HlsUnaryOp, HlsVarDecl,
        HlsWhileLoop, LoopIncrement, LoopInitializer, LoopLabel, PassingStyle,
    },
    hls_ops::{KernelOpBundle, OperatorExpr, OperatorOperand, ReducerIdentity, ReducerKind},
};

use super::HlsTemplateError;
use super::utils::{
    assignment, binary, cast_ternary_branches, custom, expr_uses_operand, ident, int_decl,
    literal_bool, literal_int, literal_uint, member_expr, method_call, range_method,
    reducer_combine_expr, reducer_combine_expr_zero_sentinel, reducer_identity_expr,
    render_operator_expr,
};

pub fn graphyflow_little_unit(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("graphyflow_little.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            load_edges(edge)?,
            stream2axistream()?,
            axistream2stream()?,
            extract_src_prop_from_bus_word()?,
            request_manager(ops, edge)?,
            reduce_unit(ops, edge)?,
            partial_drain_impl(ops, edge.zero_sentinel)?,
            finalize_drain_single(ops)?,
            finalize_drain(ops, edge.zero_sentinel)?,
            graphyflow_little_top(ops, edge)?,
        ],
    })
}

fn use_reference_style_little_ops(ops: &KernelOpBundle) -> bool {
    matches!(ops.gather.kind, ReducerKind::Min)
        && ops.gather.identity == ReducerIdentity::PositiveInfinity
        && expr_uses_operand(&ops.apply.expr, &OperatorOperand::OldProp)
        && !expr_uses_operand(&ops.scatter.expr, &OperatorOperand::ScatterEdgeWeight)
}

fn use_reference_style_little(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> bool {
    edge.edge_weight_bits == 0 && use_reference_style_little_ops(ops)
}

fn use_identity_style_cc_little(ops: &KernelOpBundle) -> bool {
    matches!(ops.gather.kind, ReducerKind::Min)
        && ops.gather.identity == ReducerIdentity::PositiveInfinity
        && matches!(
            ops.apply.expr,
            OperatorExpr::Operand(OperatorOperand::GatherValue)
        )
}

fn extract_src_prop_from_bus_word() -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS INLINE")?));

    // Select the lane via shift+slice (typically a mux tree on `offset` bits),
    // instead of a fully-unrolled chain of (offset == lane) comparisons.
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("bit_low")?,
        ty: custom("ap_uint<9>"),
        init: Some(binary(
            HlsBinaryOp::Mul,
            HlsExpr::Identifier(ident("offset")?),
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        )),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("shifted")?,
        ty: custom("bus_word_t"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(ident("word")?),
            HlsExpr::Identifier(ident("bit_low")?),
        )),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("out")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("shifted")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                literal_uint(1),
            ),
            literal_uint(0),
        )?),
    }));

    body.push(HlsStatement::Return(Some(HlsExpr::Identifier(ident(
        "out",
    )?))));

    Ok(HlsFunction {
        linkage: None,
        name: ident("extract_src_prop_from_bus_word")?,
        return_type: custom("ap_fixed_pod_t"),
        params: vec![
            HlsParameter {
                name: ident("word")?,
                ty: custom("bus_word_t"),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: ident("offset")?,
                ty: custom("ap_uint<LOG_DIST_PER_WORD>"),
                passing: PassingStyle::Value,
            },
        ],
        body,
    })
}

fn stream2axistream() -> Result<HlsFunction, HlsTemplateError> {
    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t1")?,
        ty: custom("ppb_request_t"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("stream")?,
        target: ident("tmp_t1")?,
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t2")?,
        ty: custom("ppb_request_pkt_t"),
        init: None,
    }));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "data")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "request_round")?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "last")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "end_flag")?,
    ));
    loop_body.push(HlsStatement::StreamWrite {
        stream: ident("axi_stream")?,
        value: HlsExpr::Identifier(ident("tmp_t2")?),
    });
    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("stream2axistream")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("stream", "ppb_request_t")?,
            stream_param("axi_stream", "ppb_request_pkt_t")?,
        ],
        body: vec![HlsStatement::WhileLoop(HlsWhileLoop {
            label: LoopLabel::new("stream2axistream")?,
            condition: literal_bool(true),
            body: loop_body,
        })],
    })
}

fn axistream2stream() -> Result<HlsFunction, HlsTemplateError> {
    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t1")?,
        ty: custom("ppb_response_pkt_t"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("axi_stream")?,
        target: ident("tmp_t1")?,
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t2")?,
        ty: custom("ppb_response_t"),
        init: None,
    }));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "data")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "data")?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "addr")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "dest")?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "end_flag")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "last")?,
    ));
    loop_body.push(HlsStatement::StreamWrite {
        stream: ident("stream")?,
        value: HlsExpr::Identifier(ident("tmp_t2")?),
    });
    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("axistream2stream")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("axi_stream", "ppb_response_pkt_t")?,
            stream_param("stream", "ppb_response_t")?,
        ],
        body: vec![HlsStatement::WhileLoop(HlsWhileLoop {
            label: LoopLabel::new("axistream2stream")?,
            condition: literal_bool(true),
            body: loop_body,
        })],
    })
}

fn load_edges(
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edges_per_word")?,
        ty: HlsType::Int32,
        init: Some(HlsExpr::Identifier(ident("EDGES_PER_WORD")?)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("num_wide_reads")?,
        ty: HlsType::Int32,
        init: Some(binary(
            HlsBinaryOp::Div,
            HlsExpr::Identifier(ident("num_edges")?),
            HlsExpr::Identifier(ident("edges_per_word")?),
        )),
    }));

    let i_idx = ident("i")?;
    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("wide_word")?,
        ty: custom("bus_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("edge_props")?)),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        }),
    }));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edge_batch")?,
        ty: custom("edge_descriptor_batch_t"),
        init: None,
    }));

    let j_idx = ident("j")?;
    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let payload_shift = HlsExpr::Binary {
        op: HlsBinaryOp::Mul,
        left: Box::new(HlsExpr::Identifier(j_idx.clone())),
        right: Box::new(HlsExpr::Identifier(ident("EDGE_PAYLOAD_BITS")?)),
    };
    let payload_high = binary(
        HlsBinaryOp::Add,
        payload_shift.clone(),
        binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("EDGE_PAYLOAD_BITS")?),
            literal_uint(1),
        ),
    );
    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("packed_edge")?,
        ty: custom("ap_uint<EDGE_PAYLOAD_BITS>"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("wide_word")?),
            payload_high,
            payload_shift.clone(),
        )?),
    }));
    let edge_at_j = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(ident("edge_batch")?),
            "edges",
        )?),
        index: Box::new(HlsExpr::Identifier(j_idx.clone())),
    };
    inner_body.push(assignment(
        member_expr(edge_at_j.clone(), "dst_id")?,
        range_method(
            HlsExpr::Identifier(ident("packed_edge")?),
            literal_int(edge.local_id_bits as i64 - 1),
            literal_int(0),
        )?,
    ));
    inner_body.push(assignment(
        member_expr(edge_at_j.clone(), "src_id")?,
        range_method(
            HlsExpr::Identifier(ident("packed_edge")?),
            HlsExpr::Identifier(ident("EDGE_SRC_PAYLOAD_MSB")?),
            HlsExpr::Identifier(ident("EDGE_SRC_PAYLOAD_LSB")?),
        )?,
    ));
    if edge.edge_prop_bits > 0 {
        // Host packing layout is configuration-dependent: HBM stores edge
        // props above bit 63, while DDR compacts them into the upper bits of
        // the low 32-bit destination lane.
        inner_body.push(assignment(
            member_expr(edge_at_j.clone(), "edge_prop")?,
            range_method(
                HlsExpr::Identifier(ident("packed_edge")?),
                HlsExpr::Identifier(ident("EDGE_PROP_PAYLOAD_MSB")?),
                HlsExpr::Identifier(ident("EDGE_PROP_PAYLOAD_LSB")?),
            )?,
        ));
    }

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOAD_EDGES_INNER")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: j_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(j_idx.clone()),
            HlsExpr::Identifier(ident("edges_per_word")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(j_idx.clone()),
        ),
        body: inner_body,
    }));

    // Pad the remaining PE lanes when EDGES_PER_WORD < PE_NUM.
    // Uninitialized lanes can lead to undefined src/dst ids and deadlocks.
    let j_pad = ident("j_pad")?;
    let mut pad_body = Vec::new();
    pad_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    let pad_edge_at_j = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(ident("edge_batch")?),
            "edges",
        )?),
        index: Box::new(HlsExpr::Identifier(j_pad.clone())),
    };
    pad_body.push(assignment(
        member_expr(pad_edge_at_j.clone(), "dst_id")?,
        HlsExpr::Identifier(ident("INVALID_LOCAL_ID_LITTLE")?),
    ));
    let last_real_edge = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(ident("edge_batch")?),
            "edges",
        )?),
        index: Box::new(binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("edges_per_word")?),
            literal_int(1),
        )),
    };
    pad_body.push(assignment(
        member_expr(pad_edge_at_j.clone(), "src_id")?,
        member_expr(last_real_edge.clone(), "src_id")?,
    ));
    if edge.edge_prop_bits > 0 {
        pad_body.push(assignment(
            member_expr(pad_edge_at_j.clone(), "edge_prop")?,
            literal_uint(0),
        ));
    }
    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOAD_EDGES_PAD")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: j_pad.clone(),
            ty: HlsType::Int32,
            init: Some(HlsExpr::Identifier(ident("edges_per_word")?)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(j_pad.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(j_pad.clone()),
        ),
        body: pad_body,
    }));

    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("edge_stream")?,
        value: HlsExpr::Identifier(ident("edge_batch")?),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOAD_EDGES_OUTER")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("num_wide_reads")?),
        ),
        increment: LoopIncrement::Unary(HlsUnaryOp::PreIncrement, HlsExpr::Identifier(i_idx)),
        body: outer_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("load_edges")?,
        return_type: HlsType::Void,
        params: vec![
            HlsParameter {
                name: ident("edge_props")?,
                ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
                passing: PassingStyle::Value,
            },
            scalar_param("num_edges", HlsType::Int32)?,
            stream_param("edge_stream", "edge_descriptor_batch_t")?,
        ],
        body,
    })
}

fn request_manager(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let reference_style = use_reference_style_little(ops, edge);
    let src_prop_buffer = HlsType::array_with_exprs(
        custom("bus_word_t"),
        vec![
            "PE_NUM".to_string(),
            "2".to_string(),
            "(SRC_BUFFER_SIZE / DIST_PER_WORD)".to_string(),
        ],
    )?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("src_prop_buffer")?,
        ty: src_prop_buffer,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = src_prop_buffer dim = 1 complete",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS BIND_STORAGE variable = src_prop_buffer type = RAM_S2P impl = BRAM",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS dependence variable = src_prop_buffer inter false",
    )?));

    for name in ["pp_read_round", "pp_write_round", "pp_request_round"] {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident(name)?,
            ty: custom("ap_uint<22>"),
            init: Some(literal_uint(0)),
        }));
    }

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edge_set_cnt")?,
        ty: HlsType::Int32,
        init: Some(literal_int(0)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("wait_flag")?,
        ty: HlsType::Bool,
        init: Some(literal_bool(false)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("an_edge_burst")?,
        ty: custom("edge_descriptor_batch_t"),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = an_edge_burst.edges dim = 1 complete",
    )?));
    // Guard the scatter against overflow when the source property is
    // INFINITY_POD. Only the DDR flow intentionally relies on zero-sentinel
    // overflow-to-zero semantics here; HBM still needs the guard even when
    // zero_sentinel is enabled elsewhere in the data path.
    let guard_infinity = ops.gather.identity == ReducerIdentity::PositiveInfinity
        && expr_uses_operand(&ops.scatter.expr, &OperatorOperand::ScatterSrcProp)
        && !(edge.zero_sentinel && edge.allow_scatter_inf_overflow_to_zero);
    if edge.edge_weight_bits == 0 {
        let edge_weight_expr = binary(
            HlsBinaryOp::Shl,
            literal_uint(1),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                HlsExpr::Identifier(ident("DISTANCE_INTEGER_PART")?),
            ),
        );
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("edge_weight")?,
            ty: custom("ap_fixed_pod_t"),
            init: Some(edge_weight_expr),
        }));
    }
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("identity_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(reducer_identity_expr(ops.gather.identity)?),
    }));

    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    // Fill ping-pong buffer
    let mut req_body = Vec::new();
    req_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_ppb_request")?,
        ty: custom("ppb_request_t"),
        init: None,
    }));
    req_body.push(assignment(
        member_expr(
            HlsExpr::Identifier(ident("one_ppb_request")?),
            "request_round",
        )?,
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("pp_request_round")?),
            HlsExpr::Identifier(ident("memory_offset")?),
        ),
    ));
    req_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_ppb_request")?), "end_flag")?,
        literal_uint(0),
    ));
    // Match the known-good reference flow: use a blocking write to ensure the
    // request is actually issued before we proceed.
    req_body.push(HlsStatement::StreamWrite {
        stream: ident("ppb_request_stm")?,
        value: HlsExpr::Identifier(ident("one_ppb_request")?),
    });
    req_body.push(assignment(
        HlsExpr::Identifier(ident("pp_request_round")?),
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("pp_request_round")?),
            literal_uint(1),
        ),
    ));

    // Ensure `pp_request_round - pp_read_round` never underflows (ap_uint wraparound),
    // otherwise a jump in `pp_read_round` can permanently stop issuing requests.
    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(ident("pp_request_round")?),
            HlsExpr::Identifier(ident("pp_read_round")?),
        ),
        then_body: vec![assignment(
            HlsExpr::Identifier(ident("pp_request_round")?),
            HlsExpr::Identifier(ident("pp_read_round")?),
        )],
        else_body: Vec::new(),
    }));

    loop_body.push(HlsStatement::Comment(
        "Prefetch one round ahead. We treat \"seeing any response for round R\"".to_string(),
    ));
    loop_body.push(HlsStatement::Comment(
        "as evidence that the previous round (R-1) has completed, because".to_string(),
    ));
    loop_body.push(HlsStatement::Comment(
        "`hbm_writer::little_node_prop_loader` services one request at a time".to_string(),
    ));
    loop_body.push(HlsStatement::Comment(
        "(emits a full SRC_BUFFER_WORDS burst per request).".to_string(),
    ));

    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Le,
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("pp_request_round")?),
                HlsExpr::Identifier(ident("pp_read_round")?),
            ),
            literal_uint(1),
        ),
        then_body: req_body,
        else_body: Vec::new(),
    }));

    // read_nb handling
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_ppb_response")?,
        ty: custom("ppb_response_t"),
        init: None,
    }));

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("got_ppb_response")?,
        ty: HlsType::Bool,
        init: Some(method_call(
            HlsExpr::Identifier(ident("ppb_response_stm")?),
            "read_nb",
            vec![HlsExpr::Identifier(ident("one_ppb_response")?)],
        )?),
    }));

    let mut resp_body = Vec::new();
    resp_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pp_write_round_resp")?,
        ty: custom("ap_uint<22>"),
        init: Some(binary(
            HlsBinaryOp::Sub,
            binary(
                HlsBinaryOp::Shr,
                member_expr(HlsExpr::Identifier(ident("one_ppb_response")?), "addr")?,
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("LOG_SRC_BUFFER_SIZE")?),
                    HlsExpr::Identifier(ident("LOG_DIST_PER_WORD")?),
                ),
            ),
            HlsExpr::Identifier(ident("memory_offset")?),
        )),
    }));

    resp_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("write_buffer")?,
        ty: HlsType::Bool,
        init: Some(range_method(
            HlsExpr::Identifier(ident("pp_write_round_resp")?),
            literal_int(0),
            literal_int(0),
        )?),
    }));
    resp_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("write_idx")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::BitAnd,
            member_expr(HlsExpr::Identifier(ident("one_ppb_response")?), "addr")?,
            binary(
                HlsBinaryOp::Sub,
                binary(
                    HlsBinaryOp::Div,
                    HlsExpr::Identifier(ident("SRC_BUFFER_SIZE")?),
                    HlsExpr::Identifier(ident("DIST_PER_WORD")?),
                ),
                literal_uint(1),
            ),
        )),
    }));
    resp_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_read_burst")?,
        ty: custom("bus_word_t"),
        init: Some(member_expr(
            HlsExpr::Identifier(ident("one_ppb_response")?),
            "data",
        )?),
    }));

    let u_idx = ident("u")?;
    resp_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_14")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: u_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(u_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(u_idx.clone()),
        ),
        body: vec![
            HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
            assignment(
                HlsExpr::Index {
                    target: Box::new(HlsExpr::Index {
                        target: Box::new(HlsExpr::Index {
                            target: Box::new(HlsExpr::Identifier(ident("src_prop_buffer")?)),
                            index: Box::new(HlsExpr::Identifier(u_idx.clone())),
                        }),
                        index: Box::new(HlsExpr::Identifier(ident("write_buffer")?)),
                    }),
                    index: Box::new(HlsExpr::Identifier(ident("write_idx")?)),
                },
                HlsExpr::Identifier(ident("one_read_burst")?),
            ),
        ],
    }));

    // Track progress of the loader by observing the response round. Since the
    // writer services one request at a time, seeing any response for round R
    // implies round R-1 is complete (prefetch semantics).
    //
    resp_body.push(assignment(
        HlsExpr::Identifier(ident("pp_write_round")?),
        HlsExpr::Identifier(ident("pp_write_round_resp")?),
    ));

    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Identifier(ident("got_ppb_response")?),
        then_body: resp_body,
        else_body: Vec::new(),
    }));

    // read edge burst if not waiting
    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(HlsExpr::Identifier(ident("wait_flag")?)),
        },
        then_body: vec![HlsStatement::StreamRead {
            stream: ident("edge_burst_stm")?,
            target: ident("an_edge_burst")?,
        }],
        else_body: Vec::new(),
    }));

    // update counters and flags
    loop_body.push(assignment(
        HlsExpr::Identifier(ident("pp_read_round")?),
        binary(
            HlsBinaryOp::Div,
            member_expr(
                HlsExpr::Index {
                    target: Box::new(member_expr(
                        HlsExpr::Identifier(ident("an_edge_burst")?),
                        "edges",
                    )?),
                    index: Box::new(literal_int(0)),
                },
                "src_id",
            )?,
            HlsExpr::Identifier(ident("SRC_BUFFER_SIZE")?),
        ),
    ));
    loop_body.push(assignment(
        HlsExpr::Identifier(ident("wait_flag")?),
        binary(
            HlsBinaryOp::Ge,
            HlsExpr::Identifier(ident("pp_read_round")?),
            HlsExpr::Identifier(ident("pp_write_round")?),
        ),
    ));

    let exit_condition = HlsExpr::Ternary {
        condition: Box::new(binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("wait_flag")?),
            literal_bool(false),
        )),
        then_expr: Box::new(binary(
            HlsBinaryOp::Ge,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(ident("edge_set_cnt")?),
                literal_int(1),
            ),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        )),
        else_expr: Box::new(binary(
            HlsBinaryOp::Ge,
            HlsExpr::Identifier(ident("edge_set_cnt")?),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        )),
    };
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("exit_flag")?,
        ty: HlsType::Bool,
        init: Some(exit_condition),
    }));

    // process update_set when not waiting
    let mut update_body = Vec::new();
    update_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("read_buffer")?,
        ty: HlsType::Bool,
        init: Some(range_method(
            HlsExpr::Identifier(ident("pp_read_round")?),
            literal_int(0),
            literal_int(0),
        )?),
    }));
    update_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("an_update_set")?,
        ty: custom("update_tuple_t_little"),
        init: None,
    }));
    update_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = an_update_set.data dim = 1 complete",
    )?));

    let u_idx = ident("u")?;
    let mut update_loop_body = Vec::new();
    update_loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("idx")?,
        ty: custom("ap_uint<31>"),
        init: Some(binary(
            HlsBinaryOp::Mod,
            member_expr(
                HlsExpr::Index {
                    target: Box::new(member_expr(
                        HlsExpr::Identifier(ident("an_edge_burst")?),
                        "edges",
                    )?),
                    index: Box::new(HlsExpr::Identifier(u_idx.clone())),
                },
                "src_id",
            )?,
            HlsExpr::Identifier(ident("SRC_BUFFER_SIZE")?),
        )),
    }));
    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("uram_row_idx")?,
        ty: custom("ap_uint<30>"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(ident("idx")?),
            HlsExpr::Identifier(ident("LOG_DIST_PER_WORD")?),
        )),
    }));
    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("uram_row_offset")?,
        ty: custom("ap_uint<30>"),
        init: Some(binary(
            HlsBinaryOp::BitAnd,
            HlsExpr::Identifier(ident("idx")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
                literal_uint(1),
            ),
        )),
    }));
    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("uram_row")?,
        ty: custom("bus_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(ident("src_prop_buffer")?)),
                    index: Box::new(HlsExpr::Identifier(u_idx.clone())),
                }),
                index: Box::new(HlsExpr::Identifier(ident("read_buffer")?)),
            }),
            index: Box::new(HlsExpr::Identifier(ident("uram_row_idx")?)),
        }),
    }));

    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("src_prop")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(HlsExpr::Call {
            function: ident("extract_src_prop_from_bus_word")?,
            args: vec![
                HlsExpr::Identifier(ident("uram_row")?),
                HlsExpr::Identifier(ident("uram_row_offset")?),
            ],
        }),
    }));

    let edge_entry = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(ident("an_edge_burst")?),
            "edges",
        )?),
        index: Box::new(HlsExpr::Identifier(u_idx.clone())),
    };

    if edge.edge_weight_bits > 0 {
        let weight_high = edge.edge_weight_lsb + edge.edge_weight_bits - 1;
        let weight_range = range_method(
            member_expr(edge_entry.clone(), "edge_prop")?,
            literal_uint(weight_high as u64),
            literal_uint(edge.edge_weight_lsb as u64),
        )?;
        let weight_cast = HlsExpr::Cast {
            target_type: custom("ap_fixed_pod_t"),
            expr: Box::new(weight_range),
        };
        let weight_expr = if edge.edge_weight_shift > 0 {
            binary(
                HlsBinaryOp::Shl,
                weight_cast,
                literal_uint(edge.edge_weight_shift as u64),
            )
        } else if edge.edge_weight_shift < 0 {
            binary(
                HlsBinaryOp::Shr,
                weight_cast,
                literal_uint(edge.edge_weight_shift.abs() as u64),
            )
        } else {
            weight_cast
        };
        update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("edge_weight")?,
            ty: custom("ap_fixed_pod_t"),
            init: Some(weight_expr),
        }));
    }

    let src_prop_ident = ident("src_prop")?;
    let edge_weight_ident = ident("edge_weight")?;

    let scatter_expr = {
        let mut leaf_mapper = |opnd: &OperatorOperand| match opnd {
            OperatorOperand::ScatterSrcProp => Some(HlsExpr::Identifier(src_prop_ident.clone())),
            OperatorOperand::ScatterEdgeWeight => {
                Some(HlsExpr::Identifier(edge_weight_ident.clone()))
            }
            OperatorOperand::ScatterSrcId => member_expr(edge_entry.clone(), "src_id").ok(),
            OperatorOperand::ScatterDstId => member_expr(edge_entry.clone(), "dst_id").ok(),
            OperatorOperand::ConstInt(v) => Some(HlsExpr::Cast {
                target_type: custom("ap_fixed_pod_t"),
                expr: Box::new(HlsExpr::Literal(HlsLiteral::Int(*v))),
            }),
            OperatorOperand::ConstFloat(v) => Some(HlsExpr::Literal(HlsLiteral::Float(*v))),
            _ => None,
        };
        cast_ternary_branches(
            render_operator_expr(&ops.scatter.expr, &mut leaf_mapper)?,
            custom("ap_fixed_pod_t"),
        )
    };
    let guarded_expr = if guard_infinity {
        let scatter_cast = HlsExpr::Cast {
            target_type: custom("ap_fixed_pod_t"),
            expr: Box::new(scatter_expr.clone()),
        };
        HlsExpr::Ternary {
            condition: Box::new(binary(
                HlsBinaryOp::Eq,
                HlsExpr::Identifier(src_prop_ident.clone()),
                HlsExpr::Identifier(ident("INFINITY_POD")?),
            )),
            then_expr: Box::new(HlsExpr::Identifier(ident("INFINITY_POD")?)),
            else_expr: Box::new(scatter_cast),
        }
    } else {
        scatter_expr
    };
    update_loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("BinOp_68_res")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(guarded_expr),
    }));
    let dst_id_expr = member_expr(edge_entry.clone(), "dst_id")?;
    let update_prop_expr = member_expr(
        HlsExpr::Index {
            target: Box::new(member_expr(
                HlsExpr::Identifier(ident("an_update_set")?),
                "data",
            )?),
            index: Box::new(HlsExpr::Identifier(u_idx.clone())),
        },
        "prop",
    )?;
    let update_node_expr = member_expr(
        HlsExpr::Index {
            target: Box::new(member_expr(
                HlsExpr::Identifier(ident("an_update_set")?),
                "data",
            )?),
            index: Box::new(HlsExpr::Identifier(u_idx.clone())),
        },
        "node_id",
    )?;

    if reference_style || edge.zero_sentinel {
        update_loop_body.push(assignment(
            update_prop_expr,
            HlsExpr::Identifier(ident("BinOp_68_res")?),
        ));
        update_loop_body.push(assignment(update_node_expr, dst_id_expr));
    } else {
        update_loop_body.push(HlsStatement::IfElse(HlsIfElse {
            condition: binary(
                HlsBinaryOp::Lt,
                dst_id_expr.clone(),
                HlsExpr::Identifier(ident("MAX_NUM")?),
            ),
            then_body: vec![
                assignment(
                    update_prop_expr.clone(),
                    HlsExpr::Identifier(ident("BinOp_68_res")?),
                ),
                assignment(update_node_expr.clone(), dst_id_expr.clone()),
            ],
            else_body: vec![
                assignment(
                    update_prop_expr,
                    HlsExpr::Identifier(ident("identity_pod")?),
                ),
                assignment(update_node_expr, dst_id_expr),
            ],
        }));
    }

    update_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_15")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: u_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(u_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(HlsUnaryOp::PreIncrement, HlsExpr::Identifier(u_idx)),
        body: update_loop_body,
    }));
    update_body.push(HlsStatement::StreamWrite {
        stream: ident("update_set_stm")?,
        value: HlsExpr::Identifier(ident("an_update_set")?),
    });
    update_body.push(assignment(
        HlsExpr::Identifier(ident("edge_set_cnt")?),
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("edge_set_cnt")?),
            literal_int(1),
        ),
    ));

    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(HlsExpr::Identifier(ident("wait_flag")?)),
        },
        then_body: update_body,
        else_body: Vec::new(),
    }));

    // exit handling
    let mut exit_body = Vec::new();
    exit_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_ppb_request")?,
        ty: custom("ppb_request_t"),
        init: None,
    }));
    exit_body.push(assignment(
        member_expr(
            HlsExpr::Identifier(ident("one_ppb_request")?),
            "request_round",
        )?,
        literal_uint(0),
    ));
    exit_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_ppb_request")?), "end_flag")?,
        literal_uint(1),
    ));

    // Match the reference: send one end request and then drain until we observe
    // the end response.
    exit_body.push(HlsStatement::StreamWrite {
        stream: ident("ppb_request_stm")?,
        value: HlsExpr::Identifier(ident("one_ppb_request")?),
    });

    let mut drain_body = Vec::new();
    drain_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_ppb_response")?,
        ty: custom("ppb_response_t"),
        init: None,
    }));
    drain_body.push(HlsStatement::StreamRead {
        stream: ident("ppb_response_stm")?,
        target: ident("one_ppb_response")?,
    });
    drain_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(ident("one_ppb_response")?), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));

    exit_body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_16")?,
        condition: literal_bool(true),
        body: drain_body,
    }));
    exit_body.push(HlsStatement::Break);

    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Identifier(ident("exit_flag")?),
        then_body: exit_body,
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_17")?,
        condition: literal_bool(true),
        body: loop_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("request_manager")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("edge_burst_stm", "edge_descriptor_batch_t")?,
            stream_param("ppb_request_stm", "ppb_request_t")?,
            stream_param("ppb_response_stm", "ppb_response_t")?,
            stream_param("update_set_stm", "update_tuple_t_little")?,
            scalar_param("memory_offset", HlsType::UInt32)?,
            scalar_param("total_edge_sets", HlsType::UInt32)?,
        ],
        body,
    })
}

fn reduce_unit(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let reference_style = use_reference_style_little(ops, edge);
    let zero_sentinel_mode =
        edge.zero_sentinel && !reference_style && !use_identity_style_cc_little(ops);
    let _mem_size = ident("MEM_SIZE")?;
    let prop_mem = ident("prop_mem")?;
    let cache_data_buffer = ident("cache_data_buffer")?;
    let cache_addr_buffer = ident("cache_addr_buffer")?;
    let identity_val = ident("identity_val")?;
    let identity_word = ident("identity_word")?;
    let init_idx = ident("init_idx")?;
    let mem_size_expr = binary(
        HlsBinaryOp::Div,
        HlsExpr::Identifier(ident("MAX_NUM")?),
        HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
    );

    let mut body = Vec::new();
    let _ = mem_size_expr;
    body.push(HlsStatement::Raw(
        "const int32_t MEM_SIZE = (MAX_NUM / DISTANCES_PER_REDUCE_WORD);".to_string(),
    ));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: prop_mem.clone(),
        ty: HlsType::array_with_exprs(
            custom("reduce_word_t"),
            vec!["PE_NUM".to_string(), "MEM_SIZE".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = prop_mem complete dim = 1",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS BIND_STORAGE variable = prop_mem type = RAM_S2P impl = URAM",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS dependence variable = prop_mem inter false",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_data_buffer.clone(),
        ty: HlsType::array_with_exprs(
            custom("reduce_word_t"),
            vec!["PE_NUM".to_string(), "(L + 1)".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_data_buffer complete dim = 0",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_addr_buffer.clone(),
        ty: HlsType::array_with_exprs(
            custom("local_id_t"),
            vec!["PE_NUM".to_string(), "(L + 1)".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_addr_buffer complete dim = 0",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: identity_val.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(if reference_style || zero_sentinel_mode {
            literal_uint(0)
        } else {
            reducer_identity_expr(ops.gather.identity)?
        }),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: identity_word.clone(),
        ty: custom("reduce_word_t"),
        init: None,
    }));
    let dist_idx = ident("dist_idx")?;
    let mut identity_fill_body = Vec::new();
    identity_fill_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    identity_fill_body.push(assignment(
        range_method(
            HlsExpr::Identifier(identity_word.clone()),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?,
        HlsExpr::Identifier(identity_val.clone()),
    ));
    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("INIT_IDENTITY_WORD")?,
        init: LoopInitializer::Declaration(int_decl("dist_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(dist_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(dist_idx.clone()),
        ),
        body: identity_fill_body,
    }));

    if !reference_style {
        // Initialize the active reduce-memory range on every invocation.
        // Relying on URAM power-up contents causes hardware-only corruption in
        // PageRank and any other zero-sentinel reducers.
        let init_pe = ident("init_pe")?;
        let mut init_inner = Vec::new();
        init_inner.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
        init_inner.push(assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
                    index: Box::new(HlsExpr::Identifier(init_pe.clone())),
                }),
                index: Box::new(HlsExpr::Identifier(init_idx.clone())),
            },
            HlsExpr::Identifier(identity_word.clone()),
        ));
        body.push(HlsStatement::ForLoop(HlsForLoop {
            label: LoopLabel::new("INIT_REDUCE_MEM_PE")?,
            init: LoopInitializer::Declaration(HlsVarDecl {
                name: init_pe.clone(),
                ty: HlsType::Int32,
                init: Some(literal_int(0)),
            }),
            condition: binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(init_pe.clone()),
                HlsExpr::Identifier(ident("PE_NUM")?),
            ),
            increment: LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(init_pe.clone()),
            ),
            body: vec![HlsStatement::ForLoop(HlsForLoop {
                label: LoopLabel::new("INIT_REDUCE_MEM_IDX")?,
                init: LoopInitializer::Declaration(HlsVarDecl {
                    name: init_idx.clone(),
                    ty: HlsType::Int32,
                    init: Some(literal_int(0)),
                }),
                condition: binary(
                    HlsBinaryOp::Lt,
                    HlsExpr::Identifier(init_idx.clone()),
                    HlsExpr::Identifier(ident("rounded_num_words")?),
                ),
                increment: LoopIncrement::Unary(
                    HlsUnaryOp::PreIncrement,
                    HlsExpr::Identifier(init_idx.clone()),
                ),
                body: init_inner,
            })],
        }));
    }

    // Cache initialization
    let i_idx = ident("i")?;
    let pe_idx = ident("pe")?;
    let mut inner_init_body = Vec::new();
    inner_init_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    inner_init_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            }),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        literal_uint(0),
    ));
    inner_init_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            }),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        if reference_style || zero_sentinel_mode {
            literal_uint(0)
        } else {
            HlsExpr::Identifier(identity_word.clone())
        },
    ));

    let mut outer_init_body = Vec::new();
    outer_init_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    outer_init_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_31")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: inner_init_body,
    }));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_32")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(ident("L")?),
                literal_int(1),
            ),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: outer_init_body,
    }));

    // Aggregation loop
    let update_idx = ident("update_idx")?;
    let one_update = ident("one_update")?;
    let mut aggregate_body = Vec::new();
    aggregate_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    aggregate_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: one_update.clone(),
        ty: custom("update_tuple_t_little"),
        init: None,
    }));
    aggregate_body.push(HlsStatement::StreamRead {
        stream: ident("update_set_stm")?,
        target: one_update.clone(),
    });

    let pe_idx_inner = ident("pe")?;
    let mut per_pe_body = Vec::new();
    per_pe_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let update_data = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(one_update.clone()),
            "data",
        )?),
        index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
    };

    let node_id_expr = member_expr(update_data.clone(), "node_id")?;
    let guard = binary(
        HlsBinaryOp::Eq,
        range_method(
            node_id_expr.clone(),
            HlsExpr::Identifier(ident("LOCAL_ID_MSB")?),
            HlsExpr::Identifier(ident("LOCAL_ID_MSB")?),
        )?,
        literal_int(0),
    );

    let mut guard_body = Vec::new();
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("key")?,
        ty: custom("local_id_t"),
        init: Some(node_id_expr.clone()),
    }));
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("incoming_dist_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(member_expr(update_data.clone(), "prop")?),
    }));
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("word_addr")?,
        ty: custom("local_id_t"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(ident("key")?),
            HlsExpr::Identifier(ident("LOG_DISTANCES_PER_REDUCE_WORD")?),
        )),
    }));

    let prop_mem_entry = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(HlsExpr::Identifier(ident("word_addr")?)),
    };

    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("current_word")?,
        ty: custom("reduce_word_t"),
        init: Some(prop_mem_entry.clone()),
    }));

    // Cache search
    let i_search = ident("i")?;
    let mut search_body = Vec::new();
    search_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let cache_addr_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(HlsExpr::Identifier(i_search.clone())),
    };
    let cache_data_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(HlsExpr::Identifier(i_search.clone())),
    };

    search_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            cache_addr_idx.clone(),
            HlsExpr::Identifier(ident("word_addr")?),
        ),
        then_body: vec![
            assignment(
                HlsExpr::Identifier(ident("current_word")?),
                cache_data_idx.clone(),
            ),
            HlsStatement::Break,
        ],
        else_body: Vec::new(),
    }));

    guard_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_33")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_search.clone(),
            ty: HlsType::Int32,
            init: Some(HlsExpr::Identifier(ident("L")?)),
        }),
        condition: binary(
            HlsBinaryOp::Ge,
            HlsExpr::Identifier(i_search.clone()),
            literal_int(0),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreDecrement,
            HlsExpr::Identifier(i_search.clone()),
        ),
        body: search_body,
    }));

    // Cache shift
    let i_shift = ident("i")?;
    let mut shift_body = Vec::new();
    shift_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let next_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(i_shift.clone()),
            literal_int(1),
        )),
    };
    let cur_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(HlsExpr::Identifier(i_shift.clone())),
    };
    shift_body.push(assignment(cur_idx, next_idx));

    let next_data_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(i_shift.clone()),
            literal_int(1),
        )),
    };
    let cur_data_idx = HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
        }),
        index: Box::new(HlsExpr::Identifier(i_shift.clone())),
    };
    shift_body.push(assignment(cur_data_idx, next_data_idx));

    guard_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_34")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_shift.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_shift.clone()),
            HlsExpr::Identifier(ident("L")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_shift.clone()),
        ),
        body: shift_body,
    }));

    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("slot")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::BitAnd,
            HlsExpr::Identifier(ident("key")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                literal_uint(1),
            ),
        )),
    }));
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("bit_low")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Mul,
            HlsExpr::Identifier(ident("slot")?),
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        )),
    }));
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("bit_high")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("bit_low")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                literal_uint(1),
            ),
        )),
    }));
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("current_val")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("current_word")?),
            HlsExpr::Identifier(ident("bit_high")?),
            HlsExpr::Identifier(ident("bit_low")?),
        )?),
    }));
    let reduce_expr = if reference_style || zero_sentinel_mode {
        reducer_combine_expr_zero_sentinel(
            ops.gather.kind,
            HlsExpr::Identifier(ident("current_val")?),
            HlsExpr::Identifier(ident("incoming_dist_pod")?),
            Some(custom("ap_fixed_pod_t")),
            false, // reduce: check current
        )
    } else {
        reducer_combine_expr(
            ops.gather.kind,
            HlsExpr::Identifier(ident("current_val")?),
            HlsExpr::Identifier(ident("incoming_dist_pod")?),
            HlsExpr::Identifier(identity_val.clone()),
            Some(custom("ap_fixed_pod_t")),
        )
    };
    guard_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("updated_val")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(reduce_expr),
    }));
    guard_body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("current_word")?),
            HlsExpr::Identifier(ident("bit_high")?),
            HlsExpr::Identifier(ident("bit_low")?),
        )?,
        HlsExpr::Identifier(ident("updated_val")?),
    ));
    guard_body.push(assignment(
        prop_mem_entry.clone(),
        HlsExpr::Identifier(ident("current_word")?),
    ));
    guard_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
            }),
            index: Box::new(HlsExpr::Identifier(ident("L")?)),
        },
        HlsExpr::Identifier(ident("current_word")?),
    ));

    guard_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx_inner.clone())),
            }),
            index: Box::new(HlsExpr::Identifier(ident("L")?)),
        },
        HlsExpr::Identifier(ident("word_addr")?),
    ));

    per_pe_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: guard,
        then_body: guard_body,
        else_body: Vec::new(),
    }));

    aggregate_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_35")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_idx_inner.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx_inner.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx_inner.clone()),
        ),
        body: per_pe_body,
    }));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_36")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: update_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(update_idx.clone()),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        ),
        increment: LoopIncrement::Unary(HlsUnaryOp::PreIncrement, HlsExpr::Identifier(update_idx)),
        body: aggregate_body,
    }));

    // Stream out aggregated memory
    let i_out = ident("i")?;
    let mut stream_out_body = Vec::new();
    stream_out_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE")?));

    let pe_idx_out = ident("pe")?;

    let prop_mem_pe_i = |pe_expr: HlsExpr| HlsExpr::Index {
        target: Box::new(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(pe_expr),
        }),
        index: Box::new(HlsExpr::Identifier(i_out.clone())),
    };

    let mut pe1_body = Vec::new();
    pe1_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    let word_tmp = ident("word")?;
    pe1_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: word_tmp.clone(),
        ty: custom("reduce_word_t"),
        init: Some(HlsExpr::Identifier(identity_word.clone())),
    }));
    pe1_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx_out.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        then_body: vec![
            assignment(
                HlsExpr::Identifier(word_tmp.clone()),
                prop_mem_pe_i(HlsExpr::Identifier(pe_idx_out.clone())),
            ),
            assignment(
                prop_mem_pe_i(HlsExpr::Identifier(pe_idx_out.clone())),
                HlsExpr::Identifier(identity_word.clone()),
            ),
        ],
        else_body: Vec::new(),
    }));
    let pe1_stream = HlsExpr::Index {
        target: Box::new(HlsExpr::Identifier(ident("pe_mem_outs_1")?)),
        index: Box::new(HlsExpr::Identifier(pe_idx_out.clone())),
    };
    pe1_body.push(HlsStatement::Expr(method_call(
        pe1_stream,
        "write",
        vec![HlsExpr::Identifier(word_tmp.clone())],
    )?));

    stream_out_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_37")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_idx_out.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx_out.clone()),
            literal_int(4),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx_out.clone()),
        ),
        body: pe1_body,
    }));

    if edge.little_pe > 4 {
        let mut pe2_body = Vec::new();
        pe2_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
        let word_tmp2 = ident("word")?;
        let pe_offset = binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(pe_idx_out.clone()),
            literal_int(4),
        );
        pe2_body.push(HlsStatement::Declaration(HlsVarDecl {
            name: word_tmp2.clone(),
            ty: custom("reduce_word_t"),
            init: Some(HlsExpr::Identifier(identity_word.clone())),
        }));
        pe2_body.push(HlsStatement::IfElse(HlsIfElse {
            condition: binary(
                HlsBinaryOp::Lt,
                pe_offset.clone(),
                HlsExpr::Identifier(ident("PE_NUM")?),
            ),
            then_body: vec![
                assignment(
                    HlsExpr::Identifier(word_tmp2.clone()),
                    prop_mem_pe_i(pe_offset.clone()),
                ),
                assignment(
                    prop_mem_pe_i(pe_offset.clone()),
                    HlsExpr::Identifier(identity_word.clone()),
                ),
            ],
            else_body: Vec::new(),
        }));
        let pe2_stream = HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("pe_mem_outs_2")?)),
            index: Box::new(HlsExpr::Identifier(pe_idx_out.clone())),
        };
        pe2_body.push(HlsStatement::Expr(method_call(
            pe2_stream,
            "write",
            vec![HlsExpr::Identifier(word_tmp2)],
        )?));

        stream_out_body.push(HlsStatement::ForLoop(HlsForLoop {
            label: LoopLabel::new("LOOP_FOR_38")?,
            init: LoopInitializer::Declaration(HlsVarDecl {
                name: pe_idx_out.clone(),
                ty: HlsType::Int32,
                init: Some(literal_int(0)),
            }),
            condition: binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(pe_idx_out.clone()),
                literal_int(4),
            ),
            increment: LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(pe_idx_out.clone()),
            ),
            body: pe2_body,
        }));
    }

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_39")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_out.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_out.clone()),
            HlsExpr::Identifier(ident("rounded_num_words")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_out.clone()),
        ),
        body: stream_out_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_unit_reduce")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("update_set_stm", "update_tuple_t_little")?,
            stream_array_param("pe_mem_outs_1", "reduce_word_t", vec![4])?,
            stream_array_param("pe_mem_outs_2", "reduce_word_t", vec![4])?,
            scalar_param("total_edge_sets", HlsType::UInt32)?,
            scalar_param("rounded_num_words", HlsType::UInt32)?,
        ],
        body,
    })
}

fn partial_drain_impl(
    ops: &KernelOpBundle,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let reference_style = use_reference_style_little_ops(ops);
    let zero_sentinel_mode =
        zero_sentinel && !reference_style && !use_identity_style_cc_little(ops);
    let identity_pod = ident("identity_pod")?;
    let rounded_num_words = ident("rounded_num_words")?;
    let pe_mem_in = ident("pe_mem_in")?;
    let partial_out_stream = ident("partial_out_stream")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));

    let i_idx = ident("i")?;
    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("uram_res")?,
        ty: HlsType::array_with_exprs(
            custom("ap_fixed_pod_t"),
            vec!["DISTANCES_PER_REDUCE_WORD".to_string()],
        )?,
        init: None,
    }));
    outer_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = uram_res complete dim = 0",
    )?));
    let dist_idx = ident("dist_idx")?;
    let mut init_res_body = Vec::new();
    init_res_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    init_res_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
            index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
        },
        HlsExpr::Identifier(identity_pod.clone()),
    ));
    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("INIT_URAM_RES")?,
        init: LoopInitializer::Declaration(int_decl("dist_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(dist_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(dist_idx.clone()),
        ),
        body: init_res_body,
    }));

    let pe_idx = ident("pe_idx")?;
    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    let pe_stream = HlsExpr::Index {
        target: Box::new(HlsExpr::Identifier(pe_mem_in.clone())),
        index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
    };
    inner_body.push(assignment(
        HlsExpr::Identifier(ident("word")?),
        method_call(pe_stream, "read", Vec::new())?,
    ));

    let dist_idx = ident("dist_idx")?;
    let mut reduce_body = Vec::new();
    reduce_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    reduce_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("incoming_dist_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("word")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?),
    }));
    let partial_reduce_expr = if reference_style {
        // Reference-style Min drain: (uram < incoming || incoming == 0) ? uram : incoming
        HlsExpr::Ternary {
            condition: Box::new(binary(
                HlsBinaryOp::Or,
                binary(
                    HlsBinaryOp::Lt,
                    HlsExpr::Index {
                        target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
                        index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
                    },
                    HlsExpr::Identifier(ident("incoming_dist_pod")?),
                ),
                binary(
                    HlsBinaryOp::Eq,
                    HlsExpr::Identifier(ident("incoming_dist_pod")?),
                    literal_uint(0),
                ),
            )),
            then_expr: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
                index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
            }),
            else_expr: Box::new(HlsExpr::Identifier(ident("incoming_dist_pod")?)),
        }
    } else if zero_sentinel_mode {
        // Generic zero-sentinel drain
        reducer_combine_expr_zero_sentinel(
            ops.gather.kind,
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
                index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
            },
            HlsExpr::Identifier(ident("incoming_dist_pod")?),
            Some(custom("ap_fixed_pod_t")),
            true, // drain: check incoming (memory values can be 0)
        )
    } else {
        reducer_combine_expr(
            ops.gather.kind,
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
                index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
            },
            HlsExpr::Identifier(ident("incoming_dist_pod")?),
            HlsExpr::Identifier(identity_pod.clone()),
            Some(custom("ap_fixed_pod_t")),
        )
    };
    reduce_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
            index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
        },
        partial_reduce_expr,
    ));
    inner_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("REDUCE_DISTANCES")?,
        init: LoopInitializer::Declaration(int_decl("dist_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(dist_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(dist_idx.clone()),
        ),
        body: reduce_body,
    }));

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_40")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            literal_int(4),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: inner_body,
    }));

    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merged_word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    let dist_idx = ident("dist_idx")?;
    let mut pack_body = Vec::new();
    pack_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    pack_body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("merged_word")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?,
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("uram_res")?)),
            index: Box::new(HlsExpr::Identifier(dist_idx.clone())),
        },
    ));
    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("PACK_MERGED_WORD")?,
        init: LoopInitializer::Declaration(int_decl("dist_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(dist_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(dist_idx.clone()),
        ),
        body: pack_body,
    }));
    outer_body.push(HlsStatement::StreamWrite {
        stream: partial_out_stream.clone(),
        value: HlsExpr::Identifier(ident("merged_word")?),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_41")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(rounded_num_words.clone()),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: outer_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_partial_drain_impl")?,
        return_type: HlsType::Void,
        params: vec![
            scalar_param("i", HlsType::Int32)?,
            stream_array_param("pe_mem_in", "reduce_word_t", vec![4])?,
            stream_param("partial_out_stream", "reduce_word_t")?,
            scalar_param("rounded_num_words", HlsType::UInt32)?,
            scalar_param("identity_pod", custom("ap_fixed_pod_t"))?,
        ],
        body,
    })
}

fn finalize_drain_single(_ops: &KernelOpBundle) -> Result<HlsFunction, HlsTemplateError> {
    let partial_in = ident("partial_in")?;
    let kernel_out_stream = ident("kernel_out_stream")?;
    let rounded_num_words = ident("rounded_num_words")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_write_burst")?,
        ty: custom("little_out_pkt_t"),
        init: None,
    }));
    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "last")?,
        literal_bool(false),
    ));

    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    loop_body.push(assignment(
        HlsExpr::Identifier(ident("word")?),
        method_call(HlsExpr::Identifier(partial_in.clone()), "read", Vec::new())?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "data")?,
        HlsExpr::Identifier(ident("word")?),
    ));
    loop_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(kernel_out_stream.clone()),
        "write",
        vec![HlsExpr::Identifier(ident("one_write_burst")?)],
    )?));

    let i_idx = ident("i")?;
    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_42_SINGLE")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(rounded_num_words),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: loop_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_finalize_drain_single")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("partial_in", "reduce_word_t")?,
            stream_param("kernel_out_stream", "little_out_pkt_t")?,
            scalar_param("rounded_num_words", HlsType::UInt32)?,
        ],
        body,
    })
}

fn finalize_drain(
    ops: &KernelOpBundle,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let reference_style = use_reference_style_little_ops(ops);
    let zero_sentinel_mode =
        zero_sentinel && !reference_style && !use_identity_style_cc_little(ops);
    let identity_pod = ident("identity_pod")?;
    let partial_in_first = ident("partial_in_first")?;
    let partial_in_second = ident("partial_in_second")?;
    let kernel_out_stream = ident("kernel_out_stream")?;
    let rounded_num_words = ident("rounded_num_words")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_write_burst")?,
        ty: custom("little_out_pkt_t"),
        init: None,
    }));
    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "last")?,
        literal_bool(false),
    ));

    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("first_word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    loop_body.push(assignment(
        HlsExpr::Identifier(ident("first_word")?),
        method_call(
            HlsExpr::Identifier(partial_in_first.clone()),
            "read",
            Vec::new(),
        )?,
    ));

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("second_word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    loop_body.push(assignment(
        HlsExpr::Identifier(ident("second_word")?),
        method_call(
            HlsExpr::Identifier(partial_in_second.clone()),
            "read",
            Vec::new(),
        )?,
    ));

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merged_word")?,
        ty: custom("reduce_word_t"),
        init: None,
    }));
    let dist_idx = ident("dist_idx")?;
    let mut merge_body = Vec::new();
    merge_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    merge_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("first_val")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("first_word")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?),
    }));
    merge_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("second_val")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(ident("second_word")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?),
    }));
    let finalize_expr = if reference_style {
        // Reference-style Min finalize: (second != 0) ? min(first, second) : first
        HlsExpr::Ternary {
            condition: Box::new(binary(
                HlsBinaryOp::Ne,
                HlsExpr::Identifier(ident("second_val")?),
                literal_uint(0),
            )),
            then_expr: Box::new(HlsExpr::Ternary {
                condition: Box::new(binary(
                    HlsBinaryOp::Lt,
                    HlsExpr::Identifier(ident("first_val")?),
                    HlsExpr::Identifier(ident("second_val")?),
                )),
                then_expr: Box::new(HlsExpr::Identifier(ident("first_val")?)),
                else_expr: Box::new(HlsExpr::Identifier(ident("second_val")?)),
            }),
            else_expr: Box::new(HlsExpr::Identifier(ident("first_val")?)),
        }
    } else if zero_sentinel_mode {
        // Generic zero-sentinel finalize
        reducer_combine_expr_zero_sentinel(
            ops.gather.kind,
            HlsExpr::Identifier(ident("first_val")?),
            HlsExpr::Identifier(ident("second_val")?),
            Some(custom("ap_fixed_pod_t")),
            true, // finalize: check incoming (second_val can be 0)
        )
    } else {
        reducer_combine_expr(
            ops.gather.kind,
            HlsExpr::Identifier(ident("first_val")?),
            HlsExpr::Identifier(ident("second_val")?),
            HlsExpr::Identifier(identity_pod.clone()),
            Some(custom("ap_fixed_pod_t")),
        )
    };
    merge_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merged_val")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(finalize_expr),
    }));
    merge_body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("merged_word")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                ),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(dist_idx.clone()),
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            ),
        )?,
        HlsExpr::Identifier(ident("merged_val")?),
    ));
    loop_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("MERGE_DISTANCES")?,
        init: LoopInitializer::Declaration(int_decl("dist_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(dist_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(dist_idx.clone()),
        ),
        body: merge_body,
    }));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "data")?,
        HlsExpr::Identifier(ident("merged_word")?),
    ));
    loop_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(kernel_out_stream.clone()),
        "write",
        vec![HlsExpr::Identifier(ident("one_write_burst")?)],
    )?));

    let i_idx = ident("i")?;
    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_42")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(rounded_num_words),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: loop_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_finalize_drain")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("partial_in_first", "reduce_word_t")?,
            stream_param("partial_in_second", "reduce_word_t")?,
            stream_param("kernel_out_stream", "little_out_pkt_t")?,
            scalar_param("rounded_num_words", HlsType::UInt32)?,
            scalar_param("identity_pod", custom("ap_fixed_pod_t"))?,
        ],
        body,
    })
}

fn graphyflow_little_top(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let reference_style = use_reference_style_little(ops, edge);
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE m_axi port = edge_props offset = slave bundle = gmem0",
    )?));
    for port in ["edge_props", "num_nodes", "num_edges", "dst_num", "return"].iter() {
        body.push(HlsStatement::Pragma(HlsPragma::new(format!(
            "HLS INTERFACE s_axilite port = {port}"
        ))?));
    }
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));

    // Stream declarations
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edge_stream")?,
        ty: HlsType::Stream(Box::new(custom("edge_descriptor_batch_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = edge_stream depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_edge_data")?,
        ty: HlsType::Stream(Box::new(custom("update_tuple_t_little"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_edge_data depth = 8",
    )?));

    let stream_array_ty =
        |elem: HlsType| -> Result<HlsType, HlsTemplateError> { Ok(HlsType::array(elem, vec![4])?) };

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pe_mem_outs_1")?,
        ty: stream_array_ty(HlsType::Stream(Box::new(custom("reduce_word_t"))))?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = pe_mem_outs_1 depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pe_mem_outs_2")?,
        ty: stream_array_ty(HlsType::Stream(Box::new(custom("reduce_word_t"))))?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = pe_mem_outs_2 depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pe_mem_out_partial_1")?,
        ty: HlsType::Stream(Box::new(custom("reduce_word_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = pe_mem_out_partial_1 depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pe_mem_out_partial_2")?,
        ty: HlsType::Stream(Box::new(custom("reduce_word_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = pe_mem_out_partial_2 depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("ppb_req_stream_internal")?,
        ty: HlsType::Stream(Box::new(custom("ppb_request_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = ppb_req_stream_internal depth = 8",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("ppb_resp_stream_internal")?,
        ty: HlsType::Stream(Box::new(custom("ppb_response_t"))),
        init: None,
    }));
    body.push(HlsStatement::Comment(
        "Prefetch can cover two rounds (R and R+1). Each round returns".to_string(),
    ));
    body.push(HlsStatement::Comment(
        "SRC_BUFFER_WORDS responses, so size the FIFO to avoid backpressure".to_string(),
    ));
    body.push(HlsStatement::Comment(
        "deadlocks when request_manager temporarily stalls.".to_string(),
    ));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = ppb_resp_stream_internal depth = 8",
    )?));

    // Constants
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edges_per_word")?,
        ty: HlsType::Int32,
        init: Some(HlsExpr::Identifier(ident("EDGES_PER_WORD")?)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("num_wide_reads")?,
        ty: HlsType::Int32,
        init: Some(binary(
            HlsBinaryOp::Div,
            HlsExpr::Identifier(ident("num_edges")?),
            HlsExpr::Identifier(ident("edges_per_word")?),
        )),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("total_edge_sets")?,
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Cast {
            target_type: HlsType::UInt32,
            expr: Box::new(HlsExpr::Identifier(ident("num_wide_reads")?)),
        }),
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("num_words")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Shr,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(ident("dst_num")?),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                    literal_int(1),
                ),
            ),
            HlsExpr::Identifier(ident("LOG_DISTANCES_PER_REDUCE_WORD")?),
        )),
    }));
    let rounded_expr = binary(
        HlsBinaryOp::BitAnd,
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("num_words")?),
            literal_int(7),
        ),
        HlsExpr::Unary {
            op: HlsUnaryOp::BitNot,
            expr: Box::new(literal_int(7)),
        },
    );
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("rounded_num_words")?,
        ty: HlsType::UInt32,
        init: Some(rounded_expr),
    }));

    // Load edges (canonical dataflow: keep loops inside a callee).
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("load_edges")?,
        args: vec![
            HlsExpr::Identifier(ident("edge_props")?),
            HlsExpr::Identifier(ident("num_edges")?),
            HlsExpr::Identifier(ident("edge_stream")?),
        ],
    }));

    // Bridge streams
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("stream2axistream")?,
        args: vec![
            HlsExpr::Identifier(ident("ppb_req_stream_internal")?),
            HlsExpr::Identifier(ident("ppb_req_stream")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("axistream2stream")?,
        args: vec![
            HlsExpr::Identifier(ident("ppb_resp_stream")?),
            HlsExpr::Identifier(ident("ppb_resp_stream_internal")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("request_manager")?,
        args: vec![
            HlsExpr::Identifier(ident("edge_stream")?),
            HlsExpr::Identifier(ident("ppb_req_stream_internal")?),
            HlsExpr::Identifier(ident("ppb_resp_stream_internal")?),
            HlsExpr::Identifier(ident("stream_edge_data")?),
            HlsExpr::Identifier(ident("memory_offset")?),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        ],
    }));

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_unit_reduce")?,
        args: vec![
            HlsExpr::Identifier(ident("stream_edge_data")?),
            HlsExpr::Identifier(ident("pe_mem_outs_1")?),
            HlsExpr::Identifier(ident("pe_mem_outs_2")?),
            HlsExpr::Identifier(ident("total_edge_sets")?),
            HlsExpr::Identifier(ident("rounded_num_words")?),
        ],
    }));

    if reference_style {
        // The drain initial accumulator must equal INFINITY_POD so that
        // unreachable vertices retain their infinity value.  Using any
        // smaller value (e.g. INFINITY - 1) causes a 1-off mismatch for
        // vertices that receive no updates.
        body.push(HlsStatement::Raw(
            "ap_fixed_pod_t max_pod = INFINITY_POD;".to_string(),
        ));
    } else {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident("identity_pod")?,
            ty: custom("ap_fixed_pod_t"),
            init: Some(reducer_identity_expr(ops.gather.identity)?),
        }));
    }

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_partial_drain_impl")?,
        args: vec![
            HlsExpr::Literal(HlsLiteral::Int(0)),
            HlsExpr::Identifier(ident("pe_mem_outs_1")?),
            HlsExpr::Identifier(ident("pe_mem_out_partial_1")?),
            HlsExpr::Identifier(ident("rounded_num_words")?),
            HlsExpr::Identifier(ident(if reference_style {
                "max_pod"
            } else {
                "identity_pod"
            })?),
        ],
    }));

    if edge.little_pe > 4 {
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("Reduc_105_partial_drain_impl")?,
            args: vec![
                HlsExpr::Literal(HlsLiteral::Int(1)),
                HlsExpr::Identifier(ident("pe_mem_outs_2")?),
                HlsExpr::Identifier(ident("pe_mem_out_partial_2")?),
                HlsExpr::Identifier(ident("rounded_num_words")?),
                HlsExpr::Identifier(ident(if reference_style {
                    "max_pod"
                } else {
                    "identity_pod"
                })?),
            ],
        }));

        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("Reduc_105_finalize_drain")?,
            args: vec![
                HlsExpr::Identifier(ident("pe_mem_out_partial_1")?),
                HlsExpr::Identifier(ident("pe_mem_out_partial_2")?),
                HlsExpr::Identifier(ident("kernel_out_stream")?),
                HlsExpr::Identifier(ident("rounded_num_words")?),
                HlsExpr::Identifier(ident(if reference_style {
                    "max_pod"
                } else {
                    "identity_pod"
                })?),
            ],
        }));
    } else {
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("Reduc_105_finalize_drain_single")?,
            args: vec![
                HlsExpr::Identifier(ident("pe_mem_out_partial_1")?),
                HlsExpr::Identifier(ident("kernel_out_stream")?),
                HlsExpr::Identifier(ident("rounded_num_words")?),
            ],
        }));
    }

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("graphyflow_little")?,
        return_type: HlsType::Void,
        params: vec![
            HlsParameter {
                name: ident("edge_props")?,
                ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
                passing: PassingStyle::Value,
            },
            scalar_param("num_nodes", HlsType::Int32)?,
            scalar_param("num_edges", HlsType::Int32)?,
            scalar_param("dst_num", HlsType::Int32)?,
            scalar_param("memory_offset", HlsType::Int32)?,
            stream_param("ppb_req_stream", "ppb_request_pkt_t")?,
            stream_param("ppb_resp_stream", "ppb_response_pkt_t")?,
            stream_param("kernel_out_stream", "little_out_pkt_t")?,
        ],
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

fn stream_array_param(
    name: &str,
    ty: &str,
    dims: Vec<usize>,
) -> Result<HlsParameter, HlsTemplateError> {
    Ok(HlsParameter {
        name: ident(name)?,
        ty: HlsType::array(HlsType::Stream(Box::new(custom(ty))), dims)?,
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
