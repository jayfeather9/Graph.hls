use super::HlsTemplateError;
use super::utils::{
    assignment, binary, cast_ternary_branches, custom, expr_uses_operand, ident, index_ident,
    int_decl, literal_bool, literal_int, literal_uint, member_expr, method_call, range_method,
    reducer_combine_expr, reducer_combine_expr_zero_sentinel, reducer_identity_expr,
    render_operator_expr,
};
use crate::domain::{
    hls::{
        HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsForLoop, HlsFunction, HlsIfElse, HlsInclude,
        HlsLiteral, HlsParameter, HlsPragma, HlsStatement, HlsType, HlsUnaryOp, HlsVarDecl,
        HlsWhileLoop, LoopIncrement, LoopInitializer, LoopLabel, PassingStyle,
    },
    hls_ops::{KernelOpBundle, OperatorOperand, ReducerIdentity},
};

pub fn graphyflow_big_unit(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("graphyflow_big.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            stream2axistream()?,
            axistream2stream()?,
            count_end_ones()?,
            dist_req_packer()?,
            cacheline_req_sender()?,
            node_prop_resp_receiver()?,
            merge_node_props(ops, edge)?,
            demux_updates()?,
            sender_stage()?,
            receiver_stage()?,
            switch_stage()?,
            reduce_single_pe(ops, edge.zero_sentinel)?,
            partial_drain_four(ops)?,
            finalize_drain(ops)?,
            drain_variable(ops)?,
            graphyflow_big_top(ops, edge)?,
        ],
    })
}

fn stream2axistream() -> Result<HlsFunction, HlsTemplateError> {
    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t1")?,
        ty: custom("cacheline_req_t"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("stream")?,
        target: ident("tmp_t1")?,
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t2")?,
        ty: custom("cacheline_request_pkt_t"),
        init: None,
    }));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "data")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "idx")?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "dest")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "dst")?,
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
            stream_param("stream", "cacheline_req_t")?,
            stream_param("axi_stream", "cacheline_request_pkt_t")?,
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
        ty: custom("cacheline_response_pkt_t"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("axi_stream")?,
        target: ident("tmp_t1")?,
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_t2")?,
        ty: custom("cacheline_resp_t"),
        init: None,
    }));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "data")?,
        member_expr(HlsExpr::Identifier(ident("tmp_t1")?), "data")?,
    ));
    loop_body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("tmp_t2")?), "dst")?,
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
            stream_param("axi_stream", "cacheline_response_pkt_t")?,
            stream_param("stream", "cacheline_resp_t")?,
        ],
        body: vec![HlsStatement::WhileLoop(HlsWhileLoop {
            label: LoopLabel::new("axistream2stream")?,
            condition: literal_bool(true),
            body: loop_body,
        })],
    })
}

fn count_end_ones() -> Result<HlsFunction, HlsTemplateError> {
    let count_ident = ident("count")?;
    let valid_mask = ident("valid_mask")?;

    let mut ladder = HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Binary {
            op: HlsBinaryOp::Eq,
            left: Box::new(HlsExpr::Identifier(valid_mask.clone())),
            right: Box::new(HlsExpr::Literal(HlsLiteral::UInt(255))),
        },
        then_body: vec![assignment(
            HlsExpr::Identifier(count_ident.clone()),
            HlsExpr::Literal(HlsLiteral::UInt(8)),
        )],
        else_body: Vec::new(),
    });

    for (mask, value) in [
        (127_u64, 7_u64),
        (63, 6),
        (31, 5),
        (15, 4),
        (7, 3),
        (3, 2),
        (1, 1),
        (0, 0),
    ] {
        ladder = HlsStatement::IfElse(HlsIfElse {
            condition: HlsExpr::Binary {
                op: HlsBinaryOp::Eq,
                left: Box::new(HlsExpr::Identifier(valid_mask.clone())),
                right: Box::new(HlsExpr::Literal(HlsLiteral::UInt(mask))),
            },
            then_body: vec![assignment(
                HlsExpr::Identifier(count_ident.clone()),
                literal_uint(value),
            )],
            else_body: vec![ladder],
        });
    }

    let body = vec![
        HlsStatement::Pragma(HlsPragma::new("HLS INLINE")?),
        HlsStatement::Declaration(HlsVarDecl {
            name: count_ident.clone(),
            ty: custom("ap_uint<4>"),
            init: Some(literal_uint(0)),
        }),
        ladder,
        HlsStatement::Return(Some(HlsExpr::Identifier(count_ident))),
    ];

    Ok(HlsFunction {
        linkage: None,
        name: ident("count_end_ones")?,
        return_type: custom("ap_uint<4>"),
        params: vec![scalar_param("valid_mask", custom("ap_uint<PE_NUM>"))?],
        body,
    })
}

fn dist_req_packer() -> Result<HlsFunction, HlsTemplateError> {
    let cache_idx_ty = HlsType::array_with_exprs(
        custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        vec!["PE_NUM".to_string()],
    )?;
    let cache_idx_diffs_ty = HlsType::array_with_exprs(
        custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        vec!["PE_NUM".to_string()],
    )?;

    let last_idx_max = ident("last_idx_max")?;
    let cache_idx = ident("cache_idx")?;
    let cache_idx_diffs = ident("cache_idx_diffs")?;
    let node_id_burst = ident("node_id_burst")?;
    let edge_burst_idx = ident("edge_burst_idx")?;
    let pe_idx = ident("pe_idx")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: last_idx_max.clone(),
        ty: custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        init: Some(literal_uint(0)),
    }));

    // Main loop over bursts
    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: node_id_burst.clone(),
        ty: custom("node_id_burst_t"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("src_id_burst_stream")?,
        target: node_id_burst.clone(),
    });

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_idx.clone(),
        ty: cache_idx_ty.clone(),
        init: None,
    }));
    loop_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_idx complete dim = 0",
    )?));

    let node_data_member = member_expr(HlsExpr::Identifier(node_id_burst.clone()), "data")?;
    let cache_idx_loop_body = vec![
        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
        assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_idx.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            },
            HlsExpr::Binary {
                op: HlsBinaryOp::Shr,
                left: Box::new(range_method(
                    HlsExpr::Index {
                        target: Box::new(node_data_member.clone()),
                        index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                    },
                    literal_uint(30),
                    literal_uint(0),
                )?),
                right: Box::new(HlsExpr::Identifier(ident("LOG_DIST_PER_WORD")?)),
            },
        ),
    ];

    loop_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_0")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: cache_idx_loop_body,
    }));

    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_idx_diffs.clone(),
        ty: cache_idx_diffs_ty,
        init: None,
    }));
    loop_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_idx_diffs complete dim = 0",
    )?));

    let diff_loop_body = vec![
        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
        assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_idx_diffs.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            },
            HlsExpr::Binary {
                op: HlsBinaryOp::Sub,
                left: Box::new(HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(cache_idx.clone())),
                    index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                }),
                right: Box::new(HlsExpr::Identifier(last_idx_max.clone())),
            },
        ),
    ];

    loop_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_1")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: diff_loop_body,
    }));

    // if (cache_idx_diffs[PE_NUM - 1]) { ... }
    let guard = HlsExpr::Index {
        target: Box::new(HlsExpr::Identifier(cache_idx_diffs.clone())),
        index: Box::new(HlsExpr::Binary {
            op: HlsBinaryOp::Sub,
            left: Box::new(HlsExpr::Identifier(ident("PE_NUM")?)),
            right: Box::new(literal_uint(1)),
        }),
    };

    let valid_mask = ident("valid_mask")?;
    let num_unread = ident("num_unread")?;
    let req_pack = ident("req_pack")?;

    let mut if_body = Vec::new();
    if_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: valid_mask.clone(),
        ty: custom("ap_uint<PE_NUM>"),
        init: None,
    }));

    let mut mask_loop_body = Vec::new();
    mask_loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    mask_loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Binary {
            op: HlsBinaryOp::Eq,
            left: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_idx_diffs.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            }),
            right: Box::new(literal_uint(0)),
        },
        then_body: vec![assignment(
            range_method(
                HlsExpr::Identifier(valid_mask.clone()),
                HlsExpr::Identifier(pe_idx.clone()),
                HlsExpr::Identifier(pe_idx.clone()),
            )?,
            literal_uint(1),
        )],
        else_body: vec![assignment(
            range_method(
                HlsExpr::Identifier(valid_mask.clone()),
                HlsExpr::Identifier(pe_idx.clone()),
                HlsExpr::Identifier(pe_idx.clone()),
            )?,
            literal_uint(0),
        )],
    }));

    if_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_2")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: mask_loop_body,
    }));

    if_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: num_unread.clone(),
        ty: custom("ap_uint<4>"),
        init: Some(HlsExpr::Call {
            function: ident("count_end_ones")?,
            args: vec![HlsExpr::Identifier(valid_mask.clone())],
        }),
    }));

    if_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: req_pack.clone(),
        ty: custom("distance_req_pack_t"),
        init: None,
    }));
    if_body.push(assignment(
        member_expr(HlsExpr::Identifier(req_pack.clone()), "offset")?,
        HlsExpr::Identifier(num_unread.clone()),
    ));
    if_body.push(assignment(
        member_expr(HlsExpr::Identifier(req_pack.clone()), "end_flag")?,
        literal_bool(false),
    ));

    let mut copy_idx_body = Vec::new();
    copy_idx_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    copy_idx_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(member_expr(HlsExpr::Identifier(req_pack.clone()), "idx")?),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_idx.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
    ));

    if_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_3")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: copy_idx_body,
    }));

    if_body.push(HlsStatement::StreamWrite {
        stream: ident("distance_req_pack_stream")?,
        value: HlsExpr::Identifier(req_pack),
    });

    loop_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: guard,
        then_body: if_body,
        else_body: Vec::new(),
    }));

    loop_body.push(assignment(
        HlsExpr::Identifier(last_idx_max.clone()),
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_idx)),
            index: Box::new(HlsExpr::Binary {
                op: HlsBinaryOp::Sub,
                left: Box::new(HlsExpr::Identifier(ident("PE_NUM")?)),
                right: Box::new(literal_uint(1)),
            }),
        },
    ));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_4")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: edge_burst_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(edge_burst_idx),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(ident("edge_burst_idx")?),
        ),
        body: loop_body,
    }));

    // Trailing end packet
    let mut end_block = Vec::new();
    let end_req_pack = ident("end_req_pack")?;
    end_block.push(HlsStatement::Declaration(HlsVarDecl {
        name: end_req_pack.clone(),
        ty: custom("distance_req_pack_t"),
        init: None,
    }));
    end_block.push(assignment(
        member_expr(HlsExpr::Identifier(end_req_pack.clone()), "end_flag")?,
        literal_bool(true),
    ));
    end_block.push(assignment(
        member_expr(HlsExpr::Identifier(end_req_pack.clone()), "offset")?,
        literal_uint(7),
    ));
    end_block.push(HlsStatement::StreamWrite {
        stream: ident("distance_req_pack_stream")?,
        value: HlsExpr::Identifier(end_req_pack),
    });
    body.push(HlsStatement::Block(end_block));

    Ok(HlsFunction {
        linkage: None,
        name: ident("dist_req_packer")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("src_id_burst_stream", "node_id_burst_t")?,
            stream_param("distance_req_pack_stream", "distance_req_pack_t")?,
            scalar_param("total_edge_sets", HlsType::UInt32)?,
        ],
        body,
    })
}

fn cacheline_req_sender() -> Result<HlsFunction, HlsTemplateError> {
    let cache_req = ident("cache_req")?;
    let cacheline_idx = ident("cacheline_idx")?;
    let req_pack = ident("req_pack")?;
    let pe_idx = ident("pe_idx")?;
    let i_idx = ident("i")?;

    let cache_idx_ty = HlsType::array_with_exprs(
        custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        vec!["PE_NUM".to_string()],
    )?;

    let mut body = Vec::new();

    // Initial seed request
    body.push(HlsStatement::Block(vec![
        HlsStatement::Declaration(HlsVarDecl {
            name: cache_req.clone(),
            ty: custom("cacheline_req_t"),
            init: None,
        }),
        assignment(
            member_expr(HlsExpr::Identifier(cache_req.clone()), "end_flag")?,
            literal_bool(false),
        ),
        assignment(
            member_expr(HlsExpr::Identifier(cache_req.clone()), "idx")?,
            HlsExpr::Identifier(ident("memory_offset")?),
        ),
        assignment(
            member_expr(HlsExpr::Identifier(cache_req.clone()), "dst")?,
            literal_uint(0),
        ),
        HlsStatement::StreamWrite {
            stream: ident("cacheline_req_stream")?,
            value: HlsExpr::Identifier(cache_req.clone()),
        },
    ]));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cacheline_idx.clone(),
        ty: cache_idx_ty,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cacheline_idx complete dim = 0",
    )?));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    while_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS dependence variable = cacheline_idx inter false",
    )?));

    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: req_pack.clone(),
        ty: custom("distance_req_pack_t"),
        init: None,
    }));
    while_body.push(HlsStatement::StreamRead {
        stream: ident("distance_req_pack_stream")?,
        target: req_pack.clone(),
    });

    let mut copy_idx_body = Vec::new();
    copy_idx_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    copy_idx_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cacheline_idx.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(member_expr(HlsExpr::Identifier(req_pack.clone()), "idx")?),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
    ));

    while_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_5")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: copy_idx_body,
    }));

    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS PIPELINE II = 1 rewind",
    )?));
    inner_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS unroll factor = 1",
    )?));
    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_req.clone(),
        ty: custom("cacheline_req_t"),
        init: None,
    }));
    inner_body.push(assignment(
        member_expr(HlsExpr::Identifier(cache_req.clone()), "idx")?,
        HlsExpr::Binary {
            op: HlsBinaryOp::Add,
            left: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cacheline_idx.clone())),
                index: Box::new(HlsExpr::Identifier(i_idx.clone())),
            }),
            right: Box::new(HlsExpr::Identifier(ident("memory_offset")?)),
        },
    ));
    inner_body.push(assignment(
        member_expr(HlsExpr::Identifier(cache_req.clone()), "dst")?,
        HlsExpr::Identifier(i_idx.clone()),
    ));
    inner_body.push(assignment(
        member_expr(HlsExpr::Identifier(cache_req.clone()), "end_flag")?,
        member_expr(HlsExpr::Identifier(req_pack.clone()), "end_flag")?,
    ));
    inner_body.push(HlsStatement::StreamWrite {
        stream: ident("cacheline_req_stream")?,
        value: HlsExpr::Identifier(cache_req),
    });

    while_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_6")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: custom("ap_uint<4>"),
            init: Some(member_expr(
                HlsExpr::Identifier(req_pack.clone()),
                "offset",
            )?),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: inner_body,
    }));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(req_pack.clone()), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_7")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("cacheline_req_sender")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("distance_req_pack_stream", "distance_req_pack_t")?,
            stream_param("cacheline_req_stream", "cacheline_req_t")?,
            scalar_param("memory_offset", HlsType::Int32)?,
        ],
        body,
    })
}

fn node_prop_resp_receiver() -> Result<HlsFunction, HlsTemplateError> {
    let cache_resp = ident("cache_resp")?;
    let first_line = ident("first_line")?;
    let pe_idx = ident("pe_idx")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_resp.clone(),
        ty: custom("cacheline_resp_t"),
        init: None,
    }));
    body.push(HlsStatement::StreamRead {
        stream: ident("cacheline_resp_stream")?,
        target: cache_resp.clone(),
    });
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: first_line.clone(),
        ty: custom("bus_word_t"),
        init: Some(member_expr(
            HlsExpr::Identifier(cache_resp.clone()),
            "data",
        )?),
    }));

    let mut init_loop_body = Vec::new();
    init_loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    init_loop_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("cacheline_streams")?)),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        "write",
        vec![HlsExpr::Identifier(first_line.clone())],
    )?));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_8")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: init_loop_body,
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    let read_nb_call = method_call(
        HlsExpr::Identifier(ident("cacheline_resp_stream")?),
        "read_nb",
        vec![HlsExpr::Identifier(cache_resp.clone())],
    )?;

    let mut if_body = Vec::new();
    if_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(cache_resp.clone()), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));
    if_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("resp_line")?,
        ty: custom("bus_word_t"),
        init: Some(member_expr(
            HlsExpr::Identifier(cache_resp.clone()),
            "data",
        )?),
    }));
    if_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("target_pe")?,
        ty: custom("ap_uint<8>"),
        init: Some(member_expr(HlsExpr::Identifier(cache_resp.clone()), "dst")?),
    }));
    if_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("cacheline_streams")?)),
            index: Box::new(HlsExpr::Identifier(ident("target_pe")?)),
        },
        "write",
        vec![HlsExpr::Identifier(ident("resp_line")?)],
    )?));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: read_nb_call,
        then_body: if_body,
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_9")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("node_prop_resp_receiver")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("cacheline_resp_stream", "cacheline_resp_t")?,
            stream_array_param("cacheline_streams", "bus_word_t", "PE_NUM")?,
        ],
        body,
    })
}

fn merge_node_props(
    ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let last_cacheline = ident("last_cacheline")?;
    let last_cache_idx = ident("last_cache_idx")?;
    let pe_idx = ident("pe_idx")?;
    let edge_batch_idx = ident("edge_batch_idx")?;
    let edge_batch = ident("edge_batch")?;
    let out_batch = ident("out_batch")?;
    let cacheline = ident("cacheline")?;
    let cacheline_idx = ident("cacheline_idx")?;
    let offset = ident("offset")?;
    let prop = ident("prop")?;
    let binop_res = ident("BinOp_68_res")?;
    let edge_weight = ident("edge_weight")?;
    let guard_infinity = ops.gather.identity == ReducerIdentity::PositiveInfinity
        && expr_uses_operand(&ops.scatter.expr, &OperatorOperand::ScatterSrcProp)
        && !(edge.zero_sentinel && edge.allow_scatter_inf_overflow_to_zero);

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: last_cacheline.clone(),
        ty: HlsType::array_with_exprs(custom("bus_word_t"), vec!["PE_NUM".to_string()])?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = last_cacheline complete dim = 0",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: last_cache_idx.clone(),
        ty: HlsType::array_with_exprs(
            custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = last_cache_idx complete dim = 0",
    )?));

    let mut init_body = Vec::new();
    init_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    init_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cacheline.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        HlsExpr::MethodCall {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("cacheline_streams")?)),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            }),
            method: ident("read")?,
            args: vec![],
        },
    ));
    init_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cache_idx.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        literal_uint(0),
    ));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_10")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: init_body,
    }));

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
            name: edge_weight.clone(),
            ty: custom("ap_fixed_pod_t"),
            init: Some(edge_weight_expr),
        }));
    }

    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: edge_batch.clone(),
        ty: custom("edge_descriptor_batch_t"),
        init: None,
    }));
    outer_body.push(HlsStatement::StreamRead {
        stream: ident("edge_stream")?,
        target: edge_batch.clone(),
    });
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: out_batch.clone(),
        ty: custom("update_tuple_t_big"),
        init: None,
    }));

    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let edge_array = HlsExpr::Member {
        target: Box::new(HlsExpr::Identifier(edge_batch.clone())),
        field: ident("edges")?,
    };

    let edge_entry = HlsExpr::Index {
        target: Box::new(edge_array.clone()),
        index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
    };

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("node_id_low")?,
        ty: custom("ap_uint<NODE_ID_BITWIDTH - 1>"),
        init: Some(range_method(
            member_expr(edge_entry.clone(), "src_id")?,
            literal_uint(30),
            literal_uint(0),
        )?),
    }));

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cacheline_idx.clone(),
        ty: custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(ident("node_id_low")?),
            HlsExpr::Identifier(ident("LOG_DIST_PER_WORD")?),
        )),
    }));

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: offset.clone(),
        ty: custom("ap_uint<LOG_DIST_PER_WORD>"),
        init: Some(binary(
            HlsBinaryOp::BitAnd,
            HlsExpr::Identifier(ident("node_id_low")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
                literal_uint(1),
            ),
        )),
    }));

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cacheline.clone(),
        ty: custom("bus_word_t"),
        init: None,
    }));

    inner_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(cacheline_idx.clone()),
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(last_cache_idx.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            },
        ),
        then_body: vec![assignment(
            HlsExpr::Identifier(cacheline.clone()),
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(last_cacheline.clone())),
                index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
            },
        )],
        else_body: vec![assignment(
            HlsExpr::Identifier(cacheline.clone()),
            HlsExpr::MethodCall {
                target: Box::new(HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(ident("cacheline_streams")?)),
                    index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                }),
                method: ident("read")?,
                args: vec![],
            },
        )],
    }));

    let offset_cast = HlsExpr::Cast {
        target_type: custom("ap_uint<9>"),
        expr: Box::new(HlsExpr::Identifier(offset.clone())),
    };

    let offset_shift = HlsExpr::Binary {
        op: HlsBinaryOp::Mul,
        left: Box::new(offset_cast),
        right: Box::new(HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?)),
    };

    let high_bit = HlsExpr::Binary {
        op: HlsBinaryOp::Add,
        left: Box::new(offset_shift.clone()),
        right: Box::new(binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            literal_uint(1),
        )),
    };

    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: prop.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(cacheline.clone()),
            high_bit,
            offset_shift.clone(),
        )?),
    }));

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
        inner_body.push(HlsStatement::Declaration(HlsVarDecl {
            name: edge_weight.clone(),
            ty: custom("ap_fixed_pod_t"),
            init: Some(weight_expr),
        }));
    }

    let scatter_expr = {
        let mut leaf_mapper = |opnd: &OperatorOperand| match opnd {
            OperatorOperand::ScatterSrcProp => Some(HlsExpr::Identifier(prop.clone())),
            OperatorOperand::ScatterEdgeWeight => Some(HlsExpr::Identifier(edge_weight.clone())),
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
                HlsExpr::Identifier(prop.clone()),
                HlsExpr::Identifier(ident("INFINITY_POD")?),
            )),
            then_expr: Box::new(HlsExpr::Identifier(ident("INFINITY_POD")?)),
            else_expr: Box::new(scatter_cast),
        }
    } else {
        scatter_expr
    };
    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: binop_res.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(guarded_expr),
    }));

    let out_entry = HlsExpr::Index {
        target: Box::new(member_expr(HlsExpr::Identifier(out_batch.clone()), "data")?),
        index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
    };

    inner_body.push(assignment(
        member_expr(out_entry.clone(), "prop")?,
        HlsExpr::Identifier(binop_res.clone()),
    ));
    inner_body.push(assignment(
        member_expr(out_entry.clone(), "node_id")?,
        member_expr(edge_entry.clone(), "dst_id")?,
    ));
    inner_body.push(assignment(
        member_expr(out_entry.clone(), "end_flag")?,
        literal_bool(false),
    ));

    // Keep cache state aligned with `dist_req_packer`/`cacheline_req_sender`.
    //
    // The request-packer tracks a single `last_idx_max` based on the last PE's
    // cacheline index, and uses it to decide which PEs need fresh cachelines.
    // To avoid deadlock (waiting on a cacheline that was never requested), the
    // consumer side must also broadcast the last PE's cacheline as the shared
    // cache state across all PEs.
    let last_pe_idx = binary(
        HlsBinaryOp::Sub,
        HlsExpr::Identifier(ident("PE_NUM")?),
        literal_uint(1),
    );
    inner_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(pe_idx.clone()),
            last_pe_idx.clone(),
        ),
        then_body: vec![
            assignment(
                HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(last_cacheline.clone())),
                    index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                },
                HlsExpr::Identifier(cacheline.clone()),
            ),
            assignment(
                HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(last_cache_idx.clone())),
                    index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                },
                HlsExpr::Identifier(cacheline_idx.clone()),
            ),
        ],
        else_body: Vec::new(),
    }));

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_11")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: inner_body,
    }));

    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("edge_batch_stream")?,
        value: HlsExpr::Identifier(out_batch.clone()),
    });

    // Broadcast last PE's cache state to all other PEs (see comment above).
    let mut broadcast_body = Vec::new();
    broadcast_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    broadcast_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cacheline.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cacheline.clone())),
            index: Box::new(last_pe_idx.clone()),
        },
    ));
    broadcast_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cache_idx.clone())),
            index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(last_cache_idx.clone())),
            index: Box::new(last_pe_idx.clone()),
        },
    ));
    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_12")?,
        init: LoopInitializer::Declaration(int_decl("pe_idx", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_idx.clone()),
            last_pe_idx,
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_idx.clone()),
        ),
        body: broadcast_body,
    }));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_13")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: edge_batch_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(edge_batch_idx.clone()),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(edge_batch_idx.clone()),
        ),
        body: outer_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_node_props")?,
        return_type: HlsType::Void,
        params: vec![
            stream_array_param("cacheline_streams", "bus_word_t", "PE_NUM")?,
            stream_param("edge_stream", "edge_descriptor_batch_t")?,
            stream_param("edge_batch_stream", "update_tuple_t_big")?,
            scalar_param("total_edge_sets", HlsType::UInt32)?,
        ],
        body,
    })
}

fn demux_updates() -> Result<HlsFunction, HlsTemplateError> {
    let batch_idx = ident("batch_idx")?;
    let i_idx = ident("i")?;
    let in_batch = ident("in_batch")?;
    let mut body = Vec::new();

    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: in_batch.clone(),
        ty: custom("update_tuple_t_big"),
        init: None,
    }));
    outer_body.push(HlsStatement::StreamRead {
        stream: ident("in_batch_stream")?,
        target: in_batch.clone(),
    });

    let node_array = member_expr(HlsExpr::Identifier(in_batch.clone()), "data")?;
    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));

    let node_at_i = HlsExpr::Index {
        target: Box::new(node_array.clone()),
        index: Box::new(HlsExpr::Identifier(i_idx.clone())),
    };
    let node_id = member_expr(node_at_i.clone(), "node_id")?;
    let cond = HlsExpr::Binary {
        op: HlsBinaryOp::Eq,
        left: Box::new(range_method(
            node_id,
            HlsExpr::Identifier(ident("LOCAL_ID_MSB")?),
            HlsExpr::Identifier(ident("LOCAL_ID_MSB")?),
        )?),
        right: Box::new(literal_uint(0)),
    };
    let write_call = method_call(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("out_streams")?)),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        "write",
        vec![node_at_i],
    )?;
    inner_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: cond,
        then_body: vec![HlsStatement::Expr(write_call)],
        else_body: Vec::new(),
    }));

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_18")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: inner_body,
    }));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_19")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: batch_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(batch_idx),
            HlsExpr::Identifier(ident("total_edge_sets")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(ident("batch_idx")?),
        ),
        body: outer_body,
    }));

    // Propagate end flags
    let end_wrapper = ident("end_wrapper")?;
    let i_tail = ident("i_tail")?;
    let mut tail_body = Vec::new();
    tail_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    tail_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: end_wrapper.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    tail_body.push(assignment(
        member_expr(HlsExpr::Identifier(end_wrapper.clone()), "end_flag")?,
        literal_bool(true),
    ));
    tail_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("out_streams")?)),
            index: Box::new(HlsExpr::Identifier(i_tail.clone())),
        },
        "write",
        vec![HlsExpr::Identifier(end_wrapper)],
    )?));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_20")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_tail.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_tail.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(HlsUnaryOp::PreIncrement, HlsExpr::Identifier(i_tail)),
        body: tail_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("demux_1")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("in_batch_stream", "update_tuple_t_big")?,
            stream_array_param("out_streams", "update_t_big", "PE_NUM")?,
            scalar_param("total_edge_sets", HlsType::UInt32)?,
        ],
        body,
    })
}

fn sender_stage() -> Result<HlsFunction, HlsTemplateError> {
    let in1_end_flag = ident("in1_end_flag")?;
    let in2_end_flag = ident("in2_end_flag")?;
    let data1 = ident("data1")?;
    let data2 = ident("data2")?;
    let tmp_data = ident("data")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: in1_end_flag.clone(),
        ty: HlsType::Bool,
        init: Some(literal_bool(false)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: in2_end_flag.clone(),
        ty: HlsType::Bool,
        init: Some(literal_bool(false)),
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    let mut in1_body = Vec::new();
    in1_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: data1.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    in1_body.push(HlsStatement::StreamRead {
        stream: ident("in1")?,
        target: data1.clone(),
    });

    let route_cond_1 = binary(
        HlsBinaryOp::Ne,
        binary(
            HlsBinaryOp::BitAnd,
            binary(
                HlsBinaryOp::Shr,
                member_expr(HlsExpr::Identifier(data1.clone()), "node_id")?,
                HlsExpr::Identifier(ident("i")?),
            ),
            literal_int(1),
        ),
        literal_int(0),
    );

    in1_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data1.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::IfElse(HlsIfElse {
            condition: route_cond_1,
            then_body: vec![HlsStatement::Expr(method_call(
                HlsExpr::Identifier(ident("out2")?),
                "write",
                vec![HlsExpr::Identifier(data1.clone())],
            )?)],
            else_body: vec![HlsStatement::Expr(method_call(
                HlsExpr::Identifier(ident("out1")?),
                "write",
                vec![HlsExpr::Identifier(data1.clone())],
            )?)],
        })],
        else_body: vec![assignment(
            HlsExpr::Identifier(in1_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(method_call(
                HlsExpr::Identifier(ident("in1")?),
                "empty",
                vec![],
            )?),
        },
        then_body: in1_body,
        else_body: Vec::new(),
    }));

    let mut in2_body = Vec::new();
    in2_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: data2.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    in2_body.push(HlsStatement::StreamRead {
        stream: ident("in2")?,
        target: data2.clone(),
    });

    let route_cond_2 = binary(
        HlsBinaryOp::Ne,
        binary(
            HlsBinaryOp::BitAnd,
            binary(
                HlsBinaryOp::Shr,
                member_expr(HlsExpr::Identifier(data2.clone()), "node_id")?,
                HlsExpr::Identifier(ident("i")?),
            ),
            literal_int(1),
        ),
        literal_int(0),
    );

    in2_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data2.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::IfElse(HlsIfElse {
            condition: route_cond_2,
            then_body: vec![HlsStatement::Expr(method_call(
                HlsExpr::Identifier(ident("out4")?),
                "write",
                vec![HlsExpr::Identifier(data2.clone())],
            )?)],
            else_body: vec![HlsStatement::Expr(method_call(
                HlsExpr::Identifier(ident("out3")?),
                "write",
                vec![HlsExpr::Identifier(data2.clone())],
            )?)],
        })],
        else_body: vec![assignment(
            HlsExpr::Identifier(in2_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(method_call(
                HlsExpr::Identifier(ident("in2")?),
                "empty",
                vec![],
            )?),
        },
        then_body: in2_body,
        else_body: Vec::new(),
    }));

    let mut drain_body = Vec::new();
    drain_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: tmp_data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    drain_body.push(assignment(
        member_expr(HlsExpr::Identifier(tmp_data.clone()), "end_flag")?,
        literal_bool(true),
    ));
    for out_name in ["out1", "out2", "out3", "out4"].iter() {
        drain_body.push(HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident(out_name)?),
            "write",
            vec![HlsExpr::Identifier(tmp_data.clone())],
        )?));
    }
    drain_body.push(assignment(
        HlsExpr::Identifier(in1_end_flag.clone()),
        literal_bool(false),
    ));
    drain_body.push(assignment(
        HlsExpr::Identifier(in2_end_flag.clone()),
        literal_bool(false),
    ));
    drain_body.push(HlsStatement::Break);

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::And,
            HlsExpr::Identifier(in1_end_flag.clone()),
            HlsExpr::Identifier(in2_end_flag.clone()),
        ),
        then_body: drain_body,
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_21")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("sender_2")?,
        return_type: HlsType::Void,
        params: vec![
            scalar_param("i", HlsType::Int32)?,
            stream_param("in1", "update_t_big")?,
            stream_param("in2", "update_t_big")?,
            stream_param("out1", "update_t_big")?,
            stream_param("out2", "update_t_big")?,
            stream_param("out3", "update_t_big")?,
            stream_param("out4", "update_t_big")?,
        ],
        body,
    })
}

fn receiver_stage() -> Result<HlsFunction, HlsTemplateError> {
    let in1_end_flag = ident("in1_end_flag")?;
    let in2_end_flag = ident("in2_end_flag")?;
    let in3_end_flag = ident("in3_end_flag")?;
    let in4_end_flag = ident("in4_end_flag")?;
    let data = ident("data")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));
    for flag in [
        in1_end_flag.clone(),
        in2_end_flag.clone(),
        in3_end_flag.clone(),
        in4_end_flag.clone(),
    ] {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: flag,
            ty: HlsType::Bool,
            init: Some(literal_bool(false)),
        }));
    }

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));

    let mut primary_if = Vec::new();
    primary_if.push(HlsStatement::Declaration(HlsVarDecl {
        name: data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    primary_if.push(HlsStatement::StreamRead {
        stream: ident("in1")?,
        target: data.clone(),
    });
    primary_if.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("out1")?),
            "write",
            vec![HlsExpr::Identifier(data.clone())],
        )?)],
        else_body: vec![assignment(
            HlsExpr::Identifier(in1_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    let mut tertiary_if = Vec::new();
    tertiary_if.push(HlsStatement::Declaration(HlsVarDecl {
        name: data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    tertiary_if.push(HlsStatement::StreamRead {
        stream: ident("in3")?,
        target: data.clone(),
    });
    tertiary_if.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("out1")?),
            "write",
            vec![HlsExpr::Identifier(data.clone())],
        )?)],
        else_body: vec![assignment(
            HlsExpr::Identifier(in3_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(method_call(
                HlsExpr::Identifier(ident("in1")?),
                "empty",
                vec![],
            )?),
        },
        then_body: primary_if,
        else_body: vec![HlsStatement::IfElse(HlsIfElse {
            condition: HlsExpr::Unary {
                op: HlsUnaryOp::LogicalNot,
                expr: Box::new(method_call(
                    HlsExpr::Identifier(ident("in3")?),
                    "empty",
                    vec![],
                )?),
            },
            then_body: tertiary_if,
            else_body: Vec::new(),
        })],
    }));

    let mut secondary_if = Vec::new();
    secondary_if.push(HlsStatement::Declaration(HlsVarDecl {
        name: data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    secondary_if.push(HlsStatement::StreamRead {
        stream: ident("in2")?,
        target: data.clone(),
    });
    secondary_if.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("out2")?),
            "write",
            vec![HlsExpr::Identifier(data.clone())],
        )?)],
        else_body: vec![assignment(
            HlsExpr::Identifier(in2_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    let mut quaternary_if = Vec::new();
    quaternary_if.push(HlsStatement::Declaration(HlsVarDecl {
        name: data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    quaternary_if.push(HlsStatement::StreamRead {
        stream: ident("in4")?,
        target: data.clone(),
    });
    quaternary_if.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(member_expr(HlsExpr::Identifier(data.clone()), "end_flag")?),
        },
        then_body: vec![HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("out2")?),
            "write",
            vec![HlsExpr::Identifier(data.clone())],
        )?)],
        else_body: vec![assignment(
            HlsExpr::Identifier(in4_end_flag.clone()),
            literal_bool(true),
        )],
    }));

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: HlsExpr::Unary {
            op: HlsUnaryOp::LogicalNot,
            expr: Box::new(method_call(
                HlsExpr::Identifier(ident("in2")?),
                "empty",
                vec![],
            )?),
        },
        then_body: secondary_if,
        else_body: vec![HlsStatement::IfElse(HlsIfElse {
            condition: HlsExpr::Unary {
                op: HlsUnaryOp::LogicalNot,
                expr: Box::new(method_call(
                    HlsExpr::Identifier(ident("in4")?),
                    "empty",
                    vec![],
                )?),
            },
            then_body: quaternary_if,
            else_body: Vec::new(),
        })],
    }));

    let mut final_if = Vec::new();
    final_if.push(HlsStatement::Declaration(HlsVarDecl {
        name: data.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    final_if.push(assignment(
        member_expr(HlsExpr::Identifier(data.clone()), "end_flag")?,
        literal_bool(true),
    ));
    for out_name in ["out1", "out2"].iter() {
        final_if.push(HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident(out_name)?),
            "write",
            vec![HlsExpr::Identifier(data.clone())],
        )?));
    }
    final_if.push(HlsStatement::Break);

    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::And,
            binary(
                HlsBinaryOp::And,
                HlsExpr::Identifier(in1_end_flag.clone()),
                HlsExpr::Identifier(in2_end_flag.clone()),
            ),
            binary(
                HlsBinaryOp::And,
                HlsExpr::Identifier(in3_end_flag.clone()),
                HlsExpr::Identifier(in4_end_flag.clone()),
            ),
        ),
        then_body: final_if,
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_22")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("receiver_2")?,
        return_type: HlsType::Void,
        params: vec![
            scalar_param("i", HlsType::Int32)?,
            stream_param("out1", "update_t_big")?,
            stream_param("out2", "update_t_big")?,
            stream_param("in1", "update_t_big")?,
            stream_param("in2", "update_t_big")?,
            stream_param("in3", "update_t_big")?,
            stream_param("in4", "update_t_big")?,
        ],
        body,
    })
}

fn switch_stage() -> Result<HlsFunction, HlsTemplateError> {
    let l1_1 = ident("l1_1")?;
    let l1_2 = ident("l1_2")?;
    let l1_3 = ident("l1_3")?;
    let l1_4 = ident("l1_4")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));

    for stream_name in [&l1_1, &l1_2, &l1_3, &l1_4] {
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: stream_name.clone(),
            ty: HlsType::Stream(Box::new(custom("update_t_big"))),
            init: None,
        }));
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS STREAM variable = {} depth = 2",
            stream_name
        ))?));
    }

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("sender_2")?,
        args: vec![
            HlsExpr::Identifier(ident("i")?),
            HlsExpr::Identifier(ident("in1")?),
            HlsExpr::Identifier(ident("in2")?),
            HlsExpr::Identifier(l1_1.clone()),
            HlsExpr::Identifier(l1_2.clone()),
            HlsExpr::Identifier(l1_3.clone()),
            HlsExpr::Identifier(l1_4.clone()),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("receiver_2")?,
        args: vec![
            HlsExpr::Identifier(ident("i")?),
            HlsExpr::Identifier(ident("out1")?),
            HlsExpr::Identifier(ident("out2")?),
            HlsExpr::Identifier(l1_1),
            HlsExpr::Identifier(l1_2),
            HlsExpr::Identifier(l1_3),
            HlsExpr::Identifier(l1_4),
        ],
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("switch2x2_2")?,
        return_type: HlsType::Void,
        params: vec![
            scalar_param("i", HlsType::Int32)?,
            stream_param("in1", "update_t_big")?,
            stream_param("in2", "update_t_big")?,
            stream_param("out1", "update_t_big")?,
            stream_param("out2", "update_t_big")?,
        ],
        body,
    })
}

fn reduce_single_pe(
    ops: &KernelOpBundle,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let use_zero_sentinel = zero_sentinel && ops.gather.identity == ReducerIdentity::Zero;
    let mem_size = ident("MEM_SIZE")?;
    let prop_mem = ident("prop_mem")?;
    let cache_data_buffer = ident("cache_data_buffer")?;
    let cache_addr_buffer = ident("cache_addr_buffer")?;
    let identity_val = ident("identity_val")?;
    let identity_word = ident("identity_word")?;
    let init_idx = ident("init_idx")?;
    let i_idx = ident("i")?;
    let kt_elem = ident("kt_elem")?;
    let key = ident("key")?;
    let incoming_dist_pod = ident("incoming_dist_pod")?;
    let word_addr = ident("word_addr")?;
    let current_word = ident("current_word")?;
    let slot = ident("slot")?;
    let bit_low = ident("bit_low")?;
    let bit_high = ident("bit_high")?;
    let current_val = ident("current_val")?;
    let updated_val = ident("updated_val")?;

    let mut body = Vec::new();

    let mem_size_expr = binary(
        HlsBinaryOp::Div,
        binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(ident("MAX_NUM")?),
            HlsExpr::Identifier(ident("LOG_PE_NUM")?),
        ),
        HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
    );

    let _ = mem_size_expr;
    body.push(HlsStatement::Raw(
        "const int32_t MEM_SIZE = ((MAX_NUM >> LOG_PE_NUM) / DISTANCES_PER_REDUCE_WORD);"
            .to_string(),
    ));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: prop_mem.clone(),
        ty: HlsType::array_with_exprs(custom("reduce_word_t"), vec![mem_size.to_string()])?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS BIND_STORAGE variable = prop_mem type = RAM_2P impl = URAM",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS dependence variable = prop_mem inter false",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_data_buffer.clone(),
        ty: HlsType::array_with_exprs(custom("reduce_word_t"), vec!["(L + 1)".to_string()])?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_data_buffer complete dim = 0",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: cache_addr_buffer.clone(),
        ty: HlsType::array_with_exprs(custom("local_id_t"), vec!["(L + 1)".to_string()])?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = cache_addr_buffer complete dim = 0",
    )?));

    if use_zero_sentinel {
        // Zero-sentinel reducers must still initialize the active URAM range on
        // hardware; relying on power-up state causes first-iteration corruption.
        let mut init_mem_body = Vec::new();
        init_mem_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
        init_mem_body.push(assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
                index: Box::new(HlsExpr::Identifier(init_idx.clone())),
            },
            literal_uint(0),
        ));
        body.push(HlsStatement::ForLoop(HlsForLoop {
            label: LoopLabel::new("INIT_REDUCE_MEM")?,
            init: LoopInitializer::Declaration(int_decl("init_idx", literal_int(0))?),
            condition: binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(init_idx.clone()),
                HlsExpr::Identifier(ident("num_word_per_pe")?),
            ),
            increment: LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(init_idx.clone()),
            ),
            body: init_mem_body,
        }));
    } else {
        // Identity-based mode: explicit URAM init with identity value.
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: identity_val.clone(),
            ty: custom("ap_fixed_pod_t"),
            init: Some(reducer_identity_expr(ops.gather.identity)?),
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

        let mut init_mem_body = Vec::new();
        init_mem_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
        init_mem_body.push(assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
                index: Box::new(HlsExpr::Identifier(init_idx.clone())),
            },
            HlsExpr::Identifier(identity_word.clone()),
        ));
        body.push(HlsStatement::ForLoop(HlsForLoop {
            label: LoopLabel::new("INIT_REDUCE_MEM")?,
            init: LoopInitializer::Declaration(int_decl("init_idx", literal_int(0))?),
            condition: binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(init_idx.clone()),
                HlsExpr::Identifier(ident("num_word_per_pe")?),
            ),
            increment: LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(init_idx.clone()),
            ),
            body: init_mem_body,
        }));
    }

    let mut init_cache_body = Vec::new();
    init_cache_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    init_cache_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        literal_uint(0),
    ));
    init_cache_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        if use_zero_sentinel {
            literal_uint(0)
        } else {
            HlsExpr::Identifier(identity_word.clone())
        },
    ));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_23")?,
        init: LoopInitializer::Declaration(int_decl("i", literal_int(0))?),
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
        body: init_cache_body,
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: kt_elem.clone(),
        ty: custom("update_t_big"),
        init: None,
    }));
    while_body.push(HlsStatement::StreamRead {
        stream: ident("kt_wrap_item_single")?,
        target: kt_elem.clone(),
    });
    while_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(kt_elem.clone()), "end_flag")?,
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));

    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: key.clone(),
        ty: custom("local_id_t"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            member_expr(HlsExpr::Identifier(kt_elem.clone()), "node_id")?,
            HlsExpr::Identifier(ident("LOG_PE_NUM")?),
        )),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: incoming_dist_pod.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(member_expr(HlsExpr::Identifier(kt_elem.clone()), "prop")?),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: word_addr.clone(),
        ty: custom("local_id_t"),
        init: Some(binary(
            HlsBinaryOp::Shr,
            HlsExpr::Identifier(key.clone()),
            HlsExpr::Identifier(ident("LOG_DISTANCES_PER_REDUCE_WORD")?),
        )),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: current_word.clone(),
        ty: custom("reduce_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(HlsExpr::Identifier(word_addr.clone())),
        }),
    }));

    let mut cache_probe_body = Vec::new();
    cache_probe_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    cache_probe_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(i_idx.clone())),
            },
            HlsExpr::Identifier(word_addr.clone()),
        ),
        then_body: vec![assignment(
            HlsExpr::Identifier(current_word.clone()),
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
                index: Box::new(HlsExpr::Identifier(i_idx.clone())),
            },
        )],
        else_body: Vec::new(),
    }));

    while_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_24")?,
        init: LoopInitializer::Declaration(int_decl("i", literal_int(0))?),
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
        body: cache_probe_body,
    }));

    let mut cache_shift_body = Vec::new();
    cache_shift_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    cache_shift_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(i_idx.clone()),
                literal_int(1),
            )),
        },
    ));
    cache_shift_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(i_idx.clone()),
                literal_int(1),
            )),
        },
    ));

    while_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_25")?,
        init: LoopInitializer::Declaration(int_decl("i", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("L")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: cache_shift_body,
    }));

    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: slot.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::BitAnd,
            HlsExpr::Identifier(key.clone()),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                literal_uint(1),
            ),
        )),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: bit_low.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Mul,
            HlsExpr::Identifier(slot.clone()),
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        )),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: bit_high.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(bit_low.clone()),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                literal_uint(1),
            ),
        )),
    }));
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: current_val.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(range_method(
            HlsExpr::Identifier(current_word.clone()),
            HlsExpr::Identifier(bit_high.clone()),
            HlsExpr::Identifier(bit_low.clone()),
        )?),
    }));
    let reduce_expr = if use_zero_sentinel {
        reducer_combine_expr_zero_sentinel(
            ops.gather.kind,
            HlsExpr::Identifier(current_val.clone()),
            HlsExpr::Identifier(incoming_dist_pod.clone()),
            Some(custom("ap_fixed_pod_t")),
            false, // reduce: check current
        )
    } else {
        reducer_combine_expr(
            ops.gather.kind,
            HlsExpr::Identifier(current_val.clone()),
            HlsExpr::Identifier(incoming_dist_pod.clone()),
            HlsExpr::Identifier(identity_val.clone()),
            Some(custom("ap_fixed_pod_t")),
        )
    };
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: updated_val.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(reduce_expr),
    }));
    while_body.push(assignment(
        range_method(
            HlsExpr::Identifier(current_word.clone()),
            HlsExpr::Identifier(bit_high.clone()),
            HlsExpr::Identifier(bit_low.clone()),
        )?,
        HlsExpr::Identifier(updated_val.clone()),
    ));
    while_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(HlsExpr::Identifier(word_addr.clone())),
        },
        HlsExpr::Identifier(current_word.clone()),
    ));
    while_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_data_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(ident("L")?)),
        },
        HlsExpr::Identifier(current_word.clone()),
    ));

    while_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(cache_addr_buffer.clone())),
            index: Box::new(HlsExpr::Identifier(ident("L")?)),
        },
        HlsExpr::Identifier(word_addr.clone()),
    ));

    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("LOOP_WHILE_26")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    let mut stream_out_body = Vec::new();
    stream_out_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS UNROLL factor = 1",
    )?));
    stream_out_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("tmp_word")?,
        ty: custom("reduce_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        }),
    }));
    stream_out_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(prop_mem.clone())),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        },
        if use_zero_sentinel {
            literal_uint(0)
        } else {
            HlsExpr::Identifier(identity_word.clone())
        },
    ));
    stream_out_body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(ident("pe_mem_out")?),
        "write",
        vec![HlsExpr::Identifier(ident("tmp_word")?)],
    )?));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_27")?,
        init: LoopInitializer::Declaration(int_decl("i", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("num_word_per_pe")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: stream_out_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_unit_reduce_single_pe")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("kt_wrap_item_single", "update_t_big")?,
            stream_param("pe_mem_out", "reduce_word_t")?,
            scalar_param("num_word_per_pe", HlsType::UInt32)?,
        ],
        body,
    })
}

fn partial_drain_four(_ops: &KernelOpBundle) -> Result<HlsFunction, HlsTemplateError> {
    let word_idx = ident("word_idx")?;
    let pe_offset = ident("pe_offset")?;
    let packed_out = ident("packed_out")?;
    let tmp_word = ident("tmp_word")?;
    let bit_low = ident("bit_low")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = base_idx",
    )?));

    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: packed_out.clone(),
        ty: custom("ap_uint<256>"),
        init: Some(literal_uint(0)),
    }));

    let mut inner_body = Vec::new();
    inner_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    inner_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: tmp_word.clone(),
        ty: custom("reduce_word_t"),
        init: Some(HlsExpr::MethodCall {
            target: Box::new(HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("pe_mem_in")?)),
                index: Box::new(binary(
                    HlsBinaryOp::Add,
                    HlsExpr::Identifier(ident("base_idx")?),
                    HlsExpr::Identifier(pe_offset.clone()),
                )),
            }),
            method: ident("read")?,
            args: vec![],
        }),
    }));
    let dist_idx = ident("dist_idx")?;
    let mut pack_body = Vec::new();
    pack_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    pack_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: bit_low.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Mul,
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Mul,
                    HlsExpr::Identifier(dist_idx.clone()),
                    literal_uint(4),
                ),
                HlsExpr::Identifier(pe_offset.clone()),
            ),
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        )),
    }));
    pack_body.push(assignment(
        range_method(
            HlsExpr::Identifier(packed_out.clone()),
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(bit_low.clone()),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            HlsExpr::Identifier(bit_low.clone()),
        )?,
        range_method(
            HlsExpr::Identifier(tmp_word.clone()),
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
    ));
    inner_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("PACK_DISTANCES_4")?,
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

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_28")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_offset.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(pe_offset.clone()),
            literal_uint(4),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(pe_offset.clone()),
        ),
        body: inner_body,
    }));

    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("partial_out_stream")?,
        value: HlsExpr::Identifier(packed_out.clone()),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_29")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: word_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(word_idx.clone()),
            HlsExpr::Identifier(ident("num_word_per_pe")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(word_idx.clone()),
        ),
        body: outer_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_partial_drain_four")?,
        return_type: HlsType::Void,
        params: vec![
            stream_array_param("pe_mem_in", "reduce_word_t", "PE_NUM")?,
            scalar_param("base_idx", HlsType::UInt32)?,
            scalar_param("num_word_per_pe", HlsType::UInt32)?,
            stream_param("partial_out_stream", "ap_uint<256>")?,
        ],
        body,
    })
}

fn finalize_drain(_ops: &KernelOpBundle) -> Result<HlsFunction, HlsTemplateError> {
    let word_idx = ident("word_idx")?;
    let lower_pe_pack = ident("lower_pe_pack")?;
    let upper_pe_pack = ident("upper_pe_pack")?;
    let one_write_burst = ident("one_write_burst")?;

    let mut body = Vec::new();

    let mut loop_body = Vec::new();
    loop_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: lower_pe_pack.clone(),
        ty: custom("ap_uint<256>"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("lower_pe_pack_stream")?,
        target: lower_pe_pack.clone(),
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: upper_pe_pack.clone(),
        ty: custom("ap_uint<256>"),
        init: None,
    }));
    loop_body.push(HlsStatement::StreamRead {
        stream: ident("upper_pe_pack_stream")?,
        target: upper_pe_pack.clone(),
    });
    loop_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: one_write_burst.clone(),
        ty: custom("write_burst_pkt_t"),
        init: None,
    }));

    loop_body.push(assignment(
        range_method(
            member_expr(HlsExpr::Identifier(one_write_burst.clone()), "data")?,
            literal_uint(127),
            literal_uint(0),
        )?,
        range_method(
            HlsExpr::Identifier(lower_pe_pack.clone()),
            literal_uint(127),
            literal_uint(0),
        )?,
    ));
    loop_body.push(assignment(
        range_method(
            member_expr(HlsExpr::Identifier(one_write_burst.clone()), "data")?,
            literal_uint(255),
            literal_uint(128),
        )?,
        range_method(
            HlsExpr::Identifier(upper_pe_pack.clone()),
            literal_uint(127),
            literal_uint(0),
        )?,
    ));
    loop_body.push(assignment(
        range_method(
            member_expr(HlsExpr::Identifier(one_write_burst.clone()), "data")?,
            literal_uint(383),
            literal_uint(256),
        )?,
        range_method(
            HlsExpr::Identifier(lower_pe_pack.clone()),
            literal_uint(255),
            literal_uint(128),
        )?,
    ));
    loop_body.push(assignment(
        range_method(
            member_expr(HlsExpr::Identifier(one_write_burst.clone()), "data")?,
            literal_uint(511),
            literal_uint(384),
        )?,
        range_method(
            HlsExpr::Identifier(upper_pe_pack.clone()),
            literal_uint(255),
            literal_uint(128),
        )?,
    ));

    loop_body.push(HlsStatement::StreamWrite {
        stream: ident("kernel_out_stream")?,
        value: HlsExpr::Identifier(one_write_burst.clone()),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_30")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: word_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(word_idx.clone()),
            HlsExpr::Identifier(ident("num_word_per_pe")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(word_idx.clone()),
        ),
        body: loop_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_finalize_drain")?,
        return_type: HlsType::Void,
        params: vec![
            stream_param("lower_pe_pack_stream", "ap_uint<256>")?,
            stream_param("upper_pe_pack_stream", "ap_uint<256>")?,
            scalar_param("num_word_per_pe", HlsType::UInt32)?,
            stream_param("kernel_out_stream", "write_burst_pkt_t")?,
        ],
        body,
    })
}

fn drain_variable(ops: &KernelOpBundle) -> Result<HlsFunction, HlsTemplateError> {
    let word_idx = ident("word_idx")?;
    let pe_idx = ident("pe_idx")?;
    let tmp_word = ident("tmp_word")?;
    let bit_low = ident("bit_low")?;
    let identity_val = ident("identity_val")?;
    let identity_word = ident("identity_word")?;
    let one_write_burst = ident("one_write_burst")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: identity_val.clone(),
        ty: custom("ap_fixed_pod_t"),
        init: Some(reducer_identity_expr(ops.gather.identity)?),
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
        label: LoopLabel::new("DRAIN_INIT_IDENTITY_WORD")?,
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

    let reduce_words_per_bus = ident("reduce_words_per_bus")?;
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: reduce_words_per_bus.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Div,
            HlsExpr::Identifier(ident("DIST_PER_WORD")?),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        )),
    }));
    let reduce_words_per_pe = ident("reduce_words_per_pe")?;
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: reduce_words_per_pe.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Div,
            HlsExpr::Identifier(reduce_words_per_bus.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        )),
    }));
    let num_bus_words = ident("num_bus_words")?;
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: num_bus_words.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Div,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(ident("num_word_per_pe")?),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(reduce_words_per_pe.clone()),
                    literal_uint(1),
                ),
            ),
            HlsExpr::Identifier(reduce_words_per_pe.clone()),
        )),
    }));

    let sub_idx = ident("sub_idx")?;
    let word_base = ident("word_base")?;
    let slot_idx = ident("slot_idx")?;
    let dist_idx = ident("dist_idx")?;

    let mut pack_body = Vec::new();
    pack_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    pack_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: slot_idx.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Add,
            binary(
                HlsBinaryOp::Mul,
                binary(
                    HlsBinaryOp::Add,
                    binary(
                        HlsBinaryOp::Mul,
                        HlsExpr::Identifier(sub_idx.clone()),
                        HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                    ),
                    HlsExpr::Identifier(dist_idx.clone()),
                ),
                HlsExpr::Identifier(ident("PE_NUM")?),
            ),
            HlsExpr::Identifier(pe_idx.clone()),
        )),
    }));
    pack_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: bit_low.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Mul,
            HlsExpr::Identifier(slot_idx.clone()),
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
        )),
    }));
    pack_body.push(assignment(
        range_method(
            member_expr(HlsExpr::Identifier(one_write_burst.clone()), "data")?,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(bit_low.clone()),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                    literal_uint(1),
                ),
            ),
            HlsExpr::Identifier(bit_low.clone()),
        )?,
        range_method(
            HlsExpr::Identifier(tmp_word.clone()),
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
    ));

    let mut pe_body = Vec::new();
    pe_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    pe_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: tmp_word.clone(),
        ty: custom("reduce_word_t"),
        init: Some(HlsExpr::Identifier(identity_word.clone())),
    }));
    pe_body.push(HlsStatement::IfElse(HlsIfElse {
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(word_base.clone()),
            HlsExpr::Identifier(ident("num_word_per_pe")?),
        ),
        then_body: vec![assignment(
            HlsExpr::Identifier(tmp_word.clone()),
            HlsExpr::MethodCall {
                target: Box::new(HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(ident("pe_mem_in")?)),
                    index: Box::new(HlsExpr::Identifier(pe_idx.clone())),
                }),
                method: ident("read")?,
                args: vec![],
            },
        )],
        else_body: Vec::new(),
    }));
    pe_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("PACK_DISTANCES")?,
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

    let mut sub_body = Vec::new();
    sub_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    sub_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: word_base.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Add,
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(word_idx.clone()),
                HlsExpr::Identifier(reduce_words_per_pe.clone()),
            ),
            HlsExpr::Identifier(sub_idx.clone()),
        )),
    }));
    sub_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_DRAIN_PE")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: pe_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
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
        body: pe_body,
    }));

    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: one_write_burst.clone(),
        ty: custom("write_burst_pkt_t"),
        init: None,
    }));
    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_DRAIN_SUBWORD")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: sub_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(sub_idx.clone()),
            HlsExpr::Identifier(reduce_words_per_pe.clone()),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(sub_idx.clone()),
        ),
        body: sub_body,
    }));
    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("kernel_out_stream")?,
        value: HlsExpr::Identifier(one_write_burst.clone()),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_DRAIN_WORD")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: word_idx.clone(),
            ty: HlsType::UInt32,
            init: Some(literal_uint(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(word_idx.clone()),
            HlsExpr::Identifier(num_bus_words.clone()),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(word_idx.clone()),
        ),
        body: outer_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("Reduc_105_drain_variable")?,
        return_type: HlsType::Void,
        params: vec![
            stream_array_param("pe_mem_in", "reduce_word_t", "PE_NUM")?,
            scalar_param("num_word_per_pe", HlsType::UInt32)?,
            stream_param("kernel_out_stream", "write_burst_pkt_t")?,
        ],
        body,
    })
}

fn graphyflow_big_top(
    _ops: &KernelOpBundle,
    edge: &crate::domain::hls_template::HlsEdgeConfig,
) -> Result<HlsFunction, HlsTemplateError> {
    let num_words = ident("num_words")?;
    let num_word_per_pe = ident("num_word_per_pe")?;
    let edges_per_word = ident("edges_per_word")?;
    let num_wide_reads = ident("num_wide_reads")?;
    let total_edge_sets = ident("total_edge_sets")?;
    let i_idx = ident("i")?;
    let j_idx = ident("j")?;
    let wide_word = ident("wide_word")?;
    let edge_batch = ident("edge_batch")?;
    let packed_edge = ident("packed_edge")?;
    let src_id_burst = ident("src_id_burst")?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE m_axi port = edge_props offset = slave bundle = gmem0",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = edge_props",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = num_nodes",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = num_edges",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = dst_num",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = memory_offset",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS INTERFACE s_axilite port = return",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_src_ids")?,
        ty: HlsType::Stream(Box::new(custom("node_id_burst_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_src_ids depth = 16",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_dist_req")?,
        ty: HlsType::Stream(Box::new(custom("distance_req_pack_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_dist_req depth = 16",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_cachelines")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("bus_word_t"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_cachelines depth = 16",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("edge_stream")?,
        ty: HlsType::Stream(Box::new(custom("edge_descriptor_batch_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = edge_stream depth = 16",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_edge_data")?,
        ty: HlsType::Stream(Box::new(custom("update_tuple_t_big"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_edge_data depth = 16",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("cacheline_req")?,
        ty: HlsType::Stream(Box::new(custom("cacheline_req_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = cacheline_req depth = 32",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("cacheline_resp")?,
        ty: HlsType::Stream(Box::new(custom("cacheline_resp_t"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = cacheline_resp depth = 32",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("reduce_105_d2o_pair")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("update_t_big"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = reduce_105_d2o_pair depth = 8",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = reduce_105_d2o_pair complete dim = 0",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("reduce_105_o2u_pair")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("update_t_big"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = reduce_105_o2u_pair depth = 2",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = reduce_105_o2u_pair complete dim = 0",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: num_words.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Shr,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(ident("dst_num")?),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                    literal_uint(1),
                ),
            ),
            HlsExpr::Identifier(ident("LOG_DISTANCES_PER_REDUCE_WORD")?),
        )),
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: num_word_per_pe.clone(),
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Shr,
            binary(
                HlsBinaryOp::Add,
                HlsExpr::Identifier(num_words.clone()),
                binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(ident("PE_NUM")?),
                    literal_uint(1),
                ),
            ),
            HlsExpr::Identifier(ident("LOG_PE_NUM")?),
        )),
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: edges_per_word.clone(),
        ty: HlsType::Int32,
        init: Some(HlsExpr::Identifier(ident("EDGES_PER_WORD")?)),
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: num_wide_reads.clone(),
        ty: HlsType::Int32,
        init: Some(binary(
            HlsBinaryOp::Div,
            HlsExpr::Identifier(ident("num_edges")?),
            HlsExpr::Identifier(edges_per_word.clone()),
        )),
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: total_edge_sets.clone(),
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Cast {
            target_type: HlsType::UInt32,
            expr: Box::new(HlsExpr::Identifier(num_wide_reads.clone())),
        }),
    }));

    let mut outer_body = Vec::new();
    outer_body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: wide_word.clone(),
        ty: custom("bus_word_t"),
        init: Some(HlsExpr::Index {
            target: Box::new(HlsExpr::Identifier(ident("edge_props")?)),
            index: Box::new(HlsExpr::Identifier(i_idx.clone())),
        }),
    }));
    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: edge_batch.clone(),
        ty: custom("edge_descriptor_batch_t"),
        init: None,
    }));

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

    let mut unpack_body = Vec::new();
    unpack_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    unpack_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: packed_edge.clone(),
        ty: custom("ap_uint<EDGE_PAYLOAD_BITS>"),
        init: Some(range_method(
            HlsExpr::Identifier(wide_word.clone()),
            payload_high,
            payload_shift.clone(),
        )?),
    }));

    let edge_entry = HlsExpr::Index {
        target: Box::new(member_expr(
            HlsExpr::Identifier(edge_batch.clone()),
            "edges",
        )?),
        index: Box::new(HlsExpr::Identifier(j_idx.clone())),
    };

    unpack_body.push(assignment(
        member_expr(edge_entry.clone(), "dst_id")?,
        range_method(
            HlsExpr::Identifier(packed_edge.clone()),
            HlsExpr::Identifier(ident("LOCAL_ID_MSB")?),
            literal_uint(0),
        )?,
    ));
    unpack_body.push(assignment(
        member_expr(edge_entry.clone(), "src_id")?,
        range_method(
            HlsExpr::Identifier(packed_edge.clone()),
            HlsExpr::Identifier(ident("EDGE_SRC_PAYLOAD_MSB")?),
            HlsExpr::Identifier(ident("EDGE_SRC_PAYLOAD_LSB")?),
        )?,
    ));
    if edge.edge_prop_bits > 0 {
        // Host packing layout is configuration-dependent: HBM stores edge
        // props above bit 63, while DDR compacts them into the upper bits of
        // the low 32-bit destination lane.
        unpack_body.push(assignment(
            member_expr(edge_entry.clone(), "edge_prop")?,
            range_method(
                HlsExpr::Identifier(packed_edge.clone()),
                HlsExpr::Identifier(ident("EDGE_PROP_PAYLOAD_MSB")?),
                HlsExpr::Identifier(ident("EDGE_PROP_PAYLOAD_LSB")?),
            )?,
        ));
    }

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_45")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: j_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(j_idx.clone()),
            HlsExpr::Identifier(edges_per_word.clone()),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(j_idx.clone()),
        ),
        body: unpack_body,
    }));

    let mut dummy_body = Vec::new();
    dummy_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    dummy_body.push(assignment(
        member_expr(edge_entry.clone(), "dst_id")?,
        HlsExpr::Identifier(ident("INVALID_LOCAL_ID_BIG")?),
    ));
    dummy_body.push(assignment(
        member_expr(edge_entry.clone(), "src_id")?,
        member_expr(
            HlsExpr::Index {
                target: Box::new(member_expr(
                    HlsExpr::Identifier(edge_batch.clone()),
                    "edges",
                )?),
                index: Box::new(binary(
                    HlsBinaryOp::Sub,
                    HlsExpr::Identifier(edges_per_word.clone()),
                    literal_int(1),
                )),
            },
            "src_id",
        )?,
    ));
    if edge.edge_prop_bits > 0 {
        dummy_body.push(assignment(
            member_expr(edge_entry.clone(), "edge_prop")?,
            literal_uint(0),
        ));
    }

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_45_PAD")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: j_idx.clone(),
            ty: HlsType::Int32,
            init: Some(HlsExpr::Identifier(edges_per_word.clone())),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(j_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(j_idx.clone()),
        ),
        body: dummy_body,
    }));

    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("edge_stream")?,
        value: HlsExpr::Identifier(edge_batch.clone()),
    });

    outer_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: src_id_burst.clone(),
        ty: custom("node_id_burst_t"),
        init: None,
    }));

    let mut burst_body = Vec::new();
    burst_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    burst_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("src_id")?,
        ty: custom("node_id_t"),
        init: Some(member_expr(edge_entry.clone(), "src_id")?),
    }));
    burst_body.push(assignment(
        HlsExpr::Index {
            target: Box::new(member_expr(
                HlsExpr::Identifier(src_id_burst.clone()),
                "data",
            )?),
            index: Box::new(HlsExpr::Identifier(j_idx.clone())),
        },
        HlsExpr::Identifier(ident("src_id")?),
    ));

    outer_body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_46")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: j_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(j_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(j_idx.clone()),
        ),
        body: burst_body,
    }));

    outer_body.push(HlsStatement::StreamWrite {
        stream: ident("stream_src_ids")?,
        value: HlsExpr::Identifier(src_id_burst.clone()),
    });

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_47")?,
        init: LoopInitializer::Declaration(HlsVarDecl {
            name: i_idx.clone(),
            ty: HlsType::Int32,
            init: Some(literal_int(0)),
        }),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(num_wide_reads.clone()),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: outer_body,
    }));

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("dist_req_packer")?,
        args: vec![
            HlsExpr::Identifier(ident("stream_src_ids")?),
            HlsExpr::Identifier(ident("stream_dist_req")?),
            HlsExpr::Identifier(total_edge_sets.clone()),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("cacheline_req_sender")?,
        args: vec![
            HlsExpr::Identifier(ident("stream_dist_req")?),
            HlsExpr::Identifier(ident("cacheline_req")?),
            HlsExpr::Identifier(ident("memory_offset")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("stream2axistream")?,
        args: vec![
            HlsExpr::Identifier(ident("cacheline_req")?),
            HlsExpr::Identifier(ident("cacheline_req_stream")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("axistream2stream")?,
        args: vec![
            HlsExpr::Identifier(ident("cacheline_resp_stream")?),
            HlsExpr::Identifier(ident("cacheline_resp")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("node_prop_resp_receiver")?,
        args: vec![
            HlsExpr::Identifier(ident("cacheline_resp")?),
            HlsExpr::Identifier(ident("stream_cachelines")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("merge_node_props")?,
        args: vec![
            HlsExpr::Identifier(ident("stream_cachelines")?),
            HlsExpr::Identifier(ident("edge_stream")?),
            HlsExpr::Identifier(ident("stream_edge_data")?),
            HlsExpr::Identifier(total_edge_sets.clone()),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("demux_1")?,
        args: vec![
            HlsExpr::Identifier(ident("stream_edge_data")?),
            HlsExpr::Identifier(ident("reduce_105_d2o_pair")?),
            HlsExpr::Identifier(total_edge_sets.clone()),
        ],
    }));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_stage_0")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("update_t_big"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_stage_0 depth = 2",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = stream_stage_0 complete dim = 0",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("stream_stage_1")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("update_t_big"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = stream_stage_1 depth = 2",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = stream_stage_1 complete dim = 0",
    )?));

    let mut network_body = Vec::new();
    match edge.big_pe {
        8 => {
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(2),
                    index_ident("reduce_105_d2o_pair", literal_int(0))?,
                    index_ident("reduce_105_d2o_pair", literal_int(1))?,
                    index_ident("stream_stage_0", literal_int(0))?,
                    index_ident("stream_stage_0", literal_int(1))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(2),
                    index_ident("reduce_105_d2o_pair", literal_int(2))?,
                    index_ident("reduce_105_d2o_pair", literal_int(3))?,
                    index_ident("stream_stage_0", literal_int(2))?,
                    index_ident("stream_stage_0", literal_int(3))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(2),
                    index_ident("reduce_105_d2o_pair", literal_int(4))?,
                    index_ident("reduce_105_d2o_pair", literal_int(5))?,
                    index_ident("stream_stage_0", literal_int(4))?,
                    index_ident("stream_stage_0", literal_int(5))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(2),
                    index_ident("reduce_105_d2o_pair", literal_int(6))?,
                    index_ident("reduce_105_d2o_pair", literal_int(7))?,
                    index_ident("stream_stage_0", literal_int(6))?,
                    index_ident("stream_stage_0", literal_int(7))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("stream_stage_0", literal_int(0))?,
                    index_ident("stream_stage_0", literal_int(4))?,
                    index_ident("stream_stage_1", literal_int(0))?,
                    index_ident("stream_stage_1", literal_int(1))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("stream_stage_0", literal_int(1))?,
                    index_ident("stream_stage_0", literal_int(5))?,
                    index_ident("stream_stage_1", literal_int(2))?,
                    index_ident("stream_stage_1", literal_int(3))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("stream_stage_0", literal_int(2))?,
                    index_ident("stream_stage_0", literal_int(6))?,
                    index_ident("stream_stage_1", literal_int(4))?,
                    index_ident("stream_stage_1", literal_int(5))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("stream_stage_0", literal_int(3))?,
                    index_ident("stream_stage_0", literal_int(7))?,
                    index_ident("stream_stage_1", literal_int(6))?,
                    index_ident("stream_stage_1", literal_int(7))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_1", literal_int(0))?,
                    index_ident("stream_stage_1", literal_int(4))?,
                    index_ident("reduce_105_o2u_pair", literal_int(0))?,
                    index_ident("reduce_105_o2u_pair", literal_int(1))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_1", literal_int(1))?,
                    index_ident("stream_stage_1", literal_int(5))?,
                    index_ident("reduce_105_o2u_pair", literal_int(2))?,
                    index_ident("reduce_105_o2u_pair", literal_int(3))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_1", literal_int(2))?,
                    index_ident("stream_stage_1", literal_int(6))?,
                    index_ident("reduce_105_o2u_pair", literal_int(4))?,
                    index_ident("reduce_105_o2u_pair", literal_int(5))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_1", literal_int(3))?,
                    index_ident("stream_stage_1", literal_int(7))?,
                    index_ident("reduce_105_o2u_pair", literal_int(6))?,
                    index_ident("reduce_105_o2u_pair", literal_int(7))?,
                ],
            }));
        }
        4 => {
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("reduce_105_d2o_pair", literal_int(0))?,
                    index_ident("reduce_105_d2o_pair", literal_int(1))?,
                    index_ident("stream_stage_0", literal_int(0))?,
                    index_ident("stream_stage_0", literal_int(1))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(1),
                    index_ident("reduce_105_d2o_pair", literal_int(2))?,
                    index_ident("reduce_105_d2o_pair", literal_int(3))?,
                    index_ident("stream_stage_0", literal_int(2))?,
                    index_ident("stream_stage_0", literal_int(3))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_0", literal_int(0))?,
                    index_ident("stream_stage_0", literal_int(2))?,
                    index_ident("reduce_105_o2u_pair", literal_int(0))?,
                    index_ident("reduce_105_o2u_pair", literal_int(1))?,
                ],
            }));
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("stream_stage_0", literal_int(1))?,
                    index_ident("stream_stage_0", literal_int(3))?,
                    index_ident("reduce_105_o2u_pair", literal_int(2))?,
                    index_ident("reduce_105_o2u_pair", literal_int(3))?,
                ],
            }));
        }
        2 => {
            network_body.push(HlsStatement::Expr(HlsExpr::Call {
                function: ident("switch2x2_2")?,
                args: vec![
                    literal_int(0),
                    index_ident("reduce_105_d2o_pair", literal_int(0))?,
                    index_ident("reduce_105_d2o_pair", literal_int(1))?,
                    index_ident("reduce_105_o2u_pair", literal_int(0))?,
                    index_ident("reduce_105_o2u_pair", literal_int(1))?,
                ],
            }));
        }
        _ => {}
    }
    body.extend(network_body);

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("pe_mem_out_streams")?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom("reduce_word_t"))),
            vec!["PE_NUM".to_string()],
        )?,
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = pe_mem_out_streams depth = 4",
    )?));

    let reduce_input = if edge.big_pe == 1 {
        "reduce_105_d2o_pair"
    } else {
        "reduce_105_o2u_pair"
    };
    let mut reduce_body = Vec::new();
    reduce_body.push(HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?));
    reduce_body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_unit_reduce_single_pe")?,
        args: vec![
            index_ident(reduce_input, HlsExpr::Identifier(i_idx.clone()))?,
            index_ident("pe_mem_out_streams", HlsExpr::Identifier(i_idx.clone()))?,
            HlsExpr::Identifier(num_word_per_pe.clone()),
        ],
    }));

    body.push(HlsStatement::ForLoop(HlsForLoop {
        label: LoopLabel::new("LOOP_FOR_48")?,
        init: LoopInitializer::Declaration(int_decl("i", literal_int(0))?),
        condition: binary(
            HlsBinaryOp::Lt,
            HlsExpr::Identifier(i_idx.clone()),
            HlsExpr::Identifier(ident("PE_NUM")?),
        ),
        increment: LoopIncrement::Unary(
            HlsUnaryOp::PreIncrement,
            HlsExpr::Identifier(i_idx.clone()),
        ),
        body: reduce_body,
    }));

    // Two-stage drain: 4 PEs lower + 4 PEs upper → finalize merge.
    // This matches the SG reference drain structure.
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("drain_lower_stream")?,
        ty: HlsType::Stream(Box::new(custom("ap_uint<256>"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = drain_lower_stream depth = 4",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("drain_upper_stream")?,
        ty: HlsType::Stream(Box::new(custom("ap_uint<256>"))),
        init: None,
    }));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS STREAM variable = drain_upper_stream depth = 4",
    )?));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_partial_drain_four")?,
        args: vec![
            HlsExpr::Identifier(ident("pe_mem_out_streams")?),
            literal_uint(0),
            HlsExpr::Identifier(num_word_per_pe.clone()),
            HlsExpr::Identifier(ident("drain_lower_stream")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_partial_drain_four")?,
        args: vec![
            HlsExpr::Identifier(ident("pe_mem_out_streams")?),
            literal_uint(4),
            HlsExpr::Identifier(num_word_per_pe.clone()),
            HlsExpr::Identifier(ident("drain_upper_stream")?),
        ],
    }));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("Reduc_105_finalize_drain")?,
        args: vec![
            HlsExpr::Identifier(ident("drain_lower_stream")?),
            HlsExpr::Identifier(ident("drain_upper_stream")?),
            HlsExpr::Identifier(num_word_per_pe.clone()),
            HlsExpr::Identifier(ident("kernel_out_stream")?),
        ],
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("graphyflow_big")?,
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
            stream_param("cacheline_req_stream", "cacheline_request_pkt_t")?,
            stream_param("cacheline_resp_stream", "cacheline_response_pkt_t")?,
            stream_param("kernel_out_stream", "write_burst_pkt_t")?,
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

fn stream_array_param(name: &str, ty: &str, dim: &str) -> Result<HlsParameter, HlsTemplateError> {
    Ok(HlsParameter {
        name: ident(name)?,
        ty: HlsType::array_with_exprs(
            HlsType::Stream(Box::new(custom(ty))),
            vec![dim.to_string()],
        )?,
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
