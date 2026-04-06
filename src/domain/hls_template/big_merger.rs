use crate::domain::hls::{
    HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsFunction, HlsInclude, HlsParameter, HlsPragma,
    HlsStatement, HlsType, HlsUnaryOp, HlsVarDecl, HlsWhileLoop, LoopIncrement, LoopLabel,
    PassingStyle,
};
use crate::domain::hls_ops::{KernelOpBundle, ReducerKind};

use super::utils::{
    HlsForLoopBuilder, assignment, binary, custom, ident, index_ident, int_decl, literal_bool,
    literal_int, literal_uint, member_expr, method_call, range_method, raw, reducer_combine_expr,
    reducer_combine_expr_zero_sentinel,
    reducer_identity_expr,
};
use super::{HlsKernelConfig, HlsTemplateError};

fn use_zero_sentinel_big_merge(ops: &KernelOpBundle, zero_sentinel: bool) -> bool {
    zero_sentinel
        && matches!(
            ops.gather.kind,
            ReducerKind::Sum | ReducerKind::Or | ReducerKind::MaskedMinIgnoreZero { .. }
        )
}

/// Structured description of `big_merger.cpp`.
pub fn big_merger_unit(
    ops: &KernelOpBundle,
    config: &HlsKernelConfig,
    zero_sentinel: bool,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![merge_big_kernels(ops, config, zero_sentinel)?, big_merger_top(config)?],
    })
}

/// Structured description of a group-local `big_merger_<gid>.cpp` (pipeline count is per group).
pub fn big_merger_group_unit(
    ops: &KernelOpBundle,
    pipelines: usize,
    group_id: usize,
    zero_sentinel: bool,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            merge_big_kernels_group(ops, pipelines, group_id, zero_sentinel)?,
            big_merger_top_group(pipelines, group_id)?,
        ],
    })
}

fn merge_big_kernels(
    ops: &KernelOpBundle,
    config: &HlsKernelConfig,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(raw("write_burst_pkt_t tmp_prop_pkt[BIG_MERGER_LENGTH];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete",
    )?));
    body.push(raw("bool process_flag[BIG_MERGER_LENGTH];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = process_flag dim = 0 complete",
    )?));
    body.push(
        HlsForLoopBuilder::new("init_process_flag")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("BIG_MERGER_LENGTH")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("process_flag", HlsExpr::Identifier(ident("i")?))?,
                    literal_bool(false),
                ),
            ])
            .build(),
    );

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merged_write_burst")?,
        ty: custom("bus_word_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_write_burst")?,
        ty: custom("write_burst_pkt_t"),
        init: None,
    }));
    body.push(raw("ap_fixed_pod_t tmp_prop_arrary[DIST_PER_WORD];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = tmp_prop_arrary dim = 0 complete",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("identity_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(reducer_identity_expr(ops.gather.identity)?),
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS pipeline style = flp",
    )?));

    for idx in 0..config.big_kernels {
        let stream = format!("big_kernel_{}_out_stream", idx + 1);
        while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
            condition: HlsExpr::Unary {
                op: HlsUnaryOp::LogicalNot,
                expr: Box::new(index_ident("process_flag", literal_uint(idx as u64))?),
            },
            then_body: vec![assignment(
                index_ident("process_flag", literal_uint(idx as u64))?,
                method_call(
                    HlsExpr::Identifier(ident(&stream)?),
                    "read_nb",
                    vec![index_ident("tmp_prop_pkt", literal_uint(idx as u64))?],
                )?,
            )],
            else_body: Vec::new(),
        }));
    }

    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merge_flag")?,
        ty: HlsType::Bool,
        init: Some(merge_flag_expr(config)?),
    }));
    while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("merge_flag")?),
        then_body: {
            let mut then_body = Vec::new();
            then_body.extend(merge_inner_body(ops, zero_sentinel)?);
            then_body.push(
                HlsForLoopBuilder::new("reset_process_flag")?
                    .init(int_decl("i", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("i")?),
                        HlsExpr::Identifier(ident("BIG_MERGER_LENGTH")?),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("i")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("process_flag", HlsExpr::Identifier(ident("i")?))?,
                            literal_bool(false),
                        ),
                    ])
                    .build(),
            );
            then_body
        },
        else_body: Vec::new(),
    }));
    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("merge_tmp_prop_big_krnls")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_big_kernels")?,
        return_type: HlsType::Void,
        params: big_merger_params(config)?,
        body,
    })
}

fn merge_big_kernels_group(
    ops: &KernelOpBundle,
    pipelines: usize,
    group_id: usize,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(raw(&format!(
        "write_burst_pkt_t tmp_prop_pkt[{pipelines}];"
    )));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete",
    )?));
    body.push(raw(&format!("bool process_flag[{pipelines}];")));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = process_flag dim = 0 complete",
    )?));
    body.push(
        HlsForLoopBuilder::new("init_process_flag")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                literal_int(pipelines as i64),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("process_flag", HlsExpr::Identifier(ident("i")?))?,
                    literal_bool(false),
                ),
            ])
            .build(),
    );

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merged_write_burst")?,
        ty: custom("bus_word_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_write_burst")?,
        ty: custom("write_burst_pkt_t"),
        init: None,
    }));
    body.push(raw("ap_fixed_pod_t tmp_prop_arrary[DIST_PER_WORD];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = tmp_prop_arrary dim = 0 complete",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("identity_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(reducer_identity_expr(ops.gather.identity)?),
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS pipeline style = flp",
    )?));
    for idx in 0..pipelines {
        let stream = format!("big_kernel_{}_out_stream", idx + 1);
        while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
            condition: HlsExpr::Unary {
                op: HlsUnaryOp::LogicalNot,
                expr: Box::new(index_ident("process_flag", literal_uint(idx as u64))?),
            },
            then_body: vec![assignment(
                index_ident("process_flag", literal_uint(idx as u64))?,
                method_call(
                    HlsExpr::Identifier(ident(&stream)?),
                    "read_nb",
                    vec![index_ident("tmp_prop_pkt", literal_uint(idx as u64))?],
                )?,
            )],
            else_body: Vec::new(),
        }));
    }
    while_body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("merge_flag")?,
        ty: HlsType::Bool,
        init: Some(merge_flag_expr_group(pipelines)?),
    }));
    while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("merge_flag")?),
        then_body: {
            let mut then_body = Vec::new();
            then_body.extend(merge_inner_body_group(zero_sentinel, ops, pipelines)?);
            then_body.push(
                HlsForLoopBuilder::new("reset_process_flag")?
                    .init(int_decl("i", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("i")?),
                        literal_int(pipelines as i64),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("i")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("process_flag", HlsExpr::Identifier(ident("i")?))?,
                            literal_bool(false),
                        ),
                    ])
                    .build(),
            );
            then_body
        },
        else_body: Vec::new(),
    }));
    body.push(HlsStatement::WhileLoop(HlsWhileLoop {
        label: LoopLabel::new("merge_tmp_prop_big_krnls")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident(&format!("merge_big_kernels_{group_id}"))?,
        return_type: HlsType::Void,
        params: big_merger_params_group(pipelines)?,
        body,
    })
}

fn merge_inner_body(ops: &KernelOpBundle, zero_sentinel: bool) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let use_zero_sentinel = use_zero_sentinel_big_merge(ops, zero_sentinel);
    let mut body = Vec::new();

    body.push(
        HlsForLoopBuilder::new("init_tmp_prop")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("i")?))?,
                    HlsExpr::Identifier(ident("identity_pod")?),
                ),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("merge_outer")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("BIG_MERGER_LENGTH")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                HlsForLoopBuilder::new("merge_inner")?
                    .init(int_decl("j", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("j")?),
                        HlsExpr::Identifier(ident("DIST_PER_WORD")?),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("j")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("j")?))?,
                            {
                                let current = index_ident(
                                    "tmp_prop_arrary",
                                    HlsExpr::Identifier(ident("j")?),
                                )?;
                                let update = range_method(
                                    member_expr(
                                        index_ident(
                                            "tmp_prop_pkt",
                                            HlsExpr::Identifier(ident("i")?),
                                        )?,
                                        "data",
                                    )?,
                                    range_high("j")?,
                                    range_low("j")?,
                                )?;
                                if use_zero_sentinel {
                                    reducer_combine_expr_zero_sentinel(
                                        ops.gather.kind,
                                        current,
                                        update,
                                        Some(custom("ap_fixed_pod_t")),
                                        true, // merger: check incoming
                                    )
                                } else {
                                    reducer_combine_expr(
                                        ops.gather.kind,
                                        current,
                                        update,
                                        HlsExpr::Identifier(ident("identity_pod")?),
                                        Some(custom("ap_fixed_pod_t")),
                                    )
                                }
                            },
                        ),
                    ])
                    .build(),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("pack_output")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    range_method(
                        HlsExpr::Identifier(ident("merged_write_burst")?),
                        range_high("i")?,
                        range_low("i")?,
                    )?,
                    index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("i")?))?,
                ),
            ])
            .build(),
    );

    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "data")?,
        HlsExpr::Identifier(ident("merged_write_burst")?),
    ));
    body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(ident("kernel_out_stream")?),
        "write",
        vec![HlsExpr::Identifier(ident("one_write_burst")?)],
    )?));

    Ok(body)
}

fn merge_inner_body_group(zero_sentinel: bool,
    ops: &KernelOpBundle,
    pipelines: usize,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let use_zero_sentinel = use_zero_sentinel_big_merge(ops, zero_sentinel);
    let mut body = Vec::new();

    body.push(
        HlsForLoopBuilder::new("init_tmp_prop")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("i")?))?,
                    HlsExpr::Identifier(ident("identity_pod")?),
                ),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("merge_outer")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                literal_int(pipelines as i64),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                HlsForLoopBuilder::new("merge_inner")?
                    .init(int_decl("j", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("j")?),
                        HlsExpr::Identifier(ident("DIST_PER_WORD")?),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("j")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("j")?))?,
                            {
                                let current = index_ident(
                                    "tmp_prop_arrary",
                                    HlsExpr::Identifier(ident("j")?),
                                )?;
                                let update = range_method(
                                    member_expr(
                                        index_ident(
                                            "tmp_prop_pkt",
                                            HlsExpr::Identifier(ident("i")?),
                                        )?,
                                        "data",
                                    )?,
                                    range_high("j")?,
                                    range_low("j")?,
                                )?;
                                if use_zero_sentinel {
                                    reducer_combine_expr_zero_sentinel(
                                        ops.gather.kind,
                                        current,
                                        update,
                                        Some(custom("ap_fixed_pod_t")),
                                        true, // merger: check incoming
                                    )
                                } else {
                                    reducer_combine_expr(
                                        ops.gather.kind,
                                        current,
                                        update,
                                        HlsExpr::Identifier(ident("identity_pod")?),
                                        Some(custom("ap_fixed_pod_t")),
                                    )
                                }
                            },
                        ),
                    ])
                    .build(),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("pack")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("DIST_PER_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    range_method(
                        HlsExpr::Identifier(ident("merged_write_burst")?),
                        range_high("i")?,
                        range_low("i")?,
                    )?,
                    index_ident("tmp_prop_arrary", HlsExpr::Identifier(ident("i")?))?,
                ),
            ])
            .build(),
    );

    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "data")?,
        HlsExpr::Identifier(ident("merged_write_burst")?),
    ));
    body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(ident("kernel_out_stream")?),
        "write",
        vec![HlsExpr::Identifier(ident("one_write_burst")?)],
    )?));

    Ok(body)
}

fn big_merger_top(config: &HlsKernelConfig) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS interface ap_ctrl_none port = return",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));
    let params = big_merger_params(config)?;
    let call_args = params
        .iter()
        .map(|param| HlsExpr::Identifier(param.name.clone()))
        .collect::<Vec<_>>();
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("merge_big_kernels")?,
        args: call_args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("big_merger")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn big_merger_top_group(
    pipelines: usize,
    group_id: usize,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS interface ap_ctrl_none port = return",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));
    let params = big_merger_params_group(pipelines)?;
    let call_args = params
        .iter()
        .map(|param| HlsExpr::Identifier(param.name.clone()))
        .collect::<Vec<_>>();
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident(&format!("merge_big_kernels_{group_id}"))?,
        args: call_args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident(&format!("big_merger_{group_id}"))?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn big_merger_params(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    params.extend(streams_with_output(config)?);
    Ok(params)
}

fn big_merger_params_group(pipelines: usize) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    params.extend(streams_with_output_group(pipelines)?);
    Ok(params)
}

fn merge_flag_expr_group(pipelines: usize) -> Result<HlsExpr, HlsTemplateError> {
    let mut expr = index_ident("process_flag", literal_uint(0))?;
    for idx in 1..pipelines {
        expr = binary(
            HlsBinaryOp::BitAnd,
            expr,
            index_ident("process_flag", literal_uint(idx as u64))?,
        );
    }
    Ok(binary(HlsBinaryOp::BitAnd, expr, literal_uint(1)))
}

fn streams_with_output(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    for idx in 1..=config.big_kernels {
        params.push(HlsParameter {
            name: ident(&format!("big_kernel_{}_out_stream", idx))?,
            ty: HlsType::Stream(Box::new(custom("write_burst_pkt_t"))),
            passing: PassingStyle::Reference,
        });
    }
    params.push(HlsParameter {
        name: ident("kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(custom("write_burst_pkt_t"))),
        passing: PassingStyle::Reference,
    });
    Ok(params)
}

fn streams_with_output_group(pipelines: usize) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    for idx in 1..=pipelines {
        params.push(HlsParameter {
            name: ident(&format!("big_kernel_{}_out_stream", idx))?,
            ty: HlsType::Stream(Box::new(custom("write_burst_pkt_t"))),
            passing: PassingStyle::Reference,
        });
    }
    params.push(HlsParameter {
        name: ident("kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(custom("write_burst_pkt_t"))),
        passing: PassingStyle::Reference,
    });
    Ok(params)
}

fn merge_flag_expr(config: &HlsKernelConfig) -> Result<HlsExpr, HlsTemplateError> {
    let mut expr = index_ident("process_flag", literal_uint(0))?;
    for idx in 1..config.big_kernels {
        expr = binary(
            HlsBinaryOp::BitAnd,
            expr,
            index_ident("process_flag", literal_uint(idx as u64))?,
        );
    }
    Ok(binary(HlsBinaryOp::BitAnd, expr, literal_uint(1)))
}

fn range_high(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Add,
        shift_idx(idx)?,
        binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            literal_int(1),
        ),
    ))
}

fn range_low(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    shift_idx(idx)
}

fn shift_idx(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Mul,
        HlsExpr::Identifier(ident(idx)?),
        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
    ))
}
