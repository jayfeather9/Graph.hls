use crate::domain::hls::{
    HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsFunction, HlsInclude, HlsParameter, HlsPragma,
    HlsStatement, HlsType, HlsUnaryOp, HlsVarDecl, HlsWhileLoop, LoopIncrement, LoopLabel,
    PassingStyle,
};
use crate::domain::hls_ops::{KernelOpBundle, OperatorOperand, ReducerIdentity, ReducerKind};

use super::utils::{
    HlsForLoopBuilder, assignment, binary, custom, expr_uses_operand, ident, index_ident, int_decl,
    literal_bool, literal_int, literal_uint, member_expr, method_call, range_method, raw,
    reducer_combine_expr, reducer_combine_expr_zero_sentinel, reducer_identity_expr,
};
use super::{HlsKernelConfig, HlsTemplateError};

fn use_zero_sentinel_little_merge(ops: &KernelOpBundle, zero_sentinel: bool) -> bool {
    if !zero_sentinel {
        return false;
    }

    match ops.gather.kind {
        ReducerKind::Sum | ReducerKind::Or | ReducerKind::MaskedMinIgnoreZero { .. } => true,
        ReducerKind::Min => {
            ops.gather.identity == ReducerIdentity::PositiveInfinity
                && expr_uses_operand(&ops.apply.expr, &OperatorOperand::OldProp)
                && !expr_uses_operand(&ops.scatter.expr, &OperatorOperand::ScatterEdgeWeight)
        }
        ReducerKind::Max => false,
    }
}

/// Structured description of `little_merger.cpp`.
pub fn little_merger_unit(
    ops: &KernelOpBundle,
    config: &HlsKernelConfig,
    zero_sentinel: bool,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![
            merge_little_kernels(ops, config, zero_sentinel)?,
            little_merger_top(config)?,
        ],
    })
}

/// Structured description of a group-local `little_merger_<gid>.cpp` (pipeline count is per group).
pub fn little_merger_group_unit(
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
            merge_little_kernels_group(ops, pipelines, group_id, zero_sentinel)?,
            little_merger_top_group(pipelines, group_id)?,
        ],
    })
}

fn merge_little_kernels(
    ops: &KernelOpBundle,
    config: &HlsKernelConfig,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(raw("little_out_pkt_t tmp_prop_pkt[LITTLE_MERGER_LENGTH];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = tmp_prop_pkt dim = 0 complete",
    )?));
    body.push(raw("bool process_flag[LITTLE_MERGER_LENGTH];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = process_flag dim = 0 complete",
    )?));
    body.push(
        HlsForLoopBuilder::new("init_process_flag")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("LITTLE_MERGER_LENGTH")?),
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
        name: ident("one_write_burst")?,
        ty: custom("bus_word_t"),
        init: Some(literal_uint(0)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("inner_idx")?,
        ty: HlsType::UInt32,
        init: Some(literal_uint(0)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("identity_pod")?,
        ty: custom("ap_fixed_pod_t"),
        init: Some(reducer_identity_expr(ops.gather.identity)?),
    }));

    let mut while_body = Vec::new();
    while_body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS pipeline style = flp",
    )?));

    for idx in 0..config.little_kernels {
        let stream = format!("little_kernel_{}_out_stream", idx + 1);
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
        init: Some(little_merge_flag(config)?),
    }));
    while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("merge_flag")?),
        then_body: {
            let mut then_body = Vec::new();
            then_body.extend(little_merge_inner_body(ops, zero_sentinel)?);
            then_body.push(
                HlsForLoopBuilder::new("reset_process_flag")?
                    .init(int_decl("i", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("i")?),
                        HlsExpr::Identifier(ident("LITTLE_MERGER_LENGTH")?),
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
        label: LoopLabel::new("merge_tmp_prop_little_krnls")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("merge_little_kernels")?,
        return_type: HlsType::Void,
        params: little_merger_params(config)?,
        body,
    })
}

fn merge_little_kernels_group(
    ops: &KernelOpBundle,
    pipelines: usize,
    group_id: usize,
    zero_sentinel: bool,
) -> Result<HlsFunction, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(raw(&format!("little_out_pkt_t tmp_prop_pkt[{pipelines}];")));
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
        name: ident("one_write_burst")?,
        ty: custom("bus_word_t"),
        init: Some(literal_uint(0)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("inner_idx")?,
        ty: HlsType::UInt32,
        init: Some(literal_uint(0)),
    }));
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
        let stream = format!("little_kernel_{}_out_stream", idx + 1);
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
        init: Some(little_merge_flag_group(pipelines)?),
    }));
    while_body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("merge_flag")?),
        then_body: {
            let mut then_body = Vec::new();
            then_body.extend(little_merge_inner_body_group(
                zero_sentinel,
                ops,
                pipelines,
            )?);
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
        label: LoopLabel::new("merge_tmp_prop_little_krnls")?,
        condition: literal_bool(true),
        body: while_body,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident(&format!("merge_little_kernels_{group_id}"))?,
        return_type: HlsType::Void,
        params: little_merger_params_group(pipelines)?,
        body,
    })
}

fn little_merge_inner_body_group(
    zero_sentinel: bool,
    ops: &KernelOpBundle,
    pipelines: usize,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let use_zero_sentinel = use_zero_sentinel_little_merge(ops, zero_sentinel);
    let mut body = Vec::new();
    body.push(raw("ap_fixed_pod_t uram_vals[DISTANCES_PER_REDUCE_WORD];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = uram_vals dim = 0 complete",
    )?));
    body.push(
        HlsForLoopBuilder::new("init_uram_vals")?
            .init(int_decl("s", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("s")?),
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("s")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                    HlsExpr::Identifier(ident("identity_pod")?),
                ),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("merge_reduction")?
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
                HlsForLoopBuilder::new("merge_slots")?
                    .init(int_decl("s", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("s")?),
                        HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("s")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                            {
                                let current =
                                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?;
                                let update = range_method(
                                    member_expr(
                                        index_ident(
                                            "tmp_prop_pkt",
                                            HlsExpr::Identifier(ident("i")?),
                                        )?,
                                        "data",
                                    )?,
                                    binary(
                                        HlsBinaryOp::Add,
                                        binary(
                                            HlsBinaryOp::Sub,
                                            binary(
                                                HlsBinaryOp::Mul,
                                                HlsExpr::Identifier(ident("s")?),
                                                HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                                            ),
                                            literal_int(1),
                                        ),
                                        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                                    ),
                                    binary(
                                        HlsBinaryOp::Mul,
                                        HlsExpr::Identifier(ident("s")?),
                                        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                                    ),
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

    body.push(raw("reduce_word_t merged_write_burst;"));
    body.push(
        HlsForLoopBuilder::new("pack_slots")?
            .init(int_decl("s", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("s")?),
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("s")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    range_method(
                        HlsExpr::Identifier(ident("merged_write_burst")?),
                        binary(
                            HlsBinaryOp::Add,
                            binary(
                                HlsBinaryOp::Sub,
                                binary(
                                    HlsBinaryOp::Mul,
                                    HlsExpr::Identifier(ident("s")?),
                                    HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                                ),
                                literal_int(1),
                            ),
                            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                        ),
                        binary(
                            HlsBinaryOp::Mul,
                            HlsExpr::Identifier(ident("s")?),
                            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
                        ),
                    )?,
                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                ),
            ])
            .build(),
    );

    body.push(assignment(
        range_method(
            HlsExpr::Identifier(ident("one_write_burst")?),
            binary(
                HlsBinaryOp::Add,
                binary(
                    HlsBinaryOp::Sub,
                    binary(
                        HlsBinaryOp::Mul,
                        HlsExpr::Identifier(ident("inner_idx")?),
                        literal_int(64),
                    ),
                    literal_int(1),
                ),
                literal_int(64),
            ),
            binary(
                HlsBinaryOp::Mul,
                HlsExpr::Identifier(ident("inner_idx")?),
                literal_int(64),
            ),
        )?,
        HlsExpr::Identifier(ident("merged_write_burst")?),
    ));

    body.push(assignment(
        HlsExpr::Identifier(ident("inner_idx")?),
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("inner_idx")?),
            literal_uint(1),
        ),
    ));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("inner_idx")?),
            HlsExpr::Identifier(ident("REDUCE_WORDS_PER_BUS")?),
        ),
        then_body: vec![
            HlsStatement::Declaration(HlsVarDecl {
                name: ident("out_pkt")?,
                ty: custom("write_burst_pkt_t"),
                init: None,
            }),
            assignment(
                member_expr(HlsExpr::Identifier(ident("out_pkt")?), "data")?,
                HlsExpr::Identifier(ident("one_write_burst")?),
            ),
            assignment(
                member_expr(HlsExpr::Identifier(ident("out_pkt")?), "last")?,
                literal_bool(false),
            ),
            HlsStatement::Expr(method_call(
                HlsExpr::Identifier(ident("kernel_out_stream")?),
                "write",
                vec![HlsExpr::Identifier(ident("out_pkt")?)],
            )?),
            assignment(HlsExpr::Identifier(ident("inner_idx")?), literal_uint(0)),
            assignment(
                HlsExpr::Identifier(ident("one_write_burst")?),
                literal_uint(0),
            ),
        ],
        else_body: Vec::new(),
    }));

    Ok(body)
}

fn little_merge_inner_body(
    ops: &KernelOpBundle,
    zero_sentinel: bool,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let use_zero_sentinel = use_zero_sentinel_little_merge(ops, zero_sentinel);
    let mut body = Vec::new();
    body.push(raw("ap_fixed_pod_t uram_vals[DISTANCES_PER_REDUCE_WORD];"));
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS ARRAY_PARTITION variable = uram_vals dim = 0 complete",
    )?));
    body.push(
        HlsForLoopBuilder::new("init_uram_vals")?
            .init(int_decl("s", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("s")?),
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("s")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                    HlsExpr::Identifier(ident("identity_pod")?),
                ),
            ])
            .build(),
    );

    body.push(
        HlsForLoopBuilder::new("merge_reduction")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("LITTLE_MERGER_LENGTH")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                HlsForLoopBuilder::new("merge_slots")?
                    .init(int_decl("s", literal_int(0))?)
                    .condition(binary(
                        HlsBinaryOp::Lt,
                        HlsExpr::Identifier(ident("s")?),
                        HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
                    ))
                    .increment(LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(ident("s")?),
                    ))
                    .body(vec![
                        HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                        assignment(
                            index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                            {
                                let current =
                                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?;
                                let update = range_method(
                                    member_expr(
                                        index_ident(
                                            "tmp_prop_pkt",
                                            HlsExpr::Identifier(ident("i")?),
                                        )?,
                                        "data",
                                    )?,
                                    slot_range_high("s")?,
                                    slot_range_low("s")?,
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
        HlsForLoopBuilder::new("pack_reduce_word")?
            .init(int_decl("s", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("s")?),
                HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("s")?),
            ))
            .body(vec![
                HlsStatement::Pragma(HlsPragma::new("HLS UNROLL")?),
                assignment(
                    range_method(
                        HlsExpr::Identifier(ident("one_write_burst")?),
                        bus_slot_range_high("s")?,
                        bus_slot_range_low("s")?,
                    )?,
                    index_ident("uram_vals", HlsExpr::Identifier(ident("s")?))?,
                ),
            ])
            .build(),
    );
    body.push(assignment(
        HlsExpr::Identifier(ident("inner_idx")?),
        binary(
            HlsBinaryOp::Add,
            HlsExpr::Identifier(ident("inner_idx")?),
            literal_uint(1),
        ),
    ));

    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("inner_idx")?),
            HlsExpr::Identifier(ident("REDUCE_WORDS_PER_BUS")?),
        ),
        then_body: flush_packet_body()?,
        else_body: Vec::new(),
    }));

    Ok(body)
}

fn little_merger_top(config: &HlsKernelConfig) -> Result<HlsFunction, HlsTemplateError> {
    let params = little_merger_params(config)?;
    let args = params
        .iter()
        .map(|param| HlsExpr::Identifier(param.name.clone()))
        .collect::<Vec<_>>();

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS interface ap_ctrl_none port = return",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("merge_little_kernels")?,
        args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("little_merger")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn little_merger_top_group(
    pipelines: usize,
    group_id: usize,
) -> Result<HlsFunction, HlsTemplateError> {
    let params = little_merger_params_group(pipelines)?;
    let args = params
        .iter()
        .map(|param| HlsExpr::Identifier(param.name.clone()))
        .collect::<Vec<_>>();

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS interface ap_ctrl_none port = return",
    )?));
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS DATAFLOW")?));
    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident(&format!("merge_little_kernels_{group_id}"))?,
        args,
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident(&format!("little_merger_{group_id}"))?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn little_merger_params(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    params.extend(little_stream_params(config)?);
    Ok(params)
}

fn little_merger_params_group(pipelines: usize) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    params.extend(little_stream_params_group(pipelines)?);
    Ok(params)
}

fn little_merge_flag_group(pipelines: usize) -> Result<HlsExpr, HlsTemplateError> {
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

fn little_stream_params(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    for idx in 1..=config.little_kernels {
        params.push(HlsParameter {
            name: ident(&format!("little_kernel_{}_out_stream", idx))?,
            ty: HlsType::Stream(Box::new(custom("little_out_pkt_t"))),
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

fn little_stream_params_group(pipelines: usize) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    for idx in 1..=pipelines {
        params.push(HlsParameter {
            name: ident(&format!("little_kernel_{}_out_stream", idx))?,
            ty: HlsType::Stream(Box::new(custom("little_out_pkt_t"))),
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

fn little_merge_flag(config: &HlsKernelConfig) -> Result<HlsExpr, HlsTemplateError> {
    let mut expr = index_ident("process_flag", literal_uint(0))?;
    for idx in 1..config.little_kernels {
        expr = binary(
            HlsBinaryOp::BitAnd,
            expr,
            index_ident("process_flag", literal_uint(idx as u64))?,
        );
    }
    Ok(binary(HlsBinaryOp::BitAnd, expr, literal_uint(1)))
}

fn flush_packet_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![
        HlsStatement::Declaration(HlsVarDecl {
            name: ident("out_pkt")?,
            ty: custom("write_burst_pkt_t"),
            init: None,
        }),
        assignment(
            member_expr(HlsExpr::Identifier(ident("out_pkt")?), "data")?,
            HlsExpr::Identifier(ident("one_write_burst")?),
        ),
        assignment(
            member_expr(HlsExpr::Identifier(ident("out_pkt")?), "last")?,
            literal_bool(false),
        ),
        HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("kernel_out_stream")?),
            "write",
            vec![HlsExpr::Identifier(ident("out_pkt")?)],
        )?),
        assignment(HlsExpr::Identifier(ident("inner_idx")?), literal_uint(0)),
        assignment(
            HlsExpr::Identifier(ident("one_write_burst")?),
            literal_uint(0),
        ),
    ])
}

fn bus_slot_range_high(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Add,
        bus_slot_shift(idx)?,
        binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            literal_int(1),
        ),
    ))
}

fn bus_slot_range_low(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    bus_slot_shift(idx)
}

fn bus_slot_shift(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    let slot_idx = binary(
        HlsBinaryOp::Add,
        binary(
            HlsBinaryOp::Mul,
            HlsExpr::Identifier(ident("inner_idx")?),
            HlsExpr::Identifier(ident("DISTANCES_PER_REDUCE_WORD")?),
        ),
        HlsExpr::Identifier(ident(idx)?),
    );
    Ok(binary(
        HlsBinaryOp::Mul,
        slot_idx,
        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
    ))
}

fn slot_range_high(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Add,
        slot_shift(idx)?,
        binary(
            HlsBinaryOp::Sub,
            HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
            literal_int(1),
        ),
    ))
}

fn slot_range_low(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    slot_shift(idx)
}

fn slot_shift(idx: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(binary(
        HlsBinaryOp::Mul,
        HlsExpr::Identifier(ident(idx)?),
        HlsExpr::Identifier(ident("DISTANCE_BITWIDTH")?),
    ))
}
