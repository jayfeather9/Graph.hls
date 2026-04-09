use crate::domain::hls::{
    HlsBinaryOp, HlsCompilationUnit, HlsExpr, HlsFunction, HlsIdentifier, HlsInclude, HlsParameter,
    HlsPragma, HlsStatement, HlsStruct, HlsType, HlsUnaryOp, HlsVarDecl, LoopIncrement, LoopLabel,
    PassingStyle,
};

use super::utils::{
    HlsForLoopBuilder, assignment, binary, custom, ident, int_decl, literal_bool, literal_int,
    literal_uint, member_expr, method_call,
};
use super::{HlsKernelConfig, HlsTemplateError};

/// Structured description of `hbm_writer.cpp`.
pub fn hbm_writer_unit(config: &HlsKernelConfig) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: vec![little_ppb_struct()],
        functions: vec![
            little_response_packer()?,
            little_node_prop_loader()?,
            big_node_prop_loader()?,
            write_out_func()?,
            hbm_writer_top(config)?,
        ],
    })
}

/// `hbm_writer.cpp` variant for multi-group topologies.
///
/// Takes **per-group** partition counts (matching the baseline multi-merger host):
/// - Little groups: `little_group_1_num_partitions .. little_group_N_num_partitions`
/// - Big groups: `big_group_0_num_partitions .. big_group_(M-1)_num_partitions`
pub fn hbm_writer_multi_group_unit(
    config: &HlsKernelConfig,
    little_pipeline_group_ids: &[usize],
    big_pipeline_group_ids: &[usize],
    little_group_count: usize,
    big_group_count: usize,
) -> Result<HlsCompilationUnit, HlsTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![HlsInclude::new("shared_kernel_params.h", false)?],
        defines: Vec::new(),
        globals: vec![little_ppb_struct()],
        functions: vec![
            little_response_packer()?,
            little_node_prop_loader_streaming_fn()?,
            big_node_prop_loader()?,
            write_out_func()?,
            hbm_writer_top_multi_group(
                config,
                little_pipeline_group_ids,
                big_pipeline_group_ids,
                little_group_count,
                big_group_count,
            )?,
        ],
    })
}

fn little_ppb_struct() -> HlsStatement {
    HlsStatement::Struct(HlsStruct {
        name: ident("little_ppb_resp_t").expect("valid identifier"),
        fields: vec![
            field("data", custom("bus_word_t")),
            field("dest", HlsType::UInt32),
            field("last", HlsType::Bool),
        ],
        attributes: Vec::new(),
    })
}

fn field(name: &str, ty: HlsType) -> crate::domain::hls::HlsField {
    crate::domain::hls::HlsField {
        name: ident(name).expect("valid identifier"),
        ty,
    }
}

fn little_response_packer() -> Result<HlsFunction, HlsTemplateError> {
    let i_param = HlsParameter {
        name: ident("i")?,
        ty: HlsType::Int32,
        passing: PassingStyle::Value,
    };
    let out_stream = stream_param("prop_loader_out", "little_ppb_resp_t")?;
    let response_stream = stream_param("ppb_response_stm", "ppb_response_pkt_t")?;
    let num_partitions = scalar_param("num_partitions", HlsType::UInt32)?;

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));

    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("left_partitions")?,
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Identifier(num_partitions.name.clone())),
    }));

    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("LOOP_PACK_RESPONSES")?,
        condition: literal_bool(true),
        body: little_response_body(response_stream.name.clone(), out_stream.name.clone())?,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("little_response_packer")?,
        return_type: HlsType::Void,
        params: vec![i_param, out_stream, response_stream, num_partitions],
        body,
    })
}

fn little_response_body(
    response_stream: HlsIdentifier,
    loader_stream: HlsIdentifier,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("prop_data")?,
        ty: custom("little_ppb_resp_t"),
        init: Some(method_call(
            HlsExpr::Identifier(loader_stream),
            "read",
            Vec::new(),
        )?),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("ppb_response")?,
        ty: custom("ppb_response_pkt_t"),
        init: None,
    }));
    for (field, target) in [("data", "data"), ("dest", "dest"), ("last", "last")] {
        body.push(assignment(
            member_expr(HlsExpr::Identifier(ident("ppb_response")?), target)?,
            member_expr(HlsExpr::Identifier(ident("prop_data")?), field)?,
        ));
    }
    body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(response_stream.clone()),
        "write",
        vec![HlsExpr::Identifier(ident("ppb_response")?)],
    )?));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: member_expr(HlsExpr::Identifier(ident("prop_data")?), "last")?,
        then_body: vec![assignment(
            HlsExpr::Identifier(ident("left_partitions")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("left_partitions")?),
                literal_uint(1),
            ),
        )],
        else_body: Vec::new(),
    }));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("left_partitions")?),
            literal_uint(0),
        ),
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));
    Ok(body)
}

fn little_node_prop_loader() -> Result<HlsFunction, HlsTemplateError> {
    let mut params = vec![
        HlsParameter {
            name: ident("i")?,
            ty: HlsType::Int32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: ident("node_distances_ddr")?,
            ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        scalar_param("num_partitions", HlsType::UInt32)?,
    ];
    params.push(stream_param("ppb_req_stream", "ppb_request_pkt_t")?);
    params.push(stream_param("ppb_resp_stream", "little_ppb_resp_t")?);

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("left_partitions")?,
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Identifier(ident("num_partitions")?)),
    }));
    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("littleKernelReadMemory")?,
        condition: literal_bool(true),
        body: little_loader_body()?,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("little_node_prop_loader")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn little_node_prop_loader_streaming_fn() -> Result<HlsFunction, HlsTemplateError> {
    let mut params = vec![
        HlsParameter {
            name: ident("i")?,
            ty: HlsType::Int32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: ident("node_distances_ddr")?,
            ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        scalar_param("num_partitions", HlsType::UInt32)?,
    ];
    params.push(stream_param("ppb_req_stream", "ppb_request_pkt_t")?);
    params.push(stream_param("ppb_resp_stream", "little_ppb_resp_t")?);

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("left_partitions")?,
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Identifier(ident("num_partitions")?)),
    }));
    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("littleKernelReadMemory")?,
        condition: literal_bool(true),
        body: little_loader_body()?,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("little_node_prop_loader_streaming")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn little_loader_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE")?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_ppb_request_pkg")?,
        ty: custom("ppb_request_pkt_t"),
        init: None,
    }));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: method_call(
            HlsExpr::Identifier(ident("ppb_req_stream")?),
            "read_nb",
            vec![HlsExpr::Identifier(ident("one_ppb_request_pkg")?)],
        )?,
        then_body: little_loader_then_body()?,
        else_body: Vec::new(),
    }));
    Ok(body)
}

fn little_loader_then_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    let req_round = ident("request_round")?;
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: req_round.clone(),
        ty: HlsType::UInt32,
        init: Some(member_expr(
            HlsExpr::Identifier(ident("one_ppb_request_pkg")?),
            "data",
        )?),
    }));
    let end_flag_expr = member_expr(HlsExpr::Identifier(ident("one_ppb_request_pkg")?), "last")?;
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("base_addr")?,
        ty: HlsType::UInt32,
        init: Some(binary(
            HlsBinaryOp::Shr,
            binary(
                HlsBinaryOp::Shl,
                HlsExpr::Identifier(req_round),
                HlsExpr::Identifier(ident("LOG_SRC_BUFFER_SIZE")?),
            ),
            HlsExpr::Identifier(ident("LOG_DIST_PER_WORD")?),
        )),
    }));

    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: end_flag_expr.clone(),
        then_body: little_loader_end_body()?,
        else_body: little_loader_stream_body()?,
    }));
    Ok(body)
}

fn little_loader_end_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![
        HlsStatement::Declaration(HlsVarDecl {
            name: ident("one_ppb_response_pkg")?,
            ty: custom("little_ppb_resp_t"),
            init: None,
        }),
        assignment(
            member_expr(HlsExpr::Identifier(ident("one_ppb_response_pkg")?), "last")?,
            literal_bool(true),
        ),
        HlsStatement::Expr(method_call(
            HlsExpr::Identifier(ident("ppb_resp_stream")?),
            "write",
            vec![HlsExpr::Identifier(ident("one_ppb_response_pkg")?)],
        )?),
        assignment(
            HlsExpr::Identifier(ident("left_partitions")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("left_partitions")?),
                literal_uint(1),
            ),
        ),
        HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
            condition: binary(
                HlsBinaryOp::Eq,
                HlsExpr::Identifier(ident("left_partitions")?),
                literal_uint(0),
            ),
            then_body: vec![HlsStatement::Break],
            else_body: Vec::new(),
        }),
    ])
}

fn little_loader_stream_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(
        HlsForLoopBuilder::new("little_stream_loop")?
            .init(int_decl("i", literal_int(0))?)
            .condition(binary(
                HlsBinaryOp::Lt,
                HlsExpr::Identifier(ident("i")?),
                HlsExpr::Identifier(ident("SRC_BUFFER_WORDS")?),
            ))
            .increment(LoopIncrement::Unary(
                HlsUnaryOp::PreIncrement,
                HlsExpr::Identifier(ident("i")?),
            ))
            .body(vec![
                HlsStatement::Declaration(HlsVarDecl {
                    name: ident("addr")?,
                    ty: HlsType::UInt32,
                    init: Some(binary(
                        HlsBinaryOp::Add,
                        HlsExpr::Identifier(ident("base_addr")?),
                        HlsExpr::Identifier(ident("i")?),
                    )),
                }),
                HlsStatement::Declaration(HlsVarDecl {
                    name: ident("one_ppb_response_pkg")?,
                    ty: custom("little_ppb_resp_t"),
                    init: None,
                }),
                assignment(
                    member_expr(HlsExpr::Identifier(ident("one_ppb_response_pkg")?), "data")?,
                    HlsExpr::Index {
                        target: Box::new(HlsExpr::Identifier(ident("node_distances_ddr")?)),
                        index: Box::new(HlsExpr::Identifier(ident("addr")?)),
                    },
                ),
                assignment(
                    member_expr(HlsExpr::Identifier(ident("one_ppb_response_pkg")?), "dest")?,
                    HlsExpr::Identifier(ident("addr")?),
                ),
                assignment(
                    member_expr(HlsExpr::Identifier(ident("one_ppb_response_pkg")?), "last")?,
                    literal_bool(false),
                ),
                HlsStatement::Expr(method_call(
                    HlsExpr::Identifier(ident("ppb_resp_stream")?),
                    "write",
                    vec![HlsExpr::Identifier(ident("one_ppb_response_pkg")?)],
                )?),
            ])
            .build(),
    );
    Ok(body)
}

fn big_node_prop_loader() -> Result<HlsFunction, HlsTemplateError> {
    let params = vec![
        HlsParameter {
            name: ident("i")?,
            ty: HlsType::Int32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: ident("node_distances_ddr")?,
            ty: HlsType::ConstPointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        scalar_param("num_partitions", HlsType::UInt32)?,
        stream_param("cacheline_req_stream", "cacheline_request_pkt_t")?,
        stream_param("cacheline_resp_stream", "cacheline_response_pkt_t")?,
    ];

    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new(
        "HLS function_instantiate variable = i",
    )?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("cache_req")?,
        ty: custom("cacheline_request_pkt_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("cache_resp")?,
        ty: custom("cacheline_response_pkt_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("last_cache_idx")?,
        ty: custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        init: Some(literal_int(-1)),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("last_cacheline")?,
        ty: custom("bus_word_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("left_partitions")?,
        ty: HlsType::UInt32,
        init: Some(HlsExpr::Identifier(ident("num_partitions")?)),
    }));
    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("LOOP_BIG_KRL_READ_MEMORY")?,
        condition: literal_bool(true),
        body: big_loader_loop_body()?,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("big_node_prop_loader")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn big_loader_loop_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("process_flag")?,
        ty: HlsType::Bool,
        init: Some(method_call(
            HlsExpr::Identifier(ident("cacheline_req_stream")?),
            "read_nb",
            vec![HlsExpr::Identifier(ident("cache_req")?)],
        )?),
    }));

    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("process_flag")?),
        then_body: big_loader_then_body()?,
        else_body: Vec::new(),
    }));

    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("left_partitions")?),
            literal_uint(0),
        ),
        then_body: vec![HlsStatement::Break],
        else_body: Vec::new(),
    }));
    Ok(body)
}

fn big_loader_then_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("idx")?,
        ty: custom("ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>"),
        init: Some(member_expr(
            HlsExpr::Identifier(ident("cache_req")?),
            "data",
        )?),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("target_pe")?,
        ty: custom("ap_uint<8>"),
        init: Some(member_expr(
            HlsExpr::Identifier(ident("cache_req")?),
            "dest",
        )?),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("end_flag")?,
        ty: HlsType::Bool,
        init: Some(member_expr(
            HlsExpr::Identifier(ident("cache_req")?),
            "last",
        )?),
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("dst_pe")?,
        ty: custom("ap_uint<8>"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("out_data")?,
        ty: custom("bus_word_t"),
        init: None,
    }));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("out_end_flag")?,
        ty: HlsType::Bool,
        init: None,
    }));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: HlsExpr::Identifier(ident("end_flag")?),
        then_body: big_loader_end_branch()?,
        else_body: big_loader_data_branch()?,
    }));
    body.push(assignment(
        HlsExpr::Identifier(ident("out_end_flag")?),
        HlsExpr::Identifier(ident("end_flag")?),
    ));
    body.push(assignment(
        HlsExpr::Identifier(ident("dst_pe")?),
        HlsExpr::Identifier(ident("target_pe")?),
    ));
    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("cache_resp")?), "data")?,
        HlsExpr::Identifier(ident("out_data")?),
    ));
    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("cache_resp")?), "dest")?,
        HlsExpr::Identifier(ident("dst_pe")?),
    ));
    body.push(assignment(
        member_expr(HlsExpr::Identifier(ident("cache_resp")?), "last")?,
        HlsExpr::Identifier(ident("out_end_flag")?),
    ));
    body.push(HlsStatement::Expr(method_call(
        HlsExpr::Identifier(ident("cacheline_resp_stream")?),
        "write",
        vec![HlsExpr::Identifier(ident("cache_resp")?)],
    )?));
    Ok(body)
}

fn big_loader_end_branch() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![
        assignment(HlsExpr::Identifier(ident("out_data")?), literal_uint(0)),
        assignment(
            HlsExpr::Identifier(ident("last_cache_idx")?),
            literal_int(-1),
        ),
        assignment(
            HlsExpr::Identifier(ident("left_partitions")?),
            binary(
                HlsBinaryOp::Sub,
                HlsExpr::Identifier(ident("left_partitions")?),
                literal_uint(1),
            ),
        ),
    ])
}

fn big_loader_data_branch() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: binary(
            HlsBinaryOp::Eq,
            HlsExpr::Identifier(ident("idx")?),
            HlsExpr::Identifier(ident("last_cache_idx")?),
        ),
        then_body: vec![assignment(
            HlsExpr::Identifier(ident("out_data")?),
            HlsExpr::Identifier(ident("last_cacheline")?),
        )],
        else_body: vec![
            assignment(
                HlsExpr::Identifier(ident("out_data")?),
                HlsExpr::Index {
                    target: Box::new(HlsExpr::Identifier(ident("node_distances_ddr")?)),
                    index: Box::new(HlsExpr::Identifier(ident("idx")?)),
                },
            ),
            assignment(
                HlsExpr::Identifier(ident("last_cache_idx")?),
                HlsExpr::Identifier(ident("idx")?),
            ),
            assignment(
                HlsExpr::Identifier(ident("last_cacheline")?),
                HlsExpr::Identifier(ident("out_data")?),
            ),
        ],
    })])
}

fn write_out_func() -> Result<HlsFunction, HlsTemplateError> {
    let params = vec![
        HlsParameter {
            name: ident("output")?,
            ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        },
        stream_param("write_burst_stream", "write_burst_w_dst_pkt_t")?,
    ];

    let mut body = Vec::new();
    body.push(HlsStatement::WhileLoop(crate::domain::hls::HlsWhileLoop {
        label: LoopLabel::new("LOOP_WRITE_OUT")?,
        condition: literal_bool(true),
        body: write_out_loop_body()?,
    }));

    Ok(HlsFunction {
        linkage: None,
        name: ident("write_out")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn write_out_loop_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut body = Vec::new();
    body.push(HlsStatement::Pragma(HlsPragma::new("HLS PIPELINE II = 1")?));
    body.push(HlsStatement::Declaration(HlsVarDecl {
        name: ident("one_write_burst")?,
        ty: custom("write_burst_w_dst_pkt_t"),
        init: None,
    }));
    body.push(HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
        condition: method_call(
            HlsExpr::Identifier(ident("write_burst_stream")?),
            "read_nb",
            vec![HlsExpr::Identifier(ident("one_write_burst")?)],
        )?,
        then_body: write_out_then_body()?,
        else_body: Vec::new(),
    }));
    Ok(body)
}

fn write_out_then_body() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    Ok(vec![
        HlsStatement::IfElse(crate::domain::hls::HlsIfElse {
            condition: member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "last")?,
            then_body: vec![HlsStatement::Break],
            else_body: Vec::new(),
        }),
        assignment(
            HlsExpr::Index {
                target: Box::new(HlsExpr::Identifier(ident("output")?)),
                index: Box::new(member_expr(
                    HlsExpr::Identifier(ident("one_write_burst")?),
                    "dest",
                )?),
            },
            member_expr(HlsExpr::Identifier(ident("one_write_burst")?), "data")?,
        ),
    ])
}

fn hbm_writer_top(config: &HlsKernelConfig) -> Result<HlsFunction, HlsTemplateError> {
    let params = hbm_writer_params(config)?;
    let mut body = Vec::new();

    for pragma in hbm_interface_pragmas(config) {
        body.push(HlsStatement::Pragma(HlsPragma::new(&pragma)?));
    }

    for idx in 1..=config.little_kernels {
        let stream_name = format!("little_prop_loader_out_{idx}");
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident(&stream_name)?,
            ty: HlsType::Stream(Box::new(custom("little_ppb_resp_t"))),
            init: None,
        }));
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS STREAM variable = {stream_name} depth = 16"
        ))?));
    }

    for idx in 1..=config.little_kernels {
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("little_node_prop_loader")?,
            args: vec![
                literal_int(idx as i64 - 1),
                HlsExpr::Identifier(ident(&format!("src_prop_{idx}"))?),
                HlsExpr::Identifier(ident("num_partitions_little")?),
                HlsExpr::Identifier(ident(&format!("ppb_req_stream_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("little_prop_loader_out_{idx}"))?),
            ],
        }));
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("little_response_packer")?,
            args: vec![
                literal_int(idx as i64 - 1),
                HlsExpr::Identifier(ident(&format!("little_prop_loader_out_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("ppb_resp_stream_{idx}"))?),
                HlsExpr::Identifier(ident("num_partitions_little")?),
            ],
        }));
    }

    let big_src_base = config.little_kernels + 1;
    for idx in 1..=config.big_kernels {
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("big_node_prop_loader")?,
            args: vec![
                literal_int((idx - 1) as i64),
                HlsExpr::Identifier(ident(&format!("src_prop_{}", big_src_base + idx - 1))?),
                HlsExpr::Identifier(ident("num_partitions_big")?),
                HlsExpr::Identifier(ident(&format!("cacheline_req_stream_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("cacheline_resp_stream_{idx}"))?),
            ],
        }));
    }

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("write_out")?,
        args: vec![
            HlsExpr::Identifier(ident("output")?),
            HlsExpr::Identifier(ident("write_burst_stream")?),
        ],
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("hbm_writer")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn hbm_writer_top_multi_group(
    config: &HlsKernelConfig,
    little_pipeline_group_ids: &[usize],
    big_pipeline_group_ids: &[usize],
    little_group_count: usize,
    big_group_count: usize,
) -> Result<HlsFunction, HlsTemplateError> {
    if little_pipeline_group_ids.len() != config.little_kernels {
        return Err(HlsTemplateError::InvalidConfig(format!(
            "little pipeline group ids length {} must equal little_kernels {}",
            little_pipeline_group_ids.len(),
            config.little_kernels
        )));
    }
    if big_pipeline_group_ids.len() != config.big_kernels {
        return Err(HlsTemplateError::InvalidConfig(format!(
            "big pipeline group ids length {} must equal big_kernels {}",
            big_pipeline_group_ids.len(),
            config.big_kernels
        )));
    }
    if little_group_count == 0 && big_group_count == 0 {
        return Err(HlsTemplateError::InvalidConfig(
            "at least one kernel group count must be >= 1".to_string(),
        ));
    }
    if little_pipeline_group_ids
        .iter()
        .any(|&gid| gid >= little_group_count)
    {
        return Err(HlsTemplateError::InvalidConfig(
            "little pipeline group id out of range".to_string(),
        ));
    }
    if big_pipeline_group_ids
        .iter()
        .any(|&gid| gid >= big_group_count)
    {
        return Err(HlsTemplateError::InvalidConfig(
            "big pipeline group id out of range".to_string(),
        ));
    }

    let params = hbm_writer_params_multi_group(config, little_group_count, big_group_count)?;
    let mut body = Vec::new();

    for pragma in hbm_interface_pragmas_multi_group(config, little_group_count, big_group_count) {
        body.push(HlsStatement::Pragma(HlsPragma::new(&pragma)?));
    }

    for idx in 1..=config.little_kernels {
        let stream_name = format!("little_prop_loader_out_{idx}");
        body.push(HlsStatement::Declaration(HlsVarDecl {
            name: ident(&stream_name)?,
            ty: HlsType::Stream(Box::new(custom("little_ppb_resp_t"))),
            init: None,
        }));
        body.push(HlsStatement::Pragma(HlsPragma::new(&format!(
            "HLS STREAM variable = {stream_name} depth = 16"
        ))?));
    }

    for idx in 1..=config.little_kernels {
        let gid = little_pipeline_group_ids[idx - 1];
        let partitions_arg = format!("little_group_{}_num_partitions", gid + 1);
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("little_node_prop_loader_streaming")?,
            args: vec![
                literal_int(idx as i64 - 1),
                HlsExpr::Identifier(ident(&format!("src_prop_{idx}"))?),
                HlsExpr::Identifier(ident(&partitions_arg)?),
                HlsExpr::Identifier(ident(&format!("ppb_req_stream_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("little_prop_loader_out_{idx}"))?),
            ],
        }));
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("little_response_packer")?,
            args: vec![
                literal_int(idx as i64 - 1),
                HlsExpr::Identifier(ident(&format!("little_prop_loader_out_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("ppb_resp_stream_{idx}"))?),
                HlsExpr::Identifier(ident(&partitions_arg)?),
            ],
        }));
    }

    let big_src_base = config.little_kernels + 1;
    for idx in 1..=config.big_kernels {
        let gid = big_pipeline_group_ids[idx - 1];
        let partitions_arg = format!("big_group_{gid}_num_partitions");
        body.push(HlsStatement::Expr(HlsExpr::Call {
            function: ident("big_node_prop_loader")?,
            args: vec![
                literal_int((idx - 1) as i64),
                HlsExpr::Identifier(ident(&format!("src_prop_{}", big_src_base + idx - 1))?),
                HlsExpr::Identifier(ident(&partitions_arg)?),
                HlsExpr::Identifier(ident(&format!("cacheline_req_stream_{idx}"))?),
                HlsExpr::Identifier(ident(&format!("cacheline_resp_stream_{idx}"))?),
            ],
        }));
    }

    body.push(HlsStatement::Expr(HlsExpr::Call {
        function: ident("write_out")?,
        args: vec![
            HlsExpr::Identifier(ident("output")?),
            HlsExpr::Identifier(ident("write_burst_stream")?),
        ],
    }));

    Ok(HlsFunction {
        linkage: Some(r#"extern "C""#),
        name: ident("hbm_writer")?,
        return_type: HlsType::Void,
        params,
        body,
    })
}

fn hbm_writer_params(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    let total_kernels = config.little_kernels + config.big_kernels;
    for idx in 1..=total_kernels {
        params.push(HlsParameter {
            name: ident(&format!("src_prop_{idx}"))?,
            ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        });
    }
    params.push(HlsParameter {
        name: ident("output")?,
        ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
        passing: PassingStyle::Value,
    });
    params.push(scalar_param("num_partitions_little", HlsType::UInt32)?);
    params.push(scalar_param("num_partitions_big", HlsType::UInt32)?);
    for idx in 1..=config.little_kernels {
        params.push(stream_param(
            &format!("ppb_req_stream_{idx}"),
            "ppb_request_pkt_t",
        )?);
        params.push(stream_param(
            &format!("ppb_resp_stream_{idx}"),
            "ppb_response_pkt_t",
        )?);
    }
    for idx in 1..=config.big_kernels {
        params.push(stream_param(
            &format!("cacheline_req_stream_{idx}"),
            "cacheline_request_pkt_t",
        )?);
        params.push(stream_param(
            &format!("cacheline_resp_stream_{idx}"),
            "cacheline_response_pkt_t",
        )?);
    }
    params.push(stream_param(
        "write_burst_stream",
        "write_burst_w_dst_pkt_t",
    )?);
    Ok(params)
}

fn hbm_writer_params_multi_group(
    config: &HlsKernelConfig,
    little_group_count: usize,
    big_group_count: usize,
) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    let total_kernels = config.little_kernels + config.big_kernels;
    for idx in 1..=total_kernels {
        params.push(HlsParameter {
            name: ident(&format!("src_prop_{idx}"))?,
            ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
            passing: PassingStyle::Value,
        });
    }
    params.push(HlsParameter {
        name: ident("output")?,
        ty: HlsType::Pointer(Box::new(custom("bus_word_t"))),
        passing: PassingStyle::Value,
    });
    for gid in 0..little_group_count {
        params.push(scalar_param(
            &format!("little_group_{}_num_partitions", gid + 1),
            HlsType::UInt32,
        )?);
    }
    for gid in 0..big_group_count {
        params.push(scalar_param(
            &format!("big_group_{gid}_num_partitions"),
            HlsType::UInt32,
        )?);
    }
    for idx in 1..=config.little_kernels {
        params.push(stream_param(
            &format!("ppb_req_stream_{idx}"),
            "ppb_request_pkt_t",
        )?);
        params.push(stream_param(
            &format!("ppb_resp_stream_{idx}"),
            "ppb_response_pkt_t",
        )?);
    }
    for idx in 1..=config.big_kernels {
        params.push(stream_param(
            &format!("cacheline_req_stream_{idx}"),
            "cacheline_request_pkt_t",
        )?);
        params.push(stream_param(
            &format!("cacheline_resp_stream_{idx}"),
            "cacheline_response_pkt_t",
        )?);
    }
    params.push(stream_param(
        "write_burst_stream",
        "write_burst_w_dst_pkt_t",
    )?);
    Ok(params)
}

fn hbm_interface_pragmas(config: &HlsKernelConfig) -> Vec<String> {
    let mut pragmas = Vec::new();
    let total_kernels = config.little_kernels + config.big_kernels;
    let output_bundle_idx = total_kernels;
    for idx in 1..=total_kernels {
        pragmas.push(format!(
            "HLS INTERFACE m_axi port = src_prop_{idx} offset = slave bundle = gmem{}",
            idx - 1
        ));
    }
    pragmas.push(format!(
        "HLS INTERFACE m_axi port = output offset = slave bundle = gmem{output_bundle_idx}"
    ));
    for idx in 1..=total_kernels {
        pragmas.push(format!(
            "HLS INTERFACE s_axilite port = src_prop_{idx} bundle = control"
        ));
    }
    pragmas.push("HLS INTERFACE s_axilite port = output bundle = control".to_string());
    pragmas
        .push("HLS INTERFACE s_axilite port = num_partitions_little bundle = control".to_string());
    pragmas.push("HLS INTERFACE s_axilite port = num_partitions_big bundle = control".to_string());
    pragmas.push("HLS INTERFACE s_axilite port = return bundle = control".to_string());
    pragmas.push("HLS DATAFLOW".to_string());
    pragmas
}

fn hbm_interface_pragmas_multi_group(
    config: &HlsKernelConfig,
    little_group_count: usize,
    big_group_count: usize,
) -> Vec<String> {
    let mut pragmas = Vec::new();
    let total_kernels = config.little_kernels + config.big_kernels;
    let output_bundle_idx = total_kernels;
    for idx in 1..=total_kernels {
        pragmas.push(format!(
            "HLS INTERFACE m_axi port = src_prop_{idx} offset = slave bundle = gmem{}",
            idx - 1
        ));
    }
    pragmas.push(format!(
        "HLS INTERFACE m_axi port = output offset = slave bundle = gmem{output_bundle_idx}"
    ));
    for idx in 1..=total_kernels {
        pragmas.push(format!(
            "HLS INTERFACE s_axilite port = src_prop_{idx} bundle = control"
        ));
    }
    pragmas.push("HLS INTERFACE s_axilite port = output bundle = control".to_string());
    for gid in 0..little_group_count {
        pragmas.push(format!(
            "HLS INTERFACE s_axilite port = little_group_{}_num_partitions bundle = control",
            gid + 1
        ));
    }
    for gid in 0..big_group_count {
        pragmas.push(format!(
            "HLS INTERFACE s_axilite port = big_group_{gid}_num_partitions bundle = control"
        ));
    }
    pragmas.push("HLS INTERFACE s_axilite port = return bundle = control".to_string());
    pragmas.push("HLS DATAFLOW".to_string());
    pragmas
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
