use crate::domain::hls::{
    HlsDefine, HlsExpr, HlsField, HlsFunctionPrototype, HlsIdentifier, HlsInclude, HlsLiteral,
    HlsParameter, HlsStatement, HlsStruct, HlsType, PassingStyle,
};

use super::{HlsKernelConfig, HlsNodeConfig, HlsTemplateError};

/// Builds the logical representation of `shared_kernel_params.h`.
pub fn shared_kernel_params_unit(
    config: &HlsKernelConfig,
    node: &HlsNodeConfig,
    apply_needs_aux: bool,
    is_pr: bool,
) -> Result<crate::domain::hls::HlsCompilationUnit, HlsTemplateError> {
    shared_kernel_params_impl(
        config,
        node,
        function_prototypes(config, apply_needs_aux, is_pr)?,
    )
}

/// Builds a topology-aware `shared_kernel_params.h` for multi-merger designs.
pub fn shared_kernel_params_multi_merger_unit(
    config: &HlsKernelConfig,
    node: &HlsNodeConfig,
    little_group_pipelines: &[usize],
    big_group_pipelines: &[usize],
    apply_needs_aux: bool,
    is_pr: bool,
) -> Result<crate::domain::hls::HlsCompilationUnit, HlsTemplateError> {
    shared_kernel_params_impl(
        config,
        node,
        function_prototypes_multi_merger(
            config,
            little_group_pipelines,
            big_group_pipelines,
            apply_needs_aux,
            is_pr,
        )?,
    )
}

/// Builds `shared_kernel_params.h` for DDR memory backend.
///
/// Differences from HBM: no `hbm_writer` prototype, `apply_kernel` has `output`
/// m_axi port, and `little_prop_loader` / `big_prop_loader` prototypes are added.
pub fn shared_kernel_params_ddr_unit(
    config: &HlsKernelConfig,
    node: &HlsNodeConfig,
    is_pr: bool,
) -> Result<crate::domain::hls::HlsCompilationUnit, HlsTemplateError> {
    shared_kernel_params_impl(config, node, function_prototypes_ddr(config, is_pr)?)
}

fn shared_kernel_params_impl(
    config: &HlsKernelConfig,
    node: &HlsNodeConfig,
    prototypes: Vec<HlsStatement>,
) -> Result<crate::domain::hls::HlsCompilationUnit, HlsTemplateError> {
    let mut globals = Vec::new();
    globals.push(HlsStatement::Raw(
        "#ifndef __SHARED_KERNEL_PARAMS_H__".to_string(),
    ));
    globals.push(HlsStatement::Raw(
        "#define __SHARED_KERNEL_PARAMS_H__".to_string(),
    ));
    globals.push(HlsStatement::Raw(String::new()));

    for header in [
        "ap_axi_sdata.h",
        "ap_fixed.h",
        "ap_int.h",
        "hls_stream.h",
        "stdint.h",
        "stdio.h",
        "string.h",
    ] {
        globals.push(HlsStatement::Include(HlsInclude::new(header, true)?));
    }

    globals.push(HlsStatement::Raw(String::new()));
    for (name, value) in [
        ("PE_NUM", "8"),
        ("DBL_PE_NUM", "16"),
        ("LOG_PE_NUM", "3"),
        ("L", "4"),
        ("SRC_BUFFER_SIZE", "4096"),
        ("LOG_SRC_BUFFER_SIZE", "12"),
        ("NODE_ID_BITWIDTH", "32"),
        ("DISTANCE_BITWIDTH", &node.node_prop_bits.to_string()),
        (
            "DISTANCE_INTEGER_PART",
            &node.node_prop_int_bits.to_string(),
        ),
        ("WEIGHT_BITWIDTH", "DISTANCE_BITWIDTH"),
        ("WEIGHT_INTEGER_PART", "DISTANCE_INTEGER_PART"),
        ("OUT_END_MARKER_BITWIDTH", "4"),
        ("DIST_PER_WORD", &node.dist_per_word.to_string()),
        ("LOG_DIST_PER_WORD", &node.log_dist_per_word.to_string()),
        ("AXI_BUS_WIDTH", "512"),
        ("SRC_BUFFER_WORDS", "(SRC_BUFFER_SIZE / DIST_PER_WORD)"),
        (
            "LOG_SRC_BUFFER_WORDS",
            "(LOG_SRC_BUFFER_SIZE - LOG_DIST_PER_WORD)",
        ),
    ] {
        globals.push(HlsStatement::Define(HlsDefine::new(
            HlsIdentifier::new(name)?,
            Some(value.to_string()),
        )?));
    }
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("BIG_MERGER_LENGTH")?,
        Some(config.big_kernels.to_string()),
    )?));
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("LITTLE_MERGER_LENGTH")?,
        Some(config.little_kernels.to_string()),
    )?));
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("REDUCE_MEM_WIDTH")?,
        Some("64".to_string()),
    )?));
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("REDUCE_WORDS_PER_BUS")?,
        Some("(AXI_BUS_WIDTH / REDUCE_MEM_WIDTH)".to_string()),
    )?));
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("DISTANCES_PER_REDUCE_WORD")?,
        Some(node.distances_per_reduce_word.to_string()),
    )?));
    globals.push(HlsStatement::Define(HlsDefine::new(
        HlsIdentifier::new("LOG_DISTANCES_PER_REDUCE_WORD")?,
        Some("((LOG_DIST_PER_WORD > 3) ? (LOG_DIST_PER_WORD - 3) : 0)".to_string()),
    )?));

    // Note: DISTANCE_SIGNED is NOT emitted. ap_fixed_pod_t is always ap_uint
    // (unsigned), matching the SG reference. The fixed-point type (distance_t)
    // handles signedness via ap_fixed.

    globals.push(HlsStatement::Raw(String::new()));
    globals.push(HlsStatement::Comment("Constants".to_string()));
    let infinity_pod_val: u64 = if node.node_prop_signed {
        if node.node_prop_bits == 0 {
            0
        } else {
            (1u128
                .wrapping_shl((node.node_prop_bits - 1) as u32)
                .wrapping_sub(1)) as u64
        }
    } else if node.node_prop_bits == 0 {
        0
    } else if node.node_prop_bits >= 64 {
        u64::MAX
    } else {
        ((1u128 << node.node_prop_bits) - 1) as u64
    };
    let neg_infinity_pod_val: u64 = if node.node_prop_signed && node.node_prop_bits > 0 {
        (1u128 << (node.node_prop_bits - 1)) as u64
    } else {
        0
    };

    globals.push(HlsStatement::Raw(String::new()));
    globals.extend(type_aliases(node.node_prop_signed)?);

    globals.push(HlsStatement::Raw(String::new()));
    globals.push(HlsStatement::Comment("Derived constants".to_string()));
    globals.push(HlsStatement::Declaration(crate::domain::hls::HlsVarDecl {
        name: HlsIdentifier::new("INFINITY_POD")?,
        ty: HlsType::Custom("const ap_fixed_pod_t".to_string()),
        init: Some(HlsExpr::Literal(HlsLiteral::UInt(infinity_pod_val))),
    }));
    globals.push(HlsStatement::Declaration(crate::domain::hls::HlsVarDecl {
        name: HlsIdentifier::new("NEG_INFINITY_POD")?,
        ty: HlsType::Custom("const ap_fixed_pod_t".to_string()),
        init: Some(HlsExpr::Literal(HlsLiteral::UInt(neg_infinity_pod_val))),
    }));
    globals.push(HlsStatement::Declaration(crate::domain::hls::HlsVarDecl {
        name: HlsIdentifier::new("INFINITY_DIST_VAL")?,
        ty: HlsType::Custom("const distance_t".to_string()),
        init: Some(HlsExpr::Cast {
            target_type: HlsType::Custom("distance_t".to_string()),
            expr: Box::new(HlsExpr::Identifier(HlsIdentifier::new("INFINITY_POD")?)),
        }),
    }));

    globals.push(HlsStatement::Raw(String::new()));
    globals.push(HlsStatement::Struct(HlsStruct {
        name: HlsIdentifier::new("in_write_burst_w_dst_pkt_t")?,
        fields: vec![
            HlsField {
                name: HlsIdentifier::new("data")?,
                ty: HlsType::Custom("bus_word_t".to_string()),
            },
            HlsField {
                name: HlsIdentifier::new("dest_addr")?,
                ty: HlsType::UInt32,
            },
            HlsField {
                name: HlsIdentifier::new("end_flag")?,
                ty: HlsType::Bool,
            },
        ],
        attributes: vec!["__attribute__((packed))".to_string()],
    }));

    globals.push(HlsStatement::Raw(String::new()));
    globals.push(HlsStatement::Comment("Kernel prototypes".to_string()));
    globals.extend(prototypes);
    globals.push(HlsStatement::Raw(String::new()));
    globals.push(HlsStatement::Raw(
        "#endif // __SHARED_KERNEL_PARAMS_H__".to_string(),
    ));

    Ok(crate::domain::hls::HlsCompilationUnit {
        includes: Vec::new(),
        defines: Vec::new(),
        globals,
        functions: Vec::new(),
    })
}

fn type_aliases(distance_signed: bool) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let distance_alias = if distance_signed {
        "ap_fixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART>"
    } else {
        "ap_ufixed<DISTANCE_BITWIDTH, DISTANCE_INTEGER_PART>"
    };
    // ap_fixed_pod_t is always unsigned — it holds the raw bit pattern of
    // a distance value. The signedness of distance_t does not affect the pod.
    let pod_alias = "ap_uint<DISTANCE_BITWIDTH>";
    let mut statements = Vec::new();
    for (alias, target) in [
        ("bus_word_t", "ap_uint<AXI_BUS_WIDTH>"),
        ("reduce_word_t", "ap_uint<REDUCE_MEM_WIDTH>"),
        ("node_id_t", "ap_uint<NODE_ID_BITWIDTH>"),
        ("edge_id_t", "ap_uint<32>"),
        ("ap_fixed_pod_t", pod_alias),
        ("distance_t", distance_alias),
        ("out_end_marker_t", "ap_uint<OUT_END_MARKER_BITWIDTH>"),
        ("node_dist_pkt_t", "ap_axiu<256, 0, 0, 0>"),
        ("write_burst_pkt_t", "ap_axiu<512, 0, 0, 0>"),
        ("little_out_pkt_t", "ap_axiu<64, 0, 0, 0>"),
        ("write_burst_w_dst_pkt_t", "ap_axiu<512, 0, 0, 32>"),
        ("cacheline_request_pkt_t", "ap_axiu<32, 0, 0, 8>"),
        ("cacheline_response_pkt_t", "ap_axiu<512, 0, 0, 8>"),
        ("ppb_request_pkt_t", "ap_axiu<32, 0, 0, 0>"),
        ("ppb_response_pkt_t", "ap_axiu<512, 0, 0, 32>"),
        ("cacheline_data_pkt_t", "ap_axiu<512, 0, 0, 0>"),
    ] {
        statements.push(HlsStatement::UsingAlias {
            name: HlsIdentifier::new(alias)?,
            ty: HlsType::Custom(target.to_string()),
        });
    }
    Ok(statements)
}

fn function_prototypes_graphyflow() -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut prototypes = Vec::new();
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("graphyflow_big")?,
        params: vec![
            HlsParameter {
                name: HlsIdentifier::new("edge_props")?,
                ty: HlsType::ConstPointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_nodes")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_edges")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("dst_num")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("memory_offset")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("cacheline_req_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "cacheline_request_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("cacheline_resp_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "cacheline_response_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
        ],
    }));

    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("graphyflow_little")?,
        params: vec![
            HlsParameter {
                name: HlsIdentifier::new("edge_props")?,
                ty: HlsType::ConstPointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_nodes")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_edges")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("dst_num")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("memory_offset")?,
                ty: HlsType::Int32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("ppb_req_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_request_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("ppb_resp_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_response_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("little_out_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
        ],
    }));

    Ok(prototypes)
}

fn function_prototypes(
    config: &HlsKernelConfig,
    apply_needs_aux: bool,
    is_pr: bool,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut prototypes = function_prototypes_graphyflow()?;

    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("apply_kernel")?,
        params: {
            let mut params = Vec::new();
            params.push(HlsParameter {
                name: HlsIdentifier::new("node_props")?,
                ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            });
            if apply_needs_aux {
                params.push(HlsParameter {
                    name: HlsIdentifier::new("aux_node_props")?,
                    ty: HlsType::ConstPointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                    passing: PassingStyle::Value,
                });
            }
            for name in [
                "little_kernel_length",
                "big_kernel_length",
                "little_kernel_st_offset",
                "big_kernel_st_offset",
            ] {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(name)?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            if is_pr {
                params.push(HlsParameter {
                    name: HlsIdentifier::new("arg_reg")?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("little_kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params.push(HlsParameter {
                name: HlsIdentifier::new("big_kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "write_burst_w_dst_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("big_merger")?,
        params: {
            let mut params = Vec::new();
            for idx in 1..=config.big_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_kernel_{}_out_stream", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("little_merger")?,
        params: {
            let mut params = Vec::new();
            for idx in 1..=config.little_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_kernel_{}_out_stream", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("little_out_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("hbm_writer")?,
        params: hbm_writer_params(config)?,
    }));

    Ok(prototypes)
}

fn hbm_writer_params(config: &HlsKernelConfig) -> Result<Vec<HlsParameter>, HlsTemplateError> {
    let mut params = Vec::new();
    let total_kernels = config.little_kernels + config.big_kernels;
    for idx in 1..=total_kernels {
        params.push(HlsParameter {
            name: HlsIdentifier::new(&format!("src_prop_{}", idx))?,
            ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
            passing: PassingStyle::Value,
        });
    }
    params.push(HlsParameter {
        name: HlsIdentifier::new("output")?,
        ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
        passing: PassingStyle::Value,
    });
    params.push(HlsParameter {
        name: HlsIdentifier::new("num_partitions_little")?,
        ty: HlsType::UInt32,
        passing: PassingStyle::Value,
    });
    params.push(HlsParameter {
        name: HlsIdentifier::new("num_partitions_big")?,
        ty: HlsType::UInt32,
        passing: PassingStyle::Value,
    });
    for idx in 1..=config.little_kernels {
        params.push(HlsParameter {
            name: HlsIdentifier::new(&format!("ppb_req_stream_{}", idx))?,
            ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_request_pkt_t".to_string()))),
            passing: PassingStyle::Reference,
        });
        params.push(HlsParameter {
            name: HlsIdentifier::new(&format!("ppb_resp_stream_{}", idx))?,
            ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_response_pkt_t".to_string()))),
            passing: PassingStyle::Reference,
        });
    }
    for idx in 1..=config.big_kernels {
        params.push(HlsParameter {
            name: HlsIdentifier::new(&format!("cacheline_req_stream_{}", idx))?,
            ty: HlsType::Stream(Box::new(HlsType::Custom(
                "cacheline_request_pkt_t".to_string(),
            ))),
            passing: PassingStyle::Reference,
        });
        params.push(HlsParameter {
            name: HlsIdentifier::new(&format!("cacheline_resp_stream_{}", idx))?,
            ty: HlsType::Stream(Box::new(HlsType::Custom(
                "cacheline_response_pkt_t".to_string(),
            ))),
            passing: PassingStyle::Reference,
        });
    }
    params.push(HlsParameter {
        name: HlsIdentifier::new("write_burst_stream")?,
        ty: HlsType::Stream(Box::new(HlsType::Custom(
            "write_burst_w_dst_pkt_t".to_string(),
        ))),
        passing: PassingStyle::Reference,
    });
    Ok(params)
}

fn function_prototypes_multi_merger(
    config: &HlsKernelConfig,
    little_group_pipelines: &[usize],
    big_group_pipelines: &[usize],
    apply_needs_aux: bool,
    is_pr: bool,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut prototypes = function_prototypes_graphyflow()?;

    // Add group-specific merger kernels (names match system.cfg nk= lines).
    for (gid, &pipelines) in big_group_pipelines.iter().enumerate() {
        prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
            linkage: Some(r#"extern "C""#),
            return_type: HlsType::Void,
            name: HlsIdentifier::new(&format!("big_merger_{}", gid))?,
            params: {
                let mut params = Vec::new();
                for idx in 1..=pipelines {
                    params.push(HlsParameter {
                        name: HlsIdentifier::new(&format!("big_kernel_{}_out_stream", idx))?,
                        ty: HlsType::Stream(Box::new(HlsType::Custom(
                            "write_burst_pkt_t".to_string(),
                        ))),
                        passing: PassingStyle::Reference,
                    });
                }
                params.push(HlsParameter {
                    name: HlsIdentifier::new("kernel_out_stream")?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
                params
            },
        }));
    }

    for (gid0, &pipelines) in little_group_pipelines.iter().enumerate() {
        let kernel_id = big_group_pipelines.len() + gid0;
        prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
            linkage: Some(r#"extern "C""#),
            return_type: HlsType::Void,
            name: HlsIdentifier::new(&format!("little_merger_{}", kernel_id))?,
            params: {
                let mut params = Vec::new();
                for idx in 1..=pipelines {
                    params.push(HlsParameter {
                        name: HlsIdentifier::new(&format!("little_kernel_{}_out_stream", idx))?,
                        ty: HlsType::Stream(Box::new(HlsType::Custom(
                            "little_out_pkt_t".to_string(),
                        ))),
                        passing: PassingStyle::Reference,
                    });
                }
                params.push(HlsParameter {
                    name: HlsIdentifier::new("kernel_out_stream")?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
                params
            },
        }));
    }

    // Multi-stream apply_kernel prototype.
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("apply_kernel")?,
        params: {
            let mut params = Vec::new();
            params.push(HlsParameter {
                name: HlsIdentifier::new("node_props")?,
                ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            });
            if apply_needs_aux {
                params.push(HlsParameter {
                    name: HlsIdentifier::new("aux_node_props")?,
                    ty: HlsType::ConstPointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                    passing: PassingStyle::Value,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("num_little_mergers")?,
                ty: HlsType::UInt32,
                passing: PassingStyle::Value,
            });
            params.push(HlsParameter {
                name: HlsIdentifier::new("num_big_mergers")?,
                ty: HlsType::UInt32,
                passing: PassingStyle::Value,
            });
            for (gid0, _) in little_group_pipelines.iter().enumerate() {
                let kernel_id = big_group_pipelines.len() + gid0;
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_merger_{}_length", kernel_id))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for gid in 0..big_group_pipelines.len() {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_merger_{}_length", gid))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for (gid0, _) in little_group_pipelines.iter().enumerate() {
                let kernel_id = big_group_pipelines.len() + gid0;
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_merger_{}_st_offset", kernel_id))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for gid in 0..big_group_pipelines.len() {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_merger_{}_st_offset", gid))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            if is_pr {
                params.push(HlsParameter {
                    name: HlsIdentifier::new("arg_reg")?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for (gid0, _) in little_group_pipelines.iter().enumerate() {
                let kernel_id = big_group_pipelines.len() + gid0;
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_merger_{}_out_stream", kernel_id))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            for gid in 0..big_group_pipelines.len() {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_merger_{}_out_stream", gid))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "write_burst_w_dst_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    // Multi-group hbm_writer prototype (per-group partition counts).
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("hbm_writer")?,
        params: {
            let mut params = Vec::new();
            let total_kernels = config.little_kernels + config.big_kernels;
            for idx in 1..=total_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("src_prop_{}", idx))?,
                    ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                    passing: PassingStyle::Value,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("output")?,
                ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            });
            for (gid0, _) in little_group_pipelines.iter().enumerate() {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_group_{}_num_partitions", gid0 + 1))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for gid in 0..big_group_pipelines.len() {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_group_{}_num_partitions", gid))?,
                    ty: HlsType::UInt32,
                    passing: PassingStyle::Value,
                });
            }
            for idx in 1..=config.little_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("ppb_req_stream_{}", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_request_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("ppb_resp_stream_{}", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom(
                        "ppb_response_pkt_t".to_string(),
                    ))),
                    passing: PassingStyle::Reference,
                });
            }
            for idx in 1..=config.big_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("cacheline_req_stream_{}", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom(
                        "cacheline_request_pkt_t".to_string(),
                    ))),
                    passing: PassingStyle::Reference,
                });
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("cacheline_resp_stream_{}", idx))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom(
                        "cacheline_response_pkt_t".to_string(),
                    ))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("write_burst_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "write_burst_w_dst_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    Ok(prototypes)
}

/// DDR prototypes: graphyflow_big/little (shared), apply_kernel with output port,
/// little_prop_loader, big_prop_loader. No hbm_writer.
fn function_prototypes_ddr(
    config: &HlsKernelConfig,
    is_pr: bool,
) -> Result<Vec<HlsStatement>, HlsTemplateError> {
    let mut prototypes = function_prototypes_graphyflow()?;

    // apply_kernel: node_props (gmem0), output (gmem1), scalars, streams
    let mut apply_params = vec![
        HlsParameter {
            name: HlsIdentifier::new("node_props")?,
            ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: HlsIdentifier::new("output")?,
            ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: HlsIdentifier::new("little_kernel_length")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: HlsIdentifier::new("big_kernel_length")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: HlsIdentifier::new("little_kernel_st_offset")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        },
        HlsParameter {
            name: HlsIdentifier::new("big_kernel_st_offset")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        },
    ];
    if is_pr {
        apply_params.push(HlsParameter {
            name: HlsIdentifier::new("arg_reg")?,
            ty: HlsType::UInt32,
            passing: PassingStyle::Value,
        });
    }
    apply_params.push(HlsParameter {
        name: HlsIdentifier::new("little_kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
        passing: PassingStyle::Reference,
    });
    apply_params.push(HlsParameter {
        name: HlsIdentifier::new("big_kernel_out_stream")?,
        ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
        passing: PassingStyle::Reference,
    });
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("apply_kernel")?,
        params: apply_params,
    }));

    // little_prop_loader
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("little_prop_loader")?,
        params: vec![
            HlsParameter {
                name: HlsIdentifier::new("src_prop")?,
                ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_partitions_little")?,
                ty: HlsType::UInt32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("ppb_req_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_request_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("ppb_resp_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("ppb_response_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            },
        ],
    }));

    // big_prop_loader
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("big_prop_loader")?,
        params: vec![
            HlsParameter {
                name: HlsIdentifier::new("src_prop")?,
                ty: HlsType::Pointer(Box::new(HlsType::Custom("bus_word_t".to_string()))),
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("num_partitions_big")?,
                ty: HlsType::UInt32,
                passing: PassingStyle::Value,
            },
            HlsParameter {
                name: HlsIdentifier::new("cacheline_req_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "cacheline_request_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            },
            HlsParameter {
                name: HlsIdentifier::new("cacheline_resp_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom(
                    "cacheline_response_pkt_t".to_string(),
                ))),
                passing: PassingStyle::Reference,
            },
        ],
    }));

    // big_merger
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("big_merger")?,
        params: {
            let mut params = Vec::new();
            for i in 1..=config.big_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("big_kernel_{i}_out_stream"))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    // little_merger
    prototypes.push(HlsStatement::FunctionPrototype(HlsFunctionPrototype {
        linkage: Some(r#"extern "C""#),
        return_type: HlsType::Void,
        name: HlsIdentifier::new("little_merger")?,
        params: {
            let mut params = Vec::new();
            for i in 1..=config.little_kernels {
                params.push(HlsParameter {
                    name: HlsIdentifier::new(&format!("little_kernel_{i}_out_stream"))?,
                    ty: HlsType::Stream(Box::new(HlsType::Custom("little_out_pkt_t".to_string()))),
                    passing: PassingStyle::Reference,
                });
            }
            params.push(HlsParameter {
                name: HlsIdentifier::new("kernel_out_stream")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("write_burst_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            });
            params
        },
    }));

    Ok(prototypes)
}
