//! HLS source templates described via `domain::hls` primitives.

mod apply_kernel;
mod big_merger;
mod config;
mod graphyflow_big;
mod graphyflow_headers;
mod graphyflow_little;
mod hbm_writer;
mod host;
mod little_merger;
mod shared_kernel_params;
pub(crate) mod utils;

use thiserror::Error;

use crate::domain::hls::{HlsCodegenError, HlsCompilationUnit};

pub use apply_kernel::{apply_kernel_ddr_unit, apply_kernel_multi_merger_unit, apply_kernel_unit};
pub use big_merger::{big_merger_group_unit, big_merger_unit};
pub use config::{HlsEdgeConfig, HlsKernelConfig, HlsNodeConfig};
pub use graphyflow_big::graphyflow_big_unit;
pub use graphyflow_headers::{render_graphyflow_big_header, render_graphyflow_little_header};
pub use graphyflow_little::{graphyflow_little_unit, little_kernel_uses_zero_reduce};
pub use hbm_writer::{hbm_writer_multi_group_unit, hbm_writer_unit};
pub use little_merger::{little_merger_group_unit, little_merger_unit};
pub use shared_kernel_params::{
    shared_kernel_params_ddr_unit, shared_kernel_params_multi_merger_unit,
    shared_kernel_params_unit,
};

/// Logical description of an HLS source file.
pub struct HlsSourceFile {
    pub logical_path: &'static str,
    pub unit: HlsCompilationUnit,
}

/// Errors that can occur when assembling template units.
#[derive(Debug, Error)]
pub enum HlsTemplateError {
    #[error("{0}")]
    Codegen(#[from] HlsCodegenError),
    #[error("unsupported operator expression in template: {0}")]
    UnsupportedOperator(&'static str),
    #[error("invalid template config: {0}")]
    InvalidConfig(String),
}

impl HlsSourceFile {
    /// Builds a source file ready for emission to disk.
    pub fn new(logical_path: &'static str, unit: HlsCompilationUnit) -> Self {
        Self { logical_path, unit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::hls_ops::{
        ApplyOp, GatherOp, KernelOpBundle, OperatorBinary, OperatorExpr, OperatorOperand,
        ReducerIdentity, ReducerKind, ScatterOp,
    };
    use crate::domain::hls_template::HlsNodeConfig;
    use crate::engine::{
        gas_lower::lower_to_gas, gas_to_hls_ops::extract_kernel_ops, ir_builder::LoweredProgram,
    };

    #[test]
    fn renders_shared_kernel_header_contains_prototypes() -> Result<(), HlsTemplateError> {
        let node = HlsNodeConfig {
            node_prop_bits: 32,
            node_prop_int_bits: 16,
            node_prop_signed: true,
            dist_per_word: 16,
            log_dist_per_word: 4,
            distances_per_reduce_word: 2,
        };
        let code =
            shared_kernel_params_unit(&HlsKernelConfig::default(), &node, false, false)?.to_code();
        assert!(
            code.contains("apply_kernel("),
            "expected apply_kernel prototype"
        );
        assert!(
            code.contains("#define PE_NUM 8"),
            "expected macro definition to render"
        );
        Ok(())
    }

    #[test]
    fn renders_apply_kernel_functions() -> Result<(), HlsTemplateError> {
        let ops = KernelOpBundle::sssp_default();
        let code = apply_kernel_unit(&ops)?.to_code();
        assert!(
            code.contains("void merge_big_little_writes("),
            "merge function missing"
        );
        assert!(code.contains("apply_kernel("), "top-level kernel present");
        Ok(())
    }

    #[test]
    fn renders_big_merger_units() -> Result<(), HlsTemplateError> {
        let code = big_merger_unit(
            &KernelOpBundle::sssp_default(),
            &HlsKernelConfig::default(),
            false,
        )?
        .to_code();
        assert!(
            code.contains("void merge_big_kernels("),
            "expected helper function"
        );
        assert!(code.contains("big_merger("), "expected top kernel");
        Ok(())
    }

    #[test]
    fn renders_little_merger_units() -> Result<(), HlsTemplateError> {
        let code = little_merger_unit(
            &KernelOpBundle::sssp_default(),
            &HlsKernelConfig::default(),
            false,
        )?
        .to_code();
        assert!(
            code.contains("void merge_little_kernels("),
            "expected helper function"
        );
        assert!(code.contains("little_merger("), "expected top kernel");
        Ok(())
    }

    #[test]
    fn renders_hbm_writer_unit() -> Result<(), HlsTemplateError> {
        let code = hbm_writer_unit(&HlsKernelConfig::default())?.to_code();
        assert!(
            code.contains("struct little_ppb_resp_t"),
            "expected helper struct"
        );
        assert!(code.contains("hbm_writer("), "expected top kernel");
        Ok(())
    }

    #[test]
    fn renders_graphyflow_big_unit() -> Result<(), HlsTemplateError> {
        let ops = KernelOpBundle::sssp_default();
        let edge = edge_config_for_app("sssp");
        let code = graphyflow_big_unit(&ops, &edge)?.to_code();
        assert!(
            code.contains("node_id_burst_t"),
            "missing struct definitions"
        );
        assert!(
            code.contains("graphyflow_big("),
            "top-level kernel signature absent"
        );
        Ok(())
    }

    #[test]
    fn renders_graphyflow_little_unit() -> Result<(), HlsTemplateError> {
        let ops = KernelOpBundle::sssp_default();
        let edge = edge_config_for_app("sssp");
        let code = graphyflow_little_unit(&ops, &edge)?.to_code();
        assert!(code.contains("ppb_request_t"), "missing request struct");
        assert!(
            code.contains("graphyflow_little("),
            "top-level kernel signature absent"
        );
        Ok(())
    }

    #[test]
    fn apply_kernel_matches_golden() -> Result<(), HlsTemplateError> {
        let rendered = apply_kernel_unit(&KernelOpBundle::sssp_default())?.to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/apply_kernel.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "apply_kernel.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn big_merger_matches_golden() -> Result<(), HlsTemplateError> {
        let rendered = big_merger_unit(
            &KernelOpBundle::sssp_default(),
            &HlsKernelConfig::default(),
            false,
        )?
        .to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/big_merger.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "big_merger.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn little_merger_matches_golden() -> Result<(), HlsTemplateError> {
        let rendered = little_merger_unit(
            &KernelOpBundle::sssp_default(),
            &HlsKernelConfig::default(),
            false,
        )?
        .to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/little_merger.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "little_merger.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn hbm_writer_matches_golden() -> Result<(), HlsTemplateError> {
        let rendered = hbm_writer_unit(&HlsKernelConfig::default())?.to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/hbm_writer.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "hbm_writer.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn graphyflow_big_matches_golden() -> Result<(), HlsTemplateError> {
        let edge = edge_config_for_app("sssp");
        let rendered = graphyflow_big_unit(&KernelOpBundle::sssp_default(), &edge)?.to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/graphyflow_big.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "graphyflow_big.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn graphyflow_little_matches_golden() -> Result<(), HlsTemplateError> {
        let edge = edge_config_for_app("sssp");
        let rendered = graphyflow_little_unit(&KernelOpBundle::sssp_default(), &edge)?.to_code();
        let golden = include_str!("../../hls_assets/scripts/kernel/graphyflow_little.cpp");
        assert_eq!(
            normalize(&rendered),
            normalize(golden),
            "graphyflow_little.cpp rendering diverged from golden"
        );
        Ok(())
    }

    #[test]
    fn renders_multiple_operator_variants() -> Result<(), HlsTemplateError> {
        let variants = vec![
            KernelOpBundle::sssp_default(),
            KernelOpBundle {
                scatter: ScatterOp {
                    expr: OperatorExpr::Binary {
                        op: OperatorBinary::Add,
                        left: Box::new(OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)),
                        right: Box::new(OperatorExpr::Operand(OperatorOperand::ConstInt(2))),
                    },
                },
                gather: GatherOp {
                    kind: ReducerKind::Max,
                    identity: ReducerIdentity::NegativeInfinity,
                },
                apply: ApplyOp {
                    expr: OperatorExpr::Operand(OperatorOperand::GatherValue),
                },
            },
            KernelOpBundle {
                scatter: ScatterOp {
                    expr: OperatorExpr::Operand(OperatorOperand::ScatterDstId),
                },
                gather: GatherOp {
                    kind: ReducerKind::Sum,
                    identity: ReducerIdentity::Zero,
                },
                apply: ApplyOp {
                    expr: OperatorExpr::Binary {
                        op: OperatorBinary::Sub,
                        left: Box::new(OperatorExpr::Operand(OperatorOperand::GatherValue)),
                        right: Box::new(OperatorExpr::Operand(OperatorOperand::OldProp)),
                    },
                },
            },
        ];

        for ops in variants {
            let edge = edge_config_for_app("sssp");
            graphyflow_big_unit(&ops, &edge)?.to_code();
            graphyflow_little_unit(&ops, &edge)?.to_code();
            apply_kernel_unit(&ops)?.to_code();
        }

        Ok(())
    }

    #[test]
    fn connected_components_bundle_matches_golden() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("connected_components");
        let edge = edge_config_for_app("connected_components");
        let big = graphyflow_big_unit(&ops, &edge)?.to_code();
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        let apply = apply_kernel_unit(&ops)?.to_code();
        assert_eq!(
            normalize(&big),
            normalize(include_str!(
                "../../hls_assets/scripts_cc/kernel/graphyflow_big.cpp"
            ))
        );
        assert_eq!(
            normalize(&little),
            normalize(include_str!(
                "../../hls_assets/scripts_cc/kernel/graphyflow_little.cpp"
            ))
        );
        assert_eq!(
            normalize(&apply),
            normalize(include_str!(
                "../../hls_assets/scripts_cc/kernel/apply_kernel.cpp"
            ))
        );
        Ok(())
    }

    #[test]
    fn pagerank_bundle_matches_golden() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("pagerank");
        let edge = edge_config_for_app("pagerank");
        let big = graphyflow_big_unit(&ops, &edge)?.to_code();
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        let apply = apply_kernel_unit(&ops)?.to_code();
        assert_eq!(
            normalize(&big),
            normalize(include_str!(
                "../../hls_assets/scripts_pr/kernel/graphyflow_big.cpp"
            ))
        );
        assert_eq!(
            normalize(&little),
            normalize(include_str!(
                "../../hls_assets/scripts_pr/kernel/graphyflow_little.cpp"
            ))
        );
        assert_eq!(
            normalize(&apply),
            normalize(include_str!(
                "../../hls_assets/scripts_pr/kernel/apply_kernel.cpp"
            ))
        );
        Ok(())
    }

    #[test]
    fn pagerank_little_zero_identity_uses_emulation_only_init() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("pagerank");
        let edge = edge_config_for_app("pagerank");
        let big = graphyflow_big_unit(&ops, &edge)?.to_code();
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        let little_norm = normalize(&little);

        assert!(
            big.contains("INIT_REDUCE_MEM: for"),
            "expected big-kernel reducer init loop"
        );
        assert!(
            little.contains("INIT_PROP_MEM: for"),
            "expected little-kernel reducer init loop"
        );
        assert!(
            !big.contains("#ifdef EMULATION"),
            "big-kernel reducer init must run on hardware too"
        );
        assert!(little.contains("#ifdef EMULATION"));
        assert!(little.contains("#ifndef EMULATION"));
        assert!(little_norm.contains("prop_mem[pe][i]=0u;"));
        assert!(little_norm.contains("prop_mem[(pe+4)][i]=0u;"));

        Ok(())
    }

    #[test]
    fn unweighted_sssp_little_zero_identity_uses_emulation_only_init()
    -> Result<(), HlsTemplateError> {
        let ops = ops_for_path("apps/topology_variants/sssp_topo_l11_b3.dsl");
        let edge = edge_config_for_path("apps/topology_variants/sssp_topo_l11_b3.dsl");
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        let little_norm = normalize(&little);

        assert!(little.contains("INIT_PROP_MEM: for"));
        assert!(little.contains("INIT_PROP_MEM_PE: for"));
        assert!(little.contains("#ifdef EMULATION"));
        assert!(little.contains("#ifndef EMULATION"));
        assert!(little_norm.contains("prop_mem[pe][i]=0u;"));
        assert!(little_norm.contains("prop_mem[(pe+4)][i]=0u;"));
        assert!(!little.contains("prop_mem[pe][i] = identity_word;"));
        assert!(!little.contains("prop_mem[(pe + 4)][i] = identity_word;"));

        Ok(())
    }

    #[test]
    fn rendered_hbm_headers_limit_max_dst_only_for_hw_emu() {
        let edge = edge_config_for_app("sssp");
        let node = HlsNodeConfig {
            node_prop_bits: 32,
            node_prop_int_bits: 32,
            node_prop_signed: false,
            dist_per_word: 16,
            log_dist_per_word: 4,
            distances_per_reduce_word: 2,
        };

        let big = render_graphyflow_big_header(&edge, &node, 524_288);
        let little = render_graphyflow_little_header(&edge, &node, 65_536);

        assert!(big.contains("#ifdef GRAPHYFLOW_HW_EMU_LIMIT_MAX_DST"));
        assert!(little.contains("#ifdef GRAPHYFLOW_HW_EMU_LIMIT_MAX_DST"));
        assert!(!big.contains("#ifdef EMULATION"));
        assert!(!little.contains("#ifdef EMULATION"));
        assert!(big.contains("#define MAX_DST_BIG 524288"));
        assert!(little.contains("#define MAX_DST_LITTLE 65536"));
        assert!(big.contains("#define INVALID_LOCAL_ID_BIG (local_id_t(1) << LOCAL_ID_MSB)"));
        assert!(little.contains("#define INVALID_LOCAL_ID_LITTLE (local_id_t(1) << LOCAL_ID_MSB)"));
    }

    #[test]
    fn rendered_hbm_kernels_filter_invalid_updates_by_top_bit() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("sssp");
        let edge = edge_config_for_app("sssp");
        let big = graphyflow_big_unit(&ops, &edge)?.to_code();
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();

        assert!(big.contains("node_id.range(LOCAL_ID_MSB, LOCAL_ID_MSB) == 0"));
        assert!(little.contains("node_id.range(LOCAL_ID_MSB, LOCAL_ID_MSB) == 0"));
        assert!(!big.contains("node_id < MAX_NUM"));
        assert!(!little.contains("node_id < MAX_NUM"));
        assert!(!big.contains("node_id < INVALID_LOCAL_ID_BIG"));
        assert!(!little.contains("node_id < INVALID_LOCAL_ID_LITTLE"));
        assert!(big.contains("INVALID_LOCAL_ID_BIG"));
        assert!(little.contains("INVALID_LOCAL_ID_LITTLE"));

        Ok(())
    }

    #[test]
    fn rendered_hbm_little_zero_identity_hw_flushes_without_init() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("sssp");
        let edge = edge_config_for_app("sssp");
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        let little_norm = normalize(&little);

        assert!(little.contains("INIT_PROP_MEM: for"));
        assert!(little.contains("INIT_PROP_MEM_PE: for"));
        assert!(little.contains("#ifdef EMULATION"));
        assert!(little.contains("#ifndef EMULATION"));
        assert!(little_norm.contains("prop_mem[pe][i]=0u;"));
        assert!(little_norm.contains("prop_mem[(pe+4)][i]=0u;"));
        assert!(!little.contains("prop_mem[pe][i] = identity_word;"));
        assert!(!little.contains("prop_mem[(pe + 4)][i] = identity_word;"));

        Ok(())
    }

    #[test]
    fn ddr_sssp_codegen_uses_compact_edge_prop_unpack() -> Result<(), HlsTemplateError> {
        let ops = ops_for_app("sssp");
        let edge = edge_config_for_path("apps/topology_variants/sssp_ddr_4b4l_codegen.dsl");
        let big = graphyflow_big_unit(&ops, &edge)?.to_code();
        let little = graphyflow_little_unit(&ops, &edge)?.to_code();
        assert!(edge.compact_edge_payload);
        assert_eq!(edge.payload_bits(), 64);
        assert_eq!(edge.edge_prop_payload_lsb(), 22);
        assert!(big.contains("EDGE_PROP_PAYLOAD_MSB"));
        assert!(big.contains("EDGE_PROP_PAYLOAD_LSB"));
        assert!(little.contains("EDGE_PROP_PAYLOAD_MSB"));
        assert!(little.contains("EDGE_PROP_PAYLOAD_LSB"));
        Ok(())
    }

    fn normalize(input: &str) -> String {
        input.split_whitespace().collect()
    }

    fn load_source(relative_path: &str) -> String {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let source_path = std::path::Path::new(&manifest).join(relative_path);
        std::fs::read_to_string(&source_path).expect("read app")
    }

    fn ops_for_app(app: &str) -> KernelOpBundle {
        ops_for_path(&format!("apps/{app}.dsl"))
    }

    fn ops_for_path(relative_path: &str) -> KernelOpBundle {
        let source = load_source(relative_path);
        let lowered = LoweredProgram::parse_and_lower(&source).expect("lower");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("gas");
        extract_kernel_ops(&gas).expect("ops")
    }

    fn edge_config_for_app(app: &str) -> HlsEdgeConfig {
        edge_config_for_path(&format!("apps/{app}.dsl"))
    }

    fn edge_config_for_path(relative_path: &str) -> HlsEdgeConfig {
        let source = load_source(relative_path);
        let lowered = LoweredProgram::parse_and_lower(&source).expect("lower");
        let mut edge_prop_widths = Vec::new();
        let mut edge_prop_bits = 0u32;
        let mut edge_weight_bits = 0u32;
        let mut edge_weight_lsb = 0u32;
        let mut edge_weight_shift = 0i32;

        if let Some(edge_def) = &lowered.ast.schema.edge {
            for prop in &edge_def.properties {
                let width = typeexpr_bitwidth(&prop.ty);
                if prop.name.as_str() == "weight" {
                    edge_weight_bits = width;
                    edge_weight_lsb = edge_prop_bits;
                    edge_weight_shift = edge_weight_shift_for_type(&prop.ty);
                }
                edge_prop_widths.push(width);
                edge_prop_bits += width;
            }
        }

        let compact_edge_payload = lowered
            .ast
            .hls
            .as_ref()
            .map(|h| h.memory == crate::domain::ast::MemoryBackend::Ddr)
            .unwrap_or(false);
        let payload_bits = if compact_edge_payload {
            64
        } else {
            edge_prop_bits + 64
        };
        let edges_per_word = 512 / payload_bits;
        let big_pe = if edges_per_word > 4 {
            8
        } else if edges_per_word > 2 {
            4
        } else {
            edges_per_word.max(1)
        };
        let big_log_pe = match big_pe {
            1 => 0,
            2 => 1,
            4 => 2,
            8 => 3,
            _ => panic!("unsupported big PE count {big_pe}"),
        };
        let little_pe = edges_per_word.max(1);
        let local_id_bits = lowered
            .ast
            .hls
            .as_ref()
            .map(|h| h.local_id_bits)
            .unwrap_or(32);
        let zero_sentinel = lowered
            .ast
            .hls
            .as_ref()
            .map(|h| h.zero_sentinel)
            .unwrap_or(true);

        HlsEdgeConfig {
            edge_prop_bits,
            edge_prop_widths,
            edge_weight_bits,
            edge_weight_lsb,
            edge_weight_shift,
            edges_per_word,
            big_pe,
            big_log_pe,
            little_pe,
            local_id_bits,
            compact_edge_payload,
            zero_sentinel,
            allow_scatter_inf_overflow_to_zero: lowered
                .ast
                .hls
                .as_ref()
                .map(|h| h.memory == crate::domain::ast::MemoryBackend::Ddr)
                .unwrap_or(false),
        }
    }

    fn typeexpr_bitwidth(ty: &crate::domain::ast::TypeExpr) -> u32 {
        use crate::domain::ast::TypeExpr;
        match ty {
            TypeExpr::Int { width } => *width,
            TypeExpr::Float => 32,
            TypeExpr::Fixed { width, .. } => *width,
            TypeExpr::Bool => 1,
            TypeExpr::Tuple(items) => items.iter().map(typeexpr_bitwidth).sum(),
            TypeExpr::Vector { element, len } => typeexpr_bitwidth(element) * *len,
            TypeExpr::Matrix {
                element,
                rows,
                cols,
            } => typeexpr_bitwidth(element) * *rows * *cols,
            TypeExpr::Array(_) | TypeExpr::Set(_) => {
                panic!("edge properties must be fixed-size types")
            }
        }
    }

    fn edge_weight_shift_for_type(ty: &crate::domain::ast::TypeExpr) -> i32 {
        use crate::domain::ast::TypeExpr;
        const DISTANCE_BITWIDTH: u32 = 32;
        const DISTANCE_INTEGER_PART: u32 = 16;
        let dist_frac_bits = DISTANCE_BITWIDTH - DISTANCE_INTEGER_PART;
        match ty {
            TypeExpr::Int { .. } | TypeExpr::Bool => dist_frac_bits as i32,
            TypeExpr::Fixed { width, int_width } => {
                let frac_bits = width.saturating_sub(*int_width);
                dist_frac_bits as i32 - frac_bits as i32
            }
            TypeExpr::Float => {
                panic!("edge weight float type is not supported for HLS scatter");
            }
            TypeExpr::Tuple(_)
            | TypeExpr::Vector { .. }
            | TypeExpr::Matrix { .. }
            | TypeExpr::Array(_)
            | TypeExpr::Set(_) => {
                panic!("edge weight must be a scalar int/bool/fixed type");
            }
        }
    }
}
