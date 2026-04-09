use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::gas_to_hls_ops::{GasToHlsOpsError, extract_kernel_ops};
use super::hls_spec::{
    HlsAlgorithmKind, HlsAlgorithmSpec, HlsConvergenceMode, HlsNumericKind, HlsUpdateMode,
};
use crate::domain::{
    ast::{Accessor, BinaryOp, Expr, Operation, Program, TypeExpr},
    gas::GasProgram,
    hls_ops::{KernelOpBundle, OperatorExpr, OperatorOperand},
    hls_template::{
        HlsEdgeConfig, HlsKernelConfig, HlsNodeConfig, HlsTemplateError,
        apply_kernel_multi_merger_unit, apply_kernel_unit, big_merger_group_unit, big_merger_unit,
        graphyflow_big_unit, graphyflow_little_unit, hbm_writer_multi_group_unit, hbm_writer_unit,
        little_kernel_uses_zero_reduce, little_merger_group_unit, little_merger_unit,
        render_graphyflow_big_header, render_graphyflow_little_header,
        shared_kernel_params_multi_merger_unit, shared_kernel_params_unit,
    },
    host_template::{
        HostPreprocessSpec, render_graph_preprocess_cpp, render_graph_preprocess_no_l1_cpp,
    },
};

const AXI_BUS_WIDTH: u32 = 512;

/// Errors that can occur while materializing an HLS project tree.
#[derive(Debug, Error)]
pub enum HlsProjectError {
    #[error("destination '{path}' already exists and is not empty")]
    DestinationNotEmpty { path: PathBuf },
    #[error("template directory '{path}' does not exist")]
    MissingTemplate { path: PathBuf },
    #[error(transparent)]
    GasOps(#[from] GasToHlsOpsError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Template(#[from] HlsTemplateError),
    #[error("invalid HLS config: {0}")]
    InvalidConfig(String),
}

#[derive(Clone, Debug)]
struct HlsProjectConfig {
    kernel: HlsKernelConfig,
    topology: TopologyConfig,
    memory: crate::domain::ast::MemoryBackend,
    max_dst_big: u32,
    max_dst_little: u32,
    preprocess_dense_partitions_per_group: u32,
    preprocess_sparse_partitions_per_group: u32,
    preprocess_dense_balance_window: u32,
    preprocess_sparse_balance_window: u32,
    preprocess_dense_throughput_scale_pct: u32,
    preprocess_sparse_throughput_scale_pct: u32,
    edge: HlsEdgeConfig,
    node: HlsNodeConfig,
    no_l1_preprocess: bool,
}

impl HlsProjectConfig {
    fn from_program_env(
        program: &Program,
        edge: HlsEdgeConfig,
        node: HlsNodeConfig,
    ) -> Result<Self, HlsProjectError> {
        let default_big_kernels = parse_env_usize("GRAPHYFLOW_BIG_KERNELS")?.unwrap_or(4);
        let default_little_kernels = parse_env_usize("GRAPHYFLOW_LITTLE_KERNELS")?.unwrap_or(10);
        let max_dst_big = parse_env_u32("GRAPHYFLOW_MAX_DST_BIG")?.unwrap_or(524_288);
        let max_dst_little = parse_env_u32("GRAPHYFLOW_MAX_DST_LITTLE")?.unwrap_or(65_536);
        let preprocess_dense_partitions_per_group =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_DENSE_PARTITIONS_PER_GROUP")?.unwrap_or(0);
        let preprocess_sparse_partitions_per_group =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_SPARSE_PARTITIONS_PER_GROUP")?.unwrap_or(0);
        let preprocess_dense_balance_window =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_DENSE_BALANCE_WINDOW")?.unwrap_or(0);
        let preprocess_sparse_balance_window =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_SPARSE_BALANCE_WINDOW")?.unwrap_or(0);
        let preprocess_dense_throughput_scale_pct =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_DENSE_THROUGHPUT_SCALE_PCT")?.unwrap_or(100);
        let preprocess_sparse_throughput_scale_pct =
            parse_env_u32("GRAPHYFLOW_PREPROCESS_SPARSE_THROUGHPUT_SCALE_PCT")?.unwrap_or(100);

        let topology =
            TopologyConfig::from_program(program, default_big_kernels, default_little_kernels)?;
        let big_kernels = topology.total_big_pipelines();
        let little_kernels = topology.total_little_pipelines();

        let memory = program.hls.as_ref().map(|h| h.memory).unwrap_or_default();
        let no_l1_preprocess = program
            .hls
            .as_ref()
            .map(|h| h.no_l1_preprocess)
            .unwrap_or(false);

        // Validate: no_l1_preprocess is incompatible with multi-group topologies
        if no_l1_preprocess {
            let has_multi_groups =
                topology.little_groups.len() > 1 || topology.big_groups.len() > 1;
            if has_multi_groups {
                return Err(HlsProjectError::InvalidConfig(
                    "no_l1_preprocess is incompatible with multi-group topologies. \
                     Use a single little_groups and single big_groups entry, or \
                     remove no_l1_preprocess."
                        .to_string(),
                ));
            }
        }

        Ok(Self {
            kernel: HlsKernelConfig::new(big_kernels, little_kernels),
            topology,
            memory,
            max_dst_big,
            max_dst_little,
            preprocess_dense_partitions_per_group,
            preprocess_sparse_partitions_per_group,
            preprocess_dense_balance_window,
            preprocess_sparse_balance_window,
            preprocess_dense_throughput_scale_pct,
            preprocess_sparse_throughput_scale_pct,
            edge,
            node,
            no_l1_preprocess,
        })
    }
}

fn parse_env_usize(name: &str) -> Result<Option<usize>, HlsProjectError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|e| HlsProjectError::InvalidConfig(format!("{name} must be usize: {e}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(HlsProjectError::InvalidConfig(format!(
            "{name} could not be read: {err}"
        ))),
    }
}

fn parse_env_u32(name: &str) -> Result<Option<u32>, HlsProjectError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u32>()
            .map(Some)
            .map_err(|e| HlsProjectError::InvalidConfig(format!("{name} must be u32: {e}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(HlsProjectError::InvalidConfig(format!(
            "{name} could not be read: {err}"
        ))),
    }
}

/// Generates a full SSSP HLS project in `dest_root`, overwriting kernel sources with freshly
/// rendered templates and copying host-side assets unchanged.
pub fn generate_sssp_hls_project(
    gas: &GasProgram,
    program: &Program,
    dest_root: impl AsRef<Path>,
) -> Result<PathBuf, HlsProjectError> {
    let ops = extract_kernel_ops(gas)?;
    let spec = build_algorithm_spec(program, &ops, scatter_uses_edge_weight(&ops));
    let node = build_node_config(program, &spec)?;
    let edge = build_edge_config(program, &ops, &node)?;
    let config = HlsProjectConfig::from_program_env(program, edge, node)?;

    let dest_root = dest_root.as_ref();
    ensure_empty_destination(dest_root)?;

    let template_root = default_template_root();
    if !template_root.exists() {
        return Err(HlsProjectError::MissingTemplate {
            path: template_root,
        });
    }

    copy_dir_recursive(&template_root, dest_root)?;
    render_host_templates(dest_root, config.no_l1_preprocess)?;
    // For PageRank, the apply kernel reads out-degree from node_props directly
    // (ref-style), so the aux port is not needed even though needs_out_degree is true.
    let is_pr = matches!(ops.gather.kind, crate::domain::hls_ops::ReducerKind::Sum)
        && crate::domain::hls_template::utils::expr_uses_operand(
            &ops.apply.expr,
            &crate::domain::hls_ops::OperatorOperand::OldAux,
        );

    let is_ddr = config.memory == crate::domain::ast::MemoryBackend::Ddr;
    let ddr_weighted_sssp = is_ddr_weighted_sssp(&config, &spec);
    let ddr_article_rank = is_ddr_article_rank_marker(program, &spec);

    if is_ddr {
        let ddr_assets_base = default_template_root()
            .parent()
            .unwrap()
            .join("hls_assets")
            .join("scripts_ddr");

        // DDR mode: overlay DDR-specific host assets over the HBM defaults.
        let ddr_host_root = ddr_assets_base.join("host");
        let dest_host = dest_root.join("scripts").join("host");
        if ddr_host_root.exists() {
            copy_dir_recursive(&ddr_host_root, &dest_host)?;
        }

        // Overlay per-algorithm host files (generated_host, host_verifier).
        let algo_host_dir_name = match spec.kind {
            crate::engine::hls_spec::HlsAlgorithmKind::Sssp => "host_sssp",
            crate::engine::hls_spec::HlsAlgorithmKind::ConnectedComponents => "host_cc",
            crate::engine::hls_spec::HlsAlgorithmKind::Pagerank => "host_pr",
            crate::engine::hls_spec::HlsAlgorithmKind::Wcc => "host_wcc",
            crate::engine::hls_spec::HlsAlgorithmKind::ArticleRank => "host_ar",
            _ => "host_sssp",
        };
        let algo_host_root = ddr_assets_base.join(algo_host_dir_name);
        if algo_host_root.exists() {
            copy_dir_recursive(&algo_host_root, &dest_host)?;
        }

        if ddr_weighted_sssp {
            emit_static_ddr_weighted_sssp_assets(dest_root, &ddr_assets_base, &spec)?;
        } else if ddr_article_rank {
            emit_static_ddr_article_rank_assets(dest_root, &ddr_assets_base, &spec)?;
        } else {
            // DDR kernel files: generated through the codegen pipeline
            let kernel_dir = dest_root.join("scripts").join("kernel");
            write_unit(
                &kernel_dir.join("shared_kernel_params.h"),
                crate::domain::hls_template::shared_kernel_params_ddr_unit(
                    &config.kernel,
                    &config.node,
                    is_pr,
                )?,
            )?;
            write_unit(
                &kernel_dir.join("apply_kernel.cpp"),
                crate::domain::hls_template::apply_kernel_ddr_unit(&ops)?,
            )?;
            write_unit(
                &kernel_dir.join("big_merger.cpp"),
                crate::domain::hls_template::big_merger_unit(
                    &ops,
                    &config.kernel,
                    config.edge.zero_sentinel,
                )?,
            )?;
            write_unit(
                &kernel_dir.join("little_merger.cpp"),
                crate::domain::hls_template::little_merger_unit(
                    &ops,
                    &config.kernel,
                    config.edge.zero_sentinel,
                )?,
            )?;
            render_graphyflow_kernels(dest_root, &ops, &config.edge, &config.node)?;

            // Remove hbm_writer (prop_loaders replace it in DDR mode)
            let hbm_writer_path = kernel_dir.join("hbm_writer.cpp");
            if hbm_writer_path.exists() {
                let _ = fs::remove_file(&hbm_writer_path);
            }

            // DDR config files: system.cfg and host_config.h are provided by the
            // per-algorithm DDR host overlay. The codegen's bank assignment
            // (ddr_bank_for_slr) doesn't match the SG reference, so we use the
            // static SG configs. host_config.h was already copied by the overlay.
            let sys_cfg_src = ddr_assets_base.join("system.cfg");
            if sys_cfg_src.exists() {
                fs::copy(&sys_cfg_src, dest_root.join("system.cfg"))?;
            }

            // Generated algorithm config (same for both backends)
            let algo_config_path = dest_root
                .join("scripts")
                .join("host")
                .join("generated_algorithm_config.h");
            fs::write(algo_config_path, build_generated_algorithm_config(&spec)?)?;

            // Emit kernel headers from the active config instead of patching copied
            // static headers. This keeps the header types consistent with the
            // generated kernel source.
            write_generated_kernel_headers(dest_root, &config)?;

            // DDR kernel.mk
            let mk_path = dest_root.join("scripts").join("kernel").join("kernel.mk");
            let mk = fs::read_to_string(&mk_path)?;
            let kernel_names = "graphyflow_little graphyflow_big apply_kernel big_merger little_merger little_prop_loader big_prop_loader";
            let mk_new = mk
                .lines()
                .map(|line| {
                    if line.starts_with("KERNEL_NAMES") {
                        format!("KERNEL_NAMES := {kernel_names}")
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(&mk_path, mk_new)?;
        }
        // DDR mode: rewrite Makefile DEVICE to U200 platform
        let makefile_path = dest_root.join("Makefile");
        if makefile_path.exists() {
            let mf = fs::read_to_string(&makefile_path)?;
            let mf_new = mf
                .lines()
                .map(|line| {
                    if line.starts_with("DEVICE") {
                        "DEVICE := xilinx_u200_gen3x16_xdma_2_202110_1".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(&makefile_path, mf_new)?;
        }
    } else {
        // HBM mode: existing path
        let apply_needs_aux = spec.needs_out_degree && !is_pr;
        render_kernel_templates(
            dest_root,
            &ops,
            &config.kernel,
            &config.edge,
            &config.node,
            &config.topology,
            apply_needs_aux,
            is_pr,
        )?;
        render_project_config(dest_root, &config, &spec, apply_needs_aux)?;
    }

    Ok(dest_root.to_path_buf())
}

fn render_host_templates(dest_root: &Path, no_l1_preprocess: bool) -> Result<(), HlsProjectError> {
    let host_graph_preprocess_dir = dest_root
        .join("scripts")
        .join("host")
        .join("graph_preprocess");
    fs::create_dir_all(&host_graph_preprocess_dir)?;

    let cpp_path = host_graph_preprocess_dir.join("graph_preprocess.cpp");
    if no_l1_preprocess {
        fs::write(&cpp_path, render_graph_preprocess_no_l1_cpp())?;
    } else {
        fs::write(
            &cpp_path,
            render_graph_preprocess_cpp(&HostPreprocessSpec::default()),
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct KernelGroup {
    pipelines: usize,
    merger_slr: Slr,
    pipeline_slr: Vec<Slr>,
}

#[derive(Clone, Debug)]
struct TopologyConfig {
    apply_slr: Slr,
    hbm_writer_slr: Slr,
    cross_slr_fifo_depth: u32,
    little_groups: Vec<KernelGroup>,
    big_groups: Vec<KernelGroup>,
}

impl TopologyConfig {
    fn from_program(
        program: &Program,
        default_big_kernels: usize,
        default_little_kernels: usize,
    ) -> Result<Self, HlsProjectError> {
        let Some(hls) = program.hls.as_ref() else {
            return Ok(Self::default_from_counts(
                default_big_kernels,
                default_little_kernels,
            ));
        };
        let Some(topo) = hls.topology.as_ref() else {
            return Ok(Self::default_from_counts(
                default_big_kernels,
                default_little_kernels,
            ));
        };

        let apply_slr = slr_from_u8(topo.apply_slr)?;
        let hbm_writer_slr = slr_from_u8(topo.hbm_writer_slr)?;
        let cross_slr_fifo_depth = topo.cross_slr_fifo_depth;
        if cross_slr_fifo_depth == 0 {
            return Err(HlsProjectError::InvalidConfig(
                "topology.cross_slr_fifo_depth must be >= 1".to_string(),
            ));
        }

        let little_groups = topo
            .little_groups
            .iter()
            .map(|g| {
                let pipelines = g.pipelines as usize;
                if pipelines == 0 {
                    return Err(HlsProjectError::InvalidConfig(
                        "topology.little_groups[].pipelines must be >= 1".to_string(),
                    ));
                }
                let merger_slr = slr_from_u8(g.merger_slr)?;
                let pipeline_slr = g
                    .pipeline_slr
                    .iter()
                    .map(|v| slr_from_u8(*v))
                    .collect::<Result<Vec<_>, _>>()?;
                if pipeline_slr.len() != pipelines {
                    return Err(HlsProjectError::InvalidConfig(format!(
                        "topology.little_groups[].pipeline_slr length ({}) must equal pipelines ({pipelines})",
                        pipeline_slr.len()
                    )));
                }
                Ok(KernelGroup {
                    pipelines,
                    merger_slr,
                    pipeline_slr,
                })
            })
            .collect::<Result<Vec<_>, HlsProjectError>>()?;

        let big_groups = topo
            .big_groups
            .iter()
            .map(|g| {
                let pipelines = g.pipelines as usize;
                if pipelines == 0 {
                    return Err(HlsProjectError::InvalidConfig(
                        "topology.big_groups[].pipelines must be >= 1".to_string(),
                    ));
                }
                let merger_slr = slr_from_u8(g.merger_slr)?;
                let pipeline_slr = g
                    .pipeline_slr
                    .iter()
                    .map(|v| slr_from_u8(*v))
                    .collect::<Result<Vec<_>, _>>()?;
                if pipeline_slr.len() != pipelines {
                    return Err(HlsProjectError::InvalidConfig(format!(
                        "topology.big_groups[].pipeline_slr length ({}) must equal pipelines ({pipelines})",
                        pipeline_slr.len()
                    )));
                }
                Ok(KernelGroup {
                    pipelines,
                    merger_slr,
                    pipeline_slr,
                })
            })
            .collect::<Result<Vec<_>, HlsProjectError>>()?;

        if little_groups.is_empty() && big_groups.is_empty() {
            return Err(HlsProjectError::InvalidConfig(
                "topology must configure at least one kernel group".to_string(),
            ));
        }

        Ok(Self {
            apply_slr,
            hbm_writer_slr,
            cross_slr_fifo_depth,
            little_groups,
            big_groups,
        })
    }

    fn default_from_counts(big: usize, little: usize) -> Self {
        let mut little_slrs = Vec::with_capacity(little);
        for idx in 0..little {
            little_slrs.push(little_kernel_slr(idx));
        }
        let mut big_slrs = Vec::with_capacity(big);
        for idx in 0..big {
            big_slrs.push(big_kernel_slr(idx));
        }
        Self {
            apply_slr: Slr::Slr1,
            hbm_writer_slr: Slr::Slr0,
            cross_slr_fifo_depth: 16,
            little_groups: vec![KernelGroup {
                pipelines: little,
                merger_slr: Slr::Slr1,
                pipeline_slr: little_slrs,
            }],
            big_groups: vec![KernelGroup {
                pipelines: big,
                merger_slr: Slr::Slr1,
                pipeline_slr: big_slrs,
            }],
        }
    }

    fn total_little_pipelines(&self) -> usize {
        self.little_groups.iter().map(|g| g.pipelines).sum()
    }

    fn total_big_pipelines(&self) -> usize {
        self.big_groups.iter().map(|g| g.pipelines).sum()
    }
}

fn slr_from_u8(v: u8) -> Result<Slr, HlsProjectError> {
    match v {
        0 => Ok(Slr::Slr0),
        1 => Ok(Slr::Slr1),
        2 => Ok(Slr::Slr2),
        _ => Err(HlsProjectError::InvalidConfig(format!(
            "SLR must be 0,1,2 (got {v})"
        ))),
    }
}

fn ensure_empty_destination(dest_root: &Path) -> Result<(), HlsProjectError> {
    if dest_root.exists() {
        if dest_root.is_dir() {
            let mut entries = dest_root.read_dir()?;
            if entries.next().is_some() {
                return Err(HlsProjectError::DestinationNotEmpty {
                    path: dest_root.to_path_buf(),
                });
            }
        } else {
            return Err(HlsProjectError::DestinationNotEmpty {
                path: dest_root.to_path_buf(),
            });
        }
    } else {
        fs::create_dir_all(dest_root)?;
    }
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), HlsProjectError> {
    fs::create_dir_all(dst)?;
    for entry in src.read_dir()? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target_path)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target_path)?;
        }
    }
    Ok(())
}

fn default_template_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("hls_assets")
}

fn is_ddr_weighted_sssp(config: &HlsProjectConfig, spec: &HlsAlgorithmSpec) -> bool {
    config.memory == crate::domain::ast::MemoryBackend::Ddr
        && spec.kind == HlsAlgorithmKind::Sssp
        && spec.needs_edge_weight
}

fn is_ddr_article_rank_marker(program: &Program, spec: &HlsAlgorithmSpec) -> bool {
    if program.hls.as_ref().map(|h| h.memory).unwrap_or_default()
        != crate::domain::ast::MemoryBackend::Ddr
    {
        return false;
    }
    if spec.kind != HlsAlgorithmKind::ArticleRank || spec.target_property != "score" {
        return false;
    }

    let Some(node) = program.schema.node.as_ref() else {
        return false;
    };
    if !node_has_i32_prop(node, "score")
        || !node_has_i32_prop(node, "out_deg")
        || !node_has_i32_prop(node, "avg_out_deg")
    {
        return false;
    }

    let Some(stmt) = program
        .algorithm
        .statements
        .iter()
        .find(|stmt| stmt.target == program.algorithm.return_stmt.value)
    else {
        return false;
    };

    let Operation::Map(map) = &stmt.operation else {
        return false;
    };

    matches_article_rank_apply_expr(&map.lambda.body)
}

fn node_has_i32_prop(node: &crate::domain::ast::EntityDef, name: &str) -> bool {
    node.properties
        .iter()
        .any(|prop| prop.name.as_str() == name && matches!(prop.ty, TypeExpr::Int { width: 32 }))
}

fn matches_article_rank_apply_expr(expr: &Expr) -> bool {
    let Expr::Binary {
        op: BinaryOp::Mul,
        left,
        right,
    } = expr
    else {
        return false;
    };

    (matches_article_rank_score_term(left) && matches_article_rank_scale_term(right))
        || (matches_article_rank_score_term(right) && matches_article_rank_scale_term(left))
}

fn matches_article_rank_score_term(expr: &Expr) -> bool {
    let Expr::Binary {
        op: BinaryOp::Add,
        left,
        right,
    } = expr
    else {
        return false;
    };

    (matches_108_times_param(left) && matches_int_literal(right, 1_258_291))
        || (matches_108_times_param(right) && matches_int_literal(left, 1_258_291))
}

fn matches_108_times_param(expr: &Expr) -> bool {
    let Expr::Binary {
        op: BinaryOp::Mul,
        left,
        right,
    } = expr
    else {
        return false;
    };

    (matches_int_literal(left, 108) && matches_identifier(right))
        || (matches_int_literal(right, 108) && matches_identifier(left))
}

fn matches_article_rank_scale_term(expr: &Expr) -> bool {
    let Expr::Binary {
        op: BinaryOp::Div,
        left,
        right,
    } = expr
    else {
        return false;
    };

    matches_int_literal(left, 65_536) && matches_outdeg_plus_avg_outdeg(right)
}

fn matches_outdeg_plus_avg_outdeg(expr: &Expr) -> bool {
    let Expr::Binary {
        op: BinaryOp::Add,
        left,
        right,
    } = expr
    else {
        return false;
    };

    (matches_self_prop(left, "out_deg") && matches_self_prop(right, "avg_out_deg"))
        || (matches_self_prop(left, "avg_out_deg") && matches_self_prop(right, "out_deg"))
}

fn matches_self_prop(expr: &Expr, prop_name: &str) -> bool {
    matches!(
        expr,
        Expr::MemberAccess {
            target,
            access: Accessor::Property(prop),
        } if matches!(target.as_ref(), Expr::Identifier(id) if id.as_str() == "self")
            && prop.as_str() == prop_name
    )
}

fn matches_int_literal(expr: &Expr, value: i64) -> bool {
    matches!(expr, Expr::Literal(crate::domain::ast::Literal::Int(v)) if *v == value)
}

fn matches_identifier(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_))
}

fn emit_static_ddr_weighted_sssp_assets(
    dest_root: &Path,
    ddr_assets_base: &Path,
    spec: &HlsAlgorithmSpec,
) -> Result<(), HlsProjectError> {
    let kernel_src = ddr_assets_base.join("kernel_sssp");
    if !kernel_src.exists() {
        return Err(HlsProjectError::MissingTemplate { path: kernel_src });
    }
    copy_dir_recursive(&kernel_src, &dest_root.join("scripts").join("kernel"))?;

    let sys_cfg_src = ddr_assets_base.join("system.cfg");
    if !sys_cfg_src.exists() {
        return Err(HlsProjectError::MissingTemplate { path: sys_cfg_src });
    }
    fs::copy(&sys_cfg_src, dest_root.join("system.cfg"))?;

    let algo_config_path = dest_root
        .join("scripts")
        .join("host")
        .join("generated_algorithm_config.h");
    fs::write(algo_config_path, build_generated_algorithm_config(spec)?)?;

    Ok(())
}

fn emit_static_ddr_article_rank_assets(
    dest_root: &Path,
    ddr_assets_base: &Path,
    spec: &HlsAlgorithmSpec,
) -> Result<(), HlsProjectError> {
    let kernel_src = ddr_assets_base.join("kernel_ar");
    if !kernel_src.exists() {
        return Err(HlsProjectError::MissingTemplate { path: kernel_src });
    }
    copy_dir_recursive(&kernel_src, &dest_root.join("scripts").join("kernel"))?;

    let sys_cfg_src = ddr_assets_base.join("system.cfg");
    if !sys_cfg_src.exists() {
        return Err(HlsProjectError::MissingTemplate { path: sys_cfg_src });
    }
    fs::copy(&sys_cfg_src, dest_root.join("system.cfg"))?;

    let algo_config_path = dest_root
        .join("scripts")
        .join("host")
        .join("generated_algorithm_config.h");
    fs::write(algo_config_path, build_generated_algorithm_config(spec)?)?;

    Ok(())
}

fn render_kernel_templates(
    dest_root: &Path,
    ops: &crate::domain::hls_ops::KernelOpBundle,
    config: &HlsKernelConfig,
    edge: &HlsEdgeConfig,
    node: &HlsNodeConfig,
    topology: &TopologyConfig,
    apply_needs_aux: bool,
    is_pr: bool,
) -> Result<(), HlsProjectError> {
    let kernel_dir = dest_root.join("scripts").join("kernel");
    fs::create_dir_all(&kernel_dir)?;

    // This function is only called from the HBM path. The HBM host template
    // always sets per-group merger length/offset args, so HBM must always use
    // multi-merger even for single-group topologies.
    let multi_merger = true;
    if multi_merger {
        let little_group_pipelines: Vec<usize> = if config.little_kernels > 0 {
            topology.little_groups.iter().map(|g| g.pipelines).collect()
        } else {
            Vec::new()
        };
        let big_group_pipelines: Vec<usize> = if config.big_kernels > 0 {
            topology.big_groups.iter().map(|g| g.pipelines).collect()
        } else {
            Vec::new()
        };
        write_unit(
            &kernel_dir.join("shared_kernel_params.h"),
            shared_kernel_params_multi_merger_unit(
                config,
                node,
                &little_group_pipelines,
                &big_group_pipelines,
                apply_needs_aux,
                is_pr,
            )?,
        )?;
    } else {
        write_unit(
            &kernel_dir.join("shared_kernel_params.h"),
            shared_kernel_params_unit(config, node, apply_needs_aux, is_pr)?,
        )?;
    }
    let effective_little_mergers = if config.little_kernels > 0 {
        topology.little_groups.len()
    } else {
        0
    };
    let effective_big_mergers = if config.big_kernels > 0 {
        topology.big_groups.len()
    } else {
        0
    };
    if multi_merger {
        write_unit(
            &kernel_dir.join("apply_kernel.cpp"),
            apply_kernel_multi_merger_unit(ops, effective_little_mergers, effective_big_mergers)?,
        )?;
        if config.big_kernels > 0 {
            for (gid, group) in topology.big_groups.iter().enumerate() {
                write_unit(
                    &kernel_dir.join(format!("big_merger_{gid}.cpp")),
                    big_merger_group_unit(ops, group.pipelines, gid, edge.zero_sentinel)?,
                )?;
            }
        }
        if config.little_kernels > 0 {
            for (gid, group) in topology.little_groups.iter().enumerate() {
                let kernel_id = topology.big_groups.len() + gid;
                write_unit(
                    &kernel_dir.join(format!("little_merger_{kernel_id}.cpp")),
                    little_merger_group_unit(
                        ops,
                        group.pipelines,
                        kernel_id,
                        edge.zero_sentinel || little_kernel_uses_zero_reduce(ops, edge),
                    )?,
                )?;
            }
        }
    } else {
        write_unit(
            &kernel_dir.join("apply_kernel.cpp"),
            apply_kernel_unit(ops)?,
        )?;
        write_unit(
            &kernel_dir.join("big_merger.cpp"),
            big_merger_unit(ops, config, edge.zero_sentinel)?,
        )?;
        // When the little kernel uses reference-style zero-based reduce
        // (identity_val = 0), the merger must also use zero-sentinel logic
        // to correctly skip 0-valued (empty) slots.
        let little_effective_zs = edge.zero_sentinel || little_kernel_uses_zero_reduce(ops, edge);
        write_unit(
            &kernel_dir.join("little_merger.cpp"),
            little_merger_unit(ops, config, little_effective_zs)?,
        )?;
    }
    if multi_merger {
        let little_group_ids = expand_group_ids(&topology.little_groups)
            .into_iter()
            .map(|v| v as usize)
            .collect::<Vec<_>>();
        let big_group_ids = expand_group_ids(&topology.big_groups)
            .into_iter()
            .map(|v| v as usize)
            .collect::<Vec<_>>();
        write_unit(
            &kernel_dir.join("hbm_writer.cpp"),
            hbm_writer_multi_group_unit(
                config,
                &little_group_ids,
                &big_group_ids,
                topology.little_groups.len(),
                topology.big_groups.len(),
            )?,
        )?;
    } else {
        write_unit(&kernel_dir.join("hbm_writer.cpp"), hbm_writer_unit(config)?)?;
    }
    write_unit(
        &kernel_dir.join("graphyflow_big.cpp"),
        graphyflow_big_unit(ops, edge)?,
    )?;
    write_unit(
        &kernel_dir.join("graphyflow_little.cpp"),
        graphyflow_little_unit(ops, edge)?,
    )?;

    optimize_reduce_word_access_big(&kernel_dir.join("graphyflow_big.cpp"))?;
    optimize_reduce_word_access_little(&kernel_dir.join("graphyflow_little.cpp"))?;

    Ok(())
}

fn render_project_config(
    dest_root: &Path,
    config: &HlsProjectConfig,
    spec: &HlsAlgorithmSpec,
    apply_needs_aux: bool,
) -> Result<(), HlsProjectError> {
    let system_cfg_path = dest_root.join("system.cfg");
    fs::write(system_cfg_path, build_system_cfg(config, apply_needs_aux)?)?;

    let host_config_path = dest_root.join("scripts").join("host").join("host_config.h");
    fs::write(
        host_config_path,
        build_host_config(config, apply_needs_aux)?,
    )?;

    let algo_config_path = dest_root
        .join("scripts")
        .join("host")
        .join("generated_algorithm_config.h");
    fs::write(algo_config_path, build_generated_algorithm_config(spec)?)?;

    rewrite_kernel_mk(dest_root, &config.topology)?;
    write_generated_kernel_headers(dest_root, config)?;

    Ok(())
}

/// Generates graphyflow_big.cpp and graphyflow_little.cpp and runs the
/// reduce-word-access optimization pass. Shared between HBM and DDR paths.
fn render_graphyflow_kernels(
    dest_root: &Path,
    ops: &crate::domain::hls_ops::KernelOpBundle,
    edge: &HlsEdgeConfig,
    _node: &HlsNodeConfig,
) -> Result<(), HlsProjectError> {
    let kernel_dir = dest_root.join("scripts").join("kernel");
    write_unit(
        &kernel_dir.join("graphyflow_big.cpp"),
        graphyflow_big_unit(ops, edge)?,
    )?;
    write_unit(
        &kernel_dir.join("graphyflow_little.cpp"),
        graphyflow_little_unit(ops, edge)?,
    )?;
    optimize_reduce_word_access_big(&kernel_dir.join("graphyflow_big.cpp"))?;
    optimize_reduce_word_access_little(&kernel_dir.join("graphyflow_little.cpp"))?;
    Ok(())
}

fn effective_kernel_max_dsts(config: &HlsProjectConfig) -> (u32, u32) {
    if config.memory == crate::domain::ast::MemoryBackend::Ddr {
        (655_360, 122_880)
    } else {
        (config.max_dst_big, config.max_dst_little)
    }
}

/// Emits graphyflow_big.h and graphyflow_little.h directly from the active
/// HLS configuration. This avoids the previous split-brain flow where copied
/// static headers drifted from generated kernel source and required fragile
/// string surgery.
fn write_generated_kernel_headers(
    dest_root: &Path,
    config: &HlsProjectConfig,
) -> Result<(), HlsProjectError> {
    let kernel_dir = dest_root.join("scripts").join("kernel");
    let (max_dst_big, max_dst_little) = effective_kernel_max_dsts(config);

    fs::write(
        kernel_dir.join("graphyflow_big.h"),
        render_graphyflow_big_header(&config.edge, &config.node, max_dst_big),
    )?;
    fs::write(
        kernel_dir.join("graphyflow_little.h"),
        render_graphyflow_little_header(&config.edge, &config.node, max_dst_little),
    )?;
    validate_generated_kernel_headers(&kernel_dir, config)?;

    Ok(())
}

fn validate_generated_kernel_headers(
    kernel_dir: &Path,
    config: &HlsProjectConfig,
) -> Result<(), HlsProjectError> {
    let expected_local_id = format!("#define LOCAL_ID_BITWIDTH {}", config.edge.local_id_bits);
    let expected_payload_bits = format!("#define EDGE_PAYLOAD_BITS {}", config.edge.payload_bits());
    let expected_edges_per_word = format!("#define EDGES_PER_WORD {}", config.edge.edges_per_word);
    let expected_prop_lsb = format!(
        "#define EDGE_PROP_PAYLOAD_LSB {}",
        config.edge.edge_prop_payload_lsb()
    );
    let expected_invalid_macro = [
        ("graphyflow_big.h", "INVALID_LOCAL_ID_BIG"),
        ("graphyflow_little.h", "INVALID_LOCAL_ID_LITTLE"),
    ];
    for header in ["graphyflow_big.h", "graphyflow_little.h"] {
        let path = kernel_dir.join(header);
        let content = fs::read_to_string(&path)?;
        if !content.contains(&expected_local_id) {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} does not define the expected local ID width ({})",
                path.display(),
                config.edge.local_id_bits
            )));
        }
        if !content.contains("typedef ap_uint<LOCAL_ID_BITWIDTH> local_id_t;") {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} is missing local_id_t typedef",
                path.display()
            )));
        }
        if content.contains("ap_uint<20> dst_id") || content.contains("ap_uint<20> node_id") {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} still contains hardcoded 20-bit local ID fields",
                path.display()
            )));
        }
        if !content.contains(&expected_payload_bits) {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} does not define the expected packed edge payload width ({})",
                path.display(),
                config.edge.payload_bits()
            )));
        }
        if !content.contains(&expected_edges_per_word) {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} does not define the expected packed edges-per-word ({})",
                path.display(),
                config.edge.edges_per_word
            )));
        }
        if !content.contains(&expected_prop_lsb) {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} does not define the expected edge property payload offset ({})",
                path.display(),
                config.edge.edge_prop_payload_lsb()
            )));
        }
        let invalid_macro = expected_invalid_macro
            .iter()
            .find(|(name, _)| *name == header)
            .map(|(_, macro_name)| *macro_name)
            .unwrap();
        if !content.contains(invalid_macro) {
            return Err(HlsProjectError::InvalidConfig(format!(
                "{} is missing the invalid local-id sentinel macro {}",
                path.display(),
                invalid_macro
            )));
        }
    }
    Ok(())
}

fn rewrite_kernel_mk(dest_root: &Path, topology: &TopologyConfig) -> Result<(), HlsProjectError> {
    let mk_path = dest_root.join("scripts").join("kernel").join("kernel.mk");
    let mk = fs::read_to_string(&mk_path)?;

    let multi_merger = true; // Always use multi-merger to match host template
    let little_kernels = topology.total_little_pipelines();
    let big_kernels = topology.total_big_pipelines();
    let mut kernel_names: Vec<String> = Vec::new();
    if little_kernels > 0 {
        kernel_names.push("graphyflow_little".to_string());
    }
    if big_kernels > 0 {
        kernel_names.push("graphyflow_big".to_string());
    }
    kernel_names.push("apply_kernel".to_string());
    kernel_names.push("hbm_writer".to_string());
    if multi_merger {
        for gid in 0..topology.big_groups.len() {
            if big_kernels > 0 {
                kernel_names.push(format!("big_merger_{gid}"));
            }
        }
        for gid in 0..topology.little_groups.len() {
            if little_kernels > 0 {
                let kernel_id = topology.big_groups.len() + gid;
                kernel_names.push(format!("little_merger_{kernel_id}"));
            }
        }
    } else {
        if big_kernels > 0 {
            kernel_names.push("big_merger".to_string());
        }
        if little_kernels > 0 {
            kernel_names.push("little_merger".to_string());
        }
    }

    let replacement = format!("KERNEL_NAMES := {}", kernel_names.join(" "));
    let mut out_lines = Vec::new();
    let mut replaced = false;
    for line in mk.lines() {
        if line.trim_start().starts_with("KERNEL_NAMES :=") {
            out_lines.push(replacement.clone());
            replaced = true;
        } else {
            out_lines.push(line.to_string());
        }
    }
    if !replaced {
        return Err(HlsProjectError::InvalidConfig(format!(
            "could not find KERNEL_NAMES line in {}",
            mk_path.display()
        )));
    }

    fs::write(mk_path, out_lines.join("\n"))?;
    Ok(())
}

fn build_algorithm_spec(
    program: &Program,
    ops: &KernelOpBundle,
    needs_edge_weight: bool,
) -> HlsAlgorithmSpec {
    let target_property = program.algorithm.return_stmt.property.as_str();
    let node_schema = program.schema.node.as_ref();
    let mut numeric_kind = HlsNumericKind::Fixed;
    let mut bitwidth = 32u32;
    let mut int_width = 16u32;
    if let Some(node) = node_schema {
        if let Some(prop_def) = node
            .properties
            .iter()
            .find(|prop| prop.name.as_str() == target_property)
        {
            match &prop_def.ty {
                crate::domain::ast::TypeExpr::Int { width } => {
                    numeric_kind = HlsNumericKind::Int;
                    bitwidth = *width;
                    int_width = *width;
                }
                crate::domain::ast::TypeExpr::Fixed {
                    width,
                    int_width: iw,
                } => {
                    numeric_kind = HlsNumericKind::Fixed;
                    bitwidth = *width;
                    int_width = *iw;
                }
                crate::domain::ast::TypeExpr::Float => {
                    numeric_kind = HlsNumericKind::Float;
                    bitwidth = 32;
                    int_width = 16;
                }
                crate::domain::ast::TypeExpr::Bool => {
                    numeric_kind = HlsNumericKind::Int;
                    bitwidth = 1;
                    int_width = 1;
                }
                _ => {}
            }
        }
    }
    HlsAlgorithmSpec::classify(
        target_property,
        ops,
        needs_edge_weight,
        numeric_kind,
        bitwidth,
        int_width,
    )
}

fn build_generated_algorithm_config(spec: &HlsAlgorithmSpec) -> Result<String, HlsProjectError> {
    let kind = match spec.kind {
        HlsAlgorithmKind::Sssp => "AlgorithmKind::Sssp",
        HlsAlgorithmKind::ConnectedComponents => "AlgorithmKind::ConnectedComponents",
        HlsAlgorithmKind::Pagerank => "AlgorithmKind::Pagerank",
        HlsAlgorithmKind::Bfs => "AlgorithmKind::Bfs",
        HlsAlgorithmKind::ArticleRank => "AlgorithmKind::ArticleRank",
        HlsAlgorithmKind::Wcc => "AlgorithmKind::Wcc",
        HlsAlgorithmKind::Unknown => "AlgorithmKind::Sssp",
    };
    let conv = match spec.convergence_mode {
        HlsConvergenceMode::MinImprove => "ConvergenceMode::MinImprove",
        HlsConvergenceMode::EqualityStable => "ConvergenceMode::EqualityStable",
        HlsConvergenceMode::DeltaThreshold => "ConvergenceMode::DeltaThreshold",
        HlsConvergenceMode::FixedIterations => "ConvergenceMode::FixedIterations",
        HlsConvergenceMode::NewlyDiscoveredZero => "ConvergenceMode::NewlyDiscoveredZero",
    };
    let numeric_kind = match spec.numeric_kind {
        HlsNumericKind::Fixed => "NumericKind::Fixed",
        HlsNumericKind::Float => "NumericKind::Float",
        HlsNumericKind::Int => "NumericKind::Int",
    };
    let update_mode = match spec.update_mode {
        HlsUpdateMode::Min => "UpdateMode::Min",
        HlsUpdateMode::Max => "UpdateMode::Max",
        HlsUpdateMode::Overwrite => "UpdateMode::Overwrite",
    };
    let needs_edge_weight = if spec.needs_edge_weight {
        "true"
    } else {
        "false"
    };
    let needs_out_degree = if spec.needs_out_degree {
        "true"
    } else {
        "false"
    };
    let delta_threshold = if spec.delta_threshold.fract() == 0.0 {
        format!("{:.1}", spec.delta_threshold)
    } else {
        spec.delta_threshold.to_string()
    };

    Ok(format!(
        r#"#ifndef __GENERATED_ALGORITHM_CONFIG_H__
#define __GENERATED_ALGORITHM_CONFIG_H__

// Auto-generated by the GraphyFlow backend. Do not edit by hand.

#include "algorithm_config.h"

inline AlgorithmConfig graphyflow_generated_config() {{
    AlgorithmConfig cfg;
    cfg.target_property = "{target_property}";
    cfg.numeric_kind = {numeric_kind};
    cfg.bitwidth = {bitwidth};
    cfg.int_width = {int_width};
    cfg.convergence_mode = {conv};
    cfg.delta_threshold = {delta_threshold}f;
    cfg.max_iterations = {max_iterations};
    cfg.needs_edge_weight = {needs_edge_weight};
    cfg.needs_out_degree = {needs_out_degree};
    cfg.update_mode = {update_mode};
    cfg.active_mask = {active_mask}u;
    cfg.inf_value = {inf_value}u;
    cfg.algorithm_kind = {kind};
    return cfg;
}}

#endif // __GENERATED_ALGORITHM_CONFIG_H__
"#,
        target_property = spec.target_property,
        numeric_kind = numeric_kind,
        bitwidth = spec.bitwidth,
        int_width = spec.int_width,
        conv = conv,
        delta_threshold = delta_threshold,
        max_iterations = spec.max_iterations,
        needs_edge_weight = needs_edge_weight,
        needs_out_degree = needs_out_degree,
        update_mode = update_mode,
        active_mask = spec.active_mask,
        inf_value = spec.inf_value,
        kind = kind,
    ))
}

fn write_unit(
    path: &Path,
    unit: crate::domain::hls::HlsCompilationUnit,
) -> Result<(), HlsProjectError> {
    fs::write(path, unit.to_code())?;
    Ok(())
}

fn optimize_reduce_word_access_big(path: &Path) -> Result<(), HlsProjectError> {
    let mut code = fs::read_to_string(path)?;

    let old_extract = "        uint32_t bit_low = (slot * DISTANCE_BITWIDTH);\n        uint32_t bit_high = (bit_low + (DISTANCE_BITWIDTH - 1u));\n        ap_fixed_pod_t current_val = current_word.range(bit_high, bit_low);";
    let new_extract = "        ap_fixed_pod_t lane_vals[DISTANCES_PER_REDUCE_WORD];\n        #pragma HLS ARRAY_PARTITION variable = lane_vals complete dim = 0\n        LOOP_FOR_24_EXTRACT: for (int32_t lane = 0; (lane < DISTANCES_PER_REDUCE_WORD); ++lane) {\n            #pragma HLS UNROLL\n            uint32_t lane_low = (lane * DISTANCE_BITWIDTH);\n            lane_vals[lane] = current_word.range((lane_low + (DISTANCE_BITWIDTH - 1u)), lane_low);\n        }\n        ap_fixed_pod_t current_val = lane_vals[slot];";
    let old_pack = "        current_word.range(bit_high, bit_low) = updated_val;";
    let new_pack = "        LOOP_FOR_24_PACK: for (int32_t lane = 0; (lane < DISTANCES_PER_REDUCE_WORD); ++lane) {\n            #pragma HLS UNROLL\n            uint32_t lane_low = (lane * DISTANCE_BITWIDTH);\n            ap_fixed_pod_t packed_lane = lane_vals[lane];\n            if (slot == static_cast<ap_uint<LOG_DISTANCES_PER_REDUCE_WORD>>(lane)) {\n                packed_lane = updated_val;\n            }\n            current_word.range((lane_low + (DISTANCE_BITWIDTH - 1u)), lane_low) = packed_lane;\n        }";

    code = code.replace(old_extract, new_extract);
    code = code.replace(old_pack, new_pack);
    fs::write(path, code)?;
    Ok(())
}

fn optimize_reduce_word_access_little(path: &Path) -> Result<(), HlsProjectError> {
    let mut code = fs::read_to_string(path)?;

    let old_extract = "                uint32_t bit_low = (slot * DISTANCE_BITWIDTH);\n                uint32_t bit_high = (bit_low + (DISTANCE_BITWIDTH - 1u));\n                ap_fixed_pod_t current_val = current_word.range(bit_high, bit_low);";
    let new_extract = "                ap_fixed_pod_t lane_vals[DISTANCES_PER_REDUCE_WORD];\n                #pragma HLS ARRAY_PARTITION variable = lane_vals complete dim = 0\n                LOOP_FOR_33_EXTRACT: for (int32_t lane = 0; (lane < DISTANCES_PER_REDUCE_WORD); ++lane) {\n                    #pragma HLS UNROLL\n                    uint32_t lane_low = (lane * DISTANCE_BITWIDTH);\n                    lane_vals[lane] = current_word.range((lane_low + (DISTANCE_BITWIDTH - 1u)), lane_low);\n                }\n                ap_fixed_pod_t current_val = lane_vals[slot];";
    let old_pack = "                current_word.range(bit_high, bit_low) = updated_val;";
    let new_pack = "                LOOP_FOR_33_PACK: for (int32_t lane = 0; (lane < DISTANCES_PER_REDUCE_WORD); ++lane) {\n                    #pragma HLS UNROLL\n                    uint32_t lane_low = (lane * DISTANCE_BITWIDTH);\n                    ap_fixed_pod_t packed_lane = lane_vals[lane];\n                    if (slot == static_cast<ap_uint<LOG_DISTANCES_PER_REDUCE_WORD>>(lane)) {\n                        packed_lane = updated_val;\n                    }\n                    current_word.range((lane_low + (DISTANCE_BITWIDTH - 1u)), lane_low) = packed_lane;\n                }";

    code = code.replace(old_extract, new_extract);
    code = code.replace(old_pack, new_pack);
    fs::write(path, code)?;
    Ok(())
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slr {
    Slr0,
    Slr1,
    Slr2,
}

impl Slr {
    fn as_str(self) -> &'static str {
        match self {
            Slr::Slr0 => "SLR0",
            Slr::Slr1 => "SLR1",
            Slr::Slr2 => "SLR2",
        }
    }
}

fn little_kernel_slr(idx: usize) -> Slr {
    match idx % 3 {
        0 => Slr::Slr0,
        1 => Slr::Slr1,
        _ => Slr::Slr2,
    }
}

fn big_kernel_slr(idx: usize) -> Slr {
    const PATTERN: [Slr; 4] = [Slr::Slr2, Slr::Slr1, Slr::Slr2, Slr::Slr0];
    PATTERN[idx % PATTERN.len()]
}

fn stream_depth_suffix(src: Slr, dst: Slr, depth: u32, force: bool) -> String {
    if !force && src == dst {
        String::new()
    } else {
        format!(":{depth}")
    }
}

fn env_truthy(name: &str) -> Option<bool> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            let truthy = matches!(value.as_str(), "1" | "true" | "yes" | "on");
            let falsy = matches!(value.as_str(), "0" | "false" | "no" | "off");
            if truthy {
                Some(true)
            } else if falsy {
                Some(false)
            } else {
                None
            }
        }
        Err(env::VarError::NotPresent) => None,
        Err(_) => None,
    }
}

fn build_system_cfg(
    config: &HlsProjectConfig,
    apply_needs_aux: bool,
) -> Result<String, HlsProjectError> {
    let mapping = build_hbm_mapping(&config.kernel)?;
    let writer_output_hbm = select_writer_output_hbm(&mapping, apply_needs_aux)?;
    let mut lines = Vec::new();
    let topo = &config.topology;
    let multi_merger = true; // Always use multi-merger to match host template

    lines.push("[connectivity]".to_string());
    lines.push(String::new());
    lines.push("# --- 1. Kernel Instantiation (nk) ---".to_string());
    if config.kernel.little_kernels > 0 {
        lines.push(format!(
            "nk=graphyflow_little:{}",
            config.kernel.little_kernels
        ));
    }
    if config.kernel.big_kernels > 0 {
        lines.push(format!("nk=graphyflow_big:{}", config.kernel.big_kernels));
    }
    lines.push("nk=hbm_writer:1".to_string());
    lines.push("nk=apply_kernel:1".to_string());
    if multi_merger {
        if config.kernel.big_kernels > 0 {
            for gid in 0..topo.big_groups.len() {
                lines.push(format!("nk=big_merger_{gid}:1"));
            }
        }
        if config.kernel.little_kernels > 0 {
            for gid in 0..topo.little_groups.len() {
                let kernel_id = topo.big_groups.len() + gid;
                lines.push(format!("nk=little_merger_{kernel_id}:1"));
            }
        }
    } else {
        if config.kernel.big_kernels > 0 {
            lines.push("nk=big_merger:1".to_string());
        }
        if config.kernel.little_kernels > 0 {
            lines.push("nk=little_merger:1".to_string());
        }
    }
    lines.push(String::new());
    lines.push("# --- 2. HBM Port Mapping (sp) ---".to_string());
    lines.push(String::new());

    for (idx, hbm_id) in mapping.little_edge.iter().enumerate() {
        let instance = idx + 1;
        lines.push(format!(
            "# -- Mapping for instance: graphyflow_little_{instance} --"
        ));
        lines.push(format!(
            "sp=graphyflow_little_{instance}.edge_props:HBM[{hbm_id}]"
        ));
    }

    for (idx, hbm_id) in mapping.big_edge.iter().enumerate() {
        let instance = idx + 1;
        lines.push(format!(
            "# -- Mapping for instance: graphyflow_big_{instance} --"
        ));
        lines.push(format!(
            "sp=graphyflow_big_{instance}.edge_props:HBM[{hbm_id}]"
        ));
    }

    lines.push("# -- Mapping for instance: hbm_writer_1 --".to_string());
    for (idx, hbm_id) in mapping
        .little_node
        .iter()
        .chain(mapping.big_node.iter())
        .enumerate()
    {
        let port_idx = idx + 1;
        lines.push(format!("sp=hbm_writer_1.src_prop_{port_idx}:HBM[{hbm_id}]"));
    }
    lines.push(format!("sp=hbm_writer_1.output:HBM[{writer_output_hbm}]"));
    lines.push(String::new());
    lines.push("# -- Mapping for instance: apply_kernel_1 --".to_string());
    lines.push("sp=apply_kernel_1.node_props:HBM[30]".to_string());
    if apply_needs_aux {
        lines.push("sp=apply_kernel_1.aux_node_props:HBM[31]".to_string());
    }
    lines.push(String::new());
    lines.push("# --- 3. Stream Connections ---".to_string());
    lines.push(String::new());

    let hbm_writer_slr = topo.hbm_writer_slr;
    let apply_slr = topo.apply_slr;
    let cross_depth = topo.cross_slr_fifo_depth;
    let force_depth =
        env_truthy("GRAPHYFLOW_FORCE_STREAM_DEPTH").unwrap_or_else(|| topology_single_slr(topo));

    // --- big pipelines ---
    let mut big_global = 0usize;
    for (gid, group) in topo.big_groups.iter().enumerate() {
        let merger_slr = group.merger_slr;
        let merger_inst = if multi_merger {
            format!("big_merger_{gid}_1")
        } else {
            "big_merger_1".to_string()
        };
        for local in 0..group.pipelines {
            let idx = big_global;
            let instance = idx + 1;
            let kernel_slr = group
                .pipeline_slr
                .get(local)
                .copied()
                .unwrap_or_else(|| big_kernel_slr(idx));

            lines.push(format!(
                "# -- Stream connections for graphyflow_big_{instance} --"
            ));
            lines.push(format!(
                "stream_connect=graphyflow_big_{instance}.cacheline_req_stream:hbm_writer_1.cacheline_req_stream_{instance}{}",
                stream_depth_suffix(kernel_slr, hbm_writer_slr, cross_depth, force_depth)
            ));
            lines.push(format!(
                "stream_connect=hbm_writer_1.cacheline_resp_stream_{instance}:graphyflow_big_{instance}.cacheline_resp_stream{}",
                stream_depth_suffix(hbm_writer_slr, kernel_slr, cross_depth, force_depth)
            ));
            lines.push(format!(
                "stream_connect=graphyflow_big_{instance}.kernel_out_stream:{merger_inst}.big_kernel_{}_out_stream{}",
                local + 1,
                stream_depth_suffix(kernel_slr, merger_slr, cross_depth, force_depth)
            ));

            big_global += 1;
        }
    }

    // --- little pipelines ---
    let mut little_global = 0usize;
    for (gid, group) in topo.little_groups.iter().enumerate() {
        let merger_slr = group.merger_slr;
        let merger_inst = if multi_merger {
            format!("little_merger_{}_1", topo.big_groups.len() + gid)
        } else {
            "little_merger_1".to_string()
        };
        for local in 0..group.pipelines {
            let idx = little_global;
            let instance = idx + 1;
            let kernel_slr = group
                .pipeline_slr
                .get(local)
                .copied()
                .unwrap_or_else(|| little_kernel_slr(idx));

            lines.push(format!(
                "# -- Stream connections for graphyflow_little_{instance} --"
            ));
            lines.push(format!(
                "stream_connect=graphyflow_little_{instance}.ppb_req_stream:hbm_writer_1.ppb_req_stream_{instance}{}",
                stream_depth_suffix(kernel_slr, hbm_writer_slr, cross_depth, force_depth)
            ));
            lines.push(format!(
                "stream_connect=hbm_writer_1.ppb_resp_stream_{instance}:graphyflow_little_{instance}.ppb_resp_stream{}",
                stream_depth_suffix(hbm_writer_slr, kernel_slr, cross_depth, force_depth)
            ));
            lines.push(format!(
                "stream_connect=graphyflow_little_{instance}.kernel_out_stream:{merger_inst}.little_kernel_{}_out_stream{}",
                local + 1,
                stream_depth_suffix(kernel_slr, merger_slr, cross_depth, force_depth)
            ));

            little_global += 1;
        }
    }

    // --- mergers -> apply ---
    if multi_merger {
        if config.kernel.big_kernels > 0 {
            for (gid, group) in topo.big_groups.iter().enumerate() {
                let merger_inst = format!("big_merger_{gid}_1");
                lines.push(format!("# -- Stream connections for {merger_inst} --"));
                lines.push(format!(
                    "stream_connect={merger_inst}.kernel_out_stream:apply_kernel_1.big_merger_{gid}_out_stream{}",
                    stream_depth_suffix(group.merger_slr, apply_slr, cross_depth, true)
                ));
                lines.push(String::new());
            }
        }
        if config.kernel.little_kernels > 0 {
            for (gid, group) in topo.little_groups.iter().enumerate() {
                let kernel_id = topo.big_groups.len() + gid;
                let merger_inst = format!("little_merger_{kernel_id}_1");
                lines.push(format!("# -- Stream connections for {merger_inst} --"));
                lines.push(format!(
                    "stream_connect={merger_inst}.kernel_out_stream:apply_kernel_1.little_merger_{kernel_id}_out_stream{}",
                    stream_depth_suffix(group.merger_slr, apply_slr, cross_depth, true)
                ));
                lines.push(String::new());
            }
        }
    } else {
        let merger_slr_little = topo
            .little_groups
            .first()
            .map(|g| g.merger_slr)
            .unwrap_or(Slr::Slr1);
        let merger_slr_big = topo
            .big_groups
            .first()
            .map(|g| g.merger_slr)
            .unwrap_or(Slr::Slr1);
        if config.kernel.little_kernels > 0 {
            lines.push("# -- Stream connections for little_merger_1 --".to_string());
            lines.push(format!(
                "stream_connect=little_merger_1.kernel_out_stream:apply_kernel_1.little_kernel_out_stream{}",
                stream_depth_suffix(merger_slr_little, apply_slr, cross_depth, force_depth)
            ));
            lines.push(String::new());
        }
        if config.kernel.big_kernels > 0 {
            lines.push("# -- Stream connections for big_merger_1 --".to_string());
            lines.push(format!(
                "stream_connect=big_merger_1.kernel_out_stream:apply_kernel_1.big_kernel_out_stream{}",
                stream_depth_suffix(merger_slr_big, apply_slr, cross_depth, force_depth)
            ));
            lines.push(String::new());
        }
    }

    lines.push("# -- Stream connections for apply_kernel_1 --".to_string());
    lines.push(format!(
        "stream_connect=apply_kernel_1.kernel_out_stream:hbm_writer_1.write_burst_stream{}",
        stream_depth_suffix(apply_slr, hbm_writer_slr, cross_depth, force_depth)
    ));
    lines.push(String::new());

    lines.push("# --- 4. SLR Placement ---".to_string());
    // little kernel SLRs
    let mut little_global = 0usize;
    for group in &topo.little_groups {
        for local in 0..group.pipelines {
            let idx = little_global;
            let instance = idx + 1;
            let slr = group
                .pipeline_slr
                .get(local)
                .copied()
                .unwrap_or_else(|| little_kernel_slr(idx));
            lines.push(format!("slr=graphyflow_little_{instance}:{}", slr.as_str()));
            little_global += 1;
        }
    }
    // big kernel SLRs
    let mut big_global = 0usize;
    for group in &topo.big_groups {
        for local in 0..group.pipelines {
            let idx = big_global;
            let instance = idx + 1;
            let slr = group
                .pipeline_slr
                .get(local)
                .copied()
                .unwrap_or_else(|| big_kernel_slr(idx));
            lines.push(format!("slr=graphyflow_big_{instance}:{}", slr.as_str()));
            big_global += 1;
        }
    }
    lines.push(format!("slr=hbm_writer_1:{}", hbm_writer_slr.as_str()));
    lines.push(format!("slr=apply_kernel_1:{}", apply_slr.as_str()));
    if multi_merger {
        if config.kernel.little_kernels > 0 {
            for (gid, group) in topo.little_groups.iter().enumerate() {
                let kernel_id = topo.big_groups.len() + gid;
                lines.push(format!(
                    "slr=little_merger_{}_1:{}",
                    kernel_id,
                    group.merger_slr.as_str()
                ));
            }
        }
        if config.kernel.big_kernels > 0 {
            for (gid, group) in topo.big_groups.iter().enumerate() {
                lines.push(format!(
                    "slr=big_merger_{gid}_1:{}",
                    group.merger_slr.as_str()
                ));
            }
        }
    } else {
        if config.kernel.little_kernels > 0 {
            let merger_slr_little = topo
                .little_groups
                .first()
                .map(|g| g.merger_slr)
                .unwrap_or(Slr::Slr1);
            lines.push(format!(
                "slr=little_merger_1:{}",
                merger_slr_little.as_str()
            ));
        }
        if config.kernel.big_kernels > 0 {
            let merger_slr_big = topo
                .big_groups
                .first()
                .map(|g| g.merger_slr)
                .unwrap_or(Slr::Slr1);
            lines.push(format!("slr=big_merger_1:{}", merger_slr_big.as_str()));
        }
    }

    Ok(lines.join("\n"))
}

fn topology_single_slr(topo: &TopologyConfig) -> bool {
    let base = topo.hbm_writer_slr;
    if topo.apply_slr != base {
        return false;
    }
    for group in topo.little_groups.iter().chain(topo.big_groups.iter()) {
        if group.merger_slr != base {
            return false;
        }
        if group.pipeline_slr.iter().any(|&slr| slr != base) {
            return false;
        }
    }
    true
}

struct HbmMapping {
    little_edge: Vec<u32>,
    little_node: Vec<u32>,
    big_edge: Vec<u32>,
    big_node: Vec<u32>,
}

fn select_writer_output_hbm(
    mapping: &HbmMapping,
    apply_needs_aux: bool,
) -> Result<u32, HlsProjectError> {
    let mut used = mapping
        .little_edge
        .iter()
        .chain(mapping.little_node.iter())
        .chain(mapping.big_edge.iter())
        .chain(mapping.big_node.iter())
        .copied()
        .collect::<Vec<_>>();
    used.push(30);
    if apply_needs_aux {
        used.push(31);
    }

    for candidate in (0u32..32).rev() {
        if !used.contains(&candidate) {
            return Ok(candidate);
        }
    }

    Err(HlsProjectError::InvalidConfig(
        "No free HBM bank left for hbm_writer output".to_string(),
    ))
}

fn build_hbm_mapping(config: &HlsKernelConfig) -> Result<HbmMapping, HlsProjectError> {
    let little_edge = hbm_sequence(0, config.little_kernels);
    let little_node = hbm_sequence(1, config.little_kernels);
    let base = (config.little_kernels as u32) * 2;
    let big_edge = hbm_sequence(base, config.big_kernels);
    let big_node = hbm_sequence(base + 1, config.big_kernels);

    let all_used = little_edge
        .iter()
        .chain(little_node.iter())
        .chain(big_edge.iter())
        .chain(big_node.iter())
        .copied()
        .collect::<Vec<_>>();

    if all_used.iter().any(|&id| id >= 32) {
        return Err(HlsProjectError::InvalidConfig(
            "HBM mapping exceeds HBM[31]".to_string(),
        ));
    }
    if all_used.iter().any(|&id| id == 30) {
        return Err(HlsProjectError::InvalidConfig(
            "HBM[30] is reserved for apply_kernel node_props".to_string(),
        ));
    }

    Ok(HbmMapping {
        little_edge,
        little_node,
        big_edge,
        big_node,
    })
}

fn hbm_sequence(base: u32, count: usize) -> Vec<u32> {
    (0..count).map(|idx| base + (idx as u32 * 2)).collect()
}

// ---------------------------------------------------------------------------
// DDR system.cfg and host_config builders
// ---------------------------------------------------------------------------

fn build_host_config(
    config: &HlsProjectConfig,
    apply_needs_aux: bool,
) -> Result<String, HlsProjectError> {
    if config.edge.local_id_bits == 0 || config.edge.local_id_bits > 32 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "HBM local_id_bits {} is incompatible with host-side invalid sentinel generation",
            config.edge.local_id_bits
        )));
    }
    let invalid_local_id = 1u64 << (config.edge.local_id_bits - 1);
    if u64::from(config.max_dst_big) > invalid_local_id
        || u64::from(config.max_dst_little) > invalid_local_id
    {
        return Err(HlsProjectError::InvalidConfig(format!(
            "max_dst values ({}, {}) exceed the top-bit invalid-local-id sentinel for local_id_bits={}",
            config.max_dst_big, config.max_dst_little, config.edge.local_id_bits
        )));
    }

    let mapping = build_hbm_mapping(&config.kernel)?;
    let writer_output_hbm = select_writer_output_hbm(&mapping, apply_needs_aux)?;

    let little_edge = join_vec_init(&mapping.little_edge, 0);
    let little_node = join_vec_init(&mapping.little_node, 0);
    let big_edge = join_vec_init(&mapping.big_edge, 0);
    let big_node = join_vec_init(&mapping.big_node, 0);

    let little_group_lengths = config
        .topology
        .little_groups
        .iter()
        .map(|g| g.pipelines.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let big_group_lengths = config
        .topology
        .big_groups
        .iter()
        .map(|g| g.pipelines.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let little_group_offsets = group_offsets(&config.topology.little_groups)
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let big_group_offsets = group_offsets(&config.topology.big_groups)
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let little_group_ids = expand_group_ids(&config.topology.little_groups)
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let big_group_ids = expand_group_ids(&config.topology.big_groups)
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let little_group_lengths = nonempty_array_init(&little_group_lengths, "0");
    let big_group_lengths = nonempty_array_init(&big_group_lengths, "0");
    let little_group_offsets = nonempty_array_init(&little_group_offsets, "0");
    let big_group_offsets = nonempty_array_init(&big_group_offsets, "0");
    let little_group_ids = nonempty_array_init(&little_group_ids, "0");
    let big_group_ids = nonempty_array_init(&big_group_ids, "0");

    let edge_prop_count = config.edge.edge_prop_widths.len();
    let edge_prop_bits = config.edge.edge_prop_bits;
    let edge_prop_widths = config
        .edge
        .edge_prop_widths
        .iter()
        .map(|width| width.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let edge_prop_widths_block = if edge_prop_count > 0 {
        format!(
            "#define EDGE_PROP_COUNT {edge_prop_count}\n\
static const uint32_t EDGE_PROP_WIDTHS[EDGE_PROP_COUNT] = {{{edge_prop_widths}}};\n"
        )
    } else {
        format!("#define EDGE_PROP_COUNT 0\n")
    };

    Ok(format!(
        "\
#ifndef __HOST_CONFIG_H__\n\
#define __HOST_CONFIG_H__\n\
\n\
#include <stdint.h>\n\
\n\
#define BIG_KERNEL_NUM {big_kernels}\n\
#define LITTLE_KERNEL_NUM {little_kernels}\n\
\n\
#define NUM_LITTLE_MERGERS {num_little_mergers}\n\
#define NUM_BIG_MERGERS {num_big_mergers}\n\
\n\
#define NUM_KERNEL (BIG_KERNEL_NUM + LITTLE_KERNEL_NUM)\n\
\n\
static constexpr uint32_t LITTLE_MERGER_PIPELINE_LENGTHS[] = {{ {little_group_lengths} }};\n\
static constexpr uint32_t LITTLE_MERGER_KERNEL_OFFSETS[] = {{ {little_group_offsets} }};\n\
static constexpr uint32_t BIG_MERGER_PIPELINE_LENGTHS[] = {{ {big_group_lengths} }};\n\
static constexpr uint32_t BIG_MERGER_KERNEL_OFFSETS[] = {{ {big_group_offsets} }};\n\
static constexpr uint32_t LITTLE_KERNEL_GROUP_ID[] = {{ {little_group_ids} }};\n\
static constexpr uint32_t BIG_KERNEL_GROUP_ID[] = {{ {big_group_ids} }};\n\
\n\
#define MAX_DST_BIG {max_dst_big}\n\
#define MAX_DST_LITTLE {max_dst_little}\n\
#define LOCAL_ID_BITWIDTH {local_id_bits}\n\
#define LOCAL_ID_MSB (LOCAL_ID_BITWIDTH - 1)\n\
#define INVALID_LOCAL_ID_BIG (1u << LOCAL_ID_MSB)\n\
#define INVALID_LOCAL_ID_LITTLE (1u << LOCAL_ID_MSB)\n\
\n\
#define GRAPH_PREPROCESS_DENSE_PARTITIONS_PER_GROUP {preprocess_dense_partitions_per_group}\n\
#define GRAPH_PREPROCESS_SPARSE_PARTITIONS_PER_GROUP {preprocess_sparse_partitions_per_group}\n\
#define GRAPH_PREPROCESS_DENSE_BALANCE_WINDOW {preprocess_dense_balance_window}\n\
#define GRAPH_PREPROCESS_SPARSE_BALANCE_WINDOW {preprocess_sparse_balance_window}\n\
#define GRAPH_PREPROCESS_DENSE_THROUGHPUT_SCALE_PCT {preprocess_dense_throughput_scale_pct}\n\
#define GRAPH_PREPROCESS_SPARSE_THROUGHPUT_SCALE_PCT {preprocess_sparse_throughput_scale_pct}\n\
\n\
#define DISTANCE_BITWIDTH {distance_bits}\n\
#define DISTANCE_INTEGER_PART {distance_int_bits}\n\
#define DISTANCE_SIGNED {distance_signed}\n\
#define DIST_PER_WORD {dist_per_word}\n\
#define LOG_DIST_PER_WORD {log_dist_per_word}\n\
#define DISTANCES_PER_REDUCE_WORD {distances_per_reduce_word}\n\
\n\
#define EDGE_PROP_BITS {edge_prop_bits}\n\
{edge_prop_widths_block}\
\n\
#define LITTLE_KERNEL_HBM_EDGE_ID {{{little_edge}}}\n\
#define LITTLE_KERNEL_HBM_NODE_ID {{{little_node}}}\n\
#define BIG_KERNEL_HBM_EDGE_ID {{{big_edge}}}\n\
#define BIG_KERNEL_HBM_NODE_ID {{{big_node}}}\n\
\n\
// HBM bank IDs used by system.cfg (see hls_codegen.rs).
// Keep these in sync with the `sp=...:HBM[...]` mappings.
#define WRITER_OUTPUT_HBM_ID {writer_output_hbm}\n\
#define APPLY_KERNEL_NODE_HBM_ID 30\n\
#define APPLY_KERNEL_HAS_AUX_NODE_PROPS {apply_has_aux}\n\
{apply_aux_hbm_block}\
\n\
#endif /* __HOST_CONFIG_H__ */\n",
        big_kernels = config.kernel.big_kernels,
        little_kernels = config.kernel.little_kernels,
        num_little_mergers = config.topology.little_groups.len(),
        num_big_mergers = config.topology.big_groups.len(),
        little_group_lengths = little_group_lengths,
        little_group_offsets = little_group_offsets,
        big_group_lengths = big_group_lengths,
        big_group_offsets = big_group_offsets,
        little_group_ids = little_group_ids,
        big_group_ids = big_group_ids,
        max_dst_big = config.max_dst_big,
        max_dst_little = config.max_dst_little,
        local_id_bits = config.edge.local_id_bits,
        preprocess_dense_partitions_per_group = config.preprocess_dense_partitions_per_group,
        preprocess_sparse_partitions_per_group = config.preprocess_sparse_partitions_per_group,
        preprocess_dense_balance_window = config.preprocess_dense_balance_window,
        preprocess_sparse_balance_window = config.preprocess_sparse_balance_window,
        preprocess_dense_throughput_scale_pct = config.preprocess_dense_throughput_scale_pct,
        preprocess_sparse_throughput_scale_pct = config.preprocess_sparse_throughput_scale_pct,
        distance_bits = config.node.node_prop_bits,
        distance_int_bits = config.node.node_prop_int_bits,
        distance_signed = if config.node.node_prop_signed { 1 } else { 0 },
        dist_per_word = config.node.dist_per_word,
        log_dist_per_word = config.node.log_dist_per_word,
        distances_per_reduce_word = config.node.distances_per_reduce_word,
        edge_prop_bits = edge_prop_bits,
        edge_prop_widths_block = edge_prop_widths_block,
        little_edge = little_edge,
        little_node = little_node,
        big_edge = big_edge,
        big_node = big_node,
        writer_output_hbm = writer_output_hbm,
        apply_has_aux = if apply_needs_aux { 1 } else { 0 },
        apply_aux_hbm_block = if apply_needs_aux {
            "#define APPLY_KERNEL_AUX_NODE_HBM_ID 31\n"
        } else {
            ""
        }
    ))
}

fn group_offsets(groups: &[KernelGroup]) -> Vec<u32> {
    let mut offsets = Vec::with_capacity(groups.len());
    let mut current = 0u32;
    for group in groups {
        offsets.push(current);
        current += group.pipelines as u32;
    }
    offsets
}

fn expand_group_ids(groups: &[KernelGroup]) -> Vec<u32> {
    let mut out = Vec::new();
    for (gid, group) in groups.iter().enumerate() {
        out.extend(std::iter::repeat(gid as u32).take(group.pipelines));
    }
    out
}

fn build_node_config(
    program: &Program,
    spec: &HlsAlgorithmSpec,
) -> Result<HlsNodeConfig, HlsProjectError> {
    let node_schema = program.schema.node.as_ref().ok_or_else(|| {
        HlsProjectError::InvalidConfig("node schema is required for HLS".to_string())
    })?;
    let target_prop = &program.algorithm.return_stmt.property;
    let prop_def = node_schema
        .properties
        .iter()
        .find(|prop| prop.name == *target_prop)
        .ok_or_else(|| {
            HlsProjectError::InvalidConfig(format!(
                "node property '{}' not found in schema",
                target_prop
            ))
        })?;

    let (node_prop_bits, mut node_prop_int_bits, mut node_prop_signed) = match &prop_def.ty {
        crate::domain::ast::TypeExpr::Int { width } => (*width, *width, true),
        crate::domain::ast::TypeExpr::Fixed { width, int_width } => (*width, *int_width, true),
        crate::domain::ast::TypeExpr::Bool => (1, 1, false),
        crate::domain::ast::TypeExpr::Float => (32, 16, true),
        _ => {
            return Err(HlsProjectError::InvalidConfig(
                "node properties must be scalar int/fixed/bool/float types".to_string(),
            ));
        }
    };

    let is_ddr = program
        .hls
        .as_ref()
        .map(|h| h.memory == crate::domain::ast::MemoryBackend::Ddr)
        .unwrap_or(false);
    if is_ddr && spec.kind == HlsAlgorithmKind::Wcc {
        // FIX: DDR WCC kernels follow the SG reference design, which carries
        // labels as unsigned pods with a zero identity instead of signed
        // 32/32 "distance" values.
        node_prop_signed = false;
        node_prop_int_bits = node_prop_int_bits.min(16).min(node_prop_bits);
    }

    if node_prop_bits == 0 {
        return Err(HlsProjectError::InvalidConfig(
            "node property bitwidth cannot be zero".to_string(),
        ));
    }
    if node_prop_int_bits == 0 || node_prop_int_bits > node_prop_bits {
        return Err(HlsProjectError::InvalidConfig(format!(
            "node property integer bits {node_prop_int_bits} must be in [1, {node_prop_bits}]"
        )));
    }

    let max_per_word = AXI_BUS_WIDTH / node_prop_bits;
    if max_per_word < 8 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "node property width {node_prop_bits} bits is too large for 512-bit packing"
        )));
    }

    let dist_per_word = largest_power_of_two_leq(max_per_word);
    let log_dist_per_word = dist_per_word.trailing_zeros();
    let distances_per_reduce_word = dist_per_word / 8;
    if distances_per_reduce_word == 0 {
        return Err(HlsProjectError::InvalidConfig(
            "distance packing cannot produce reduce words".to_string(),
        ));
    }

    Ok(HlsNodeConfig {
        node_prop_bits,
        node_prop_int_bits,
        node_prop_signed,
        dist_per_word,
        log_dist_per_word,
        distances_per_reduce_word,
    })
}

fn build_edge_config(
    program: &Program,
    ops: &KernelOpBundle,
    node: &HlsNodeConfig,
) -> Result<HlsEdgeConfig, HlsProjectError> {
    let mut edge_prop_widths = Vec::new();
    let mut edge_prop_bits = 0u32;
    let mut edge_weight_bits = 0u32;
    let mut edge_weight_lsb = 0u32;
    let mut edge_weight_shift = None;

    if let Some(edge_def) = &program.schema.edge {
        for prop in &edge_def.properties {
            let width = typeexpr_bitwidth(&prop.ty)?;
            if width == 0 {
                return Err(HlsProjectError::InvalidConfig(format!(
                    "edge property '{}' has zero width",
                    prop.name
                )));
            }
            if width > 64 {
                return Err(HlsProjectError::InvalidConfig(format!(
                    "edge property '{}' width {width} exceeds 64 bits",
                    prop.name
                )));
            }
            if prop.name.as_str() == "weight" {
                edge_weight_bits = width;
                edge_weight_lsb = edge_prop_bits;
                edge_weight_shift = Some(edge_weight_shift_for_type(
                    &prop.ty,
                    node.node_prop_bits,
                    node.node_prop_int_bits,
                )?);
            }
            edge_prop_widths.push(width);
            edge_prop_bits += width;
        }
    }

    let memory = program.hls.as_ref().map(|h| h.memory).unwrap_or_default();
    let compact_edge_payload = memory == crate::domain::ast::MemoryBackend::Ddr;
    let local_id_bits = program.hls.as_ref().map(|h| h.local_id_bits).unwrap_or(32);
    if compact_edge_payload && local_id_bits > 32 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "DDR local_id_bits {local_id_bits} exceeds 32 packed destination bits"
        )));
    }
    if compact_edge_payload && edge_prop_bits > 0 && local_id_bits + edge_prop_bits > 32 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "DDR compact edge payload cannot fit local_id_bits ({local_id_bits}) + edge_prop_bits ({edge_prop_bits}) into 32 bits"
        )));
    }
    let payload_bits = if compact_edge_payload {
        64
    } else {
        edge_prop_bits + 64
    };
    if payload_bits == 0 {
        return Err(HlsProjectError::InvalidConfig(
            "edge payload width cannot be zero".to_string(),
        ));
    }
    if payload_bits > 512 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "edge payload width {payload_bits} exceeds AXI bus width 512"
        )));
    }
    let edges_per_word = 512 / payload_bits;
    if edges_per_word == 0 {
        return Err(HlsProjectError::InvalidConfig(format!(
            "edge payload width {payload_bits} yields zero edges per word"
        )));
    }

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
        _ => {
            return Err(HlsProjectError::InvalidConfig(format!(
                "unsupported big PE count {big_pe}"
            )));
        }
    };

    let little_pe = edges_per_word.max(1);

    if scatter_uses_edge_weight(ops) {
        if edge_weight_bits == 0 {
            return Err(HlsProjectError::InvalidConfig(
                "scatter uses edge weight but schema has no 'weight' property".to_string(),
            ));
        }
        if edge_weight_shift.is_none() {
            return Err(HlsProjectError::InvalidConfig(
                "edge weight type could not be mapped to fixed-point shift".to_string(),
            ));
        }
    }

    let zero_sentinel = program
        .hls
        .as_ref()
        .map(|h| h.zero_sentinel)
        .unwrap_or(true);

    Ok(HlsEdgeConfig {
        edge_prop_bits,
        edge_prop_widths,
        edge_weight_bits,
        edge_weight_lsb,
        edge_weight_shift: edge_weight_shift.unwrap_or(0),
        edges_per_word,
        big_pe,
        big_log_pe,
        little_pe,
        local_id_bits,
        compact_edge_payload,
        zero_sentinel,
        allow_scatter_inf_overflow_to_zero: program
            .hls
            .as_ref()
            .map(|h| h.memory == crate::domain::ast::MemoryBackend::Ddr)
            .unwrap_or(false),
    })
}

fn scatter_uses_edge_weight(ops: &KernelOpBundle) -> bool {
    operator_expr_uses_edge_weight(&ops.scatter.expr)
}

fn operator_expr_uses_edge_weight(expr: &OperatorExpr) -> bool {
    match expr {
        OperatorExpr::Operand(OperatorOperand::ScatterEdgeWeight) => true,
        OperatorExpr::Operand(_) => false,
        OperatorExpr::Unary { expr, .. } => operator_expr_uses_edge_weight(expr),
        OperatorExpr::Binary { left, right, .. } => {
            operator_expr_uses_edge_weight(left) || operator_expr_uses_edge_weight(right)
        }
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            operator_expr_uses_edge_weight(condition)
                || operator_expr_uses_edge_weight(then_expr)
                || operator_expr_uses_edge_weight(else_expr)
        }
    }
}

fn typeexpr_bitwidth(ty: &crate::domain::ast::TypeExpr) -> Result<u32, HlsProjectError> {
    use crate::domain::ast::TypeExpr;
    match ty {
        TypeExpr::Int { width } => Ok(*width),
        TypeExpr::Float => Ok(32),
        TypeExpr::Fixed { width, .. } => Ok(*width),
        TypeExpr::Bool => Ok(1),
        TypeExpr::Tuple(items) => items
            .iter()
            .map(typeexpr_bitwidth)
            .try_fold(0u32, |acc, w| Ok(acc + w?)),
        TypeExpr::Vector { element, len } => {
            let inner = typeexpr_bitwidth(element)?;
            Ok(inner * *len)
        }
        TypeExpr::Matrix {
            element,
            rows,
            cols,
        } => {
            let inner = typeexpr_bitwidth(element)?;
            Ok(inner * *rows * *cols)
        }
        TypeExpr::Array(_) | TypeExpr::Set(_) => Err(HlsProjectError::InvalidConfig(
            "edge properties must be fixed-size scalar/tuple/vector/matrix types".to_string(),
        )),
    }
}

fn edge_weight_shift_for_type(
    ty: &crate::domain::ast::TypeExpr,
    dist_bits: u32,
    dist_int_bits: u32,
) -> Result<i32, HlsProjectError> {
    use crate::domain::ast::TypeExpr;
    let dist_frac_bits = dist_bits.saturating_sub(dist_int_bits);
    match ty {
        TypeExpr::Int { .. } | TypeExpr::Bool => Ok(dist_frac_bits as i32),
        TypeExpr::Fixed { width, int_width } => {
            let frac_bits = width.saturating_sub(*int_width);
            Ok(dist_frac_bits as i32 - frac_bits as i32)
        }
        TypeExpr::Float => Err(HlsProjectError::InvalidConfig(
            "edge weight float type is not supported for HLS scatter".to_string(),
        )),
        TypeExpr::Tuple(_)
        | TypeExpr::Vector { .. }
        | TypeExpr::Matrix { .. }
        | TypeExpr::Array(_)
        | TypeExpr::Set(_) => Err(HlsProjectError::InvalidConfig(
            "edge weight must be a scalar int/bool/fixed type".to_string(),
        )),
    }
}

fn join_ints(values: &[u32]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_vec_init(values: &[u32], fallback: u32) -> String {
    let joined = join_ints(values);
    if joined.is_empty() {
        fallback.to_string()
    } else {
        joined
    }
}

fn nonempty_array_init(values: &str, fallback: &str) -> String {
    if values.trim().is_empty() {
        fallback.to_string()
    } else {
        values.to_string()
    }
}

fn largest_power_of_two_leq(value: u32) -> u32 {
    if value == 0 {
        return 0;
    }
    1u32 << (31 - value.leading_zeros())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use crate::engine::{gas_lower::lower_to_gas, ir_builder::LoweredProgram};

    use super::*;

    fn assert_files_match(
        actual_path: &Path,
        expected_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let actual = fs::read(actual_path)?;
        let expected = fs::read(expected_path)?;
        assert_eq!(
            actual,
            expected,
            "file contents diverged for {} vs {}",
            actual_path.display(),
            expected_path.display()
        );
        Ok(())
    }

    #[test]
    fn builds_sssp_project_tree() -> Result<(), Box<dyn std::error::Error>> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_path = manifest.join("apps/sssp.dsl");
        let source = fs::read_to_string(&source_path)?;
        let lowered = LoweredProgram::parse_and_lower(&source)?;
        let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;

        let dest_root = manifest.join("target/sssp_hls_fixture");
        if dest_root.exists() {
            fs::remove_dir_all(&dest_root)?;
        }

        let output_root = generate_sssp_hls_project(&gas, &lowered.ast, &dest_root)?;
        let expected_files = [
            "Makefile",
            "global_para.mk",
            "run.sh",
            "system.cfg",
            "scripts/main.mk",
            "scripts/utils.mk",
            "scripts/kernel/graphyflow_big.cpp",
            "scripts/kernel/graphyflow_big.h",
            "scripts/kernel/graphyflow_little.cpp",
            "scripts/kernel/graphyflow_little.h",
            "scripts/kernel/apply_kernel.cpp",
            "scripts/kernel/shared_kernel_params.h",
            "scripts/kernel/big_merger.cpp",
            "scripts/kernel/little_merger.cpp",
            "scripts/kernel/hbm_writer.cpp",
            "scripts/host/host.cpp",
            "scripts/host/graph_loader.cpp",
            "scripts/host/host_config.h",
            "scripts/host/generated_algorithm_config.h",
            "scripts/host/host.mk",
        ];
        for relative in expected_files {
            let path = output_root.join(relative);
            assert!(path.exists(), "missing generated artifact: {:?}", path);
        }

        let big_header = fs::read_to_string(output_root.join("scripts/kernel/graphyflow_big.h"))?;
        let little_header =
            fs::read_to_string(output_root.join("scripts/kernel/graphyflow_little.h"))?;
        let generated_host =
            fs::read_to_string(output_root.join("scripts/host/generated_host.cpp"))?;
        assert!(
            big_header.contains("typedef ap_uint<LOCAL_ID_BITWIDTH> local_id_t;"),
            "big header must define local_id_t"
        );
        assert!(
            big_header.contains("local_id_t dst_id;") && big_header.contains("local_id_t node_id;"),
            "big header must use local_id_t for width-sensitive fields"
        );
        assert!(
            little_header.contains("local_id_t dst_id;")
                && little_header.contains("local_id_t node_id;"),
            "little header must use local_id_t for width-sensitive fields"
        );
        assert!(
            !big_header.contains("ap_uint<20> dst_id")
                && !little_header.contains("ap_uint<20> dst_id"),
            "generated headers must not contain hardcoded 20-bit dst_id fields"
        );
        assert!(
            generated_host.contains("packed_edge_props.size() *")
                && generated_host.contains("static_cast<size_t>(edges_per_word)"),
            "generated host must derive kernel edge counts from packed edge words"
        );

        fs::remove_dir_all(&output_root)?;
        Ok(())
    }

    #[test]
    fn emits_static_ddr_weighted_sssp_project() -> Result<(), Box<dyn std::error::Error>> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_path = manifest.join("apps/topology_variants/sssp_ddr_4b4l_codegen.dsl");
        let source = fs::read_to_string(&source_path)?;
        let lowered = LoweredProgram::parse_and_lower(&source)?;
        let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;

        let dest_root = manifest.join("target/sssp_ddr_weighted_hls_fixture");
        if dest_root.exists() {
            fs::remove_dir_all(&dest_root)?;
        }

        let output_root = generate_sssp_hls_project(&gas, &lowered.ast, &dest_root)?;
        let expected_root = manifest.join("src/hls_assets/scripts_ddr/kernel_sssp");

        let expected_files = [
            ("scripts/kernel/graphyflow_big.cpp", "graphyflow_big.cpp"),
            ("scripts/kernel/graphyflow_big.h", "graphyflow_big.h"),
            (
                "scripts/kernel/graphyflow_little.cpp",
                "graphyflow_little.cpp",
            ),
            ("scripts/kernel/graphyflow_little.h", "graphyflow_little.h"),
            (
                "scripts/kernel/shared_kernel_params.h",
                "shared_kernel_params.h",
            ),
            ("scripts/kernel/big_merger.cpp", "big_merger.cpp"),
            ("scripts/kernel/little_merger.cpp", "little_merger.cpp"),
            ("scripts/kernel/kernel.mk", "kernel.mk"),
        ];
        for (actual_rel, expected_rel) in expected_files {
            let path = output_root.join(actual_rel);
            assert!(path.exists(), "missing generated artifact: {:?}", path);
            assert_files_match(&path, &expected_root.join(expected_rel))?;
        }

        let big_header = fs::read_to_string(output_root.join("scripts/kernel/graphyflow_big.h"))?;
        let little_header =
            fs::read_to_string(output_root.join("scripts/kernel/graphyflow_little.h"))?;
        let big_cpp = fs::read_to_string(output_root.join("scripts/kernel/graphyflow_big.cpp"))?;
        let little_cpp =
            fs::read_to_string(output_root.join("scripts/kernel/graphyflow_little.cpp"))?;

        for content in [&big_header, &little_header] {
            assert!(
                !content.contains("NODE_ID_BITWIDTH + NODE_ID_BITWIDTH + EDGE_PROP_BITS"),
                "emitted DDR weighted SSSP header regressed to 74-bit edge packing",
            );
            assert!(
                !content.contains("~ap_fixed_pod_t(0)"),
                "emitted DDR weighted SSSP header regressed to max-uint infinity",
            );
        }
        for content in [&big_cpp, &little_cpp] {
            assert!(
                !content.contains("range(73u, 64u)"),
                "emitted DDR weighted SSSP kernel regressed to 74-bit weight extraction",
            );
        }

        fs::remove_dir_all(&output_root)?;
        Ok(())
    }

    #[test]
    fn emits_unsigned_kernel_labels_for_ddr_wcc_project() -> Result<(), Box<dyn std::error::Error>>
    {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_path = manifest.join("apps/topology_variants/wcc_ddr_4b4l.dsl");
        let source = fs::read_to_string(&source_path)?;
        let lowered = LoweredProgram::parse_and_lower(&source)?;
        let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;

        let dest_root = manifest.join("target/wcc_ddr_hls_fixture");
        if dest_root.exists() {
            fs::remove_dir_all(&dest_root)?;
        }

        let output_root = generate_sssp_hls_project(&gas, &lowered.ast, &dest_root)?;
        let big_header = fs::read_to_string(output_root.join("scripts/kernel/graphyflow_big.h"))?;
        let little_header =
            fs::read_to_string(output_root.join("scripts/kernel/graphyflow_little.h"))?;
        let shared = fs::read_to_string(output_root.join("scripts/kernel/shared_kernel_params.h"))?;

        for content in [&big_header, &little_header] {
            assert!(
                content.contains("#define DISTANCE_SIGNED 0"),
                "DDR WCC kernel headers must use unsigned label pods"
            );
            assert!(
                content.contains("#define DISTANCE_INTEGER_PART 16"),
                "DDR WCC kernel headers must keep the reference 32/16 label layout"
            );
        }
        assert!(
            shared.contains("#define DISTANCE_INTEGER_PART 16"),
            "DDR WCC shared params must keep the reference 32/16 label layout"
        );
        assert!(
            shared.contains("using ap_fixed_pod_t = ap_uint<DISTANCE_BITWIDTH>;"),
            "DDR WCC shared params must use unsigned label pods"
        );
        assert!(
            shared.contains("const ap_fixed_pod_t NEG_INFINITY_POD = 0u;")
                || shared.contains("const ap_fixed_pod_t NEG_INFINITY_POD = 0;"),
            "DDR WCC shared params must use zero as the kernel empty-label identity"
        );
        assert!(
            !shared.contains("2147483648u"),
            "DDR WCC shared params must not emit signed min-value identities"
        );

        fs::remove_dir_all(&output_root)?;
        Ok(())
    }
}
