use std::fs;
use std::path::{Path, PathBuf};

use refactor_Graphyflow::domain::hls_ops::KernelOpBundle;
use refactor_Graphyflow::domain::hls_template::{
    HlsEdgeConfig, HlsKernelConfig, apply_kernel_unit, big_merger_unit, graphyflow_big_unit,
    graphyflow_little_unit, hbm_writer_unit, little_merger_unit, render_graphyflow_big_header,
    render_graphyflow_little_header,
};
use refactor_Graphyflow::engine::gas_lower::lower_to_gas;
use refactor_Graphyflow::engine::gas_to_hls_ops::extract_kernel_ops;
use refactor_Graphyflow::engine::ir_builder::LoweredProgram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);

    write_sssp_goldens(&manifest_dir)?;
    write_bundle_goldens(&manifest_dir, "connected_components", "scripts_cc")?;
    write_bundle_goldens(&manifest_dir, "pagerank", "scripts_pr")?;

    eprintln!("Regenerated HLS golden kernel sources under src/hls_assets/.");
    Ok(())
}

fn write_sssp_goldens(manifest_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let ops = KernelOpBundle::sssp_default();
    let edge = edge_config_for_app(manifest_dir, "sssp")?;
    let node = node_config_for_app(manifest_dir, "sssp")?;

    let kernel_root = manifest_dir
        .join("src")
        .join("hls_assets")
        .join("scripts")
        .join("kernel");

    write_file(
        &kernel_root.join("graphyflow_big.cpp"),
        graphyflow_big_unit(&ops, &edge)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("graphyflow_big.h"),
        render_graphyflow_big_header(&edge, &node, 524_288),
    )?;
    write_file(
        &kernel_root.join("graphyflow_little.cpp"),
        graphyflow_little_unit(&ops, &edge)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("graphyflow_little.h"),
        render_graphyflow_little_header(&edge, &node, 65_536),
    )?;
    write_file(
        &kernel_root.join("apply_kernel.cpp"),
        apply_kernel_unit(&ops)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("big_merger.cpp"),
        big_merger_unit(&ops, &HlsKernelConfig::default(), edge.zero_sentinel)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("little_merger.cpp"),
        little_merger_unit(&ops, &HlsKernelConfig::default(), edge.zero_sentinel)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("hbm_writer.cpp"),
        hbm_writer_unit(&HlsKernelConfig::default())?.to_code(),
    )?;

    Ok(())
}

fn write_bundle_goldens(
    manifest_dir: &Path,
    app: &str,
    bundle_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let ops = ops_for_app(manifest_dir, app)?;
    let edge = edge_config_for_app(manifest_dir, app)?;
    let node = node_config_for_app(manifest_dir, app)?;

    let kernel_root = manifest_dir
        .join("src")
        .join("hls_assets")
        .join(bundle_dir)
        .join("kernel");

    write_file(
        &kernel_root.join("graphyflow_big.cpp"),
        graphyflow_big_unit(&ops, &edge)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("graphyflow_big.h"),
        render_graphyflow_big_header(&edge, &node, 524_288),
    )?;
    write_file(
        &kernel_root.join("graphyflow_little.cpp"),
        graphyflow_little_unit(&ops, &edge)?.to_code(),
    )?;
    write_file(
        &kernel_root.join("graphyflow_little.h"),
        render_graphyflow_little_header(&edge, &node, 65_536),
    )?;
    write_file(
        &kernel_root.join("apply_kernel.cpp"),
        apply_kernel_unit(&ops)?.to_code(),
    )?;

    Ok(())
}

fn write_file(path: &Path, contents: String) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)?;
    Ok(())
}

fn ops_for_app(
    manifest_dir: &Path,
    app: &str,
) -> Result<KernelOpBundle, Box<dyn std::error::Error>> {
    let source_path = manifest_dir.join("apps").join(format!("{app}.dsl"));
    let source = fs::read_to_string(&source_path)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;
    Ok(extract_kernel_ops(&gas)?)
}

fn edge_config_for_app(
    manifest_dir: &Path,
    app: &str,
) -> Result<HlsEdgeConfig, Box<dyn std::error::Error>> {
    let source_path = manifest_dir.join("apps").join(format!("{app}.dsl"));
    let source = fs::read_to_string(&source_path)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;

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
        .map(|h| h.memory == refactor_Graphyflow::domain::ast::MemoryBackend::Ddr)
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
        _ => return Err(format!("unsupported big PE count {big_pe}").into()),
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

    Ok(HlsEdgeConfig {
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
        allow_scatter_inf_overflow_to_zero: compact_edge_payload,
    })
}

fn node_config_for_app(
    manifest_dir: &Path,
    app: &str,
) -> Result<refactor_Graphyflow::domain::hls_template::HlsNodeConfig, Box<dyn std::error::Error>> {
    let source_path = manifest_dir.join("apps").join(format!("{app}.dsl"));
    let source = fs::read_to_string(&source_path)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;

    let target_prop = lowered.ast.algorithm.return_stmt.property.as_str();
    let node_prop_ty = lowered
        .ast
        .schema
        .node
        .as_ref()
        .and_then(|node| {
            node.properties
                .iter()
                .find(|prop| prop.name.as_str() == target_prop)
        })
        .map(|prop| &prop.ty)
        .ok_or_else(|| format!("app '{app}' is missing target node property '{target_prop}'"))?;

    let (node_prop_bits, node_prop_int_bits, node_prop_signed) = match node_prop_ty {
        refactor_Graphyflow::domain::ast::TypeExpr::Int { width } => (*width, *width, false),
        refactor_Graphyflow::domain::ast::TypeExpr::Float => (32, 16, true),
        refactor_Graphyflow::domain::ast::TypeExpr::Fixed { width, int_width } => {
            (*width, *int_width, true)
        }
        refactor_Graphyflow::domain::ast::TypeExpr::Bool => (1, 1, false),
        other => {
            return Err(
                format!("unsupported node property type for HLS header regen: {other:?}").into(),
            );
        }
    };

    let dist_per_word: u32 = 512 / node_prop_bits;
    let log_dist_per_word = if dist_per_word > 0 {
        dist_per_word.ilog2()
    } else {
        0
    };
    let distances_per_reduce_word: u32 = 64 / node_prop_bits;

    Ok(refactor_Graphyflow::domain::hls_template::HlsNodeConfig {
        node_prop_bits,
        node_prop_int_bits,
        node_prop_signed,
        dist_per_word,
        log_dist_per_word,
        distances_per_reduce_word,
    })
}

fn typeexpr_bitwidth(ty: &refactor_Graphyflow::domain::ast::TypeExpr) -> u32 {
    use refactor_Graphyflow::domain::ast::TypeExpr;
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
        TypeExpr::Array(_) | TypeExpr::Set(_) => panic!("edge properties must be fixed-size types"),
    }
}

fn edge_weight_shift_for_type(ty: &refactor_Graphyflow::domain::ast::TypeExpr) -> i32 {
    use refactor_Graphyflow::domain::ast::TypeExpr;
    const DISTANCE_BITWIDTH: u32 = 32;
    const DISTANCE_INTEGER_PART: u32 = 16;
    let dist_frac_bits = DISTANCE_BITWIDTH - DISTANCE_INTEGER_PART;
    match ty {
        TypeExpr::Int { .. } | TypeExpr::Bool => dist_frac_bits as i32,
        TypeExpr::Fixed { width, int_width } => {
            let frac_bits = width.saturating_sub(*int_width);
            dist_frac_bits as i32 - frac_bits as i32
        }
        TypeExpr::Float => panic!("edge weight float type is not supported for HLS scatter"),
        TypeExpr::Tuple(_)
        | TypeExpr::Vector { .. }
        | TypeExpr::Matrix { .. }
        | TypeExpr::Array(_)
        | TypeExpr::Set(_) => panic!("edge weight must be a scalar int/bool/fixed type"),
    }
}
