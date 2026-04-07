use std::{
    collections::HashMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use refactor_Graphyflow::{
    domain::{DebugSummary, gas::GasType},
    engine::{
        gas_lower::typeexpr_to_gastype,
        gas_simulator::{
            GasSimError, GraphInput, GraphState, Value, load_graph_from_json,
            simulate_gas_for_iters, simulate_gas_for_iters_measure_only_timed,
        },
        ir_builder::LoweredProgram,
    },
    generate_graph, generate_sssp_hls_project, lower_to_gas, parse_program,
    utils::{
        graph_converter::convert_graph,
        graph_generator::{AppKind, GraphSpec},
        graph_metadata::{
            extract_graph_metadata_from_dataset_path, extract_graph_metadata_from_profile_log,
        },
        grouping_predictor::{
            emit_saved_dataset_groupings, evaluate_metadata_predictor, load_static_grouping_model,
            predict_best_grouping, predict_best_grouping_from_metadata,
            predict_best_grouping_from_static_model,
            predict_best_grouping_from_static_model_for_dataset, save_static_grouping_model,
            train_static_grouping_model,
        },
        grouping_predictor_32::{
            evaluate_grouping32_model, load_static_grouping32_model,
            predict_grouping32_from_static_model_for_dataset, save_static_grouping32_model,
            train_static_grouping32_model,
        },
        reference_calcs::{reference_values, reference_values_with_iters},
    },
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let Some(first_arg) = args.next() else {
        print_usage();
        return Ok(());
    };

    if first_arg == "--generate" {
        let app = args
            .next()
            .ok_or_else(|| format!("missing app name"))?
            .parse::<AppKind>()?;
        let nodes = args
            .next()
            .ok_or_else(|| format!("missing node count"))?
            .parse::<usize>()
            .map_err(|e| format!("invalid node count: {e}"))?;
        let edges = args
            .next()
            .ok_or_else(|| format!("missing edge count"))?
            .parse::<usize>()
            .map_err(|e| format!("invalid edge count: {e}"))?;
        let seed = args.next().and_then(|raw| raw.parse::<u64>().ok());
        let spec = if let Some(seed) = seed {
            GraphSpec::new(nodes, edges).with_seed(seed)
        } else {
            GraphSpec::new(nodes, edges)
        };
        let graph = generate_graph(app, spec)?;
        let json = serde_json::to_string_pretty(&graph)?;
        println!("{json}");
        return Ok(());
    }

    if first_arg == "--convert-graph" {
        let input_path = args
            .next()
            .ok_or_else(|| "missing input graph path".to_string())?;
        let output_path = args
            .next()
            .ok_or_else(|| "missing output json path".to_string())?;
        let dsl_name = args.next().ok_or_else(|| {
            "missing dsl/app name (e.g. sssp, pagerank, connected_components)".to_string()
        })?;
        let app = dsl_name.parse::<AppKind>()?;
        let graph = convert_graph(Path::new(&input_path), &app)?;
        let json = serde_json::to_string(&graph)?;
        fs::write(&output_path, json)?;
        eprintln!(
            "converted {} -> {} ({} nodes, {} edges, dsl={})",
            input_path,
            output_path,
            graph.nodes.len(),
            graph.edges.len(),
            dsl_name
        );
        return Ok(());
    }

    if first_arg == "--simulate-json" {
        let app = args
            .next()
            .ok_or_else(|| format!("missing app name or dsl path"))?;
        let graph_path = args
            .next()
            .ok_or_else(|| format!("missing graph json path"))?;
        let max_iters = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid max iters: {e}"))
            })
            .transpose()?;
        simulate_from_json(&app, &graph_path, max_iters)?;
        return Ok(());
    }

    if first_arg == "--simulate-raw" {
        let app = args
            .next()
            .ok_or_else(|| format!("missing app name or dsl path"))?;
        let graph_path = args
            .next()
            .ok_or_else(|| format!("missing raw graph path"))?;
        let convert_app = args
            .next()
            .ok_or_else(|| format!("missing convert app name"))?;
        let max_iters = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid max iters: {e}"))
            })
            .transpose()?;
        simulate_from_raw(&app, &graph_path, &convert_app, max_iters)?;
        return Ok(());
    }

    if first_arg == "--emit-hls" {
        let app = args
            .next()
            .ok_or_else(|| format!("missing app name or dsl path"))?;
        let dest_root = args.next().map(PathBuf::from);
        emit_hls_project(&app, dest_root)?;
        return Ok(());
    }

    if first_arg == "--predict-grouping-for-dataset" {
        let dataset_path = args
            .next()
            .ok_or_else(|| "missing dataset path".to_string())?;
        let model_path = args.next().map(PathBuf::from);
        let model_path = model_path.unwrap_or(default_static_grouping_model_path()?);
        let prediction = predict_best_grouping_from_static_model_for_dataset(
            &model_path,
            Path::new(&dataset_path),
        )?;
        println!("recommended variant: {}", prediction.recommended_variant);
        println!("recommended family: {}", prediction.recommended_family);
        println!(
            "recommended grouping: big=[{}] little=[{}]",
            prediction
                .recommended_big_groups
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            prediction
                .recommended_little_groups
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        return Ok(());
    }

    if first_arg == "--auto-emit-sssp-bw8" {
        let dataset_path = args
            .next()
            .ok_or_else(|| "missing dataset path".to_string())?;
        let dest_root = args.next().map(PathBuf::from);
        let model_path = args.next().map(PathBuf::from);
        auto_emit_predicted_sssp_bw8_hls_cmd(&dataset_path, dest_root, model_path)?;
        return Ok(());
    }

    if first_arg == "--auto-emit-hls-from-dsl" {
        let dsl_path = args.next().ok_or_else(|| "missing DSL path".to_string())?;
        let dataset_path = args
            .next()
            .ok_or_else(|| "missing dataset path".to_string())?;
        let dest_root = args.next().map(PathBuf::from);
        let model_path = args.next().map(PathBuf::from);
        auto_emit_hls_from_dsl_cmd(&dsl_path, &dataset_path, dest_root, model_path)?;
        return Ok(());
    }

    if first_arg == "--auto-emit-hls-from-dsl-32bit" {
        let dsl_path = args.next().ok_or_else(|| "missing DSL path".to_string())?;
        let dataset_path = args
            .next()
            .ok_or_else(|| "missing dataset path".to_string())?;
        let dest_root = args.next().map(PathBuf::from);
        let model_path = args.next().map(PathBuf::from);
        auto_emit_hls_from_dsl_32bit_cmd(&dsl_path, &dataset_path, dest_root, model_path)?;
        return Ok(());
    }

    if first_arg == "--predict-grouping" {
        let history_root = args
            .next()
            .ok_or_else(|| "missing history log root".to_string())?;
        let probe_log = args
            .next()
            .ok_or_else(|| "missing probe log path".to_string())?;
        let neighbor_count = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid neighbor count: {e}"))
            })
            .transpose()?
            .unwrap_or(5);
        predict_grouping(&history_root, &probe_log, neighbor_count)?;
        return Ok(());
    }

    if first_arg == "--predict-grouping-from-metadata" {
        let history_root = args
            .next()
            .ok_or_else(|| "missing history log root".to_string())?;
        let probe_log = args
            .next()
            .ok_or_else(|| "missing probe log path".to_string())?;
        let neighbor_count = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid neighbor count: {e}"))
            })
            .transpose()?
            .unwrap_or(5);
        predict_grouping_from_metadata_cmd(&history_root, &probe_log, neighbor_count)?;
        return Ok(());
    }

    if first_arg == "--evaluate-grouping-from-metadata" {
        let history_root = args
            .next()
            .ok_or_else(|| "missing history log root".to_string())?;
        let neighbor_count = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid neighbor count: {e}"))
            })
            .transpose()?
            .unwrap_or(5);
        evaluate_grouping_from_metadata_cmd(&history_root, neighbor_count)?;
        return Ok(());
    }

    if first_arg == "--train-static-grouping-model" {
        let history_root = args
            .next()
            .ok_or_else(|| "missing history log root".to_string())?;
        let model_path = args
            .next()
            .ok_or_else(|| "missing output model path".to_string())?;
        let neighbor_count = args
            .next()
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| format!("invalid neighbor count: {e}"))
            })
            .transpose()?
            .unwrap_or(5);
        train_static_grouping_model_cmd(&history_root, &model_path, neighbor_count)?;
        return Ok(());
    }

    if first_arg == "--predict-grouping-from-static-model" {
        let model_path = args
            .next()
            .ok_or_else(|| "missing model path".to_string())?;
        let probe_log = args
            .next()
            .ok_or_else(|| "missing probe log path".to_string())?;
        predict_grouping_from_static_model_cmd(&model_path, &probe_log)?;
        return Ok(());
    }

    if first_arg == "--emit-static-model-datasets" {
        let model_path = args
            .next()
            .ok_or_else(|| "missing model path".to_string())?;
        let output_path = args
            .next()
            .ok_or_else(|| "missing output json path".to_string())?;
        emit_static_model_datasets_cmd(&model_path, &output_path)?;
        return Ok(());
    }

    if first_arg == "--evaluate-grouping32-model" {
        let benchmark_path = args.next().map(PathBuf::from);
        evaluate_grouping32_model_cmd(benchmark_path)?;
        return Ok(());
    }

    if first_arg == "--train-static-grouping32-model" {
        let benchmark_path = args
            .next()
            .ok_or_else(|| "missing benchmark path".to_string())?;
        let model_path = args
            .next()
            .ok_or_else(|| "missing output model path".to_string())?;
        train_static_grouping32_model_cmd(&benchmark_path, &model_path)?;
        return Ok(());
    }

    if first_arg == "--predict-grouping32-from-static-model" {
        let model_path = args
            .next()
            .ok_or_else(|| "missing model path".to_string())?;
        let dataset_path = args
            .next()
            .ok_or_else(|| "missing dataset path".to_string())?;
        predict_grouping32_from_static_model_cmd(&model_path, &dataset_path)?;
        return Ok(());
    }

    let resolved_path = resolve_app_path(&first_arg)?;
    let source = fs::read_to_string(&resolved_path)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;

    println!("AST:\n{}", lowered.ast.debug_summary());
    println!("IR:\n{}", lowered.ir.debug_summary());
    println!("GAS:\n{}", gas.debug_summary());

    Ok(())
}

fn simulate_from_json(
    app_arg: &str,
    graph_path: &str,
    max_iters: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let file = fs::File::open(graph_path)?;
    let input: GraphInput = serde_json::from_reader(file)?;
    simulate_from_input(app_arg, input, max_iters)
}

fn simulate_from_raw(
    app_arg: &str,
    graph_path: &str,
    convert_app: &str,
    max_iters: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let app = convert_app.parse::<AppKind>()?;
    let input = convert_graph(Path::new(graph_path), &app)?;
    simulate_from_input(app_arg, input, max_iters)
}

fn simulate_from_input(
    app_arg: &str,
    input: GraphInput,
    max_iters: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let resolved_path = resolve_app_path(app_arg)?;
    let (gas, node_types, edge_types, target_prop) = load_program_and_types(&resolved_path)?;
    let sim_iters = max_iters.unwrap_or(32);
    let quiet_output = env::var("GRAPHYFLOW_SIM_QUIET")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let skip_reference = env::var("GRAPHYFLOW_SIM_SKIP_REFERENCE")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let measure_only = env::var("GRAPHYFLOW_SIM_MEASURE_ONLY")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let reference_input = (!skip_reference).then(|| input.clone());
    let graph = load_graph_from_json(input, &node_types, &edge_types)?;
    let compute_timing = env::var("GRAPHYFLOW_SIM_TIMING")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if measure_only && max_iters.is_some() {
        let compute_seconds = simulate_gas_for_iters_measure_only_timed(&gas, &graph, sim_iters)?;
        if compute_timing {
            println!("simulation compute time sec: {:.6}", compute_seconds);
        }
        return Ok(());
    }
    let compute_start = compute_timing.then(Instant::now);
    let state = if max_iters.is_some() {
        simulate_gas_for_iters(&gas, &graph, sim_iters)?
    } else {
        match refactor_Graphyflow::simulate_gas(&gas, &graph, sim_iters) {
            Ok(state) => state,
            Err(GasSimError::NoConvergence) => simulate_gas_for_iters(&gas, &graph, sim_iters)?,
            Err(other) => return Err(other.into()),
        }
    };
    if let Some(start) = compute_start {
        println!(
            "simulation compute time sec: {:.6}",
            start.elapsed().as_secs_f64()
        );
    }
    let rendered = (!quiet_output).then(|| render_property(&state, &target_prop));
    if skip_reference {
        if let Some(rendered) = &rendered {
            println!("{}", serde_json::to_string_pretty(rendered)?);
        }
        return Ok(());
    }
    let app_name = app_name_from_arg(app_arg);
    let reference_input =
        reference_input.expect("reference input should exist when reference is enabled");
    let reference_result = if max_iters.is_some() {
        reference_values_with_iters(&app_name, &reference_input, &target_prop, max_iters)
    } else {
        reference_values(&app_name, &reference_input, &target_prop)
    };
    if let Ok(reference) = reference_result {
        let (ok, mismatches) = compare_results(&state, &reference, &target_prop);
        if let Some(rendered) = &rendered {
            println!("{}", serde_json::to_string_pretty(rendered)?);
        }
        if ok {
            println!("reference match: true");
        } else {
            println!("reference match: false");
            if !quiet_output {
                println!("mismatches: {}", serde_json::to_string_pretty(&mismatches)?);
            } else {
                println!(
                    "mismatch_count: {}",
                    mismatches.as_array().map(|items| items.len()).unwrap_or(0)
                );
            }
        }
    } else {
        if let Some(rendered) = &rendered {
            println!("{}", serde_json::to_string_pretty(rendered)?);
        }
    }
    Ok(())
}

fn load_program_and_types(
    path: &Path,
) -> Result<
    (
        refactor_Graphyflow::domain::gas::GasProgram,
        HashMap<String, GasType>,
        HashMap<String, GasType>,
        String,
    ),
    Box<dyn Error>,
> {
    let source = fs::read_to_string(path)?;
    let ast = parse_program(&source)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;
    let gas = lower_to_gas(&ast, &lowered.ir)?;

    let node_types = ast
        .schema
        .node
        .as_ref()
        .map(collect_types)
        .unwrap_or_default();
    let edge_types = ast
        .schema
        .edge
        .as_ref()
        .map(collect_types)
        .unwrap_or_default();
    let target_prop = gas.apply.target_property.as_str().to_string();
    Ok((gas, node_types, edge_types, target_prop))
}

fn collect_types(entity: &refactor_Graphyflow::domain::ast::EntityDef) -> HashMap<String, GasType> {
    entity
        .properties
        .iter()
        .map(|p| (p.name.as_str().to_string(), typeexpr_to_gastype(&p.ty)))
        .collect()
}

fn render_property(state: &GraphState, prop: &str) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for node_id in state.node_ids() {
        if let Some(val) = state.node_prop(node_id, prop) {
            out.insert(node_id.to_string(), value_to_json(val));
        }
    }
    serde_json::Value::Object(out)
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Int(i) => serde_json::Value::from(*i),
        Value::Float(f) => serde_json::Value::from(*f),
        Value::Bool(b) => serde_json::Value::from(*b),
        Value::Tuple(items) | Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        Value::IntSet(set) => {
            serde_json::Value::Array(set.iter().copied().map(serde_json::Value::from).collect())
        }
        Value::Vector(v) => {
            serde_json::Value::Array(v.iter().copied().map(serde_json::Value::from).collect())
        }
        Value::Matrix { rows, cols, data } => serde_json::json!({
            "rows": rows,
            "cols": cols,
            "data": data,
        }),
        Value::NodeRef(id) => serde_json::Value::from(*id),
        Value::EdgeRef { src, dst, .. } => serde_json::json!({"src": src, "dst": dst}),
        Value::Unit => serde_json::Value::Null,
    }
}

fn app_name_from_arg(arg: &str) -> String {
    let path = Path::new(arg);
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| arg.to_string())
}

fn compare_results(
    state: &GraphState,
    reference: &HashMap<i64, Value>,
    prop: &str,
) -> (bool, serde_json::Value) {
    let mut mismatches = Vec::new();
    for (id, expected) in reference {
        match state.node_prop(*id, prop) {
            Some(actual) if expected.approx_eq(actual) => {}
            Some(actual) => mismatches.push(serde_json::json!({
                "id": id,
                "expected": value_to_json(expected),
                "actual": value_to_json(actual),
            })),
            None => mismatches.push(serde_json::json!({
                "id": id,
                "expected": value_to_json(expected),
                "actual": serde_json::Value::Null,
            })),
        }
    }
    (mismatches.is_empty(), serde_json::Value::Array(mismatches))
}

fn print_usage() {
    eprintln!(
        "usage:\n  refactor_Graphyflow <dsl-file | app-name>\n  refactor_Graphyflow --generate <app> <nodes> <edges> [seed]\n  refactor_Graphyflow --convert-graph <input.txt|.mtx> <output.json> <dsl-name>\n  refactor_Graphyflow --simulate-json <app|dsl-path> <graph.json> [max_iters]\n  refactor_Graphyflow --simulate-raw <app|dsl-path> <graph.txt|.mtx> <convert-app> [max_iters]\n  refactor_Graphyflow --emit-hls <app|dsl-path> [output-dir]\n  refactor_Graphyflow --auto-emit-sssp-bw8 <dataset-path> [output-dir] [model.json]\n  refactor_Graphyflow --auto-emit-hls-from-dsl <dsl-path> <dataset-path> [output-dir] [model.json]\n  refactor_Graphyflow --predict-grouping <history-log-root> <probe-log> [neighbors]\n  refactor_Graphyflow --predict-grouping-from-metadata <history-log-root> <probe-log> [neighbors]\n  refactor_Graphyflow --evaluate-grouping-from-metadata <history-log-root> [neighbors]\n  refactor_Graphyflow --train-static-grouping-model <history-log-root> <model.json> [neighbors]\n  refactor_Graphyflow --predict-grouping-from-static-model <model.json> <probe-log>\n  refactor_Graphyflow --emit-static-model-datasets <model.json> <output.json>\n  refactor_Graphyflow --evaluate-grouping32-model [benchmark.tsv]\n  refactor_Graphyflow --train-static-grouping32-model <benchmark.tsv> <model.json>\n  refactor_Graphyflow --predict-grouping32-from-static-model <model.json> <dataset-path>\napps: sssp | bfs | ar | wcc | pagerank | connected_components | graph_coloring | als"
    );
}

fn default_grouping32_benchmark_path() -> Result<PathBuf, Box<dyn Error>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest
        .join("docs")
        .join("grouping32_benchmark_2026-04-04.tsv"))
}

fn evaluate_grouping32_model_cmd(benchmark_path: Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let benchmark_path = benchmark_path.unwrap_or(default_grouping32_benchmark_path()?);
    let summary = evaluate_grouping32_model(&benchmark_path)?;
    println!("32-bit Grouping Evaluation");
    println!("benchmark path: {}", benchmark_path.display());
    println!("cases: {}", summary.dataset_holdout_cases);
    println!("variant accuracy: {:.1}%", summary.variant_accuracy * 100.0);
    println!("average regret: {:.3}", summary.average_regret);
    Ok(())
}

fn train_static_grouping32_model_cmd(
    benchmark_path: &str,
    model_path: &str,
) -> Result<(), Box<dyn Error>> {
    let model = train_static_grouping32_model(Path::new(benchmark_path))?;
    save_static_grouping32_model(&model, Path::new(model_path))?;

    println!("32-bit Static Grouping Model");
    println!("benchmark path: {}", benchmark_path);
    println!("model path: {}", model_path);
    println!("datasets: {}", model.total_datasets);
    println!("candidate variants: {}", model.candidate_shapes.len());
    println!(
        "label kNN: k={} distance_power={:.2}",
        model.label_knn_neighbors, model.label_distance_power
    );
    Ok(())
}

fn predict_grouping32_from_static_model_cmd(
    model_path: &str,
    dataset_path: &str,
) -> Result<(), Box<dyn Error>> {
    let model = load_static_grouping32_model(Path::new(model_path))?;
    let prediction = predict_grouping32_from_static_model_for_dataset(
        Path::new(model_path),
        Path::new(dataset_path),
    )?;
    let metadata = extract_graph_metadata_from_dataset_path(Path::new(dataset_path))
        .ok_or_else(|| format!("failed to extract metadata from {dataset_path}"))?;

    println!("32-bit Static Grouping Prediction");
    println!("model path: {}", model_path);
    println!("dataset path: {}", dataset_path);
    println!(
        "history coverage: {} datasets, {} variants",
        model.total_datasets,
        model.candidate_shapes.len()
    );
    println!(
        "graph metadata: dataset={} format={} domain={} vertices={} edges={} avg_degree={:.3} density={:.3e}",
        metadata.dataset,
        metadata.format,
        metadata.domain,
        metadata.vertices,
        metadata.edges,
        metadata.average_degree,
        metadata.density
    );
    println!("recommended variant: {}", prediction.recommended_variant);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("top candidates:");
    for candidate in &prediction.ranked_candidates {
        println!(
            "  {} big=[{}] little=[{}] vote={:.3} est_throughput={:.3} neighbor_est={} seen={}",
            candidate.variant,
            candidate
                .big_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate
                .little_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate.classifier_probability,
            candidate.score,
            candidate
                .knn_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate.seen_in_training,
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct AutoGroupPlacement {
    pipelines: usize,
    merger_slr: usize,
    pipeline_slr: Vec<usize>,
}

fn predict_grouping(
    history_root: &str,
    probe_log: &str,
    neighbor_count: usize,
) -> Result<(), Box<dyn Error>> {
    let prediction = predict_best_grouping(
        Path::new(history_root),
        Path::new(probe_log),
        neighbor_count,
    )?;

    println!("Grouping Prediction");
    println!("history root: {}", prediction.history_root.display());
    println!("probe log: {probe_log}");
    println!(
        "history coverage: {} runs across {} datasets",
        prediction.total_runs, prediction.total_datasets
    );
    println!("training variants: {}", prediction.training_variants);
    println!("candidate variants: {}", prediction.candidate_variants);
    println!(
        "graph metadata: dataset={} format={} domain={} vertices={} edges={} avg_degree={:.3} density={:.3e}",
        prediction.metadata.dataset,
        prediction.metadata.format,
        prediction.metadata.domain,
        prediction.metadata.vertices,
        prediction.metadata.edges,
        prediction.metadata.average_degree,
        prediction.metadata.density
    );
    println!("recommended variant: {}", prediction.recommended_variant);
    println!("recommended family: {}", prediction.recommended_family);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("top candidates:");
    for candidate in &prediction.ranked_candidates {
        println!(
            "  {} family={} big=[{}] little=[{}] score={:.3} linear={:.3} ranking={:.3} variant_knn={} family_knn={} precise={} seen_variant={} seen_family={}",
            candidate.variant,
            candidate.family,
            candidate
                .big_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate
                .little_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate.score,
            candidate.linear_score,
            candidate.ranking_score,
            candidate
                .variant_knn_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate
                .family_knn_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate
                .precise_partition_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate.variant_seen_in_training,
            candidate.family_seen_in_training,
        );
    }
    Ok(())
}

fn predict_grouping_from_metadata_cmd(
    history_root: &str,
    probe_log: &str,
    neighbor_count: usize,
) -> Result<(), Box<dyn Error>> {
    let prediction = predict_best_grouping_from_metadata(
        Path::new(history_root),
        Path::new(probe_log),
        neighbor_count,
    )?;
    let metadata = extract_graph_metadata_from_profile_log(Path::new(probe_log))
        .ok_or_else(|| format!("failed to extract metadata from {probe_log}"))?;

    println!("Metadata Grouping Prediction");
    println!("history root: {}", prediction.history_root.display());
    println!("probe log: {}", probe_log);
    println!(
        "history coverage: {} runs across {} datasets",
        prediction.total_runs, prediction.total_datasets
    );
    println!("training variants: {}", prediction.training_variants);
    println!("candidate variants: {}", prediction.candidate_variants);
    println!(
        "graph metadata: dataset={} format={} domain={} vertices={} edges={} avg_degree={:.3} density={:.3e}",
        metadata.dataset,
        metadata.format,
        metadata.domain,
        metadata.vertices,
        metadata.edges,
        metadata.average_degree,
        metadata.density
    );
    if let Some(graph_path) = &metadata.graph_path {
        println!("graph metadata: raw_graph_path={}", graph_path.display());
    }
    println!(
        "graph metadata: active_src={:.3} active_dst={:.3} max_out={} max_in={} out_cv={:.3} in_cv={:.3}",
        metadata.active_src_fraction,
        metadata.active_dst_fraction,
        metadata.max_out_degree,
        metadata.max_in_degree,
        metadata.out_degree_cv,
        metadata.in_degree_cv
    );
    if let Some(scale) = metadata.scale_hint {
        println!("graph metadata: scale_hint={scale:.0}");
    }
    if let Some(edge_factor) = metadata.edge_factor_hint {
        println!("graph metadata: edge_factor_hint={edge_factor:.0}");
    }
    println!("recommended variant: {}", prediction.recommended_variant);
    println!("recommended family: {}", prediction.recommended_family);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("top candidates:");
    for candidate in &prediction.ranked_candidates {
        println!(
            "  {} family={} big=[{}] little=[{}] score={:.3} linear={:.3} ranking={:.3} variant_knn={} family_knn={} precise={} seen_variant={} seen_family={}",
            candidate.variant,
            candidate.family,
            candidate
                .big_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate
                .little_groups
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
            candidate.score,
            candidate.linear_score,
            candidate.ranking_score,
            candidate
                .variant_knn_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate
                .family_knn_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate
                .precise_partition_score
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            candidate.variant_seen_in_training,
            candidate.family_seen_in_training,
        );
    }
    Ok(())
}

fn evaluate_grouping_from_metadata_cmd(
    history_root: &str,
    neighbor_count: usize,
) -> Result<(), Box<dyn Error>> {
    let summary = evaluate_metadata_predictor(Path::new(history_root), neighbor_count)?;

    println!("Metadata Grouping Evaluation");
    println!("history root: {history_root}");
    println!(
        "  dataset holdout: cases={} family_acc={:.1}% variant_acc={:.1}%",
        summary.dataset_holdout_cases,
        summary.dataset_holdout_family_accuracy * 100.0,
        summary.dataset_holdout_variant_accuracy * 100.0
    );
    println!(
        "  unseen grouping holdout: cases={} family_acc={:.1}% variant_acc={:.1}%",
        summary.unseen_grouping_cases,
        summary.unseen_grouping_family_accuracy * 100.0,
        summary.unseen_grouping_variant_accuracy * 100.0
    );
    println!(
        "  combined holdout: cases={} family_acc={:.1}% variant_acc={:.1}%",
        summary.combined_holdout_cases,
        summary.combined_holdout_family_accuracy * 100.0,
        summary.combined_holdout_variant_accuracy * 100.0
    );
    Ok(())
}

fn train_static_grouping_model_cmd(
    history_root: &str,
    model_path: &str,
    neighbor_count: usize,
) -> Result<(), Box<dyn Error>> {
    let model = train_static_grouping_model(Path::new(history_root), neighbor_count)?;
    save_static_grouping_model(&model, Path::new(model_path))?;

    println!("Static Grouping Model");
    println!("history root: {history_root}");
    println!("model path: {model_path}");
    println!("neighbor_count: {}", model.neighbor_count);
    println!(
        "history coverage: {} runs across {} datasets",
        model.total_runs, model.total_datasets
    );
    println!("candidate variants: {}", model.candidate_shapes.len());
    println!("known dataset winners: {}", model.known_dataset_best.len());
    Ok(())
}

fn predict_grouping_from_static_model_cmd(
    model_path: &str,
    probe_log: &str,
) -> Result<(), Box<dyn Error>> {
    let prediction =
        predict_best_grouping_from_static_model(Path::new(model_path), Path::new(probe_log))?;
    let metadata = extract_graph_metadata_from_profile_log(Path::new(probe_log))
        .ok_or_else(|| format!("failed to extract metadata from {probe_log}"))?;

    println!("Static Model Grouping Prediction");
    println!("model path: {model_path}");
    println!("probe log: {probe_log}");
    println!(
        "history coverage: {} runs across {} datasets",
        prediction.total_runs, prediction.total_datasets
    );
    println!("training variants: {}", prediction.training_variants);
    println!("candidate variants: {}", prediction.candidate_variants);
    println!(
        "graph metadata: dataset={} format={} domain={} vertices={} edges={} avg_degree={:.3} density={:.3e}",
        metadata.dataset,
        metadata.format,
        metadata.domain,
        metadata.vertices,
        metadata.edges,
        metadata.average_degree,
        metadata.density
    );
    println!("recommended variant: {}", prediction.recommended_variant);
    println!("recommended family: {}", prediction.recommended_family);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    Ok(())
}

fn emit_static_model_datasets_cmd(
    model_path: &str,
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    let model = load_static_grouping_model(Path::new(model_path))?;
    let emitted = emit_saved_dataset_groupings(&model)?;
    fs::write(output_path, serde_json::to_string_pretty(&emitted)?)?;

    println!("Static Model Dataset Emission");
    println!("model path: {model_path}");
    println!("output path: {output_path}");
    println!("datasets emitted: {}", emitted.len());
    Ok(())
}

fn auto_emit_predicted_sssp_bw8_hls_cmd(
    dataset_path: &str,
    dest_root: Option<PathBuf>,
    model_path: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let dataset_path = PathBuf::from(dataset_path);
    let model_path = model_path.unwrap_or(default_static_grouping_model_path()?);
    let prediction =
        predict_best_grouping_from_static_model_for_dataset(&model_path, dataset_path.as_path())?;
    let output_root = dest_root.unwrap_or_else(|| {
        default_auto_bw8_hls_output_root(dataset_path.as_path(), &prediction.recommended_variant)
    });

    if output_root.exists() {
        fs::remove_dir_all(&output_root)?;
    }

    let dsl = build_predicted_sssp_bw8_dsl(
        &prediction.recommended_big_groups,
        &prediction.recommended_little_groups,
    )?;
    let lowered = LoweredProgram::parse_and_lower(&dsl)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;
    let emitted_root = generate_sssp_hls_project(&gas, &lowered.ast, &output_root)?;
    fs::write(emitted_root.join("autogenerated_input.dsl"), &dsl)?;
    fs::write(
        emitted_root.join("autogenerated_prediction.json"),
        serde_json::to_string_pretty(&prediction)?,
    )?;

    println!("Auto-Emitted 8-bit SSSP HLS");
    println!("dataset path: {}", dataset_path.display());
    println!("model path: {}", model_path.display());
    println!(
        "graph metadata: dataset={} format={} domain={} vertices={} edges={} avg_degree={:.3}",
        prediction.metadata.dataset,
        prediction.metadata.format,
        prediction.metadata.domain,
        prediction.metadata.vertices,
        prediction.metadata.edges,
        prediction.metadata.average_degree,
    );
    println!("recommended variant: {}", prediction.recommended_variant);
    println!("recommended family: {}", prediction.recommended_family);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("HLS project generated at {}", emitted_root.display());
    Ok(())
}

fn auto_emit_hls_from_dsl_cmd(
    dsl_arg: &str,
    dataset_path: &str,
    dest_root: Option<PathBuf>,
    model_path: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let dsl_path = resolve_app_path(dsl_arg)?;
    let dataset_path = PathBuf::from(dataset_path);
    let model_path = model_path.unwrap_or(default_static_grouping_model_path()?);
    let prediction =
        predict_best_grouping_from_static_model_for_dataset(&model_path, dataset_path.as_path())?;
    let output_root = dest_root.unwrap_or_else(|| {
        default_auto_template_hls_output_root(
            dsl_path.as_path(),
            dataset_path.as_path(),
            &prediction.recommended_variant,
        )
    });

    if output_root.exists() {
        fs::remove_dir_all(&output_root)?;
    }

    let template = fs::read_to_string(&dsl_path)?;
    let dsl = replace_auto_grouping_placeholders(
        &template,
        &prediction.recommended_big_groups,
        &prediction.recommended_little_groups,
    )?;
    let lowered = LoweredProgram::parse_and_lower(&dsl)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;
    let emitted_root = generate_sssp_hls_project(&gas, &lowered.ast, &output_root)?;
    fs::write(emitted_root.join("autogenerated_template.dsl"), &template)?;
    fs::write(emitted_root.join("autogenerated_input.dsl"), &dsl)?;
    fs::write(
        emitted_root.join("autogenerated_prediction.json"),
        serde_json::to_string_pretty(&prediction)?,
    )?;

    println!("Auto-Emitted HLS From DSL Template");
    println!("dsl path: {}", dsl_path.display());
    println!("dataset path: {}", dataset_path.display());
    println!("model path: {}", model_path.display());
    println!("recommended variant: {}", prediction.recommended_variant);
    println!("recommended family: {}", prediction.recommended_family);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("HLS project generated at {}", emitted_root.display());
    Ok(())
}

fn auto_emit_hls_from_dsl_32bit_cmd(
    dsl_arg: &str,
    dataset_path: &str,
    dest_root: Option<PathBuf>,
    model_path: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let dsl_path = resolve_app_path(dsl_arg)?;
    let dataset_path = PathBuf::from(dataset_path);
    let model_path = model_path.unwrap_or(default_grouping32_model_path()?);
    let prediction = predict_grouping32_from_static_model_for_dataset(
        model_path.as_path(),
        dataset_path.as_path(),
    )?;
    let output_root = dest_root.unwrap_or_else(|| {
        default_auto_template_hls_output_root(
            dsl_path.as_path(),
            dataset_path.as_path(),
            &prediction.recommended_variant,
        )
    });

    if output_root.exists() {
        fs::remove_dir_all(&output_root)?;
    }

    let template = fs::read_to_string(&dsl_path)?;
    let dsl = replace_auto_grouping_placeholders(
        &template,
        &prediction.recommended_big_groups,
        &prediction.recommended_little_groups,
    )?;
    let lowered = LoweredProgram::parse_and_lower(&dsl)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;
    let emitted_root = generate_sssp_hls_project(&gas, &lowered.ast, &output_root)?;
    fs::write(emitted_root.join("autogenerated_template.dsl"), &template)?;
    fs::write(emitted_root.join("autogenerated_input.dsl"), &dsl)?;
    fs::write(
        emitted_root.join("autogenerated_prediction.json"),
        serde_json::to_string_pretty(&prediction)?,
    )?;

    println!("Auto-Emitted HLS From DSL Template (32-bit model)");
    println!("dsl path: {}", dsl_path.display());
    println!("dataset path: {}", dataset_path.display());
    println!("model path: {}", model_path.display());
    println!("recommended variant: {}", prediction.recommended_variant);
    println!(
        "recommended grouping: big=[{}] little=[{}]",
        prediction
            .recommended_big_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(","),
        prediction
            .recommended_little_groups
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("HLS project generated at {}", emitted_root.display());
    Ok(())
}

fn default_grouping32_model_path() -> Result<PathBuf, Box<dyn Error>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest
        .join("docs")
        .join("grouping32_static_model_2026-04-04.json"))
}

fn emit_hls_project(app_arg: &str, dest_root: Option<PathBuf>) -> Result<(), Box<dyn Error>> {
    let resolved_path = resolve_app_path(app_arg)?;
    let dest_root = dest_root
        .or_else(|| default_hls_output_root(resolved_path.as_path()).ok())
        .ok_or_else(|| "could not determine output path for HLS emission".to_string())?;

    if dest_root.exists() {
        fs::remove_dir_all(&dest_root)?;
    }

    let source = fs::read_to_string(&resolved_path)?;
    let lowered = LoweredProgram::parse_and_lower(&source)?;
    let gas = lower_to_gas(&lowered.ast, &lowered.ir)?;
    let output_root = generate_sssp_hls_project(&gas, &lowered.ast, &dest_root)?;
    println!("HLS project generated at {}", output_root.display());
    Ok(())
}

fn default_hls_output_root(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| "input path has no valid file stem".to_string())?;
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest.join("target").join("generated_hls").join(stem))
}

fn default_static_grouping_model_path() -> Result<PathBuf, Box<dyn Error>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest
        .join("docs")
        .join("grouping_static_model_2026-04-02.json"))
}

fn default_auto_bw8_hls_output_root(dataset_path: &Path, variant: &str) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dataset_name = dataset_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("dataset");
    let dataset_key = sanitize_name_component(dataset_name);
    let variant_key = sanitize_name_component(variant);
    manifest
        .join("target")
        .join("generated_hls")
        .join(format!("sssp_auto_{}_{}_bw8", dataset_key, variant_key))
}

fn default_auto_template_hls_output_root(
    dsl_path: &Path,
    dataset_path: &Path,
    variant: &str,
) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dsl_key = sanitize_name_component(
        dsl_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("template"),
    );
    let dataset_key = sanitize_name_component(
        dataset_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("dataset"),
    );
    let variant_key = sanitize_name_component(variant);
    manifest
        .join("target")
        .join("generated_hls")
        .join(format!("{}_auto_{}_{}", dsl_key, dataset_key, variant_key))
}

fn sanitize_name_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_lowercase()
}

fn build_predicted_sssp_bw8_dsl(
    big_groups: &[usize],
    little_groups: &[usize],
) -> Result<String, Box<dyn Error>> {
    let placements = assign_group_slrs(big_groups, little_groups)
        .ok_or_else(|| "could not derive SLR placement for predicted grouping".to_string())?;

    let little_text = placements
        .1
        .iter()
        .map(render_group_placement)
        .collect::<Vec<_>>()
        .join(",\n");
    let big_text = placements
        .0
        .iter()
        .map(render_group_placement)
        .collect::<Vec<_>>()
        .join(",\n");

    Ok(format!(
        "{{\n    Node: {{\n        dist: int<8>\n    }}\n    Edge: {{}}\n}}\n\nHlsConfig {{\n    topology: {{\n        apply_slr: 1\n        hbm_writer_slr: 0\n        cross_slr_fifo_depth: 16\n        little_groups: [\n{little_text}\n        ]\n        big_groups: [\n{big_text}\n        ]\n    }}\n}}\n\n{{\n    edges = iteration_input(G.EDGES)\n    dst_ids = map([edges], lambda e: e.dst)\n    updates = map([edges], lambda e: e.src.dist + 1)\n    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)\n    relaxed = map([min_dists], lambda d: self.dist > d ? d : self.dist)\n    return relaxed as result_node_prop.dist\n}}\n"
    ))
}

fn render_group_placement(placement: &AutoGroupPlacement) -> String {
    render_group_placement_with_indent(placement, "            ")
}

fn render_group_placement_with_indent(placement: &AutoGroupPlacement, indent: &str) -> String {
    let pipeline_slr = placement
        .pipeline_slr
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{indent}{{ pipelines: {} merger_slr: {} pipeline_slr: [{}] }}",
        placement.pipelines, placement.merger_slr, pipeline_slr
    )
}

fn replace_auto_grouping_placeholders(
    template: &str,
    big_groups: &[usize],
    little_groups: &[usize],
) -> Result<String, Box<dyn Error>> {
    let (big_placements, little_placements) = assign_group_slrs(big_groups, little_groups)
        .ok_or_else(|| "could not derive SLR placement for predicted grouping".to_string())?;

    let mut out = Vec::new();
    let mut saw_big_auto = false;
    let mut saw_little_auto = false;

    for line in template.lines() {
        let indent_len = line.len() - line.trim_start().len();
        let indent = &line[..indent_len];
        let trimmed = line.trim();

        if let Some(value) = trimmed.strip_prefix("little_groups:") {
            if is_auto_group_marker(value) {
                saw_little_auto = true;
                out.push(format!("{indent}little_groups: ["));
                for (idx, placement) in little_placements.iter().enumerate() {
                    let suffix = if idx + 1 == little_placements.len() {
                        ""
                    } else {
                        ","
                    };
                    out.push(format!(
                        "{}{}",
                        render_group_placement_with_indent(placement, &format!("{indent}    ")),
                        suffix,
                    ));
                }
                out.push(format!("{indent}]"));
                continue;
            }
        }

        if let Some(value) = trimmed.strip_prefix("big_groups:") {
            if is_auto_group_marker(value) {
                saw_big_auto = true;
                out.push(format!("{indent}big_groups: ["));
                for (idx, placement) in big_placements.iter().enumerate() {
                    let suffix = if idx + 1 == big_placements.len() {
                        ""
                    } else {
                        ","
                    };
                    out.push(format!(
                        "{}{}",
                        render_group_placement_with_indent(placement, &format!("{indent}    ")),
                        suffix,
                    ));
                }
                out.push(format!("{indent}]"));
                continue;
            }
        }

        out.push(line.to_string());
    }

    if !saw_little_auto || !saw_big_auto {
        return Err(
            "DSL template must contain both 'little_groups: auto' and 'big_groups: auto'"
                .to_string()
                .into(),
        );
    }

    let mut rendered = out.join("\n");
    if template.ends_with('\n') {
        rendered.push('\n');
    }
    Ok(rendered)
}

fn is_auto_group_marker(value: &str) -> bool {
    value.trim().trim_end_matches(',').trim() == "auto"
}

fn assign_group_slrs(
    big_groups: &[usize],
    little_groups: &[usize],
) -> Option<(Vec<AutoGroupPlacement>, Vec<AutoGroupPlacement>)> {
    const TARGET_SLR_PIPELINES: [usize; 3] = [4, 5, 5];
    let group_sizes: Vec<usize> = big_groups
        .iter()
        .chain(little_groups.iter())
        .copied()
        .collect();
    let mut assignments_by_size = HashMap::new();
    for size in group_sizes.iter().copied() {
        assignments_by_size
            .entry(size)
            .or_insert_with(|| partition_options(size));
    }

    fn solve(
        idx: usize,
        group_sizes: &[usize],
        remaining: [usize; 3],
        options: &HashMap<
            usize,
            Vec<(
                (usize, usize, usize),
                usize,
                (usize, usize, usize, usize, usize),
            )>,
        >,
    ) -> Option<(usize, usize, usize, Vec<((usize, usize, usize), usize)>)> {
        if idx == group_sizes.len() {
            return Some((0, 0, 0, Vec::new()));
        }

        let size = group_sizes[idx];
        let mut best = None;
        for (counts, merger_slr, score) in options.get(&size)? {
            let (c0, c1, c2) = *counts;
            if c0 > remaining[0] || c1 > remaining[1] || c2 > remaining[2] {
                continue;
            }
            let next = [remaining[0] - c0, remaining[1] - c1, remaining[2] - c2];
            let Some((tail0, tail1, tail2, mut tail)) = solve(idx + 1, group_sizes, next, options)
            else {
                continue;
            };
            let total = (score.0 + tail0, score.1 + tail1, score.2 + tail2);
            let candidate = (total.0, total.1, total.2);
            let current_best = best.as_ref().map(
                |(a, b, c, _): &(usize, usize, usize, Vec<((usize, usize, usize), usize)>)| {
                    (*a, *b, *c)
                },
            );
            if current_best.is_none() || candidate < current_best.unwrap() {
                tail.insert(0, (*counts, *merger_slr));
                best = Some((total.0, total.1, total.2, tail));
            }
        }
        best
    }

    let solved = solve(0, &group_sizes, TARGET_SLR_PIPELINES, &assignments_by_size)?;
    let placements: Vec<_> = solved
        .3
        .into_iter()
        .enumerate()
        .map(|(idx, (counts, merger_slr))| AutoGroupPlacement {
            pipelines: group_sizes[idx],
            merger_slr,
            pipeline_slr: build_pipeline_slr_list(counts, merger_slr),
        })
        .collect();

    let big = placements[..big_groups.len()].to_vec();
    let little = placements[big_groups.len()..].to_vec();
    Some((big, little))
}

fn partition_options(
    pipelines: usize,
) -> Vec<(
    (usize, usize, usize),
    usize,
    (usize, usize, usize, usize, usize),
)> {
    let mut out = Vec::new();
    for slr0 in 0..=pipelines {
        for slr1 in 0..=(pipelines - slr0) {
            let slr2 = pipelines - slr0 - slr1;
            let counts = (slr0, slr1, slr2);
            let mut best = None;
            for merger_slr in [1usize, 2, 0] {
                let distance = counts.0 * merger_slr.abs_diff(0)
                    + counts.1 * merger_slr.abs_diff(1)
                    + counts.2 * merger_slr.abs_diff(2);
                let crossing = pipelines
                    - match merger_slr {
                        0 => counts.0,
                        1 => counts.1,
                        _ => counts.2,
                    };
                let spread = [counts.0, counts.1, counts.2]
                    .into_iter()
                    .filter(|count| *count > 0)
                    .count()
                    .saturating_sub(1);
                let score = (
                    distance,
                    crossing,
                    spread,
                    pipelines
                        - match merger_slr {
                            0 => counts.0,
                            1 => counts.1,
                            _ => counts.2,
                        },
                    merger_slr,
                );
                if best
                    .as_ref()
                    .map(
                        |(best_score, _): &((usize, usize, usize, usize, usize), usize)| {
                            &score < best_score
                        },
                    )
                    .unwrap_or(true)
                {
                    best = Some((score, merger_slr));
                }
            }
            if let Some((score, merger_slr)) = best {
                out.push((counts, merger_slr, score));
            }
        }
    }
    out.sort_by_key(|entry| entry.2);
    out
}

fn build_pipeline_slr_list(counts: (usize, usize, usize), merger_slr: usize) -> Vec<usize> {
    let mut order = [0usize, 1, 2];
    order.sort_by_key(|slr| {
        (
            (*slr != merger_slr) as usize,
            slr.abs_diff(merger_slr),
            *slr,
        )
    });

    let mut out = Vec::new();
    for slr in order {
        let count = match slr {
            0 => counts.0,
            1 => counts.1,
            _ => counts.2,
        };
        out.extend(std::iter::repeat_n(slr, count));
    }
    out
}

fn resolve_app_path(arg: &str) -> Result<PathBuf, Box<dyn Error>> {
    let candidate = PathBuf::from(arg);
    if candidate.exists() {
        return Ok(candidate);
    }

    let apps_root = PathBuf::from("apps");
    let mut app_path = apps_root.join(format!("{arg}.dsl"));
    if app_path.exists() {
        return Ok(app_path);
    }

    // try exact name in apps folder
    app_path = apps_root.join(arg);
    if app_path.exists() {
        return Ok(app_path);
    }

    Err(format!("could not locate app or file for '{arg}'").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulate_generated_sssp_outputs_values() -> Result<(), Box<dyn Error>> {
        let resolved = resolve_app_path("sssp")?;
        let (gas, node_types, edge_types, target_prop) =
            load_program_and_types(resolved.as_path())?;
        let spec = GraphSpec::new(3, 4).with_seed(11);
        let graph_input = generate_graph(AppKind::Sssp, spec)?;
        let graph = load_graph_from_json(graph_input, &node_types, &edge_types)?;
        let state = refactor_Graphyflow::simulate_gas(&gas, &graph, 32)?;
        let rendered = render_property(&state, &target_prop);
        let map = rendered
            .as_object()
            .ok_or_else(|| "rendered output is not an object".to_string())?;
        assert_eq!(map.len(), 3, "expected one entry per node");
        Ok(())
    }
}
