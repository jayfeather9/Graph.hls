use std::collections::HashMap;

use refactor_Graphyflow::domain::{ast::EntityDef, gas::GasProgram, gas::GasType};
use refactor_Graphyflow::engine::gas_lower::lower_to_gas;
use refactor_Graphyflow::engine::gas_simulator::{
    GraphInput, GraphState, load_graph_from_json, simulate_gas, simulate_gas_for_iters,
};
use refactor_Graphyflow::engine::ir_builder::LoweredProgram;
use refactor_Graphyflow::parse_program;
use refactor_Graphyflow::utils::reference_calcs::reference_values;

fn load_app(app: &str) -> (GasProgram, GraphInput, GraphState, String) {
    let source = std::fs::read_to_string(format!("apps/{app}.dsl")).expect("read dsl");
    let ast = parse_program(&source).expect("parse program");
    let lowered = LoweredProgram::parse_and_lower(&source).expect("lower program");
    let gas = lower_to_gas(&ast, &lowered.ir).expect("gas lower");

    let node_types = ast
        .schema
        .node
        .as_ref()
        .map(|n| collect_types(n))
        .unwrap_or_default();
    let edge_types = ast
        .schema
        .edge
        .as_ref()
        .map(|e| collect_types(e))
        .unwrap_or_default();

    let fixture = std::fs::read_to_string(format!("apps/test_graphs/{app}_small.json"))
        .expect("read fixture");
    let input: GraphInput = serde_json::from_str(&fixture).expect("parse fixture");
    let graph = load_graph_from_json(input.clone(), &node_types, &edge_types).expect("graph");
    let target_prop = gas.apply.target_property.as_str().to_string();
    (gas, input, graph, target_prop)
}

fn collect_types(entity: &EntityDef) -> HashMap<String, GasType> {
    entity
        .properties
        .iter()
        .map(|p| {
            (
                p.name.as_str().to_string(),
                refactor_Graphyflow::engine::gas_lower::typeexpr_to_gastype(&p.ty),
            )
        })
        .collect()
}

#[test]
fn sssp_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("sssp");
    let simulated = simulate_gas(&gas, &graph, 16).expect("simulate sssp");
    let expected = reference_values("sssp", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("distance present");
        assert!(exp.approx_eq(&got), "node {id} distance mismatch");
    }
}

#[test]
fn bfs_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("bfs");
    let simulated = simulate_gas(&gas, &graph, 64).expect("simulate bfs");
    let expected = reference_values("bfs", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("prop present");
        assert!(exp.approx_eq(&got), "node {id} prop mismatch");
    }
}

#[test]
fn ar_matches_reference_for_fixed_iters() {
    let (gas, input, graph, target_prop) = load_app("ar");
    let simulated = simulate_gas_for_iters(&gas, &graph, 10).expect("simulate ar");
    let expected = reference_values("ar", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("score present");
        assert!(exp.approx_eq(&got), "node {id} score mismatch");
    }
}

#[test]
fn pagerank_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("pagerank");
    let simulated = simulate_gas(&gas, &graph, 128).expect("simulate pagerank");
    let expected = reference_values("pagerank", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("rank present");
        assert!(exp.approx_eq(&got), "node {id} rank mismatch");
    }
}

#[test]
fn connected_components_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("connected_components");
    let simulated = simulate_gas(&gas, &graph, 16).expect("simulate cc");
    let expected =
        reference_values("connected_components", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("label present");
        assert!(exp.approx_eq(&got), "node {id} label mismatch");
    }
}

#[test]
fn wcc_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("wcc");
    let simulated = simulate_gas(&gas, &graph, 32).expect("simulate wcc");
    let expected = reference_values("wcc", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("label present");
        assert!(exp.approx_eq(&got), "node {id} label mismatch");
    }
}

#[test]
fn graph_coloring_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("graph_coloring");
    let simulated = simulate_gas(&gas, &graph, 8).expect("simulate coloring");
    let expected = reference_values("graph_coloring", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("color present");
        assert!(exp.approx_eq(&got), "node {id} color mismatch");
    }
}

#[test]
fn als_matches_reference() {
    let (gas, input, graph, target_prop) = load_app("als");
    let simulated = simulate_gas(&gas, &graph, 4).expect("simulate als");
    let expected = reference_values("als", &input, &target_prop).expect("reference");

    for (id, exp) in expected {
        let got = simulated
            .node_prop(id, &target_prop)
            .cloned()
            .expect("vec present");
        assert!(exp.approx_eq(&got), "node {id} vector mismatch");
    }
}
