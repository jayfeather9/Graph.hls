#![allow(dead_code)]
use std::{
    collections::{BTreeSet, HashMap},
    time::Instant,
};

use crate::domain::{
    ast::{Accessor, BinaryOp, Literal as AstLiteral, UnaryOp},
    gas::{GasProgram, GasType},
    ir::{IrExpr, IrLambda},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors produced during GAS simulation.
#[derive(Debug, Error)]
pub enum GasSimError {
    #[error("missing node id {0}")]
    MissingNode(i64),
    #[error("missing edge property '{0}'")]
    MissingEdgeProp(String),
    #[error("missing node property '{0}'")]
    MissingNodeProp(String),
    #[error("type mismatch for property '{0}'")]
    TypeMismatch(String),
    #[error("unsupported operation: {0}")]
    UnsupportedOp(String),
    #[error("reduce lambda must have exactly two params")]
    InvalidReduceArity,
    #[error("call error: {0}")]
    CallError(String),
    #[error("iteration limit reached without convergence")]
    NoConvergence,
    #[error("key/value length mismatch in reduce")]
    ReduceLengthMismatch,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Tuple(Vec<Value>),
    Array(Vec<Value>),
    IntSet(BTreeSet<i64>),
    Vector(Vec<f64>),
    Matrix {
        rows: usize,
        cols: usize,
        data: Vec<f64>,
    },
    NodeRef(i64),
    EdgeRef {
        src: i64,
        dst: i64,
        props: HashMap<String, Value>,
    },
    Unit,
}

impl Value {
    fn as_bool(&self) -> Result<bool, GasSimError> {
        match self {
            Value::Bool(b) => Ok(*b),
            _ => Err(GasSimError::TypeMismatch("bool".into())),
        }
    }

    pub fn as_f64(&self) -> Result<f64, GasSimError> {
        match self {
            Value::Int(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(GasSimError::TypeMismatch("number".into())),
        }
    }

    pub fn approx_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (*a - *b).abs() < 1e-6,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Tuple(a), Value::Tuple(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.approx_eq(y))
            }
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.approx_eq(y))
            }
            (Value::IntSet(a), Value::IntSet(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (*x - *y).abs() < 1e-6)
            }
            (
                Value::Matrix {
                    rows: ra,
                    cols: ca,
                    data: da,
                },
                Value::Matrix {
                    rows: rb,
                    cols: cb,
                    data: db,
                },
            ) => {
                ra == rb
                    && ca == cb
                    && da.len() == db.len()
                    && da.iter().zip(db).all(|(x, y)| (*x - *y).abs() < 1e-6)
            }
            (Value::NodeRef(a), Value::NodeRef(b)) => a == b,
            (
                Value::EdgeRef {
                    src: asrc,
                    dst: adst,
                    ..
                },
                Value::EdgeRef {
                    src: bsrc,
                    dst: bdst,
                    ..
                },
            ) => asrc == bsrc && adst == bdst,
            (Value::Unit, Value::Unit) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NodeRecord {
    pub id: i64,
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct EdgeRecord {
    pub src: i64,
    pub dst: i64,
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct GraphInput {
    pub nodes: Vec<NodeRecord>,
    pub edges: Vec<EdgeRecord>,
}

#[derive(Clone, Debug)]
struct NodeState {
    props: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
struct EdgeState {
    src: i64,
    dst: i64,
    props: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct GraphState {
    nodes: HashMap<i64, NodeState>,
    edges: Vec<EdgeState>,
    edge_uniform_props: HashMap<String, Value>,
}

impl GraphState {
    pub fn node_prop(&self, id: i64, prop: &str) -> Option<&Value> {
        self.nodes.get(&id).and_then(|n| n.props.get(prop))
    }

    pub fn node_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.nodes.keys().copied()
    }
}

/// Simulate a GAS program until convergence or max_iters.
pub fn simulate_gas(
    program: &GasProgram,
    graph: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let int_mask = int_mask_for_program(program);
    match simulate_gas_compiled(program, graph, max_iters) {
        Ok(state) => Ok(state),
        Err(GasSimError::UnsupportedOp(_)) => {
            let mut state = graph.clone();
            for _ in 0..max_iters {
                let updates = execute_iteration_masked(program, &state, int_mask)?;
                let changed =
                    apply_updates(&mut state, &updates, program.apply.target_property.as_str());
                if !changed {
                    return Ok(state);
                }
            }
            Err(GasSimError::NoConvergence)
        }
        Err(err) => Err(err),
    }
}

/// Simulate using the compiled execution plan. Returns `UnsupportedOp` when
/// expressions cannot be compiled, allowing a fallback to the interpreter.
pub fn simulate_gas_compiled(
    program: &GasProgram,
    graph: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let mut dense = DenseGraph::from_state(graph)?;
    let plan = CompiledPlan::from_program(program)?;

    for _ in 0..max_iters {
        let changed = execute_iteration_compiled(&plan, &mut dense)?;
        if !changed {
            return Ok(dense.into_state());
        }
    }

    Err(GasSimError::NoConvergence)
}

/// Simulate a GAS program for a fixed number of iterations, ignoring convergence.
pub fn simulate_gas_for_iters(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    match simulate_gas_compiled_for_iters(program, graph, iters) {
        Ok(state) => Ok(state),
        Err(GasSimError::UnsupportedOp(_)) => simulate_gas_interpreted_for_iters(program, graph, iters),
        Err(err) => Err(err),
    }
}

pub fn simulate_gas_for_iters_measure_only_timed(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<f64, GasSimError> {
    match simulate_gas_compiled_for_iters_measure_only_timed(program, graph, iters) {
        Ok(seconds) => Ok(seconds),
        Err(GasSimError::UnsupportedOp(_)) => {
            simulate_gas_interpreted_for_iters_measure_only_timed(program, graph, iters)
        }
        Err(err) => Err(err),
    }
}

fn simulate_gas_compiled_for_iters(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    let mut dense = DenseGraph::from_state(graph)?;
    let plan = CompiledPlan::from_program(program)?;
    for _ in 0..iters {
        let _ = execute_iteration_compiled(&plan, &mut dense)?;
    }
    Ok(dense.into_state())
}

fn simulate_gas_compiled_for_iters_measure_only_timed(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<f64, GasSimError> {
    let mut dense = DenseGraph::from_state(graph)?;
    let plan = CompiledPlan::from_program(program)?;
    let start = Instant::now();
    for _ in 0..iters {
        let _ = execute_iteration_compiled(&plan, &mut dense)?;
    }
    Ok(start.elapsed().as_secs_f64())
}

fn simulate_gas_interpreted_for_iters(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    let mut state = graph.clone();
    let int_mask = int_mask_for_program(program);
    for _ in 0..iters {
        let updates = execute_iteration_masked(program, &state, int_mask)?;
        apply_updates(&mut state, &updates, program.apply.target_property.as_str());
    }
    Ok(state)
}

fn simulate_gas_interpreted_for_iters_measure_only_timed(
    program: &GasProgram,
    graph: &GraphState,
    iters: usize,
) -> Result<f64, GasSimError> {
    let mut state = graph.clone();
    let int_mask = int_mask_for_program(program);
    let start = Instant::now();
    for _ in 0..iters {
        let updates = execute_iteration_masked(program, &state, int_mask)?;
        apply_updates(&mut state, &updates, program.apply.target_property.as_str());
    }
    Ok(start.elapsed().as_secs_f64())
}

fn simulate_gas_interpreted(
    program: &GasProgram,
    graph: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let mut state = graph.clone();
    for _ in 0..max_iters {
        let updates = execute_iteration(program, &state)?;
        let changed = apply_updates(&mut state, &updates, program.apply.target_property.as_str());
        if !changed {
            return Ok(state);
        }
    }
    Err(GasSimError::NoConvergence)
}

fn execute_iteration(
    program: &GasProgram,
    state: &GraphState,
) -> Result<HashMap<i64, Value>, GasSimError> {
    execute_iteration_masked(program, state, None)
}

fn execute_iteration_masked(
    program: &GasProgram,
    state: &GraphState,
    int_mask: Option<u64>,
) -> Result<HashMap<i64, Value>, GasSimError> {
    // Scatter
    let mut scatter_keys = Vec::new();
    let mut scatter_vals = Vec::new();
    for edge in &state.edges {
        let env = build_edge_env(edge, state);
        let edge_val = Value::EdgeRef {
            src: edge.src,
            dst: edge.dst,
            props: edge.props.clone(),
        };
        let key_args = vec![edge_val.clone(); program.scatter.key_lambda.params.len()];
        let key = eval_lambda(&program.scatter.key_lambda, &key_args, &env, state, int_mask)?;
        let val_args = vec![edge_val; program.scatter.value_lambda.params.len()];
        let val =
            eval_lambda(&program.scatter.value_lambda, &val_args, &env, state, int_mask)?;
        scatter_keys.push(key);
        scatter_vals.push(val);
    }

    // Gather
    let grouped = group_by_key(&scatter_keys, &scatter_vals)?;
    let mut gather_out: HashMap<i64, Value> = HashMap::new();
    for (key, values) in grouped {
        let mut acc = values
            .first()
            .ok_or(GasSimError::ReduceLengthMismatch)?
            .clone();
        for v in values.iter().skip(1) {
            acc = eval_lambda(
                &program.gather.reducer,
                &[acc.clone(), v.clone()],
                &HashMap::new(),
                state,
                int_mask,
            )?;
        }
        gather_out.insert(key, acc);
    }

    // Apply
    let mut updates = HashMap::new();
    for (node_id, node) in &state.nodes {
        let target_prop = program.apply.target_property.as_str();
        let current_prop = node.props.get(target_prop).cloned().unwrap_or(Value::Unit);
        let gather_val = gather_out
            .get(node_id)
            .cloned()
            .unwrap_or(current_prop.clone());
        let result = if let Some(lambda) = &program.apply.lambda {
            let mut env = HashMap::new();
            env.insert("self".to_string(), Value::NodeRef(*node_id));
            eval_lambda(lambda, &[gather_val], &env, state, int_mask)?
        } else {
            gather_val
        };
        updates.insert(*node_id, result);
    }

    Ok(updates)
}

#[derive(Clone, Debug)]
struct DenseEdge {
    src_idx: usize,
    dst_idx: usize,
}

#[derive(Clone, Debug)]
struct DenseGraph {
    node_ids: Vec<i64>,
    id_to_idx: HashMap<i64, usize>,
    edges: Vec<DenseEdge>,
    node_props: HashMap<String, Vec<Value>>,
    edge_props: HashMap<String, Vec<Value>>,
}

impl DenseGraph {
    fn from_state(state: &GraphState) -> Result<Self, GasSimError> {
        let mut node_ids = state.nodes.keys().copied().collect::<Vec<_>>();
        node_ids.sort_unstable();

        let id_to_idx = node_ids
            .iter()
            .enumerate()
            .map(|(idx, id)| (*id, idx))
            .collect::<HashMap<_, _>>();

        let mut node_props: HashMap<String, Vec<Value>> = HashMap::new();
        for node in state.nodes.values() {
            for name in node.props.keys() {
                node_props
                    .entry(name.clone())
                    .or_insert_with(|| vec![Value::Unit; node_ids.len()]);
            }
        }

        for (node_id, node) in &state.nodes {
            let idx = *id_to_idx
                .get(node_id)
                .ok_or(GasSimError::MissingNode(*node_id))?;
            for (name, value) in &node.props {
                if let Some(column) = node_props.get_mut(name) {
                    column[idx] = value.clone();
                }
            }
        }

        let mut edge_props: HashMap<String, Vec<Value>> = HashMap::new();
        for (name, value) in &state.edge_uniform_props {
            edge_props.insert(name.clone(), vec![value.clone(); state.edges.len()]);
        }
        for edge in &state.edges {
            for name in edge.props.keys() {
                edge_props
                    .entry(name.clone())
                    .or_insert_with(|| vec![Value::Unit; state.edges.len()]);
            }
        }

        let mut edges = Vec::with_capacity(state.edges.len());
        for (edge_idx, edge) in state.edges.iter().enumerate() {
            let src_idx = *id_to_idx
                .get(&edge.src)
                .ok_or(GasSimError::MissingNode(edge.src))?;
            let dst_idx = *id_to_idx
                .get(&edge.dst)
                .ok_or(GasSimError::MissingNode(edge.dst))?;
            edges.push(DenseEdge { src_idx, dst_idx });

            for (name, value) in &edge.props {
                if let Some(column) = edge_props.get_mut(name) {
                    column[edge_idx] = value.clone();
                }
            }
        }

        Ok(Self {
            node_ids,
            id_to_idx,
            edges,
            node_props,
            edge_props,
        })
    }

    fn into_state(self) -> GraphState {
        let mut nodes = HashMap::with_capacity(self.node_ids.len());
        for (idx, node_id) in self.node_ids.iter().enumerate() {
            let mut props = HashMap::new();
            for (name, values) in &self.node_props {
                props.insert(name.clone(), values[idx].clone());
            }
            nodes.insert(*node_id, NodeState { props });
        }

        let mut edges = Vec::with_capacity(self.edges.len());
        for (edge_idx, edge) in self.edges.iter().enumerate() {
            let mut props = HashMap::new();
            for (name, values) in &self.edge_props {
                props.insert(name.clone(), values[edge_idx].clone());
            }
            edges.push(EdgeState {
                src: self.node_ids[edge.src_idx],
                dst: self.node_ids[edge.dst_idx],
                props,
            });
        }

        GraphState {
            nodes,
            edges,
            edge_uniform_props: HashMap::new(),
        }
    }

    fn node_prop(&self, node_idx: usize, name: &str) -> Option<&Value> {
        self.node_props
            .get(name)
            .and_then(|vals| vals.get(node_idx))
    }

    fn edge_prop(&self, edge_idx: usize, name: &str) -> Option<&Value> {
        self.edge_props
            .get(name)
            .and_then(|vals| vals.get(edge_idx))
    }

    fn node_idx_for_id(&self, id: i64) -> Option<usize> {
        self.id_to_idx.get(&id).copied()
    }
}

#[derive(Clone, Debug)]
enum SpecializedKernel {
    PageRank(PageRankKernel),
    Sssp(SsspKernel),
    ConnectedComponents(ConnectedComponentsKernel),
}

impl SpecializedKernel {
    fn from_program(program: &GasProgram) -> Option<Self> {
        compile_pagerank_kernel(program)
            .map(SpecializedKernel::PageRank)
            .or_else(|| compile_sssp_kernel(program).map(SpecializedKernel::Sssp))
            .or_else(|| {
                compile_connected_components_kernel(program)
                    .map(SpecializedKernel::ConnectedComponents)
            })
    }
}

#[derive(Clone, Debug)]
struct PageRankKernel {
    target_property: String,
    out_degree_property: String,
    base: f64,
    scale: f64,
}

#[derive(Clone, Debug)]
struct SsspKernel {
    target_property: String,
    edge_weight_property: Option<String>,
    edge_weight_constant: Option<i64>,
}

#[derive(Clone, Debug)]
struct ConnectedComponentsKernel {
    target_property: String,
}

fn sssp_uniform_weight_from_state(
    spec: &SsspKernel,
    state: &GraphState,
) -> Result<Option<i64>, GasSimError> {
    if let Some(weight) = spec.edge_weight_constant {
        return Ok(Some(weight));
    }
    if let Some(prop) = spec.edge_weight_property.as_deref() {
        return match state.edge_uniform_props.get(prop) {
            Some(Value::Int(v)) => Ok(Some(*v)),
            Some(_) => Err(GasSimError::TypeMismatch(prop.to_string())),
            None => Ok(None),
        };
    }
    Ok(None)
}

fn sssp_weight_from_edge_state(spec: &SsspKernel, edge: &EdgeState) -> Result<i64, GasSimError> {
    if let Some(weight) = spec.edge_weight_constant {
        return Ok(weight);
    }
    let prop = spec
        .edge_weight_property
        .as_deref()
        .ok_or_else(|| GasSimError::UnsupportedOp("missing SSSP edge weight source".into()))?;
    let edge_weight = edge
        .props
        .get(prop)
        .ok_or_else(|| GasSimError::MissingEdgeProp(prop.to_string()))?;
    match edge_weight {
        Value::Int(v) => Ok(*v),
        _ => Err(GasSimError::TypeMismatch(prop.to_string())),
    }
}

fn sssp_uniform_weight_from_dense(spec: &SsspKernel, state: &DenseGraph) -> Result<Option<i64>, GasSimError> {
    if let Some(weight) = spec.edge_weight_constant {
        return Ok(Some(weight));
    }
    if let Some(prop) = spec.edge_weight_property.as_deref() {
        return match state.edge_props.get(prop) {
            Some(values) if values.is_empty() => Ok(None),
            Some(values) => match &values[0] {
                Value::Int(v) if values.iter().all(|value| matches!(value, Value::Int(other) if other == v)) => Ok(Some(*v)),
                Value::Int(_) => Ok(None),
                _ => Err(GasSimError::TypeMismatch(prop.to_string())),
            },
            None => Ok(None),
        };
    }
    Ok(None)
}

fn sssp_weight_column_from_dense(spec: &SsspKernel, state: &DenseGraph) -> Result<Option<Vec<i64>>, GasSimError> {
    if sssp_uniform_weight_from_dense(spec, state)?.is_some() {
        return Ok(None);
    }
    if let Some(prop) = spec.edge_weight_property.as_deref() {
        return Ok(Some(read_edge_prop_as_i64(state, prop)?));
    }
    Ok(None)
}

fn simulate_specialized_kernel(
    kernel: &SpecializedKernel,
    state: &mut DenseGraph,
    max_iters: usize,
) -> Result<(), GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => simulate_pagerank_kernel(spec, state, max_iters),
        SpecializedKernel::Sssp(spec) => simulate_sssp_kernel(spec, state, max_iters),
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_kernel(spec, state, max_iters)
        }
    }
}

fn simulate_specialized_kernel_for_iters(
    kernel: &SpecializedKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<(), GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => simulate_pagerank_kernel_for_iters(spec, state, iters),
        SpecializedKernel::Sssp(spec) => simulate_sssp_kernel_for_iters(spec, state, iters),
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_kernel_for_iters(spec, state, iters)
        }
    }
}

fn simulate_specialized_kernel_for_iters_measure_only_timed(
    kernel: &SpecializedKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<f64, GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => {
            simulate_pagerank_kernel_for_iters_measure_only_timed(spec, state, iters)
        }
        SpecializedKernel::Sssp(spec) => {
            simulate_sssp_kernel_for_iters_measure_only_timed(spec, state, iters)
        }
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_kernel_for_iters_measure_only_timed(spec, state, iters)
        }
    }
}

fn simulate_specialized_kernel_on_state_for_iters(
    kernel: &SpecializedKernel,
    state: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => simulate_pagerank_on_state_for_iters(spec, state, iters),
        SpecializedKernel::Sssp(spec) => simulate_sssp_on_state_for_iters(spec, state, iters),
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_on_state_for_iters(spec, state, iters)
        }
    }
}

fn simulate_specialized_kernel_on_state_for_iters_measure_only(
    kernel: &SpecializedKernel,
    state: &GraphState,
    iters: usize,
) -> Result<(), GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => {
            simulate_pagerank_on_state_for_iters_measure_only(spec, state, iters)
        }
        SpecializedKernel::Sssp(spec) => {
            simulate_sssp_on_state_for_iters_measure_only(spec, state, iters)
        }
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_on_state_for_iters_measure_only(spec, state, iters)
        }
    }
}

#[derive(Clone, Debug)]
enum NodeIndexer {
    Dense {
        len: usize,
    },
    Sparse {
        node_ids: Vec<i64>,
        id_to_idx: HashMap<i64, usize>,
    },
}

impl NodeIndexer {
    fn from_state(state: &GraphState) -> Self {
        if state.nodes.is_empty() {
            return NodeIndexer::Dense { len: 0 };
        }

        let mut min_id = i64::MAX;
        let mut max_id = i64::MIN;
        for id in state.nodes.keys().copied() {
            min_id = min_id.min(id);
            max_id = max_id.max(id);
        }

        if min_id == 0
            && max_id >= 0
            && usize::try_from(max_id)
                .ok()
                .and_then(|m| m.checked_add(1))
                .is_some_and(|span| span == state.nodes.len())
        {
            return NodeIndexer::Dense {
                len: state.nodes.len(),
            };
        }

        let node_ids = state.nodes.keys().copied().collect::<Vec<_>>();
        let id_to_idx = node_ids
            .iter()
            .enumerate()
            .map(|(idx, id)| (*id, idx))
            .collect::<HashMap<_, _>>();
        NodeIndexer::Sparse {
            node_ids,
            id_to_idx,
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeIndexer::Dense { len } => *len,
            NodeIndexer::Sparse { node_ids, .. } => node_ids.len(),
        }
    }

    fn idx_for_id(&self, id: i64) -> Option<usize> {
        match self {
            NodeIndexer::Dense { len } => usize::try_from(id).ok().filter(|idx| *idx < *len),
            NodeIndexer::Sparse { id_to_idx, .. } => id_to_idx.get(&id).copied(),
        }
    }

    fn id_for_idx(&self, idx: usize) -> Option<i64> {
        match self {
            NodeIndexer::Dense { len } => (idx < *len).then_some(idx as i64),
            NodeIndexer::Sparse { node_ids, .. } => node_ids.get(idx).copied(),
        }
    }
}

fn simulate_specialized_kernel_on_state(
    kernel: &SpecializedKernel,
    state: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    match kernel {
        SpecializedKernel::PageRank(spec) => simulate_pagerank_on_state(spec, state, max_iters),
        SpecializedKernel::Sssp(spec) => simulate_sssp_on_state(spec, state, max_iters),
        SpecializedKernel::ConnectedComponents(spec) => {
            simulate_connected_components_on_state(spec, state, max_iters)
        }
    }
}

fn simulate_pagerank_on_state(
    spec: &PageRankKernel,
    state: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);
    let mut rank = read_node_prop_f64_from_state(state, &indexer, &spec.target_property)?;
    let out_degree = read_node_prop_f64_from_state(state, &indexer, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0.0f64; indexer.len()];

    for _ in 0..max_iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
        }

        let mut changed = false;
        for idx in 0..indexer.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            let updated = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
            if (rank[idx] - updated).abs() >= 1e-6 {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            return write_node_prop_f64_to_state(state, &indexer, &spec.target_property, &next);
        }

        std::mem::swap(&mut rank, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_sssp_on_state(
    spec: &SsspKernel,
    state: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);

    let mut dist = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];
    let uniform_weight = sssp_uniform_weight_from_state(spec, state)?;

    for _ in 0..max_iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
        }

        let mut changed = false;
        for idx in 0..indexer.len() {
            let updated = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
            if updated != dist[idx] {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            return write_node_prop_i64_to_state(state, &indexer, &spec.target_property, &next);
        }

        std::mem::swap(&mut dist, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_connected_components_on_state(
    spec: &ConnectedComponentsKernel,
    state: &GraphState,
    max_iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);

    let mut labels = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];

    for _ in 0..max_iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
        }

        let mut changed = false;
        for idx in 0..indexer.len() {
            let updated = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
            if updated != labels[idx] {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            return write_node_prop_i64_to_state(state, &indexer, &spec.target_property, &next);
        }

        std::mem::swap(&mut labels, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_pagerank_on_state_for_iters(
    spec: &PageRankKernel,
    state: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);
    let mut rank = read_node_prop_f64_from_state(state, &indexer, &spec.target_property)?;
    let out_degree = read_node_prop_f64_from_state(state, &indexer, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0.0f64; indexer.len()];

    for _ in 0..iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
        }

        for idx in 0..indexer.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            next[idx] = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
        }

        std::mem::swap(&mut rank, &mut next);
    }

    write_node_prop_f64_to_state(state, &indexer, &spec.target_property, &rank)
}

fn simulate_sssp_on_state_for_iters(
    spec: &SsspKernel,
    state: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);

    let mut dist = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];
    let uniform_weight = sssp_uniform_weight_from_state(spec, state)?;

    for _ in 0..iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
        }

        for idx in 0..indexer.len() {
            next[idx] = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
        }

        std::mem::swap(&mut dist, &mut next);
    }

    write_node_prop_i64_to_state(state, &indexer, &spec.target_property, &dist)
}

fn simulate_connected_components_on_state_for_iters(
    spec: &ConnectedComponentsKernel,
    state: &GraphState,
    iters: usize,
) -> Result<GraphState, GasSimError> {
    let indexer = NodeIndexer::from_state(state);

    let mut labels = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];

    for _ in 0..iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
        }

        for idx in 0..indexer.len() {
            next[idx] = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
        }

        std::mem::swap(&mut labels, &mut next);
    }

    write_node_prop_i64_to_state(state, &indexer, &spec.target_property, &labels)
}

fn simulate_pagerank_on_state_for_iters_measure_only(
    spec: &PageRankKernel,
    state: &GraphState,
    iters: usize,
) -> Result<(), GasSimError> {
    let indexer = NodeIndexer::from_state(state);
    let mut rank = read_node_prop_f64_from_state(state, &indexer, &spec.target_property)?;
    let out_degree = read_node_prop_f64_from_state(state, &indexer, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0.0f64; indexer.len()];

    for _ in 0..iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    gathered[dst_idx] += rank[src_idx];
                    has_gathered[dst_idx] = true;
                }
            }
        }

        for idx in 0..indexer.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            next[idx] = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
        }

        std::mem::swap(&mut rank, &mut next);
    }

    Ok(())
}

fn simulate_sssp_on_state_for_iters_measure_only(
    spec: &SsspKernel,
    state: &GraphState,
    iters: usize,
) -> Result<(), GasSimError> {
    let indexer = NodeIndexer::from_state(state);
    let mut dist = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];
    let uniform_weight = sssp_uniform_weight_from_state(spec, state)?;

    for _ in 0..iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = edge.src as usize;
                        let dst_idx = edge.dst as usize;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                if let Some(weight) = uniform_weight {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                } else {
                    for edge in &state.edges {
                        let src_idx = *id_to_idx
                            .get(&edge.src)
                            .ok_or(GasSimError::MissingNode(edge.src))?;
                        let dst_idx = *id_to_idx
                            .get(&edge.dst)
                            .ok_or(GasSimError::MissingNode(edge.dst))?;
                        let weight = sssp_weight_from_edge_state(spec, edge)?;
                        let candidate = dist[src_idx] + weight;
                        if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                            gathered[dst_idx] = candidate;
                            has_gathered[dst_idx] = true;
                        }
                    }
                }
            }
        }

        for idx in 0..indexer.len() {
            next[idx] = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
        }

        std::mem::swap(&mut dist, &mut next);
    }

    Ok(())
}

fn simulate_connected_components_on_state_for_iters_measure_only(
    spec: &ConnectedComponentsKernel,
    state: &GraphState,
    iters: usize,
) -> Result<(), GasSimError> {
    let indexer = NodeIndexer::from_state(state);
    let mut labels = read_node_prop_i64_from_state(state, &indexer, &spec.target_property)?;
    let mut gathered = vec![0i64; indexer.len()];
    let mut has_gathered = vec![false; indexer.len()];
    let mut next = vec![0i64; indexer.len()];

    for _ in 0..iters {
        has_gathered.fill(false);

        match &indexer {
            NodeIndexer::Dense { .. } => {
                for edge in &state.edges {
                    let src_idx = edge.src as usize;
                    let dst_idx = edge.dst as usize;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
            NodeIndexer::Sparse { id_to_idx, .. } => {
                for edge in &state.edges {
                    let src_idx = *id_to_idx
                        .get(&edge.src)
                        .ok_or(GasSimError::MissingNode(edge.src))?;
                    let dst_idx = *id_to_idx
                        .get(&edge.dst)
                        .ok_or(GasSimError::MissingNode(edge.dst))?;
                    let candidate = labels[src_idx];
                    if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                        gathered[dst_idx] = candidate;
                        has_gathered[dst_idx] = true;
                    }
                }
            }
        }

        for idx in 0..indexer.len() {
            next[idx] = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
        }

        std::mem::swap(&mut labels, &mut next);
    }

    Ok(())
}

fn read_node_prop_i64_from_state(
    state: &GraphState,
    indexer: &NodeIndexer,
    prop: &str,
) -> Result<Vec<i64>, GasSimError> {
    match indexer {
        NodeIndexer::Dense { len } => {
            let mut values = vec![0_i64; *len];
            for (id, node) in &state.nodes {
                let idx = usize::try_from(*id).map_err(|_| GasSimError::MissingNode(*id))?;
                let value = node
                    .props
                    .get(prop)
                    .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
                values[idx] = match value {
                    Value::Int(v) => *v,
                    _ => return Err(GasSimError::TypeMismatch(prop.to_string())),
                };
            }
            Ok(values)
        }
        NodeIndexer::Sparse { id_to_idx, .. } => {
            let mut values = vec![0_i64; indexer.len()];
            for (id, node) in &state.nodes {
                let idx = *id_to_idx.get(id).ok_or(GasSimError::MissingNode(*id))?;
                let value = node
                    .props
                    .get(prop)
                    .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
                values[idx] = match value {
                    Value::Int(v) => *v,
                    _ => return Err(GasSimError::TypeMismatch(prop.to_string())),
                };
            }
            Ok(values)
        }
    }
}

fn read_node_prop_f64_from_state(
    state: &GraphState,
    indexer: &NodeIndexer,
    prop: &str,
) -> Result<Vec<f64>, GasSimError> {
    match indexer {
        NodeIndexer::Dense { len } => {
            let mut values = vec![0.0f64; *len];
            for (id, node) in &state.nodes {
                let idx = usize::try_from(*id).map_err(|_| GasSimError::MissingNode(*id))?;
                let value = node
                    .props
                    .get(prop)
                    .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
                values[idx] = match value {
                    Value::Int(v) => *v as f64,
                    Value::Float(v) => *v,
                    _ => return Err(GasSimError::TypeMismatch(prop.to_string())),
                };
            }
            Ok(values)
        }
        NodeIndexer::Sparse { id_to_idx, .. } => {
            let mut values = vec![0.0f64; indexer.len()];
            for (id, node) in &state.nodes {
                let idx = *id_to_idx.get(id).ok_or(GasSimError::MissingNode(*id))?;
                let value = node
                    .props
                    .get(prop)
                    .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
                values[idx] = match value {
                    Value::Int(v) => *v as f64,
                    Value::Float(v) => *v,
                    _ => return Err(GasSimError::TypeMismatch(prop.to_string())),
                };
            }
            Ok(values)
        }
    }
}

fn write_node_prop_i64_to_state(
    state: &GraphState,
    indexer: &NodeIndexer,
    prop: &str,
    values: &[i64],
) -> Result<GraphState, GasSimError> {
    let mut out = state.clone();
    for idx in 0..values.len() {
        let node_id = indexer.id_for_idx(idx).ok_or_else(|| {
            GasSimError::TypeMismatch("node index out of bounds for writeback".into())
        })?;
        let node = out
            .nodes
            .get_mut(&node_id)
            .ok_or(GasSimError::MissingNode(node_id))?;
        if let Some(slot) = node.props.get_mut(prop) {
            *slot = Value::Int(values[idx]);
        } else {
            node.props.insert(prop.to_string(), Value::Int(values[idx]));
        }
    }
    Ok(out)
}

fn write_node_prop_f64_to_state(
    state: &GraphState,
    indexer: &NodeIndexer,
    prop: &str,
    values: &[f64],
) -> Result<GraphState, GasSimError> {
    let mut out = state.clone();
    for idx in 0..values.len() {
        let node_id = indexer.id_for_idx(idx).ok_or_else(|| {
            GasSimError::TypeMismatch("node index out of bounds for writeback".into())
        })?;
        let node = out
            .nodes
            .get_mut(&node_id)
            .ok_or(GasSimError::MissingNode(node_id))?;
        if let Some(slot) = node.props.get_mut(prop) {
            *slot = Value::Float(values[idx]);
        } else {
            node.props
                .insert(prop.to_string(), Value::Float(values[idx]));
        }
    }
    Ok(out)
}

fn simulate_pagerank_kernel(
    spec: &PageRankKernel,
    state: &mut DenseGraph,
    max_iters: usize,
) -> Result<(), GasSimError> {
    let mut rank = read_node_prop_as_f64(state, &spec.target_property)?;
    let out_degree = read_node_prop_as_f64(state, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0.0f64; state.node_ids.len()];

    for _ in 0..max_iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        for edge in &state.edges {
            gathered[edge.dst_idx] += rank[edge.src_idx];
            has_gathered[edge.dst_idx] = true;
        }

        let mut changed = false;
        for idx in 0..state.node_ids.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            let updated = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
            if (rank[idx] - updated).abs() >= 1e-6 {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            write_node_prop_as_f64(state, &spec.target_property, &next);
            return Ok(());
        }

        std::mem::swap(&mut rank, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_sssp_kernel(
    spec: &SsspKernel,
    state: &mut DenseGraph,
    max_iters: usize,
) -> Result<(), GasSimError> {
    let mut dist = read_node_prop_as_i64(state, &spec.target_property)?;
    let uniform_weight = sssp_uniform_weight_from_dense(spec, state)?;
    let edge_weight = sssp_weight_column_from_dense(spec, state)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];

    for _ in 0..max_iters {
        has_gathered.fill(false);

        for (edge_idx, edge) in state.edges.iter().enumerate() {
            let weight = match (uniform_weight, edge_weight.as_ref()) {
                (Some(weight), _) => weight,
                (None, Some(column)) => column[edge_idx],
                (None, None) => unreachable!("SSSP weight source should be present"),
            };
            let candidate = dist[edge.src_idx] + weight;
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        let mut changed = false;
        for idx in 0..state.node_ids.len() {
            let updated = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
            if updated != dist[idx] {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            write_node_prop_as_i64(state, &spec.target_property, &next);
            return Ok(());
        }

        std::mem::swap(&mut dist, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_connected_components_kernel(
    spec: &ConnectedComponentsKernel,
    state: &mut DenseGraph,
    max_iters: usize,
) -> Result<(), GasSimError> {
    let mut labels = read_node_prop_as_i64(state, &spec.target_property)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];

    for _ in 0..max_iters {
        has_gathered.fill(false);

        for edge in &state.edges {
            let candidate = labels[edge.src_idx];
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        let mut changed = false;
        for idx in 0..state.node_ids.len() {
            let updated = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
            if updated != labels[idx] {
                changed = true;
            }
            next[idx] = updated;
        }

        if !changed {
            write_node_prop_as_i64(state, &spec.target_property, &next);
            return Ok(());
        }

        std::mem::swap(&mut labels, &mut next);
    }

    Err(GasSimError::NoConvergence)
}

fn simulate_pagerank_kernel_for_iters(
    spec: &PageRankKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<(), GasSimError> {
    let mut rank = read_node_prop_as_f64(state, &spec.target_property)?;
    let out_degree = read_node_prop_as_f64(state, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0.0f64; state.node_ids.len()];

    for _ in 0..iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        for edge in &state.edges {
            gathered[edge.dst_idx] += rank[edge.src_idx];
            has_gathered[edge.dst_idx] = true;
        }

        for idx in 0..state.node_ids.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            next[idx] = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
        }

        std::mem::swap(&mut rank, &mut next);
    }

    write_node_prop_as_f64(state, &spec.target_property, &rank);
    Ok(())
}

fn simulate_sssp_kernel_for_iters(
    spec: &SsspKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<(), GasSimError> {
    let mut dist = read_node_prop_as_i64(state, &spec.target_property)?;
    let uniform_weight = sssp_uniform_weight_from_dense(spec, state)?;
    let edge_weight = sssp_weight_column_from_dense(spec, state)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];

    for _ in 0..iters {
        has_gathered.fill(false);

        for (edge_idx, edge) in state.edges.iter().enumerate() {
            let weight = match (uniform_weight, edge_weight.as_ref()) {
                (Some(weight), _) => weight,
                (None, Some(column)) => column[edge_idx],
                (None, None) => unreachable!("SSSP weight source should be present"),
            };
            let candidate = dist[edge.src_idx] + weight;
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        for idx in 0..state.node_ids.len() {
            next[idx] = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
        }

        std::mem::swap(&mut dist, &mut next);
    }

    write_node_prop_as_i64(state, &spec.target_property, &dist);
    Ok(())
}

fn simulate_connected_components_kernel_for_iters(
    spec: &ConnectedComponentsKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<(), GasSimError> {
    let mut labels = read_node_prop_as_i64(state, &spec.target_property)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];

    for _ in 0..iters {
        has_gathered.fill(false);

        for edge in &state.edges {
            let candidate = labels[edge.src_idx];
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        for idx in 0..state.node_ids.len() {
            next[idx] = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
        }

        std::mem::swap(&mut labels, &mut next);
    }

    write_node_prop_as_i64(state, &spec.target_property, &labels);
    Ok(())
}

fn simulate_pagerank_kernel_for_iters_measure_only_timed(
    spec: &PageRankKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<f64, GasSimError> {
    let mut rank = read_node_prop_as_f64(state, &spec.target_property)?;
    let out_degree = read_node_prop_as_f64(state, &spec.out_degree_property)?;
    let mut gathered = vec![0.0f64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0.0f64; state.node_ids.len()];
    let start = Instant::now();

    for _ in 0..iters {
        gathered.fill(0.0);
        has_gathered.fill(false);

        for edge in &state.edges {
            gathered[edge.dst_idx] += rank[edge.src_idx];
            has_gathered[edge.dst_idx] = true;
        }

        for idx in 0..state.node_ids.len() {
            let gathered_or_current = if has_gathered[idx] {
                gathered[idx]
            } else {
                rank[idx]
            };
            next[idx] = spec.base + spec.scale * (gathered_or_current / out_degree[idx]);
        }

        std::mem::swap(&mut rank, &mut next);
    }

    Ok(start.elapsed().as_secs_f64())
}

fn simulate_sssp_kernel_for_iters_measure_only_timed(
    spec: &SsspKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<f64, GasSimError> {
    let mut dist = read_node_prop_as_i64(state, &spec.target_property)?;
    let uniform_weight = sssp_uniform_weight_from_dense(spec, state)?;
    let edge_weight = sssp_weight_column_from_dense(spec, state)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];
    let start = Instant::now();

    for _ in 0..iters {
        has_gathered.fill(false);

        for (edge_idx, edge) in state.edges.iter().enumerate() {
            let weight = match (uniform_weight, edge_weight.as_ref()) {
                (Some(weight), _) => weight,
                (None, Some(column)) => column[edge_idx],
                (None, None) => unreachable!("SSSP weight source should be present"),
            };
            let candidate = dist[edge.src_idx] + weight;
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        for idx in 0..state.node_ids.len() {
            next[idx] = if has_gathered[idx] {
                dist[idx].min(gathered[idx])
            } else {
                dist[idx]
            };
        }

        std::mem::swap(&mut dist, &mut next);
    }

    Ok(start.elapsed().as_secs_f64())
}

fn simulate_connected_components_kernel_for_iters_measure_only_timed(
    spec: &ConnectedComponentsKernel,
    state: &mut DenseGraph,
    iters: usize,
) -> Result<f64, GasSimError> {
    let mut labels = read_node_prop_as_i64(state, &spec.target_property)?;
    let mut gathered = vec![0i64; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];
    let mut next = vec![0i64; state.node_ids.len()];
    let start = Instant::now();

    for _ in 0..iters {
        has_gathered.fill(false);

        for edge in &state.edges {
            let candidate = labels[edge.src_idx];
            let dst_idx = edge.dst_idx;
            if !has_gathered[dst_idx] || candidate < gathered[dst_idx] {
                gathered[dst_idx] = candidate;
                has_gathered[dst_idx] = true;
            }
        }

        for idx in 0..state.node_ids.len() {
            next[idx] = if has_gathered[idx] {
                gathered[idx]
            } else {
                labels[idx]
            };
        }

        std::mem::swap(&mut labels, &mut next);
    }

    Ok(start.elapsed().as_secs_f64())
}

fn compile_pagerank_kernel(program: &GasProgram) -> Option<PageRankKernel> {
    if !is_edge_dst_lambda(&program.scatter.key_lambda) {
        return None;
    }
    if classify_reducer_kind(&program.gather.reducer) != Some(ReducerKind::Sum) {
        return None;
    }

    let target_property = program.apply.target_property.as_str();
    let src_property = parse_edge_src_prop_lambda(&program.scatter.value_lambda)?;
    if src_property != target_property {
        return None;
    }

    let apply = program.apply.lambda.as_ref()?;
    let (base, scale, out_degree_property) = parse_pagerank_apply_lambda(apply)?;

    Some(PageRankKernel {
        target_property: target_property.to_string(),
        out_degree_property,
        base,
        scale,
    })
}

fn compile_sssp_kernel(program: &GasProgram) -> Option<SsspKernel> {
    if !is_edge_dst_lambda(&program.scatter.key_lambda) {
        return None;
    }
    if classify_reducer_kind(&program.gather.reducer) != Some(ReducerKind::Min) {
        return None;
    }

    let target_property = program.apply.target_property.as_str();
    let (src_property, edge_weight_property, edge_weight_constant) =
        parse_edge_src_plus_weight_lambda(&program.scatter.value_lambda)?;
    if src_property != target_property {
        return None;
    }

    let apply = program.apply.lambda.as_ref()?;
    if !is_min_with_self_apply_lambda(apply, target_property) {
        return None;
    }

    Some(SsspKernel {
        target_property: target_property.to_string(),
        edge_weight_property,
        edge_weight_constant,
    })
}

fn compile_connected_components_kernel(program: &GasProgram) -> Option<ConnectedComponentsKernel> {
    if !is_edge_dst_lambda(&program.scatter.key_lambda) {
        return None;
    }
    if classify_reducer_kind(&program.gather.reducer) != Some(ReducerKind::Min) {
        return None;
    }
    if program.apply.lambda.is_some() {
        return None;
    }

    let target_property = program.apply.target_property.as_str();
    let src_property = parse_edge_src_prop_lambda(&program.scatter.value_lambda)?;
    if src_property != target_property {
        return None;
    }

    Some(ConnectedComponentsKernel {
        target_property: target_property.to_string(),
    })
}

fn is_edge_dst_lambda(lambda: &IrLambda) -> bool {
    if lambda.params.len() != 1 {
        return false;
    }
    let edge = lambda.params[0].as_str();
    is_edge_dst_expr(&lambda.body, edge)
}

fn is_edge_dst_expr(expr: &IrExpr, edge_param: &str) -> bool {
    match expr {
        IrExpr::MemberAccess {
            target,
            access: Accessor::Property(name),
        } => {
            if name.as_str() == "dst" {
                return is_identifier_expr(target, edge_param);
            }
            if name.as_str() == "id" {
                return matches!(
                    target.as_ref(),
                    IrExpr::MemberAccess {
                        target: inner,
                        access: Accessor::Property(prefix),
                    } if prefix.as_str() == "dst" && is_identifier_expr(inner, edge_param)
                );
            }
            false
        }
        _ => false,
    }
}

fn parse_edge_src_prop_lambda(lambda: &IrLambda) -> Option<String> {
    if lambda.params.len() != 1 {
        return None;
    }
    parse_edge_src_property(&lambda.body, lambda.params[0].as_str())
}

fn parse_edge_src_plus_weight_lambda(
    lambda: &IrLambda,
) -> Option<(String, Option<String>, Option<i64>)> {
    if lambda.params.len() != 1 {
        return None;
    }
    let edge = lambda.params[0].as_str();
    let IrExpr::Binary {
        op: BinaryOp::Add,
        left,
        right,
    } = &lambda.body
    else {
        return None;
    };

    if let (Some(src_prop), Some(edge_prop)) = (
        parse_edge_src_property(left, edge),
        parse_edge_direct_property(right, edge),
    ) {
        return Some((src_prop, Some(edge_prop), None));
    }
    if let (Some(src_prop), Some(edge_prop)) = (
        parse_edge_src_property(right, edge),
        parse_edge_direct_property(left, edge),
    ) {
        return Some((src_prop, Some(edge_prop), None));
    }
    if let (Some(src_prop), Some(weight)) = (
        parse_edge_src_property(left, edge),
        parse_int_literal(right),
    ) {
        return Some((src_prop, None, Some(weight)));
    }
    if let (Some(src_prop), Some(weight)) = (
        parse_edge_src_property(right, edge),
        parse_int_literal(left),
    ) {
        return Some((src_prop, None, Some(weight)));
    }
    None
}

fn parse_edge_src_property(expr: &IrExpr, edge_param: &str) -> Option<String> {
    let IrExpr::MemberAccess {
        target,
        access: Accessor::Property(prop),
    } = expr
    else {
        return None;
    };
    let IrExpr::MemberAccess {
        target: inner,
        access: Accessor::Property(prefix),
    } = target.as_ref()
    else {
        return None;
    };
    if prefix.as_str() != "src" || !is_identifier_expr(inner, edge_param) {
        return None;
    }
    Some(prop.as_str().to_string())
}

fn parse_edge_direct_property(expr: &IrExpr, edge_param: &str) -> Option<String> {
    let IrExpr::MemberAccess {
        target,
        access: Accessor::Property(prop),
    } = expr
    else {
        return None;
    };
    if !is_identifier_expr(target, edge_param) {
        return None;
    }
    match prop.as_str() {
        "src" | "dst" => None,
        _ => Some(prop.as_str().to_string()),
    }
}

fn parse_pagerank_apply_lambda(lambda: &IrLambda) -> Option<(f64, f64, String)> {
    if lambda.params.len() != 1 {
        return None;
    }
    let gathered = lambda.params[0].as_str();
    parse_add_const_scaled_div(&lambda.body, gathered)
}

fn parse_add_const_scaled_div(expr: &IrExpr, gathered_param: &str) -> Option<(f64, f64, String)> {
    let IrExpr::Binary {
        op: BinaryOp::Add,
        left,
        right,
    } = expr
    else {
        return None;
    };

    if let Some(base) = parse_number_literal(left) {
        if let Some((scale, denom_prop)) = parse_scale_mul_div(right, gathered_param) {
            return Some((base, scale, denom_prop));
        }
    }
    if let Some(base) = parse_number_literal(right) {
        if let Some((scale, denom_prop)) = parse_scale_mul_div(left, gathered_param) {
            return Some((base, scale, denom_prop));
        }
    }
    None
}

fn parse_scale_mul_div(expr: &IrExpr, gathered_param: &str) -> Option<(f64, String)> {
    let IrExpr::Binary {
        op: BinaryOp::Mul,
        left,
        right,
    } = expr
    else {
        return None;
    };

    if let Some(scale) = parse_number_literal(left) {
        if let Some(denom_prop) = parse_gathered_div_self_prop(right, gathered_param) {
            return Some((scale, denom_prop));
        }
    }
    if let Some(scale) = parse_number_literal(right) {
        if let Some(denom_prop) = parse_gathered_div_self_prop(left, gathered_param) {
            return Some((scale, denom_prop));
        }
    }
    None
}

fn parse_gathered_div_self_prop(expr: &IrExpr, gathered_param: &str) -> Option<String> {
    let IrExpr::Binary {
        op: BinaryOp::Div,
        left,
        right,
    } = expr
    else {
        return None;
    };
    if !is_identifier_expr(left, gathered_param) {
        return None;
    }
    parse_self_property(right)
}

fn parse_self_property(expr: &IrExpr) -> Option<String> {
    let IrExpr::MemberAccess {
        target,
        access: Accessor::Property(prop),
    } = expr
    else {
        return None;
    };
    if !is_identifier_expr(target, "self") {
        return None;
    }
    Some(prop.as_str().to_string())
}

fn is_min_with_self_apply_lambda(lambda: &IrLambda, self_prop: &str) -> bool {
    if lambda.params.len() != 1 {
        return false;
    }
    let incoming = lambda.params[0].as_str();
    let IrExpr::Ternary {
        condition,
        then_expr,
        else_expr,
    } = &lambda.body
    else {
        return false;
    };
    is_min_ternary_with_pair(condition, then_expr, else_expr, self_prop, incoming)
}

fn is_min_ternary_with_pair(
    condition: &IrExpr,
    then_expr: &IrExpr,
    else_expr: &IrExpr,
    self_prop: &str,
    incoming: &str,
) -> bool {
    match condition {
        IrExpr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } if is_self_prop_expr(left, self_prop) && is_identifier_expr(right, incoming) => {
            is_identifier_expr(then_expr, incoming) && is_self_prop_expr(else_expr, self_prop)
        }
        IrExpr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } if is_self_prop_expr(left, self_prop) && is_identifier_expr(right, incoming) => {
            is_self_prop_expr(then_expr, self_prop) && is_identifier_expr(else_expr, incoming)
        }
        IrExpr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } if is_identifier_expr(left, incoming) && is_self_prop_expr(right, self_prop) => {
            is_self_prop_expr(then_expr, self_prop) && is_identifier_expr(else_expr, incoming)
        }
        IrExpr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } if is_identifier_expr(left, incoming) && is_self_prop_expr(right, self_prop) => {
            is_identifier_expr(then_expr, incoming) && is_self_prop_expr(else_expr, self_prop)
        }
        _ => false,
    }
}

fn parse_number_literal(expr: &IrExpr) -> Option<f64> {
    match expr {
        IrExpr::Literal(AstLiteral::Int(i)) => Some(*i as f64),
        IrExpr::Literal(AstLiteral::Float(f)) => f.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_int_literal(expr: &IrExpr) -> Option<i64> {
    match expr {
        IrExpr::Literal(AstLiteral::Int(i)) => Some(*i),
        _ => None,
    }
}

fn is_identifier_expr(expr: &IrExpr, name: &str) -> bool {
    matches!(expr, IrExpr::Identifier(id) if id.as_str() == name)
}

fn is_self_prop_expr(expr: &IrExpr, prop: &str) -> bool {
    matches!(
        expr,
        IrExpr::MemberAccess {
            target,
            access: Accessor::Property(name),
        } if name.as_str() == prop && is_identifier_expr(target, "self")
    )
}

fn read_node_prop_as_f64(state: &DenseGraph, prop: &str) -> Result<Vec<f64>, GasSimError> {
    let values = state
        .node_props
        .get(prop)
        .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
    values
        .iter()
        .map(|value| match value {
            Value::Int(v) => Ok(*v as f64),
            Value::Float(v) => Ok(*v),
            _ => Err(GasSimError::TypeMismatch(prop.to_string())),
        })
        .collect()
}

fn read_node_prop_as_i64(state: &DenseGraph, prop: &str) -> Result<Vec<i64>, GasSimError> {
    let values = state
        .node_props
        .get(prop)
        .ok_or_else(|| GasSimError::MissingNodeProp(prop.to_string()))?;
    values
        .iter()
        .map(|value| match value {
            Value::Int(v) => Ok(*v),
            _ => Err(GasSimError::TypeMismatch(prop.to_string())),
        })
        .collect()
}

fn read_edge_prop_as_i64(state: &DenseGraph, prop: &str) -> Result<Vec<i64>, GasSimError> {
    let values = state
        .edge_props
        .get(prop)
        .ok_or_else(|| GasSimError::MissingEdgeProp(prop.to_string()))?;
    values
        .iter()
        .map(|value| match value {
            Value::Int(v) => Ok(*v),
            _ => Err(GasSimError::TypeMismatch(prop.to_string())),
        })
        .collect()
}

fn write_node_prop_as_f64(state: &mut DenseGraph, prop: &str, values: &[f64]) {
    state.node_props.insert(
        prop.to_string(),
        values.iter().copied().map(Value::Float).collect(),
    );
}

fn write_node_prop_as_i64(state: &mut DenseGraph, prop: &str, values: &[i64]) {
    state.node_props.insert(
        prop.to_string(),
        values.iter().copied().map(Value::Int).collect(),
    );
}

#[derive(Clone, Copy)]
enum CompileKind {
    Edge,
    Generic,
    Apply,
}

struct CompileCtx {
    kind: CompileKind,
    params: HashMap<String, usize>,
}

impl CompileCtx {
    fn new(kind: CompileKind, lambda: &IrLambda) -> Self {
        let params = lambda
            .params
            .iter()
            .enumerate()
            .map(|(idx, id)| (id.as_str().to_string(), idx))
            .collect();
        Self { kind, params }
    }
}

#[derive(Clone, Debug)]
struct CompiledLambda {
    expr: CompiledExpr,
    arity: usize,
    enforce_arity: bool,
}

impl CompiledLambda {
    fn compile(kind: CompileKind, lambda: &IrLambda) -> Result<Self, GasSimError> {
        let ctx = CompileCtx::new(kind, lambda);
        let expr = compile_expr(&lambda.body, &ctx)?;
        Ok(Self {
            expr,
            arity: lambda.params.len(),
            enforce_arity: !matches!(kind, CompileKind::Edge),
        })
    }

    fn eval(
        &self,
        graph: &DenseGraph,
        edge_idx: Option<usize>,
        self_node_idx: Option<usize>,
        params: &[Value],
    ) -> Result<Value, GasSimError> {
        if self.enforce_arity && params.len() != self.arity {
            return Err(GasSimError::TypeMismatch(
                "compiled lambda argument arity mismatch".into(),
            ));
        }
        let ctx = CompiledEvalCtx {
            graph,
            edge_idx,
            self_node_idx,
            params,
        };
        eval_compiled_expr(&self.expr, &ctx)
    }
}

#[derive(Clone, Debug)]
struct CompiledPlan {
    scatter_key: CompiledScatterKey,
    scatter_value: CompiledLambda,
    gather_reducer: CompiledReducer,
    apply: Option<CompiledLambda>,
    target_property: String,
}

impl CompiledPlan {
    fn from_program(program: &GasProgram) -> Result<Self, GasSimError> {
        let scatter_key_lambda =
            CompiledLambda::compile(CompileKind::Edge, &program.scatter.key_lambda)?;
        let scatter_key = if matches!(scatter_key_lambda.expr, CompiledExpr::EdgeDstId) {
            CompiledScatterKey::EdgeDst
        } else {
            CompiledScatterKey::Lambda(scatter_key_lambda)
        };
        let scatter_value =
            CompiledLambda::compile(CompileKind::Edge, &program.scatter.value_lambda)?;
        let gather_reducer = if let Some(kind) = classify_reducer_kind(&program.gather.reducer) {
            CompiledReducer::Fast(kind)
        } else {
            CompiledReducer::Lambda(CompiledLambda::compile(
                CompileKind::Generic,
                &program.gather.reducer,
            )?)
        };
        let apply = match &program.apply.lambda {
            Some(lambda) => Some(CompiledLambda::compile(CompileKind::Apply, lambda)?),
            None => None,
        };

        Ok(Self {
            scatter_key,
            scatter_value,
            gather_reducer,
            apply,
            target_property: program.apply.target_property.as_str().to_string(),
        })
    }
}

#[derive(Clone, Debug)]
enum CompiledScatterKey {
    EdgeDst,
    Lambda(CompiledLambda),
}

#[derive(Clone, Debug)]
enum CompiledReducer {
    Fast(ReducerKind),
    Lambda(CompiledLambda),
}

impl CompiledReducer {
    fn reduce(&self, state: &DenseGraph, lhs: Value, rhs: Value) -> Result<Value, GasSimError> {
        match self {
            CompiledReducer::Fast(ReducerKind::Sum) => eval_binary(BinaryOp::Add, lhs, rhs),
            CompiledReducer::Fast(ReducerKind::Min) => {
                let l = lhs.as_f64()?;
                let r = rhs.as_f64()?;
                Ok(if l <= r { lhs } else { rhs })
            }
            CompiledReducer::Fast(ReducerKind::Max) => {
                let l = lhs.as_f64()?;
                let r = rhs.as_f64()?;
                Ok(if l >= r { lhs } else { rhs })
            }
            CompiledReducer::Lambda(lambda) => lambda.eval(state, None, None, &[lhs, rhs]),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReducerKind {
    Sum,
    Min,
    Max,
}

fn classify_reducer_kind(lambda: &IrLambda) -> Option<ReducerKind> {
    if lambda.params.len() != 2 {
        return None;
    }
    let acc = lambda.params[0].as_str();
    let incoming = lambda.params[1].as_str();

    match &lambda.body {
        IrExpr::Binary {
            op: BinaryOp::Add,
            left,
            right,
        } if matches_param_pair(left, right, acc, incoming) => Some(ReducerKind::Sum),
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => classify_min_max_reducer(condition, then_expr, else_expr, acc, incoming),
        _ => None,
    }
}

fn classify_min_max_reducer(
    condition: &IrExpr,
    then_expr: &IrExpr,
    else_expr: &IrExpr,
    acc: &str,
    incoming: &str,
) -> Option<ReducerKind> {
    match condition {
        IrExpr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } if matches_param(left, acc) && matches_param(right, incoming) => {
            if matches_param(then_expr, incoming) && matches_param(else_expr, acc) {
                Some(ReducerKind::Min)
            } else if matches_param(then_expr, acc) && matches_param(else_expr, incoming) {
                Some(ReducerKind::Max)
            } else {
                None
            }
        }
        IrExpr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } if matches_param(left, acc) && matches_param(right, incoming) => {
            if matches_param(then_expr, acc) && matches_param(else_expr, incoming) {
                Some(ReducerKind::Min)
            } else if matches_param(then_expr, incoming) && matches_param(else_expr, acc) {
                Some(ReducerKind::Max)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn matches_param(expr: &IrExpr, param: &str) -> bool {
    matches!(expr, IrExpr::Identifier(id) if id.as_str() == param)
}

fn matches_param_pair(lhs: &IrExpr, rhs: &IrExpr, a: &str, b: &str) -> bool {
    (matches_param(lhs, a) && matches_param(rhs, b))
        || (matches_param(lhs, b) && matches_param(rhs, a))
}

#[derive(Clone, Debug)]
enum CompiledExpr {
    Param(usize),
    SelfNode,
    SelfProp(String),
    EdgeSrcId,
    EdgeDstId,
    EdgeProp(String),
    EdgeSrcProp(String),
    EdgeDstProp(String),
    Literal(Value),
    MemberProp {
        target: Box<CompiledExpr>,
        name: String,
    },
    MemberIndex {
        target: Box<CompiledExpr>,
        idx: u32,
    },
    Call {
        function: String,
        args: Vec<CompiledExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<CompiledExpr>,
        right: Box<CompiledExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<CompiledExpr>,
    },
    Ternary {
        condition: Box<CompiledExpr>,
        then_expr: Box<CompiledExpr>,
        else_expr: Box<CompiledExpr>,
    },
}

fn compile_expr(expr: &IrExpr, ctx: &CompileCtx) -> Result<CompiledExpr, GasSimError> {
    match expr {
        IrExpr::Identifier(id) => {
            if id.as_str() == "self" && matches!(ctx.kind, CompileKind::Apply) {
                return Ok(CompiledExpr::SelfNode);
            }
            if let Some(idx) = ctx.params.get(id.as_str()) {
                if matches!(ctx.kind, CompileKind::Edge) {
                    return Err(GasSimError::UnsupportedOp(
                        "direct edge parameter usage in compiled path".into(),
                    ));
                }
                Ok(CompiledExpr::Param(*idx))
            } else {
                Err(GasSimError::UnsupportedOp(format!(
                    "unknown identifier '{}' in compiled path",
                    id.as_str()
                )))
            }
        }
        IrExpr::Literal(AstLiteral::Int(i)) => Ok(CompiledExpr::Literal(Value::Int(*i))),
        IrExpr::Literal(AstLiteral::Float(f)) => Ok(CompiledExpr::Literal(Value::Float(
            f.parse::<f64>()
                .map_err(|_| GasSimError::TypeMismatch("invalid float literal".into()))?,
        ))),
        IrExpr::Literal(AstLiteral::Bool(b)) => Ok(CompiledExpr::Literal(Value::Bool(*b))),
        IrExpr::MemberAccess { target, access } => match access {
            Accessor::Property(name) => compile_member_property(target, name.as_str(), ctx),
            Accessor::Index(idx) => Ok(CompiledExpr::MemberIndex {
                target: Box::new(compile_expr(target, ctx)?),
                idx: *idx,
            }),
        },
        IrExpr::Call { function, args } => Ok(CompiledExpr::Call {
            function: function.as_str().to_string(),
            args: args
                .iter()
                .map(|arg| compile_expr(arg, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        IrExpr::Binary { op, left, right } => Ok(CompiledExpr::Binary {
            op: *op,
            left: Box::new(compile_expr(left, ctx)?),
            right: Box::new(compile_expr(right, ctx)?),
        }),
        IrExpr::Unary { op, expr } => Ok(CompiledExpr::Unary {
            op: *op,
            expr: Box::new(compile_expr(expr, ctx)?),
        }),
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => Ok(CompiledExpr::Ternary {
            condition: Box::new(compile_expr(condition, ctx)?),
            then_expr: Box::new(compile_expr(then_expr, ctx)?),
            else_expr: Box::new(compile_expr(else_expr, ctx)?),
        }),
    }
}

fn compile_member_property(
    target: &IrExpr,
    name: &str,
    ctx: &CompileCtx,
) -> Result<CompiledExpr, GasSimError> {
    if matches!(ctx.kind, CompileKind::Edge) {
        if let Some(edge_expr) = compile_edge_member(target, name, &ctx.params) {
            return Ok(edge_expr);
        }
    }
    if matches!(ctx.kind, CompileKind::Apply)
        && matches!(target, IrExpr::Identifier(id) if id.as_str() == "self")
    {
        if name == "id" {
            return Ok(CompiledExpr::SelfNode);
        }
        return Ok(CompiledExpr::SelfProp(name.to_string()));
    }
    Ok(CompiledExpr::MemberProp {
        target: Box::new(compile_expr(target, ctx)?),
        name: name.to_string(),
    })
}

fn compile_edge_member(
    target: &IrExpr,
    name: &str,
    params: &HashMap<String, usize>,
) -> Option<CompiledExpr> {
    let edge_param = params
        .iter()
        .find_map(|(name, idx)| (*idx == 0).then_some(name.as_str()))?;

    match target {
        IrExpr::Identifier(id) if id.as_str() == edge_param => match name {
            "src" => Some(CompiledExpr::EdgeSrcId),
            "dst" => Some(CompiledExpr::EdgeDstId),
            prop => Some(CompiledExpr::EdgeProp(prop.to_string())),
        },
        IrExpr::MemberAccess {
            target: inner,
            access: Accessor::Property(prefix),
        } if matches!(inner.as_ref(), IrExpr::Identifier(id) if id.as_str() == edge_param) => {
            match prefix.as_str() {
                "src" => {
                    if name == "id" {
                        Some(CompiledExpr::EdgeSrcId)
                    } else {
                        Some(CompiledExpr::EdgeSrcProp(name.to_string()))
                    }
                }
                "dst" => {
                    if name == "id" {
                        Some(CompiledExpr::EdgeDstId)
                    } else {
                        Some(CompiledExpr::EdgeDstProp(name.to_string()))
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

struct CompiledEvalCtx<'a> {
    graph: &'a DenseGraph,
    edge_idx: Option<usize>,
    self_node_idx: Option<usize>,
    params: &'a [Value],
}

fn eval_compiled_expr(
    expr: &CompiledExpr,
    ctx: &CompiledEvalCtx<'_>,
) -> Result<Value, GasSimError> {
    match expr {
        CompiledExpr::Param(idx) => {
            ctx.params.get(*idx).cloned().ok_or_else(|| {
                GasSimError::TypeMismatch("compiled param index out of bounds".into())
            })
        }
        CompiledExpr::SelfNode => {
            let node_idx = ctx
                .self_node_idx
                .ok_or_else(|| GasSimError::TypeMismatch("self node is unavailable".into()))?;
            Ok(Value::NodeRef(ctx.graph.node_ids[node_idx]))
        }
        CompiledExpr::SelfProp(name) => {
            let node_idx = ctx
                .self_node_idx
                .ok_or_else(|| GasSimError::TypeMismatch("self node is unavailable".into()))?;
            ctx.graph
                .node_prop(node_idx, name)
                .cloned()
                .ok_or_else(|| GasSimError::MissingNodeProp(name.clone()))
        }
        CompiledExpr::EdgeSrcId => {
            let edge = ctx
                .edge_idx
                .and_then(|idx| ctx.graph.edges.get(idx))
                .ok_or_else(|| GasSimError::TypeMismatch("edge context is unavailable".into()))?;
            Ok(Value::NodeRef(ctx.graph.node_ids[edge.src_idx]))
        }
        CompiledExpr::EdgeDstId => {
            let edge = ctx
                .edge_idx
                .and_then(|idx| ctx.graph.edges.get(idx))
                .ok_or_else(|| GasSimError::TypeMismatch("edge context is unavailable".into()))?;
            Ok(Value::NodeRef(ctx.graph.node_ids[edge.dst_idx]))
        }
        CompiledExpr::EdgeProp(name) => {
            let edge_idx = ctx
                .edge_idx
                .ok_or_else(|| GasSimError::TypeMismatch("edge context is unavailable".into()))?;
            ctx.graph
                .edge_prop(edge_idx, name)
                .cloned()
                .ok_or_else(|| GasSimError::MissingEdgeProp(name.clone()))
        }
        CompiledExpr::EdgeSrcProp(name) => {
            let edge = ctx
                .edge_idx
                .and_then(|idx| ctx.graph.edges.get(idx))
                .ok_or_else(|| GasSimError::TypeMismatch("edge context is unavailable".into()))?;
            ctx.graph
                .node_prop(edge.src_idx, name)
                .cloned()
                .ok_or_else(|| GasSimError::MissingNodeProp(name.clone()))
        }
        CompiledExpr::EdgeDstProp(name) => {
            let edge = ctx
                .edge_idx
                .and_then(|idx| ctx.graph.edges.get(idx))
                .ok_or_else(|| GasSimError::TypeMismatch("edge context is unavailable".into()))?;
            ctx.graph
                .node_prop(edge.dst_idx, name)
                .cloned()
                .ok_or_else(|| GasSimError::MissingNodeProp(name.clone()))
        }
        CompiledExpr::Literal(value) => Ok(value.clone()),
        CompiledExpr::MemberProp { target, name } => {
            let base = eval_compiled_expr(target, ctx)?;
            eval_member_prop(base, name, ctx.graph)
        }
        CompiledExpr::MemberIndex { target, idx } => {
            let base = eval_compiled_expr(target, ctx)?;
            eval_member_index(base, *idx)
        }
        CompiledExpr::Call { function, args } => {
            let values = args
                .iter()
                .map(|arg| eval_compiled_expr(arg, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            eval_call(function, &values)
        }
        CompiledExpr::Binary { op, left, right } => {
            let lhs = eval_compiled_expr(left, ctx)?;
            let rhs = eval_compiled_expr(right, ctx)?;
            eval_binary(*op, lhs, rhs)
        }
        CompiledExpr::Unary { op, expr } => {
            let value = eval_compiled_expr(expr, ctx)?;
            eval_unary(*op, value)
        }
        CompiledExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            let cond = eval_compiled_expr(condition, ctx)?.as_bool()?;
            if cond {
                eval_compiled_expr(then_expr, ctx)
            } else {
                eval_compiled_expr(else_expr, ctx)
            }
        }
    }
}

fn eval_member_prop(base: Value, name: &str, graph: &DenseGraph) -> Result<Value, GasSimError> {
    match base {
        Value::EdgeRef { src, dst, props } => match name {
            "src" => Ok(Value::NodeRef(src)),
            "dst" => Ok(Value::NodeRef(dst)),
            prop => props
                .get(prop)
                .cloned()
                .ok_or_else(|| GasSimError::MissingEdgeProp(prop.to_string())),
        },
        Value::NodeRef(id) => {
            if name == "id" {
                return Ok(Value::Int(id));
            }
            let node_idx = graph
                .node_idx_for_id(id)
                .ok_or(GasSimError::MissingNode(id))?;
            graph
                .node_prop(node_idx, name)
                .cloned()
                .ok_or_else(|| GasSimError::MissingNodeProp(name.to_string()))
        }
        other => Err(GasSimError::UnsupportedOp(format!(
            "member property on {other:?}"
        ))),
    }
}

fn eval_member_index(base: Value, idx: u32) -> Result<Value, GasSimError> {
    let idx = idx as usize;
    match base {
        Value::Tuple(items) => items
            .get(idx)
            .cloned()
            .ok_or_else(|| GasSimError::TypeMismatch("tuple index".into())),
        Value::Vector(values) => values
            .get(idx)
            .copied()
            .map(Value::Float)
            .ok_or_else(|| GasSimError::TypeMismatch("vector index".into())),
        Value::Array(items) => items
            .get(idx)
            .cloned()
            .ok_or_else(|| GasSimError::TypeMismatch("array index".into())),
        other => Err(GasSimError::UnsupportedOp(format!(
            "member index on {other:?}"
        ))),
    }
}

fn execute_iteration_compiled(
    plan: &CompiledPlan,
    state: &mut DenseGraph,
) -> Result<bool, GasSimError> {
    let hot_scatter = HotScatter::from_lambda(&plan.scatter_value, state);
    let mut gathered: Vec<Value> = vec![Value::Unit; state.node_ids.len()];
    let mut has_gathered = vec![false; state.node_ids.len()];

    for edge_idx in 0..state.edges.len() {
        let dst_idx = match &plan.scatter_key {
            CompiledScatterKey::EdgeDst => Some(state.edges[edge_idx].dst_idx),
            CompiledScatterKey::Lambda(lambda) => {
                let key = lambda.eval(state, Some(edge_idx), None, &[])?;
                key_to_node_idx(&key, state)?
            }
        };
        let Some(dst_idx) = dst_idx else {
            continue;
        };

        let value = if let Some(hot) = hot_scatter.as_ref() {
            hot.eval(state, edge_idx)?
        } else {
            plan.scatter_value.eval(state, Some(edge_idx), None, &[])?
        };

        if has_gathered[dst_idx] {
            reduce_in_place_compiled(&plan.gather_reducer, state, &mut gathered[dst_idx], value)?;
        } else {
            gathered[dst_idx] = value;
            has_gathered[dst_idx] = true;
        }
    }

    let current = state.node_props.get(&plan.target_property);
    let mut next = Vec::with_capacity(state.node_ids.len());
    let mut changed = false;

    for node_idx in 0..state.node_ids.len() {
        let current_prop = current
            .and_then(|values| values.get(node_idx))
            .unwrap_or(&Value::Unit);
        let gathered_val = if has_gathered[node_idx] {
            gathered[node_idx].clone()
        } else {
            current_prop.clone()
        };
        let result = if let Some(apply) = &plan.apply {
            apply.eval(state, None, Some(node_idx), &[gathered_val])?
        } else {
            gathered_val
        };
        if !current_prop.approx_eq(&result) {
            changed = true;
        }
        next.push(result);
    }

    state.node_props.insert(plan.target_property.clone(), next);
    Ok(changed)
}

enum HotScatter<'a> {
    EdgeProp(&'a [Value]),
    EdgeSrcProp(&'a [Value]),
    EdgeDstProp(&'a [Value]),
    EdgeSrcPropPlusIntConst {
        src_values: &'a [Value],
        constant: i64,
    },
    EdgeSrcPropPlusEdgeProp {
        src_values: &'a [Value],
        edge_values: &'a [Value],
    },
}

impl<'a> HotScatter<'a> {
    fn from_lambda(lambda: &CompiledLambda, state: &'a DenseGraph) -> Option<Self> {
        match &lambda.expr {
            CompiledExpr::EdgeProp(name) => state.edge_props.get(name).map(|v| Self::EdgeProp(v)),
            CompiledExpr::EdgeSrcProp(name) => {
                state.node_props.get(name).map(|v| Self::EdgeSrcProp(v))
            }
            CompiledExpr::EdgeDstProp(name) => {
                state.node_props.get(name).map(|v| Self::EdgeDstProp(v))
            }
            CompiledExpr::Binary {
                op: BinaryOp::Add,
                left,
                right,
            } => Self::from_add_expr(left.as_ref(), right.as_ref(), state)
                .or_else(|| Self::from_add_expr(right.as_ref(), left.as_ref(), state)),
            _ => None,
        }
    }

    fn from_add_expr(lhs: &CompiledExpr, rhs: &CompiledExpr, state: &'a DenseGraph) -> Option<Self> {
        let CompiledExpr::EdgeSrcProp(src_name) = lhs else {
            return None;
        };
        let src_values = state.node_props.get(src_name)?;
        match rhs {
            CompiledExpr::Literal(Value::Int(constant)) => Some(Self::EdgeSrcPropPlusIntConst {
                src_values,
                constant: *constant,
            }),
            CompiledExpr::EdgeProp(edge_name) => {
                let edge_values = state.edge_props.get(edge_name)?;
                Some(Self::EdgeSrcPropPlusEdgeProp {
                    src_values,
                    edge_values,
                })
            }
            _ => None,
        }
    }

    fn eval(&self, state: &DenseGraph, edge_idx: usize) -> Result<Value, GasSimError> {
        let edge = &state.edges[edge_idx];
        match self {
            HotScatter::EdgeProp(values) => Ok(values[edge_idx].clone()),
            HotScatter::EdgeSrcProp(values) => Ok(values[edge.src_idx].clone()),
            HotScatter::EdgeDstProp(values) => Ok(values[edge.dst_idx].clone()),
            HotScatter::EdgeSrcPropPlusIntConst {
                src_values,
                constant,
            } => match &src_values[edge.src_idx] {
                Value::Int(v) => Ok(Value::Int(*v + *constant)),
                _ => Err(GasSimError::TypeMismatch("compiled edge scatter int add".into())),
            },
            HotScatter::EdgeSrcPropPlusEdgeProp {
                src_values,
                edge_values,
            } => match (&src_values[edge.src_idx], &edge_values[edge_idx]) {
                (Value::Int(lhs), Value::Int(rhs)) => Ok(Value::Int(*lhs + *rhs)),
                _ => Err(GasSimError::TypeMismatch(
                    "compiled edge scatter property add".into(),
                )),
            },
        }
    }
}

fn reduce_in_place_compiled(
    reducer: &CompiledReducer,
    state: &DenseGraph,
    slot: &mut Value,
    incoming: Value,
) -> Result<(), GasSimError> {
    let handled = match reducer {
        CompiledReducer::Fast(ReducerKind::Sum) => match (&mut *slot, &incoming) {
            (Value::Int(lhs), Value::Int(rhs)) => {
                *lhs += *rhs;
                true
            }
            (Value::Float(lhs), Value::Float(rhs)) => {
                *lhs += *rhs;
                true
            }
            _ => false,
        },
        CompiledReducer::Fast(ReducerKind::Min) => match (&mut *slot, &incoming) {
            (Value::Int(lhs), Value::Int(rhs)) => {
                if *rhs < *lhs {
                    *lhs = *rhs;
                }
                true
            }
            (Value::Float(lhs), Value::Float(rhs)) => {
                if *rhs < *lhs {
                    *lhs = *rhs;
                }
                true
            }
            _ => false,
        },
        CompiledReducer::Fast(ReducerKind::Max) => match (&mut *slot, &incoming) {
            (Value::Int(lhs), Value::Int(rhs)) => {
                if *rhs > *lhs {
                    *lhs = *rhs;
                }
                true
            }
            (Value::Float(lhs), Value::Float(rhs)) => {
                if *rhs > *lhs {
                    *lhs = *rhs;
                }
                true
            }
            _ => false,
        },
        CompiledReducer::Lambda(_) => false,
    };

    if handled {
        Ok(())
    } else {
        let existing = std::mem::replace(slot, Value::Unit);
        *slot = reducer.reduce(state, existing, incoming)?;
        Ok(())
    }
}

fn key_to_node_idx(key: &Value, state: &DenseGraph) -> Result<Option<usize>, GasSimError> {
    let node_id = match key {
        Value::Int(i) => *i,
        Value::NodeRef(id) => *id,
        other => {
            return Err(GasSimError::TypeMismatch(format!(
                "reduce key must be node id, got {other:?}"
            )));
        }
    };
    Ok(state.node_idx_for_id(node_id))
}

fn group_by_key(keys: &[Value], values: &[Value]) -> Result<HashMap<i64, Vec<Value>>, GasSimError> {
    if keys.len() != values.len() {
        return Err(GasSimError::ReduceLengthMismatch);
    }
    let mut grouped: HashMap<i64, Vec<Value>> = HashMap::new();
    for (k, v) in keys.iter().zip(values) {
        let key_int = match k {
            Value::Int(i) => *i,
            Value::NodeRef(id) => *id,
            _ => {
                return Err(GasSimError::TypeMismatch(
                    "reduce key must be node id".into(),
                ));
            }
        };
        grouped.entry(key_int).or_default().push(v.clone());
    }
    Ok(grouped)
}

fn apply_updates(state: &mut GraphState, updates: &HashMap<i64, Value>, property: &str) -> bool {
    let mut changed = false;
    for (node_id, new_val) in updates {
        if let Some(node) = state.nodes.get_mut(node_id) {
            let entry = node
                .props
                .entry(property.to_string())
                .or_insert(Value::Unit);
            if !entry.approx_eq(new_val) {
                *entry = new_val.clone();
                changed = true;
            }
        }
    }
    changed
}

fn build_edge_env(edge: &EdgeState, _state: &GraphState) -> HashMap<String, Value> {
    let mut env = HashMap::new();
    env.insert(
        "e".to_string(),
        Value::EdgeRef {
            src: edge.src,
            dst: edge.dst,
            props: edge.props.clone(),
        },
    );
    env.insert("src".to_string(), Value::NodeRef(edge.src));
    env.insert("dst".to_string(), Value::NodeRef(edge.dst));
    env
}

fn eval_lambda(
    lambda: &IrLambda,
    args: &[Value],
    extra_env: &HashMap<String, Value>,
    state: &GraphState,
    int_mask: Option<u64>,
) -> Result<Value, GasSimError> {
    if lambda.params.len() != args.len() {
        return Err(GasSimError::TypeMismatch(
            "lambda argument arity mismatch".into(),
        ));
    }
    let mut env: HashMap<String, Value> = extra_env.clone();
    for (name, val) in lambda.params.iter().zip(args.iter()) {
        env.insert(name.as_str().to_string(), val.clone());
    }
    eval_expr(&lambda.body, &env, state, int_mask)
}

fn eval_expr(
    expr: &IrExpr,
    env: &HashMap<String, Value>,
    state: &GraphState,
    int_mask: Option<u64>,
) -> Result<Value, GasSimError> {
    match expr {
        IrExpr::Identifier(id) => env
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| GasSimError::MissingNodeProp(id.as_str().into())),
        IrExpr::Literal(lit) => match lit {
            AstLiteral::Int(i) => Ok(Value::Int(apply_int_mask(*i, int_mask))),
            AstLiteral::Float(f) => Ok(Value::Float(f.parse::<f64>().unwrap_or(0.0))),
            AstLiteral::Bool(b) => Ok(Value::Bool(*b)),
        },
        IrExpr::MemberAccess { target, access } => {
            let base = eval_expr(target, env, state, int_mask)?;
            match (base, access) {
                (Value::EdgeRef { src, dst, props }, Accessor::Property(name)) => {
                    match name.as_str() {
                        "src" => Ok(Value::NodeRef(src)),
                        "dst" => Ok(Value::NodeRef(dst)),
                        prop => props
                            .get(prop)
                            .cloned()
                            .or_else(|| state.edge_uniform_props.get(prop).cloned())
                            .ok_or_else(|| GasSimError::MissingEdgeProp(prop.into())),
                    }
                }
                (Value::NodeRef(id), Accessor::Property(name)) => {
                    if name.as_str() == "id" {
                        return Ok(Value::Int(apply_int_mask(id, int_mask)));
                    }
                    let node = state.nodes.get(&id).ok_or(GasSimError::MissingNode(id))?;
                    node.props
                        .get(name.as_str())
                        .cloned()
                        .ok_or_else(|| GasSimError::MissingNodeProp(name.as_str().into()))
                }
                (Value::Tuple(items), Accessor::Index(idx)) => {
                    let idx = *idx as usize;
                    items
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| GasSimError::TypeMismatch("tuple index".into()))
                }
                (Value::Vector(vec), Accessor::Index(idx)) => {
                    let idx = *idx as usize;
                    vec.get(idx)
                        .copied()
                        .map(Value::Float)
                        .ok_or_else(|| GasSimError::TypeMismatch("vector index".into()))
                }
                (Value::Array(items), Accessor::Index(idx)) => {
                    let idx = *idx as usize;
                    items
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| GasSimError::TypeMismatch("array index".into()))
                }
                (other, _) => Err(GasSimError::UnsupportedOp(format!(
                    "member access on {other:?}"
                ))),
            }
        }
        IrExpr::Binary { op, left, right } => {
            let l = eval_expr(left, env, state, int_mask)?;
            let r = eval_expr(right, env, state, int_mask)?;
            eval_binary_with_mask(*op, l, r, int_mask)
        }
        IrExpr::Unary { op, expr } => {
            let v = eval_expr(expr, env, state, int_mask)?;
            eval_unary_with_mask(*op, v, int_mask)
        }
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            let cond = eval_expr(condition, env, state, int_mask)?.as_bool()?;
            if cond {
                eval_expr(then_expr, env, state, int_mask)
            } else {
                eval_expr(else_expr, env, state, int_mask)
            }
        }
        IrExpr::Call { function, args } => {
            let evaled = args
                .iter()
                .map(|a| eval_expr(a, env, state, int_mask))
                .collect::<Result<Vec<_>, _>>()?;
            eval_call(function.as_str(), &evaled)
        }
    }
}

fn eval_unary(op: UnaryOp, v: Value) -> Result<Value, GasSimError> {
    eval_unary_with_mask(op, v, None)
}

fn eval_unary_with_mask(
    op: UnaryOp,
    v: Value,
    int_mask: Option<u64>,
) -> Result<Value, GasSimError> {
    match op {
        UnaryOp::Not => Ok(Value::Bool(!v.as_bool()?)),
        UnaryOp::BitNot => match v {
            Value::Int(i) => Ok(Value::Int(apply_int_mask(!i, int_mask))),
            _ => Err(GasSimError::TypeMismatch("bitnot".into())),
        },
    }
}

fn eval_binary(op: BinaryOp, l: Value, r: Value) -> Result<Value, GasSimError> {
    eval_binary_with_mask(op, l, r, None)
}

fn eval_binary_with_mask(
    op: BinaryOp,
    l: Value,
    r: Value,
    int_mask: Option<u64>,
) -> Result<Value, GasSimError> {
    match op {
        BinaryOp::Add => match (l, r) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(apply_int_mask(
                u64_from_i64(a).wrapping_add(u64_from_i64(b)) as i64,
                int_mask,
            ))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Vector(a), Value::Vector(b)) => {
                Ok(Value::Vector(zip_vec(&a, &b, |x, y| x + y)))
            }
            _ => Err(GasSimError::TypeMismatch("add".into())),
        },
        BinaryOp::Sub => match (l, r) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(apply_int_mask(
                u64_from_i64(a).wrapping_sub(u64_from_i64(b)) as i64,
                int_mask,
            ))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            _ => Err(GasSimError::TypeMismatch("sub".into())),
        },
        BinaryOp::Mul => match (l, r) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(apply_int_mask(
                u64_from_i64(a).wrapping_mul(u64_from_i64(b)) as i64,
                int_mask,
            ))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            _ => Err(GasSimError::TypeMismatch("mul".into())),
        },
        BinaryOp::Div => match (l, r) {
            (Value::Int(a), Value::Int(b)) => {
                let denom = u64_from_i64(b);
                let out = if denom == 0 { 0 } else { u64_from_i64(a) / denom };
                Ok(Value::Int(apply_int_mask(out as i64, int_mask)))
            }
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
            _ => Err(GasSimError::TypeMismatch("div".into())),
        },
        BinaryOp::Eq => Ok(Value::Bool(l.approx_eq(&r))),
        BinaryOp::Ne => Ok(Value::Bool(!l.approx_eq(&r))),
        BinaryOp::Gt => cmp_vals(l, r, |a, b| a > b),
        BinaryOp::Lt => cmp_vals(l, r, |a, b| a < b),
        BinaryOp::Ge => cmp_vals(l, r, |a, b| a >= b),
        BinaryOp::Le => cmp_vals(l, r, |a, b| a <= b),
        BinaryOp::BitAnd => match (l, r) {
            (Value::Int(a), Value::Int(b)) => {
                let out = u64_from_i64(a) & u64_from_i64(b);
                Ok(Value::Int(apply_int_mask(out as i64, int_mask)))
            }
            _ => Err(GasSimError::TypeMismatch("bitand".into())),
        },
        BinaryOp::BitOr => match (l, r) {
            (Value::Int(a), Value::Int(b)) => {
                let out = u64_from_i64(a) | u64_from_i64(b);
                Ok(Value::Int(apply_int_mask(out as i64, int_mask)))
            }
            _ => Err(GasSimError::TypeMismatch("bitor".into())),
        },
        BinaryOp::And => Ok(Value::Bool(l.as_bool()? && r.as_bool()?)),
        BinaryOp::Or => Ok(Value::Bool(l.as_bool()? || r.as_bool()?)),
    }
}

fn u64_from_i64(v: i64) -> u64 {
    v as u64
}

fn apply_int_mask(v: i64, mask: Option<u64>) -> i64 {
    match mask {
        Some(m) => (u64_from_i64(v) & m) as i64,
        None => v,
    }
}

fn int_mask_for_program(program: &GasProgram) -> Option<u64> {
    let candidates = [
        &program.apply.output_type,
        &program.apply.input_type,
        &program.gather.output_type,
        &program.gather.input_value_type,
        &program.scatter.value_type,
    ];

    let width = candidates.iter().find_map(|ty| match ty {
        GasType::Int(w) => Some(*w),
        _ => None,
    })?;

    int_mask_for_width(width)
}

fn int_mask_for_width(width: u32) -> Option<u64> {
    match width {
        0 => None,
        1..=63 => Some((1u64 << width) - 1u64),
        64 => None,
        _ => None,
    }
}

fn cmp_vals<F: Fn(f64, f64) -> bool>(l: Value, r: Value, f: F) -> Result<Value, GasSimError> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Bool(f(a as f64, b as f64))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Bool(f(a, b))),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Bool(f(a as f64, b))),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Bool(f(a, b as f64))),
        _ => Err(GasSimError::TypeMismatch("cmp".into())),
    }
}

fn eval_call(name: &str, args: &[Value]) -> Result<Value, GasSimError> {
    match (name, args) {
        ("make_set", [Value::Int(v)]) => Ok(Value::IntSet(BTreeSet::from([*v]))),
        ("set_union", [Value::IntSet(a), Value::IntSet(b)]) => {
            let mut out = a.clone();
            out.extend(b.iter().copied());
            Ok(Value::IntSet(out))
        }
        ("mex", [Value::IntSet(set)]) => {
            let mut candidate = 0;
            loop {
                if !set.contains(&candidate) {
                    return Ok(Value::Int(candidate));
                }
                candidate += 1;
            }
        }
        ("pair", [a, b]) => Ok(Value::Tuple(vec![a.clone(), b.clone()])),
        ("outer_product", [Value::Vector(v1), Value::Vector(v2)]) => {
            let rows = v1.len();
            let cols = v2.len();
            let mut data = vec![0.0; rows * cols];
            for i in 0..rows {
                for j in 0..cols {
                    data[i * cols + j] = v1[i] * v2[j];
                }
            }
            Ok(Value::Matrix { rows, cols, data })
        }
        ("vector_scale", [Value::Vector(v), Value::Float(s)]) => {
            Ok(Value::Vector(v.iter().map(|x| x * s).collect()))
        }
        ("vector_scale", [Value::Vector(v), Value::Int(s)]) => {
            Ok(Value::Vector(v.iter().map(|x| x * *s as f64).collect()))
        }
        ("vector_add", [Value::Vector(a), Value::Vector(b)]) => {
            Ok(Value::Vector(zip_vec(a, b, |x, y| x + y)))
        }
        (
            "matrix_add",
            [
                Value::Matrix { rows, cols, data },
                Value::Matrix { data: d2, .. },
            ],
        ) => Ok(Value::Matrix {
            rows: *rows,
            cols: *cols,
            data: zip_vec(data, d2, |x, y| x + y),
        }),
        ("solve_linear", [Value::Matrix { rows, cols, data }, Value::Vector(b)]) => {
            let n = *rows;
            if n == 0 || *cols != n || b.len() != n {
                return Err(GasSimError::CallError(
                    "solve_linear dimension mismatch".into(),
                ));
            }
            let mut a = data.clone();
            for i in 0..n {
                a[i * n + i] += 1e-6; // mild regularization to avoid singular matrices
            }
            let mut rhs = b.clone();
            // simple Gaussian elimination with partial pivot
            for i in 0..n {
                // pivot
                let mut pivot = i;
                let mut max = a[i * n + i].abs();
                for r in (i + 1)..n {
                    let v = a[r * n + i].abs();
                    if v > max {
                        max = v;
                        pivot = r;
                    }
                }
                if max < 1e-12 {
                    max = 1e-12;
                    pivot = i;
                    a[i * n + i] = max;
                }
                if pivot != i {
                    for c in 0..n {
                        a.swap(i * n + c, pivot * n + c);
                    }
                    rhs.swap(i, pivot);
                }
                let pivot_val = a[i * n + i];
                for c in i..n {
                    a[i * n + c] /= pivot_val;
                }
                rhs[i] /= pivot_val;
                for r in 0..n {
                    if r == i {
                        continue;
                    }
                    let factor = a[r * n + i];
                    for c in i..n {
                        a[r * n + c] -= factor * a[i * n + c];
                    }
                    rhs[r] -= factor * rhs[i];
                }
            }
            Ok(Value::Vector(rhs))
        }
        _ => Err(GasSimError::CallError(format!("unsupported call {name}"))),
    }
}

fn zip_vec<F: Fn(f64, f64) -> f64>(a: &[f64], b: &[f64], f: F) -> Vec<f64> {
    a.iter().zip(b.iter()).map(|(x, y)| f(*x, *y)).collect()
}

/// Load a graph state from JSON using schema types to coerce values.
pub fn load_graph_from_json(
    input: GraphInput,
    node_types: &HashMap<String, GasType>,
    edge_types: &HashMap<String, GasType>,
) -> Result<GraphState, GasSimError> {
    let GraphInput {
        nodes: input_nodes,
        edges: input_edges,
    } = input;

    let mut nodes = HashMap::new();
    for node in input_nodes {
        let mut props = HashMap::new();
        for (name, ty) in node_types {
            let raw = node
                .properties
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let val = json_to_value(&raw, ty)?;
            props.insert(name.clone(), val);
        }
        nodes.insert(node.id, NodeState { props });
    }

    let mut edge_uniform_props: HashMap<String, Value> = HashMap::new();
    let mut non_uniform_edge_props: HashMap<String, bool> =
        edge_types.keys().map(|k| (k.clone(), false)).collect();

    if let Some(first_edge) = input_edges.first() {
        for (name, ty) in edge_types {
            let raw = first_edge
                .properties
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let val = json_to_value(&raw, ty)?;
            edge_uniform_props.insert(name.clone(), val);
        }

        for edge in input_edges.iter().skip(1) {
            for (name, ty) in edge_types {
                if *non_uniform_edge_props.get(name).unwrap_or(&false) {
                    continue;
                }
                let raw = edge
                    .properties
                    .get(name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let val = json_to_value(&raw, ty)?;
                if edge_uniform_props
                    .get(name)
                    .is_some_and(|base| !base.approx_eq(&val))
                {
                    non_uniform_edge_props.insert(name.clone(), true);
                }
            }
        }
    }

    for (name, is_non_uniform) in &non_uniform_edge_props {
        if *is_non_uniform {
            edge_uniform_props.remove(name);
        }
    }

    let non_uniform_names = non_uniform_edge_props
        .iter()
        .filter_map(|(name, is_non_uniform)| (*is_non_uniform).then_some(name.clone()))
        .collect::<Vec<_>>();

    let mut edges = Vec::new();
    for edge in input_edges {
        let mut props = HashMap::new();
        for name in &non_uniform_names {
            if let Some(ty) = edge_types.get(name) {
                let raw = edge
                    .properties
                    .get(name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let val = json_to_value(&raw, ty)?;
                props.insert(name.clone(), val);
            }
        }
        edges.push(EdgeState {
            src: edge.src,
            dst: edge.dst,
            props,
        });
    }

    Ok(GraphState {
        nodes,
        edges,
        edge_uniform_props,
    })
}

fn json_to_value(raw: &serde_json::Value, ty: &GasType) -> Result<Value, GasSimError> {
    match ty {
        GasType::Int(_) => Ok(Value::Int(raw.as_i64().unwrap_or(0))),
        GasType::Float | GasType::Fixed { .. } => Ok(Value::Float(raw.as_f64().unwrap_or(0.0))),
        GasType::Bool => Ok(Value::Bool(raw.as_bool().unwrap_or(false))),
        GasType::Vector { len, .. } => {
            let arr = raw.as_array().cloned().unwrap_or_default();
            let mut vec = vec![0.0; *len as usize];
            for (i, v) in arr.iter().enumerate().take(*len as usize) {
                vec[i] = v.as_f64().unwrap_or(0.0);
            }
            Ok(Value::Vector(vec))
        }
        GasType::Matrix { rows, cols, .. } => {
            let arr = raw.as_array().cloned().unwrap_or_default();
            let mut data = vec![0.0; (*rows * *cols) as usize];
            for (i, v) in arr.iter().enumerate().take((*rows * *cols) as usize) {
                data[i] = v.as_f64().unwrap_or(0.0);
            }
            Ok(Value::Matrix {
                rows: *rows as usize,
                cols: *cols as usize,
                data,
            })
        }
        GasType::Tuple(items) => {
            let arr = raw.as_array().cloned().unwrap_or_default();
            let mut out = Vec::new();
            for (idx, sub_ty) in items.iter().enumerate() {
                let sub_raw = arr.get(idx).cloned().unwrap_or(serde_json::Value::Null);
                out.push(json_to_value(&sub_raw, sub_ty)?);
            }
            Ok(Value::Tuple(out))
        }
        GasType::Set(inner) => {
            if matches!(**inner, GasType::Int(_)) {
                let arr = raw.as_array().cloned().unwrap_or_default();
                let mut set = BTreeSet::new();
                for v in arr {
                    if let Some(i) = v.as_i64() {
                        set.insert(i);
                    }
                }
                Ok(Value::IntSet(set))
            } else {
                Err(GasSimError::TypeMismatch("only int sets supported".into()))
            }
        }
        GasType::Array(inner) => {
            let arr = raw.as_array().cloned().unwrap_or_default();
            let mut out = Vec::new();
            for v in arr {
                out.push(json_to_value(&v, inner)?);
            }
            Ok(Value::Array(out))
        }
        GasType::EdgeRecord { .. }
        | GasType::NodeRecord { .. }
        | GasType::NodeRef
        | GasType::Unknown => Err(GasSimError::TypeMismatch(
            "unsupported type in json load".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::gas_lower::lower_to_gas, engine::ir_builder::LoweredProgram,
        services::parser::parse_program,
    };

    fn load_app(app: &str) -> (GasProgram, GraphState) {
        let source = std::fs::read_to_string(format!("apps/{app}.dsl")).expect("read app");
        let prog = parse_program(&source).expect("parse app");
        let lowered = LoweredProgram::parse_and_lower(&source).expect("lower app");
        let gas = lower_to_gas(&prog, &lowered.ir).expect("gas lower");
        let node_types = prog
            .schema
            .node
            .as_ref()
            .map(|n| collect_types(n))
            .unwrap_or_default();
        let edge_types = prog
            .schema
            .edge
            .as_ref()
            .map(|e| collect_types(e))
            .unwrap_or_default();
        let fixture =
            std::fs::read_to_string(format!("apps/test_graphs/{app}_small.json")).expect("fixture");
        let input: GraphInput = serde_json::from_str(&fixture).expect("json");
        let graph = load_graph_from_json(input, &node_types, &edge_types).expect("graph");
        (gas, graph)
    }

    fn collect_types(entity: &crate::domain::ast::EntityDef) -> HashMap<String, GasType> {
        entity
            .properties
            .iter()
            .map(|p| {
                (
                    p.name.as_str().to_string(),
                    crate::engine::gas_lower::typeexpr_to_gastype(&p.ty),
                )
            })
            .collect()
    }

    #[test]
    fn simulate_sssp() {
        let (gas, graph) = load_app("sssp");
        let out = simulate_gas(&gas, &graph, 10).expect("simulate");
        let dist0 = out.nodes.get(&0).unwrap().props.get("dist").unwrap();
        if let Value::Int(d) = dist0 {
            assert_eq!(*d, 0);
        } else {
            panic!("dist wrong type");
        }
    }

    #[test]
    fn compiled_matches_interpreted_on_builtin_apps() {
        let cases = [
            ("sssp", 16usize),
            ("pagerank", 64usize),
            ("connected_components", 16usize),
            ("graph_coloring", 8usize),
            ("als", 4usize),
        ];

        for (app, max_iters) in cases {
            let (gas, graph) = load_app(app);
            let interpreted =
                simulate_gas_interpreted(&gas, &graph, max_iters).expect("interpreted");
            let compiled = simulate_gas_compiled(&gas, &graph, max_iters).expect("compiled");
            let target = gas.apply.target_property.as_str().to_string();

            for node_id in interpreted.node_ids() {
                let lhs = interpreted
                    .node_prop(node_id, &target)
                    .expect("interpreted value missing");
                let rhs = compiled
                    .node_prop(node_id, &target)
                    .expect("compiled value missing");
                assert!(
                    lhs.approx_eq(rhs),
                    "app={app} node={node_id} property={target} interpreted={lhs:?} compiled={rhs:?}"
                );
            }
        }
    }

    #[test]
    fn compiled_for_iters_matches_interpreted_on_builtin_apps() {
        let cases = [
            ("sssp", 1usize),
            ("pagerank", 2usize),
            ("connected_components", 2usize),
            ("graph_coloring", 2usize),
            ("als", 2usize),
        ];

        for (app, iters) in cases {
            let (gas, graph) = load_app(app);
            let interpreted =
                simulate_gas_interpreted_for_iters(&gas, &graph, iters).expect("interpreted");
            let compiled = simulate_gas_for_iters(&gas, &graph, iters).expect("compiled");
            let target = gas.apply.target_property.as_str().to_string();

            for node_id in interpreted.node_ids() {
                let lhs = interpreted
                    .node_prop(node_id, &target)
                    .expect("interpreted value missing");
                let rhs = compiled
                    .node_prop(node_id, &target)
                    .expect("compiled value missing");
                assert!(
                    lhs.approx_eq(rhs),
                    "app={app} node={node_id} property={target} interpreted={lhs:?} compiled={rhs:?}"
                );
            }
        }
    }

    #[test]
    fn detects_specialized_kernels_for_core_apps() {
        for app in ["pagerank", "sssp", "connected_components"] {
            let (gas, _) = load_app(app);
            assert!(
                SpecializedKernel::from_program(&gas).is_some(),
                "expected specialized kernel for {app}"
            );
        }

        for app in ["graph_coloring", "als"] {
            let (gas, _) = load_app(app);
            assert!(
                SpecializedKernel::from_program(&gas).is_none(),
                "did not expect specialized kernel for {app}"
            );
        }
    }

    #[test]
    fn evals_bitwise_and_or() {
        assert_eq!(
            eval_binary(BinaryOp::BitAnd, Value::Int(0b1010), Value::Int(0b1100)).unwrap(),
            Value::Int(0b1000)
        );
        assert_eq!(
            eval_binary(BinaryOp::BitOr, Value::Int(0b1010), Value::Int(0b1100)).unwrap(),
            Value::Int(0b1110)
        );
        assert_eq!(
            eval_unary(UnaryOp::BitNot, Value::Int(0)).unwrap(),
            Value::Int(!0)
        );
    }

    #[test]
    fn evals_logical_and_or() {
        assert_eq!(
            eval_binary(BinaryOp::And, Value::Bool(true), Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_binary(BinaryOp::Or, Value::Bool(true), Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
    }
}
