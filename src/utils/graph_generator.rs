use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::json;
use thiserror::Error;

use crate::engine::gas_simulator::{EdgeRecord, GraphInput, NodeRecord};

/// Supported built-in applications.
///
/// This mirrors the five sample DSL programs bundled with the crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppKind {
    Sssp,
    Pagerank,
    ConnectedComponents,
    Bfs,
    Ar,
    Wcc,
    GraphColoring,
    Als,
}

impl FromStr for AppKind {
    type Err = GeneratorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sssp" | "sssp_swemu_one_big" => Ok(AppKind::Sssp),
            "pagerank" => Ok(AppKind::Pagerank),
            "connected_components" => Ok(AppKind::ConnectedComponents),
            "bfs" => Ok(AppKind::Bfs),
            "ar" => Ok(AppKind::Ar),
            "wcc" => Ok(AppKind::Wcc),
            "graph_coloring" => Ok(AppKind::GraphColoring),
            "als" => Ok(AppKind::Als),
            other => Err(GeneratorError::UnknownApp(other.to_string())),
        }
    }
}

/// Errors produced by the graph generator.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum GeneratorError {
    #[error("unknown app '{0}'")]
    UnknownApp(String),
    #[error("node count must be positive")]
    InvalidNodeCount,
    #[error("edge count must be positive")]
    InvalidEdgeCount,
    #[error("requested {requested} edges exceeds maximum {maximum} for {nodes} nodes")]
    EdgeCountTooLarge {
        requested: usize,
        maximum: usize,
        nodes: usize,
    },
    #[error("failed to parse count: {0}")]
    ParseCount(String),
}

/// Parameters controlling graph generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphSpec {
    pub nodes: usize,
    pub edges: usize,
    pub seed: Option<u64>,
}

impl GraphSpec {
    /// Builds a new spec with mandatory node/edge counts.
    pub fn new(nodes: usize, edges: usize) -> Self {
        Self {
            nodes,
            edges,
            seed: None,
        }
    }

    /// Attaches a deterministic seed for reproducible output.
    pub fn with_seed(self, seed: u64) -> Self {
        Self {
            seed: Some(seed),
            ..self
        }
    }
}

/// Generate a synthetic `GraphInput` for the given application with random properties.
///
/// # Examples
/// ```
/// use refactor_Graphyflow::utils::graph_generator::{generate_graph, AppKind, GraphSpec};
/// let spec = GraphSpec::new(4, 5).with_seed(42);
/// let graph = generate_graph(AppKind::Sssp, spec).unwrap();
/// assert_eq!(graph.nodes.len(), 4);
/// assert_eq!(graph.edges.len(), 5);
/// ```
pub fn generate_graph(app: AppKind, spec: GraphSpec) -> Result<GraphInput, GeneratorError> {
    if spec.nodes == 0 {
        return Err(GeneratorError::InvalidNodeCount);
    }
    if spec.edges == 0 {
        return Err(GeneratorError::InvalidEdgeCount);
    }

    let max_edges = spec.nodes.saturating_mul(spec.nodes);
    if spec.edges > max_edges {
        return Err(GeneratorError::EdgeCountTooLarge {
            requested: spec.edges,
            maximum: max_edges,
            nodes: spec.nodes,
        });
    }

    let mut rng = init_rng(spec.seed);
    let edges = build_edges(spec.nodes, spec.edges, &mut rng);
    let nodes = build_nodes(app.clone(), spec.nodes, &edges, &mut rng);
    let edge_records = build_edge_props(app, edges, &mut rng);

    Ok(GraphInput {
        nodes,
        edges: edge_records,
    })
}

fn init_rng(seed: Option<u64>) -> StdRng {
    match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_entropy(),
    }
}

fn build_edges(nodes: usize, edges: usize, rng: &mut StdRng) -> Vec<(i64, i64)> {
    let mut chosen = HashSet::new();
    while chosen.len() < edges {
        let src = rng.gen_range(0..nodes as i64);
        let dst = rng.gen_range(0..nodes as i64);
        chosen.insert((src, dst));
    }
    let mut out: Vec<(i64, i64)> = chosen.into_iter().collect();
    out.sort_unstable();
    out
}

fn build_nodes(
    app: AppKind,
    nodes: usize,
    edges: &[(i64, i64)],
    rng: &mut StdRng,
) -> Vec<NodeRecord> {
    let mut out = Vec::with_capacity(nodes);
    let raw_out_deg = compute_raw_out_degrees(nodes, edges);
    let avg_out_deg = if nodes == 0 {
        0i64
    } else {
        let total: i64 = raw_out_deg.values().copied().sum();
        total / nodes as i64
    };
    for id in 0..nodes {
        let properties = match app {
            AppKind::Sssp => sssp_node_props(id as i64, rng),
            AppKind::Pagerank => pagerank_node_props(id as i64, &raw_out_deg, rng),
            AppKind::ConnectedComponents => cc_node_props(id as i64),
            AppKind::Bfs => bfs_node_props(id as i64),
            AppKind::Ar => ar_node_props(id as i64, &raw_out_deg, avg_out_deg),
            AppKind::Wcc => wcc_node_props(id as i64),
            AppKind::GraphColoring => coloring_node_props(id as i64, rng),
            AppKind::Als => als_node_props(rng),
        };
        out.push(NodeRecord {
            id: id as i64,
            properties,
        });
    }
    out
}

fn build_edge_props(app: AppKind, edges: Vec<(i64, i64)>, rng: &mut StdRng) -> Vec<EdgeRecord> {
    edges
        .into_iter()
        .map(|(src, dst)| match app {
            AppKind::Sssp => EdgeRecord {
                src,
                dst,
                properties: HashMap::from([("weight".to_string(), json!(rng.gen_range(1..10)))]),
            },
            AppKind::Pagerank
            | AppKind::ConnectedComponents
            | AppKind::Bfs
            | AppKind::Ar
            | AppKind::Wcc
            | AppKind::GraphColoring => EdgeRecord {
                src,
                dst,
                properties: HashMap::new(),
            },
            AppKind::Als => EdgeRecord {
                src,
                dst,
                properties: HashMap::from([("rating".to_string(), json!(rng.gen_range(0.0..5.0)))]),
            },
        })
        .collect()
}

fn compute_raw_out_degrees(nodes: usize, edges: &[(i64, i64)]) -> HashMap<i64, i64> {
    let mut out_deg = HashMap::new();
    for id in 0..nodes {
        out_deg.insert(id as i64, 0);
    }
    for (src, _) in edges {
        if let Some(entry) = out_deg.get_mut(src) {
            *entry += 1;
        }
    }
    out_deg
}

fn sssp_node_props(id: i64, rng: &mut StdRng) -> HashMap<String, serde_json::Value> {
    let initial = if id == 0 { 0 } else { rng.gen_range(5..50) };
    HashMap::from([("dist".to_string(), json!(initial))])
}

fn pagerank_node_props(
    id: i64,
    out_deg: &HashMap<i64, i64>,
    rng: &mut StdRng,
) -> HashMap<String, serde_json::Value> {
    let rank: f64 = rng.gen_range(0.5..1.5);
    let mut deg = out_deg.get(&id).copied().unwrap_or(0);
    if deg == 0 {
        deg = 1;
    }
    HashMap::from([
        ("rank".to_string(), json!(rank)),
        ("out_deg".to_string(), json!(deg)),
    ])
}

fn bfs_node_props(id: i64) -> HashMap<String, serde_json::Value> {
    const ACTIVE: i64 = 0x8000_0000;
    const INF: i64 = 0x7FFF_FFFE;
    let initial = if id == 0 { ACTIVE | 1 } else { INF };
    HashMap::from([("prop".to_string(), json!(initial))])
}

fn ar_node_props(
    id: i64,
    out_deg: &HashMap<i64, i64>,
    avg_out_deg: i64,
) -> HashMap<String, serde_json::Value> {
    let raw = out_deg.get(&id).copied().unwrap_or(0);
    let mut denom = raw + avg_out_deg;
    if denom == 0 {
        denom = 1;
    }
    HashMap::from([
        ("score".to_string(), json!(0)),
        ("out_deg".to_string(), json!(denom)),
    ])
}

fn wcc_node_props(id: i64) -> HashMap<String, serde_json::Value> {
    let initial = if id == 0 { 1 } else { 0 };
    HashMap::from([("label".to_string(), json!(initial))])
}

fn cc_node_props(id: i64) -> HashMap<String, serde_json::Value> {
    HashMap::from([("label".to_string(), json!(id))])
}

fn coloring_node_props(id: i64, rng: &mut StdRng) -> HashMap<String, serde_json::Value> {
    let priority = rng.gen_range(0..1000);
    HashMap::from([
        ("color".to_string(), json!(id)),
        ("priority".to_string(), json!(priority)),
    ])
}

fn als_node_props(rng: &mut StdRng) -> HashMap<String, serde_json::Value> {
    let vec: Vec<f64> = (0..16).map(|_| rng.gen_range(0.0..1.0)).collect();
    HashMap::from([("vec".to_string(), json!(vec))])
}

impl fmt::Display for AppKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            AppKind::Sssp => "sssp",
            AppKind::Pagerank => "pagerank",
            AppKind::ConnectedComponents => "connected_components",
            AppKind::Bfs => "bfs",
            AppKind::Ar => "ar",
            AppKind::Wcc => "wcc",
            AppKind::GraphColoring => "graph_coloring",
            AppKind::Als => "als",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("sssp", AppKind::Sssp)]
    #[case("pagerank", AppKind::Pagerank)]
    #[case("connected_components", AppKind::ConnectedComponents)]
    #[case("bfs", AppKind::Bfs)]
    #[case("ar", AppKind::Ar)]
    #[case("wcc", AppKind::Wcc)]
    #[case("graph_coloring", AppKind::GraphColoring)]
    #[case("als", AppKind::Als)]
    fn parses_app_kinds(#[case] raw: &str, #[case] expected: AppKind) {
        let parsed = AppKind::from_str(raw).expect("app parses");
        assert_eq!(parsed, expected);
    }

    #[test]
    fn rejects_unknown_app() {
        let err = AppKind::from_str("unknown").unwrap_err();
        assert!(matches!(err, GeneratorError::UnknownApp(name) if name == "unknown"));
    }

    #[test]
    fn rejects_zero_nodes_or_edges() {
        let spec = GraphSpec::new(0, 1);
        let err = generate_graph(AppKind::Sssp, spec).unwrap_err();
        assert!(matches!(err, GeneratorError::InvalidNodeCount));

        let spec = GraphSpec::new(2, 0);
        let err = generate_graph(AppKind::Sssp, spec).unwrap_err();
        assert!(matches!(err, GeneratorError::InvalidEdgeCount));
    }

    #[test]
    fn rejects_too_many_edges() {
        let spec = GraphSpec::new(2, 5);
        let err = generate_graph(AppKind::Sssp, spec).unwrap_err();
        assert!(matches!(
            err,
            GeneratorError::EdgeCountTooLarge {
                requested: 5,
                maximum: 4,
                nodes: 2
            }
        ));
    }

    #[test]
    fn deterministic_with_seed() {
        let spec = GraphSpec::new(3, 4).with_seed(7);
        let g1 = generate_graph(AppKind::Pagerank, spec).expect("graph 1");
        let g2 = generate_graph(AppKind::Pagerank, spec).expect("graph 2");
        assert_eq!(g1.nodes, g2.nodes, "nodes differ for same seed");
        assert_eq!(g1.edges, g2.edges, "edges differ for same seed");
    }

    #[test]
    fn pagerank_sets_out_deg() {
        let spec = GraphSpec::new(3, 3).with_seed(3);
        let graph = generate_graph(AppKind::Pagerank, spec).expect("graph");
        for node in &graph.nodes {
            let deg = node
                .properties
                .get("out_deg")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            assert!(deg >= 1, "out_deg must be at least one");
        }
    }

    #[test]
    fn bfs_marks_seed_active() {
        let spec = GraphSpec::new(5, 5).with_seed(1);
        let graph = generate_graph(AppKind::Bfs, spec).expect("graph");
        let seed = graph
            .nodes
            .iter()
            .find(|n| n.id == 0)
            .and_then(|n| n.properties.get("prop"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(seed, 0x8000_0001);
    }
}
