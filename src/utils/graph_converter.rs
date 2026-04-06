use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::json;
use thiserror::Error;

use crate::engine::gas_simulator::{EdgeRecord, GraphInput, NodeRecord};
use crate::utils::graph_generator::AppKind;

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported file extension: {0}")]
    UnsupportedFormat(String),
    #[error("empty graph: no edges found")]
    EmptyGraph,
}

/// Reads a raw graph file (.txt edge list or .mtx MatrixMarket) and converts
/// it to `GraphInput` JSON with algorithm-specific default node/edge properties.
pub fn convert_graph(path: &Path, app: &AppKind) -> Result<GraphInput, ConvertError> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let edges = match ext.as_str() {
        "txt" => parse_edge_list(path)?,
        "mtx" => parse_matrix_market(path)?,
        other => return Err(ConvertError::UnsupportedFormat(other.to_string())),
    };

    if edges.is_empty() {
        return Err(ConvertError::EmptyGraph);
    }

    let mut node_ids = HashSet::new();
    for (src, dst, _) in &edges {
        node_ids.insert(*src);
        node_ids.insert(*dst);
    }

    let mut sorted_ids: Vec<i64> = node_ids.into_iter().collect();
    sorted_ids.sort_unstable();

    // Compute out-degrees for algorithms that need them.
    let mut out_deg: HashMap<i64, i64> = HashMap::new();
    for (src, _, _) in &edges {
        *out_deg.entry(*src).or_insert(0) += 1;
    }

    let nodes: Vec<NodeRecord> = sorted_ids
        .iter()
        .map(|&id| NodeRecord {
            id,
            properties: node_props_for_app(app, id, &out_deg),
        })
        .collect();

    let edge_records: Vec<EdgeRecord> = edges
        .into_iter()
        .map(|(src, dst, weight)| EdgeRecord {
            src,
            dst,
            properties: edge_props_for_app(app, weight),
        })
        .collect();

    Ok(GraphInput {
        nodes,
        edges: edge_records,
    })
}

fn node_props_for_app(
    app: &AppKind,
    id: i64,
    out_deg: &HashMap<i64, i64>,
) -> HashMap<String, serde_json::Value> {
    match app {
        AppKind::Sssp => {
            let dist = if id == 0 { 0 } else { 999999 };
            HashMap::from([("dist".to_string(), json!(dist))])
        }
        AppKind::Pagerank | AppKind::Ar => {
            let deg = out_deg.get(&id).copied().unwrap_or(1).max(1);
            HashMap::from([
                ("rank".to_string(), json!(1.0)),
                ("out_deg".to_string(), json!(deg)),
            ])
        }
        AppKind::ConnectedComponents => HashMap::from([("label".to_string(), json!(id))]),
        AppKind::Wcc => HashMap::from([("label".to_string(), json!(id))]),
        AppKind::Bfs => {
            let active = if id == 0 { 1_i64 } else { 0_i64 };
            HashMap::from([("active".to_string(), json!(active))])
        }
        AppKind::GraphColoring => HashMap::from([
            ("color".to_string(), json!(id)),
            ("priority".to_string(), json!(id % 1000)),
        ]),
        AppKind::Als => {
            let vec: Vec<f64> = vec![0.0; 16];
            HashMap::from([("vec".to_string(), json!(vec))])
        }
    }
}

fn edge_props_for_app(
    app: &AppKind,
    weight: Option<i64>,
) -> HashMap<String, serde_json::Value> {
    match app {
        AppKind::Sssp => {
            HashMap::from([("weight".to_string(), json!(weight.unwrap_or(1)))])
        }
        AppKind::Als => {
            HashMap::from([("rating".to_string(), json!(weight.unwrap_or(1) as f64))])
        }
        _ => HashMap::new(),
    }
}

/// Parses a text edge list file: one `src dst [weight]` per line.
/// Skips lines starting with `#` or `%`.
fn parse_edge_list(path: &Path) -> Result<Vec<(i64, i64, Option<i64>)>, ConvertError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut edges = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('%') {
            continue;
        }
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let src: i64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let dst: i64 = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let weight: Option<i64> = parts.get(2).and_then(|w| w.parse().ok());
        edges.push((src, dst, weight));
    }

    Ok(edges)
}

/// Parses a MatrixMarket coordinate file (.mtx).
/// Handles `pattern` (no values) and `integer`/`real` (with values).
fn parse_matrix_market(path: &Path) -> Result<Vec<(i64, i64, Option<i64>)>, ConvertError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut edges = Vec::new();
    let mut header_seen = false;
    let mut is_pattern = false;
    let mut size_line_seen = false;

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        if trimmed.starts_with("%%") {
            let lower = trimmed.to_lowercase();
            is_pattern = lower.contains("pattern");
            header_seen = true;
            continue;
        }

        if trimmed.starts_with('%') {
            continue;
        }

        if trimmed.is_empty() {
            continue;
        }

        // First non-comment line after header is the size line: rows cols nnz
        if header_seen && !size_line_seen {
            size_line_seen = true;
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let row: i64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let col: i64 = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let weight: Option<i64> = if is_pattern {
            None
        } else {
            parts.get(2).and_then(|w| {
                // Try integer first, then float truncated to int
                w.parse::<i64>()
                    .ok()
                    .or_else(|| w.parse::<f64>().ok().map(|f| f as i64))
            })
        };

        // MatrixMarket is 1-indexed; convert to 0-indexed
        edges.push((row - 1, col - 1, weight));
    }

    Ok(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_txt_edge_list() {
        let dir = std::env::temp_dir().join("graphyflow_test_txt");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("test.txt");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "# comment").unwrap();
        writeln!(f, "0 1 5").unwrap();
        writeln!(f, "1 2").unwrap();
        writeln!(f, "2 0 3").unwrap();
        drop(f);

        let edges = parse_edge_list(&path).unwrap();
        assert_eq!(edges.len(), 3);
        assert_eq!(edges[0], (0, 1, Some(5)));
        assert_eq!(edges[1], (1, 2, None));
        assert_eq!(edges[2], (2, 0, Some(3)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parses_mtx_pattern() {
        let dir = std::env::temp_dir().join("graphyflow_test_mtx");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("test.mtx");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "%%MatrixMarket matrix coordinate pattern general").unwrap();
        writeln!(f, "3 3 2").unwrap();
        writeln!(f, "1 2").unwrap();
        writeln!(f, "2 3").unwrap();
        drop(f);

        let edges = parse_matrix_market(&path).unwrap();
        assert_eq!(edges.len(), 2);
        // 1-indexed -> 0-indexed
        assert_eq!(edges[0], (0, 1, None));
        assert_eq!(edges[1], (1, 2, None));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn convert_sssp_sets_properties() {
        let dir = std::env::temp_dir().join("graphyflow_test_conv");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("test.txt");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "0 1 3").unwrap();
        writeln!(f, "1 2 1").unwrap();
        drop(f);

        let graph = convert_graph(&path, &AppKind::Sssp).unwrap();
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.nodes[0].properties["dist"], json!(0));
        assert_eq!(graph.nodes[1].properties["dist"], json!(999999));
        assert_eq!(graph.edges[0].properties["weight"], json!(3));
        let _ = fs::remove_dir_all(&dir);
    }
}
