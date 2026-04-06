use std::collections::{BTreeSet, HashMap};

use crate::engine::gas_simulator::{GraphInput, Value};

/// Compute expected node property values for a given app using a deterministic reference implementation.
pub fn reference_values(
    app: &str,
    input: &GraphInput,
    _target_prop: &str,
) -> Result<HashMap<i64, Value>, String> {
    reference_values_with_iters(app, input, _target_prop, None)
}

/// Compute expected node property values for a given app, optionally bounded to a fixed
/// number of iterations for partial-run comparisons.
pub fn reference_values_with_iters(
    app: &str,
    input: &GraphInput,
    _target_prop: &str,
    max_iters: Option<usize>,
) -> Result<HashMap<i64, Value>, String> {
    match app {
        "sssp" => Ok(reference_sssp(input, max_iters)
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "bfs" => Ok(reference_bfs(input, max_iters)
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "ar" => Ok(reference_ar(input)
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "pagerank" => Ok(reference_pagerank(input, max_iters.unwrap_or(64))
            .into_iter()
            .map(|(k, v)| (k, Value::Float(v)))
            .collect()),
        "connected_components" => Ok(reference_connected_components(input)
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "wcc" => Ok(reference_wcc(input, max_iters)
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "graph_coloring" => Ok(reference_graph_coloring(input, max_iters.unwrap_or(8))
            .into_iter()
            .map(|(k, v)| (k, Value::Int(v)))
            .collect()),
        "als" => Ok(reference_als(input)
            .into_iter()
            .map(|(k, v)| (k, Value::Vector(v)))
            .collect()),
        other => Err(format!(
            "unsupported app '{other}' for reference calculation"
        )),
    }
}

fn reference_bfs(input: &GraphInput, max_iters_override: Option<usize>) -> HashMap<i64, i64> {
    const ACTIVE: i64 = 0x8000_0000;
    const LOW_MASK: i64 = 0x7FFF_FFFF;
    const INF: i64 = 0x7FFF_FFFE;

    let mut prop: HashMap<i64, i64> = input
        .nodes
        .iter()
        .map(|n| {
            let v = n
                .properties
                .get("prop")
                .and_then(|v| v.as_i64())
                .unwrap_or(INF);
            (n.id, v)
        })
        .collect();

    let nodes: Vec<i64> = input.nodes.iter().map(|n| n.id).collect();
    let mut iter = 0usize;
    let max_iters = max_iters_override.unwrap_or_else(|| nodes.len().saturating_add(8).max(8));

    loop {
        if iter >= max_iters {
            break;
        }
        iter += 1;

        let mut gathered: HashMap<i64, i64> = nodes.iter().map(|id| (*id, 0)).collect();
        for edge in &input.edges {
            let src_prop = *prop.get(&edge.src).unwrap_or(&INF);
            if (src_prop & ACTIVE) == 0 {
                continue;
            }
            let update = src_prop.saturating_add(1);
            let entry = gathered.entry(edge.dst).or_insert(0);
            if *entry == 0 || ((*entry & LOW_MASK) > (update & LOW_MASK)) {
                *entry = update;
            }
        }

        let mut newly_discovered = 0usize;
        for id in &nodes {
            let old = *prop.get(id).unwrap_or(&INF);
            let incoming = *gathered.get(id).unwrap_or(&0);

            if (incoming & ACTIVE) != 0 && old == INF {
                prop.insert(*id, incoming);
                newly_discovered += 1;
            } else {
                prop.insert(*id, old & LOW_MASK);
            }
        }

        if newly_discovered == 0 {
            break;
        }
    }

    prop
}

fn reference_ar(input: &GraphInput) -> HashMap<i64, i64> {
    const MASK: u32 = 0xFFFF_FFFF;
    const SCALE_DEGREE: u32 = 1u32 << 16;
    const KDAMP_FIX: u32 = 108;
    const CONST_TERM: u32 = 1_258_291;
    const ITERS: usize = 10;

    let nodes: Vec<i64> = input.nodes.iter().map(|n| n.id).collect();
    let mut score: HashMap<i64, u32> = input
        .nodes
        .iter()
        .map(|n| {
            let v = n
                .properties
                .get("score")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            (n.id, (v as u32) & MASK)
        })
        .collect();
    let denom: HashMap<i64, u32> = input
        .nodes
        .iter()
        .map(|n| {
            let v = n
                .properties
                .get("out_deg")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            (n.id, (v as u32) & MASK)
        })
        .collect();

    for _ in 0..ITERS {
        let mut summed: HashMap<i64, u32> = nodes.iter().map(|id| (*id, 0u32)).collect();
        for edge in &input.edges {
            let src_score = *score.get(&edge.src).unwrap_or(&0u32);
            let entry = summed.entry(edge.dst).or_insert(0u32);
            *entry = entry.wrapping_add(src_score);
        }

        let mut next: HashMap<i64, u32> = HashMap::new();
        for id in &nodes {
            let t_prop = *summed.get(id).unwrap_or(&0u32);
            let d = *denom.get(id).unwrap_or(&1u32);
            let tmp = if d == 0 { 0u32 } else { SCALE_DEGREE / d };
            let new_score = KDAMP_FIX.wrapping_mul(t_prop).wrapping_add(CONST_TERM);
            let update = new_score.wrapping_mul(tmp);
            next.insert(*id, update);
        }
        score = next;
    }

    score.into_iter().map(|(k, v)| (k, v as i64)).collect()
}

fn reference_wcc(input: &GraphInput, max_iters_override: Option<usize>) -> HashMap<i64, i64> {
    let nodes: Vec<i64> = input.nodes.iter().map(|n| n.id).collect();
    let mut label: HashMap<i64, i64> = input
        .nodes
        .iter()
        .map(|n| {
            let v = n
                .properties
                .get("label")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            (n.id, v)
        })
        .collect();

    let mut iter = 0usize;
    let max_iters = max_iters_override.unwrap_or_else(|| nodes.len().saturating_add(8).max(8));

    loop {
        if iter >= max_iters {
            break;
        }
        iter += 1;

        let mut gathered: HashMap<i64, i64> = nodes.iter().map(|id| (*id, 0)).collect();
        for edge in &input.edges {
            let src_label = *label.get(&edge.src).unwrap_or(&0);
            let entry = gathered.entry(edge.dst).or_insert(0);
            if src_label > *entry {
                *entry = src_label;
            }
        }

        let mut changed = false;
        for id in &nodes {
            let incoming = *gathered.get(id).unwrap_or(&0);
            let old = *label.get(id).unwrap_or(&0);
            if incoming != old {
                changed = true;
                label.insert(*id, incoming);
            }
        }

        if !changed {
            break;
        }
    }

    label
}

fn reference_sssp(input: &GraphInput, max_iters_override: Option<usize>) -> HashMap<i64, i64> {
    let mut dist: HashMap<i64, i64> = input
        .nodes
        .iter()
        .map(|n| {
            let d = n
                .properties
                .get("dist")
                .and_then(|v| v.as_i64())
                .unwrap_or(i64::MAX / 4);
            (n.id, d)
        })
        .collect();

    let mut changed = true;
    let mut iters = 0usize;
    let max_iters = max_iters_override.unwrap_or(usize::MAX);
    while changed && iters < max_iters {
        iters += 1;
        changed = false;
        for edge in &input.edges {
            let w = edge
                .properties
                .get("weight")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let src_d = *dist.get(&edge.src).unwrap_or(&(i64::MAX / 4));
            let dst_d = dist.get_mut(&edge.dst).unwrap();
            if src_d.saturating_add(w) < *dst_d {
                *dst_d = src_d.saturating_add(w);
                changed = true;
            }
        }
    }
    dist
}

fn reference_pagerank(input: &GraphInput, iters: usize) -> HashMap<i64, f64> {
    let mut rank: HashMap<i64, f64> = input
        .nodes
        .iter()
        .map(|n| {
            let r = n
                .properties
                .get("rank")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            (n.id, r)
        })
        .collect();
    let out_deg: HashMap<i64, f64> = input
        .nodes
        .iter()
        .map(|n| {
            let d = n
                .properties
                .get("out_deg")
                .and_then(|v| v.as_i64())
                .unwrap_or(1) as f64;
            (n.id, d)
        })
        .collect();

    for _ in 0..iters {
        let mut incoming: HashMap<i64, f64> = rank.keys().map(|id| (*id, 0.0)).collect();
        for edge in &input.edges {
            let contrib = rank.get(&edge.src).copied().unwrap_or(0.0);
            *incoming.entry(edge.dst).or_insert(0.0) += contrib;
        }
        let mut next: HashMap<i64, f64> = HashMap::new();
        for (node, sum_rank) in incoming {
            let deg = out_deg.get(&node).copied().unwrap_or(1.0).max(1.0);
            let new_rank = 0.15 + 0.85 * (sum_rank / deg);
            next.insert(node, new_rank);
        }
        rank = next;
    }

    rank
}

fn reference_connected_components(input: &GraphInput) -> HashMap<i64, i64> {
    let mut parent: HashMap<i64, i64> = input.nodes.iter().map(|n| (n.id, n.id)).collect();

    fn find(parent: &mut HashMap<i64, i64>, x: i64) -> i64 {
        let p = parent[&x];
        if p == x {
            x
        } else {
            let root = find(parent, p);
            parent.insert(x, root);
            root
        }
    }

    for edge in &input.edges {
        let a = find(&mut parent, edge.src);
        let b = find(&mut parent, edge.dst);
        let min = a.min(b);
        parent.insert(a, min);
        parent.insert(b, min);
    }

    let mut result = HashMap::new();
    for id in parent.keys().copied().collect::<Vec<_>>() {
        let root = find(&mut parent, id);
        let current = result.entry(id).or_insert(root);
        *current = root.min(*current);
    }

    result
}

fn reference_graph_coloring(input: &GraphInput, iters: usize) -> HashMap<i64, i64> {
    let mut colors: HashMap<i64, i64> = input
        .nodes
        .iter()
        .map(|n| {
            let c = n
                .properties
                .get("color")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            (n.id, c)
        })
        .collect();

    for _ in 0..iters {
        let mut next = colors.clone();
        for node in &input.nodes {
            let mut used = BTreeSet::new();
            for edge in input.edges.iter().filter(|e| e.dst == node.id) {
                if let Some(c) = colors.get(&edge.src) {
                    used.insert(*c);
                }
            }
            let mut candidate = 0;
            while used.contains(&candidate) {
                candidate += 1;
            }
            next.insert(node.id, candidate);
        }
        if next == colors {
            break;
        }
        colors = next;
    }

    colors
}

fn reference_als(input: &GraphInput) -> HashMap<i64, Vec<f64>> {
    input
        .nodes
        .iter()
        .map(|n| {
            let vec = n
                .properties
                .get("vec")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().map(|x| x.as_f64().unwrap_or(0.0)).collect())
                .unwrap_or_else(|| vec![0.0; 16]);
            (n.id, vec)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::gas_simulator::{EdgeRecord, NodeRecord};
    use rstest::rstest;

    #[rstest]
    #[case("sssp")]
    #[case("bfs")]
    #[case("ar")]
    #[case("pagerank")]
    #[case("connected_components")]
    #[case("wcc")]
    #[case("graph_coloring")]
    #[case("als")]
    fn rejects_unknown_field_counts(#[case] app: &str) {
        // sanity: should compute without error on empty minimal graph shapes
        let input = GraphInput {
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        let res = reference_values(app, &input, "dist");
        assert!(res.is_ok());
    }

    #[test]
    fn bounded_sssp_reference_respects_iteration_limit() {
        let input = GraphInput {
            nodes: vec![
                NodeRecord {
                    id: 0,
                    properties: HashMap::from([("dist".into(), serde_json::json!(0))]),
                },
                NodeRecord {
                    id: 1,
                    properties: HashMap::from([("dist".into(), serde_json::json!(1000000))]),
                },
                NodeRecord {
                    id: 2,
                    properties: HashMap::from([("dist".into(), serde_json::json!(1000000))]),
                },
            ],
            edges: vec![
                EdgeRecord {
                    src: 1,
                    dst: 2,
                    properties: HashMap::from([("weight".into(), serde_json::json!(1))]),
                },
                EdgeRecord {
                    src: 0,
                    dst: 1,
                    properties: HashMap::from([("weight".into(), serde_json::json!(1))]),
                },
            ],
        };

        let one_iter = reference_values_with_iters("sssp", &input, "dist", Some(1))
            .expect("bounded reference");
        let full = reference_values("sssp", &input, "dist").expect("full reference");

        assert_eq!(one_iter.get(&1), Some(&Value::Int(1)));
        assert_eq!(one_iter.get(&2), Some(&Value::Int(1_000_000)));
        assert_eq!(full.get(&2), Some(&Value::Int(2)));
    }
}
