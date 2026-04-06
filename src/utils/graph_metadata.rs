use std::{
    collections::hash_map::DefaultHasher,
    fmt, fs,
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use serde::{Deserialize, Serialize};

const RUNNER_CANDIDATES: &[&str] = &[
    "run_all_benchmarks.py",
    "run_all_benchmarks.py",
    "run_all_benchmarks.py",
];

const DATASET_ROOT_CANDIDATES: &[&str] = &[
    "/path/to/datasets",
    "/path/to/datasets",
    "/path/to/datasets",
];

const METADATA_CACHE_VERSION: u32 = 2;
const RAW_GRAPH_CACHE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphFormat {
    MatrixMarket,
    EdgeListTxt,
    Unknown,
}

impl fmt::Display for GraphFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MatrixMarket => write!(f, "mtx"),
            Self::EdgeListTxt => write!(f, "txt"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphDomain {
    Rmat,
    Social,
    Web,
    Wiki,
    Commerce,
    Knowledge,
    Entertainment,
    Other,
}

impl fmt::Display for GraphDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rmat => write!(f, "rmat"),
            Self::Social => write!(f, "social"),
            Self::Web => write!(f, "web"),
            Self::Wiki => write!(f, "wiki"),
            Self::Commerce => write!(f, "commerce"),
            Self::Knowledge => write!(f, "knowledge"),
            Self::Entertainment => write!(f, "entertainment"),
            Self::Other => write!(f, "other"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphMetadata {
    pub source: PathBuf,
    pub graph_path: Option<PathBuf>,
    pub dataset: String,
    pub format: GraphFormat,
    pub domain: GraphDomain,
    pub vertices: u64,
    pub edges: u64,
    pub average_degree: f64,
    pub density: f64,
    pub log_vertices: f64,
    pub log_edges: f64,
    pub scale_hint: Option<f64>,
    pub edge_factor_hint: Option<f64>,
    pub active_src_fraction: f64,
    pub active_dst_fraction: f64,
    pub max_out_degree: u64,
    pub max_in_degree: u64,
    pub out_degree_cv: f64,
    pub in_degree_cv: f64,
    pub p90_in_degree: u64,
    pub p99_in_degree: u64,
    pub top_1pct_in_share: f64,
    pub top_0_1pct_in_share: f64,
    pub in_degree_gini: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPartitionProfile {
    pub metadata: GraphMetadata,
    pub dst_indegrees_desc: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawGraphEdge {
    pub src: u32,
    pub dst: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawGraph {
    pub vertices: usize,
    pub edges: Vec<RawGraphEdge>,
    pub in_degree: Vec<u32>,
}

impl GraphMetadata {
    pub fn feature_vector(&self) -> Vec<f64> {
        let mean_degree = self.average_degree.max(1e-9);
        vec![
            self.log_vertices,
            self.log_edges,
            mean_degree.max(1.0).ln(),
            (self.density + 1e-18).ln(),
            self.scale_hint.unwrap_or(0.0),
            self.edge_factor_hint.unwrap_or(0.0),
            self.active_src_fraction,
            self.active_dst_fraction,
            ((self.max_out_degree as f64 / mean_degree) + 1.0).ln(),
            ((self.max_in_degree as f64 / mean_degree) + 1.0).ln(),
            self.out_degree_cv,
            self.in_degree_cv,
            ((self.p90_in_degree as f64 / mean_degree) + 1.0).ln(),
            ((self.p99_in_degree as f64 / mean_degree) + 1.0).ln(),
            self.top_1pct_in_share,
            self.top_0_1pct_in_share,
            self.in_degree_gini,
            domain_score(&self.domain),
            format_score(&self.format),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RawGraphStats {
    graph_path: PathBuf,
    format: GraphFormat,
    vertices: u64,
    edges: u64,
    average_degree: f64,
    density: f64,
    active_src_fraction: f64,
    active_dst_fraction: f64,
    max_out_degree: u64,
    max_in_degree: u64,
    out_degree_cv: f64,
    in_degree_cv: f64,
    #[serde(default)]
    p90_in_degree: u64,
    #[serde(default)]
    p99_in_degree: u64,
    #[serde(default)]
    top_1pct_in_share: f64,
    #[serde(default)]
    top_0_1pct_in_share: f64,
    #[serde(default)]
    in_degree_gini: f64,
    #[serde(default)]
    dst_indegrees_desc: Vec<u32>,
    file_len: u64,
    modified_secs: u64,
}

pub fn extract_graph_metadata_from_profile_log(path: &Path) -> Option<GraphMetadata> {
    let dataset = dataset_name_from_profile_log(path)?;
    let canonical_dataset = canonical_dataset_name(&dataset);
    let body = fs::read_to_string(path).ok()?;
    let mut format = infer_format_from_name(&canonical_dataset);
    let mut vertices = None;
    let mut edges = None;

    for line in body.lines() {
        if line.contains("Detected .mtx format") {
            format = GraphFormat::MatrixMarket;
        } else if line.contains("Detected .txt format") {
            format = GraphFormat::EdgeListTxt;
        } else if line.contains("Global graph has ")
            && let Some((v, e)) = parse_graph_size_line(line)
        {
            vertices = Some(v);
            edges = Some(e);
            break;
        }
    }

    let graph_path = resolve_raw_graph_path(&canonical_dataset);
    let raw = graph_path.as_deref().and_then(|graph_path| {
        load_or_compute_raw_graph_stats(graph_path, &format, vertices, edges)
    });

    let vertices = raw.as_ref().map(|item| item.vertices).or(vertices)?;
    let edges = raw.as_ref().map(|item| item.edges).or(edges)?;
    let average_degree = raw
        .as_ref()
        .map(|item| item.average_degree)
        .unwrap_or_else(|| {
            if vertices == 0 {
                0.0
            } else {
                edges as f64 / vertices as f64
            }
        });
    let density = raw.as_ref().map(|item| item.density).unwrap_or_else(|| {
        if vertices <= 1 {
            0.0
        } else {
            edges as f64 / ((vertices as f64) * ((vertices - 1) as f64))
        }
    });
    let (scale_hint, edge_factor_hint) = parse_scale_edge_factor(&canonical_dataset);

    Some(GraphMetadata {
        source: path.to_path_buf(),
        graph_path,
        dataset: canonical_dataset.clone(),
        format,
        domain: infer_domain(&canonical_dataset),
        vertices,
        edges,
        average_degree,
        density,
        log_vertices: (vertices as f64).max(1.0).ln(),
        log_edges: (edges as f64).max(1.0).ln(),
        scale_hint,
        edge_factor_hint,
        active_src_fraction: raw
            .as_ref()
            .map(|item| item.active_src_fraction)
            .unwrap_or(0.0),
        active_dst_fraction: raw
            .as_ref()
            .map(|item| item.active_dst_fraction)
            .unwrap_or(0.0),
        max_out_degree: raw.as_ref().map(|item| item.max_out_degree).unwrap_or(0),
        max_in_degree: raw.as_ref().map(|item| item.max_in_degree).unwrap_or(0),
        out_degree_cv: raw.as_ref().map(|item| item.out_degree_cv).unwrap_or(0.0),
        in_degree_cv: raw.as_ref().map(|item| item.in_degree_cv).unwrap_or(0.0),
        p90_in_degree: raw.as_ref().map(|item| item.p90_in_degree).unwrap_or(0),
        p99_in_degree: raw.as_ref().map(|item| item.p99_in_degree).unwrap_or(0),
        top_1pct_in_share: raw
            .as_ref()
            .map(|item| item.top_1pct_in_share)
            .unwrap_or(0.0),
        top_0_1pct_in_share: raw
            .as_ref()
            .map(|item| item.top_0_1pct_in_share)
            .unwrap_or(0.0),
        in_degree_gini: raw.as_ref().map(|item| item.in_degree_gini).unwrap_or(0.0),
    })
}

pub fn extract_graph_metadata_from_dataset_path(path: &Path) -> Option<GraphMetadata> {
    let graph_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        fs::canonicalize(path).ok()?
    };
    let dataset = graph_path.file_name()?.to_str()?.to_string();
    let format = infer_format_from_name(&dataset);
    let raw = load_or_compute_raw_graph_stats(&graph_path, &format, None, None)?;
    let (scale_hint, edge_factor_hint) = parse_scale_edge_factor(&dataset);

    Some(GraphMetadata {
        source: graph_path.clone(),
        graph_path: Some(graph_path),
        dataset: dataset.clone(),
        format,
        domain: infer_domain(&dataset),
        vertices: raw.vertices,
        edges: raw.edges,
        average_degree: raw.average_degree,
        density: raw.density,
        log_vertices: (raw.vertices as f64).max(1.0).ln(),
        log_edges: (raw.edges as f64).max(1.0).ln(),
        scale_hint,
        edge_factor_hint,
        active_src_fraction: raw.active_src_fraction,
        active_dst_fraction: raw.active_dst_fraction,
        max_out_degree: raw.max_out_degree,
        max_in_degree: raw.max_in_degree,
        out_degree_cv: raw.out_degree_cv,
        in_degree_cv: raw.in_degree_cv,
        p90_in_degree: raw.p90_in_degree,
        p99_in_degree: raw.p99_in_degree,
        top_1pct_in_share: raw.top_1pct_in_share,
        top_0_1pct_in_share: raw.top_0_1pct_in_share,
        in_degree_gini: raw.in_degree_gini,
    })
}

pub fn extract_partition_profile_from_profile_log(path: &Path) -> Option<GraphPartitionProfile> {
    let metadata = extract_graph_metadata_from_profile_log(path)?;
    let graph_path = metadata.graph_path.as_ref()?;
    let raw = load_or_compute_raw_graph_stats(
        graph_path,
        &metadata.format,
        Some(metadata.vertices),
        Some(metadata.edges),
    )?;
    Some(GraphPartitionProfile {
        metadata,
        dst_indegrees_desc: raw.dst_indegrees_desc,
    })
}

pub fn extract_partition_profile_from_dataset_path(path: &Path) -> Option<GraphPartitionProfile> {
    let metadata = extract_graph_metadata_from_dataset_path(path)?;
    let graph_path = metadata.graph_path.as_ref()?;
    let raw = load_or_compute_raw_graph_stats(
        graph_path,
        &metadata.format,
        Some(metadata.vertices),
        Some(metadata.edges),
    )?;
    Some(GraphPartitionProfile {
        metadata,
        dst_indegrees_desc: raw.dst_indegrees_desc,
    })
}

pub fn load_raw_graph_from_profile_log(path: &Path) -> Option<RawGraph> {
    let metadata = extract_graph_metadata_from_profile_log(path)?;
    let graph_path = metadata.graph_path?;
    load_raw_graph(&graph_path, &metadata.format, metadata.vertices as usize)
}

pub fn load_raw_graph_from_metadata(metadata: &GraphMetadata) -> Option<RawGraph> {
    let graph_path = metadata.graph_path.as_ref()?;
    load_raw_graph(graph_path, &metadata.format, metadata.vertices as usize)
}

pub fn dataset_name_from_profile_log(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    if name.contains("__") {
        let mut parts = name.splitn(3, "__");
        let _variant = parts.next()?;
        let dataset = parts.next()?;
        return Some(canonical_dataset_name(dataset));
    }

    if name.starts_with("rmat-") && name.contains("_hw_") {
        return Some(canonical_dataset_name(name));
    }

    None
}

fn canonical_dataset_name(name: &str) -> String {
    if name.starts_with("rmat-") && name.contains("_hw_") {
        let base = name
            .split_once("_hw_")
            .map(|(prefix, _)| prefix)
            .unwrap_or(name);
        return format!("{base}.txt");
    }
    name.to_string()
}

fn infer_format_from_name(dataset: &str) -> GraphFormat {
    if dataset.ends_with(".mtx") {
        GraphFormat::MatrixMarket
    } else if dataset.ends_with(".txt") {
        GraphFormat::EdgeListTxt
    } else {
        GraphFormat::Unknown
    }
}

fn resolve_raw_graph_path(dataset: &str) -> Option<PathBuf> {
    for runner in RUNNER_CANDIDATES {
        if let Some(path) = resolve_dataset_from_runner(Path::new(runner), dataset) {
            return Some(path);
        }
    }

    for root in DATASET_ROOT_CANDIDATES {
        let path = Path::new(root).join(dataset);
        if path.exists() {
            return Some(path);
        }
    }

    None
}

fn resolve_dataset_from_runner(runner: &Path, dataset: &str) -> Option<PathBuf> {
    let body = fs::read_to_string(runner).ok()?;
    for line in body.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('"') {
            continue;
        }
        let mut pieces = trimmed.split('"');
        let _ = pieces.next();
        let candidate = pieces.next()?;
        let candidate_path = PathBuf::from(candidate);
        if candidate_path.file_name()?.to_str()? == dataset && candidate_path.exists() {
            return Some(candidate_path);
        }
    }
    None
}

fn load_or_compute_raw_graph_stats(
    graph_path: &Path,
    format: &GraphFormat,
    fallback_vertices: Option<u64>,
    fallback_edges: Option<u64>,
) -> Option<RawGraphStats> {
    let file_meta = fs::metadata(graph_path).ok()?;
    let modified_secs = file_meta
        .modified()
        .ok()
        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
        .map(|dur| dur.as_secs())
        .unwrap_or(0);
    let file_len = file_meta.len();

    let cache_path = metadata_cache_path(graph_path);
    if let Ok(cached) = fs::read_to_string(&cache_path)
        && let Ok(stats) = serde_json::from_str::<RawGraphStats>(&cached)
        && stats.graph_path == graph_path
        && stats.file_len == file_len
        && stats.modified_secs == modified_secs
    {
        return Some(stats);
    }

    let stats = compute_raw_graph_stats(
        graph_path,
        format,
        fallback_vertices.unwrap_or(0),
        fallback_edges.unwrap_or(0),
        file_len,
        modified_secs,
    )?;

    if let Some(parent) = cache_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(serialized) = serde_json::to_string(&stats) {
        let _ = fs::write(cache_path, serialized);
    }

    Some(stats)
}

fn load_raw_graph(
    graph_path: &Path,
    format: &GraphFormat,
    fallback_vertices: usize,
) -> Option<RawGraph> {
    let file_meta = fs::metadata(graph_path).ok()?;
    let modified_secs = file_meta
        .modified()
        .ok()
        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
        .map(|dur| dur.as_secs())
        .unwrap_or(0);
    let file_len = file_meta.len();
    if let Some(raw) = load_raw_graph_cache(graph_path, file_len, modified_secs) {
        return Some(raw);
    }

    let file = fs::File::open(graph_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut vertices = fallback_vertices;
    let is_one_based = matches!(format, GraphFormat::MatrixMarket);

    if matches!(format, GraphFormat::MatrixMarket) {
        loop {
            line.clear();
            if reader.read_line(&mut line).ok()? == 0 {
                return None;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('%') {
                continue;
            }
            let header = trimmed
                .split_whitespace()
                .take(3)
                .map(str::parse::<usize>)
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            if header.len() == 3 {
                vertices = header[0].max(header[1]);
                break;
            }
        }
    }

    if vertices == 0 {
        vertices = infer_vertex_count_from_edgelist(graph_path, is_one_based)?;
    }

    if vertices == 0 {
        return None;
    }

    let mut edges = Vec::new();
    let mut in_degree = vec![0u32; vertices];
    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('%') || trimmed.starts_with('#') {
            continue;
        }
        let mut fields = trimmed.split_whitespace();
        let src = fields.next()?.parse::<usize>().ok()?;
        let dst = fields.next()?.parse::<usize>().ok()?;
        let src = if is_one_based {
            src.checked_sub(1)?
        } else {
            src
        };
        let dst = if is_one_based {
            dst.checked_sub(1)?
        } else {
            dst
        };
        if src >= vertices || dst >= vertices {
            return None;
        }
        edges.push(RawGraphEdge {
            src: src as u32,
            dst: dst as u32,
        });
        in_degree[dst] = in_degree[dst].saturating_add(1);
    }

    edges.sort_unstable_by(|lhs, rhs| lhs.src.cmp(&rhs.src).then_with(|| lhs.dst.cmp(&rhs.dst)));

    let raw = RawGraph {
        vertices,
        edges,
        in_degree,
    };
    let _ = store_raw_graph_cache(graph_path, &raw, file_len, modified_secs);
    Some(raw)
}

fn metadata_cache_path(graph_path: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    graph_path.hash(&mut hasher);
    let hash = hasher.finish();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("graph_metadata_cache")
        .join(format!("v{METADATA_CACHE_VERSION}_{hash:016x}.json"))
}

fn raw_graph_cache_path(graph_path: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    graph_path.hash(&mut hasher);
    let hash = hasher.finish();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("graph_metadata_cache")
        .join(format!("raw_v{RAW_GRAPH_CACHE_VERSION}_{hash:016x}.bin"))
}

fn load_raw_graph_cache(graph_path: &Path, file_len: u64, modified_secs: u64) -> Option<RawGraph> {
    let cache_path = raw_graph_cache_path(graph_path);
    let mut file = fs::File::open(cache_path).ok()?;
    let version = read_u32(&mut file)?;
    if version != RAW_GRAPH_CACHE_VERSION {
        return None;
    }
    let cached_file_len = read_u64(&mut file)?;
    let cached_modified_secs = read_u64(&mut file)?;
    if cached_file_len != file_len || cached_modified_secs != modified_secs {
        return None;
    }
    let vertices = read_u64(&mut file)? as usize;
    let in_degree_len = read_u64(&mut file)? as usize;
    let edge_len = read_u64(&mut file)? as usize;
    let mut in_degree = vec![0u32; in_degree_len];
    for value in &mut in_degree {
        *value = read_u32(&mut file)?;
    }
    let mut edges = Vec::with_capacity(edge_len);
    for _ in 0..edge_len {
        edges.push(RawGraphEdge {
            src: read_u32(&mut file)?,
            dst: read_u32(&mut file)?,
        });
    }
    Some(RawGraph {
        vertices,
        edges,
        in_degree,
    })
}

fn store_raw_graph_cache(
    graph_path: &Path,
    graph: &RawGraph,
    file_len: u64,
    modified_secs: u64,
) -> Option<()> {
    let cache_path = raw_graph_cache_path(graph_path);
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).ok()?;
    }
    let mut file = fs::File::create(cache_path).ok()?;
    write_u32(&mut file, RAW_GRAPH_CACHE_VERSION).ok()?;
    write_u64(&mut file, file_len).ok()?;
    write_u64(&mut file, modified_secs).ok()?;
    write_u64(&mut file, graph.vertices as u64).ok()?;
    write_u64(&mut file, graph.in_degree.len() as u64).ok()?;
    write_u64(&mut file, graph.edges.len() as u64).ok()?;
    for &value in &graph.in_degree {
        write_u32(&mut file, value).ok()?;
    }
    for edge in &graph.edges {
        write_u32(&mut file, edge.src).ok()?;
        write_u32(&mut file, edge.dst).ok()?;
    }
    Some(())
}

fn read_u32(file: &mut fs::File) -> Option<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).ok()?;
    Some(u32::from_le_bytes(buf))
}

fn read_u64(file: &mut fs::File) -> Option<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).ok()?;
    Some(u64::from_le_bytes(buf))
}

fn write_u32(file: &mut fs::File, value: u32) -> std::io::Result<()> {
    file.write_all(&value.to_le_bytes())
}

fn write_u64(file: &mut fs::File, value: u64) -> std::io::Result<()> {
    file.write_all(&value.to_le_bytes())
}

fn compute_raw_graph_stats(
    graph_path: &Path,
    format: &GraphFormat,
    fallback_vertices: u64,
    fallback_edges: u64,
    file_len: u64,
    modified_secs: u64,
) -> Option<RawGraphStats> {
    let file = fs::File::open(graph_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut vertices = fallback_vertices as usize;
    let mut declared_edges = fallback_edges;
    let is_one_based = matches!(format, GraphFormat::MatrixMarket);

    if matches!(format, GraphFormat::MatrixMarket) {
        loop {
            line.clear();
            if reader.read_line(&mut line).ok()? == 0 {
                return None;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('%') {
                continue;
            }
            let header = trimmed
                .split_whitespace()
                .take(3)
                .map(str::parse::<usize>)
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            if header.len() == 3 {
                vertices = header[0].max(header[1]);
                declared_edges = header[2] as u64;
                break;
            }
        }
    }

    if vertices == 0 {
        vertices = fallback_vertices as usize;
    }
    if vertices == 0 {
        vertices = infer_vertex_count_from_edgelist(graph_path, is_one_based)?;
    }
    if vertices == 0 {
        return None;
    }

    let mut out_degree = vec![0u32; vertices];
    let mut in_degree = vec![0u32; vertices];
    let mut actual_edges = 0u64;

    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('%') || trimmed.starts_with('#') {
            continue;
        }
        let mut fields = trimmed.split_whitespace();
        let src = fields.next()?.parse::<usize>().ok()?;
        let dst = fields.next()?.parse::<usize>().ok()?;
        let src = if is_one_based {
            src.checked_sub(1)?
        } else {
            src
        };
        let dst = if is_one_based {
            dst.checked_sub(1)?
        } else {
            dst
        };
        if src >= vertices || dst >= vertices {
            return None;
        }
        out_degree[src] = out_degree[src].saturating_add(1);
        in_degree[dst] = in_degree[dst].saturating_add(1);
        actual_edges += 1;
    }

    let edges = actual_edges.max(declared_edges);
    let average_degree = if vertices == 0 {
        0.0
    } else {
        edges as f64 / vertices as f64
    };
    let density = if vertices <= 1 {
        0.0
    } else {
        edges as f64 / ((vertices as f64) * ((vertices - 1) as f64))
    };

    let (active_src_fraction, max_out_degree, out_degree_cv) =
        degree_stats(&out_degree, average_degree);
    let (active_dst_fraction, max_in_degree, in_degree_cv) =
        degree_stats(&in_degree, average_degree);
    let (p90_in_degree, p99_in_degree, top_1pct_in_share, top_0_1pct_in_share, in_degree_gini) =
        indegree_shape_stats(&in_degree, edges);
    let mut dst_indegrees_desc: Vec<u32> = in_degree
        .iter()
        .copied()
        .filter(|degree| *degree > 0)
        .collect();
    dst_indegrees_desc.sort_unstable_by(|lhs, rhs| rhs.cmp(lhs));

    Some(RawGraphStats {
        graph_path: graph_path.to_path_buf(),
        format: format.clone(),
        vertices: vertices as u64,
        edges,
        average_degree,
        density,
        active_src_fraction,
        active_dst_fraction,
        max_out_degree,
        max_in_degree,
        out_degree_cv,
        in_degree_cv,
        p90_in_degree,
        p99_in_degree,
        top_1pct_in_share,
        top_0_1pct_in_share,
        in_degree_gini,
        dst_indegrees_desc,
        file_len,
        modified_secs,
    })
}

fn infer_vertex_count_from_edgelist(graph_path: &Path, is_one_based: bool) -> Option<usize> {
    let file = fs::File::open(graph_path).ok()?;
    let reader = BufReader::new(file);
    let mut max_vertex = None::<usize>;

    for line in reader.lines() {
        let line = line.ok()?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('%') || trimmed.starts_with('#') {
            continue;
        }
        let mut fields = trimmed.split_whitespace();
        let src = fields.next()?.parse::<usize>().ok()?;
        let dst = fields.next()?.parse::<usize>().ok()?;
        let src = if is_one_based {
            src.checked_sub(1)?
        } else {
            src
        };
        let dst = if is_one_based {
            dst.checked_sub(1)?
        } else {
            dst
        };
        let local_max = src.max(dst);
        max_vertex = Some(max_vertex.map(|value| value.max(local_max)).unwrap_or(local_max));
    }

    max_vertex.map(|value| value + 1)
}

fn degree_stats(degrees: &[u32], mean: f64) -> (f64, u64, f64) {
    if degrees.is_empty() {
        return (0.0, 0, 0.0);
    }

    let mut active = 0usize;
    let mut max_degree = 0u64;
    let mut sum_sq = 0.0;

    for &degree in degrees {
        if degree > 0 {
            active += 1;
        }
        max_degree = max_degree.max(degree as u64);
        let value = degree as f64;
        sum_sq += value * value;
    }

    let variance = (sum_sq / degrees.len() as f64) - mean * mean;
    let stddev = variance.max(0.0).sqrt();
    let cv = if mean <= f64::EPSILON {
        0.0
    } else {
        stddev / mean
    };

    (active as f64 / degrees.len() as f64, max_degree, cv)
}

fn indegree_shape_stats(degrees: &[u32], total_edges: u64) -> (u64, u64, f64, f64, f64) {
    if degrees.is_empty() {
        return (0, 0, 0.0, 0.0, 0.0);
    }

    let mut sorted = degrees.to_vec();
    sorted.sort_unstable();

    let p90 = percentile_u32(&sorted, 0.90) as u64;
    let p99 = percentile_u32(&sorted, 0.99) as u64;
    let total_edges_f = total_edges.max(1) as f64;

    let top_1pct_count = ((sorted.len() as f64) * 0.01).ceil().max(1.0) as usize;
    let top_0_1pct_count = ((sorted.len() as f64) * 0.001).ceil().max(1.0) as usize;
    let top_1pct_sum = sorted
        .iter()
        .rev()
        .take(top_1pct_count)
        .map(|&v| v as u64)
        .sum::<u64>();
    let top_0_1pct_sum = sorted
        .iter()
        .rev()
        .take(top_0_1pct_count)
        .map(|&v| v as u64)
        .sum::<u64>();

    let sum = sorted.iter().map(|&v| v as f64).sum::<f64>();
    let n = sorted.len() as f64;
    let weighted_sum = sorted
        .iter()
        .enumerate()
        .map(|(idx, &value)| (idx as f64 + 1.0) * value as f64)
        .sum::<f64>();
    let gini = if sum <= f64::EPSILON {
        0.0
    } else {
        ((2.0 * weighted_sum) / (n * sum)) - ((n + 1.0) / n)
    };

    (
        p90,
        p99,
        top_1pct_sum as f64 / total_edges_f,
        top_0_1pct_sum as f64 / total_edges_f,
        gini.max(0.0),
    )
}

fn percentile_u32(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

fn parse_graph_size_line(line: &str) -> Option<(u64, u64)> {
    let marker = "Global graph has ";
    let start = line.find(marker)? + marker.len();
    let tail = &line[start..];
    let vertices_end = tail.find(" vertices and ")?;
    let vertices = tail[..vertices_end]
        .trim()
        .replace(',', "")
        .parse::<u64>()
        .ok()?;
    let tail = &tail[vertices_end + " vertices and ".len()..];
    let edges_end = tail.find(" edges")?;
    let edges = tail[..edges_end]
        .trim()
        .replace(',', "")
        .parse::<u64>()
        .ok()?;
    Some((vertices, edges))
}

fn infer_domain(dataset: &str) -> GraphDomain {
    if dataset.starts_with("rmat-") || dataset.starts_with("graph500-") {
        GraphDomain::Rmat
    } else if dataset.starts_with("soc-") {
        GraphDomain::Social
    } else if dataset.starts_with("web-") {
        GraphDomain::Web
    } else if dataset.starts_with("wiki-") {
        GraphDomain::Wiki
    } else if dataset.starts_with("amazon-") {
        GraphDomain::Commerce
    } else if dataset.starts_with("dbpedia-") {
        GraphDomain::Knowledge
    } else if dataset.contains("hollywood") {
        GraphDomain::Entertainment
    } else {
        GraphDomain::Other
    }
}

fn parse_scale_edge_factor(dataset: &str) -> (Option<f64>, Option<f64>) {
    if let Some(rest) = dataset.strip_prefix("rmat-") {
        let mut parts = rest.split('-');
        let scale = parts.next().and_then(|value| value.parse::<f64>().ok());
        let ef = parts
            .next()
            .map(|value| value.trim_end_matches(".txt"))
            .and_then(|value| value.parse::<f64>().ok());
        return (scale, ef);
    }

    if let Some(scale_pos) = dataset.find("scale") {
        let scale_part = &dataset[scale_pos + "scale".len()..];
        let scale_digits: String = scale_part
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect();
        let scale = scale_digits.parse::<f64>().ok();

        let ef = dataset.find("-ef").and_then(|pos| {
            let ef_part = &dataset[pos + "-ef".len()..];
            let digits: String = ef_part
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .collect();
            digits.parse::<f64>().ok()
        });
        return (scale, ef);
    }

    (None, None)
}

fn domain_score(domain: &GraphDomain) -> f64 {
    match domain {
        GraphDomain::Rmat => 1.0,
        GraphDomain::Social => 2.0,
        GraphDomain::Web => 3.0,
        GraphDomain::Wiki => 4.0,
        GraphDomain::Commerce => 5.0,
        GraphDomain::Knowledge => 6.0,
        GraphDomain::Entertainment => 7.0,
        GraphDomain::Other => 0.0,
    }
}

fn format_score(format: &GraphFormat) -> f64 {
    match format {
        GraphFormat::MatrixMarket => 1.0,
        GraphFormat::EdgeListTxt => 2.0,
        GraphFormat::Unknown => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    #[test]
    fn parses_metadata_from_autoprofile_log() {
        let dir = temp_dir("graph-metadata");
        let path = dir.join("c4l10_n1__amazon-2008.mtx__autoprofile.log");
        fs::write(
            &path,
            concat!(
                "--- Step 1: Loading Graph Data ---\n",
                "Detected .mtx format (1-based indexing).\n",
                "Global graph has 735323 vertices and 5158389 edges.\n"
            ),
        )
        .expect("write log");

        let metadata = extract_graph_metadata_from_profile_log(&path).expect("metadata");
        assert_eq!(metadata.dataset, "amazon-2008.mtx");
        assert_eq!(metadata.format, GraphFormat::MatrixMarket);
        assert_eq!(metadata.domain, GraphDomain::Commerce);
        assert_eq!(metadata.vertices, 735323);
        assert!(metadata.edges >= 5_158_388);
        assert!(metadata.average_degree > 7.0);
    }

    #[test]
    fn parses_rmat_hints_from_dataset_name() {
        let (scale, ef) = parse_scale_edge_factor("rmat-19-32.txt");
        assert_eq!(scale, Some(19.0));
        assert_eq!(ef, Some(32.0));

        let (scale, ef) = parse_scale_edge_factor("graph500-scale23-ef16_adj.mtx");
        assert_eq!(scale, Some(23.0));
        assert_eq!(ef, Some(16.0));
    }

    #[test]
    fn canonicalizes_profiled_rmat_name() {
        assert_eq!(
            dataset_name_from_profile_log(Path::new("rmat-19-32_hw_b280_l1040.log")).as_deref(),
            Some("rmat-19-32.txt")
        );
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("graphyflow-{label}-{}", std::process::id()));
        if dir.exists() {
            fs::remove_dir_all(&dir).expect("clear temp dir");
        }
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}
