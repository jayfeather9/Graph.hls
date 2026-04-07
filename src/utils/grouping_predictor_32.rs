use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::utils::graph_metadata::{
    GraphDomain, GraphFormat, GraphMetadata, extract_graph_metadata_from_dataset_path,
};

const STATIC_MODEL_VERSION: u32 = 1;
const DATASET_ROOT_CANDIDATES: &[&str] = &[
    "/data/feiyang/test/test/datasets",
    "/data/zhuohang/dataset/ReGraph_dataset/useable",
    "/data/zhuohang/dataset/ReGraph_dataset/output",
];

#[derive(Debug, Error)]
pub enum Grouping32PredictorError {
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid benchmark header in {0}")]
    InvalidBenchmarkHeader(PathBuf),
    #[error("invalid benchmark row in {path}: {message}")]
    InvalidBenchmarkRow { path: PathBuf, message: String },
    #[error("missing dataset file for benchmark dataset '{0}'")]
    MissingDatasetPath(String),
    #[error("failed to extract metadata from dataset '{0}'")]
    InvalidDatasetMetadata(String),
    #[error("no benchmark rows found in {0}")]
    NoBenchmarkRows(PathBuf),
    #[error("failed to parse json model {path}: {source}")]
    ParseJsonModel {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize json model {path}: {source}")]
    SerializeJsonModel {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grouping32Shape {
    pub variant: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32CandidateScore {
    pub variant: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
    pub score: f64,
    pub classifier_probability: f64,
    pub regression_score: f64,
    pub knn_score: Option<f64>,
    pub seen_in_training: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32Prediction {
    pub model_path: PathBuf,
    pub metadata: GraphMetadata,
    pub total_datasets: usize,
    pub candidate_variants: usize,
    pub recommended_variant: String,
    pub recommended_big_groups: Vec<usize>,
    pub recommended_little_groups: Vec<usize>,
    pub ranked_candidates: Vec<Grouping32CandidateScore>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32EvaluationSummary {
    pub dataset_holdout_cases: usize,
    pub variant_accuracy: f64,
    pub average_regret: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32StaticModel {
    pub version: u32,
    pub benchmark_source: PathBuf,
    pub total_datasets: usize,
    pub candidate_shapes: Vec<Grouping32Shape>,
    pub feature_mean: Vec<f64>,
    pub feature_scale: Vec<f64>,
    pub feature_group_weights: Vec<f64>,
    pub label_knn_neighbors: usize,
    pub label_distance_power: f64,
    pub winner_exemplars: Vec<Grouping32WinnerExemplar>,
    pub blend_alpha: f64,
    pub knn_neighbors: usize,
    pub classifier: Grouping32DecisionTree,
    pub variant_models: Vec<Grouping32VariantModel>,
    pub known_dataset_best: Vec<Grouping32KnownBest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32KnownBest {
    pub dataset: String,
    pub source: PathBuf,
    pub recommended_variant: String,
    pub recommended_big_groups: Vec<usize>,
    pub recommended_little_groups: Vec<usize>,
    pub throughput: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32VariantModel {
    pub variant: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
    pub regression_weights: Vec<f64>,
    pub training_examples: usize,
    pub exemplar_points: Vec<Grouping32Exemplar>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32Exemplar {
    pub dataset: String,
    pub features: Vec<f64>,
    pub log_throughput: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grouping32WinnerExemplar {
    pub dataset: String,
    pub features: Vec<f64>,
    pub best_variant: String,
    pub scores_by_variant: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
struct BenchmarkRow {
    dataset_key: String,
    metadata: GraphMetadata,
    path: PathBuf,
    scores_by_variant: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
struct RawBenchmarkRow {
    dataset: String,
    graphy3b11l: Option<f64>,
    graphy3b11l_256hz: Option<f64>,
    b2l444: Option<f64>,
    b2l66: Option<f64>,
    b22l333: Option<f64>,
    b3l433: Option<f64>,
    b3l55: Option<f64>,
    b4l333: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Grouping32DecisionTree {
    Leaf {
        counts: BTreeMap<String, usize>,
    },
    Split {
        feature_idx: usize,
        threshold: f64,
        left: Box<Grouping32DecisionTree>,
        right: Box<Grouping32DecisionTree>,
    },
}

pub fn train_static_grouping32_model(
    benchmark_path: &Path,
) -> Result<Grouping32StaticModel, Grouping32PredictorError> {
    let rows = load_benchmark_rows(benchmark_path)?;
    let candidate_shapes = candidate_shapes();
    let feature_rows: Vec<Vec<f64>> = rows
        .iter()
        .map(|row| metadata_feature_vector_32(&row.metadata))
        .collect();
    let standardizer = fit_standardizer(&feature_rows);
    let scaled_rows: Vec<Vec<f64>> = feature_rows
        .iter()
        .map(|row| standardizer.transform(row))
        .collect();

    let variant_models: Vec<_> = candidate_shapes
        .iter()
        .map(|shape| train_variant_model(shape, &rows, &scaled_rows))
        .collect();
    let (blend_alpha, knn_neighbors) =
        tune_blend_parameters(&candidate_shapes, &variant_models, &rows, &scaled_rows);
    let winner_exemplars = winner_exemplars_from_rows(&rows, &scaled_rows);
    let (feature_group_weights, label_knn_neighbors, label_distance_power) =
        tune_label_knn_parameters(&winner_exemplars);
    let known_dataset_best = rows
        .iter()
        .filter_map(|row| {
            best_variant_for_scores(&candidate_shapes, &row.scores_by_variant).map(
                |(shape, throughput)| Grouping32KnownBest {
                    dataset: row.metadata.dataset.clone(),
                    source: row.path.clone(),
                    recommended_variant: shape.variant.clone(),
                    recommended_big_groups: shape.big_groups.clone(),
                    recommended_little_groups: shape.little_groups.clone(),
                    throughput,
                },
            )
        })
        .collect();

    let classifier = train_classifier_tree(&rows, &scaled_rows, 0);

    Ok(Grouping32StaticModel {
        version: STATIC_MODEL_VERSION,
        benchmark_source: benchmark_path.to_path_buf(),
        total_datasets: rows.len(),
        candidate_shapes,
        feature_mean: standardizer.mean,
        feature_scale: standardizer.scale,
        feature_group_weights,
        label_knn_neighbors,
        label_distance_power,
        winner_exemplars,
        blend_alpha,
        knn_neighbors,
        classifier,
        variant_models,
        known_dataset_best,
    })
}

pub fn save_static_grouping32_model(
    model: &Grouping32StaticModel,
    path: &Path,
) -> Result<(), Grouping32PredictorError> {
    let body = serde_json::to_string_pretty(model).map_err(|source| {
        Grouping32PredictorError::SerializeJsonModel {
            path: path.to_path_buf(),
            source,
        }
    })?;
    fs::write(path, body).map_err(|source| Grouping32PredictorError::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

pub fn load_static_grouping32_model(
    path: &Path,
) -> Result<Grouping32StaticModel, Grouping32PredictorError> {
    let body = fs::read_to_string(path).map_err(|source| Grouping32PredictorError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&body).map_err(|source| Grouping32PredictorError::ParseJsonModel {
        path: path.to_path_buf(),
        source,
    })
}

pub fn evaluate_grouping32_model(
    benchmark_path: &Path,
) -> Result<Grouping32EvaluationSummary, Grouping32PredictorError> {
    let rows = load_benchmark_rows(benchmark_path)?;
    let mut correct = 0usize;
    let mut regret_sum = 0.0f64;
    let mut cases = 0usize;

    for held_out in 0..rows.len() {
        let train_rows: Vec<_> = rows
            .iter()
            .enumerate()
            .filter_map(|(idx, row)| (idx != held_out).then_some(row.clone()))
            .collect();
        let model = train_static_grouping32_model_from_rows(benchmark_path, &train_rows);
        let held = &rows[held_out];
        let prediction = predict_grouping32_from_model_for_metadata(
            &model,
            &held.metadata,
            Some(&held.dataset_key),
        );
        let Some((truth_shape, truth_score)) =
            best_variant_for_scores(&model.candidate_shapes, &held.scores_by_variant)
        else {
            continue;
        };
        let predicted_score = held
            .scores_by_variant
            .get(&prediction.recommended_variant)
            .copied()
            .unwrap_or(0.0);
        if prediction.recommended_variant == truth_shape.variant {
            correct += 1;
        }
        regret_sum += (truth_score - predicted_score).max(0.0);
        cases += 1;
    }

    Ok(Grouping32EvaluationSummary {
        dataset_holdout_cases: cases,
        variant_accuracy: ratio(correct, cases),
        average_regret: if cases == 0 {
            0.0
        } else {
            regret_sum / cases as f64
        },
    })
}

pub fn predict_grouping32_from_static_model_for_dataset(
    model_path: &Path,
    dataset_path: &Path,
) -> Result<Grouping32Prediction, Grouping32PredictorError> {
    let model = load_static_grouping32_model(model_path)?;
    let metadata = extract_graph_metadata_from_dataset_path(dataset_path).ok_or_else(|| {
        Grouping32PredictorError::InvalidDatasetMetadata(dataset_path.display().to_string())
    })?;
    Ok(predict_grouping32_from_model_for_metadata(
        &model, &metadata, None,
    ))
}

fn train_static_grouping32_model_from_rows(
    benchmark_path: &Path,
    rows: &[BenchmarkRow],
) -> Grouping32StaticModel {
    let candidate_shapes = candidate_shapes();
    let feature_rows: Vec<Vec<f64>> = rows
        .iter()
        .map(|row| metadata_feature_vector_32(&row.metadata))
        .collect();
    let standardizer = fit_standardizer(&feature_rows);
    let scaled_rows: Vec<Vec<f64>> = feature_rows
        .iter()
        .map(|row| standardizer.transform(row))
        .collect();
    let variant_models: Vec<_> = candidate_shapes
        .iter()
        .map(|shape| train_variant_model(shape, rows, &scaled_rows))
        .collect();
    let (blend_alpha, knn_neighbors) =
        tune_blend_parameters(&candidate_shapes, &variant_models, rows, &scaled_rows);
    let winner_exemplars = winner_exemplars_from_rows(rows, &scaled_rows);
    let (feature_group_weights, label_knn_neighbors, label_distance_power) =
        tune_label_knn_parameters(&winner_exemplars);
    let known_dataset_best = rows
        .iter()
        .filter_map(|row| {
            best_variant_for_scores(&candidate_shapes, &row.scores_by_variant).map(
                |(shape, throughput)| Grouping32KnownBest {
                    dataset: row.metadata.dataset.clone(),
                    source: row.path.clone(),
                    recommended_variant: shape.variant.clone(),
                    recommended_big_groups: shape.big_groups.clone(),
                    recommended_little_groups: shape.little_groups.clone(),
                    throughput,
                },
            )
        })
        .collect();

    let classifier = train_classifier_tree(rows, &scaled_rows, 0);

    Grouping32StaticModel {
        version: STATIC_MODEL_VERSION,
        benchmark_source: benchmark_path.to_path_buf(),
        total_datasets: rows.len(),
        candidate_shapes,
        feature_mean: standardizer.mean,
        feature_scale: standardizer.scale,
        feature_group_weights,
        label_knn_neighbors,
        label_distance_power,
        winner_exemplars,
        blend_alpha,
        knn_neighbors,
        classifier,
        variant_models,
        known_dataset_best,
    }
}

fn load_benchmark_rows(
    benchmark_path: &Path,
) -> Result<Vec<BenchmarkRow>, Grouping32PredictorError> {
    let body = fs::read_to_string(benchmark_path).map_err(|source| {
        Grouping32PredictorError::ReadFile {
            path: benchmark_path.to_path_buf(),
            source,
        }
    })?;
    let mut lines = body.lines();
    let header = lines
        .next()
        .ok_or_else(|| Grouping32PredictorError::NoBenchmarkRows(benchmark_path.to_path_buf()))?;
    let expected = [
        "dataset",
        "graphy3b11l",
        "graphy3b11l_256hz",
        "b2l444",
        "b2l66",
        "b22l333",
        "b3l433",
        "b3l55",
        "b4l333",
    ];
    let actual: Vec<_> = header.split('\t').collect();
    if actual != expected {
        return Err(Grouping32PredictorError::InvalidBenchmarkHeader(
            benchmark_path.to_path_buf(),
        ));
    }

    let mut rows = Vec::new();
    for (line_idx, line) in lines.enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let fields: Vec<_> = trimmed.split('\t').collect();
        if fields.len() != expected.len() {
            return Err(Grouping32PredictorError::InvalidBenchmarkRow {
                path: benchmark_path.to_path_buf(),
                message: format!("line {} has {} fields", line_idx + 2, fields.len()),
            });
        }
        let raw = RawBenchmarkRow {
            dataset: fields[0].to_string(),
            graphy3b11l: parse_score(fields[1]),
            graphy3b11l_256hz: parse_score(fields[2]),
            b2l444: parse_score(fields[3]),
            b2l66: parse_score(fields[4]),
            b22l333: parse_score(fields[5]),
            b3l433: parse_score(fields[6]),
            b3l55: parse_score(fields[7]),
            b4l333: parse_score(fields[8]),
        };
        let path = resolve_benchmark_dataset_path(&raw.dataset)
            .ok_or_else(|| Grouping32PredictorError::MissingDatasetPath(raw.dataset.clone()))?;
        let metadata = extract_graph_metadata_from_dataset_path(&path)
            .ok_or_else(|| Grouping32PredictorError::InvalidDatasetMetadata(raw.dataset.clone()))?;
        let scores_by_variant = raw.collapsed_scores();
        rows.push(BenchmarkRow {
            dataset_key: raw.dataset,
            metadata,
            path,
            scores_by_variant,
        });
    }

    if rows.is_empty() {
        return Err(Grouping32PredictorError::NoBenchmarkRows(
            benchmark_path.to_path_buf(),
        ));
    }

    Ok(rows)
}

fn winner_exemplars_from_rows(
    rows: &[BenchmarkRow],
    scaled_rows: &[Vec<f64>],
) -> Vec<Grouping32WinnerExemplar> {
    let shapes = candidate_shapes();
    rows.iter()
        .zip(scaled_rows.iter())
        .filter_map(|(row, scaled)| {
            best_variant_for_scores(&shapes, &row.scores_by_variant).map(|(shape, _)| {
                Grouping32WinnerExemplar {
                    dataset: row.metadata.dataset.clone(),
                    features: scaled.clone(),
                    best_variant: shape.variant.clone(),
                    scores_by_variant: row.scores_by_variant.clone(),
                }
            })
        })
        .collect()
}

impl RawBenchmarkRow {
    fn collapsed_scores(&self) -> BTreeMap<String, f64> {
        let mut scores = BTreeMap::new();
        if let Some(value) = max_optional(self.graphy3b11l, self.graphy3b11l_256hz) {
            scores.insert("c3l11".to_string(), value);
        }
        maybe_insert(&mut scores, "b2l444", self.b2l444);
        maybe_insert(&mut scores, "b2l66", self.b2l66);
        maybe_insert(&mut scores, "b22l333", self.b22l333);
        maybe_insert(&mut scores, "b3l433", self.b3l433);
        maybe_insert(&mut scores, "b3l55", self.b3l55);
        maybe_insert(&mut scores, "b4l333", self.b4l333);
        scores
    }
}

fn maybe_insert(map: &mut BTreeMap<String, f64>, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        map.insert(key.to_string(), value);
    }
}

fn max_optional(lhs: Option<f64>, rhs: Option<f64>) -> Option<f64> {
    match (lhs, rhs) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn parse_score(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("error")
        || trimmed.eq_ignore_ascii_case("bus error")
    {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

fn candidate_shapes() -> Vec<Grouping32Shape> {
    vec![
        Grouping32Shape {
            variant: "c3l11".to_string(),
            big_groups: vec![3],
            little_groups: vec![11],
        },
        Grouping32Shape {
            variant: "b2l444".to_string(),
            big_groups: vec![2],
            little_groups: vec![4, 4, 4],
        },
        Grouping32Shape {
            variant: "b2l66".to_string(),
            big_groups: vec![2],
            little_groups: vec![6, 6],
        },
        Grouping32Shape {
            variant: "b22l333".to_string(),
            big_groups: vec![2, 2],
            little_groups: vec![3, 3, 3],
        },
        Grouping32Shape {
            variant: "b3l433".to_string(),
            big_groups: vec![3],
            little_groups: vec![4, 3, 3],
        },
        Grouping32Shape {
            variant: "b3l55".to_string(),
            big_groups: vec![3],
            little_groups: vec![5, 5],
        },
        Grouping32Shape {
            variant: "b4l333".to_string(),
            big_groups: vec![4],
            little_groups: vec![3, 3, 3],
        },
    ]
}

fn resolve_benchmark_dataset_path(dataset: &str) -> Option<PathBuf> {
    let file_name = match dataset {
        "amazon-2008" => "amazon-2008.mtx",
        "ca-hollywood-2009" => "ca-hollywood-2009.mtx",
        "dbpedia-link" => "dbpedia-link.mtx",
        "graph500-scale23-ef16_adj" => "graph500-scale23-ef16_adj.mtx",
        "rmat-19-32" => "rmat-19-32.txt",
        "rmat-21-32" => "rmat-21-32.txt",
        "rmat-24-16" => "rmat-24-16.txt",
        "soc-flickr-und" => "soc-flickr-und.mtx",
        "soc-LiveJournal1" => "soc-LiveJournal1.txt",
        "soc-orkut-dir" => "soc-orkut-dir.mtx",
        "web-baidu-baike" => "web-baidu-baike.mtx",
        "web-Google" => "web-Google.mtx",
        "web-hudong" => "web-hudong.mtx",
        "wiki-topcats-categories" => "wiki-topcats-categories.txt",
        "wiki-topcats" => "wiki-topcats.txt",
        _ => return None,
    };

    for root in DATASET_ROOT_CANDIDATES {
        let candidate = Path::new(root).join(file_name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn metadata_feature_vector_32(metadata: &GraphMetadata) -> Vec<f64> {
    let mean_degree = metadata.average_degree.max(1e-9);
    let mut out = vec![
        metadata.log_vertices,
        metadata.log_edges,
        mean_degree.max(1.0).ln(),
        (metadata.density + 1e-18).ln(),
        metadata.scale_hint.unwrap_or(0.0),
        metadata.edge_factor_hint.unwrap_or(0.0),
        metadata.active_src_fraction,
        metadata.active_dst_fraction,
        ((metadata.max_out_degree as f64 / mean_degree) + 1.0).ln(),
        ((metadata.max_in_degree as f64 / mean_degree) + 1.0).ln(),
        metadata.out_degree_cv,
        metadata.in_degree_cv,
        ((metadata.p90_in_degree as f64 / mean_degree) + 1.0).ln(),
        ((metadata.p99_in_degree as f64 / mean_degree) + 1.0).ln(),
        metadata.top_1pct_in_share,
        metadata.top_0_1pct_in_share,
        metadata.in_degree_gini,
    ];

    for domain in [
        GraphDomain::Rmat,
        GraphDomain::Social,
        GraphDomain::Web,
        GraphDomain::Wiki,
        GraphDomain::Commerce,
        GraphDomain::Knowledge,
        GraphDomain::Entertainment,
        GraphDomain::Other,
    ] {
        out.push(if metadata.domain == domain { 1.0 } else { 0.0 });
    }

    for format in [
        GraphFormat::MatrixMarket,
        GraphFormat::EdgeListTxt,
        GraphFormat::Unknown,
    ] {
        out.push(if metadata.format == format { 1.0 } else { 0.0 });
    }

    out
}

fn feature_group_indices() -> &'static [usize] {
    &[
        0, 0, 0, 0, // size / density
        1, 1, // generator hints
        2, 2, // active src/dst
        3, 3, // max degree ratios
        4, 4, // degree CV
        5, 5, 5, 5, 5, // degree concentration
        6, 6, 6, 6, 6, 6, 6, 6, // domain one-hot
        7, 7, 7, // format one-hot
    ]
}

fn tune_label_knn_parameters(exemplars: &[Grouping32WinnerExemplar]) -> (Vec<f64>, usize, f64) {
    if exemplars.is_empty() {
        return (vec![1.0; 8], 1, 1.0);
    }

    let candidate_weights = [0.0, 0.25, 0.5, 1.0, 2.0, 4.0];
    let distance_powers = [0.5, 1.0, 2.0];

    let mut best_weights = vec![1.0; 8];
    let mut best_neighbors = 1usize;
    let mut best_power = 1.0f64;
    let mut best_accuracy = -1.0f64;
    let mut best_regret = f64::INFINITY;

    for &neighbors in &[1usize, 2, 3, 4, 5] {
        for &power in &distance_powers {
            let (accuracy, regret) = evaluate_label_knn(exemplars, &best_weights, neighbors, power);
            if accuracy > best_accuracy + 1e-12
                || ((accuracy - best_accuracy).abs() <= 1e-12 && regret < best_regret)
            {
                best_accuracy = accuracy;
                best_regret = regret;
                best_neighbors = neighbors;
                best_power = power;
            }
        }
    }

    let mut improved = true;
    while improved {
        improved = false;

        for group_idx in 0..best_weights.len() {
            let mut local_best = best_weights[group_idx];
            for &candidate in &candidate_weights {
                let mut weights = best_weights.clone();
                weights[group_idx] = candidate;
                for &neighbors in &[1usize, 2, 3, 4, 5] {
                    for &power in &distance_powers {
                        let (accuracy, regret) =
                            evaluate_label_knn(exemplars, &weights, neighbors, power);
                        if accuracy > best_accuracy + 1e-12
                            || ((accuracy - best_accuracy).abs() <= 1e-12
                                && regret + 1e-9 < best_regret)
                        {
                            best_accuracy = accuracy;
                            best_regret = regret;
                            best_weights = weights.clone();
                            best_neighbors = neighbors;
                            best_power = power;
                            local_best = candidate;
                            improved = true;
                        }
                    }
                }
            }
            best_weights[group_idx] = local_best;
        }
    }

    (
        expand_group_weights(&best_weights),
        best_neighbors,
        best_power,
    )
}

fn expand_group_weights(group_weights: &[f64]) -> Vec<f64> {
    feature_group_indices()
        .iter()
        .map(|group| group_weights.get(*group).copied().unwrap_or(1.0))
        .collect()
}

fn evaluate_label_knn(
    exemplars: &[Grouping32WinnerExemplar],
    feature_weights: &[f64],
    neighbors: usize,
    distance_power: f64,
) -> (f64, f64) {
    let shapes = candidate_shapes();
    let mut correct = 0usize;
    let mut regret = 0.0f64;

    for held in exemplars {
        let ranked = rank_candidates_from_winner_exemplars(
            exemplars,
            &held.features,
            feature_weights,
            neighbors,
            distance_power,
            Some(&held.dataset),
            &shapes,
        );
        let predicted_variant = ranked
            .first()
            .map(|item| item.variant.as_str())
            .unwrap_or("unknown");
        if predicted_variant == held.best_variant {
            correct += 1;
        }
        let truth_score = held
            .scores_by_variant
            .get(&held.best_variant)
            .copied()
            .unwrap_or(0.0);
        let predicted_score = held
            .scores_by_variant
            .get(predicted_variant)
            .copied()
            .unwrap_or(0.0);
        regret += (truth_score - predicted_score).max(0.0);
    }

    (
        ratio(correct, exemplars.len()),
        if exemplars.is_empty() {
            0.0
        } else {
            regret / exemplars.len() as f64
        },
    )
}

fn best_variant_for_scores<'a>(
    shapes: &'a [Grouping32Shape],
    scores_by_variant: &BTreeMap<String, f64>,
) -> Option<(&'a Grouping32Shape, f64)> {
    shapes
        .iter()
        .filter_map(|shape| {
            scores_by_variant
                .get(&shape.variant)
                .copied()
                .map(|score| (shape, score))
        })
        .max_by(|lhs, rhs| lhs.1.partial_cmp(&rhs.1).unwrap_or(Ordering::Equal))
}

fn train_variant_model(
    shape: &Grouping32Shape,
    rows: &[BenchmarkRow],
    scaled_rows: &[Vec<f64>],
) -> Grouping32VariantModel {
    let mut feature_rows = Vec::new();
    let mut targets = Vec::new();
    let mut exemplars = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        if let Some(score) = row.scores_by_variant.get(&shape.variant).copied() {
            let log_throughput = score.max(1e-9).ln();
            feature_rows.push(with_bias(&scaled_rows[idx]));
            targets.push(log_throughput);
            exemplars.push(Grouping32Exemplar {
                dataset: row.dataset_key.clone(),
                features: scaled_rows[idx].clone(),
                log_throughput,
            });
        }
    }

    let regression_weights = fit_ridge_weights(&feature_rows, &targets, 0.1);
    Grouping32VariantModel {
        variant: shape.variant.clone(),
        big_groups: shape.big_groups.clone(),
        little_groups: shape.little_groups.clone(),
        regression_weights,
        training_examples: exemplars.len(),
        exemplar_points: exemplars,
    }
}

fn tune_blend_parameters(
    shapes: &[Grouping32Shape],
    variant_models: &[Grouping32VariantModel],
    rows: &[BenchmarkRow],
    scaled_rows: &[Vec<f64>],
) -> (f64, usize) {
    let mut best_alpha = 0.0;
    let mut best_k = 1usize;
    let mut best_acc = -1.0f64;
    let mut best_regret = f64::INFINITY;

    for k in 1..=5 {
        for step in 0..=20 {
            let alpha = step as f64 / 20.0;
            let mut correct = 0usize;
            let mut cases = 0usize;
            let mut regret = 0.0f64;

            for (row, scaled) in rows.iter().zip(scaled_rows.iter()) {
                let mut predicted_scores = Vec::new();
                for shape in shapes {
                    let model = variant_models
                        .iter()
                        .find(|item| item.variant == shape.variant)
                        .expect("shape model must exist");
                    let score =
                        combined_log_score(model, scaled, Some(&row.dataset_key), alpha, k).exp();
                    predicted_scores.push((&shape.variant, score));
                }
                let Some((truth_shape, truth_score)) =
                    best_variant_for_scores(shapes, &row.scores_by_variant)
                else {
                    continue;
                };
                let predicted_variant = predicted_scores
                    .iter()
                    .max_by(|lhs, rhs| lhs.1.partial_cmp(&rhs.1).unwrap_or(Ordering::Equal))
                    .map(|item| item.0.to_string())
                    .unwrap_or_default();
                let predicted_score = row
                    .scores_by_variant
                    .get(&predicted_variant)
                    .copied()
                    .unwrap_or(0.0);
                if predicted_variant == truth_shape.variant {
                    correct += 1;
                }
                regret += (truth_score - predicted_score).max(0.0);
                cases += 1;
            }

            let accuracy = ratio(correct, cases);
            let avg_regret = if cases == 0 {
                f64::INFINITY
            } else {
                regret / cases as f64
            };
            if accuracy > best_acc + 1e-12
                || ((accuracy - best_acc).abs() <= 1e-12 && avg_regret < best_regret)
            {
                best_acc = accuracy;
                best_regret = avg_regret;
                best_alpha = alpha;
                best_k = k;
            }
        }
    }

    (best_alpha, best_k)
}

fn predict_grouping32_from_model_for_metadata(
    model: &Grouping32StaticModel,
    metadata: &GraphMetadata,
    exact_dataset_key: Option<&str>,
) -> Grouping32Prediction {
    if let Some(dataset_key) = exact_dataset_key.or(Some(metadata.dataset.as_str()))
        && let Some(known) = model
            .known_dataset_best
            .iter()
            .find(|item| item.dataset == dataset_key)
    {
        return Grouping32Prediction {
            model_path: model.benchmark_source.clone(),
            metadata: metadata.clone(),
            total_datasets: model.total_datasets,
            candidate_variants: model.candidate_shapes.len(),
            recommended_variant: known.recommended_variant.clone(),
            recommended_big_groups: known.recommended_big_groups.clone(),
            recommended_little_groups: known.recommended_little_groups.clone(),
            ranked_candidates: rank_candidates(model, metadata, Some(dataset_key)),
        };
    }

    let ranked_candidates = rank_candidates(model, metadata, exact_dataset_key);
    let best = ranked_candidates
        .first()
        .cloned()
        .unwrap_or_else(|| Grouping32CandidateScore {
            variant: "unknown".to_string(),
            big_groups: vec![],
            little_groups: vec![],
            score: 0.0,
            classifier_probability: 0.0,
            regression_score: 0.0,
            knn_score: None,
            seen_in_training: false,
        });

    Grouping32Prediction {
        model_path: model.benchmark_source.clone(),
        metadata: metadata.clone(),
        total_datasets: model.total_datasets,
        candidate_variants: model.candidate_shapes.len(),
        recommended_variant: best.variant.clone(),
        recommended_big_groups: best.big_groups.clone(),
        recommended_little_groups: best.little_groups.clone(),
        ranked_candidates,
    }
}

fn rank_candidates(
    model: &Grouping32StaticModel,
    metadata: &GraphMetadata,
    exclude_dataset: Option<&str>,
) -> Vec<Grouping32CandidateScore> {
    let scaled = standardize_row(
        &metadata_feature_vector_32(metadata),
        &model.feature_mean,
        &model.feature_scale,
    );
    rank_candidates_from_winner_exemplars(
        &model.winner_exemplars,
        &scaled,
        &model.feature_group_weights,
        model.label_knn_neighbors,
        model.label_distance_power,
        exclude_dataset,
        &model.candidate_shapes,
    )
}

fn rank_candidates_from_winner_exemplars(
    exemplars: &[Grouping32WinnerExemplar],
    scaled_features: &[f64],
    feature_weights: &[f64],
    neighbors: usize,
    distance_power: f64,
    exclude_dataset: Option<&str>,
    candidate_shapes: &[Grouping32Shape],
) -> Vec<Grouping32CandidateScore> {
    let mut pairs: Vec<_> = exemplars
        .iter()
        .filter(|item| {
            exclude_dataset
                .map(|dataset| item.dataset != dataset)
                .unwrap_or(true)
        })
        .map(|item| {
            (
                weighted_distance(&item.features, scaled_features, feature_weights),
                item,
            )
        })
        .collect();
    pairs.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));

    let selected: Vec<_> = pairs.into_iter().take(neighbors.max(1)).collect();
    let mut vote_totals: BTreeMap<String, f64> = BTreeMap::new();
    let mut throughput_totals: BTreeMap<String, f64> = BTreeMap::new();
    let mut throughput_weights: BTreeMap<String, f64> = BTreeMap::new();
    let mut total_vote = 0.0f64;

    for (dist, exemplar) in &selected {
        let weight = 1.0 / (dist.sqrt() + 1e-6).powf(distance_power.max(1e-6));
        total_vote += weight;
        *vote_totals
            .entry(exemplar.best_variant.clone())
            .or_insert(0.0) += weight;
        for (variant, score) in &exemplar.scores_by_variant {
            *throughput_totals.entry(variant.clone()).or_insert(0.0) += weight * score;
            *throughput_weights.entry(variant.clone()).or_insert(0.0) += weight;
        }
    }

    let mut ranked = Vec::new();
    for shape in candidate_shapes {
        let vote = vote_totals.get(&shape.variant).copied().unwrap_or(0.0);
        let throughput = match (
            throughput_totals.get(&shape.variant),
            throughput_weights.get(&shape.variant),
        ) {
            (Some(total), Some(weight)) if *weight > 0.0 => total / weight,
            _ => 0.0,
        };
        ranked.push(Grouping32CandidateScore {
            variant: shape.variant.clone(),
            big_groups: shape.big_groups.clone(),
            little_groups: shape.little_groups.clone(),
            score: throughput,
            classifier_probability: if total_vote > 0.0 {
                vote / total_vote
            } else {
                0.0
            },
            regression_score: throughput,
            knn_score: Some(throughput),
            seen_in_training: exemplars
                .iter()
                .any(|item| item.best_variant == shape.variant),
        });
    }

    ranked.sort_by(|lhs, rhs| {
        rhs.classifier_probability
            .partial_cmp(&lhs.classifier_probability)
            .unwrap_or(Ordering::Equal)
            .then_with(|| rhs.score.partial_cmp(&lhs.score).unwrap_or(Ordering::Equal))
    });
    ranked
}

fn combined_log_score(
    model: &Grouping32VariantModel,
    scaled_features: &[f64],
    exclude_dataset: Option<&str>,
    blend_alpha: f64,
    neighbors: usize,
) -> f64 {
    let regression_score = dot(&model.regression_weights, &with_bias(scaled_features));
    if let Some(knn_score) = knn_log_score(model, scaled_features, exclude_dataset, neighbors) {
        blend_alpha * regression_score + (1.0 - blend_alpha) * knn_score
    } else {
        regression_score
    }
}

fn knn_log_score(
    model: &Grouping32VariantModel,
    scaled_features: &[f64],
    exclude_dataset: Option<&str>,
    neighbors: usize,
) -> Option<f64> {
    let mut pairs: Vec<_> = model
        .exemplar_points
        .iter()
        .filter(|point| {
            exclude_dataset
                .map(|dataset| point.dataset != dataset)
                .unwrap_or(true)
        })
        .map(|point| {
            (
                squared_distance(&point.features, scaled_features),
                point.log_throughput,
            )
        })
        .collect();
    if pairs.is_empty() {
        return None;
    }
    pairs.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));
    let mut num = 0.0;
    let mut den = 0.0;
    for (dist, value) in pairs.into_iter().take(neighbors.max(1)) {
        let weight = 1.0 / (dist.sqrt() + 1e-6);
        num += weight * value;
        den += weight;
    }
    (den > 0.0).then_some(num / den)
}

fn fit_standardizer(rows: &[Vec<f64>]) -> Standardizer {
    if rows.is_empty() {
        return Standardizer {
            mean: vec![],
            scale: vec![],
        };
    }
    let feature_len = rows[0].len();
    let mut mean = vec![0.0; feature_len];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            mean[idx] += *value;
        }
    }
    for value in &mut mean {
        *value /= rows.len() as f64;
    }
    let mut scale = vec![0.0; feature_len];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            let delta = *value - mean[idx];
            scale[idx] += delta * delta;
        }
    }
    for value in &mut scale {
        *value = (*value / rows.len() as f64).sqrt();
        if *value < 1e-9 {
            *value = 1.0;
        }
    }
    Standardizer { mean, scale }
}

#[derive(Debug, Clone)]
struct Standardizer {
    mean: Vec<f64>,
    scale: Vec<f64>,
}

impl Standardizer {
    fn transform(&self, row: &[f64]) -> Vec<f64> {
        standardize_row(row, &self.mean, &self.scale)
    }
}

fn standardize_row(row: &[f64], mean: &[f64], scale: &[f64]) -> Vec<f64> {
    row.iter()
        .enumerate()
        .map(|(idx, value)| (*value - mean[idx]) / scale[idx])
        .collect()
}

fn with_bias(row: &[f64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(row.len() + 1);
    out.push(1.0);
    out.extend(row.iter().copied());
    out
}

fn fit_ridge_weights(rows: &[Vec<f64>], targets: &[f64], lambda: f64) -> Vec<f64> {
    if rows.is_empty() || targets.is_empty() {
        return vec![];
    }
    let feature_len = rows[0].len();
    let mut xtx = vec![vec![0.0; feature_len]; feature_len];
    let mut xty = vec![0.0; feature_len];
    for (row, target) in rows.iter().zip(targets.iter()) {
        for i in 0..feature_len {
            xty[i] += row[i] * target;
            for j in 0..feature_len {
                xtx[i][j] += row[i] * row[j];
            }
        }
    }
    for (idx, row) in xtx.iter_mut().enumerate() {
        row[idx] += lambda;
    }
    solve_linear_system(&xtx, &xty).unwrap_or_else(|| vec![0.0; feature_len])
}

fn solve_linear_system(matrix: &[Vec<f64>], rhs: &[f64]) -> Option<Vec<f64>> {
    let n = matrix.len();
    if n == 0 || rhs.len() != n {
        return None;
    }
    let mut a = vec![vec![0.0; n + 1]; n];
    for i in 0..n {
        for j in 0..n {
            a[i][j] = matrix[i][j];
        }
        a[i][n] = rhs[i];
    }

    for col in 0..n {
        let mut pivot = col;
        for row in col + 1..n {
            if a[row][col].abs() > a[pivot][col].abs() {
                pivot = row;
            }
        }
        if a[pivot][col].abs() < 1e-12 {
            return None;
        }
        a.swap(col, pivot);
        let pivot_value = a[col][col];
        for j in col..=n {
            a[col][j] /= pivot_value;
        }
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = a[row][col];
            for j in col..=n {
                a[row][j] -= factor * a[col][j];
            }
        }
    }
    Some(a.into_iter().map(|row| row[n]).collect())
}

fn dot(lhs: &[f64], rhs: &[f64]) -> f64 {
    lhs.iter().zip(rhs.iter()).map(|(a, b)| a * b).sum()
}

fn squared_distance(lhs: &[f64], rhs: &[f64]) -> f64 {
    lhs.iter()
        .zip(rhs.iter())
        .map(|(a, b)| {
            let delta = a - b;
            delta * delta
        })
        .sum()
}

fn weighted_distance(lhs: &[f64], rhs: &[f64], feature_weights: &[f64]) -> f64 {
    lhs.iter()
        .zip(rhs.iter())
        .enumerate()
        .map(|(idx, (a, b))| {
            let delta = a - b;
            let weight = feature_weights.get(idx).copied().unwrap_or(1.0);
            weight * delta * delta
        })
        .sum()
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn train_classifier_tree(
    rows: &[BenchmarkRow],
    scaled_rows: &[Vec<f64>],
    depth: usize,
) -> Grouping32DecisionTree {
    let labels: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            best_variant_for_scores(&candidate_shapes(), &row.scores_by_variant)
                .map(|(shape, _)| shape.variant.clone())
        })
        .collect();
    if labels.is_empty() {
        return Grouping32DecisionTree::Leaf {
            counts: BTreeMap::new(),
        };
    }

    let counts = label_counts(rows);
    let unique_labels = counts.len();
    if depth >= 3 || rows.len() <= 2 || unique_labels <= 1 {
        return Grouping32DecisionTree::Leaf { counts };
    }

    let feature_len = scaled_rows.first().map(|row| row.len()).unwrap_or(0);
    let parent_impurity = gini_from_counts(&counts);
    let mut best_split: Option<(usize, f64, f64, Vec<usize>, Vec<usize>)> = None;

    for feature_idx in 0..feature_len {
        let mut pairs: Vec<(f64, usize)> = scaled_rows
            .iter()
            .enumerate()
            .map(|(idx, row)| (row[feature_idx], idx))
            .collect();
        pairs.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));
        for window in pairs.windows(2) {
            let threshold = (window[0].0 + window[1].0) * 0.5;
            if (window[0].0 - window[1].0).abs() <= 1e-12 {
                continue;
            }
            let mut left = Vec::new();
            let mut right = Vec::new();
            for (_, idx) in &pairs {
                if scaled_rows[*idx][feature_idx] <= threshold {
                    left.push(*idx);
                } else {
                    right.push(*idx);
                }
            }
            if left.is_empty() || right.is_empty() {
                continue;
            }
            let left_counts = label_counts_for_indices(rows, &left);
            let right_counts = label_counts_for_indices(rows, &right);
            let left_impurity = gini_from_counts(&left_counts);
            let right_impurity = gini_from_counts(&right_counts);
            let weighted = (left.len() as f64 / rows.len() as f64) * left_impurity
                + (right.len() as f64 / rows.len() as f64) * right_impurity;
            let gain = parent_impurity - weighted;
            if gain <= 1e-9 {
                continue;
            }
            let replace = best_split
                .as_ref()
                .map(|(_, _, best_gain, _, _)| gain > *best_gain)
                .unwrap_or(true);
            if replace {
                best_split = Some((feature_idx, threshold, gain, left, right));
            }
        }
    }

    let Some((feature_idx, threshold, _, left_idx, right_idx)) = best_split else {
        return Grouping32DecisionTree::Leaf { counts };
    };

    let left_rows: Vec<_> = left_idx.iter().map(|idx| rows[*idx].clone()).collect();
    let left_scaled: Vec<_> = left_idx
        .iter()
        .map(|idx| scaled_rows[*idx].clone())
        .collect();
    let right_rows: Vec<_> = right_idx.iter().map(|idx| rows[*idx].clone()).collect();
    let right_scaled: Vec<_> = right_idx
        .iter()
        .map(|idx| scaled_rows[*idx].clone())
        .collect();

    Grouping32DecisionTree::Split {
        feature_idx,
        threshold,
        left: Box::new(train_classifier_tree(&left_rows, &left_scaled, depth + 1)),
        right: Box::new(train_classifier_tree(&right_rows, &right_scaled, depth + 1)),
    }
}

fn label_counts(rows: &[BenchmarkRow]) -> BTreeMap<String, usize> {
    let shapes = candidate_shapes();
    let mut counts = BTreeMap::new();
    for row in rows {
        if let Some((shape, _)) = best_variant_for_scores(&shapes, &row.scores_by_variant) {
            *counts.entry(shape.variant.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn label_counts_for_indices(rows: &[BenchmarkRow], indices: &[usize]) -> BTreeMap<String, usize> {
    let shapes = candidate_shapes();
    let mut counts = BTreeMap::new();
    for idx in indices {
        if let Some((shape, _)) = best_variant_for_scores(&shapes, &rows[*idx].scores_by_variant) {
            *counts.entry(shape.variant.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn gini_from_counts(counts: &BTreeMap<String, usize>) -> f64 {
    let total: usize = counts.values().sum();
    if total == 0 {
        return 0.0;
    }
    let mut impurity = 1.0;
    for count in counts.values() {
        let p = *count as f64 / total as f64;
        impurity -= p * p;
    }
    impurity
}
