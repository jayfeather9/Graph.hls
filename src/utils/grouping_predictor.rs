use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::utils::graph_metadata::{
    GraphMetadata, GraphPartitionProfile, RawGraph, RawGraphEdge,
    extract_graph_metadata_from_profile_log, extract_partition_profile_from_dataset_path,
    extract_partition_profile_from_profile_log, load_raw_graph_from_metadata,
    load_raw_graph_from_profile_log,
};

const VARIANT_METADATA_CANDIDATES: &[&str] = &[
    "variant_metadata.csv",
    "variant_metadata_new10_mixed.csv",
    "variant_metadata_new12_candidates.csv",
];

const BIG_GROUP_DST_CAPACITY: usize = 524_288;
const LITTLE_GROUP_DST_CAPACITY: usize = 65_536;
const PRECISE_RERANK_CANDIDATES: usize = 2;
const MAX_PRECISE_SAMPLE_EDGES: usize = 200_000;
const STATIC_MODEL_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum GroupingPredictorError {
    #[error("no parseable profiling logs found under {0}")]
    NoLogsFound(PathBuf),
    #[error("probe log is missing required graph metadata lines: {0}")]
    InvalidMetadata(PathBuf),
    #[error("no variant grouping metadata files were found")]
    NoVariantMetadata,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GroupingShape {
    pub variant: String,
    pub family: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
}

impl GroupingShape {
    pub fn big_pipelines(&self) -> usize {
        self.big_groups.iter().sum()
    }

    pub fn little_pipelines(&self) -> usize {
        self.little_groups.iter().sum()
    }

    pub fn total_pipelines(&self) -> usize {
        self.big_pipelines() + self.little_pipelines()
    }

    pub fn big_group_count(&self) -> usize {
        self.big_groups.len()
    }

    pub fn little_group_count(&self) -> usize {
        self.little_groups.len()
    }

    pub fn feature_vector(&self) -> Vec<f64> {
        let big_total = self.big_pipelines().max(1) as f64;
        let little_total = self.little_pipelines().max(1) as f64;
        let total = (self.total_pipelines().max(1)) as f64;

        let big_count = self.big_group_count().max(1) as f64;
        let little_count = self.little_group_count().max(1) as f64;

        let big_mean = big_total / big_count;
        let little_mean = little_total / little_count;

        let big_cv = coeff_var_usize(&self.big_groups);
        let little_cv = coeff_var_usize(&self.little_groups);

        let big_max = *self.big_groups.iter().max().unwrap_or(&0) as f64;
        let big_min = *self.big_groups.iter().min().unwrap_or(&0) as f64;
        let little_max = *self.little_groups.iter().max().unwrap_or(&0) as f64;
        let little_min = *self.little_groups.iter().min().unwrap_or(&0) as f64;

        let big_max_share = big_max / big_total;
        let little_max_share = little_max / little_total;

        let big_entropy = normalized_entropy(&self.big_groups);
        let little_entropy = normalized_entropy(&self.little_groups);

        vec![
            big_total,
            little_total,
            total,
            big_total / total,
            little_total / total,
            big_count,
            little_count,
            (big_count + little_count),
            big_mean,
            little_mean,
            big_cv,
            little_cv,
            big_max,
            little_max,
            big_min,
            little_min,
            big_max_share,
            little_max_share,
            big_entropy,
            little_entropy,
            bool_score(self.big_group_count() == 1),
            bool_score(self.little_group_count() == 1),
            big_count / big_total,
            little_count / little_total,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileRun {
    pub path: PathBuf,
    pub variant: String,
    pub family: String,
    pub dataset: String,
    pub total_mteps: f64,
    pub shape: GroupingShape,
    pub avg_big_pipe_throughput: Option<f64>,
    pub avg_little_pipe_throughput: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateScore {
    pub variant: String,
    pub family: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
    pub score: f64,
    pub linear_score: f64,
    pub ranking_score: f64,
    pub variant_knn_score: Option<f64>,
    pub family_knn_score: Option<f64>,
    pub precise_partition_score: Option<f64>,
    pub variant_seen_in_training: bool,
    pub family_seen_in_training: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupingPrediction {
    pub history_root: PathBuf,
    pub metadata: GraphMetadata,
    pub total_runs: usize,
    pub total_datasets: usize,
    pub candidate_variants: usize,
    pub training_variants: usize,
    pub recommended_variant: String,
    pub recommended_family: String,
    pub recommended_big_groups: Vec<usize>,
    pub recommended_little_groups: Vec<usize>,
    pub ranked_candidates: Vec<CandidateScore>,
}

pub type MetadataGroupingPrediction = GroupingPrediction;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataEvaluationSummary {
    pub dataset_holdout_cases: usize,
    pub dataset_holdout_family_accuracy: f64,
    pub dataset_holdout_variant_accuracy: f64,
    pub unseen_grouping_cases: usize,
    pub unseen_grouping_family_accuracy: f64,
    pub unseen_grouping_variant_accuracy: f64,
    pub combined_holdout_cases: usize,
    pub combined_holdout_family_accuracy: f64,
    pub combined_holdout_variant_accuracy: f64,
}

#[derive(Debug, Clone)]
struct TrainingExample {
    run: ProfileRun,
    profile: Arc<GraphPartitionProfile>,
    relative_score: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct ThroughputAverages {
    big_per_pipe: f64,
    little_per_pipe: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatasetThroughputProfile {
    dataset: String,
    metadata: GraphMetadata,
    throughput: ThroughputAverages,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlendModel {
    feature_mean: Vec<f64>,
    feature_scale: Vec<f64>,
    weights: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegressionModel {
    feature_mean: Vec<f64>,
    feature_scale: Vec<f64>,
    weights: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RankingModel {
    feature_mean: Vec<f64>,
    feature_scale: Vec<f64>,
    weights: Vec<f64>,
}

#[derive(Debug, Clone)]
struct DatasetTruth {
    dataset: String,
    profile: Arc<GraphPartitionProfile>,
    winner_variant: String,
    winner_family: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Standardizer {
    mean: Vec<f64>,
    scale: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StaticTrainingExample {
    variant: String,
    family: String,
    dataset: String,
    relative_score: f64,
    metadata: GraphMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnownDatasetBestGrouping {
    pub dataset: String,
    pub source: PathBuf,
    pub variant: String,
    pub family: String,
    pub big_groups: Vec<usize>,
    pub little_groups: Vec<usize>,
    pub total_mteps: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavedDatasetGrouping {
    pub dataset: String,
    pub source: PathBuf,
    pub recommended_variant: String,
    pub recommended_family: String,
    pub recommended_big_groups: Vec<usize>,
    pub recommended_little_groups: Vec<usize>,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticGroupingModel {
    pub model_version: u32,
    pub history_root: PathBuf,
    pub neighbor_count: usize,
    pub total_runs: usize,
    pub total_datasets: usize,
    pub dominant_pipeline_total: usize,
    pub candidate_shapes: Vec<GroupingShape>,
    pub observed_variants: Vec<String>,
    pub observed_families: Vec<String>,
    examples: Vec<StaticTrainingExample>,
    dataset_throughput_profiles: Vec<DatasetThroughputProfile>,
    metadata_scale: Standardizer,
    regression_model: RegressionModel,
    ranking_model: RankingModel,
    blend_model: BlendModel,
    pub known_dataset_best: Vec<KnownDatasetBestGrouping>,
}

#[derive(Debug, Clone, PartialEq)]
struct PartitionSimulation {
    partition_number: usize,
    dense_est_ms: f64,
    sparse_est_ms: f64,
    dense_group_edge_times: Vec<f64>,
    sparse_group_edge_times: Vec<f64>,
    dense_group_fill: Vec<f64>,
    sparse_group_fill: Vec<f64>,
    dense_partition_fill: Vec<f64>,
    sparse_partition_fill: Vec<f64>,
    dense_output_words: Vec<f64>,
    sparse_output_words: Vec<f64>,
    dense_active_partitions: usize,
    sparse_active_partitions: usize,
}

pub fn collect_profile_runs(root: &Path) -> Result<Vec<ProfileRun>, GroupingPredictorError> {
    let shape_library = load_variant_shape_library()?;
    let mut logs = Vec::new();
    walk_logs(root, &mut logs)?;
    let runs: Vec<_> = logs
        .into_iter()
        .filter_map(|path| parse_profile_log(&path, &shape_library))
        .collect();
    if runs.is_empty() {
        return Err(GroupingPredictorError::NoLogsFound(root.to_path_buf()));
    }
    Ok(runs)
}

pub fn predict_best_grouping(
    history_root: &Path,
    probe_log: &Path,
    neighbor_count: usize,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    predict_best_grouping_from_metadata(history_root, probe_log, neighbor_count)
}

pub fn predict_best_grouping_from_metadata(
    history_root: &Path,
    probe_log: &Path,
    neighbor_count: usize,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    let runs = collect_profile_runs(history_root)?;
    let dataset_profiles = collect_dataset_profiles(&runs);
    let profile = extract_partition_profile_from_profile_log(probe_log)
        .ok_or_else(|| GroupingPredictorError::InvalidMetadata(probe_log.to_path_buf()))?;
    let candidates = collect_candidate_shapes(&runs);
    let mut prediction = predict_from_training_runs(
        &runs,
        Some(&dataset_profiles),
        &candidates,
        &profile,
        Some(profile.metadata.dataset.as_str()),
        neighbor_count,
        true,
    )?;
    prediction.history_root = history_root.to_path_buf();
    Ok(prediction)
}

pub fn evaluate_metadata_predictor(
    history_root: &Path,
    neighbor_count: usize,
) -> Result<MetadataEvaluationSummary, GroupingPredictorError> {
    let all_runs = collect_profile_runs(history_root)?;
    let dataset_profiles = collect_dataset_profiles(&all_runs);
    let candidates = collect_candidate_shapes(&all_runs);
    let truths = dataset_truths(&all_runs, &dataset_profiles);

    let mut dataset_holdout_cases = 0usize;
    let mut dataset_holdout_family_correct = 0usize;
    let mut dataset_holdout_variant_correct = 0usize;

    let mut unseen_grouping_cases = 0usize;
    let mut unseen_grouping_family_correct = 0usize;
    let mut unseen_grouping_variant_correct = 0usize;

    let mut combined_holdout_cases = 0usize;
    let mut combined_holdout_family_correct = 0usize;
    let mut combined_holdout_variant_correct = 0usize;

    for truth in &truths {
        let dataset_train: Vec<_> = all_runs
            .iter()
            .filter(|run| run.dataset != truth.dataset)
            .cloned()
            .collect();
        if !dataset_train.is_empty() {
            let prediction = predict_from_training_runs(
                &dataset_train,
                Some(&dataset_profiles),
                &candidates,
                &truth.profile,
                Some(truth.dataset.as_str()),
                neighbor_count,
                false,
            )?;
            dataset_holdout_cases += 1;
            if prediction.recommended_family == truth.winner_family {
                dataset_holdout_family_correct += 1;
            }
            if prediction.recommended_variant == truth.winner_variant {
                dataset_holdout_variant_correct += 1;
            }
        }

        let grouping_train: Vec<_> = all_runs
            .iter()
            .filter(|run| run.variant != truth.winner_variant)
            .cloned()
            .collect();
        if !grouping_train.is_empty() {
            let prediction = predict_from_training_runs(
                &grouping_train,
                Some(&dataset_profiles),
                &candidates,
                &truth.profile,
                None,
                neighbor_count,
                false,
            )?;
            unseen_grouping_cases += 1;
            if prediction.recommended_family == truth.winner_family {
                unseen_grouping_family_correct += 1;
            }
            if prediction.recommended_variant == truth.winner_variant {
                unseen_grouping_variant_correct += 1;
            }
        }

        let combined_train: Vec<_> = all_runs
            .iter()
            .filter(|run| run.dataset != truth.dataset && run.variant != truth.winner_variant)
            .cloned()
            .collect();
        if !combined_train.is_empty() {
            let prediction = predict_from_training_runs(
                &combined_train,
                Some(&dataset_profiles),
                &candidates,
                &truth.profile,
                Some(truth.dataset.as_str()),
                neighbor_count,
                false,
            )?;
            combined_holdout_cases += 1;
            if prediction.recommended_family == truth.winner_family {
                combined_holdout_family_correct += 1;
            }
            if prediction.recommended_variant == truth.winner_variant {
                combined_holdout_variant_correct += 1;
            }
        }
    }

    Ok(MetadataEvaluationSummary {
        dataset_holdout_cases,
        dataset_holdout_family_accuracy: ratio(
            dataset_holdout_family_correct,
            dataset_holdout_cases,
        ),
        dataset_holdout_variant_accuracy: ratio(
            dataset_holdout_variant_correct,
            dataset_holdout_cases,
        ),
        unseen_grouping_cases,
        unseen_grouping_family_accuracy: ratio(
            unseen_grouping_family_correct,
            unseen_grouping_cases,
        ),
        unseen_grouping_variant_accuracy: ratio(
            unseen_grouping_variant_correct,
            unseen_grouping_cases,
        ),
        combined_holdout_cases,
        combined_holdout_family_accuracy: ratio(
            combined_holdout_family_correct,
            combined_holdout_cases,
        ),
        combined_holdout_variant_accuracy: ratio(
            combined_holdout_variant_correct,
            combined_holdout_cases,
        ),
    })
}

pub fn train_static_grouping_model(
    history_root: &Path,
    neighbor_count: usize,
) -> Result<StaticGroupingModel, GroupingPredictorError> {
    let runs = collect_profile_runs(history_root)?;
    let dataset_profiles = collect_dataset_profiles(&runs);
    Ok(build_static_grouping_model(
        history_root,
        neighbor_count,
        &runs,
        &dataset_profiles,
    ))
}

pub fn save_static_grouping_model(
    model: &StaticGroupingModel,
    path: &Path,
) -> Result<(), GroupingPredictorError> {
    let body = serde_json::to_string_pretty(model).map_err(|source| {
        GroupingPredictorError::SerializeJsonModel {
            path: path.to_path_buf(),
            source,
        }
    })?;
    fs::write(path, body).map_err(|source| GroupingPredictorError::WriteFile {
        path: path.to_path_buf(),
        source,
    })
}

pub fn load_static_grouping_model(
    path: &Path,
) -> Result<StaticGroupingModel, GroupingPredictorError> {
    let body = fs::read_to_string(path).map_err(|source| GroupingPredictorError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&body).map_err(|source| GroupingPredictorError::ParseJsonModel {
        path: path.to_path_buf(),
        source,
    })
}

pub fn predict_best_grouping_from_static_model(
    model_path: &Path,
    probe_log: &Path,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    let model = load_static_grouping_model(model_path)?;
    let profile = extract_partition_profile_from_profile_log(probe_log)
        .ok_or_else(|| GroupingPredictorError::InvalidMetadata(probe_log.to_path_buf()))?;
    let mut prediction = predict_from_static_model(&model, &profile, true)?;
    prediction.history_root = model_path.to_path_buf();
    Ok(prediction)
}

pub fn predict_best_grouping_from_static_model_for_dataset(
    model_path: &Path,
    dataset_path: &Path,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    let model = load_static_grouping_model(model_path)?;
    let profile = extract_partition_profile_from_dataset_path(dataset_path)
        .ok_or_else(|| GroupingPredictorError::InvalidMetadata(dataset_path.to_path_buf()))?;
    let mut prediction = predict_from_static_model(&model, &profile, true)?;
    prediction.history_root = model_path.to_path_buf();
    Ok(prediction)
}

pub fn emit_saved_dataset_groupings(
    model: &StaticGroupingModel,
) -> Result<Vec<SavedDatasetGrouping>, GroupingPredictorError> {
    let mut emitted = model
        .known_dataset_best
        .iter()
        .map(|item| SavedDatasetGrouping {
            dataset: item.dataset.clone(),
            source: item.source.clone(),
            recommended_variant: item.variant.clone(),
            recommended_family: item.family.clone(),
            recommended_big_groups: item.big_groups.clone(),
            recommended_little_groups: item.little_groups.clone(),
            mode: "known_best".to_string(),
        })
        .collect::<Vec<_>>();
    emitted.sort_by(|lhs, rhs| lhs.dataset.cmp(&rhs.dataset));
    Ok(emitted)
}

fn build_static_grouping_model(
    history_root: &Path,
    neighbor_count: usize,
    runs: &[ProfileRun],
    dataset_profiles: &BTreeMap<String, Arc<GraphPartitionProfile>>,
) -> StaticGroupingModel {
    let candidates = collect_candidate_shapes(runs);
    let examples = build_training_examples(runs, Some(dataset_profiles));
    let throughput = average_pipe_throughputs(runs);
    let dataset_throughput_profiles =
        build_dataset_throughput_profiles(runs, Some(dataset_profiles), throughput);
    let example_rows =
        prepare_example_feature_rows(&examples, &dataset_throughput_profiles, throughput);
    let metadata_scale = fit_standardizer(
        &examples
            .iter()
            .map(|item| metadata_feature_row(&item.profile.metadata))
            .collect::<Vec<_>>(),
    );
    let regression_model = fit_regression_model(&examples, &example_rows);
    let ranking_model = fit_ranking_model(&examples, &example_rows);
    let blend_model = fit_blend_model(
        &examples,
        &example_rows,
        &regression_model,
        &ranking_model,
        &metadata_scale,
        None,
        neighbor_count,
    );
    let observed_variants: BTreeSet<_> = runs.iter().map(|run| run.variant.clone()).collect();
    let observed_families: BTreeSet<_> = runs.iter().map(|run| run.family.clone()).collect();

    StaticGroupingModel {
        model_version: STATIC_MODEL_VERSION,
        history_root: history_root.to_path_buf(),
        neighbor_count,
        total_runs: runs.len(),
        total_datasets: runs
            .iter()
            .map(|run| run.dataset.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        dominant_pipeline_total: dominant_pipeline_total(runs),
        candidate_shapes: candidates,
        observed_variants: observed_variants.into_iter().collect(),
        observed_families: observed_families.into_iter().collect(),
        examples: examples
            .iter()
            .map(|example| StaticTrainingExample {
                variant: example.run.variant.clone(),
                family: example.run.family.clone(),
                dataset: example.run.dataset.clone(),
                relative_score: example.relative_score,
                metadata: example.profile.metadata.clone(),
            })
            .collect(),
        dataset_throughput_profiles,
        metadata_scale,
        regression_model,
        ranking_model,
        blend_model,
        known_dataset_best: known_dataset_best_groupings(runs),
    }
}

fn predict_from_training_runs(
    training_runs: &[ProfileRun],
    dataset_profiles: Option<&BTreeMap<String, Arc<GraphPartitionProfile>>>,
    candidates: &[GroupingShape],
    profile: &GraphPartitionProfile,
    exclude_dataset: Option<&str>,
    neighbor_count: usize,
    enable_precise_rerank: bool,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    if training_runs.is_empty() {
        return Err(GroupingPredictorError::NoLogsFound(PathBuf::from(".")));
    }

    let examples = build_training_examples(training_runs, dataset_profiles);
    let throughput = average_pipe_throughputs(training_runs);
    let dataset_throughput_profiles =
        build_dataset_throughput_profiles(training_runs, dataset_profiles, throughput);
    let example_rows =
        prepare_example_feature_rows(&examples, &dataset_throughput_profiles, throughput);
    let metadata_scale = fit_standardizer(
        &examples
            .iter()
            .map(|item| metadata_feature_row(&item.profile.metadata))
            .collect::<Vec<_>>(),
    );
    let model = fit_regression_model(&examples, &example_rows);
    let ranking_model = fit_ranking_model(&examples, &example_rows);
    let probe_throughput = predict_dataset_throughput(
        &profile.metadata,
        &dataset_throughput_profiles,
        &metadata_scale,
        exclude_dataset,
        neighbor_count,
        throughput,
    );
    let blend_model = fit_blend_model(
        &examples,
        &example_rows,
        &model,
        &ranking_model,
        &metadata_scale,
        exclude_dataset,
        neighbor_count,
    );

    let observed_variants: BTreeSet<_> = training_runs
        .iter()
        .map(|run| run.variant.clone())
        .collect();
    let observed_families: BTreeSet<_> =
        training_runs.iter().map(|run| run.family.clone()).collect();
    let raw_graph = if enable_precise_rerank {
        load_raw_graph_from_profile_log(&profile.metadata.source)
    } else {
        None
    };

    let target_total = dominant_pipeline_total(training_runs);
    let mut ranked = Vec::new();
    for shape in candidates
        .iter()
        .filter(|shape| shape.total_pipelines() == target_total)
    {
        let linear_score = model.predict(profile, shape, probe_throughput);
        let ranking_score = ranking_model.predict(profile, shape, probe_throughput);
        let variant_knn = knn_score_for(
            &profile.metadata,
            &metadata_scale,
            &examples
                .iter()
                .filter(|item| item.run.variant == shape.variant)
                .collect::<Vec<_>>(),
            exclude_dataset,
            neighbor_count,
        );
        let family_knn = knn_score_for(
            &profile.metadata,
            &metadata_scale,
            &examples
                .iter()
                .filter(|item| item.run.family == shape.family)
                .collect::<Vec<_>>(),
            exclude_dataset,
            neighbor_count,
        );
        let score = blend_model.predict(linear_score, ranking_score, variant_knn, family_knn);
        ranked.push(CandidateScore {
            variant: shape.variant.clone(),
            family: shape.family.clone(),
            big_groups: shape.big_groups.clone(),
            little_groups: shape.little_groups.clone(),
            score,
            linear_score,
            ranking_score,
            variant_knn_score: variant_knn,
            family_knn_score: family_knn,
            precise_partition_score: None,
            variant_seen_in_training: observed_variants.contains(&shape.variant),
            family_seen_in_training: observed_families.contains(&shape.family),
        });
    }

    ranked.sort_by(|lhs, rhs| {
        rhs.score
            .partial_cmp(&lhs.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.variant.cmp(&rhs.variant))
    });

    if let Some(graph) = raw_graph.as_ref() {
        let shortlist: BTreeSet<_> = ranked
            .iter()
            .take(PRECISE_RERANK_CANDIDATES)
            .map(|candidate| candidate.variant.clone())
            .collect();
        let shortlist_shapes: Vec<_> = candidates
            .iter()
            .filter(|shape| shortlist.contains(&shape.variant))
            .cloned()
            .collect();
        let precise_scores = score_candidates_with_precise_partition(
            graph,
            &shortlist_shapes,
            probe_throughput,
            profile.metadata.dataset.as_str(),
        );

        for candidate in &mut ranked {
            let precise_partition_score = precise_scores.get(&candidate.variant).copied();
            candidate.precise_partition_score = precise_partition_score;
            candidate.score = blend_scores(
                &blend_model,
                candidate.linear_score,
                candidate.ranking_score,
                candidate.variant_knn_score,
                candidate.family_knn_score,
                precise_partition_score,
            );
        }

        ranked.sort_by(|lhs, rhs| {
            rhs.score
                .partial_cmp(&lhs.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| lhs.variant.cmp(&rhs.variant))
        });
    }

    let best = ranked
        .first()
        .cloned()
        .expect("candidate library must not be empty");

    Ok(GroupingPrediction {
        history_root: PathBuf::new(),
        metadata: profile.metadata.clone(),
        total_runs: training_runs.len(),
        total_datasets: training_runs
            .iter()
            .map(|run| run.dataset.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        candidate_variants: ranked.len(),
        training_variants: observed_variants.len(),
        recommended_variant: best.variant.clone(),
        recommended_family: best.family.clone(),
        recommended_big_groups: best.big_groups.clone(),
        recommended_little_groups: best.little_groups.clone(),
        ranked_candidates: ranked.into_iter().take(8).collect(),
    })
}

fn predict_from_static_model(
    model: &StaticGroupingModel,
    profile: &GraphPartitionProfile,
    enable_precise_rerank: bool,
) -> Result<GroupingPrediction, GroupingPredictorError> {
    let observed_variants: BTreeSet<_> = model.observed_variants.iter().cloned().collect();
    let observed_families: BTreeSet<_> = model.observed_families.iter().cloned().collect();
    let known_best = model
        .known_dataset_best
        .iter()
        .find(|item| item.dataset == profile.metadata.dataset)
        .cloned();
    let probe_throughput = predict_dataset_throughput(
        &profile.metadata,
        &model.dataset_throughput_profiles,
        &model.metadata_scale,
        None,
        model.neighbor_count,
        average_model_throughput(model),
    );
    let raw_graph = if enable_precise_rerank && known_best.is_none() {
        load_raw_graph_from_metadata(&profile.metadata)
    } else {
        None
    };

    let mut ranked = Vec::new();
    for shape in model
        .candidate_shapes
        .iter()
        .filter(|shape| shape.total_pipelines() == model.dominant_pipeline_total)
    {
        let linear_score = model
            .regression_model
            .predict(profile, shape, probe_throughput);
        let ranking_score = model
            .ranking_model
            .predict(profile, shape, probe_throughput);
        let variant_knn = knn_score_for_saved(
            &profile.metadata,
            &model.metadata_scale,
            &model
                .examples
                .iter()
                .filter(|item| item.variant == shape.variant)
                .collect::<Vec<_>>(),
            None,
            model.neighbor_count,
        );
        let family_knn = knn_score_for_saved(
            &profile.metadata,
            &model.metadata_scale,
            &model
                .examples
                .iter()
                .filter(|item| item.family == shape.family)
                .collect::<Vec<_>>(),
            None,
            model.neighbor_count,
        );
        let score = model
            .blend_model
            .predict(linear_score, ranking_score, variant_knn, family_knn);
        ranked.push(CandidateScore {
            variant: shape.variant.clone(),
            family: shape.family.clone(),
            big_groups: shape.big_groups.clone(),
            little_groups: shape.little_groups.clone(),
            score,
            linear_score,
            ranking_score,
            variant_knn_score: variant_knn,
            family_knn_score: family_knn,
            precise_partition_score: None,
            variant_seen_in_training: observed_variants.contains(&shape.variant),
            family_seen_in_training: observed_families.contains(&shape.family),
        });
    }

    ranked.sort_by(|lhs, rhs| {
        rhs.score
            .partial_cmp(&lhs.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.variant.cmp(&rhs.variant))
    });

    if let Some(graph) = raw_graph.as_ref() {
        let shortlist: BTreeSet<_> = ranked
            .iter()
            .take(PRECISE_RERANK_CANDIDATES)
            .map(|candidate| candidate.variant.clone())
            .collect();
        let shortlist_shapes: Vec<_> = model
            .candidate_shapes
            .iter()
            .filter(|shape| shortlist.contains(&shape.variant))
            .cloned()
            .collect();
        let precise_scores = score_candidates_with_precise_partition(
            graph,
            &shortlist_shapes,
            probe_throughput,
            profile.metadata.dataset.as_str(),
        );

        for candidate in &mut ranked {
            let precise_partition_score = precise_scores.get(&candidate.variant).copied();
            candidate.precise_partition_score = precise_partition_score;
            candidate.score = blend_scores(
                &model.blend_model,
                candidate.linear_score,
                candidate.ranking_score,
                candidate.variant_knn_score,
                candidate.family_knn_score,
                precise_partition_score,
            );
        }

        ranked.sort_by(|lhs, rhs| {
            rhs.score
                .partial_cmp(&lhs.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| lhs.variant.cmp(&rhs.variant))
        });
    }

    let mut override_mode = false;
    if let Some(best) = known_best.as_ref() {
        override_mode = true;
        if let Some(index) = ranked
            .iter()
            .position(|candidate| candidate.variant == best.variant)
        {
            let mut candidate = ranked.remove(index);
            candidate.score = ranked.first().map(|item| item.score + 1.0).unwrap_or(1.0);
            ranked.insert(0, candidate);
        } else {
            ranked.insert(
                0,
                CandidateScore {
                    variant: best.variant.clone(),
                    family: best.family.clone(),
                    big_groups: best.big_groups.clone(),
                    little_groups: best.little_groups.clone(),
                    score: ranked.first().map(|item| item.score + 1.0).unwrap_or(1.0),
                    linear_score: 0.0,
                    ranking_score: 0.0,
                    variant_knn_score: None,
                    family_knn_score: None,
                    precise_partition_score: None,
                    variant_seen_in_training: true,
                    family_seen_in_training: true,
                },
            );
        }
    }

    let best = ranked
        .first()
        .cloned()
        .expect("candidate library must not be empty");

    Ok(GroupingPrediction {
        history_root: model.history_root.clone(),
        metadata: profile.metadata.clone(),
        total_runs: model.total_runs,
        total_datasets: model.total_datasets,
        candidate_variants: ranked.len(),
        training_variants: model.observed_variants.len(),
        recommended_variant: best.variant.clone(),
        recommended_family: best.family.clone(),
        recommended_big_groups: best.big_groups.clone(),
        recommended_little_groups: best.little_groups.clone(),
        ranked_candidates: if override_mode {
            ranked.into_iter().take(8).collect()
        } else {
            ranked.into_iter().take(8).collect()
        },
    })
}

fn parse_profile_log(
    path: &Path,
    shape_library: &BTreeMap<String, GroupingShape>,
) -> Option<ProfileRun> {
    let (raw_variant, raw_dataset) = infer_variant_and_dataset(path)?;
    let variant = normalize_variant_name(&raw_variant);
    let shape = shape_library.get(&variant)?.clone();
    let body = fs::read_to_string(path).ok()?;
    let mut total_mteps = None;
    let mut big_tp_sum = 0.0;
    let mut big_tp_count = 0usize;
    let mut little_tp_sum = 0.0;
    let mut little_tp_count = 0usize;

    for line in body.lines() {
        if total_mteps.is_none() {
            total_mteps = extract_number_after(line, "Total MTEPS (Edges / Total Time): ");
        }
        if line.contains("Sparse Group") && line.contains("Throughput = ") {
            if let Some(value) = extract_number_after(line, "Throughput = ") {
                big_tp_sum += value;
                big_tp_count += 1;
            }
        }
        if line.contains("Dense Group") && line.contains("Throughput = ") {
            if let Some(value) = extract_number_after(line, "Throughput = ") {
                little_tp_sum += value;
                little_tp_count += 1;
            }
        }
    }

    let dataset = extract_graph_metadata_from_profile_log(path)
        .map(|metadata| metadata.dataset)
        .unwrap_or(raw_dataset);

    Some(ProfileRun {
        path: path.to_path_buf(),
        variant: variant.clone(),
        family: shape.family.clone(),
        dataset,
        total_mteps: total_mteps?,
        shape,
        avg_big_pipe_throughput: if big_tp_count == 0 {
            None
        } else {
            Some(big_tp_sum / big_tp_count as f64)
        },
        avg_little_pipe_throughput: if little_tp_count == 0 {
            None
        } else {
            Some(little_tp_sum / little_tp_count as f64)
        },
    })
}

fn collect_dataset_profiles(runs: &[ProfileRun]) -> BTreeMap<String, Arc<GraphPartitionProfile>> {
    let mut metadata_by_dataset = BTreeMap::new();
    for run in runs {
        if metadata_by_dataset.contains_key(&run.dataset) {
            continue;
        }
        if let Some(profile) = extract_partition_profile_from_profile_log(&run.path) {
            metadata_by_dataset.insert(run.dataset.clone(), Arc::new(profile));
        }
    }
    metadata_by_dataset
}

fn build_training_examples(
    runs: &[ProfileRun],
    dataset_profiles: Option<&BTreeMap<String, Arc<GraphPartitionProfile>>>,
) -> Vec<TrainingExample> {
    let best_by_dataset: BTreeMap<_, _> =
        runs.iter()
            .fold(BTreeMap::<String, f64>::new(), |mut acc, run| {
                acc.entry(run.dataset.clone())
                    .and_modify(|best| *best = best.max(run.total_mteps))
                    .or_insert(run.total_mteps);
                acc
            });

    runs.iter()
        .filter_map(|run| {
            let best = *best_by_dataset.get(&run.dataset)?;
            let profile = dataset_profiles
                .and_then(|profiles| profiles.get(&run.dataset))
                .cloned()
                .or_else(|| extract_partition_profile_from_profile_log(&run.path).map(Arc::new))?;
            Some(TrainingExample {
                run: run.clone(),
                profile,
                relative_score: if best <= f64::EPSILON {
                    0.0
                } else {
                    run.total_mteps / best
                },
            })
        })
        .collect()
}

fn prepare_example_feature_rows(
    examples: &[TrainingExample],
    throughput_profiles: &[DatasetThroughputProfile],
    fallback_throughput: ThroughputAverages,
) -> Vec<Vec<f64>> {
    examples
        .iter()
        .map(|example| {
            let throughput = throughput_for_dataset(
                example.run.dataset.as_str(),
                throughput_profiles,
                fallback_throughput,
            );
            compose_feature_vector(&example.profile, &example.run.shape, throughput)
        })
        .collect()
}

fn average_pipe_throughputs(runs: &[ProfileRun]) -> ThroughputAverages {
    let mut big_sum = 0.0;
    let mut big_count = 0usize;
    let mut little_sum = 0.0;
    let mut little_count = 0usize;

    for run in runs {
        if let Some(value) = run.avg_big_pipe_throughput {
            big_sum += value;
            big_count += 1;
        }
        if let Some(value) = run.avg_little_pipe_throughput {
            little_sum += value;
            little_count += 1;
        }
    }

    ThroughputAverages {
        big_per_pipe: if big_count == 0 {
            1.0
        } else {
            big_sum / big_count as f64
        },
        little_per_pipe: if little_count == 0 {
            1.0
        } else {
            little_sum / little_count as f64
        },
    }
}

fn build_dataset_throughput_profiles(
    runs: &[ProfileRun],
    dataset_profiles: Option<&BTreeMap<String, Arc<GraphPartitionProfile>>>,
    fallback: ThroughputAverages,
) -> Vec<DatasetThroughputProfile> {
    let mut values = BTreeMap::<String, (Vec<f64>, Vec<f64>)>::new();

    for run in runs {
        let entry = values
            .entry(run.dataset.clone())
            .or_insert_with(|| (Vec::new(), Vec::new()));
        if let Some(value) = run.avg_big_pipe_throughput {
            entry.0.push(value);
        }
        if let Some(value) = run.avg_little_pipe_throughput {
            entry.1.push(value);
        }
    }

    values
        .into_iter()
        .filter_map(|(dataset, (mut big, mut little))| {
            let metadata = dataset_profiles
                .and_then(|profiles| profiles.get(&dataset))
                .map(|profile| profile.metadata.clone())
                .or_else(|| {
                    runs.iter()
                        .find(|run| run.dataset == dataset)
                        .and_then(|run| extract_partition_profile_from_profile_log(&run.path))
                        .map(|profile| profile.metadata)
                })?;
            Some(DatasetThroughputProfile {
                dataset,
                metadata,
                throughput: ThroughputAverages {
                    big_per_pipe: median_or(big.as_mut_slice(), fallback.big_per_pipe),
                    little_per_pipe: median_or(little.as_mut_slice(), fallback.little_per_pipe),
                },
            })
        })
        .collect()
}

fn predict_dataset_throughput(
    metadata: &GraphMetadata,
    profiles: &[DatasetThroughputProfile],
    scaler: &Standardizer,
    exclude_dataset: Option<&str>,
    neighbor_count: usize,
    fallback: ThroughputAverages,
) -> ThroughputAverages {
    let mut items: Vec<_> = profiles
        .iter()
        .filter(|profile| exclude_dataset != Some(profile.dataset.as_str()))
        .map(|profile| {
            (
                metadata_distance_standardized(metadata, &profile.metadata, scaler),
                profile.throughput,
            )
        })
        .collect();
    if items.is_empty() {
        return fallback;
    }
    items.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));
    let k = neighbor_count.max(1).min(items.len());
    let mut big_weighted = 0.0;
    let mut little_weighted = 0.0;
    let mut total_weight = 0.0;
    for (distance, throughput) in items.into_iter().take(k) {
        let weight = 1.0 / (0.05 + distance);
        big_weighted += weight * throughput.big_per_pipe;
        little_weighted += weight * throughput.little_per_pipe;
        total_weight += weight;
    }
    if total_weight <= f64::EPSILON {
        fallback
    } else {
        ThroughputAverages {
            big_per_pipe: big_weighted / total_weight,
            little_per_pipe: little_weighted / total_weight,
        }
    }
}

fn fit_regression_model(examples: &[TrainingExample], rows: &[Vec<f64>]) -> RegressionModel {
    let targets: Vec<_> = examples.iter().map(|item| item.relative_score).collect();
    let scaler = fit_standardizer(&rows);
    let standardized: Vec<_> = rows.iter().map(|row| scaler.transform(row)).collect();
    let weights = fit_ridge_weights(&standardized, &targets, 0.01);

    RegressionModel {
        feature_mean: scaler.mean,
        feature_scale: scaler.scale,
        weights,
    }
}

fn fit_blend_model(
    examples: &[TrainingExample],
    example_rows: &[Vec<f64>],
    regression_model: &RegressionModel,
    ranking_model: &RankingModel,
    metadata_scaler: &Standardizer,
    exclude_dataset: Option<&str>,
    neighbor_count: usize,
) -> BlendModel {
    let rows: Vec<_> = examples
        .iter()
        .enumerate()
        .map(|(idx, example)| {
            let linear = regression_model.predict_row(&example_rows[idx]);
            let ranking = ranking_model.predict_row(&example_rows[idx]);
            let variant_knn = knn_score_for(
                &example.profile.metadata,
                metadata_scaler,
                &examples
                    .iter()
                    .filter(|item| item.run.variant == example.run.variant)
                    .collect::<Vec<_>>(),
                exclude_dataset.or(Some(example.run.dataset.as_str())),
                neighbor_count,
            );
            let family_knn = knn_score_for(
                &example.profile.metadata,
                metadata_scaler,
                &examples
                    .iter()
                    .filter(|item| item.run.family == example.run.family)
                    .collect::<Vec<_>>(),
                exclude_dataset.or(Some(example.run.dataset.as_str())),
                neighbor_count,
            );
            blend_feature_row(linear, ranking, variant_knn, family_knn)
        })
        .collect();
    let targets: Vec<_> = examples.iter().map(|item| item.relative_score).collect();
    let scaler = fit_standardizer(&rows);
    let standardized: Vec<_> = rows.iter().map(|row| scaler.transform(row)).collect();
    let weights = fit_ridge_weights(&standardized, &targets, 0.02);

    BlendModel {
        feature_mean: scaler.mean,
        feature_scale: scaler.scale,
        weights,
    }
}

impl RegressionModel {
    fn predict(
        &self,
        profile: &GraphPartitionProfile,
        shape: &GroupingShape,
        throughput: ThroughputAverages,
    ) -> f64 {
        let raw = compose_feature_vector(profile, shape, throughput);
        self.predict_row(&raw)
    }

    fn predict_row(&self, raw: &[f64]) -> f64 {
        let standardized = standardize_row(raw, &self.feature_mean, &self.feature_scale);
        dot(&self.weights, &standardized)
    }
}

impl RankingModel {
    fn predict(
        &self,
        profile: &GraphPartitionProfile,
        shape: &GroupingShape,
        throughput: ThroughputAverages,
    ) -> f64 {
        let raw = compose_feature_vector(profile, shape, throughput);
        self.predict_row(&raw)
    }

    fn predict_row(&self, raw: &[f64]) -> f64 {
        let standardized = standardize_row(raw, &self.feature_mean, &self.feature_scale);
        dot(&self.weights, &standardized)
    }
}

impl BlendModel {
    fn predict(
        &self,
        linear_score: f64,
        ranking_score: f64,
        variant_knn_score: Option<f64>,
        family_knn_score: Option<f64>,
    ) -> f64 {
        let row = blend_feature_row(
            linear_score,
            ranking_score,
            variant_knn_score,
            family_knn_score,
        );
        let standardized = standardize_row(&row, &self.feature_mean, &self.feature_scale);
        dot(&self.weights, &standardized)
    }
}

fn blend_feature_row(
    linear_score: f64,
    ranking_score: f64,
    variant_knn_score: Option<f64>,
    family_knn_score: Option<f64>,
) -> Vec<f64> {
    vec![
        1.0,
        linear_score,
        ranking_score,
        variant_knn_score.unwrap_or(linear_score),
        family_knn_score.unwrap_or(linear_score),
        bool_score(variant_knn_score.is_some()),
        bool_score(family_knn_score.is_some()),
    ]
}

fn throughput_for_dataset(
    dataset: &str,
    profiles: &[DatasetThroughputProfile],
    fallback: ThroughputAverages,
) -> ThroughputAverages {
    profiles
        .iter()
        .find(|profile| profile.dataset == dataset)
        .map(|profile| profile.throughput)
        .unwrap_or(fallback)
}

fn median_or(values: &mut [f64], fallback: f64) -> f64 {
    if values.is_empty() {
        return fallback;
    }
    values.sort_by(|lhs, rhs| lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) * 0.5
    } else {
        values[mid]
    }
}

fn fit_ranking_model(examples: &[TrainingExample], example_rows: &[Vec<f64>]) -> RankingModel {
    let mut rows = Vec::new();
    let mut targets = Vec::new();
    let mut by_dataset = BTreeMap::<String, Vec<usize>>::new();
    for (idx, example) in examples.iter().enumerate() {
        by_dataset
            .entry(example.run.dataset.clone())
            .or_default()
            .push(idx);
    }

    for dataset_examples in by_dataset.values() {
        for i in 0..dataset_examples.len() {
            for j in (i + 1)..dataset_examples.len() {
                let lhs_idx = dataset_examples[i];
                let rhs_idx = dataset_examples[j];
                let lhs = &examples[lhs_idx];
                let rhs = &examples[rhs_idx];
                let lhs_row = &example_rows[lhs_idx];
                let rhs_row = &example_rows[rhs_idx];
                if lhs.relative_score >= rhs.relative_score {
                    rows.push(vector_sub(lhs_row, rhs_row));
                    targets.push((lhs.relative_score - rhs.relative_score).max(1e-3));
                } else {
                    rows.push(vector_sub(rhs_row, lhs_row));
                    targets.push((rhs.relative_score - lhs.relative_score).max(1e-3));
                }
            }
        }
    }

    if rows.is_empty() {
        return RankingModel {
            feature_mean: vec![0.0],
            feature_scale: vec![1.0],
            weights: vec![0.0],
        };
    }

    let scaler = fit_standardizer(&rows);
    let standardized: Vec<_> = rows.iter().map(|row| scaler.transform(row)).collect();
    let weights = fit_ridge_weights(&standardized, &targets, 0.01);

    RankingModel {
        feature_mean: scaler.mean,
        feature_scale: scaler.scale,
        weights,
    }
}

fn vector_sub(lhs: &[f64], rhs: &[f64]) -> Vec<f64> {
    lhs.iter().zip(rhs.iter()).map(|(a, b)| a - b).collect()
}

fn compose_feature_vector(
    profile: &GraphPartitionProfile,
    shape: &GroupingShape,
    throughput: ThroughputAverages,
) -> Vec<f64> {
    let meta = profile.metadata.feature_vector();
    let shape_features = shape.feature_vector();
    let simulation =
        simulate_profile_based_assignment(&profile.dst_indegrees_desc, shape, throughput);

    let big_ratio = shape.big_pipelines() as f64 / shape.total_pipelines().max(1) as f64;
    let big_group_count = shape.big_group_count() as f64;
    let little_group_count = shape.little_group_count() as f64;
    let total_group_count = big_group_count + little_group_count;
    let big_cv = coeff_var_usize(&shape.big_groups);
    let little_cv = coeff_var_usize(&shape.little_groups);
    let big_max_share = max_share(&shape.big_groups);
    let little_max_share = max_share(&shape.little_groups);

    let mut row = vec![1.0];
    row.extend(meta.iter().copied());
    row.extend(shape_features.iter().copied());
    row.extend([
        meta[2] * big_ratio,
        meta[6] * big_group_count,
        meta[7] * little_group_count,
        meta[8] * big_max_share,
        meta[9] * little_max_share,
        meta[10] * big_cv,
        meta[11] * little_cv,
        meta[3] * total_group_count,
    ]);
    row.extend(simulation.feature_vector(profile, shape));
    row
}

impl PartitionSimulation {
    fn feature_vector(&self, profile: &GraphPartitionProfile, shape: &GroupingShape) -> Vec<f64> {
        let total_dst = profile.dst_indegrees_desc.len().max(1) as f64;
        let one_partition_capacity = (shape.big_group_count() * BIG_GROUP_DST_CAPACITY
            + shape.little_group_count() * LITTLE_GROUP_DST_CAPACITY)
            .max(1) as f64;
        let dense_ms = self.dense_est_ms.max(1e-9);
        let sparse_ms = self.sparse_est_ms.max(1e-9);
        let dense_time_cv = coeff_var_f64(&self.dense_group_edge_times);
        let sparse_time_cv = coeff_var_f64(&self.sparse_group_edge_times);
        let dense_fill_cv = coeff_var_f64(&self.dense_group_fill);
        let sparse_fill_cv = coeff_var_f64(&self.sparse_group_fill);
        let dense_partition_fill_cv = coeff_var_f64(&self.dense_partition_fill);
        let sparse_partition_fill_cv = coeff_var_f64(&self.sparse_partition_fill);
        let dense_output_words_total = self.dense_output_words.iter().sum::<f64>();
        let sparse_output_words_total = self.sparse_output_words.iter().sum::<f64>();

        vec![
            self.partition_number as f64,
            total_dst / one_partition_capacity,
            dense_ms,
            sparse_ms,
            dense_ms / sparse_ms,
            sparse_ms / dense_ms,
            mean_f64(&self.dense_group_edge_times),
            mean_f64(&self.sparse_group_edge_times),
            dense_time_cv,
            sparse_time_cv,
            max_f64(&self.dense_group_fill),
            max_f64(&self.sparse_group_fill),
            mean_f64(&self.dense_group_fill),
            mean_f64(&self.sparse_group_fill),
            dense_fill_cv,
            sparse_fill_cv,
            max_f64(&self.dense_partition_fill),
            max_f64(&self.sparse_partition_fill),
            mean_f64(&self.dense_partition_fill),
            mean_f64(&self.sparse_partition_fill),
            dense_partition_fill_cv,
            sparse_partition_fill_cv,
            dense_output_words_total,
            sparse_output_words_total,
            dense_output_words_total / total_dst,
            sparse_output_words_total / total_dst,
            max_f64(&self.dense_output_words),
            max_f64(&self.sparse_output_words),
            self.dense_active_partitions as f64,
            self.sparse_active_partitions as f64,
        ]
    }
}

fn score_candidates_with_precise_partition(
    graph: &RawGraph,
    candidates: &[GroupingShape],
    throughput: ThroughputAverages,
    _dataset: &str,
) -> BTreeMap<String, f64> {
    let (sampled_edges, edge_scale) = sample_edges_for_precise_sim(&graph.edges);
    let mut raw_scores = Vec::new();
    for shape in candidates {
        let cost = simulate_precise_partition_cost(
            graph.vertices,
            &graph.in_degree,
            &sampled_edges,
            edge_scale,
            shape,
            throughput,
        );
        raw_scores.push((shape.variant.clone(), 1.0 / cost.max(1e-9)));
    }
    let min_score = raw_scores
        .iter()
        .map(|(_, score)| *score)
        .fold(f64::INFINITY, f64::min);
    let max_score = raw_scores
        .iter()
        .map(|(_, score)| *score)
        .fold(f64::NEG_INFINITY, f64::max);

    raw_scores
        .into_iter()
        .map(|(variant, score)| {
            let normalized = if (max_score - min_score).abs() <= 1e-12 {
                1.0
            } else {
                (score - min_score) / (max_score - min_score)
            };
            (variant, normalized)
        })
        .collect()
}

fn simulate_precise_partition_cost(
    vertices: usize,
    in_degree: &[u32],
    edges: &[RawGraphEdge],
    edge_scale: f64,
    shape: &GroupingShape,
    throughput: ThroughputAverages,
) -> f64 {
    let mut dsts: Vec<(u32, u32)> = in_degree
        .iter()
        .enumerate()
        .filter_map(|(dst, indegree)| (*indegree > 0).then_some((dst as u32, *indegree)))
        .collect();
    dsts.sort_unstable_by(|lhs, rhs| rhs.1.cmp(&lhs.1).then_with(|| lhs.0.cmp(&rhs.0)));

    let dense_groups = shape.little_group_count();
    let sparse_groups = shape.big_group_count();
    let one_partition_capacity =
        dense_groups * LITTLE_GROUP_DST_CAPACITY + sparse_groups * BIG_GROUP_DST_CAPACITY;
    let partition_number = ceil_div_usize(dsts.len().max(1), one_partition_capacity.max(1)).max(1);

    let dense_tp: Vec<f64> = shape
        .little_groups
        .iter()
        .map(|count| throughput.little_per_pipe * *count as f64)
        .collect();
    let sparse_tp: Vec<f64> = shape
        .big_groups
        .iter()
        .map(|count| throughput.big_per_pipe * *count as f64)
        .collect();

    let mut dst_assignment = vec![None; vertices];
    let mut little_dst_lists = vec![vec![Vec::<u32>::new(); partition_number]; dense_groups];
    let mut big_dst_lists = vec![vec![Vec::<u32>::new(); partition_number]; sparse_groups];

    let mut dense_remaining = vec![LITTLE_GROUP_DST_CAPACITY * partition_number; dense_groups];
    let mut sparse_remaining = vec![BIG_GROUP_DST_CAPACITY * partition_number; sparse_groups];
    let mut dense_assigned_edges = vec![0.0; dense_groups];
    let mut sparse_assigned_edges = vec![0.0; sparse_groups];
    let mut dense_assigned_edges_total = 0.0;
    let mut sparse_assigned_edges_total = 0.0;
    let mut dense_remaining_total = dense_remaining.iter().sum::<usize>();
    let mut sparse_remaining_total = sparse_remaining.iter().sum::<usize>();
    let dense_total_tp = dense_tp.iter().sum::<f64>().max(1e-9);
    let sparse_total_tp = sparse_tp.iter().sum::<f64>().max(1e-9);

    for (vertex_id, indegree) in dsts {
        let edge_cost = indegree as f64;
        let can_dense = dense_remaining_total > 0;
        let can_sparse = sparse_remaining_total > 0;
        if !can_dense && !can_sparse {
            break;
        }

        let choose_dense_class = if can_dense && !can_sparse {
            true
        } else if !can_dense && can_sparse {
            false
        } else {
            let dense_ms_now = dense_assigned_edges_total / dense_total_tp;
            let sparse_ms_now = sparse_assigned_edges_total / sparse_total_tp;
            let dense_ms_if = (dense_assigned_edges_total + edge_cost) / dense_total_tp;
            let sparse_ms_if = (sparse_assigned_edges_total + edge_cost) / sparse_total_tp;
            (dense_ms_if - sparse_ms_now).abs() <= (dense_ms_now - sparse_ms_if).abs()
        };

        let mut choose_dense = choose_dense_class;
        let mut group_idx = usize::MAX;
        if choose_dense {
            group_idx = choose_group_by_projected_ms(
                &dense_remaining,
                &dense_assigned_edges,
                &dense_tp,
                edge_cost,
            );
            if group_idx == usize::MAX {
                choose_dense = false;
            }
        }
        if !choose_dense {
            group_idx = choose_group_by_projected_ms(
                &sparse_remaining,
                &sparse_assigned_edges,
                &sparse_tp,
                edge_cost,
            );
            if group_idx == usize::MAX {
                choose_dense = true;
                group_idx = choose_group_by_projected_ms(
                    &dense_remaining,
                    &dense_assigned_edges,
                    &dense_tp,
                    edge_cost,
                );
            }
        }
        if group_idx == usize::MAX {
            continue;
        }

        let placed = if choose_dense {
            place_dst_vertex(
                &mut little_dst_lists[group_idx],
                LITTLE_GROUP_DST_CAPACITY,
                vertex_id,
            )
        } else {
            place_dst_vertex(
                &mut big_dst_lists[group_idx],
                BIG_GROUP_DST_CAPACITY,
                vertex_id,
            )
        };
        let Some(partition_idx) = placed else {
            continue;
        };
        dst_assignment[vertex_id as usize] = Some((choose_dense, group_idx, partition_idx));

        if choose_dense {
            dense_remaining[group_idx] = dense_remaining[group_idx].saturating_sub(1);
            dense_remaining_total = dense_remaining_total.saturating_sub(1);
            dense_assigned_edges[group_idx] += edge_cost;
            dense_assigned_edges_total += edge_cost;
        } else {
            sparse_remaining[group_idx] = sparse_remaining[group_idx].saturating_sub(1);
            sparse_remaining_total = sparse_remaining_total.saturating_sub(1);
            sparse_assigned_edges[group_idx] += edge_cost;
            sparse_assigned_edges_total += edge_cost;
        }
    }

    let mut dense_edges = vec![vec![Vec::<RawGraphEdge>::new(); partition_number]; dense_groups];
    let mut sparse_edges = vec![vec![Vec::<RawGraphEdge>::new(); partition_number]; sparse_groups];
    for edge in edges {
        if let Some((is_dense, group_idx, partition_idx)) = dst_assignment[edge.dst as usize] {
            if is_dense {
                dense_edges[group_idx][partition_idx].push(*edge);
            } else {
                sparse_edges[group_idx][partition_idx].push(*edge);
            }
        }
    }

    let mut global_to_local = vec![-1i32; vertices];
    let mut dense_group_times = vec![0.0; dense_groups];
    let mut sparse_group_times = vec![0.0; sparse_groups];
    let mut total_work = 0usize;
    let mut total_output_words = 0usize;
    let mut active_partitions = 0usize;

    for group_idx in 0..dense_groups {
        let num_pipelines = shape.little_groups[group_idx];
        for partition_idx in 0..partition_number {
            let dst_count = little_dst_lists[group_idx][partition_idx].len();
            if dst_count > 0 {
                active_partitions += 1;
                total_output_words += ceil_div_usize(dst_count, 16);
            }
            let works = simulate_partition_pipeline_works(
                &dense_edges[group_idx][partition_idx],
                &little_dst_lists[group_idx][partition_idx],
                true,
                num_pipelines,
                &mut global_to_local,
            );
            total_work += (works.iter().sum::<usize>() as f64 * edge_scale).round() as usize;
            let partition_time =
                max_usize(&works) as f64 * edge_scale / throughput.little_per_pipe.max(1e-9);
            dense_group_times[group_idx] += partition_time;
        }
    }
    for group_idx in 0..sparse_groups {
        let num_pipelines = shape.big_groups[group_idx];
        for partition_idx in 0..partition_number {
            let dst_count = big_dst_lists[group_idx][partition_idx].len();
            if dst_count > 0 {
                active_partitions += 1;
                total_output_words += ceil_div_usize(dst_count, 16);
            }
            let works = simulate_partition_pipeline_works(
                &sparse_edges[group_idx][partition_idx],
                &big_dst_lists[group_idx][partition_idx],
                false,
                num_pipelines,
                &mut global_to_local,
            );
            total_work += (works.iter().sum::<usize>() as f64 * edge_scale).round() as usize;
            let partition_time =
                max_usize(&works) as f64 * edge_scale / throughput.big_per_pipe.max(1e-9);
            sparse_group_times[group_idx] += partition_time;
        }
    }

    let base_time = max_f64(&dense_group_times)
        .max(max_f64(&sparse_group_times))
        .max(1e-9);
    let padding_ratio = total_work as f64 / ((edges.len() as f64 * edge_scale).max(1.0));
    let output_ratio = total_output_words as f64 / in_degree.len().max(1) as f64;
    let partition_ratio = active_partitions as f64
        / ((dense_groups + sparse_groups).max(1) * partition_number.max(1)) as f64;

    base_time * (1.0 + 0.08 * padding_ratio + 0.04 * output_ratio + 0.03 * partition_ratio)
}

fn sample_edges_for_precise_sim(edges: &[RawGraphEdge]) -> (Vec<RawGraphEdge>, f64) {
    if edges.len() <= MAX_PRECISE_SAMPLE_EDGES {
        return (edges.to_vec(), 1.0);
    }

    let stride = ceil_div_usize(edges.len(), MAX_PRECISE_SAMPLE_EDGES).max(1);
    let sampled = edges.iter().step_by(stride).copied().collect::<Vec<_>>();
    let scale = edges.len() as f64 / sampled.len().max(1) as f64;
    (sampled, scale)
}

fn place_dst_vertex(parts: &mut [Vec<u32>], cap: usize, vertex_id: u32) -> Option<usize> {
    for (idx, part) in parts.iter_mut().enumerate() {
        if part.len() < cap {
            part.push(vertex_id);
            return Some(idx);
        }
    }
    None
}

fn simulate_partition_pipeline_works(
    partition_edges: &[RawGraphEdge],
    ordered_dst_vertices: &[u32],
    is_dense: bool,
    num_pipelines: usize,
    global_to_local: &mut [i32],
) -> Vec<usize> {
    if partition_edges.is_empty() || num_pipelines == 0 {
        return vec![0; num_pipelines];
    }

    const SRC_ASSIGN_GRANULARITY: usize = 256;
    const SRC_BUFFER_SIZE: usize = 4096;
    let mut touched = Vec::with_capacity(ordered_dst_vertices.len() + 1024);
    let mut local_id_counter = 0i32;
    let mut map_vertex = |global_id: u32, touched: &mut Vec<u32>, local_id_counter: &mut i32| {
        let slot = &mut global_to_local[global_id as usize];
        if *slot == -1 {
            *slot = *local_id_counter;
            *local_id_counter += 1;
            touched.push(global_id);
        }
    };

    for &dst in ordered_dst_vertices {
        map_vertex(dst, &mut touched, &mut local_id_counter);
    }
    for edge in partition_edges {
        map_vertex(edge.src, &mut touched, &mut local_id_counter);
        map_vertex(edge.dst, &mut touched, &mut local_id_counter);
    }

    let num_vertices = local_id_counter.max(0) as usize;
    let num_blocks = ceil_div_usize(num_vertices, SRC_BUFFER_SIZE);
    let num_units = ceil_div_usize(num_vertices, SRC_ASSIGN_GRANULARITY);
    let units_per_block = SRC_BUFFER_SIZE / SRC_ASSIGN_GRANULARITY;
    let mut unit_edge_counts = vec![0usize; num_units];
    for edge in partition_edges {
        let src_local = global_to_local[edge.src as usize] as usize;
        let unit = src_local / SRC_ASSIGN_GRANULARITY;
        unit_edge_counts[unit] += 1;
    }

    let padded_work_for_unit_range = |start_unit: usize, end_unit: usize| -> usize {
        let mut work_with_pads = 0usize;
        let mut last_edge_block: Option<usize> = None;
        for unit in start_unit..end_unit {
            let edges = unit_edge_counts[unit];
            if edges == 0 {
                continue;
            }
            let block = unit / units_per_block;
            if is_dense && last_edge_block.is_some() && last_edge_block != Some(block) {
                work_with_pads += pad_to_word_usize(work_with_pads);
            }
            last_edge_block = Some(block);
            work_with_pads += edges;
        }
        if last_edge_block.is_some() {
            work_with_pads += pad_to_word_usize(work_with_pads);
        }
        work_with_pads
    };

    let total_edges = unit_edge_counts.iter().sum::<usize>();
    let mut cut_units = vec![0usize; num_pipelines + 1];
    let mut unit_idx = 0usize;
    let mut cum_edges = 0usize;
    for pip in 0..num_pipelines.saturating_sub(1) {
        let target = (total_edges * (pip + 1) + num_pipelines - 1) / num_pipelines.max(1);
        while unit_idx < num_units && cum_edges + unit_edge_counts[unit_idx] < target {
            cum_edges += unit_edge_counts[unit_idx];
            unit_idx += 1;
        }
        if unit_idx < num_units {
            let without = cum_edges;
            let with = cum_edges + unit_edge_counts[unit_idx];
            if with >= target && with - target <= target.saturating_sub(without) {
                cum_edges = with;
                unit_idx += 1;
            }
        }
        cut_units[pip + 1] = unit_idx;
    }
    cut_units[num_pipelines] = num_units;

    let recompute_work = |cut_units: &[usize]| -> Vec<usize> {
        (0..num_pipelines)
            .map(|pip| padded_work_for_unit_range(cut_units[pip], cut_units[pip + 1]))
            .collect()
    };

    let mut work_with_pads = recompute_work(&cut_units);
    if total_edges != 0 && !balanced_under_one_pct(&work_with_pads) {
        for _ in 0..4096 {
            let Some((_min_pip, max_pip, min_work, max_work)) =
                find_active_minmax_usize(&work_with_pads)
            else {
                break;
            };
            if (max_work - min_work) * 100 <= max_work {
                break;
            }
            let mut best_move: Option<bool> = None;
            let mut best_improvement = 0usize;

            if max_pip > 0 {
                let start = cut_units[max_pip];
                let end = cut_units[max_pip + 1];
                if start < end {
                    let new_start = start + 1;
                    if new_start <= end && new_start >= cut_units[max_pip - 1] {
                        let w_left = padded_work_for_unit_range(cut_units[max_pip - 1], new_start);
                        let w_mid = padded_work_for_unit_range(new_start, end);
                        let mut tmp = work_with_pads.clone();
                        tmp[max_pip - 1] = w_left;
                        tmp[max_pip] = w_mid;
                        if let Some((_, _, tmp_min, tmp_max)) = find_active_minmax_usize(&tmp) {
                            let old_span = max_work - min_work;
                            let new_span = tmp_max - tmp_min;
                            if new_span < old_span {
                                best_improvement = old_span - new_span;
                                best_move = Some(true);
                            }
                        }
                    }
                }
            }

            if max_pip < num_pipelines - 1 {
                let start = cut_units[max_pip];
                let end = cut_units[max_pip + 1];
                if start < end {
                    let new_end = end - 1;
                    if new_end >= start && new_end <= cut_units[max_pip + 2] {
                        let w_mid = padded_work_for_unit_range(start, new_end);
                        let w_right = padded_work_for_unit_range(new_end, cut_units[max_pip + 2]);
                        let mut tmp = work_with_pads.clone();
                        tmp[max_pip] = w_mid;
                        tmp[max_pip + 1] = w_right;
                        if let Some((_, _, tmp_min, tmp_max)) = find_active_minmax_usize(&tmp) {
                            let old_span = max_work - min_work;
                            let new_span = tmp_max - tmp_min;
                            if new_span < old_span {
                                let improvement = old_span - new_span;
                                if best_move.is_none() || improvement > best_improvement {
                                    best_improvement = improvement;
                                    best_move = Some(false);
                                }
                            }
                        }
                    }
                }
            }

            let Some(shift_start_right) = best_move else {
                break;
            };
            if shift_start_right {
                cut_units[max_pip] += 1;
            } else {
                cut_units[max_pip + 1] -= 1;
            }
            work_with_pads = recompute_work(&cut_units);
        }
    }

    let mut unit_to_pipeline = vec![0usize; num_units];
    let mut cur_pip = 0usize;
    for (unit, slot) in unit_to_pipeline.iter_mut().enumerate() {
        while cur_pip < num_pipelines - 1 && unit >= cut_units[cur_pip + 1] {
            cur_pip += 1;
        }
        *slot = cur_pip;
    }

    let mut last_src_in_block = vec![-1i32; num_pipelines * num_blocks];
    let mut block_edges_in_pipeline = vec![0usize; num_pipelines * num_blocks];
    for edge in partition_edges {
        let src_local = global_to_local[edge.src as usize] as usize;
        let block = src_local / SRC_BUFFER_SIZE;
        let unit = src_local / SRC_ASSIGN_GRANULARITY;
        let pip = unit_to_pipeline[unit];
        let index = pip * num_blocks + block;
        block_edges_in_pipeline[index] += 1;
        last_src_in_block[index] = last_src_in_block[index].max(src_local as i32);
    }

    let mut pipeline_works = vec![0usize; num_pipelines];
    for pip in 0..num_pipelines {
        let mut work_with_pads_local = 0usize;
        let mut has_blocks = false;
        for block in 0..num_blocks {
            let edges_in_block = block_edges_in_pipeline[pip * num_blocks + block];
            if edges_in_block == 0 {
                continue;
            }
            if has_blocks && is_dense {
                work_with_pads_local += pad_to_word_usize(work_with_pads_local);
            }
            work_with_pads_local += edges_in_block;
            has_blocks = true;
        }
        if has_blocks {
            work_with_pads_local += pad_to_word_usize(work_with_pads_local);
        }
        pipeline_works[pip] = work_with_pads_local;
    }

    for global_id in touched {
        global_to_local[global_id as usize] = -1;
    }

    pipeline_works
}

fn pad_to_word_usize(n: usize) -> usize {
    const EDGES_PER_WORD: usize = 16;
    let rem = n % EDGES_PER_WORD;
    if rem == 0 { 0 } else { EDGES_PER_WORD - rem }
}

fn max_usize(values: &[usize]) -> usize {
    values.iter().copied().max().unwrap_or(0)
}

fn balanced_under_one_pct(work: &[usize]) -> bool {
    match find_active_minmax_usize(work) {
        None => true,
        Some((_, _, min_work, max_work)) => {
            max_work == 0 || (max_work - min_work) * 100 <= max_work
        }
    }
}

fn find_active_minmax_usize(work: &[usize]) -> Option<(usize, usize, usize, usize)> {
    let mut min_pip = None;
    let mut max_pip = None;
    let mut min_work = 0usize;
    let mut max_work = 0usize;
    for (pip, &w) in work.iter().enumerate() {
        if w == 0 {
            continue;
        }
        if min_pip.is_none() || w < min_work {
            min_pip = Some(pip);
            min_work = w;
        }
        if max_pip.is_none() || w > max_work {
            max_pip = Some(pip);
            max_work = w;
        }
    }
    match (min_pip, max_pip) {
        (Some(min_pip), Some(max_pip)) if min_pip != max_pip => {
            Some((min_pip, max_pip, min_work, max_work))
        }
        _ => None,
    }
}

fn simulate_profile_based_assignment(
    dst_indegrees_desc: &[u32],
    shape: &GroupingShape,
    throughput: ThroughputAverages,
) -> PartitionSimulation {
    let dense_groups = shape.little_group_count();
    let sparse_groups = shape.big_group_count();
    let total_dst_vertices = dst_indegrees_desc.len();
    let one_partition_capacity =
        dense_groups * LITTLE_GROUP_DST_CAPACITY + sparse_groups * BIG_GROUP_DST_CAPACITY;
    let partition_number = if one_partition_capacity == 0 {
        1
    } else {
        ceil_div_usize(total_dst_vertices.max(1), one_partition_capacity)
    };

    let dense_throughput: Vec<f64> = shape
        .little_groups
        .iter()
        .map(|count| (throughput.little_per_pipe * *count as f64).max(1.0))
        .collect();
    let sparse_throughput: Vec<f64> = shape
        .big_groups
        .iter()
        .map(|count| (throughput.big_per_pipe * *count as f64).max(1.0))
        .collect();

    let mut dense_remaining = vec![LITTLE_GROUP_DST_CAPACITY * partition_number; dense_groups];
    let mut sparse_remaining = vec![BIG_GROUP_DST_CAPACITY * partition_number; sparse_groups];
    let mut dense_assigned_edges = vec![0.0; dense_groups];
    let mut sparse_assigned_edges = vec![0.0; sparse_groups];
    let mut dense_partitions = vec![vec![0usize; partition_number]; dense_groups];
    let mut sparse_partitions = vec![vec![0usize; partition_number]; sparse_groups];

    let mut dense_assigned_edges_total = 0.0;
    let mut sparse_assigned_edges_total = 0.0;
    let mut dense_remaining_total = dense_remaining.iter().sum::<usize>();
    let mut sparse_remaining_total = sparse_remaining.iter().sum::<usize>();
    let dense_total_throughput = dense_throughput.iter().sum::<f64>().max(1.0);
    let sparse_total_throughput = sparse_throughput.iter().sum::<f64>().max(1.0);

    for &indegree in dst_indegrees_desc {
        let edge_cost = indegree as f64;
        let can_dense = dense_remaining_total > 0;
        let can_sparse = sparse_remaining_total > 0;
        if !can_dense && !can_sparse {
            break;
        }

        let choose_dense_class = if can_dense && !can_sparse {
            true
        } else if !can_dense && can_sparse {
            false
        } else {
            let dense_ms_now = dense_assigned_edges_total / dense_total_throughput;
            let sparse_ms_now = sparse_assigned_edges_total / sparse_total_throughput;
            let dense_ms_if = (dense_assigned_edges_total + edge_cost) / dense_total_throughput;
            let sparse_ms_if = (sparse_assigned_edges_total + edge_cost) / sparse_total_throughput;
            (dense_ms_if - sparse_ms_now).abs() <= (dense_ms_now - sparse_ms_if).abs()
        };

        let mut choose_dense = choose_dense_class;
        let mut choose_group = usize::MAX;
        if choose_dense {
            choose_group = choose_group_by_projected_ms(
                &dense_remaining,
                &dense_assigned_edges,
                &dense_throughput,
                edge_cost,
            );
            if choose_group == usize::MAX {
                choose_dense = false;
            }
        }
        if !choose_dense {
            choose_group = choose_group_by_projected_ms(
                &sparse_remaining,
                &sparse_assigned_edges,
                &sparse_throughput,
                edge_cost,
            );
            if choose_group == usize::MAX {
                choose_dense = true;
                choose_group = choose_group_by_projected_ms(
                    &dense_remaining,
                    &dense_assigned_edges,
                    &dense_throughput,
                    edge_cost,
                );
            }
        }
        if choose_group == usize::MAX {
            continue;
        }

        let assigned = if choose_dense {
            assign_vertex_to_first_open_partition(
                &mut dense_partitions[choose_group],
                LITTLE_GROUP_DST_CAPACITY,
            )
        } else {
            assign_vertex_to_first_open_partition(
                &mut sparse_partitions[choose_group],
                BIG_GROUP_DST_CAPACITY,
            )
        };
        if !assigned {
            continue;
        }

        if choose_dense {
            dense_remaining[choose_group] = dense_remaining[choose_group].saturating_sub(1);
            dense_remaining_total = dense_remaining_total.saturating_sub(1);
            dense_assigned_edges[choose_group] += edge_cost;
            dense_assigned_edges_total += edge_cost;
        } else {
            sparse_remaining[choose_group] = sparse_remaining[choose_group].saturating_sub(1);
            sparse_remaining_total = sparse_remaining_total.saturating_sub(1);
            sparse_assigned_edges[choose_group] += edge_cost;
            sparse_assigned_edges_total += edge_cost;
        }
    }

    let dense_group_edge_times = dense_assigned_edges
        .iter()
        .zip(dense_throughput.iter())
        .map(|(edges, tp)| edges / tp.max(1e-9))
        .collect::<Vec<_>>();
    let sparse_group_edge_times = sparse_assigned_edges
        .iter()
        .zip(sparse_throughput.iter())
        .map(|(edges, tp)| edges / tp.max(1e-9))
        .collect::<Vec<_>>();
    let dense_group_fill = dense_partitions
        .iter()
        .map(|parts| {
            parts.iter().sum::<usize>() as f64
                / (LITTLE_GROUP_DST_CAPACITY * partition_number).max(1) as f64
        })
        .collect::<Vec<_>>();
    let sparse_group_fill = sparse_partitions
        .iter()
        .map(|parts| {
            parts.iter().sum::<usize>() as f64
                / (BIG_GROUP_DST_CAPACITY * partition_number).max(1) as f64
        })
        .collect::<Vec<_>>();
    let dense_partition_fill = (0..partition_number)
        .map(|part| {
            dense_partitions
                .iter()
                .map(|group_parts| group_parts[part])
                .sum::<usize>() as f64
                / (dense_groups * LITTLE_GROUP_DST_CAPACITY).max(1) as f64
        })
        .collect::<Vec<_>>();
    let sparse_partition_fill = (0..partition_number)
        .map(|part| {
            sparse_partitions
                .iter()
                .map(|group_parts| group_parts[part])
                .sum::<usize>() as f64
                / (sparse_groups * BIG_GROUP_DST_CAPACITY).max(1) as f64
        })
        .collect::<Vec<_>>();
    let dense_output_words = dense_partitions
        .iter()
        .map(|parts| {
            parts
                .iter()
                .map(|count| ceil_div_usize(*count, 16) as f64)
                .sum::<f64>()
        })
        .collect::<Vec<_>>();
    let sparse_output_words = sparse_partitions
        .iter()
        .map(|parts| {
            parts
                .iter()
                .map(|count| ceil_div_usize(*count, 16) as f64)
                .sum::<f64>()
        })
        .collect::<Vec<_>>();
    let dense_active_partitions = dense_partitions
        .iter()
        .flat_map(|parts| parts.iter())
        .filter(|count| **count > 0)
        .count();
    let sparse_active_partitions = sparse_partitions
        .iter()
        .flat_map(|parts| parts.iter())
        .filter(|count| **count > 0)
        .count();

    PartitionSimulation {
        partition_number,
        dense_est_ms: dense_assigned_edges_total / dense_total_throughput,
        sparse_est_ms: sparse_assigned_edges_total / sparse_total_throughput,
        dense_group_edge_times,
        sparse_group_edge_times,
        dense_group_fill,
        sparse_group_fill,
        dense_partition_fill,
        sparse_partition_fill,
        dense_output_words,
        sparse_output_words,
        dense_active_partitions,
        sparse_active_partitions,
    }
}

fn assign_vertex_to_first_open_partition(parts: &mut [usize], cap: usize) -> bool {
    for count in parts.iter_mut() {
        if *count < cap {
            *count += 1;
            return true;
        }
    }
    false
}

fn choose_group_by_projected_ms(
    remaining: &[usize],
    assigned_edges: &[f64],
    throughput: &[f64],
    edge_cost: f64,
) -> usize {
    let mut best_group = usize::MAX;
    let mut best_score = f64::INFINITY;
    for idx in 0..remaining.len() {
        if remaining[idx] == 0 {
            continue;
        }
        let projected_ms = (assigned_edges[idx] + edge_cost) / throughput[idx].max(1e-9);
        if best_group == usize::MAX
            || projected_ms < best_score - 1e-12
            || ((projected_ms - best_score).abs() <= 1e-12
                && remaining[idx] > remaining[best_group])
        {
            best_group = idx;
            best_score = projected_ms;
        }
    }
    best_group
}

fn ceil_div_usize(n: usize, d: usize) -> usize {
    if d == 0 { 0 } else { (n + d - 1) / d }
}

fn collect_candidate_shapes(runs: &[ProfileRun]) -> Vec<GroupingShape> {
    let observed: BTreeSet<_> = runs.iter().map(|run| run.variant.clone()).collect();
    let mut library = load_variant_shape_library()
        .map(|lib| {
            lib.into_values()
                .filter(|shape| observed.contains(&shape.variant))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if library.is_empty() {
        library = runs
            .iter()
            .map(|run| run.shape.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
    library.sort_by(|lhs, rhs| lhs.variant.cmp(&rhs.variant));
    library
}

fn dominant_pipeline_total(runs: &[ProfileRun]) -> usize {
    let mut counts = BTreeMap::<usize, usize>::new();
    for run in runs {
        *counts.entry(run.shape.total_pipelines()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|lhs, rhs| lhs.1.cmp(&rhs.1).then_with(|| rhs.0.cmp(&lhs.0)))
        .map(|item| item.0)
        .unwrap_or(14)
}

fn dataset_truths(
    runs: &[ProfileRun],
    dataset_profiles: &BTreeMap<String, Arc<GraphPartitionProfile>>,
) -> Vec<DatasetTruth> {
    let mut winners = BTreeMap::<String, &ProfileRun>::new();
    for run in runs {
        match winners.get(&run.dataset) {
            Some(current) if current.total_mteps >= run.total_mteps => {}
            _ => {
                winners.insert(run.dataset.clone(), run);
            }
        }
    }

    winners
        .into_iter()
        .filter_map(|(dataset, run)| {
            dataset_profiles
                .get(&dataset)
                .cloned()
                .map(|profile| DatasetTruth {
                    dataset,
                    profile,
                    winner_variant: run.variant.clone(),
                    winner_family: run.family.clone(),
                })
        })
        .collect()
}

fn load_variant_shape_library() -> Result<BTreeMap<String, GroupingShape>, GroupingPredictorError> {
    let mut library = BTreeMap::new();
    for candidate in VARIANT_METADATA_CANDIDATES {
        let path = Path::new(candidate);
        if !path.exists() {
            continue;
        }
        let body = fs::read_to_string(path).map_err(|source| GroupingPredictorError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        for (idx, line) in body.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            if let Some(shape) = parse_variant_metadata_row(line) {
                library.insert(shape.variant.clone(), shape);
            }
        }
    }

    if library.is_empty() {
        Err(GroupingPredictorError::NoVariantMetadata)
    } else {
        Ok(library)
    }
}

fn parse_variant_metadata_row(line: &str) -> Option<GroupingShape> {
    let parts: Vec<_> = line.split(',').collect();
    if parts.len() < 7 {
        return None;
    }
    let variant = normalize_variant_name(parts[0].trim());
    let family = variant.split('_').next()?.to_string();
    let big_groups = parse_group_parts(parts[5].trim())?;
    let little_groups = parse_group_parts(parts[6].trim())?;
    Some(GroupingShape {
        variant,
        family,
        big_groups,
        little_groups,
    })
}

fn parse_group_parts(raw: &str) -> Option<Vec<usize>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for item in trimmed.split('-') {
        parts.push(item.parse::<usize>().ok()?);
    }
    Some(parts)
}

fn infer_variant_and_dataset(path: &Path) -> Option<(String, String)> {
    let name = path.file_name()?.to_str()?;
    if name.contains("__") {
        let mut parts = name.splitn(3, "__");
        let variant = parts.next()?.to_string();
        let dataset = parts.next()?.to_string();
        return Some((variant, dataset));
    }

    let parent = path.parent()?.file_name()?.to_str()?;
    if parent.ends_with("_profiled") && name.starts_with("rmat-") && name.contains("_hw_") {
        return Some((parent.to_string(), name.to_string()));
    }
    None
}

fn normalize_variant_name(raw: &str) -> String {
    raw.trim_end_matches("_profiled").to_string()
}

fn knn_score_for(
    metadata: &GraphMetadata,
    scaler: &Standardizer,
    examples: &[&TrainingExample],
    exclude_dataset: Option<&str>,
    neighbor_count: usize,
) -> Option<f64> {
    let mut items: Vec<_> = examples
        .iter()
        .filter(|item| exclude_dataset != Some(item.run.dataset.as_str()))
        .map(|item| {
            (
                metadata_distance_standardized(metadata, &item.profile.metadata, scaler),
                item.relative_score,
            )
        })
        .collect();
    if items.is_empty() {
        return None;
    }
    items.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));
    let k = neighbor_count.max(1).min(items.len());
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;
    for (distance, score) in items.into_iter().take(k) {
        let weight = 1.0 / (0.05 + distance);
        weighted_sum += weight * score;
        total_weight += weight;
    }
    if total_weight <= f64::EPSILON {
        None
    } else {
        Some(weighted_sum / total_weight)
    }
}

fn knn_score_for_saved(
    metadata: &GraphMetadata,
    scaler: &Standardizer,
    examples: &[&StaticTrainingExample],
    exclude_dataset: Option<&str>,
    neighbor_count: usize,
) -> Option<f64> {
    let mut items: Vec<_> = examples
        .iter()
        .filter(|item| exclude_dataset != Some(item.dataset.as_str()))
        .map(|item| {
            (
                metadata_distance_standardized(metadata, &item.metadata, scaler),
                item.relative_score,
            )
        })
        .collect();
    if items.is_empty() {
        return None;
    }
    items.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap_or(Ordering::Equal));
    let k = neighbor_count.max(1).min(items.len());
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;
    for (distance, score) in items.into_iter().take(k) {
        let weight = 1.0 / (0.05 + distance);
        weighted_sum += weight * score;
        total_weight += weight;
    }
    if total_weight <= f64::EPSILON {
        None
    } else {
        Some(weighted_sum / total_weight)
    }
}

fn average_model_throughput(model: &StaticGroupingModel) -> ThroughputAverages {
    if model.dataset_throughput_profiles.is_empty() {
        return ThroughputAverages {
            big_per_pipe: 1.0,
            little_per_pipe: 1.0,
        };
    }
    let count = model.dataset_throughput_profiles.len() as f64;
    let big_sum = model
        .dataset_throughput_profiles
        .iter()
        .map(|item| item.throughput.big_per_pipe)
        .sum::<f64>();
    let little_sum = model
        .dataset_throughput_profiles
        .iter()
        .map(|item| item.throughput.little_per_pipe)
        .sum::<f64>();
    ThroughputAverages {
        big_per_pipe: big_sum / count,
        little_per_pipe: little_sum / count,
    }
}

fn known_dataset_best_groupings(runs: &[ProfileRun]) -> Vec<KnownDatasetBestGrouping> {
    let mut winners = BTreeMap::<String, &ProfileRun>::new();
    for run in runs {
        match winners.get(&run.dataset) {
            Some(current) if current.total_mteps >= run.total_mteps => {}
            _ => {
                winners.insert(run.dataset.clone(), run);
            }
        }
    }
    winners
        .into_iter()
        .map(|(dataset, run)| KnownDatasetBestGrouping {
            dataset,
            source: run.path.clone(),
            variant: run.variant.clone(),
            family: run.family.clone(),
            big_groups: run.shape.big_groups.clone(),
            little_groups: run.shape.little_groups.clone(),
            total_mteps: run.total_mteps,
        })
        .collect()
}

fn blend_scores(
    blend_model: &BlendModel,
    linear_score: f64,
    ranking_score: f64,
    variant_knn_score: Option<f64>,
    family_knn_score: Option<f64>,
    precise_partition_score: Option<f64>,
) -> f64 {
    let learned = blend_model.predict(
        linear_score,
        ranking_score,
        variant_knn_score,
        family_knn_score,
    );
    match precise_partition_score {
        Some(precise) => 0.35 * learned + 0.65 * precise,
        None => learned,
    }
}

fn fit_standardizer(rows: &[Vec<f64>]) -> Standardizer {
    let feature_len = rows.first().map(|row| row.len()).unwrap_or(0);
    if feature_len == 0 {
        return Standardizer {
            mean: Vec::new(),
            scale: Vec::new(),
        };
    }

    let mut mean = vec![0.0; feature_len];
    for row in rows {
        for (dst, value) in mean.iter_mut().zip(row.iter()) {
            *dst += *value;
        }
    }
    for value in &mut mean {
        *value /= rows.len() as f64;
    }

    let mut scale = vec![1.0; feature_len];
    for idx in 1..feature_len {
        let variance = rows
            .iter()
            .map(|row| {
                let delta = row[idx] - mean[idx];
                delta * delta
            })
            .sum::<f64>()
            / rows.len() as f64;
        scale[idx] = variance.sqrt().max(1e-6);
    }
    mean[0] = 0.0;
    scale[0] = 1.0;

    Standardizer { mean, scale }
}

impl Standardizer {
    fn transform(&self, row: &[f64]) -> Vec<f64> {
        standardize_row(row, &self.mean, &self.scale)
    }
}

fn standardize_row(row: &[f64], mean: &[f64], scale: &[f64]) -> Vec<f64> {
    row.iter()
        .enumerate()
        .map(|(idx, value)| {
            if idx == 0 {
                *value
            } else {
                (*value - mean[idx]) / scale[idx]
            }
        })
        .collect()
}

fn metadata_distance_standardized(
    lhs: &GraphMetadata,
    rhs: &GraphMetadata,
    scaler: &Standardizer,
) -> f64 {
    let left = scaler.transform(&metadata_feature_row(lhs));
    let right = scaler.transform(&metadata_feature_row(rhs));

    let mut sum = 0.0;
    let mut count = 0usize;
    for idx in 1..left.len() {
        let delta = left[idx] - right[idx];
        sum += delta * delta;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f64).sqrt()
    }
}

fn metadata_feature_row(metadata: &GraphMetadata) -> Vec<f64> {
    let mut row = vec![1.0];
    row.extend(metadata.feature_vector());
    row
}

fn dot(lhs: &[f64], rhs: &[f64]) -> f64 {
    lhs.iter().zip(rhs.iter()).map(|(a, b)| a * b).sum()
}

fn fit_ridge_weights(rows: &[Vec<f64>], targets: &[f64], lambda: f64) -> Vec<f64> {
    let feature_len = rows.first().map(|row| row.len()).unwrap_or(1);
    if rows.is_empty() {
        return vec![0.0; feature_len];
    }

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

    let n = rows.len().max(1) as f64;
    for i in 0..feature_len {
        for j in 0..feature_len {
            xtx[i][j] /= n;
        }
        xty[i] /= n;
        if i != 0 {
            xtx[i][i] += lambda;
        }
    }

    solve_linear_system(&xtx, &xty).unwrap_or_else(|| vec![0.0; feature_len])
}

fn solve_linear_system(matrix: &[Vec<f64>], rhs: &[f64]) -> Option<Vec<f64>> {
    let n = matrix.len();
    if n == 0 || rhs.len() != n || matrix.iter().any(|row| row.len() != n) {
        return None;
    }

    let mut aug = vec![vec![0.0; n + 1]; n];
    for i in 0..n {
        for j in 0..n {
            aug[i][j] = matrix[i][j];
        }
        aug[i][n] = rhs[i];
    }

    for col in 0..n {
        let mut pivot = col;
        let mut pivot_abs = aug[col][col].abs();
        for row in (col + 1)..n {
            let value_abs = aug[row][col].abs();
            if value_abs > pivot_abs {
                pivot = row;
                pivot_abs = value_abs;
            }
        }
        if pivot_abs <= 1e-12 {
            return None;
        }
        if pivot != col {
            aug.swap(pivot, col);
        }

        let pivot_value = aug[col][col];
        for j in col..=n {
            aug[col][j] /= pivot_value;
        }

        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = aug[row][col];
            if factor.abs() <= 1e-12 {
                continue;
            }
            for j in col..=n {
                aug[row][j] -= factor * aug[col][j];
            }
        }
    }

    Some((0..n).map(|idx| aug[idx][n]).collect())
}

fn ratio(num: usize, denom: usize) -> f64 {
    if denom == 0 {
        0.0
    } else {
        num as f64 / denom as f64
    }
}

fn mean_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn max_f64(values: &[f64]) -> f64 {
    values.iter().copied().fold(0.0, f64::max)
}

fn coeff_var_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = mean_f64(values);
    if mean <= f64::EPSILON {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

fn coeff_var_usize(values: &[usize]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<usize>() as f64 / values.len() as f64;
    if mean <= f64::EPSILON {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value as f64 - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

fn normalized_entropy(values: &[usize]) -> f64 {
    let total = values.iter().sum::<usize>() as f64;
    if total <= f64::EPSILON || values.len() <= 1 {
        return 0.0;
    }
    let entropy = values
        .iter()
        .filter(|value| **value > 0)
        .map(|value| {
            let p = *value as f64 / total;
            -p * p.ln()
        })
        .sum::<f64>();
    entropy / (values.len() as f64).ln().max(1e-9)
}

fn max_share(values: &[usize]) -> f64 {
    let total = values.iter().sum::<usize>() as f64;
    if total <= f64::EPSILON {
        return 0.0;
    }
    *values.iter().max().unwrap_or(&0) as f64 / total
}

fn bool_score(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}

fn walk_logs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), GroupingPredictorError> {
    let entries = fs::read_dir(dir).map_err(|source| GroupingPredictorError::ReadFile {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| GroupingPredictorError::ReadFile {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            walk_logs(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
            out.push(path);
        }
    }
    Ok(())
}

fn extract_number_after(line: &str, marker: &str) -> Option<f64> {
    let start = line.find(marker)? + marker.len();
    let tail = &line[start..];
    let end = tail
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(tail.len());
    tail[..end].parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_variant_metadata_rows_into_shapes() {
        let row = "c4l10_n2,4b_10l,4,10,14,2-2,6-4,/tmp/c4l10_n2";
        let shape = parse_variant_metadata_row(row).expect("shape");
        assert_eq!(shape.variant, "c4l10_n2");
        assert_eq!(shape.family, "c4l10");
        assert_eq!(shape.big_groups, vec![2, 2]);
        assert_eq!(shape.little_groups, vec![6, 4]);
    }

    #[test]
    fn normalizes_profiled_variant_names() {
        assert_eq!(normalize_variant_name("c2l12_v1_profiled"), "c2l12_v1");
        assert_eq!(normalize_variant_name("c5l9_b1l3"), "c5l9_b1l3");
    }

    #[test]
    fn grouping_features_reflect_balance() {
        let balanced = GroupingShape {
            variant: "c4l10_n2".to_string(),
            family: "c4l10".to_string(),
            big_groups: vec![2, 2],
            little_groups: vec![5, 5],
        };
        let skewed = GroupingShape {
            variant: "c4l10_n1".to_string(),
            family: "c4l10".to_string(),
            big_groups: vec![4],
            little_groups: vec![6, 4],
        };

        let balanced_features = balanced.feature_vector();
        let skewed_features = skewed.feature_vector();
        assert!(balanced_features[16] < skewed_features[16]);
        assert!(balanced_features[18] > skewed_features[18]);
    }
}
