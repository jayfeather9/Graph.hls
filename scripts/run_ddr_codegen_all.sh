#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_ddr_codegen_all.sh <sw_emu|hw_emu|hw> [dataset_dir] [options]

Run the five DDR generated projects sequentially without Docker for every
dataset file found directly under the chosen dataset directory.

Positional arguments:
  <mode>               sw_emu | hw_emu | hw
  [dataset_dir]        Folder containing dataset files
                       Absolute or repo-relative path
                       (default: /path/to/datasets)

Batch options:
  --output-dir <path>  Root folder for all per-run logs
                       (default: target/parallel_logs/ddr_codegen_all_<timestamp>)

Forwarded build/run options:
  --iters <n>
  --timeout <seconds>
  --device <path>
  --device-bdf <bdf>
  --device-index <n>
  --kernel-freq <mhz>
  --big-edge-per-ms <n>
  --little-edge-per-ms <n>
  --rebuild-exe
  --build-kernels
  --build-only
  --env <path>
  -h, --help

Notes:
  The batch runner applies dataset-specific BIG_EDGE_PER_MS and
  LITTLE_EDGE_PER_MS defaults based on the dataset filename. Explicit
  --big-edge-per-ms / --little-edge-per-ms flags override those defaults for
  all datasets.

Examples:
  scripts/run_ddr_codegen_all.sh sw_emu
  scripts/run_ddr_codegen_all.sh hw_emu /path/to/datasets --build-kernels --timeout 3600
  scripts/run_ddr_codegen_all.sh hw /path/to/datasets --output-dir target/parallel_logs/ddr_hw --build-kernels --timeout 7200
USAGE
}

resolve_dataset_config() {
  case "$1" in
    graph500-scale23-ef16_adj.mtx) printf '%s\t%s\t%s\n' "graph500" "400000" "1000000" ;;
    rmat-19-32.txt) printf '%s\t%s\t%s\n' "r19" "250000" "950000" ;;
    rmat-21-32.txt) printf '%s\t%s\t%s\n' "r21" "290000" "1000000" ;;
    rmat-24-16.txt) printf '%s\t%s\t%s\n' "r24" "270000" "1000000" ;;
    amazon-2008.mtx) printf '%s\t%s\t%s\n' "am" "160000" "460000" ;;
    ca-hollywood-2009.mtx) printf '%s\t%s\t%s\n' "hollywood" "300000" "1000000" ;;
    dbpedia-link.mtx) printf '%s\t%s\t%s\n' "dbpedia" "190000" "900000" ;;
    soc-flickr-und.mtx) printf '%s\t%s\t%s\n' "flickr" "120000" "800000" ;;
    soc-LiveJournal1.txt) printf '%s\t%s\t%s\n' "LiveJournal1" "170000" "700000" ;;
    soc-orkut-dir.mtx) printf '%s\t%s\t%s\n' "orkut" "280000" "850000" ;;
    web-baidu-baike.mtx) printf '%s\t%s\t%s\n' "baidu" "160000" "800000" ;;
    web-Google.mtx) printf '%s\t%s\t%s\n' "Google" "150000" "580000" ;;
    web-hudong.mtx) printf '%s\t%s\t%s\n' "hudong" "180000" "850000" ;;
    wiki-topcats.txt) printf '%s\t%s\t%s\n' "topcats" "170000" "830000" ;;
    *) return 1 ;;
  esac
}

is_positive_int() {
  [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -ge 1 ]]
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_DATASET_DIR="/path/to/datasets"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

MODE="$1"
shift

case "$MODE" in
  sw_emu|hw_emu|hw)
    ;;
  *)
    echo "mode must be sw_emu, hw_emu, or hw (got '$MODE')" >&2
    usage >&2
    exit 2
    ;;
esac

DATASET_DIR="$DEFAULT_DATASET_DIR"
if [[ $# -gt 0 && "$1" != --* ]]; then
  DATASET_DIR="$1"
  shift
fi

OUTPUT_DIR=""
GLOBAL_BIG_EDGE_PER_MS=""
GLOBAL_LITTLE_EDGE_PER_MS=""
FORWARDED_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="${2:?}"
      shift 2
      ;;
    --big-edge-per-ms)
      GLOBAL_BIG_EDGE_PER_MS="${2:?}"
      shift 2
      ;;
    --little-edge-per-ms)
      GLOBAL_LITTLE_EDGE_PER_MS="${2:?}"
      shift 2
      ;;
    --iters|--timeout|--device|--device-bdf|--device-index|--kernel-freq|--env)
      FORWARDED_ARGS+=( "$1" "${2:?}" )
      shift 2
      ;;
    --rebuild-exe|--build-kernels|--build-only)
      FORWARDED_ARGS+=( "$1" )
      shift
      ;;
    --project|--mode|--graph|--log-dir)
      echo "$1 is managed internally by scripts/run_ddr_codegen_all.sh" >&2
      exit 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$DATASET_DIR" != /* ]]; then
  DATASET_DIR="$REPO_ROOT/$DATASET_DIR"
fi
if [[ ! -d "$DATASET_DIR" ]]; then
  echo "dataset directory not found: $DATASET_DIR" >&2
  exit 1
fi

if [[ -n "$OUTPUT_DIR" && "$OUTPUT_DIR" != /* ]]; then
  OUTPUT_DIR="$REPO_ROOT/$OUTPUT_DIR"
fi
if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$REPO_ROOT/target/parallel_logs/ddr_codegen_all_$(date +%Y%m%d_%H%M%S)"
fi

if [[ -n "$GLOBAL_BIG_EDGE_PER_MS" ]] && ! is_positive_int "$GLOBAL_BIG_EDGE_PER_MS"; then
  echo "--big-edge-per-ms must be an integer >= 1 (got '$GLOBAL_BIG_EDGE_PER_MS')" >&2
  exit 2
fi
if [[ -n "$GLOBAL_LITTLE_EDGE_PER_MS" ]] && ! is_positive_int "$GLOBAL_LITTLE_EDGE_PER_MS"; then
  echo "--little-edge-per-ms must be an integer >= 1 (got '$GLOBAL_LITTLE_EDGE_PER_MS')" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

PROJECTS=(
  sssp_ddr_4b4l_codegen
  cc_bitmask_ddr_4b4l
  wcc_ddr_4b4l
  pr_ddr_4b4l
  ar_ddr_4b4l
)

mapfile -t DATASET_FILES < <(find "$DATASET_DIR" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)

if [[ "${#DATASET_FILES[@]}" -eq 0 ]]; then
  echo "no dataset files found under: $DATASET_DIR" >&2
  exit 1
fi

TOTAL_DATASETS="${#DATASET_FILES[@]}"
TOTAL_RUNS=0

echo "==> mode=$MODE"
echo "==> dataset_dir=$DATASET_DIR"
echo "==> output_dir=$OUTPUT_DIR"
echo "==> datasets=$TOTAL_DATASETS projects=${#PROJECTS[@]}"

for dataset_file in "${DATASET_FILES[@]}"; do
  if ! dataset_config="$(resolve_dataset_config "$dataset_file")"; then
    echo "unknown dataset filename in $DATASET_DIR: $dataset_file" >&2
    exit 1
  fi

  IFS=$'\t' read -r dataset_name default_big default_little <<<"$dataset_config"
  dataset_path="$DATASET_DIR/$dataset_file"
  big_edge_per_ms="$default_big"
  little_edge_per_ms="$default_little"
  dataset_log_dir="$OUTPUT_DIR/$dataset_name"

  if [[ -n "$GLOBAL_BIG_EDGE_PER_MS" ]]; then
    big_edge_per_ms="$GLOBAL_BIG_EDGE_PER_MS"
  fi
  if [[ -n "$GLOBAL_LITTLE_EDGE_PER_MS" ]]; then
    little_edge_per_ms="$GLOBAL_LITTLE_EDGE_PER_MS"
  fi

  mkdir -p "$dataset_log_dir"

  echo "==== dataset=$dataset_name file=$dataset_file big=$big_edge_per_ms little=$little_edge_per_ms"

  for project in "${PROJECTS[@]}"; do
    echo "==== project=$project mode=$MODE dataset=$dataset_name"
    "$SCRIPT_DIR/run_target_one.sh" \
      --project "$project" \
      --mode "$MODE" \
      --graph "$dataset_path" \
      --log-dir "$dataset_log_dir" \
      --big-edge-per-ms "$big_edge_per_ms" \
      --little-edge-per-ms "$little_edge_per_ms" \
      "${FORWARDED_ARGS[@]}"
    TOTAL_RUNS=$((TOTAL_RUNS + 1))
  done
done

echo "All DDR generated projects passed."
echo "  mode=$MODE"
echo "  dataset_dir=$DATASET_DIR"
echo "  output_dir=$OUTPUT_DIR"
echo "  datasets=$TOTAL_DATASETS"
echo "  runs=$TOTAL_RUNS"
