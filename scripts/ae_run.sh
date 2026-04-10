#!/usr/bin/env bash
# =============================================================================
# ae_run.sh — Run FPGA experiments sequentially and collect timing results
#
# Usage:
#   ./scripts/ae_run.sh --figure <7|8|9> --dataset-dir <path> [--target hw]
#                       [--output <csv>] [--iters <N>] [--env <script>]
#
# Baselines (Fig 7 / Fig 8):
#   ReGraph and ThunderGP must be built and run separately. Provide their
#   results as a CSV to the plotter (ae_plot.py --baseline-csv).
#
#   ReGraph app name mapping:
#     regraph bfs -> our sssp  (ReGraph 'bfs' implements SSSP/Bellman-Ford)
#     regraph pr  -> our pagerank
#     regraph cc  -> our connected_components
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/ddr_dataset_config.sh"

FIGURE=""
DATASET_DIR="/home/youwei/xuxiao/artifact/datasets"
TARGET="hw"
OUTPUT_CSV=""
MAX_ITERS=32
ENV_SCRIPT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --figure)      FIGURE="$2"; shift 2 ;;
        --dataset-dir) DATASET_DIR="$2"; shift 2 ;;
        --target)      TARGET="$2"; shift 2 ;;
        --output)      OUTPUT_CSV="$2"; shift 2 ;;
        --iters)       MAX_ITERS="$2"; shift 2 ;;
        --env)         ENV_SCRIPT="$2"; shift 2 ;;
        *)             echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$FIGURE" ]]; then
    echo "ERROR: --figure <7|8|9> is required" >&2
    exit 1
fi

AE_ROOT="${REPO_ROOT}/target/ae/fig${FIGURE}"
DATASET_DIR="${DATASET_DIR:-/path/to/datasets}"
OUTPUT_CSV="${OUTPUT_CSV:-${AE_ROOT}/results.csv}"

if [[ -n "$ENV_SCRIPT" && -f "$ENV_SCRIPT" ]]; then
    source "$ENV_SCRIPT"
fi

DATASET_DIR="$(cd "$DATASET_DIR" && pwd)"

echo "=== AE Runner — Figure $FIGURE ==="
echo "  AE root: $AE_ROOT"
echo "  Datasets: $DATASET_DIR"
echo "  Target: $TARGET"
echo "  Output: $OUTPUT_CSV"
echo ""

# ── Enumerate datasets ────────────────────────────────────────────────────────
DATASETS=()
for f in "$DATASET_DIR"/*.txt "$DATASET_DIR"/*.mtx; do
    [[ -f "$f" ]] && DATASETS+=("$f")
done

rebuild_fig8_host_for_dataset() {
    local project_dir="$1"
    local dataset="$2"
    local dataset_file
    local dataset_stem
    local dataset_config
    local dataset_alias
    local big_edge_per_ms
    local little_edge_per_ms
    local rebuild_log

    dataset_file="$(basename "$dataset")"
    dataset_stem="${dataset_file%.*}"

    if ! dataset_config="$(resolve_ddr_dataset_config "$dataset_file")"; then
        echo "ERROR: no Figure 8 DDR dataset configuration for '$dataset_file'" >&2
        return 1
    fi

    IFS=$'\t' read -r dataset_alias big_edge_per_ms little_edge_per_ms <<<"$dataset_config"
    rebuild_log="$project_dir/rebuild_host_${dataset_stem}.log"

    echo "    Rebuilding host for ${dataset_alias}: BIG_EDGE_PER_MS=${big_edge_per_ms} LITTLE_EDGE_PER_MS=${little_edge_per_ms}" >&2

    (
        cd "$project_dir"
        {
            echo "==> dataset=$dataset_file alias=$dataset_alias target=$TARGET"
            echo "==> BIG_EDGE_PER_MS=$big_edge_per_ms"
            echo "==> LITTLE_EDGE_PER_MS=$little_edge_per_ms"
            make cleanexe TARGET="$TARGET"
            make exe TARGET="$TARGET" \
                BIG_EDGE_PER_MS="$big_edge_per_ms" \
                LITTLE_EDGE_PER_MS="$little_edge_per_ms"
        } > "$rebuild_log" 2>&1
    )
}

# ── Run function ──────────────────────────────────────────────────────────────
run_one() {
    local project_dir="$1"
    local dataset="$2"
    local project_name
    project_name="$(basename "$project_dir")"
    local ds_name
    ds_name="$(basename "$dataset" | sed 's/\.\(txt\|mtx\)$//')"

    local xclbin="$project_dir/xclbin/graphyflow_kernels.${TARGET}.xclbin"
    if [[ ! -f "$xclbin" ]]; then
        echo "SKIP: $project_name on $ds_name (missing xclbin)" >&2
        echo "SKIP,$project_name,$ds_name,,,no_xclbin"
        return
    fi

    local run_log="$project_dir/run_${ds_name}.log"

    if [[ "$FIGURE" == "8" ]]; then
        if ! rebuild_fig8_host_for_dataset "$project_dir" "$dataset"; then
            echo "FAILED: $project_name on $ds_name (host rebuild failed)" >&2
            echo "FAIL,$project_name,$ds_name,,,host_rebuild_failed"
            return
        fi
    fi

    echo "  [app=$project_name] dataset=$ds_name target=$TARGET" >&2
    echo -n "    Running ... " >&2

    (
        cd "$project_dir"
        export GRAPHYFLOW_MAX_ITERS="$MAX_ITERS"
        export GRAPHYFLOW_ALLOW_MISMATCH=1
        ./run.sh "$TARGET" "$dataset" 2>&1
    ) > "$run_log" 2>&1
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        echo "FAILED (exit=$exit_code)" >&2
        echo "FAIL,$project_name,$ds_name,,,$exit_code"
        return
    fi

    # Extract timing from log
    local kernel_time
    kernel_time=$(grep -oP 'Total FPGA Kernel Execution Time[^0-9]*\K[0-9.]+' "$run_log" 2>/dev/null | head -1)
    local throughput
    throughput=$(grep -oP 'Total MTEPS \(Edges / Total Time\):\s*\K[0-9.]+' "$run_log" 2>/dev/null | tail -1)

    if [[ -n "$kernel_time" ]]; then
        echo "OK (${kernel_time}ms)" >&2
        echo "OK,$project_name,$ds_name,$kernel_time,$throughput"
    else
        echo "OK (no timing found)" >&2
        echo "OK,$project_name,$ds_name,,"
    fi
}

# ── Figure-specific run logic ─────────────────────────────────────────────────
echo "dataset,algorithm,kernel_time_ms,throughput_mteps,status" > "$OUTPUT_CSV"

case "$FIGURE" in
    7)
        # PR, CC, SSSP on U55C — each algo is a separate project, run on all datasets
        for algo in sssp connected_components pagerank; do
            project_dir="$AE_ROOT/$algo"
            for ds in "${DATASETS[@]}"; do
                ds_name="$(basename "$ds" | sed 's/\.\(txt\|mtx\)$//')"
                result=$(run_one "$project_dir" "$ds")
                status=$(echo "$result" | cut -d, -f1)
                ktime=$(echo "$result" | cut -d, -f4)
                tput=$(echo "$result" | cut -d, -f5)
                echo "$ds_name,$algo,$ktime,$tput,$status" >> "$OUTPUT_CSV"
            done
        done
        ;;
    8)
        # PR, WSSSP, CC, AR, WCC on U200 — each algo is a separate project
        for algo in sssp cc pr ar wcc; do
            project_dir="$AE_ROOT/$algo"
            for ds in "${DATASETS[@]}"; do
                ds_name="$(basename "$ds" | sed 's/\.\(txt\|mtx\)$//')"
                result=$(run_one "$project_dir" "$ds")
                status=$(echo "$result" | cut -d, -f1)
                ktime=$(echo "$result" | cut -d, -f4)
                tput=$(echo "$result" | cut -d, -f5)
                echo "$ds_name,$algo,$ktime,$tput,$status" >> "$OUTPUT_CSV"
            done
        done
        ;;
    9)
        # Ablation study: Naive, L1, L1+L2, L1+L3, L1+L2+L3 — all SSSP
        declare -A CONFIG_DIRS=(
            [naive]="$AE_ROOT/naive"
            [l1]="$AE_ROOT/l1_32bit_3b11l"
            [l1l2]="$AE_ROOT/l1l2_8bit_3b11l"
        )

        # Fixed configs (one project for all datasets)
        for config in naive l1 l1l2; do
            project_dir="${CONFIG_DIRS[$config]}"
            for ds in "${DATASETS[@]}"; do
                ds_name="$(basename "$ds" | sed 's/\.\(txt\|mtx\)$//')"
                result=$(run_one "$project_dir" "$ds")
                status=$(echo "$result" | cut -d, -f1)
                ktime=$(echo "$result" | cut -d, -f4)
                tput=$(echo "$result" | cut -d, -f5)
                echo "$ds_name,$config,$ktime,$tput,$status" >> "$OUTPUT_CSV"
            done
        done

        # L1+L3: per-dataset project (from manifest)
        L1L3_MANIFEST="$AE_ROOT/l1l3_32bit_grouped/manifest.csv"
        if [[ -f "$L1L3_MANIFEST" ]]; then
            tail -n +2 "$L1L3_MANIFEST" | while IFS=, read -r ds_name variant project_dir; do
                [[ "$project_dir" == *FAILED* || -z "$project_dir" ]] && continue
                ds_file=$(find "$DATASET_DIR" -name "${ds_name}.*" | head -1)
                [[ -z "$ds_file" ]] && continue
                result=$(run_one "$project_dir" "$ds_file")
                status=$(echo "$result" | cut -d, -f1)
                ktime=$(echo "$result" | cut -d, -f4)
                tput=$(echo "$result" | cut -d, -f5)
                echo "$ds_name,l1l3,$ktime,$tput,$status" >> "$OUTPUT_CSV"
            done
        fi

        # L1+L2+L3: per-dataset project (from manifest)
        L1L2L3_MANIFEST="$AE_ROOT/l1l2l3_8bit_grouped/manifest.csv"
        if [[ -f "$L1L2L3_MANIFEST" ]]; then
            tail -n +2 "$L1L2L3_MANIFEST" | while IFS=, read -r ds_name variant project_dir; do
                [[ "$project_dir" == *FAILED* || -z "$project_dir" ]] && continue
                ds_file=$(find "$DATASET_DIR" -name "${ds_name}.*" | head -1)
                [[ -z "$ds_file" ]] && continue
                result=$(run_one "$project_dir" "$ds_file")
                status=$(echo "$result" | cut -d, -f1)
                ktime=$(echo "$result" | cut -d, -f4)
                tput=$(echo "$result" | cut -d, -f5)
                echo "$ds_name,l1l2l3,$ktime,$tput,$status" >> "$OUTPUT_CSV"
            done
        fi
        ;;
    *)
        echo "ERROR: Unknown figure: $FIGURE" >&2
        exit 1
        ;;
esac

echo ""
echo "Results written to: $OUTPUT_CSV"
