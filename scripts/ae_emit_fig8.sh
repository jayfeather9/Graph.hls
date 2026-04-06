#!/usr/bin/env bash
# =============================================================================
# ae_emit_fig8.sh — Emit HLS projects for Figure 8 (Graph.hls vs ThunderGP, U200)
#
# Algorithms: PageRank (PR), Weighted SSSP, CC, ArticleRank (AR), WCC
# Platform: Alveo U200 (DDR), 4 big 4 little, 32bit
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET_DIR="/path/to/datasets"
OUTPUT_ROOT="${REPO_ROOT}/target/ae/fig8"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset-dir)  DATASET_DIR="$2"; shift 2 ;;
        --output-root)  OUTPUT_ROOT="$2"; shift 2 ;;
        *)              echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

DATASET_DIR="$(cd "$DATASET_DIR" && pwd)"
mkdir -p "$OUTPUT_ROOT"
OUTPUT_ROOT="$(cd "$OUTPUT_ROOT" && pwd)"

BINARY="${REPO_ROOT}/target/release/refactor_Graphyflow"
if [[ ! -x "$BINARY" ]]; then
    echo "Building compiler..."
    (cd "$REPO_ROOT" && cargo build --release)
fi

# DSL files: 4B4L, 32bit, DDR
DSL_DIR="${REPO_ROOT}/apps/topology_variants"

declare -A ALGO_DSL=(
    [sssp]="${DSL_DIR}/sssp_ddr_4b4l_codegen.dsl"
    [cc]="${DSL_DIR}/cc_bitmask_ddr_4b4l.dsl"
    [pr]="${DSL_DIR}/pr_ddr_4b4l.dsl"
    [ar]="${DSL_DIR}/ar_ddr_4b4l.dsl"
    [wcc]="${DSL_DIR}/wcc_ddr_4b4l.dsl"
)

# Per-algorithm kernel frequency for DDR (from reference builds)
declare -A ALGO_FREQ=(
    [sssp]=240
    [cc]=240
    [pr]=220
    [ar]=230
    [wcc]=230
)

echo "=== Fig 8: Graph.hls vs ThunderGP (U200, DDR, 4B4L, 32bit) ==="
echo "Output: $OUTPUT_ROOT"
echo ""

for algo in sssp cc pr ar wcc; do
    dsl="${ALGO_DSL[$algo]}"
    project_dir="$OUTPUT_ROOT/$algo"

    if [[ ! -f "$dsl" ]]; then
        echo "  WARNING: DSL not found: $dsl"
        continue
    fi

    if [[ -d "$project_dir" ]]; then
        echo "  [$algo] Already exists, skipping."
    else
        echo "  [$algo] Emitting from $(basename "$dsl")..."
        "$BINARY" --emit-hls "$dsl" "$project_dir"

        # Patch kernel frequency for DDR timing closure
        freq="${ALGO_FREQ[$algo]}"
        mk="$project_dir/scripts/kernel/kernel.mk"
        if [[ -f "$mk" && -n "$freq" ]]; then
            sed -i "s/^KERNEL_FREQ ?= .*/KERNEL_FREQ ?= $freq/" "$mk"
            echo "  [$algo] Patched KERNEL_FREQ=$freq in kernel.mk"
        fi
    fi
done

echo ""
echo "=== Fig 8 Emission Summary ==="
echo "  SSSP (weighted) : $OUTPUT_ROOT/sssp"
echo "  CC              : $OUTPUT_ROOT/cc"
echo "  PageRank        : $OUTPUT_ROOT/pr"
echo "  ArticleRank     : $OUTPUT_ROOT/ar"
echo "  WCC             : $OUTPUT_ROOT/wcc"
echo "  Total: 5 HLS projects"
echo ""

# Build list
BUILD_LIST="$OUTPUT_ROOT/build_list.txt"
: > "$BUILD_LIST"
for algo in sssp cc pr ar wcc; do
    echo "$OUTPUT_ROOT/$algo" >> "$BUILD_LIST"
done
echo "Build list: $BUILD_LIST"
echo ""
echo "=== ThunderGP Baseline Setup ==="
echo ""
echo "Figure 8 compares Graph.hls against ThunderGP on U200 (DDR)."
echo "ThunderGP must be built and run separately."
echo ""
echo "The artifact ships 5 pre-configured ThunderGP copies (one per app):"
echo "  ThunderGP_sssp/  ThunderGP_cc/  ThunderGP_pr/  ThunderGP_ar/  ThunderGP_wcc/"
echo ""
echo "ThunderGP build (per app):"
echo "  1. Source Vitis/XRT environment"
echo "  2. cd ThunderGP_<app>"
echo "  3. make app=<app> all"
echo "     Expected outputs:"
echo "       host_graph_fpga_<app>"
echo "       xclbin_<app>/graph_fpga.hw.<platform>.xclbin"
echo ""
echo "ThunderGP run (per app, all datasets):"
echo "  cd ThunderGP_<app>"
echo "  python3 benchmark.py"
echo "  (edit DATASETS list in benchmark.py to point to your dataset directory)"
echo "  Output: benchmark_<app>_<timestamp>.csv with columns:"
echo "    APP_MODE, Dataset, Total_Edges, Total_E2E_Time_ms, MTEPS"
echo ""
echo "Or run all 5 apps at once:"
echo "  cd ThunderGP_all && bash run_all.sh"
echo ""
echo "ThunderGP app name mapping (matches our names directly):"
echo "  thundergp sssp -> our sssp (weighted)"
echo "  thundergp cc   -> our cc"
echo "  thundergp pr   -> our pr"
echo "  thundergp ar   -> our ar"
echo "  thundergp wcc  -> our wcc"
