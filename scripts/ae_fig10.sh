#!/usr/bin/env bash
# =============================================================================
# ae_fig10.sh — Run SG-Scope simulation and plot speedup over C-Sim (Figure 10)
#
# Usage:
#   ./scripts/ae_fig10.sh [--dataset-dir <path>] [--output-dir <path>]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET_DIR="/path/to/datasets"
OUTPUT_DIR="${REPO_ROOT}/target/ae/fig10"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset-dir) DATASET_DIR="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2"; shift 2 ;;
        *)             echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

BINARY="${REPO_ROOT}/target/release/refactor_Graphyflow"
if [[ ! -x "$BINARY" ]]; then
    echo "Building compiler..."
    (cd "$REPO_ROOT" && cargo build --release)
fi

SIM_CSV="$OUTPUT_DIR/sim_results.csv"
BASELINE_CSV="${REPO_ROOT}/data/swemu_baseline.csv"
OUTPUT_PDF="$OUTPUT_DIR/fig10_sgscope_speedup.pdf"

echo "=== Fig 10: SG-Scope Simulation Speedup ==="
echo "  Datasets:  $DATASET_DIR"
echo "  Output:    $OUTPUT_DIR"
echo ""

# Step 1: Run simulation across all datasets for PR, CC, SSSP
echo "--- Step 1: Running batch simulation ---"
"$SCRIPT_DIR/batch_simulate.sh" --format raw --mode batch \
    --dataset "$DATASET_DIR" \
    --dsl "sssp,pagerank,connected_components" \
    --output "$SIM_CSV"

echo ""
echo "Simulation results: $SIM_CSV"
echo ""

# Step 2: Plot speedup over sw_emu baseline
echo "--- Step 2: Plotting speedup ---"
python3 "$SCRIPT_DIR/plot_sim_results.py" \
    --sim-csv "$SIM_CSV" \
    --dsls "sssp,pagerank,connected_components" \
    --y-mode speedup \
    --baseline-csv "$BASELINE_CSV" \
    --baseline-cols "sssp_iter0_ms,pr_iter0_ms,cc_iter0_ms" \
    --baseline-unit ms \
    --short-names "${REPO_ROOT}/data/dataset_short_names.csv" \
    --output "$OUTPUT_PDF"

echo ""
echo "=== Done ==="
echo "  CSV:  $SIM_CSV"
echo "  PDF:  $OUTPUT_PDF"
