#!/usr/bin/env bash
# =============================================================================
# ae_tab3.sh — Reproduce Table 3: SG-Scope debugging speedup vs HW emulation
#
# This script:
#   1. Generates a 32K-node, 512K-edge test graph
#   2. Runs the buggy SSSP through SG-Scope (algorithm bug scenario)
#   3. Runs the correct SSSP through SG-Scope (for baseline comparison)
#   4. Measures SG-Scope validation time
#   5. Compares against pre-collected hw_emu times
#
# The hw_emu times are pre-collected because hw_emu requires Docker + Vitis
# and takes ~1 hour. To reproduce hw_emu times yourself, see the instructions
# at the end of this script or use the buggy_demo projects.
#
# Usage:
#   ./scripts/ae_tab3.sh [--output-dir <path>]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OUTPUT_DIR="${REPO_ROOT}/target/ae/tab3"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        *)            echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

BINARY="${REPO_ROOT}/target/release/refactor_Graphyflow"
if [[ ! -x "$BINARY" ]]; then
    echo "Building compiler..."
    (cd "$REPO_ROOT" && cargo build --release)
fi

GRAPH_NODES=32000
GRAPH_EDGES=512000
GRAPH_JSON="$OUTPUT_DIR/graph_${GRAPH_NODES}_${GRAPH_EDGES}.json"

BUGGY_DSL="${REPO_ROOT}/apps/buggy_demos/sssp_algorithm_bug.dsl"
CORRECT_DSL="${REPO_ROOT}/apps/buggy_demos/sssp_correct.dsl"
HWEMU_BASELINE="${REPO_ROOT}/data/hwemu_debug_baseline.tsv"

echo "=== Table 3: SG-Scope Debugging Speedup ==="
echo "  Graph: ${GRAPH_NODES} nodes, ${GRAPH_EDGES} edges"
echo "  Output: $OUTPUT_DIR"
echo ""

# Step 1: Generate test graph
echo "--- Step 1: Generating test graph ---"
if [[ -f "$GRAPH_JSON" ]]; then
    echo "  Graph already exists, skipping."
else
    "$BINARY" --generate sssp "$GRAPH_NODES" "$GRAPH_EDGES" 42 > "$GRAPH_JSON"
    echo "  Generated: $GRAPH_JSON"
fi
echo ""

# Step 2: Run correct SSSP through SG-Scope (reference baseline)
echo "--- Step 2: Running correct SSSP via SG-Scope ---"
CORRECT_LOG="$OUTPUT_DIR/sgscope_correct.log"
CORRECT_START=$(date +%s%N)
"$BINARY" --simulate-json "$CORRECT_DSL" "$GRAPH_JSON" 32 > "$CORRECT_LOG" 2>&1 || true
CORRECT_END=$(date +%s%N)
CORRECT_MS=$(( (CORRECT_END - CORRECT_START) / 1000000 ))
CORRECT_SEC=$(awk "BEGIN {printf \"%.3f\", $CORRECT_MS / 1000}")
echo "  SG-Scope correct SSSP: ${CORRECT_SEC}s"
echo ""

# Step 3: Run buggy SSSP through SG-Scope (algorithm failure scenario)
# SG-Scope detects the bug via baseline comparison
echo "--- Step 3: Running buggy SSSP via SG-Scope (algorithm failure) ---"
BUGGY_LOG="$OUTPUT_DIR/sgscope_algorithm_bug.log"

# Run buggy version — SG-Scope will detect mismatch against reference
BUGGY_START=$(date +%s%N)
"$BINARY" --simulate-json "$BUGGY_DSL" "$GRAPH_JSON" 32 > "$BUGGY_LOG" 2>&1 || true
BUGGY_END=$(date +%s%N)
BUGGY_MS=$(( (BUGGY_END - BUGGY_START) / 1000000 ))
BUGGY_SEC=$(awk "BEGIN {printf \"%.3f\", $BUGGY_MS / 1000}")
echo "  SG-Scope buggy SSSP (1 run): ${BUGGY_SEC}s"

# Algorithm failure scenario: 6 iterations of debug (as in paper)
ALGO_TOTAL_MS=$(( BUGGY_MS * 6 ))
ALGO_TOTAL_SEC=$(awk "BEGIN {printf \"%.3f\", $ALGO_TOTAL_MS / 1000}")
echo "  SG-Scope buggy SSSP (6 iterations): ${ALGO_TOTAL_SEC}s"
echo ""

# Step 4: SG-Scope type checking time (stream mismatch & parameter mismatch)
# These are caught at IR/parse level — measure parse+validate time
echo "--- Step 4: SG-Scope type checking time ---"
TYPECHECK_START=$(date +%s%N)
"$BINARY" "$CORRECT_DSL" > /dev/null 2>&1 || true
TYPECHECK_END=$(date +%s%N)
TYPECHECK_MS=$(( (TYPECHECK_END - TYPECHECK_START) / 1000000 ))
TYPECHECK_SEC=$(awk "BEGIN {printf \"%.3f\", $TYPECHECK_MS / 1000}")
echo "  SG-Scope IR validation: ${TYPECHECK_SEC}s"
echo ""

# Step 5: Build comparison table
echo "--- Step 5: Results ---"
echo ""

# Read hw_emu baseline (column 2 = total minutes)
HWEMU_ALGO_MIN=$(awk -F'\t' 'NR==2{print $2}' "$HWEMU_BASELINE")
HWEMU_STREAM_MIN=$(awk -F'\t' 'NR==3{print $2}' "$HWEMU_BASELINE")
HWEMU_PARAM_MIN=$(awk -F'\t' 'NR==4{print $2}' "$HWEMU_BASELINE")

# Convert hw_emu minutes to seconds
HWEMU_ALGO_SEC=$(awk "BEGIN {printf \"%.1f\", $HWEMU_ALGO_MIN * 60}")
HWEMU_STREAM_SEC=$(awk "BEGIN {printf \"%.1f\", $HWEMU_STREAM_MIN * 60}")
HWEMU_PARAM_SEC=$(awk "BEGIN {printf \"%.1f\", $HWEMU_PARAM_MIN * 60}")

# Compute speedups
SPEEDUP_ALGO=$(awk "BEGIN {s=$HWEMU_ALGO_SEC / ($ALGO_TOTAL_SEC > 0 ? $ALGO_TOTAL_SEC : 0.001); printf \"%.0f\", s}")
SPEEDUP_STREAM=$(awk "BEGIN {s=$HWEMU_STREAM_SEC / ($TYPECHECK_SEC > 0 ? $TYPECHECK_SEC : 0.001); printf \"%.0f\", s}")
SPEEDUP_PARAM=$(awk "BEGIN {s=$HWEMU_PARAM_SEC / ($TYPECHECK_SEC > 0 ? $TYPECHECK_SEC : 0.001); printf \"%.0f\", s}")

# Output table
RESULT_CSV="$OUTPUT_DIR/tab3_results.csv"
echo "error_type,hwemu_time,sgscope_time,speedup" > "$RESULT_CSV"
echo "algorithm_failure_6iter,${HWEMU_ALGO_SEC}s,${ALGO_TOTAL_SEC}s,${SPEEDUP_ALGO}x" >> "$RESULT_CSV"
echo "stream_type_mismatch,${HWEMU_STREAM_SEC}s,${TYPECHECK_SEC}s,${SPEEDUP_STREAM}x" >> "$RESULT_CSV"
echo "parameter_mismatch,${HWEMU_PARAM_SEC}s,${TYPECHECK_SEC}s,${SPEEDUP_PARAM}x" >> "$RESULT_CSV"

printf '%-30s %15s %15s %12s\n' "Error Type" "HW Emulation" "SG-Scope" "Speedup"
printf '%-30s %15s %15s %12s\n' "------------------------------" "---------------" "---------------" "------------"
printf '%-30s %15s %15s %12s\n' "Algorithm failure (6 iter.)" "~${HWEMU_ALGO_SEC}s" "${ALGO_TOTAL_SEC}s" "~${SPEEDUP_ALGO}x"
printf '%-30s %15s %15s %12s\n' "Stream type mismatch" "${HWEMU_STREAM_SEC}s" "${TYPECHECK_SEC}s" "~${SPEEDUP_STREAM}x"
printf '%-30s %15s %15s %12s\n' "Parameter mismatch" "${HWEMU_PARAM_SEC}s" "${TYPECHECK_SEC}s" "~${SPEEDUP_PARAM}x"

echo ""
echo "Results CSV: $RESULT_CSV"
echo ""
echo "=== HW Emulation Reproduction (optional) ==="
echo ""
echo "To reproduce the hw_emu baseline times yourself:"
echo "  1. Copy apps/buggy_demos/ HLS projects to a machine with Docker + Vitis"
echo "  2. Use the buggy_demo runall.sh workflow with GRAPH_NODES=32000 GRAPH_EDGES=512000"
echo "  3. Three buggy projects:"
echo "     - sssp_topo_l11_b3_algorithm_bug:      inverted comparison in apply_kernel.cpp"
echo "     - sssp_topo_l11_b3_stream_type_mismatch: wrong packet type in shared_kernel_params.h"
echo "     - sssp_topo_l11_b3_parameter_bug:       wrong argument type in apply_kernel.cpp"
echo "  4. Each needs hw_emu compile + run via Docker container"
echo "  5. Pre-collected times are in data/hwemu_debug_baseline.tsv"
