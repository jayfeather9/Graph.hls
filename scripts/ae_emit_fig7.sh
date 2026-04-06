#!/usr/bin/env bash
# =============================================================================
# ae_emit_fig7.sh — Emit HLS projects for Figure 7 (Graph.hls vs ReGraph, U55C)
#
# Algorithms: PageRank (PR), Closeness Centrality (CC), SSSP
# Platform: Alveo U55C (HBM), 3 big 11 little, 32bit
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET_DIR="/path/to/datasets"
OUTPUT_ROOT="${REPO_ROOT}/target/ae/fig7"

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

# DSL files: 3B11L, 32bit, HBM — unweighted SSSP, CC (connected_components), PR (pagerank)
DSL_DIR="${REPO_ROOT}/apps/topology_variants"

declare -A ALGO_DSL=(
    [sssp]="${DSL_DIR}/sssp_topo_l11_b3.dsl"
    [connected_components]="${DSL_DIR}/connected_components_topo_l11_b3.dsl"
    [pagerank]="${DSL_DIR}/pagerank_topo_l11_b3.dsl"
)

echo "=== Fig 7: Graph.hls vs ReGraph (U55C, HBM, 3B11L, 32bit) ==="
echo "Output: $OUTPUT_ROOT"
echo ""

for algo in sssp connected_components pagerank; do
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
    fi
done

echo ""
echo "=== Fig 7 Emission Summary ==="
echo "  SSSP     : $OUTPUT_ROOT/sssp"
echo "  CC       : $OUTPUT_ROOT/connected_components"
echo "  PageRank : $OUTPUT_ROOT/pagerank"
echo "  Total: 3 HLS projects"
echo ""

# Build list
BUILD_LIST="$OUTPUT_ROOT/build_list.txt"
: > "$BUILD_LIST"
for algo in sssp connected_components pagerank; do
    echo "$OUTPUT_ROOT/$algo" >> "$BUILD_LIST"
done
echo "Build list: $BUILD_LIST"
echo ""
echo "=== ReGraph Baseline Setup ==="
echo ""
echo "Figure 7 compares Graph.hls against ReGraph. You must build and run"
echo "ReGraph separately, then provide its results to the runner/plotter."
echo ""
echo "ReGraph build instructions:"
echo "  1. Obtain the ReGraph source (not included in this repo)"
echo "  2. Source Vitis/XRT:  source /path/to/vitis/settings64.sh"
echo "  3. Generate configs:  make autogen"
echo "  4. Build each app:"
echo "       make APP=bfs all    # ReGraph 'bfs' = SSSP (compares with our sssp)"
echo "       make APP=pr  all    # PageRank"
echo "       make APP=cc  all    # Connected Components"
echo "  5. Expected outputs per app:"
echo "       host_graph_fpga_<app>"
echo "       xclbin_hw_<app>/graph_fpga.hw.<platform>.xclbin"
echo ""
echo "NOTE: ReGraph's 'bfs' app implements SSSP (BFS/Bellman-Ford)."
echo "      When collecting baseline results, map: regraph bfs -> our sssp."
