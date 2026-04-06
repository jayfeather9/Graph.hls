#!/usr/bin/env bash
# =============================================================================
# ae_emit_fig9.sh — Emit HLS projects for Figure 9 (Ablation Study)
#
# Configurations:
#   Naive  : 32bit, 3B11L, no L1 optimization   (TODO: custom graph_preprocess)
#   L1     : 32bit, 3B11L, with L1 (default)
#   L1+L2  : 8bit,  3B11L, with L1
#   L1+L3  : 32bit, per-dataset grouped (32bit predictor), with L1
#   L1+L2+L3: 8bit, per-dataset grouped (8bit predictor), with L1
#
# Usage:
#   ./scripts/ae_emit_fig9.sh [--dataset-dir <path>] [--output-root <path>]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ─────────────────────────────────────────────────────────────────
DATASET_DIR="/path/to/datasets"
OUTPUT_ROOT="${REPO_ROOT}/target/ae/fig9"

# ── Parse arguments ──────────────────────────────────────────────────────────
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

# ── Binary ───────────────────────────────────────────────────────────────────
BINARY="${REPO_ROOT}/target/release/refactor_Graphyflow"
if [[ ! -x "$BINARY" ]]; then
    echo "Building compiler..."
    (cd "$REPO_ROOT" && cargo build --release)
fi

# ── DSL paths ────────────────────────────────────────────────────────────────
DSL_NAIVE="${REPO_ROOT}/apps/topology_variants/sssp_topo_l11_b3_no_l1.dsl"
DSL_L1="${REPO_ROOT}/apps/topology_variants/sssp_topo_l11_b3.dsl"
DSL_L1L2="${REPO_ROOT}/apps/topology_variants/sssp_topo_l11_b3_bw8.dsl"
DSL_AUTO_32="${REPO_ROOT}/apps/topology_variants/sssp_auto_grouped_32bit.dsl"

# ── Predictor models ─────────────────────────────────────────────────────────
MODEL_32BIT="${REPO_ROOT}/docs/grouping32_static_model_2026-04-04.json"
MODEL_8BIT="${REPO_ROOT}/docs/grouping_static_model_2026-04-02.json"

# ── Enumerate datasets ───────────────────────────────────────────────────────
DATASETS=()
for f in "$DATASET_DIR"/*.txt "$DATASET_DIR"/*.mtx; do
    [[ -f "$f" ]] && DATASETS+=("$f")
done

if [[ ${#DATASETS[@]} -eq 0 ]]; then
    echo "ERROR: No datasets found in $DATASET_DIR" >&2
    exit 1
fi

echo "Found ${#DATASETS[@]} datasets in $DATASET_DIR"
echo "Output root: $OUTPUT_ROOT"
echo ""

# =============================================================================
# 1. Naive (TODO: needs custom graph_preprocess.cpp/.h from user)
# =============================================================================
echo "=== [Naive] Emitting 32bit 3B11L (no-L1 preprocess) ==="
NAIVE_DIR="$OUTPUT_ROOT/naive"
if [[ -d "$NAIVE_DIR" ]]; then
    echo "  Already exists, skipping emission. Remove $NAIVE_DIR to re-emit."
else
    "$BINARY" --emit-hls "$DSL_NAIVE" "$NAIVE_DIR"
fi
echo ""

# =============================================================================
# 2. L1 (32bit, 3B11L — single build for all datasets)
# =============================================================================
echo "=== [L1] Emitting 32bit 3B11L ==="
L1_DIR="$OUTPUT_ROOT/l1_32bit_3b11l"
if [[ -d "$L1_DIR" ]]; then
    echo "  Already exists, skipping."
else
    "$BINARY" --emit-hls "$DSL_L1" "$L1_DIR"
fi
echo ""

# =============================================================================
# 3. L1+L2 (8bit, 3B11L — single build for all datasets)
# =============================================================================
echo "=== [L1+L2] Emitting 8bit 3B11L ==="
L1L2_DIR="$OUTPUT_ROOT/l1l2_8bit_3b11l"
if [[ -d "$L1L2_DIR" ]]; then
    echo "  Already exists, skipping."
else
    "$BINARY" --emit-hls "$DSL_L1L2" "$L1L2_DIR"
fi
echo ""

# =============================================================================
# 4. L1+L3 (32bit, per-dataset grouping via 32bit predictor + auto template)
# =============================================================================
echo "=== [L1+L3] Emitting 32bit grouped (per-dataset via 32bit predictor) ==="
L1L3_DIR="$OUTPUT_ROOT/l1l3_32bit_grouped"
mkdir -p "$L1L3_DIR"

L1L3_MANIFEST="$L1L3_DIR/manifest.csv"
echo "dataset,variant,project_dir" > "$L1L3_MANIFEST"

declare -A EMITTED_L1L3_VARIANTS
L1L3_FAILURES=()

for ds in "${DATASETS[@]}"; do
    ds_name="$(basename "$ds" | sed 's/\.\(txt\|mtx\)$//')"

    # Use --auto-emit-hls-from-dsl-32bit which predicts + emits in one step
    # First, predict to get the variant name for dedup
    prediction=$("$BINARY" --predict-grouping32-from-static-model "$MODEL_32BIT" "$ds" 2>&1 || true)
    variant=$(echo "$prediction" | grep "recommended variant:" | awk '{print $NF}')

    if [[ -z "$variant" ]]; then
        echo "  WARNING: Could not predict variant for $ds_name, skipping."
        L1L3_FAILURES+=("$ds_name:no_prediction")
        echo "$ds_name,,PREDICT_FAILED" >> "$L1L3_MANIFEST"
        continue
    fi

    project_dir="$L1L3_DIR/$variant"

    if [[ -z "${EMITTED_L1L3_VARIANTS[$variant]+x}" ]]; then
        if [[ -d "$project_dir" ]]; then
            echo "  [$variant] Already exists, skipping emission."
        else
            echo "  [$variant] Emitting (first needed by $ds_name)..."
            if "$BINARY" --auto-emit-hls-from-dsl-32bit "$DSL_AUTO_32" "$ds" "$project_dir" "$MODEL_32BIT" 2>&1; then
                echo "  [$variant] OK"
            else
                echo "  [$variant] FAILED: SLR placement failed. Needs manual DSL."
                L1L3_FAILURES+=("$ds_name:$variant:slr_failed")
                echo "$ds_name,$variant,SLR_FAILED" >> "$L1L3_MANIFEST"
                # Don't mark as emitted so other datasets with same variant also show failure
                continue
            fi
        fi
        EMITTED_L1L3_VARIANTS[$variant]=1
    fi

    echo "$ds_name,$variant,$project_dir" >> "$L1L3_MANIFEST"
done

echo "  L1+L3 manifest: $L1L3_MANIFEST"
unique_l1l3=${#EMITTED_L1L3_VARIANTS[@]}
echo "  Unique variants emitted: $unique_l1l3"
if [[ ${#L1L3_FAILURES[@]} -gt 0 ]]; then
    echo "  FAILURES (need manual DSL files):"
    printf "    %s\n" "${L1L3_FAILURES[@]}"
fi
echo ""

# =============================================================================
# 5. L1+L2+L3 (8bit, per-dataset grouping via 8bit predictor)
#    Phase 1: Predict variant for all datasets (slow — parses graph files)
#    Phase 2: Deduplicate variants
#    Phase 3: Emit only unique HLS projects
# =============================================================================
echo "=== [L1+L2+L3] 8bit grouped (per-dataset via 8bit predictor) ==="
L1L2L3_DIR="$OUTPUT_ROOT/l1l2l3_8bit_grouped"
mkdir -p "$L1L2L3_DIR"

L1L2L3_MANIFEST="$L1L2L3_DIR/manifest.csv"
echo "dataset,variant,project_dir" > "$L1L2L3_MANIFEST"

declare -A L1L2L3_DS_VARIANT   # dataset_name → variant
declare -A L1L2L3_VARIANT_DS   # variant → first_dataset_path (for emission)
L1L2L3_FAILURES=()

# Phase 1: Predict all
echo "  Phase 1: Predicting variants for ${#DATASETS[@]} datasets..."
for ds in "${DATASETS[@]}"; do
    ds_name="$(basename "$ds" | sed 's/\.\(txt\|mtx\)$//')"
    echo -n "    $ds_name ... "
    prediction=$("$BINARY" --predict-grouping-for-dataset "$ds" "$MODEL_8BIT" 2>&1 || true)
    variant=$(echo "$prediction" | grep "recommended variant:" | awk '{print $NF}')

    if [[ -z "$variant" ]]; then
        echo "FAILED (no prediction)"
        L1L2L3_FAILURES+=("$ds_name:no_prediction")
        continue
    fi

    echo "$variant"
    L1L2L3_DS_VARIANT[$ds_name]="$variant"
    # Remember first dataset path for each variant (used for emission)
    if [[ -z "${L1L2L3_VARIANT_DS[$variant]+x}" ]]; then
        L1L2L3_VARIANT_DS[$variant]="$ds"
    fi
done

# Phase 2: Summary
echo ""
echo "  Phase 2: ${#L1L2L3_VARIANT_DS[@]} unique variants across ${#L1L2L3_DS_VARIANT[@]} datasets"

# Phase 3: Emit unique variants
echo "  Phase 3: Emitting unique HLS projects..."
declare -A EMITTED_L1L2L3_VARIANTS

for variant in "${!L1L2L3_VARIANT_DS[@]}"; do
    ds="${L1L2L3_VARIANT_DS[$variant]}"
    project_dir="$L1L2L3_DIR/$variant"

    if [[ -d "$project_dir" ]]; then
        echo "    [$variant] Already exists, skipping."
    else
        echo "    [$variant] Emitting..."
        if "$BINARY" --auto-emit-sssp-bw8 "$ds" "$project_dir" "$MODEL_8BIT" 2>&1; then
            echo "    [$variant] OK"
        else
            echo "    [$variant] FAILED"
            L1L2L3_FAILURES+=("$variant:emit_failed")
            continue
        fi
    fi
    EMITTED_L1L2L3_VARIANTS[$variant]=1
done

# Write manifest
for ds_name in "${!L1L2L3_DS_VARIANT[@]}"; do
    variant="${L1L2L3_DS_VARIANT[$ds_name]}"
    if [[ -n "${EMITTED_L1L2L3_VARIANTS[$variant]+x}" ]]; then
        echo "$ds_name,$variant,$L1L2L3_DIR/$variant" >> "$L1L2L3_MANIFEST"
    else
        echo "$ds_name,$variant,FAILED" >> "$L1L2L3_MANIFEST"
    fi
done

unique_l1l2l3=${#EMITTED_L1L2L3_VARIANTS[@]}
echo "  Unique variants emitted: $unique_l1l2l3"
if [[ ${#L1L2L3_FAILURES[@]} -gt 0 ]]; then
    echo "  FAILURES:"
    printf "    %s\n" "${L1L2L3_FAILURES[@]}"
fi
echo ""

# =============================================================================
# Summary
# =============================================================================
echo "================================================================"
echo "Fig 9 Emission Summary"
echo "================================================================"
echo "  Naive     : $NAIVE_DIR  (no-L1 preprocess)"
echo "  L1        : $L1_DIR     (1 build)"
echo "  L1+L2     : $L1L2_DIR   (1 build)"
echo "  L1+L3     : $L1L3_DIR   ($unique_l1l3 unique builds)"
echo "  L1+L2+L3  : $L1L2L3_DIR ($unique_l1l2l3 unique builds)"
echo ""

total_builds=$((2 + unique_l1l3 + unique_l1l2l3 + 1))
echo "Total HLS projects to build: $total_builds"
echo ""

# Collect all project dirs for the builder
BUILD_LIST="$OUTPUT_ROOT/build_list.txt"
: > "$BUILD_LIST"
echo "$NAIVE_DIR" >> "$BUILD_LIST"
echo "$L1_DIR" >> "$BUILD_LIST"
echo "$L1L2_DIR" >> "$BUILD_LIST"
for v in "${!EMITTED_L1L3_VARIANTS[@]}"; do
    echo "$L1L3_DIR/$v" >> "$BUILD_LIST"
done
for v in "${!EMITTED_L1L2L3_VARIANTS[@]}"; do
    echo "$L1L2L3_DIR/$v" >> "$BUILD_LIST"
done

echo "Build list written to: $BUILD_LIST"
echo "Next step: ./scripts/ae_build.sh --build-list $BUILD_LIST"
