#!/usr/bin/env bash
# =============================================================================
# ae_build.sh — Parallel HLS HW builder for AE projects
#
# Usage:
#   ./scripts/ae_build.sh --build-list <file> [--parallel <N>] [--target <hw|hw_emu|sw_emu>]
#
# The build list is a text file with one project directory per line.
# Each project must contain a Makefile (emitted by ae_emit_*.sh).
# =============================================================================
set -euo pipefail

PARALLEL=1
TARGET="hw"
BUILD_LIST=""
ENV_SCRIPT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-list) BUILD_LIST="$2"; shift 2 ;;
        --parallel)   PARALLEL="$2"; shift 2 ;;
        --target)     TARGET="$2"; shift 2 ;;
        --env)        ENV_SCRIPT="$2"; shift 2 ;;
        *)            echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$BUILD_LIST" || ! -f "$BUILD_LIST" ]]; then
    echo "ERROR: --build-list <file> is required" >&2
    exit 1
fi

# Source Vitis environment if provided
if [[ -n "$ENV_SCRIPT" && -f "$ENV_SCRIPT" ]]; then
    echo "Sourcing environment: $ENV_SCRIPT"
    source "$ENV_SCRIPT"
fi

# Check vitis availability
if ! command -v v++ &>/dev/null; then
    echo "WARNING: v++ not found in PATH. Builds will fail without Vitis."
    echo "         Set --env to your Vitis settings script, or source it before running."
fi

# Read build list, skip empty lines
PROJECTS=()
while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    PROJECTS+=("$line")
done < "$BUILD_LIST"

echo "=== AE Builder ==="
echo "  Build list: $BUILD_LIST"
echo "  Projects: ${#PROJECTS[@]}"
echo "  Parallel: $PARALLEL"
echo "  Target: $TARGET"
echo ""

# ── Check which projects need building ────────────────────────────────────────
TO_BUILD=()
SKIPPED=0
for project_dir in "${PROJECTS[@]}"; do
    if [[ ! -d "$project_dir" ]]; then
        echo "  SKIP (not found): $project_dir"
        ((SKIPPED++))
        continue
    fi

    xclbin="$project_dir/xclbin/graphyflow_kernels.${TARGET}.xclbin"
    build_log="$project_dir/build_${TARGET}.log"

    # Skip if xclbin exists AND build log shows success
    if [[ -f "$xclbin" && -f "$build_log" ]] && grep -q "INFO:.*\[v++\].*Run completed" "$build_log" 2>/dev/null; then
        echo "  SKIP (already built): $(basename "$project_dir")"
        ((SKIPPED++))
        continue
    fi

    TO_BUILD+=("$project_dir")
done

echo ""
echo "  To build: ${#TO_BUILD[@]}, Skipped: $SKIPPED"
echo ""

if [[ ${#TO_BUILD[@]} -eq 0 ]]; then
    echo "Nothing to build."
    exit 0
fi

# ── Build function ────────────────────────────────────────────────────────────
build_one() {
    local project_dir="$1"
    local project_name
    project_name="$(basename "$project_dir")"
    local build_log="$project_dir/build_${TARGET}.log"
    local start_time
    start_time=$(date +%s)

    echo "[BUILD START] $project_name ($(date '+%H:%M:%S'))"

    (
        cd "$project_dir"
        make all TARGET="$TARGET" 2>&1
    ) > "$build_log" 2>&1
    local exit_code=$?

    local end_time
    end_time=$(date +%s)
    local elapsed=$(( end_time - start_time ))
    local hours=$(( elapsed / 3600 ))
    local mins=$(( (elapsed % 3600) / 60 ))

    if [[ $exit_code -eq 0 ]]; then
        echo "[BUILD OK]    $project_name (${hours}h ${mins}m)"
    else
        echo "[BUILD FAIL]  $project_name (${hours}h ${mins}m, exit=$exit_code)"
    fi

    return $exit_code
}

export -f build_one
export TARGET

# ── Run builds ────────────────────────────────────────────────────────────────
FAILED=0
if [[ $PARALLEL -le 1 ]]; then
    # Sequential
    for project_dir in "${TO_BUILD[@]}"; do
        build_one "$project_dir" || ((FAILED++))
    done
else
    # Parallel via xargs
    printf "%s\n" "${TO_BUILD[@]}" | xargs -P "$PARALLEL" -I{} bash -c 'build_one "$@"' _ {} || true
    # Count failures by checking for xclbin
    for project_dir in "${TO_BUILD[@]}"; do
        xclbin="$project_dir/xclbin/graphyflow_kernels.${TARGET}.xclbin"
        if [[ ! -f "$xclbin" ]]; then
            ((FAILED++))
        fi
    done
fi

echo ""
echo "=== Build Summary ==="
echo "  Attempted: ${#TO_BUILD[@]}"
echo "  Failed: $FAILED"
echo "  Succeeded: $(( ${#TO_BUILD[@]} - FAILED ))"

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo "  Failed projects:"
    for project_dir in "${TO_BUILD[@]}"; do
        xclbin="$project_dir/xclbin/graphyflow_kernels.${TARGET}.xclbin"
        if [[ ! -f "$xclbin" ]]; then
            echo "    $(basename "$project_dir") — log: $project_dir/build_${TARGET}.log"
        fi
    done
    exit 1
fi
