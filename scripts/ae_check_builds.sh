#!/usr/bin/env bash
# =============================================================================
# ae_check_builds.sh — Check build status and optionally retry failed builds
#
# Usage:
#   ./scripts/ae_check_builds.sh --build-list <file> [--target <hw>] [--retry]
# =============================================================================
set -euo pipefail

BUILD_LIST=""
TARGET="hw"
RETRY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-list) BUILD_LIST="$2"; shift 2 ;;
        --target)     TARGET="$2"; shift 2 ;;
        --retry)      RETRY=1; shift ;;
        *)            echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$BUILD_LIST" || ! -f "$BUILD_LIST" ]]; then
    echo "ERROR: --build-list <file> is required" >&2
    exit 1
fi

PROJECTS=()
while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    PROJECTS+=("$line")
done < "$BUILD_LIST"

echo "=== AE Build Status Check ==="
echo "  Build list: $BUILD_LIST"
echo "  Target: $TARGET"
echo ""

SUCCEEDED=()
FAILED=()
MISSING=()

for project_dir in "${PROJECTS[@]}"; do
    project_name="$(basename "$project_dir")"

    if [[ ! -d "$project_dir" ]]; then
        echo "  [MISSING]  $project_name"
        MISSING+=("$project_dir")
        continue
    fi

    xclbin="$project_dir/xclbin/graphyflow_kernels.${TARGET}.xclbin"
    build_log="$project_dir/build_${TARGET}.log"

    if [[ -f "$xclbin" ]]; then
        # Check log for completion marker
        if [[ -f "$build_log" ]] && grep -q "INFO:.*\[v++\].*Run completed" "$build_log" 2>/dev/null; then
            echo "  [OK]       $project_name"
            SUCCEEDED+=("$project_dir")
        else
            echo "  [OK?]      $project_name (xclbin exists but no completion marker in log)"
            SUCCEEDED+=("$project_dir")
        fi
    elif [[ -f "$build_log" ]]; then
        # Has log but no xclbin — check for errors
        last_error=$(grep -i "ERROR\|CRITICAL\|FATAL" "$build_log" 2>/dev/null | tail -1 || true)
        echo "  [FAILED]   $project_name — ${last_error:-unknown error}"
        FAILED+=("$project_dir")
    else
        echo "  [NOT BUILT] $project_name"
        FAILED+=("$project_dir")
    fi
done

echo ""
echo "=== Summary ==="
echo "  Succeeded: ${#SUCCEEDED[@]}"
echo "  Failed/Not built: ${#FAILED[@]}"
echo "  Missing: ${#MISSING[@]}"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    RETRY_LIST="$(dirname "$BUILD_LIST")/retry_list.txt"
    printf "%s\n" "${FAILED[@]}" > "$RETRY_LIST"
    echo ""
    echo "  Retry list written to: $RETRY_LIST"

    if [[ $RETRY -eq 1 ]]; then
        echo ""
        echo "=== Retrying failed builds ==="
        SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        exec "$SCRIPT_DIR/ae_build.sh" --build-list "$RETRY_LIST" --target "$TARGET" "$@"
    else
        echo "  Run with --retry to rebuild failed projects."
        echo "  Or: ./scripts/ae_build.sh --build-list $RETRY_LIST"
    fi
fi

[[ ${#FAILED[@]} -eq 0 && ${#MISSING[@]} -eq 0 ]]
