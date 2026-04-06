#!/usr/bin/env bash
#
# Batch simulation runner for Graphyflow.
#
# Usage:
#   batch_simulate.sh [OPTIONS]
#
# Options:
#   --format  raw|json        Input format: "raw" (.txt/.mtx) or "json" (pre-converted)
#   --mode    single|batch    Single file or scan a directory for all .txt/.mtx files
#   --dataset <path>          Path to a single file (mode=single) or directory (mode=batch)
#   --dsl     <list>          Comma-separated DSL names, e.g. "sssp,pagerank,connected_components"
#   --output  <path>          Output CSV path (default: sim_results.csv)
#   --max-iters <n>           Max simulation iterations (default: 32)
#   --run     sim|swemu|both  What to run: simulator only, sw_emu only, or both (default: sim)
#   --project <name>          HLS project name for sw_emu (default: uses DSL name)
#   --swemu-timeout <sec>     Timeout per sw_emu run in seconds (default: 3600)
#   --swemu-iters <n>         Iteration count for sw_emu (default: 1)
#   --swemu-args <args>       Extra arguments passed to run_hwemu_docker_one.sh
#
# Examples:
#   # Simulator only
#   ./scripts/batch_simulate.sh --format raw --mode batch \
#       --dataset /data/graphs/ --dsl "sssp,pagerank,connected_components" --output results.csv
#
#   # sw_emu only
#   ./scripts/batch_simulate.sh --format raw --mode batch \
#       --dataset /data/graphs/ --dsl sssp --run swemu --project sssp --output swemu_results.csv
#
#   # Both simulator + sw_emu, then plot speedup from the same CSV
#   ./scripts/batch_simulate.sh --format raw --mode batch \
#       --dataset /data/graphs/ --dsl sssp --run both --project sssp --output combined.csv

set -euo pipefail

# ---- defaults ----
FORMAT="raw"
MODE="single"
DATASET=""
DSL_LIST=""
OUTPUT="sim_results.csv"
MAX_ITERS=32
RUN_WHAT="sim"
HLS_PROJECT=""
SWEMU_TIMEOUT=3600
SWEMU_ITERS=1
SWEMU_EXTRA_ARGS=""

# ---- locate paths ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Try release first, then debug
BINARY=""
for candidate in \
    "$PROJECT_ROOT/target/release/refactor_Graphyflow" \
    "$PROJECT_ROOT/target/debug/refactor_Graphyflow"; do
    if [[ -x "$candidate" ]]; then
        BINARY="$candidate"
        break
    fi
done

if [[ -z "$BINARY" ]]; then
    echo "error: binary not found. Run 'cargo build' or 'cargo build --release' first." >&2
    exit 1
fi

SWEMU_RUNNER="$SCRIPT_DIR/run_hwemu_docker_one.sh"

# ---- parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --format)        FORMAT="$2";          shift 2 ;;
        --mode)          MODE="$2";            shift 2 ;;
        --dataset)       DATASET="$2";         shift 2 ;;
        --dsl)           DSL_LIST="$2";        shift 2 ;;
        --output)        OUTPUT="$2";          shift 2 ;;
        --max-iters)     MAX_ITERS="$2";       shift 2 ;;
        --run)           RUN_WHAT="$2";        shift 2 ;;
        --project)       HLS_PROJECT="$2";     shift 2 ;;
        --swemu-timeout) SWEMU_TIMEOUT="$2";   shift 2 ;;
        --swemu-iters)   SWEMU_ITERS="$2";     shift 2 ;;
        --swemu-args)    SWEMU_EXTRA_ARGS="$2"; shift 2 ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$DATASET" ]]; then
    echo "error: --dataset is required" >&2
    exit 1
fi
if [[ -z "$DSL_LIST" ]]; then
    echo "error: --dsl is required" >&2
    exit 1
fi
if [[ "$RUN_WHAT" != "sim" && "$RUN_WHAT" != "swemu" && "$RUN_WHAT" != "both" ]]; then
    echo "error: --run must be 'sim', 'swemu', or 'both'" >&2
    exit 1
fi

RUN_SIM=false
RUN_SWEMU=false
case "$RUN_WHAT" in
    sim)   RUN_SIM=true ;;
    swemu) RUN_SWEMU=true ;;
    both)  RUN_SIM=true; RUN_SWEMU=true ;;
esac

if $RUN_SWEMU && [[ ! -x "$SWEMU_RUNNER" ]]; then
    echo "error: sw_emu runner not found at $SWEMU_RUNNER" >&2
    exit 1
fi

# Parse DSL list. Each entry can be either:
#   label
#   label=app-or-dsl-path
#
# The label controls CSV column names and the default sw_emu project name.
# When using a DSL path, the label must still be a built-in app name so the
# raw-graph converter knows which default properties to attach.
IFS=',' read -ra DSL_SPECS <<< "$DSL_LIST"
declare -a DSL_LABELS=()
declare -a DSL_PROGRAMS=()
declare -a DSL_CONVERT_APPS=()

is_builtin_app() {
    local name="$1"
    case "$name" in
        sssp|pagerank|connected_components|bfs|wcc|ar|graph_coloring|als)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

for spec in "${DSL_SPECS[@]}"; do
    label="$spec"
    program="$spec"
    if [[ "$spec" == *=* ]]; then
        label="${spec%%=*}"
        program="${spec#*=}"
    fi

    if is_builtin_app "$label"; then
        convert_app="$label"
    elif is_builtin_app "$program"; then
        convert_app="$program"
    else
        echo "error: DSL entry '$spec' needs a built-in app label for --convert-graph" >&2
        echo "hint: use entries like 'sssp=apps/sssp_unweighted_swemu_one_big.dsl'" >&2
        exit 1
    fi

    DSL_LABELS+=("$label")
    DSL_PROGRAMS+=("$program")
    DSL_CONVERT_APPS+=("$convert_app")
done

# ---- collect dataset files ----
declare -a DATASET_FILES=()

if [[ "$MODE" == "single" ]]; then
    if [[ ! -f "$DATASET" ]]; then
        echo "error: file not found: $DATASET" >&2
        exit 1
    fi
    DATASET_FILES+=("$DATASET")
elif [[ "$MODE" == "batch" ]]; then
    if [[ ! -d "$DATASET" ]]; then
        echo "error: directory not found: $DATASET" >&2
        exit 1
    fi
    while IFS= read -r -d '' f; do
        DATASET_FILES+=("$f")
    done < <(find "$DATASET" -maxdepth 1 -type f \( -name "*.txt" -o -name "*.mtx" -o -name "*.json" \) -print0 | sort -z)
    if [[ ${#DATASET_FILES[@]} -eq 0 ]]; then
        echo "error: no .txt, .mtx, or .json files found in $DATASET" >&2
        exit 1
    fi
else
    echo "error: --mode must be 'single' or 'batch'" >&2
    exit 1
fi

# ---- build CSV header ----
HEADER="dataset"
for dsl in "${DSL_LABELS[@]}"; do
    if $RUN_SIM; then
        HEADER="${HEADER},${dsl}_status,${dsl}_simulate_time_sec,${dsl}_error"
    fi
    if $RUN_SWEMU; then
        HEADER="${HEADER},${dsl}_swemu_status,${dsl}_swemu_time_ms,${dsl}_swemu_error"
    fi
done
echo "$HEADER" > "$OUTPUT"

# ---- temp directory for converted JSONs ----
TMPDIR_CONVERT=""
if [[ "$FORMAT" == "raw" ]]; then
    TMPDIR_CONVERT="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR_CONVERT"' EXIT
fi

echo "datasets: ${#DATASET_FILES[@]}, dsls: ${DSL_LABELS[*]}, run: $RUN_WHAT, output: $OUTPUT"
echo "---"

# ---- helper: extract sw_emu kernel time from log output ----
# Looks for "Total FPGA Kernel Execution Time: <N> ms" or
# "FPGA Iteration 0: ... Time = <N> ms"
extract_swemu_time_ms() {
    local log="$1"
    # Prefer total kernel time
    local total
    total=$(echo "$log" | grep -oP 'Total FPGA Kernel Execution Time:\s*\K[0-9.eE+\-]+' | head -1)
    if [[ -n "$total" ]]; then
        echo "$total"
        return
    fi
    # Fallback: first iteration time
    local iter0
    iter0=$(echo "$log" | grep -oP 'FPGA Iteration 0:.*?Time\s*=\s*\K[0-9.eE+\-]+' | head -1)
    if [[ -n "$iter0" ]]; then
        echo "$iter0"
        return
    fi
    echo ""
}

extract_swemu_log_path() {
    local runner_output="$1"
    local path=""
    path=$(echo "$runner_output" | sed -n 's/^[[:space:]]*log=\(.*\)$/\1/p' | tail -1)
    if [[ -n "$path" ]]; then
        echo "$path"
        return
    fi
    path=$(echo "$runner_output" | sed -n 's/^==> FAIL (rc=[^,]*, see \(.*\))$/\1/p' | tail -1)
    if [[ -n "$path" ]]; then
        echo "$path"
        return
    fi
    echo ""
}

extract_sim_compute_time() {
    local output="$1"
    local value=""
    value=$(echo "$output" | sed -n 's/^simulation compute time sec: \([0-9.eE+-][0-9.eE+-]*\)$/\1/p' | tail -1)
    echo "$value"
}

# ---- run ----
for dataset_path in "${DATASET_FILES[@]}"; do
    dataset_basename="$(basename "$dataset_path")"
    ROW="$dataset_path"

    for idx in "${!DSL_LABELS[@]}"; do
        dsl="${DSL_LABELS[$idx]}"
        dsl_program="${DSL_PROGRAMS[$idx]}"
        dsl_convert_app="${DSL_CONVERT_APPS[$idx]}"
        # --- graph conversion (shared by sim and swemu) ---
        json_path=""
        if [[ "$FORMAT" == "json" ]]; then
            json_path="$dataset_path"
        elif [[ "$FORMAT" == "raw" ]] && $RUN_SIM; then
            json_path=""
        fi

        # --- simulator ---
        if $RUN_SIM; then
            start_time=$(date +%s%N)
            set +e
            if [[ "$FORMAT" == "raw" ]]; then
                sim_output=$(
                    env GRAPHYFLOW_SIM_QUIET=1 GRAPHYFLOW_SIM_TIMING=1 GRAPHYFLOW_SIM_SKIP_REFERENCE=1 GRAPHYFLOW_SIM_MEASURE_ONLY=1 \
                        "$BINARY" --simulate-raw "$dsl_program" "$dataset_path" "$dsl_convert_app" "$MAX_ITERS" 2>&1
                )
            else
                sim_output=$(
                    env GRAPHYFLOW_SIM_QUIET=1 GRAPHYFLOW_SIM_TIMING=1 GRAPHYFLOW_SIM_SKIP_REFERENCE=1 GRAPHYFLOW_SIM_MEASURE_ONLY=1 \
                        "$BINARY" --simulate-json "$dsl_program" "$json_path" "$MAX_ITERS" 2>&1
                )
            fi
            sim_rc=$?
            set -e
            end_time=$(date +%s%N)

            elapsed_sec=$(echo "scale=3; ($end_time - $start_time) / 1000000000" | bc)
            compute_elapsed_sec=$(extract_sim_compute_time "$sim_output")
            if [[ -n "$compute_elapsed_sec" ]]; then
                elapsed_sec="$compute_elapsed_sec"
            fi

            if [[ "$sim_rc" -eq 0 ]]; then
                status="ok"; error_msg=""
            elif echo "$sim_output" | grep -qi "error\|panic\|thread.*panicked"; then
                status="error"
                error_msg=$(echo "$sim_output" | grep -i "error\|panic" | head -1 | tr ',' ';')
            else
                status="error"; error_msg="simulation failed"
            fi

            ROW="${ROW},${status},${elapsed_sec},${error_msg}"
            echo "  ${dataset_basename} / ${dsl} [sim]: ${status} (${elapsed_sec}s)"
        fi

        # --- sw_emu ---
        if $RUN_SWEMU; then
            project_name="${HLS_PROJECT:-$dsl}"

            # shellcheck disable=SC2086
            swemu_log=$("$SWEMU_RUNNER" \
                --project "$project_name" \
                --graph "$dataset_path" \
                --run-mode sw_emu \
                --iters "$SWEMU_ITERS" \
                --timeout "$SWEMU_TIMEOUT" \
                $SWEMU_EXTRA_ARGS 2>&1) || true
            swemu_runner_log_path="$(extract_swemu_log_path "$swemu_log")"

            if echo "$swemu_log" | grep -q "PASS"; then
                swemu_status="ok"
                swemu_error=""
            elif echo "$swemu_log" | grep -q "FAIL"; then
                swemu_status="error"
                swemu_error=$(echo "$swemu_log" | grep "FAIL" | head -1 | tr ',' ';')
            else
                swemu_status="error"
                swemu_error="unknown sw_emu failure"
            fi

            swemu_time_ms=$(extract_swemu_time_ms "$swemu_log")
            if [[ -z "$swemu_time_ms" && -n "$swemu_runner_log_path" && -f "$swemu_runner_log_path" ]]; then
                swemu_time_ms=$(extract_swemu_time_ms "$(cat "$swemu_runner_log_path")")
            fi
            if [[ -z "$swemu_time_ms" ]]; then
                swemu_time_ms="0"
            fi
            if [[ "$swemu_status" == "error" && "$swemu_error" == "unknown sw_emu failure" && -n "$swemu_runner_log_path" && -f "$swemu_runner_log_path" ]]; then
                swemu_error=$(tail -n 40 "$swemu_runner_log_path" | tr '\n' ' ' | tr ',' ';' | sed 's/  */ /g')
            fi

            ROW="${ROW},${swemu_status},${swemu_time_ms},${swemu_error}"
            echo "  ${dataset_basename} / ${dsl} [swemu]: ${swemu_status} (${swemu_time_ms} ms)"
        fi

        # Clean up per-DSL temp JSON
        if [[ "$FORMAT" == "raw" && -n "$json_path" && -f "$json_path" ]]; then
            rm -f "$json_path"
        fi
    done

    echo "$ROW" >> "$OUTPUT"
done

echo "---"
echo "results written to: $OUTPUT"
