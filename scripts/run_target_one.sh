#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_target_one.sh --project <name> --mode <sw_emu|hw_emu|hw> --graph <path> [options]

Build and/or run one emitted project under target/generated_hls/<project> without Docker.

Options:
  --project <name>      Project directory name under target/generated_hls (required)
  --mode <mode>         sw_emu | hw_emu | hw (required)
  --graph <path>        Dataset path, absolute or repo-relative (required)
  --iters <n>           Fixed iteration count via GRAPHYFLOW_MAX_ITERS (default: 1)
  --timeout <seconds>   Timeout for the run phase (default: 3600)
  --device <path>       Override DEVICE=<platform> for make (optional)
  --device-bdf <bdf>    Hardware BDF passed to run_hw_with_watchdog.sh for diagnostics/reset
  --device-index <n>    Hardware device index passed to run_hw_with_watchdog.sh
  --kernel-freq <mhz>   Pass KERNEL_FREQ=<mhz> to make (optional)
  --big-edge-per-ms <n> Pass BIG_EDGE_PER_MS=<n> to host compilation
  --little-edge-per-ms <n>
                        Pass LITTLE_EDGE_PER_MS=<n> to host compilation
  --rebuild-exe         Force `make cleanexe && make exe TARGET=<mode>`
  --build-kernels       Force `make all TARGET=<mode>`
  --build-only          Stop after build; do not run
  --env <path>          Environment script to source before build/run
  --log-dir <path>      Directory for the combined run log (default: target/parallel_logs)
  -h, --help            Show this help

Examples:
  scripts/run_target_one.sh --project sssp_ddr_4b4l_codegen --mode sw_emu --graph target/ddr_smoke/graph.txt
  scripts/run_target_one.sh --project pr_ddr_4b4l --mode hw_emu --graph /data/graphs/rmat-19-32.txt --build-kernels --timeout 3600
  scripts/run_target_one.sh --project sssp_ddr_4b4l_codegen --mode hw_emu --graph /data/graphs/rmat-19-32.txt --big-edge-per-ms 10000 --little-edge-per-ms 40000
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROJECT=""
MODE=""
GRAPH=""
ITERS="1"
TIMEOUT_SECONDS="3600"
DEVICE_PLATFORM=""
DEVICE_BDF=""
DEVICE_INDEX=""
KERNEL_FREQ=""
BIG_EDGE_PER_MS=""
LITTLE_EDGE_PER_MS=""
REBUILD_EXE="0"
BUILD_KERNELS="0"
BUILD_ONLY="0"
ENV_SCRIPT="${GRAPHYFLOW_ENV_SH:-}"
LOG_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="${2:?}"; shift 2 ;;
    --mode) MODE="${2:?}"; shift 2 ;;
    --graph) GRAPH="${2:?}"; shift 2 ;;
    --iters) ITERS="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT_SECONDS="${2:?}"; shift 2 ;;
    --device) DEVICE_PLATFORM="${2:?}"; shift 2 ;;
    --device-bdf) DEVICE_BDF="${2:?}"; shift 2 ;;
    --device-index) DEVICE_INDEX="${2:?}"; shift 2 ;;
    --kernel-freq) KERNEL_FREQ="${2:?}"; shift 2 ;;
    --big-edge-per-ms) BIG_EDGE_PER_MS="${2:?}"; shift 2 ;;
    --little-edge-per-ms) LITTLE_EDGE_PER_MS="${2:?}"; shift 2 ;;
    --rebuild-exe) REBUILD_EXE="1"; shift ;;
    --build-kernels) BUILD_KERNELS="1"; shift ;;
    --build-only) BUILD_ONLY="1"; shift ;;
    --env) ENV_SCRIPT="${2:?}"; shift 2 ;;
    --log-dir) LOG_DIR="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$PROJECT" || -z "$MODE" || -z "$GRAPH" ]]; then
  usage >&2
  exit 2
fi

case "$MODE" in
  sw_emu|hw_emu|hw) ;;
  *)
    echo "--mode must be sw_emu, hw_emu, or hw (got '$MODE')" >&2
    exit 2
    ;;
esac

if ! [[ "$ITERS" =~ ^[0-9]+$ ]] || [[ "$ITERS" -lt 1 ]]; then
  echo "--iters must be an integer >= 1 (got '$ITERS')" >&2
  exit 2
fi
if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT_SECONDS" -lt 1 ]]; then
  echo "--timeout must be an integer >= 1 (got '$TIMEOUT_SECONDS')" >&2
  exit 2
fi
if [[ -n "$BIG_EDGE_PER_MS" ]] && { ! [[ "$BIG_EDGE_PER_MS" =~ ^[0-9]+$ ]] || [[ "$BIG_EDGE_PER_MS" -lt 1 ]]; }; then
  echo "--big-edge-per-ms must be an integer >= 1 (got '$BIG_EDGE_PER_MS')" >&2
  exit 2
fi
if [[ -n "$LITTLE_EDGE_PER_MS" ]] && { ! [[ "$LITTLE_EDGE_PER_MS" =~ ^[0-9]+$ ]] || [[ "$LITTLE_EDGE_PER_MS" -lt 1 ]]; }; then
  echo "--little-edge-per-ms must be an integer >= 1 (got '$LITTLE_EDGE_PER_MS')" >&2
  exit 2
fi

PROJECT_DIR="$REPO_ROOT/target/generated_hls/$PROJECT"
if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "project not found: $PROJECT_DIR" >&2
  exit 1
fi

GRAPH_ABS="$GRAPH"
if [[ "$GRAPH_ABS" != /* ]]; then
  GRAPH_ABS="$REPO_ROOT/$GRAPH_ABS"
fi
if [[ ! -f "$GRAPH_ABS" ]]; then
  echo "graph not found: $GRAPH_ABS" >&2
  exit 1
fi

if [[ -n "$ENV_SCRIPT" ]]; then
  if [[ "$ENV_SCRIPT" != /* ]]; then
    ENV_SCRIPT="$REPO_ROOT/$ENV_SCRIPT"
  fi
  if [[ ! -f "$ENV_SCRIPT" ]]; then
    echo "environment script not found: $ENV_SCRIPT" >&2
    exit 1
  fi
fi

if [[ -n "$LOG_DIR" && "$LOG_DIR" != /* ]]; then
  LOG_DIR="$REPO_ROOT/$LOG_DIR"
fi

if [[ -z "$LOG_DIR" ]]; then
  LOG_DIR="$REPO_ROOT/target/parallel_logs"
fi
mkdir -p "$LOG_DIR"
tag="$(basename "$GRAPH_ABS" | sed 's/[^A-Za-z0-9_.-]/_/g')"
stamp="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/${PROJECT}__${tag}__${MODE}_${ITERS}it_${stamp}.log"
BUILD_LOG="$(mktemp "${TMPDIR:-/tmp}/graphyflow_build_${PROJECT}_${MODE}_XXXXXX.log")"
RUN_LOG=""

cleanup() {
  rm -f "$BUILD_LOG"
  if [[ -n "$RUN_LOG" ]]; then
    rm -f "$RUN_LOG"
  fi
}
trap cleanup EXIT

source_env_script() {
  local script_path="$1"
  set +e
  set +u
  set +o pipefail
  # shellcheck disable=SC1090
  source "$script_path"
  local rc=$?
  set -e
  set -u
  set -o pipefail
  return "$rc"
}

run_in_project() {
  (
    cd "$PROJECT_DIR"
    if [[ -n "$ENV_SCRIPT" ]]; then
      source_env_script "$ENV_SCRIPT"
    fi
    "$@"
  )
}

append_build_summary() {
  {
    echo "==> project=$PROJECT mode=$MODE graph=$GRAPH_ABS iters=$ITERS timeout=${TIMEOUT_SECONDS}s"
    if [[ -n "$ENV_SCRIPT" ]]; then
      echo "    env=$ENV_SCRIPT"
    fi
    echo "    log_dir=$LOG_DIR"
    if [[ -n "$DEVICE_PLATFORM" ]]; then
      echo "    device=$DEVICE_PLATFORM"
    fi
    if [[ -n "$KERNEL_FREQ" ]]; then
      echo "    kernel_freq_mhz=$KERNEL_FREQ"
    fi
    if [[ -n "$BIG_EDGE_PER_MS" ]]; then
      echo "    big_edge_per_ms=$BIG_EDGE_PER_MS"
    fi
    if [[ -n "$LITTLE_EDGE_PER_MS" ]]; then
      echo "    little_edge_per_ms=$LITTLE_EDGE_PER_MS"
    fi
  } >>"$BUILD_LOG"
}

merge_logs() {
  if [[ -n "$RUN_LOG" && -f "$RUN_LOG" ]]; then
    cat "$BUILD_LOG" "$RUN_LOG" >"$LOG_FILE"
  else
    cp "$BUILD_LOG" "$LOG_FILE"
  fi
}

ensure_build_env() {
  if [[ -n "$ENV_SCRIPT" ]]; then
    return 0
  fi
  if [[ -n "${XILINX_VITIS:-}" && -n "${XILINX_XRT:-}" ]]; then
    return 0
  fi
  {
    echo "Error: build requested but XILINX_VITIS/XILINX_XRT are not set."
    echo "Source your Vitis/XRT environment first or pass --env /path/to/env.sh."
  } >>"$BUILD_LOG"
  merge_logs
  echo "==> BUILD FAILED (see $LOG_FILE)" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
}

append_build_summary

MAKE_ARGS=( "TARGET=$MODE" )
if [[ -n "$DEVICE_PLATFORM" ]]; then
  MAKE_ARGS+=( "DEVICE=$DEVICE_PLATFORM" )
fi
if [[ -n "$KERNEL_FREQ" ]]; then
  MAKE_ARGS+=( "KERNEL_FREQ=$KERNEL_FREQ" )
fi
if [[ -n "$BIG_EDGE_PER_MS" ]]; then
  MAKE_ARGS+=( "BIG_EDGE_PER_MS=$BIG_EDGE_PER_MS" )
fi
if [[ -n "$LITTLE_EDGE_PER_MS" ]]; then
  MAKE_ARGS+=( "LITTLE_EDGE_PER_MS=$LITTLE_EDGE_PER_MS" )
fi

HOST_EXE="$PROJECT_DIR/graphyflow_host"
XCLBIN_FILE="$PROJECT_DIR/xclbin/graphyflow_kernels.${MODE}.xclbin"
NEED_XCLBIN_BUILD="0"
NEED_EXE_BUILD="0"
FORCE_HOST_MACRO_REBUILD="0"
if [[ -n "$BIG_EDGE_PER_MS" || -n "$LITTLE_EDGE_PER_MS" ]]; then
  FORCE_HOST_MACRO_REBUILD="1"
fi
if [[ "$BUILD_KERNELS" == "1" || ! -f "$XCLBIN_FILE" ]]; then
  NEED_XCLBIN_BUILD="1"
fi
if [[ "$REBUILD_EXE" == "1" || ! -x "$HOST_EXE" ]]; then
  NEED_EXE_BUILD="1"
fi
if [[ "$FORCE_HOST_MACRO_REBUILD" == "1" ]]; then
  NEED_EXE_BUILD="1"
fi

if [[ "$FORCE_HOST_MACRO_REBUILD" == "1" && "$BUILD_KERNELS" != "1" && "$BUILD_ONLY" != "1" && ! -f "$XCLBIN_FILE" ]]; then
  {
    echo "Error: $XCLBIN_FILE is missing."
    echo "BIG_EDGE_PER_MS/LITTLE_EDGE_PER_MS overrides are host-only and will not trigger a kernel rebuild."
    echo "Build kernels first or rerun with --build-kernels if you want to rebuild the xclbin."
  } >>"$BUILD_LOG"
  merge_logs
  echo "==> FAIL (missing xclbin, see $LOG_FILE)" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

if [[ "$REBUILD_EXE" == "1" || "$FORCE_HOST_MACRO_REBUILD" == "1" ]]; then
  ensure_build_env
  {
    if [[ "$FORCE_HOST_MACRO_REBUILD" == "1" && "$REBUILD_EXE" != "1" ]]; then
      echo "==> forcing clean host rebuild for BIG_EDGE_PER_MS/LITTLE_EDGE_PER_MS overrides"
    fi
    echo "==> cleanexe"
    run_in_project make cleanexe
  } >>"$BUILD_LOG" 2>&1
fi

if [[ "$NEED_XCLBIN_BUILD" == "1" ]]; then
  ensure_build_env
  {
    echo "==> make all ${MAKE_ARGS[*]}"
    run_in_project make all "${MAKE_ARGS[@]}"
  } >>"$BUILD_LOG" 2>&1 || {
    merge_logs
    echo "==> BUILD FAILED (see $LOG_FILE)" >&2
    tail -n 120 "$LOG_FILE" >&2 || true
    exit 1
  }
elif [[ "$NEED_EXE_BUILD" == "1" ]]; then
  ensure_build_env
  {
    echo "==> make exe ${MAKE_ARGS[*]}"
    run_in_project make exe "${MAKE_ARGS[@]}"
  } >>"$BUILD_LOG" 2>&1 || {
    merge_logs
    echo "==> EXE BUILD FAILED (see $LOG_FILE)" >&2
    tail -n 120 "$LOG_FILE" >&2 || true
    exit 1
  }
fi

if [[ "$BUILD_ONLY" == "1" ]]; then
  echo "==> build_only=1" >>"$BUILD_LOG"
  merge_logs
  echo "==> BUILD-ONLY OK"
  echo "    log=$LOG_FILE"
  exit 0
fi

if [[ "$MODE" == "hw" ]]; then
  RUN_LOG="$(mktemp "${TMPDIR:-/tmp}/graphyflow_run_${PROJECT}_${MODE}_XXXXXX.log")"
  WATCHDOG_ARGS=(
    --project "$PROJECT_DIR"
    --dataset "$GRAPH_ABS"
    --iters "$ITERS"
    --timeout "$TIMEOUT_SECONDS"
    --log "$RUN_LOG"
  )
  if [[ -n "$DEVICE_BDF" ]]; then
    WATCHDOG_ARGS+=( --device "$DEVICE_BDF" )
  fi
  if [[ -n "$DEVICE_INDEX" ]]; then
    WATCHDOG_ARGS+=( --device-index "$DEVICE_INDEX" )
  fi
  if GRAPHYFLOW_ENV_SH="$ENV_SCRIPT" GRAPHYFLOW_ALLOW_MISMATCH=0 \
      "$SCRIPT_DIR/run_hw_with_watchdog.sh" "${WATCHDOG_ARGS[@]}"; then
    merge_logs
    if grep -q "SUCCESS: Results match" "$LOG_FILE"; then
      echo "==> PASS"
      echo "    log=$LOG_FILE"
      exit 0
    fi
    echo "==> FAIL (missing success marker, see $LOG_FILE)" >&2
    tail -n 120 "$LOG_FILE" >&2 || true
    exit 1
  fi

  merge_logs
  echo "==> FAIL (see $LOG_FILE)" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
fi

{
  echo "==> ./run.sh $MODE $GRAPH_ABS"
  (
    cd "$PROJECT_DIR"
    if [[ -n "$ENV_SCRIPT" ]]; then
      source_env_script "$ENV_SCRIPT"
    fi
    export GRAPHYFLOW_ENV_SH="$ENV_SCRIPT"
    export GRAPHYFLOW_MAX_ITERS="$ITERS"
    export GRAPHYFLOW_ALLOW_MISMATCH=0
    stdbuf -oL -eL timeout "${TIMEOUT_SECONDS}s" ./run.sh "$MODE" "$GRAPH_ABS"
  )
} >>"$BUILD_LOG" 2>&1 || {
  merge_logs
  echo "==> FAIL (see $LOG_FILE)" >&2
  tail -n 120 "$LOG_FILE" >&2 || true
  exit 1
}

merge_logs
if grep -q "SUCCESS: Results match" "$LOG_FILE"; then
  echo "==> PASS"
  echo "    log=$LOG_FILE"
  exit 0
fi

echo "==> FAIL (missing success marker, see $LOG_FILE)" >&2
tail -n 120 "$LOG_FILE" >&2 || true
exit 1
