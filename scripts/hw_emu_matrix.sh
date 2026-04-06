#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/hw_emu_matrix.sh --cases <csv> [options]

Run hw_emu cases in Docker with timeout/retry and write a TSV summary.

Case CSV format (no header):
  project,graph_path,timeout_sec,label

Example:
  connected_components_w4,target/graph_100_200.txt,1800,cc_100_200

Options:
  --cases <csv>            Input case CSV (required)
  --summary <tsv>          Output summary file (default: target/parallel_logs/matrix_<ts>.tsv)
  --resume                 If --summary exists, append and skip already-PASS cases (default: off)
  --retries <n>            Retry count on fail (default: 1)
  --busy-retries <n>       Extra retries if device is busy (default: 6)
  --external-retries <n>   Extra retries if container disappears (EXTERNAL) (default: 6)
  --stall-seconds <n>      No-log-progress watchdog threshold in seconds (default: 900)
  --no-log-seconds <n>     Kill if log size doesn't change for N seconds (default: 0 = disabled)
  --poll-seconds <n>       Watchdog poll interval seconds (default: 15)
  --min-timeout-sec <n>    Clamp per-case timeout up to at least N seconds (default: 0 = disabled)
  --fixed-iters <n>        Force a fixed iteration count via GRAPHYFLOW_MAX_ITERS (default: unset)
  --pr-fixed-iters <n>     For pagerank projects, set fixed iteration count and rebuild host exe once/project
  --keep-container         Do not use --rm and do not auto-remove the docker container (default: off)
  --image <tag>            Docker image (default: vivado-runner:22.04-feiyang)
  --container-prefix <p>   Docker container name prefix (default: gf_hwemu)
  --name-mode <mode>       Container naming mode: full|hash (default: full)
  --repo-label <value>     Value for docker label graphyflow.repo (default: repo root path)
  --owner-label <value>    Value for docker label graphyflow.owner (default: current user)
  --repo-mount <path>      Repo mount destination in container (default: /vitis_work/refactor_Graphyflow)
  --platform-dir <path>    Host platform dir (default: /path/to/platform/xilinx_u55c)
  --device <path>          DEVICE path inside container (default: /vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm)
  --xilinx-volume <name>   Docker volume for /opt/Xilinx (default: vitis-2024.2)
  --help                   Show this help
USAGE
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CASES=""
SUMMARY=""
RESUME=0
RETRIES=1
BUSY_RETRIES=6
EXTERNAL_RETRIES=6
STALL_SECONDS=900
NO_LOG_SECONDS=0
POLL_SECONDS=15
MIN_TIMEOUT_SEC=0
FIXED_ITERS=""
PR_FIXED_ITERS=""
KEEP_CONTAINER=0
IMAGE="vivado-runner:22.04-feiyang"
CONTAINER_PREFIX="gf_hwemu"
NAME_MODE="full"
REPO_LABEL=""
OWNER_LABEL=""
REPO_MOUNT="/vitis_work/refactor_Graphyflow"
PLATFORM_DIR="/path/to/platform/xilinx_u55c"
DEVICE_PATH="/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm"
XILINX_VOLUME="vitis-2024.2"
DOCKER=( env -u DOCKER_HOST docker )
uid="$(id -u)"
gid="$(id -g)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cases) CASES="${2:?}"; shift 2 ;;
    --summary) SUMMARY="${2:?}"; shift 2 ;;
    --resume) RESUME=1; shift 1 ;;
    --retries) RETRIES="${2:?}"; shift 2 ;;
    --busy-retries) BUSY_RETRIES="${2:?}"; shift 2 ;;
    --external-retries) EXTERNAL_RETRIES="${2:?}"; shift 2 ;;
    --stall-seconds) STALL_SECONDS="${2:?}"; shift 2 ;;
    --no-log-seconds) NO_LOG_SECONDS="${2:?}"; shift 2 ;;
    --poll-seconds) POLL_SECONDS="${2:?}"; shift 2 ;;
    --min-timeout-sec) MIN_TIMEOUT_SEC="${2:?}"; shift 2 ;;
    --fixed-iters) FIXED_ITERS="${2:?}"; shift 2 ;;
    --pr-fixed-iters) PR_FIXED_ITERS="${2:?}"; shift 2 ;;
    --keep-container) KEEP_CONTAINER=1; shift 1 ;;
    --image) IMAGE="${2:?}"; shift 2 ;;
    --container-prefix) CONTAINER_PREFIX="${2:?}"; shift 2 ;;
    --name-mode) NAME_MODE="${2:?}"; shift 2 ;;
    --repo-label) REPO_LABEL="${2:?}"; shift 2 ;;
    --owner-label) OWNER_LABEL="${2:?}"; shift 2 ;;
    --repo-mount) REPO_MOUNT="${2:?}"; shift 2 ;;
    --platform-dir) PLATFORM_DIR="${2:?}"; shift 2 ;;
    --device) DEVICE_PATH="${2:?}"; shift 2 ;;
    --xilinx-volume) XILINX_VOLUME="${2:?}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$CASES" ]]; then
  echo "--cases is required" >&2
  exit 2
fi
if [[ ! -f "$CASES" ]]; then
  echo "cases file not found: $CASES" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/target/parallel_logs"

if [[ -z "$SUMMARY" ]]; then
  SUMMARY="$REPO_ROOT/target/parallel_logs/matrix_$(date +%Y%m%d_%H%M%S).tsv"
fi
if [[ "$SUMMARY" != /* ]]; then
  SUMMARY="$REPO_ROOT/$SUMMARY"
fi

declare -A PR_PATCHED
declare -A ALREADY_PASSED
declare -A NEXT_ATTEMPT
LAST_STATUS=""

if [[ -z "$REPO_LABEL" ]]; then
  REPO_LABEL="$REPO_ROOT"
fi
if [[ -z "$OWNER_LABEL" ]]; then
  OWNER_LABEL="${GRAPHYFLOW_OWNER:-$(id -un)}"
fi

stub_dir="$REPO_ROOT/target/stubs"
mkdir -p "$stub_dir"
gcc -shared -fPIC -Wl,-soname,libudev.so.1 \
  -o "$stub_dir/libudev.so.1" \
  "$REPO_ROOT/scripts/libudev_stub.c"

opencl_vendor_dir="$REPO_ROOT/target/opencl_vendors"
mkdir -p "$opencl_vendor_dir"
echo "/opt/xilinx/xrt/lib/libxilinxopencl.so" > "$opencl_vendor_dir/xilinx.icd"

docker_common=(
  # Intentionally do NOT use `--rm` here.
  #
  # Rationale:
  # - When a case exits with `rc=137` (SIGKILL) we often want to `docker inspect`
  #   the container state (OOMKilled, ExitCode) for diagnostics.
  # - We remove containers explicitly at the end of each case unless
  #   `--keep-container` is set.
  "${DOCKER[@]}" run --platform linux/amd64 --user "${uid}:${gid}" --shm-size=2g
  --label "graphyflow.repo=${REPO_LABEL}"
  --label "graphyflow.owner=${OWNER_LABEL}"
  --label "graphyflow.kind=hw_emu"
  -v "${XILINX_VOLUME}:/opt/Xilinx"
  -v /opt/xilinx:/opt/xilinx:ro
  -v "$opencl_vendor_dir:/etc/OpenCL/vendors:ro"
  -v "$stub_dir:/tmp/graphyflow_stubs:ro"
  -e XILINX_XRT=/opt/xilinx/xrt
  -e GRAPHYFLOW_DEVICE_LOCK_DIR=/tmp/graphyflow_device_locks
  -v /tmp/graphyflow_device_locks:/tmp/graphyflow_device_locks
  -v "$REPO_ROOT:$REPO_MOUNT"
  -v "$PLATFORM_DIR:/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1"
)

extra_env=()
if [[ -n "$FIXED_ITERS" ]]; then
  extra_env+=( -e "GRAPHYFLOW_MAX_ITERS=$FIXED_ITERS" )
fi
if [[ -n "${GRAPHYFLOW_EVENT_WATCHDOG_SECONDS:-}" ]]; then
  extra_env+=( -e "GRAPHYFLOW_EVENT_WATCHDOG_SECONDS=${GRAPHYFLOW_EVENT_WATCHDOG_SECONDS}" )
fi
if [[ -n "${GRAPHYFLOW_DEBUG_EVENTS:-}" ]]; then
  extra_env+=( -e "GRAPHYFLOW_DEBUG_EVENTS=${GRAPHYFLOW_DEBUG_EVENTS}" )
fi
if [[ -n "${GRAPHYFLOW_DUMP_PARTITIONS:-}" ]]; then
  extra_env+=( -e "GRAPHYFLOW_DUMP_PARTITIONS=${GRAPHYFLOW_DUMP_PARTITIONS}" )
fi

cleanup_project_processes() {
  local project="$1"
  local graph_container="$2"
  local emconfig="EMCONFIG_PATH=${REPO_MOUNT}/target/generated_hls/${project}"

  pkill -TERM -f "$emconfig" >/dev/null 2>&1 || true
  pkill -TERM -f "graphyflow_host ./xclbin/graphyflow_kernels.hw_emu.xclbin ${graph_container}" >/dev/null 2>&1 || true
  sleep 2
  pkill -KILL -f "$emconfig" >/dev/null 2>&1 || true
  pkill -KILL -f "graphyflow_host ./xclbin/graphyflow_kernels.hw_emu.xclbin ${graph_container}" >/dev/null 2>&1 || true
}

ensure_pr_iterations() {
  local project="$1"
  if [[ -z "$PR_FIXED_ITERS" ]]; then
    return
  fi
  if [[ "$project" != pagerank* && "$project" != pr* ]]; then
    return
  fi
  if [[ -n "${PR_PATCHED[$project]:-}" ]]; then
    return
  fi

  local proj_dir="${REPO_MOUNT}/target/generated_hls/${project}"
  echo "[prep] setting ${project} pagerank fixed iterations to ${PR_FIXED_ITERS}"
  "${docker_common[@]}" \
    -w "$proj_dir" "$IMAGE" bash -lc \
    "set -euo pipefail; \
     sed -i -E 's@(\/\*max_iterations=\*\/)[0-9]+,@\1${PR_FIXED_ITERS},@' scripts/host/algorithm_config.h; \
     source /opt/Xilinx/Vitis/2024.1/settings64.sh; \
     export LD_LIBRARY_PATH=\"/tmp/graphyflow_stubs:${LD_LIBRARY_PATH:-}\"; \
     make exe TARGET=hw_emu DEVICE=${DEVICE_PATH}" >/dev/null

  PR_PATCHED["$project"]=1
}

run_case_once() {
  local project="$1"
  local graph_rel="$2"
  local timeout_sec="$3"
  local label="$4"
  local attempt="$5"

  local graph_abs="$graph_rel"
  if [[ "$graph_abs" != /* ]]; then
    graph_abs="$REPO_ROOT/$graph_abs"
  fi
  if [[ ! -f "$graph_abs" ]]; then
    echo "graph not found: $graph_abs" >&2
    return 98
  fi

  local graph_container="${graph_abs/#$REPO_ROOT/$REPO_MOUNT}"
  local stamp
  stamp="$(date +%Y%m%d_%H%M%S)"
  local log_file="$REPO_ROOT/target/parallel_logs/${label}_${project}_a${attempt}_${stamp}.log"
  local proj_dir="${REPO_MOUNT}/target/generated_hls/${project}"
  local safe_project safe_label safe_prefix container_name
  safe_project="$(echo "$project" | tr -c '[:alnum:]_.-' '_')"
  safe_label="$(echo "$label" | tr -c '[:alnum:]_.-' '_')"
  safe_prefix="$(echo "$CONTAINER_PREFIX" | tr -c '[:alnum:]_.-' '_' | sed 's/^_\\+//;s/_\\+$//')"
  if [[ -z "$safe_prefix" ]]; then
    safe_prefix="gf_hwemu"
  fi
  case "$NAME_MODE" in
    full)
      container_name="${safe_prefix}_${safe_project}_${safe_label}_a${attempt}_${stamp}"
      ;;
    hash)
      short_id="$(printf '%s' "${project}|${graph_rel}|${label}" | sha1sum | awk '{print substr($1,1,10)}')"
      container_name="${safe_prefix}_${short_id}_a${attempt}_${stamp}"
      ;;
    *)
      echo "unknown --name-mode: $NAME_MODE (expected: full|hash)" >&2
      return 2
      ;;
  esac

  local start end elapsed rc status
  local start_iso end_iso
  local stalled=0
  local success_marker_seen=0
  local container_removed_by_watchdog=0
  local container_missing_external=0

  if [[ "$MIN_TIMEOUT_SEC" =~ ^[0-9]+$ ]] && (( MIN_TIMEOUT_SEC > 0 )); then
    if [[ "$timeout_sec" =~ ^[0-9]+$ ]] && (( timeout_sec < MIN_TIMEOUT_SEC )); then
      timeout_sec="$MIN_TIMEOUT_SEC"
    fi
  fi

  start="$(date +%s)"
  start_iso="$(date -Iseconds)"

  echo "[start] ${project} label=${label} attempt=${attempt} timeout=${timeout_sec}s graph=${graph_rel} container=${container_name} log=${log_file}"

  set +e
  timeout --signal=TERM --kill-after=30s "${timeout_sec}s" "${docker_common[@]}" \
    --name "$container_name" \
    --label "graphyflow.project=${project}" \
    --label "graphyflow.label=${label}" \
    "${extra_env[@]}" \
    -e GRAPHYFLOW_ENV_SH=/opt/xilinx/xrt/setup.sh \
    -e GRAPHYFLOW_ALLOW_MISMATCH=0 \
    -e GRAPHYFLOW_RUN_MODE=hw_emu \
    -e "GRAPHYFLOW_PROJECT=${project}" \
    -e "GRAPHYFLOW_REPO_MOUNT=${REPO_MOUNT}" \
    -e "GRAPHYFLOW_GRAPH_CONTAINER=${graph_container}" \
    -w "$proj_dir" "$IMAGE" bash -lc \
    'set -euo pipefail
     export LD_LIBRARY_PATH="/tmp/graphyflow_stubs:${LD_LIBRARY_PATH:-}"
     work_dir="/tmp/graphyflow_run/${GRAPHYFLOW_PROJECT}"
     rm -rf "$work_dir"
     mkdir -p "$work_dir"
     ( cd "${GRAPHYFLOW_REPO_MOUNT}/target/generated_hls/${GRAPHYFLOW_PROJECT}" && tar --exclude=.run --exclude=.Xil -cf - . ) | ( cd "$work_dir" && tar -xf - )
     cd "$work_dir"
     export EMCONFIG_PATH="$work_dir"
     source /opt/Xilinx/Vitis/2024.1/settings64.sh
     run_mode="${GRAPHYFLOW_RUN_MODE:-hw_emu}"
     ./run.sh "$run_mode" "${GRAPHYFLOW_GRAPH_CONTAINER}"' >"$log_file" 2>&1 &
  local run_pid=$!

  local last_size=0
  local last_iters=0
  local last_change
  last_change="$(date +%s)"
  local last_log_change
  last_log_change="$last_change"

  while kill -0 "$run_pid" 2>/dev/null; do
    sleep "$POLL_SECONDS"

    local cur_size=0
    local cur_iters=0
    if [[ -f "$log_file" ]]; then
      cur_size="$(stat -c %s "$log_file" 2>/dev/null || echo 0)"
      cur_iters="$(grep -c 'End-to-End Time' "$log_file" 2>/dev/null || echo 0)"
    fi
    if [[ "$cur_size" != "$last_size" ]]; then
      last_size="$cur_size"
      last_log_change="$(date +%s)"
    fi

    if [[ -f "$log_file" ]] && grep -q 'SUCCESS: Results match!' "$log_file"; then
      success_marker_seen=1
      echo "[info] success marker detected; stopping lingering simulator processes" >>"$log_file"
      pkill -TERM -P "$run_pid" >/dev/null 2>&1 || true
      kill -TERM "$run_pid" >/dev/null 2>&1 || true
      if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
        "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
        container_removed_by_watchdog=1
      fi
      sleep 2
      pkill -KILL -P "$run_pid" >/dev/null 2>&1 || true
      kill -KILL "$run_pid" >/dev/null 2>&1 || true
      if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
        "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
        container_removed_by_watchdog=1
      fi
      cleanup_project_processes "$project" "$graph_container"
      break
    fi

    # Treat only completed-iteration markers as progress.
    #
    # Some builds enable a host-side event watchdog that periodically prints
    # event dumps; those lines grow the log file even when the kernel is stuck,
    # so "log size changed" is not a reliable progress signal.
    if [[ "$cur_iters" != "$last_iters" ]]; then
      last_iters="$cur_iters"
      last_change="$(date +%s)"
      continue
    fi

    local now idle
    now="$(date +%s)"
    idle=$((now - last_change))
    if (( idle >= STALL_SECONDS )); then
      echo "[stall] no log progress for ${idle}s (project=${project}, graph=${graph_rel}), terminating run" >>"$log_file"
      pkill -TERM -P "$run_pid" >/dev/null 2>&1 || true
      kill -TERM "$run_pid" >/dev/null 2>&1 || true
      if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
        "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
        container_removed_by_watchdog=1
      fi
      sleep 3
      pkill -KILL -P "$run_pid" >/dev/null 2>&1 || true
      kill -KILL "$run_pid" >/dev/null 2>&1 || true
      if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
        "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
        container_removed_by_watchdog=1
      fi
      cleanup_project_processes "$project" "$graph_container"
      stalled=1
      break
    fi

    if (( NO_LOG_SECONDS > 0 )); then
      local log_idle
      log_idle=$((now - last_log_change))
      if (( log_idle >= NO_LOG_SECONDS )); then
        echo "[stall] log size unchanged for ${log_idle}s (project=${project}, graph=${graph_rel}), terminating run" >>"$log_file"
        pkill -TERM -P "$run_pid" >/dev/null 2>&1 || true
        kill -TERM "$run_pid" >/dev/null 2>&1 || true
        if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
          "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
          container_removed_by_watchdog=1
        fi
        sleep 3
        pkill -KILL -P "$run_pid" >/dev/null 2>&1 || true
        kill -KILL "$run_pid" >/dev/null 2>&1 || true
        if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
          "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
          container_removed_by_watchdog=1
        fi
        cleanup_project_processes "$project" "$graph_container"
        stalled=1
        break
      fi
    fi
  done

  wait "$run_pid"
  rc=$?
  set -e

  end_iso="$(date -Iseconds)"

  # Attach docker events for this container (helps diagnose external kills / rm -f).
  #
  # Note: this uses a bounded time window so it terminates even if the container
  # is already gone.
  {
    echo "[docker] events (since=${start_iso}, until=${end_iso}):"
    "${DOCKER[@]}" events \
      --since "$start_iso" \
      --until "$end_iso" \
      --filter "container=${container_name}" 2>/dev/null \
      | head -n 200 \
      || true
  } >>"$log_file"

  # Capture container exit diagnostics before removing it (when possible).
  if "${DOCKER[@]}" inspect "$container_name" >/dev/null 2>&1; then
    {
      echo "[docker] inspect:"
      "${DOCKER[@]}" inspect -f '  OOMKilled={{.State.OOMKilled}} ExitCode={{.State.ExitCode}} Error={{.State.Error}} FinishedAt={{.State.FinishedAt}}' "$container_name" || true
    } >>"$log_file"
  else
    if [[ "$container_removed_by_watchdog" -eq 1 ]]; then
      echo "[docker] inspect: container already removed by watchdog/cleanup in this script" >>"$log_file"
    else
      echo "[docker] inspect: container not found (possibly removed externally)" >>"$log_file"
      container_missing_external=1
    fi
  fi

  if [[ "$KEEP_CONTAINER" -eq 0 ]]; then
    "${DOCKER[@]}" rm -f "$container_name" >/dev/null 2>&1 || true
  fi

  if grep -q 'SUCCESS: Results match!' "$log_file"; then
    success_marker_seen=1
    status="PASS"
    if [[ "$rc" -ne 0 ]]; then
      echo "[warn] non-zero rc=${rc} but SUCCESS marker found; treating as PASS" >>"$log_file"
    fi
  elif (( stalled == 1 )); then
    status="STALL"
  elif (( container_missing_external == 1 )); then
    status="EXTERNAL"
  else
    status="FAIL"
  fi

  # Treat "device busy" as a retriable transient. This most often happens when another
  # worker is using the same U55C and does not respect GraphyFlow's lock directory.
  if [[ "$status" != "PASS" ]] && grep -qE 'device\\[[0-9]+\\] is busy/in use; skipping|Failed to initialize accelerator on all available devices' "$log_file"; then
    status="BUSY"
  fi

  end="$(date +%s)"
  elapsed=$((end - start))

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$project" "$graph_rel" "$label" "$attempt" "$status" "$rc" "$elapsed" "$log_file" >> "$SUMMARY"

  echo "[${status}] ${project} label=${label} attempt=${attempt} rc=${rc} elapsed=${elapsed}s log=${log_file}"
  if [[ "$status" == "BUSY" ]]; then
    sleep 30
  fi
  LAST_STATUS="$status"
  [[ "$status" == "PASS" ]]
}

if [[ "$RESUME" -eq 1 && -f "$SUMMARY" ]]; then
  # Build skip-set: key = project<TAB>graph<TAB>label
  while IFS=$'\t' read -r p g l _a s _rc _el _log; do
    key="$p\t$g\t$l"
    # Record next attempt as max(attempt)+1 so a resume run won't reuse attempt
    # numbers (which would confuse downstream "final attempt" logic).
    if [[ -n "${_a:-}" ]]; then
      a_num=$((_a + 0))
      if [[ -z "${NEXT_ATTEMPT[$key]:-}" || "$a_num" -ge "${NEXT_ATTEMPT[$key]}" ]]; then
        NEXT_ATTEMPT["$key"]=$((a_num + 1))
      fi
    fi
    [[ "$s" != "PASS" ]] && continue
    ALREADY_PASSED["$key"]=1
  done < <(tail -n +2 "$SUMMARY" 2>/dev/null || true)
else
  mkdir -p "$(dirname "$SUMMARY")"
  echo -e "project\tgraph\tlabel\tattempt\tstatus\trc\telapsed_sec\tlog" > "$SUMMARY"
fi

while IFS=',' read -r project graph_path timeout_sec label; do
  [[ -z "${project// }" ]] && continue
  [[ "${project:0:1}" == "#" ]] && continue

  project="$(echo "$project" | xargs)"
  graph_path="$(echo "$graph_path" | xargs)"
  timeout_sec="$(echo "$timeout_sec" | xargs)"
  label="$(echo "$label" | xargs)"

  if [[ "$RESUME" -eq 1 ]]; then
    key="${project}\t${graph_path}\t${label}"
    if [[ -n "${ALREADY_PASSED[$key]:-}" ]]; then
      echo "[skip] already PASS: ${project},${graph_path},${timeout_sec},${label}"
      continue
    fi
  fi

  ensure_pr_iterations "$project"

  ok=0
  busy_left="$BUSY_RETRIES"
  external_left="$EXTERNAL_RETRIES"
  fail_left="$RETRIES"
  if [[ "$RESUME" -eq 1 ]]; then
    key="${project}\t${graph_path}\t${label}"
    attempt="${NEXT_ATTEMPT[$key]:-0}"
  else
    attempt=0
  fi
  while :; do
    if run_case_once "$project" "$graph_path" "$timeout_sec" "$label" "$attempt"; then
      ok=1
      break
    fi

    if [[ "${LAST_STATUS:-}" == "BUSY" && "$busy_left" -gt 0 ]]; then
      busy_left=$((busy_left - 1))
      attempt=$((attempt + 1))
      continue
    fi

    if [[ "${LAST_STATUS:-}" == "EXTERNAL" && "$external_left" -gt 0 ]]; then
      external_left=$((external_left - 1))
      attempt=$((attempt + 1))
      sleep 10
      continue
    fi

    if [[ "$fail_left" -gt 0 ]]; then
      fail_left=$((fail_left - 1))
      attempt=$((attempt + 1))
      continue
    fi

    break
  done

  if [[ "$ok" -ne 1 ]]; then
    echo "[warn] case failed after retries: ${project},${graph_path},${timeout_sec},${label}" >&2
  fi
done < "$CASES"

echo "summary: $SUMMARY"
