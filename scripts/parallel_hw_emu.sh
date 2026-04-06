#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/parallel_hw_emu.sh [options]

Runs hw_emu builds and/or runs in parallel docker containers with a memory budget.

Options:
  --projects <csv>        Project names under target/generated_hls (default: all)
  --graph <path>          Graph file path (default: target/graph_small_weighted.txt)
  --target <t>            Vitis target: hw_emu | sw_emu | hw (default: hw_emu)
  --max-iters <n>         Force host to run a fixed number of iterations (sets GRAPHYFLOW_MAX_ITERS)
  --kernel-freq <mhz>     Pass KERNEL_FREQ=<mhz> to kernel builds (optional)
  --build-only            Only build projects
  --run-only              Only run projects
  --no-build              Skip builds (alias of --run-only)
  --no-run                Skip runs (alias of --build-only)
  --build-mem <MiB>       Peak MiB for one build container
  --run-mem <MiB>         Peak MiB for one run container
  --reserve-gib <GiB>     Host memory to reserve (default: 8)
  --max-builds <n>        Hard cap on parallel builds (default: derived)
  --max-runs <n>          Hard cap on parallel runs (default: derived)
  --image <tag>           Docker image (default: vivado-runner:22.04-feiyang)
  --vitis-volume <name>   Docker volume for /opt/Xilinx (default: vitis-2024.2)
  --platform-dir <path>   Host platform directory (default: u55c path)
  --device <path>         Override DEVICE path inside container
  --measure-only          Measure peak build/run memory then exit
  -h, --help              Show this help
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="vivado-runner:22.04-feiyang"
DOCKER=( env -u DOCKER_HOST docker )
VITIS_VOLUME="vitis-2024.2"
PLATFORM_DIR="/path/to/platform/xilinx_u55c"
DEVICE_PATH="/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm"
TARGET="hw_emu"
PROJECTS_CSV=""
GRAPH_PATH="target/graph_small_weighted.txt"
MAX_ITERS=""
KERNEL_FREQ=""
BUILD_ENABLED=1
RUN_ENABLED=1
BUILD_MEM_MIB=""
RUN_MEM_MIB=""
RESERVE_GIB="8"
MAX_BUILDS=""
MAX_RUNS=""
MEASURE_ONLY=0
LOG_DIR="$REPO_ROOT/target/parallel_logs"
uid="$(id -u)"
gid="$(id -g)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --projects) PROJECTS_CSV="${2:?}"; shift 2 ;;
    --graph) GRAPH_PATH="${2:?}"; shift 2 ;;
    --target) TARGET="${2:?}"; shift 2 ;;
    --max-iters) MAX_ITERS="${2:?}"; shift 2 ;;
    --kernel-freq) KERNEL_FREQ="${2:?}"; shift 2 ;;
    --build-only|--no-run) RUN_ENABLED=0; shift ;;
    --run-only|--no-build) BUILD_ENABLED=0; shift ;;
    --build-mem) BUILD_MEM_MIB="${2:?}"; shift 2 ;;
    --run-mem) RUN_MEM_MIB="${2:?}"; shift 2 ;;
    --reserve-gib) RESERVE_GIB="${2:?}"; shift 2 ;;
    --max-builds) MAX_BUILDS="${2:?}"; shift 2 ;;
    --max-runs) MAX_RUNS="${2:?}"; shift 2 ;;
    --log-dir) LOG_DIR="${2:?}"; shift 2 ;;
    --image) IMAGE="${2:?}"; shift 2 ;;
    --vitis-volume) VITIS_VOLUME="${2:?}"; shift 2 ;;
    --platform-dir) PLATFORM_DIR="${2:?}"; shift 2 ;;
    --device) DEVICE_PATH="${2:?}"; shift 2 ;;
    --measure-only) MEASURE_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$TARGET" in
  hw_emu|sw_emu|hw) ;;
  *)
    echo "--target must be hw_emu, sw_emu, or hw (got '$TARGET')" >&2
    exit 2
    ;;
esac

if [[ ! -d "$PLATFORM_DIR" ]]; then
  echo "platform directory not found: $PLATFORM_DIR" >&2
  exit 1
fi

GRAPH_HOST="$GRAPH_PATH"
if [[ "$GRAPH_HOST" != /* ]]; then
  GRAPH_HOST="$REPO_ROOT/$GRAPH_HOST"
fi
if [[ ! -f "$GRAPH_HOST" ]]; then
  echo "graph file not found: $GRAPH_HOST" >&2
  exit 1
fi
if [[ "$GRAPH_HOST" != "$REPO_ROOT"* ]]; then
  echo "graph file must be inside repo so it is mounted into docker" >&2
  exit 1
fi
GRAPH_CONTAINER="${GRAPH_HOST/#$REPO_ROOT/\/vitis_work\/refactor_Graphyflow}"

discover_projects() {
  local root="$REPO_ROOT/target/generated_hls"
  if [[ -n "$PROJECTS_CSV" ]]; then
    IFS=',' read -r -a projects <<<"$PROJECTS_CSV"
    echo "${projects[@]}"
    return
  fi
  if [[ ! -d "$root" ]]; then
    echo "generated_hls directory not found: $root" >&2
    exit 1
  fi
  mapfile -t projects < <(find "$root" -maxdepth 1 -mindepth 1 -type d -printf '%f\n' | sort)
  echo "${projects[@]}"
}

sanitize_name() {
  echo "$1" | tr -c 'A-Za-z0-9_.-' '_' | sed 's/_$//'
}

docker_common_args=(
  run --rm --platform linux/amd64 --user "${uid}:${gid}" --shm-size=8g
  -v "${VITIS_VOLUME}:/opt/Xilinx"
  -v /opt/xilinx:/opt/xilinx:ro
  -v "$REPO_ROOT/target/opencl_vendors:/etc/OpenCL/vendors:ro"
  -e XILINX_XRT=/opt/xilinx/xrt
  -e GRAPHYFLOW_DEVICE_LOCK_DIR=/tmp/graphyflow_device_locks
  -v /tmp/graphyflow_device_locks:/tmp/graphyflow_device_locks
  -v "$REPO_ROOT:/vitis_work/refactor_Graphyflow"
  -v "$PLATFORM_DIR:/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1"
)
max_iters_env=()
if [[ -n "$MAX_ITERS" ]]; then
  max_iters_env=( -e "GRAPHYFLOW_MAX_ITERS=$MAX_ITERS" )
fi
kernel_freq_make=()
if [[ -n "$KERNEL_FREQ" ]]; then
  kernel_freq_make=( "KERNEL_FREQ=$KERNEL_FREQ" )
fi

stub_dir="$REPO_ROOT/target/stubs"
mkdir -p "$stub_dir"
gcc -shared -fPIC -Wl,-soname,libudev.so.1 \
  -o "$stub_dir/libudev.so.1" \
  "$REPO_ROOT/scripts/libudev_stub.c"
stub_mount_arg=( -v "$stub_dir:/tmp/graphyflow_stubs:ro" )
stub_env_cmd='export LD_LIBRARY_PATH="/tmp/graphyflow_stubs:${LD_LIBRARY_PATH:-}"; '

opencl_vendor_dir="$REPO_ROOT/target/opencl_vendors"
mkdir -p "$opencl_vendor_dir"
echo "/opt/xilinx/xrt/lib/libxilinxopencl.so" > "$opencl_vendor_dir/xilinx.icd"

docker_peak_mem="$REPO_ROOT/scripts/docker_peak_mem.sh"
if [[ ! -x "$docker_peak_mem" ]]; then
  chmod +x "$docker_peak_mem"
fi

mkdir -p "$LOG_DIR"

measure_peak() {
  local kind="$1"
  local project="$2"
  local name="gf_${kind}_mem_$(sanitize_name "$project")"
  local proj_dir="/vitis_work/refactor_Graphyflow/target/generated_hls/$project"
  local log_file="$LOG_DIR/${project}_${kind}_measure.log"
  if [[ "$kind" == "build" ]]; then
    local clean_cmd=""
    if [[ -n "$KERNEL_FREQ" ]]; then
      clean_cmd='rm -rf ./xclbin ./_x ./.ipcache; '
    fi
    "$docker_peak_mem" --name "$name" --log "$log_file" -- \
      "${DOCKER[@]}" "${docker_common_args[@]}" --name "$name" \
      "${stub_mount_arg[@]}" \
      -w "$proj_dir" "$IMAGE" bash -lc \
      "source /opt/Xilinx/Vitis/2024.1/settings64.sh && ${stub_env_cmd}${clean_cmd}make all TARGET=$TARGET DEVICE=$DEVICE_PATH ${kernel_freq_make[*]}"
  else
    "$docker_peak_mem" --name "$name" --log "$log_file" -- \
      "${DOCKER[@]}" "${docker_common_args[@]}" --name "$name" \
      "${stub_mount_arg[@]}" \
      "${max_iters_env[@]}" \
      -e GRAPHYFLOW_ENV_SH=/opt/xilinx/xrt/setup.sh \
      -e GRAPHYFLOW_ALLOW_MISMATCH=0 \
      -w "$proj_dir" "$IMAGE" bash -lc \
      "set -euo pipefail; \
       rm -rf \"/tmp/graphyflow_run/${project}\"; \
       mkdir -p \"/tmp/graphyflow_run/${project}\"; \
       ( cd \"$proj_dir\" && tar --exclude=.run --exclude=.Xil -cf - . ) | ( cd \"/tmp/graphyflow_run/${project}\" && tar -xf - ); \
       source /opt/Xilinx/Vitis/2024.1/settings64.sh; \
       ${stub_env_cmd} \
       cd \"/tmp/graphyflow_run/${project}\"; \
       export EMCONFIG_PATH=\"/tmp/graphyflow_run/${project}\"; \
       ./run.sh $TARGET $GRAPH_CONTAINER"
  fi
}

projects=( $(discover_projects) )
if [[ ${#projects[@]} -eq 0 ]]; then
  echo "no projects found to run" >&2
  exit 1
fi

if [[ "$MEASURE_ONLY" -eq 1 ]]; then
  sample="${projects[0]}"
  echo "Measuring peak memory on project: $sample"
  echo "build:"
  measure_peak build "$sample"
  echo "run:"
  measure_peak run "$sample"
  exit 0
fi

if [[ -z "$BUILD_MEM_MIB" || -z "$RUN_MEM_MIB" ]]; then
  echo "Missing --build-mem or --run-mem. Run with --measure-only first or provide values." >&2
  exit 2
fi

mem_total_mib="$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)"
reserve_mib=$((RESERVE_GIB * 1024))
if (( reserve_mib >= mem_total_mib )); then
  echo "reserve-gib (${RESERVE_GIB}) exceeds host memory" >&2
  exit 1
fi
mem_limit_mib=$((mem_total_mib - reserve_mib))

max_builds_by_mem=$((mem_limit_mib / BUILD_MEM_MIB))
if (( max_builds_by_mem < 1 )); then
  max_builds_by_mem=1
fi
if [[ -n "$MAX_BUILDS" ]]; then
  if (( MAX_BUILDS < max_builds_by_mem )); then
    max_builds_by_mem="$MAX_BUILDS"
  fi
fi
BUILD_SLOTS="$max_builds_by_mem"

max_runs_by_mem=$((mem_limit_mib / RUN_MEM_MIB))
if (( max_runs_by_mem < 1 )); then
  max_runs_by_mem=1
fi
max_runs_half=$((mem_limit_mib / (2 * RUN_MEM_MIB)))
if (( max_runs_half < 1 )); then
  max_runs_half=1
fi
if (( max_runs_half < max_runs_by_mem )); then
  max_runs_by_mem="$max_runs_half"
fi
if [[ -n "$MAX_RUNS" ]]; then
  if (( MAX_RUNS < max_runs_by_mem )); then
    max_runs_by_mem="$MAX_RUNS"
  fi
fi
RUN_SLOTS="$max_runs_by_mem"

echo "Host memory: ${mem_total_mib} MiB"
echo "Reserve: ${reserve_mib} MiB"
echo "Budget: ${mem_limit_mib} MiB"
echo "Build peak: ${BUILD_MEM_MIB} MiB"
echo "Run peak: ${RUN_MEM_MIB} MiB"
echo "Max parallel builds: ${BUILD_SLOTS}"
echo "Max parallel runs: ${RUN_SLOTS}"
if [[ -n "$KERNEL_FREQ" ]]; then
  echo "Kernel freq (KERNEL_FREQ): ${KERNEL_FREQ} MHz"
fi
echo "Projects: ${projects[*]}"

pending_builds=()
pending_runs=()

if (( BUILD_ENABLED == 1 )); then
  for project in "${projects[@]}"; do
    pending_builds+=( "$project" )
  done
fi

if (( RUN_ENABLED == 1 )) && (( BUILD_ENABLED == 0 )); then
  for project in "${projects[@]}"; do
    pending_runs+=( "$project" )
  done
fi

running_pid=()
running_type=()
running_project=()
running_mem=()
running_runs=0
running_builds=0
used_mem=0
failures=0

start_build() {
  local project="$1"
  local name="gf_build_$(sanitize_name "$project")_$$"
  local proj_dir="/vitis_work/refactor_Graphyflow/target/generated_hls/$project"
  local log_file="$LOG_DIR/${project}_build.log"
  echo "==> build start: $project"
  local clean_cmd=""
  if [[ -n "$KERNEL_FREQ" ]]; then
    clean_cmd='rm -rf ./xclbin ./_x ./.ipcache; '
  fi
  "${DOCKER[@]}" "${docker_common_args[@]}" --name "$name" \
    "${stub_mount_arg[@]}" \
    -w "$proj_dir" "$IMAGE" bash -lc \
    "source /opt/Xilinx/Vitis/2024.1/settings64.sh && ${stub_env_cmd}${clean_cmd}make all TARGET=$TARGET DEVICE=$DEVICE_PATH ${kernel_freq_make[*]}" >"$log_file" 2>&1 &
  local pid=$!
  running_pid+=( "$pid" )
  running_type+=( "build" )
  running_project+=( "$project" )
  running_mem+=( "$BUILD_MEM_MIB" )
  running_builds=$((running_builds + 1))
  used_mem=$((used_mem + BUILD_MEM_MIB))
}

start_run() {
  local project="$1"
  local name="gf_run_$(sanitize_name "$project")_$$"
  local proj_dir="/vitis_work/refactor_Graphyflow/target/generated_hls/$project"
  local log_file="$LOG_DIR/${project}_run.log"
  echo "==> run start: $project"
  "${DOCKER[@]}" "${docker_common_args[@]}" --name "$name" \
    "${stub_mount_arg[@]}" \
    "${max_iters_env[@]}" \
    -e GRAPHYFLOW_ENV_SH=/opt/xilinx/xrt/setup.sh \
    -e GRAPHYFLOW_ALLOW_MISMATCH=0 \
    -w "$proj_dir" "$IMAGE" bash -lc \
    "set -euo pipefail; \
     rm -rf \"/tmp/graphyflow_run/${project}\"; \
     mkdir -p \"/tmp/graphyflow_run/${project}\"; \
     ( cd \"$proj_dir\" && tar --exclude=.run --exclude=.Xil -cf - . ) | ( cd \"/tmp/graphyflow_run/${project}\" && tar -xf - ); \
     source /opt/Xilinx/Vitis/2024.1/settings64.sh; \
     ${stub_env_cmd} \
     cd \"/tmp/graphyflow_run/${project}\"; \
     export EMCONFIG_PATH=\"/tmp/graphyflow_run/${project}\"; \
     ./run.sh $TARGET $GRAPH_CONTAINER" >"$log_file" 2>&1 &
  local pid=$!
  running_pid+=( "$pid" )
  running_type+=( "run" )
  running_project+=( "$project" )
  running_mem+=( "$RUN_MEM_MIB" )
  running_runs=$((running_runs + 1))
  used_mem=$((used_mem + RUN_MEM_MIB))
}

remove_running() {
  local idx="$1"
  local last=$(( ${#running_pid[@]} - 1 ))
  if (( idx < 0 || idx > last )); then
    return
  fi
  if (( idx != last )); then
    running_pid[$idx]="${running_pid[$last]}"
    running_type[$idx]="${running_type[$last]}"
    running_project[$idx]="${running_project[$last]}"
    running_mem[$idx]="${running_mem[$last]}"
  fi
  unset 'running_pid[last]'
  unset 'running_type[last]'
  unset 'running_project[last]'
  unset 'running_mem[last]'
}

can_start_build() {
  (( BUILD_ENABLED == 1 )) && (( ${#pending_builds[@]} > 0 )) && (( running_builds < BUILD_SLOTS )) && (( used_mem + BUILD_MEM_MIB <= mem_limit_mib ))
}

can_start_run() {
  (( RUN_ENABLED == 1 )) && (( ${#pending_runs[@]} > 0 )) && (( running_runs < RUN_SLOTS )) && (( used_mem + RUN_MEM_MIB <= mem_limit_mib ))
}

while :; do
  progress=0

  i=0
  while (( i < ${#running_pid[@]} )); do
    pid="${running_pid[$i]}"
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      status=$?
      type="${running_type[$i]}"
      project="${running_project[$i]}"
      mem="${running_mem[$i]}"
      if [[ "$type" == "run" ]]; then
        running_runs=$((running_runs - 1))
      else
        running_builds=$((running_builds - 1))
      fi
      used_mem=$((used_mem - mem))
      remove_running "$i"
      if (( status != 0 )); then
        echo "==> ${type} failed: $project (exit $status)"
        failures=$((failures + 1))
      else
        echo "==> ${type} done: $project"
        if [[ "$type" == "build" ]] && (( RUN_ENABLED == 1 )); then
          pending_runs+=( "$project" )
        fi
      fi
      progress=1
      continue
    fi
    i=$((i + 1))
  done

while can_start_run; do
    project="${pending_runs[0]}"
    pending_runs=( "${pending_runs[@]:1}" )
    start_run "$project"
    progress=1
  done

  while can_start_build; do
    project="${pending_builds[0]}"
    pending_builds=( "${pending_builds[@]:1}" )
    start_build "$project"
    progress=1
  done

  if (( ${#running_pid[@]} == 0 )) && (( ${#pending_builds[@]} == 0 )) && (( ${#pending_runs[@]} == 0 )); then
    break
  fi

  if (( progress == 0 )); then
    sleep 2
  fi
done

if (( failures > 0 )); then
  echo "Completed with ${failures} failures."
  exit 1
fi

echo "All jobs completed successfully."
