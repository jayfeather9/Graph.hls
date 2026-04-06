#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_hwemu_docker_one.sh --project <name> --graph <path> [options]

Runs one emitted HLS project (under target/generated_hls/<project>) in hw_emu
inside the Vitis docker image and checks for "SUCCESS: Results match!".

Options:
  --project <name>      Project directory name under target/generated_hls (required)
  --graph <path>        Graph path (absolute or repo-relative) (required)
  --iters <n>           Fixed iteration count (default: 1)
  --timeout <seconds>   Timeout for ./run.sh (default: 3600)
  --shm-size <size>     Docker /dev/shm size (default: 8g)
  --workdir-host <dir>  Mount host dir to /tmp/graphyflow_run/<project> to
                        persist build logs/artifacts even if the container is killed
  --reuse-workdir       If --workdir-host is set, reuse its existing contents
                        (skip wiping + tar extract). Useful for resuming after rc=137
  --kernel-freq <mhz>   Pass KERNEL_FREQ=<mhz> to kernel build (optional)
  --rebuild-exe         Rebuild host binary (make cleanexe && make exe TARGET=hw_emu)
  --build-kernels       Build hw_emu xclbin if missing (make all TARGET=hw_emu)
  --run-mode <mode>     hw_emu|sw_emu (default: hw_emu)
  --build-only          Stop after building xclbin/exe; do not run ./run.sh
  --keep-container      Do not use --rm; keep the container for post-mortem inspection
  --image <tag>         Docker image (default: vivado-runner:22.04-feiyang)
  --container-prefix <p> Docker container name prefix (default: gf_hwemu)
  --name-mode <mode>     Container naming mode: full|hash (default: full)
  --repo-label <value>   Value for docker label graphyflow.repo (default: repo root path)
  --owner-label <value>  Value for docker label graphyflow.owner (default: current user)
  --repo-mount <path>   Repo mount destination in container (default: /vitis_work/refactor_Graphyflow)
  --platform-host <dir> Host platform dir (default: u55c path)
  --device <xpfm>       Device path inside container (default: u55c xpfm)
  --vitis-volume <vol>  Docker volume mounted to /opt/Xilinx (default: vitis-2024.2)
USAGE
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROJECT=""
GRAPH=""
ITERS="1"
TIMEOUT_SECONDS="3600"
SHM_SIZE="8g"
WORKDIR_HOST=""
REUSE_WORKDIR="0"
KERNEL_FREQ=""
REBUILD_EXE="0"
BUILD_KERNELS="0"
BUILD_ONLY="0"
RUN_MODE="hw_emu"
KEEP_CONTAINER="0"
IMAGE="vivado-runner:22.04-feiyang"
CONTAINER_PREFIX="gf_hwemu"
NAME_MODE="full"
REPO_LABEL=""
OWNER_LABEL=""
REPO_MOUNT="/vitis_work/refactor_Graphyflow"
PLATFORM_HOST="/path/to/platform/xilinx_u55c"
DEVICE="/vitis_work/platform/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm"
XILINX_VOLUME="vitis-2024.2"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="${2:?}"; shift 2 ;;
    --graph) GRAPH="${2:?}"; shift 2 ;;
    --iters) ITERS="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT_SECONDS="${2:?}"; shift 2 ;;
    --shm-size) SHM_SIZE="${2:?}"; shift 2 ;;
    --workdir-host) WORKDIR_HOST="${2:?}"; shift 2 ;;
    --reuse-workdir) REUSE_WORKDIR="1"; shift 1 ;;
    --kernel-freq) KERNEL_FREQ="${2:?}"; shift 2 ;;
    --rebuild-exe) REBUILD_EXE="1"; shift 1 ;;
    --build-kernels) BUILD_KERNELS="1"; shift 1 ;;
    --run-mode) RUN_MODE="${2:?}"; shift 2 ;;
    --build-only) BUILD_ONLY="1"; shift 1 ;;
    --keep-container) KEEP_CONTAINER="1"; shift 1 ;;
    --image) IMAGE="${2:?}"; shift 2 ;;
    --container-prefix) CONTAINER_PREFIX="${2:?}"; shift 2 ;;
    --name-mode) NAME_MODE="${2:?}"; shift 2 ;;
    --repo-label) REPO_LABEL="${2:?}"; shift 2 ;;
    --owner-label) OWNER_LABEL="${2:?}"; shift 2 ;;
    --repo-mount) REPO_MOUNT="${2:?}"; shift 2 ;;
    --platform-host) PLATFORM_HOST="${2:?}"; shift 2 ;;
    --device) DEVICE="${2:?}"; shift 2 ;;
    --vitis-volume) XILINX_VOLUME="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$PROJECT" || -z "$GRAPH" ]]; then
  usage >&2
  exit 2
fi

if [[ "$REUSE_WORKDIR" == "1" && -z "$WORKDIR_HOST" ]]; then
  echo "--reuse-workdir requires --workdir-host" >&2
  exit 2
fi

proj_dir="$REPO_ROOT/target/generated_hls/$PROJECT"
if [[ ! -d "$proj_dir" ]]; then
  echo "project not found: $proj_dir" >&2
  exit 1
fi

# Auto-detect platform from project Makefile if it references u200
if grep -q "xilinx_u200" "$proj_dir/Makefile" 2>/dev/null; then
  u200_host="/path/to/platform/xilinx_u200"
  if [[ -d "$u200_host" ]] && [[ "$PLATFORM_HOST" == *"u55c"* ]]; then
    PLATFORM_HOST="$u200_host"
    DEVICE="/vitis_work/platform/xilinx_u200_gen3x16_xdma_2_202110_1.xpfm"
    echo "==> Auto-detected U200 platform from Makefile"
  fi
fi

if [[ -z "$REPO_LABEL" ]]; then
  REPO_LABEL="$REPO_ROOT"
fi
if [[ -z "$OWNER_LABEL" ]]; then
  OWNER_LABEL="${GRAPHYFLOW_OWNER:-$(id -un)}"
fi

graph_host="$GRAPH"
if [[ "$graph_host" != /* ]]; then
  graph_host="$REPO_ROOT/$graph_host"
fi
if [[ ! -f "$graph_host" ]]; then
  echo "graph not found: $graph_host" >&2
  exit 1
fi
graph_host="$(readlink -f "$graph_host")"
graph_container=""
graph_mounts=()
graph_link_host=""
if [[ "$graph_host" == "$REPO_ROOT"* ]]; then
  graph_container="${REPO_MOUNT}${graph_host#$REPO_ROOT}"
else
  graph_link_dir="$REPO_ROOT/target/external_graph_links"
  mkdir -p "$graph_link_dir"
  graph_hash="$(printf '%s' "$graph_host" | sha1sum | awk '{print substr($1,1,12)}')"
  graph_link_host="$graph_link_dir/${graph_hash}__$(basename "$graph_host")"
  ln -sfn "$graph_host" "$graph_link_host"
  graph_container="${REPO_MOUNT}${graph_link_host#$REPO_ROOT}"
  graph_host_parent="$(dirname "$graph_host")"
  graph_mounts+=( -v "$graph_host_parent:$graph_host_parent:ro" )
fi

stub_dir="$REPO_ROOT/target/stubs"
mkdir -p "$stub_dir"
gcc -shared -fPIC -Wl,-soname,libudev.so.1 \
  -o "$stub_dir/libudev.so.1" \
  "$REPO_ROOT/scripts/libudev_stub.c"

lock_dir_host="/tmp/graphyflow_device_locks"
mkdir -p "$lock_dir_host"

opencl_vendor_dir="$REPO_ROOT/target/opencl_vendors"
mkdir -p "$opencl_vendor_dir"
echo "/opt/xilinx/xrt/lib/libxilinxopencl.so" > "$opencl_vendor_dir/xilinx.icd"

log_dir="$REPO_ROOT/target/parallel_logs"
mkdir -p "$log_dir"
tag="$(basename "$graph_host" | sed 's/[^A-Za-z0-9_.-]/_/g')"
log_file="$log_dir/${PROJECT}__${tag}__hw_emu_${ITERS}it_$(date +%Y%m%d_%H%M%S).log"

safe_prefix="$(echo "$CONTAINER_PREFIX" | tr -c 'A-Za-z0-9_.-' '_' | sed 's/^_\\+//;s/_\\+$//')"
if [[ -z "$safe_prefix" ]]; then
  safe_prefix="gf_hwemu"
fi
safe_project="$(echo "$PROJECT" | tr -c 'A-Za-z0-9_.-' '_' | sed 's/^_\\+//;s/_\\+$//')"
safe_graph="$(basename "$graph_host" | tr -c 'A-Za-z0-9_.-' '_' | sed 's/^_\\+//;s/_\\+$//')"
stamp="$(date +%Y%m%d_%H%M%S)"
case "$NAME_MODE" in
  full)
    container_name="${safe_prefix}_${safe_project}__${safe_graph}__${stamp}"
    ;;
  hash)
    short_id="$(printf '%s' "${PROJECT}|${graph_host}|${ITERS}|${TIMEOUT_SECONDS}|${KERNEL_FREQ}" | sha1sum | awk '{print substr($1,1,10)}')"
    container_name="${safe_prefix}_${short_id}__${stamp}"
    ;;
  *)
    echo "unknown --name-mode: $NAME_MODE (expected: full|hash)" >&2
    exit 2
    ;;
esac

echo "==> ${RUN_MODE} project=$PROJECT graph=$tag iters=$ITERS timeout=${TIMEOUT_SECONDS}s"
if [[ -n "$KERNEL_FREQ" ]]; then
  echo "    kernel_freq_mhz=$KERNEL_FREQ"
fi
echo "    container=$container_name"
echo "    log=$log_file"
if [[ -n "$WORKDIR_HOST" ]]; then
  echo "    workdir_host=$WORKDIR_HOST"
fi
if [[ -n "$graph_link_host" ]]; then
  echo "    graph_link=$graph_link_host -> $graph_host"
fi

extra_envs=()
if [[ -n "${GRAPHYFLOW_DEBUG_EVENTS:-}" ]]; then
  extra_envs+=(-e "GRAPHYFLOW_DEBUG_EVENTS=${GRAPHYFLOW_DEBUG_EVENTS}")
fi
if [[ -n "${GRAPHYFLOW_EVENT_WATCHDOG_SECONDS:-}" ]]; then
  extra_envs+=(-e "GRAPHYFLOW_EVENT_WATCHDOG_SECONDS=${GRAPHYFLOW_EVENT_WATCHDOG_SECONDS}")
fi
if [[ -n "${GRAPHYFLOW_DUMP_PARTITIONS:-}" ]]; then
  extra_envs+=(-e "GRAPHYFLOW_DUMP_PARTITIONS=${GRAPHYFLOW_DUMP_PARTITIONS}")
fi

uid="$(id -u)"
gid="$(id -g)"

workdir_mount=()
workdir_host_resolved=""
if [[ -n "$WORKDIR_HOST" ]]; then
  workdir_host="$WORKDIR_HOST"
  if [[ "$workdir_host" != /* ]]; then
    workdir_host="$REPO_ROOT/$workdir_host"
  fi
  mkdir -p "$workdir_host"
  workdir_mount+=( -v "$workdir_host:/tmp/graphyflow_run/${PROJECT}" )
  workdir_host_resolved="$workdir_host"
fi

dockerrm=( --rm )
if [[ "$KEEP_CONTAINER" == "1" ]]; then
  dockerrm=()
fi

set +e
	env -u DOCKER_HOST docker run "${dockerrm[@]}" --platform linux/amd64 --user "${uid}:${gid}" --shm-size="$SHM_SIZE" \
	  --name "$container_name" \
	  --label "graphyflow.repo=${REPO_LABEL}" \
	  --label "graphyflow.owner=${OWNER_LABEL}" \
	  --label "graphyflow.kind=${RUN_MODE}" \
	  --label "graphyflow.project=${PROJECT}" \
	  -v "${XILINX_VOLUME}:/opt/Xilinx" \
	  -v /opt/xilinx:/opt/xilinx:ro \
	  -v "$opencl_vendor_dir:/etc/OpenCL/vendors:ro" \
	  -v "$lock_dir_host:/tmp/graphyflow_device_locks" \
	  -e XILINX_XRT=/opt/xilinx/xrt \
  -e GRAPHYFLOW_ENV_SH=/opt/xilinx/xrt/setup.sh \
  -e GRAPHYFLOW_ALLOW_MISMATCH=0 \
	  -e GRAPHYFLOW_DEVICE_LOCK_DIR=/tmp/graphyflow_device_locks \
	  -e GRAPHYFLOW_MAX_ITERS="$ITERS" \
	  -e GRAPHYFLOW_RUN_MODE="${RUN_MODE}" \
	  -e GF_PROJECT="$PROJECT" \
	  -e GF_GRAPH_CONTAINER="$graph_container" \
	  -e GF_REPO_MOUNT="$REPO_MOUNT" \
	  -e GF_TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
	  -e GF_DEVICE="$DEVICE" \
	  -e GF_REBUILD_EXE="$REBUILD_EXE" \
  -e GF_BUILD_KERNELS="$BUILD_KERNELS" \
  -e GF_BUILD_ONLY="$BUILD_ONLY" \
  -e GF_REUSE_WORKDIR="$REUSE_WORKDIR" \
  -e GRAPHYFLOW_BUILD_ONLY="$BUILD_ONLY" \
  -e GF_KERNEL_FREQ="$KERNEL_FREQ" \
  "${extra_envs[@]}" \
  -e EMCONFIG_PATH="/tmp/graphyflow_run/${PROJECT}" \
  -v "$REPO_ROOT:$REPO_MOUNT" \
  "${graph_mounts[@]}" \
  -v "$PLATFORM_HOST:/vitis_work/platform" \
  -v "$stub_dir:/tmp/graphyflow_stubs:ro" \
  "${workdir_mount[@]}" \
  -w "$REPO_MOUNT" \
	  "$IMAGE" bash -lc '
	    set -euo pipefail
	    work_dir="/tmp/graphyflow_run/${GF_PROJECT}"
	    mkdir -p "$work_dir"
	    if [[ "${GF_REUSE_WORKDIR}" != "1" ]]; then
	      # If the work_dir is a bind-mount (via --workdir-host), removing the
	      # directory itself can fail. Clear its contents instead.
	      find "$work_dir" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
	      ( cd "${GF_REPO_MOUNT}/target/generated_hls/${GF_PROJECT}" && \
	        tar --exclude=.run --exclude=.Xil -cf - . \
	      ) | ( cd "$work_dir" && tar -xf - )
	    else
	      test -f "$work_dir/Makefile"
	    fi
		    source /opt/Xilinx/Vitis/2024.1/settings64.sh
		    export LD_LIBRARY_PATH="/tmp/graphyflow_stubs:${LD_LIBRARY_PATH:-}"
		    # The Vitis bundled GCC misses Debian/Ubuntu multiarch headers by default.
		    export CPATH="/usr/include/x86_64-linux-gnu${CPATH:+:${CPATH}}"
		    export CPLUS_INCLUDE_PATH="/usr/include/x86_64-linux-gnu${CPLUS_INCLUDE_PATH:+:${CPLUS_INCLUDE_PATH}}"
		    cd "$work_dir"
		    dest_root="${GF_REPO_MOUNT}/target/generated_hls/${GF_PROJECT}"
		    if [[ ! -f "${GF_GRAPH_CONTAINER}" ]]; then
		      echo "graph missing inside container: ${GF_GRAPH_CONTAINER}" >&2
		      ls -l "$(dirname "${GF_GRAPH_CONTAINER}")" >&2 || true
		      exit 1
		    fi
		    build_target="${GRAPHYFLOW_RUN_MODE:-hw_emu}"
		    rebuilt_exe=0
		    if [[ "${GF_REBUILD_EXE}" == "1" ]]; then
		      make cleanexe
		      make exe TARGET="${build_target}"
		      rebuilt_exe=1
		    fi
		    if [[ ! -x ./graphyflow_host ]]; then
		      make exe TARGET="${build_target}"
		      rebuilt_exe=1
		    fi
		    if [[ "$rebuilt_exe" == "1" ]]; then
		      cp -f ./graphyflow_host "$dest_root/graphyflow_host"
		    fi
		    if [[ "${GF_BUILD_KERNELS}" == "1" || ! -f "./xclbin/graphyflow_kernels.${build_target}.xclbin" ]]; then
	      # Make does not notice changes to KERNEL_FREQ (a variable), so if the
	      # caller requests a frequency override we may need to force a clean
	      # rebuild. To avoid needlessly restarting after rc=137, track the last
	      # requested frequency in a small marker file in the (optional) persisted
	      # workdir.
	      if [[ -n "${GF_KERNEL_FREQ:-}" ]]; then
	        freq_marker=".graphyflow_kernel_freq_mhz"
	        prev_freq=""
	        if [[ -f "$freq_marker" ]]; then
	          prev_freq="$(cat "$freq_marker" 2>/dev/null || true)"
	        fi
	        if [[ "$prev_freq" != "${GF_KERNEL_FREQ}" ]]; then
	          rm -rf ./xclbin ./_x ./.ipcache
	          echo "${GF_KERNEL_FREQ}" > "$freq_marker"
	        fi
	      fi
		      if [[ -n "${GF_KERNEL_FREQ:-}" ]]; then
		        make all TARGET="${build_target}" DEVICE="${GF_DEVICE}" KERNEL_FREQ="${GF_KERNEL_FREQ}"
		      else
		        make all TARGET="${build_target}" DEVICE="${GF_DEVICE}"
		      fi
	      # Persist build products back into the repo so future runs can reuse them.
	      rm -rf "$dest_root/xclbin"
	      mkdir -p "$dest_root/xclbin"
	      cp -rf ./xclbin/. "$dest_root/xclbin/"
	      if [[ -f ./emconfig.json ]]; then
	        cp -f ./emconfig.json "$dest_root/emconfig.json"
	      fi
	      # Persist lightweight timing reports (Estimated Frequency) so they can be inspected on the host.
	      if [[ -d ./_x/reports ]]; then
	        while IFS= read -r -d "" f; do
	          rel="${f#./}"
	          mkdir -p "$dest_root/$(dirname "$rel")"
	          cp -f "$f" "$dest_root/$rel"
	        done < <(find ./_x/reports -type f \( -name "system_estimate_*.xtxt" -o -name "automation_summary*.txt" \) -print0)
	      fi
	    fi
	    if [[ "${GF_BUILD_ONLY}" == "1" ]]; then
	      echo "[build_only]=1"
	      exit 0
		    fi
		    if [[ "${GRAPHYFLOW_RUN_MODE:-hw_emu}" == "hw_emu" || "${GRAPHYFLOW_RUN_MODE:-hw_emu}" == "sw_emu" ]]; then
		      rm -rf ./.run ./.Xil
		    fi
		    start=$(date +%s)
		    set +e
		    run_mode="${GRAPHYFLOW_RUN_MODE:-hw_emu}"
		    timeout "${GF_TIMEOUT_SECONDS}s" ./run.sh "$run_mode" "${GF_GRAPH_CONTAINER}"
		    rc=$?
		    set -e
		    end=$(date +%s)
		    echo "[wall_seconds]=$((end-start)) rc=$rc"
	    exit $rc
	  ' >"$log_file" 2>&1
rc=$?
set -e

if [[ -n "$workdir_host_resolved" ]]; then
  dest_root="$REPO_ROOT/target/generated_hls/$PROJECT"
  dest_xclbin="$dest_root/xclbin/graphyflow_kernels.${RUN_MODE}.xclbin"
  src_xclbin="$workdir_host_resolved/xclbin/graphyflow_kernels.${RUN_MODE}.xclbin"
  if [[ ! -f "$dest_xclbin" && -f "$src_xclbin" ]]; then
    echo "==> salvaging xclbin from $workdir_host_resolved"
    rm -rf "$dest_root/xclbin"
    mkdir -p "$dest_root/xclbin"
    cp -rf "$workdir_host_resolved/xclbin/." "$dest_root/xclbin/"
    if [[ -f "$workdir_host_resolved/emconfig.json" ]]; then
      cp -f "$workdir_host_resolved/emconfig.json" "$dest_root/emconfig.json"
    fi
    if [[ -d "$workdir_host_resolved/_x/reports" ]]; then
      while IFS= read -r -d "" f; do
        rel="${f#"$workdir_host_resolved"/}"
        mkdir -p "$dest_root/$(dirname "$rel")"
        cp -f "$f" "$dest_root/$rel"
      done < <(find "$workdir_host_resolved/_x/reports" -type f \( -name "system_estimate_*.xtxt" -o -name "automation_summary*.txt" \) -print0)
    fi
  fi
  if [[ -f "$workdir_host_resolved/graphyflow_host" ]]; then
    if [[ ! -f "$dest_root/graphyflow_host" || "$workdir_host_resolved/graphyflow_host" -nt "$dest_root/graphyflow_host" ]]; then
      cp -f "$workdir_host_resolved/graphyflow_host" "$dest_root/graphyflow_host"
    fi
  fi
fi

if [[ "$BUILD_ONLY" == "1" ]]; then
  if grep -q "\\[build_only\\]=1" "$log_file"; then
    echo "==> BUILD-ONLY OK"
    exit 0
  fi
  echo "==> BUILD-ONLY FAILED (rc=$rc, see $log_file)" >&2
  tail -n 120 "$log_file" >&2 || true
  exit 1
fi

if grep -q "SUCCESS: Results match" "$log_file"; then
  echo "==> PASS"
else
  echo "==> FAIL (rc=$rc, see $log_file)" >&2
  tail -n 120 "$log_file" >&2 || true
  exit 1
fi
