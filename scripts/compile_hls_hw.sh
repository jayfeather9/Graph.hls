#!/usr/bin/env bash
set -euo pipefail

ENV_SCRIPT_DEFAULT="/path/to/vitis/settings64.sh"

usage() {
  cat <<'EOF'
Usage: scripts/compile_hls_hw.sh [options]

Emits an HLS project per app and builds them sequentially using Vitis makefiles.

Options:
  --env <path>          Env script to source (default: /path/to/vitis/settings64.sh)
  --out-root <dir>      Output root directory (default: mktemp under /tmp)
  --make-target <t>     Make target to run: all | exe (default: all)
  --target <t>          Vitis target: hw | hw_emu | sw_emu (default: hw)
  --device <path>       Override platform DEVICE passed to make (optional)
  --kernel-freq <mhz>   Pass KERNEL_FREQ=<mhz> to make (optional)
  --apps <list>         Comma-separated apps (default: sssp,cc,pr)
  --big <count>         Number of big kernels (env: GRAPHYFLOW_BIG_KERNELS)
  --little <count>      Number of little kernels (env: GRAPHYFLOW_LITTLE_KERNELS)
  --max-dst-big <num>   Max dst for big kernels (env: GRAPHYFLOW_MAX_DST_BIG)
  --max-dst-little <num> Max dst for little kernels (env: GRAPHYFLOW_MAX_DST_LITTLE)
  -h, --help            Show this help

App aliases:
  cc -> connected_components
  pr -> pagerank
EOF
}

ENV_SCRIPT="$ENV_SCRIPT_DEFAULT"
OUT_ROOT=""
MAKE_TARGET="all"
VITIS_TARGET="hw"
DEVICE_OVERRIDE=""
KERNEL_FREQ=""
APPS_CSV="sssp,cc,pr"
BIG_KERNELS=""
LITTLE_KERNELS=""
MAX_DST_BIG=""
MAX_DST_LITTLE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV_SCRIPT="${2:?}"; shift 2 ;;
    --out-root) OUT_ROOT="${2:?}"; shift 2 ;;
    --make-target) MAKE_TARGET="${2:?}"; shift 2 ;;
    --target) VITIS_TARGET="${2:?}"; shift 2 ;;
    --device) DEVICE_OVERRIDE="${2:?}"; shift 2 ;;
    --kernel-freq) KERNEL_FREQ="${2:?}"; shift 2 ;;
    --apps) APPS_CSV="${2:?}"; shift 2 ;;
    --big) BIG_KERNELS="${2:?}"; shift 2 ;;
    --little) LITTLE_KERNELS="${2:?}"; shift 2 ;;
    --max-dst-big) MAX_DST_BIG="${2:?}"; shift 2 ;;
    --max-dst-little) MAX_DST_LITTLE="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$MAKE_TARGET" in
  all|exe) ;;
  *)
    echo "--make-target must be 'all' or 'exe' (got '$MAKE_TARGET')" >&2
    exit 2
    ;;
esac

case "$VITIS_TARGET" in
  hw|hw_emu|sw_emu) ;;
  *)
    echo "--target must be one of: hw, hw_emu, sw_emu (got '$VITIS_TARGET')" >&2
    exit 2
    ;;
esac

if [[ ! -f "$ENV_SCRIPT" ]]; then
  echo "env script not found: $ENV_SCRIPT" >&2
  exit 1
fi

if [[ -z "$OUT_ROOT" ]]; then
  OUT_ROOT="$(mktemp -d "/tmp/graphyflow_build_${VITIS_TARGET}_XXXXXX")"
fi

mkdir -p "$OUT_ROOT"

IFS=',' read -r -a APPS <<<"$APPS_CSV"

map_app() {
  case "$1" in
    cc) echo "connected_components" ;;
    pr) echo "pagerank" ;;
    *) echo "$1" ;;
  esac
}

# Use the emitted project's top-level `Makefile` so variables like `DEVICE` are set.
make_args=( "$MAKE_TARGET" "TARGET=$VITIS_TARGET" )
if [[ -n "$DEVICE_OVERRIDE" ]]; then
  make_args+=( "DEVICE=$DEVICE_OVERRIDE" )
fi
if [[ -n "$KERNEL_FREQ" ]]; then
  make_args+=( "KERNEL_FREQ=$KERNEL_FREQ" )
fi

echo "OUT_ROOT=$OUT_ROOT"
echo "ENV_SCRIPT=$ENV_SCRIPT"
echo "APPS=$APPS_CSV"
echo "MAKE_TARGET=$MAKE_TARGET"
echo "TARGET=$VITIS_TARGET"
if [[ -n "$DEVICE_OVERRIDE" ]]; then
  echo "DEVICE=$DEVICE_OVERRIDE"
fi
if [[ -n "$KERNEL_FREQ" ]]; then
  echo "KERNEL_FREQ=$KERNEL_FREQ"
fi
if [[ -n "$BIG_KERNELS" ]]; then
  export GRAPHYFLOW_BIG_KERNELS="$BIG_KERNELS"
  echo "GRAPHYFLOW_BIG_KERNELS=$GRAPHYFLOW_BIG_KERNELS"
fi
if [[ -n "$LITTLE_KERNELS" ]]; then
  export GRAPHYFLOW_LITTLE_KERNELS="$LITTLE_KERNELS"
  echo "GRAPHYFLOW_LITTLE_KERNELS=$GRAPHYFLOW_LITTLE_KERNELS"
fi
if [[ -n "$MAX_DST_BIG" ]]; then
  export GRAPHYFLOW_MAX_DST_BIG="$MAX_DST_BIG"
  echo "GRAPHYFLOW_MAX_DST_BIG=$GRAPHYFLOW_MAX_DST_BIG"
fi
if [[ -n "$MAX_DST_LITTLE" ]]; then
  export GRAPHYFLOW_MAX_DST_LITTLE="$MAX_DST_LITTLE"
  echo "GRAPHYFLOW_MAX_DST_LITTLE=$GRAPHYFLOW_MAX_DST_LITTLE"
fi

set +e
set +u
set +o pipefail
# shellcheck disable=SC1090
source "$ENV_SCRIPT"
source_status=$?
set -euo pipefail

if [[ $source_status -ne 0 ]]; then
  echo "failed to source env script (exit=$source_status): $ENV_SCRIPT" >&2
  exit 1
fi

for short in "${APPS[@]}"; do
  app="$(map_app "$short")"
  dest="$OUT_ROOT/$app"

  if [[ -e "$dest" ]]; then
    echo "destination already exists (must be empty/non-existent): $dest" >&2
    echo "remove it or choose a different --out-root" >&2
    exit 1
  fi

  echo "==> [${app}] emit"
  cargo run -- --emit-hls "$app" "$dest"

  echo "==> [${app}] build ($MAKE_TARGET, TARGET=$VITIS_TARGET)"
  (
    cd "$dest"
    make "${make_args[@]}"
  )

  echo "==> [${app}] done"
done

echo "All builds finished. Outputs under: $OUT_ROOT"
