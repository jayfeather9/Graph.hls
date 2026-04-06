# Running `hw_emu` in the Vitis Docker Image (GraphyFlow)

This repo can run `hw_emu` inside the `vivado-runner:22.04-feiyang` container,
**if** you mount the XRT install and create an OpenCL ICD entry in the
container.

Use that container image explicitly. Do **not** substitute a plain
`ubuntu:22.04` image for GraphyFlow Vitis runs: this workflow assumes the
repo's `vivado-runner:22.04-feiyang` image plus the mounted Vitis/XRT layout.

Important: this machine may have `DOCKER_HOST=...podman.sock` set in the shell.
GraphyFlow’s Vitis runs use **system docker**, so always use:

```bash
env -u DOCKER_HOST docker ...
```

## Prerequisites on the host

- Docker image: `vivado-runner:22.04-feiyang`
- Docker volume: `vitis-2024.2` (mounted at `/opt/Xilinx` in the container)
- Host XRT at `/opt/xilinx/xrt`
- Platform folder: `/path/to/platform/xilinx_u55c`
- Project repo: `/path/to/Graph.hls`

## Recommended: one-off `hw_emu` run script

Use `scripts/run_hwemu_docker_one.sh` to run one emitted project under
`target/generated_hls/<project>` inside docker, with logs under
`target/parallel_logs/`.

Important: run this script directly on the host. The script itself launches the
`vivado-runner:22.04-feiyang` container with `env -u DOCKER_HOST docker run`;
do not wrap the script in another `docker run`.

Example (2 fixed iterations, 30 min timeout):

```bash
scripts/run_hwemu_docker_one.sh \
  --project sssp_topo_l44_b22_hwemu_depthfix \
  --graph target/graph_100_100.txt \
  --iters 2 \
  --timeout 1800 \
  --rebuild-exe \
  --kernel-freq 300
```

Notes:
- `--rebuild-exe` forces host-only rebuild (`make cleanexe && make exe TARGET=hw_emu`).
- Add `--build-kernels` to force a fresh `make all TARGET=hw_emu` and persist
  `xclbin/` and lightweight `system_estimate_*.xtxt` reports back into
  `target/generated_hls/<project>/`.
- `--kernel-freq` passes `KERNEL_FREQ=<mhz>` to `make` (controls Vitis `--kernel_frequency`).
- If you see sporadic `rc=137` during long `v++ -l`/Vivado (`vpl`) phases, use
  a persisted container workdir so you can resume without redoing the entire build:
  - First attempt (persist workdir):  
    `scripts/run_hwemu_docker_one.sh --project sssp_topo_l12_b2 --graph target/graph_32_32_weighted.txt --build-only --build-kernels --rebuild-exe --kernel-freq 300 --shm-size 32g --workdir-host target/hwemu_work/sssp_topo_l12_b2`
  - Resume after `rc=137` (reuse the same workdir):  
    `scripts/run_hwemu_docker_one.sh --project sssp_topo_l12_b2 --graph target/graph_32_32_weighted.txt --build-only --build-kernels --kernel-freq 300 --shm-size 32g --workdir-host target/hwemu_work/sssp_topo_l12_b2 --reuse-workdir`

## Build `hw_emu` (inside docker)

```bash
env -u DOCKER_HOST docker run --rm --platform linux/amd64 --user "$(id -u):$(id -g)" --shm-size=2g \
  -v vitis-2024.2:/opt/Xilinx \
  -v /opt/xilinx:/opt/xilinx:ro \
  -v "$PWD/target/opencl_vendors:/etc/OpenCL/vendors:ro" \
  -e XILINX_XRT=/opt/xilinx/xrt \
  -v /path/to/Graph.hls:/vitis_work/refactor_Graphyflow \
  -v /path/to/platform/xilinx_u55c:/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1 \
  -w /vitis_work/refactor_Graphyflow/src/hls_assets \
  vivado-runner:22.04-feiyang bash -lc \
  'source /opt/Xilinx/Vitis/2024.1/settings64.sh && \
   make all TARGET=hw_emu DEVICE=/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm'
```

## Run `hw_emu` (inside docker)

`hw_emu` requires an OpenCL ICD file for XRT **inside the container**. The recommended
approach is to create `target/opencl_vendors/xilinx.icd` on the host and mount
it read-only into the container.

```bash
mkdir -p target/opencl_vendors
echo /opt/xilinx/xrt/lib/libxilinxopencl.so > target/opencl_vendors/xilinx.icd

env -u DOCKER_HOST docker run --rm --platform linux/amd64 --user "$(id -u):$(id -g)" --shm-size=2g \
  -v vitis-2024.2:/opt/Xilinx \
  -v /opt/xilinx:/opt/xilinx:ro \
  -v "$PWD/target/opencl_vendors:/etc/OpenCL/vendors:ro" \
  -e XILINX_XRT=/opt/xilinx/xrt \
  -e GRAPHYFLOW_ENV_SH=/opt/xilinx/xrt/setup.sh \
  -e EMCONFIG_PATH=/vitis_work/refactor_Graphyflow/src/hls_assets \
  -v /path/to/Graph.hls:/vitis_work/refactor_Graphyflow \
  -v /path/to/platform/xilinx_u55c:/vitis_work/xilinx_u55c_gen3x16_xdma_3_202210_1 \
  -w /vitis_work/refactor_Graphyflow/src/hls_assets \
  vivado-runner:22.04-feiyang bash -lc \
  'source /opt/Xilinx/Vitis/2024.1/settings64.sh && ./run.sh hw_emu'
```

Notes:
- `EMCONFIG_PATH` must point to the directory containing `emconfig.json`.
- `GRAPHYFLOW_ENV_SH` is used by `run.sh`; set it to `xrt/setup.sh` inside the container.
- The ICD file is required; without it `cl::Platform::get` fails with `-1001`.
- If `xrt.ini` is present in the project directory, XRT picks it up automatically
  (this repo’s emitted projects include one that disables waveform generation for
  faster `hw_emu`).

## Avoiding root-owned artifacts

Do **not** run Docker as `--user 0:0` when bind-mounting this repo. Root-run
containers can create root-owned `.run/`, `.Xil/`, `xsa.xml`, or build artifacts
under `target/generated_hls/`, which then break `cargo run -- --emit-hls ...`
with `PermissionDenied`.

If the repo is already damaged, use:

```bash
scripts/fix_repo_permissions_docker.sh --path target/generated_hls
```

## Troubleshooting

- `rc=137` in matrix summaries typically means the process was `SIGKILL`'d:
  - **timeout kill**: `timeout ... --kill-after ...` eventually sends `SIGKILL`
    if the container doesn’t exit after `SIGTERM`. Increase per-case timeouts.
  - **OOM kill**: the host kernel can kill `docker` / `v++` under memory
    pressure. Reduce parallelism (or use `scripts/parallel_hw_emu.sh` with a
    higher `--reserve-gib` / lower `--max-runs`).
    - Quick confirmation: `env -u DOCKER_HOST docker inspect -f '{{.State.OOMKilled}}' <container>`
    - Recommended: keep `Max parallel runs` low (e.g. `2–3`) during `v++ -l` link-heavy phases.
  - **orphan cleanup kill**: another long-running “cleanup orphans” loop can
    force-remove containers via `docker rm -f`, which appears as `rc=137` with
    truncated logs. This repo’s runners label containers with
    `graphyflow.repo=/path/to/Graph.hls` and `graphyflow.owner=<user>`,
    and its cleanup scripts (`scripts/run_all_hw_emu_pending.sh`,
    `scripts/run_pending_parallel.sh`) only remove containers with those labels.
    If you have another checkout on the same machine that runs a cleanup based
    on the container **command string** (e.g. matching `./run.sh hw_emu`), it
    can still kill these runs unless stopped.
    - Override the owner label value via `GRAPHYFLOW_OWNER=...` or
      `--owner-label ...` on `scripts/hw_emu_matrix.sh` / `scripts/run_hwemu_docker_one.sh`.
  - **mount-destination cleanup kill**: if another worker has a cleanup that
    removes any `vivado-runner` container mounting to the fixed destination
    `/vitis_work/refactor_Graphyflow`, run with a different repo mount point:
    - matrix: `scripts/hw_emu_matrix.sh --repo-mount /vitis_work/gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`
    - one-off: `scripts/run_hwemu_docker_one.sh --repo-mount /vitis_work/gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`
  - **container-name cleanup kill**: if another worker has a cleanup that removes
    containers matching `gf_hwemu_*`, run with a different name prefix:
    - matrix: `scripts/hw_emu_matrix.sh --container-prefix gf_${USER}_hwemu ...`
    - one-off: `scripts/run_hwemu_docker_one.sh --container-prefix gf_${USER}_hwemu ...`
    - If the cleanup matches longer substrings like `*_topo_*`, use `--name-mode hash`
      in `scripts/hw_emu_matrix.sh` (or `scripts/run_hwemu_docker_one.sh`) to keep
      container names short and pattern-resistant.
  - **label-based cleanup kill**: if another worker has a cleanup that targets
    containers with the exact label value `graphyflow.repo=/path/to/Graph.hls`,
    run with a different label value:
    - matrix: `scripts/hw_emu_matrix.sh --repo-label gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`
    - one-off: `scripts/run_hwemu_docker_one.sh --repo-label gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`
  - **EXTERNAL** (container disappeared mid-run): newer matrix logs can mark a case as `EXTERNAL`
    when `docker inspect` cannot find the container at the end of the run (usually
    due to an external `docker rm -f`). In that situation:
    - check the per-case log for a `[docker] events` block (container create/kill/die/destroy)
    - increase `--external-retries` to keep retrying until the external cleanup stops
  - **external kill**: some `v++ -l` / `vpl` steps can be killed without a clear
    error in the log (e.g., preemption or manual termination). Persisting a
    workdir (see above) makes these failures much cheaper to retry.
- If you need post-mortem inspection on failures, use:
  - `scripts/run_hwemu_docker_one.sh --keep-container ...`
  - `scripts/hw_emu_matrix.sh --keep-container ...` (leaves containers behind;
    clean up later via `env -u DOCKER_HOST docker ps --filter label=graphyflow.repo=/path/to/Graph.hls`)
  - Note: `scripts/hw_emu_matrix.sh` also appends a short `docker inspect` line
    (OOMKilled/ExitCode/FinishedAt) into each case log before removing the
    container, even when not using `--keep-container`.
- False FAILs from `GRAPHYFLOW_EVENT_WATCHDOG_SECONDS`:
  - If `GRAPHYFLOW_EVENT_WATCHDOG_SECONDS` is set, the generated host code exits
    after that many seconds (useful for diagnosing hangs), which can interrupt
    slow-but-progressing `hw_emu` runs. For validation matrices, keep it unset.
- `Failed to initialize accelerator on all available devices` / `device[0] is busy/in use`:
- Another job is holding the U55C (often a different worker or a run that did not respect
    `GRAPHYFLOW_DEVICE_LOCK_DIR`). Wait for the other job to finish and rerun.
  - `scripts/hw_emu_matrix.sh` marks this as `BUSY` (retriable) and sleeps briefly before retrying.
  - If multiple U55C cards are present, you can force the host to use a specific
    device index by setting `GRAPHYFLOW_DEVICE_INDEX=<n>` (index is the order
    returned by `xrt-smi examine` / XRT’s device enumeration).
 - If a run hangs and prints no new output (often stuck in `--- [Host] Phase 4: Transferring results from FPGA ---`),
   use `scripts/hw_emu_matrix.sh --no-log-seconds <n>` (or the topology driver default) to avoid burning hours
   waiting for the iteration-progress watchdog.

## Useful helper scripts in this repo

- `scripts/report_estimated_freq.sh`: scan `system_estimate_*.xtxt` and summarize the minimum Estimated Frequency.
- `scripts/graphyflow_status.sh`: show tmux/docker/process/log status quickly.
- `scripts/cleanup_graphyflow_docker.sh`: dry-run (or `--force`) cleanup of stale `vivado-runner` containers that mount this repo.

## HW build guard (Docker `vpl` crash workaround)

Some Docker HW builds can abort in `config_hw_runs` with:

- `realloc(): invalid pointer`
- `Abnormal program termination (6)`

Use `scripts/docker_hw_build_guard.sh` to run HW builds with automatic crash detection and a container-side `libudev` stub workaround.

```bash
scripts/docker_hw_build_guard.sh \
  --project target/generated_hls/cc_hw_debug \
  --platform-host /path/to/platform/xilinx_u55c \
  --device /vitis_work/platform/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm
```

Quick checkpoint (stop after `config_hw_runs` passes):

```bash
scripts/docker_hw_build_guard.sh ... --checkpoint config_done
```

Logs are written to `target/parallel_logs/` by default.
