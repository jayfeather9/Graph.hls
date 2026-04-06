# Scripts Reference

This repo includes small helper scripts under `scripts/` to standardize common
build/run/debug workflows (especially for Vitis `hw_emu` and HW runs).

## Frequency / Reports

- `scripts/report_estimated_freq.sh`  
  Scans `system_estimate_*.xtxt` reports (per-kernel or link-level) and prints
  the minimum **Estimated Frequency** per report and overall.  
  Example: `scripts/report_estimated_freq.sh --root target/generated_hls --hw-emu --link-only`

## Generated Project Runs (No Docker)

- `scripts/run_target_one.sh`  
  Runs one emitted project under `target/generated_hls/<project>` without
  Docker in `sw_emu`, `hw_emu`, or `hw`. It rebuilds `graphyflow_host` and/or
  the mode-specific xclbin when requested (or when missing), writes a combined
  log under `target/parallel_logs/` by default (override with `--log-dir`), and delegates `hw` runs to
  `scripts/run_hw_with_watchdog.sh` so hardware diagnostics stay intact.  
  It also supports compile-time host throughput overrides via
  `--big-edge-per-ms <n>` and `--little-edge-per-ms <n>`, which are passed to
  `make` as `BIG_EDGE_PER_MS=<n>` / `LITTLE_EDGE_PER_MS=<n>` and compiled into
  the host as `-D...` macros. Supplying either flag forces a clean host rebuild.
  Those flags are host-only and will not implicitly rebuild a missing xclbin;
  use `--build-kernels` when you want a kernel rebuild.
  Examples:
  `scripts/run_target_one.sh --project sssp_ddr_4b4l_codegen --mode hw_emu --graph target/ddr_smoke/graph.txt --build-kernels --timeout 1800`
  `scripts/run_target_one.sh --project sssp_ddr_4b4l_codegen --mode hw_emu --graph target/ddr_smoke/graph.txt --big-edge-per-ms 10000 --little-edge-per-ms 40000 --timeout 1800`

- `scripts/run_ddr_codegen_all.sh`  
  Sequentially runs the five DDR generated projects
  (`sssp_ddr_4b4l_codegen`, `cc_bitmask_ddr_4b4l`, `wcc_ddr_4b4l`,
  `pr_ddr_4b4l`, `ar_ddr_4b4l`) for every dataset file directly under a chosen
  dataset folder, forwarding the common build/run flags to
  `scripts/run_target_one.sh`. The dataset folder defaults to
  `/path/to/datasets`. It creates a timestamped batch log
  root under `target/parallel_logs/` by default (override with `--output-dir`)
  and writes per-project logs under dataset-specific subdirectories. It also
  applies dataset-specific default `BIG_EDGE_PER_MS` / `LITTLE_EDGE_PER_MS`
  values based on the known dataset filename mapping; explicit
  `--big-edge-per-ms` / `--little-edge-per-ms` flags override those defaults
  for the entire batch. Because the batch helper always passes throughput
  values, first-time xclbin builds should use `--build-kernels`. Usage:
  `scripts/run_ddr_codegen_all.sh <sw_emu|hw_emu|hw> [dataset_dir] [options]`.  
  Examples:
  `scripts/run_ddr_codegen_all.sh sw_emu --timeout 600`
  `scripts/run_ddr_codegen_all.sh hw_emu /path/to/datasets --build-kernels --timeout 3600`
  `scripts/run_ddr_codegen_all.sh hw /path/to/datasets --output-dir target/parallel_logs/ddr_hw --build-kernels --timeout 7200`

## Hardware Runs (Host-side)

- `scripts/run_hw_with_watchdog.sh`  
  Runs `./run.sh hw <dataset>` with a wall-time timeout. On timeout/failure it
  appends XRT diagnostics (`xrt-smi examine --report dynamic-regions/memory/error`)
  to the same log file, and can optionally `xrt-smi reset --force` on a chosen
  device BDF. It avoids hanging indefinitely if the underlying host process
  enters uninterruptible sleep (D-state) by enforcing its own deadline.  
  Tip: on multi-card hosts, pass `--device-index <n>` (or set `GRAPHYFLOW_DEVICE_INDEX=<n>`)
  to force the host to pick a specific OpenCL device during `initAccelerator`.
  `--device <BDF>` is used for diagnostics/reset only.
  Example: `scripts/run_hw_with_watchdog.sh --project target/generated_hls/sssp_topo_l44_b22_hwemu_freq280_seqmerge3 --dataset /path/to/datasets/rmat-19-32.txt --iters 1 --timeout 7200`

## hw_emu Runs (Docker)

- `scripts/run_hwemu_docker_one.sh`  
  Runs one emitted project under `target/generated_hls/<project>` in `hw_emu`
  inside the Vitis docker image, optionally rebuilding the host exe and/or
  kernels. Writes a full log under `target/parallel_logs/`.  
  Example: `scripts/run_hwemu_docker_one.sh --project sssp_topo_l44_b22_hwemu_depthfix --graph target/graph_32_32.txt --iters 2 --timeout 1800 --rebuild-exe --kernel-freq 300`  
  Tip: if another worker has a cleanup job that removes containers mounting to
  `/vitis_work/refactor_Graphyflow`, run with a different mount destination:
  `scripts/run_hwemu_docker_one.sh --repo-mount /vitis_work/gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`  
  Tip: if another worker has a cleanup job that targets containers by the label
  value `graphyflow.repo=/path/to/Graph.hls`, override it with:
  `scripts/run_hwemu_docker_one.sh --repo-label gf_${USER}_$(date +%Y%m%d_%H%M%S) ...`  
  Tip: if multiple people share the same checkout path, set a unique owner label
  value so cleanup scripts only touch your containers:
  `scripts/run_hwemu_docker_one.sh --owner-label gf_${USER} ...` (or `GRAPHYFLOW_OWNER=...`)  
  Tip: if another worker has a cleanup job that matches container name patterns
  like `*_topo_*`, use `--name-mode hash` to keep container names short:
  `scripts/run_hwemu_docker_one.sh --name-mode hash ...`  
  Tip: for long `v++ -l` (link) phases that sometimes die with `rc=137`, use a
  persisted workdir and resume without redoing compile:
  `scripts/run_hwemu_docker_one.sh --project sssp_topo_l12_b2 --graph target/graph_32_32_weighted.txt --build-only --build-kernels --rebuild-exe --kernel-freq 300 --shm-size 32g --workdir-host target/hwemu_work/sssp_topo_l12_b2 && scripts/run_hwemu_docker_one.sh --project sssp_topo_l12_b2 --graph target/graph_32_32_weighted.txt --build-only --build-kernels --kernel-freq 300 --shm-size 32g --workdir-host target/hwemu_work/sssp_topo_l12_b2 --reuse-workdir`

- `scripts/run_topo_freq300_build.sh`  
  Sequentially **builds only** the 12 topology-variant `hw_emu` xclbins (SSSP/CC/PR × 4 topologies) at a chosen
  kernel frequency (default `300`) so the 24-case matrix can start immediately once all xclbins exist.  
  Example: `scripts/run_topo_freq300_build.sh --kernel-freq 300 --graph target/graph_32_32.txt --iters 1`

- `scripts/parallel_hw_emu.sh`  
  Builds and/or runs a set of emitted projects in parallel Docker containers,
  using a configurable **host memory budget** to cap concurrency. Runs Docker as
  your user and executes `hw_emu` in a container-local workdir so the repo mount
  is not polluted with root-owned `.run/` artifacts.  
  Example (build only, sequential builds): `scripts/parallel_hw_emu.sh --projects "sssp_topo_l12_b2,sssp_topo_l66_b2" --build-only --build-mem 70000 --run-mem 20000 --max-builds 1 --kernel-freq 300`

- `scripts/hw_emu_matrix.sh`  
  Runs a CSV-defined `hw_emu` matrix with retries + stall watchdog and writes a
  TSV summary. Uses **system Docker** (`env -u DOCKER_HOST docker ...`) to avoid
  accidentally running against a Podman socket.  
  Notes:
  - A `rc=137` case is usually a watchdog/container kill (or OOM); see
    `docs/hw_emu_docker.md` for mitigation.
  - A `BUSY` case indicates the U55C was in use by another job; the matrix will retry.
  - The stall watchdog is based on a stable progress marker (`End-to-End Time`
    lines), not log file size.
  - `--no-log-seconds` can kill runs that stop printing output (useful for Phase-4 hangs).
  - If your case CSV uses too-small per-case timeouts (e.g. `1200` seconds),
    use `--min-timeout-sec <n>` to clamp all cases to a safer minimum.
  - `--busy-retries` adds extra retries only for `BUSY` (device-in-use) failures,
    so temporary contention doesn’t consume your normal retry budget.
  - `--external-retries` adds extra retries for `EXTERNAL` (container disappears / likely external `docker rm -f`),
    so transient cleanup storms don’t consume your normal retry budget.
  - By default it does not use Docker `--rm`; it removes containers explicitly
    after capturing `docker inspect` diagnostics into the per-case log. Use
    `--keep-container` to leave containers behind.
  - `--keep-container` disables `--rm` and auto-removal so you can `docker inspect`
    a failing container (remember to clean up afterwards).
  - If another worker has a cleanup job that force-removes containers based on a
    fixed mount destination, use `--repo-mount /vitis_work/gf_<unique>` to avoid
    `/vitis_work/refactor_Graphyflow`.
  - If another worker has a cleanup job that force-removes containers by name
    pattern, use `--container-prefix <unique>` to avoid `gf_hwemu_*`.
  - If another worker has a cleanup job that targets containers by the label
  value `graphyflow.repo=/path/to/Graph.hls`, run with a different
  label value via `--repo-label <unique>` (and keep `--container-prefix` unique).
  - If multiple people share the same checkout path, set a unique owner label
    value so cleanup scripts only touch your containers: `--owner-label <unique>`
    (or `GRAPHYFLOW_OWNER=...`).
  - If another worker has a cleanup job that removes containers by name patterns
    (e.g., matching `*_topo_*`), use `--name-mode hash` to avoid embedding
    project/label strings in the container name.
  Example: `scripts/hw_emu_matrix.sh --cases scripts/cases/topo_hw_emu_cases.csv --resume`

- `scripts/summarize_hw_emu_tsv.sh`  
  Converts a TSV summary produced by `scripts/hw_emu_matrix.sh` into a Markdown
  table (final attempt per key).  
  Example: `scripts/summarize_hw_emu_tsv.sh target/parallel_logs/topo_hw_emu_matrix.tsv`

- `scripts/run_topology_variants_hwemu_matrix.sh`  
  Orchestrates the multi-group topology `hw_emu` validation end-to-end:
  waits for all 12 topology-variant `hw_emu` xclbins to exist under
  `target/generated_hls/`, then runs the **24-case** matrix
  (`scripts/cases/topo_hw_emu_cases.csv`) with a fixed iteration count
  (`GRAPHYFLOW_MAX_ITERS`, default `2`) and produces a Markdown summary.  
  It also passes a default `--no-log-seconds 900` to avoid wasting hours on a
  fully-silent hang (override with `--no-log-seconds 0` to disable).
  It defaults `--min-timeout-sec 2400` to avoid false `rc=137` kills on slow-but-progressing runs
  (override with `--min-timeout-sec 0` to disable).
  It defaults `--busy-retries 30` so device contention doesn’t immediately mark
  the whole matrix as failed.
  Use `--resume` if you need to restart the matrix and want to preserve the TSV
  and skip already-PASS cases.
  If you suspect external docker cleanup is killing runs, pass through
  `--repo-mount` and/or `--container-prefix` (it forwards these to
  `scripts/hw_emu_matrix.sh`).
  Example: `tmux new-session -d -s gf_topo_matrix24 'scripts/run_topology_variants_hwemu_matrix.sh'`

- `scripts/run_depthfix_hwemu_suite.sh`  
  Convenience wrapper that runs `hw_emu` for SSSP/CC/PR on `target/graph_100_100.txt`
  and `target/graph_2048_2048.txt` (defaults: `2` fixed iterations, 30/60 minute timeouts).  
  Example: `tmux new-window -n depthfix_suite 'scripts/run_depthfix_hwemu_suite.sh'`

- `scripts/run_l44_b22_hwemu_suite.sh`  
  Convenience wrapper to run the legacy `*_topo_l44_b22_hwemu` projects for SSSP/CC/PR on `target/graph_100_100.txt`
  and `target/graph_2048_2048.txt`. This is mainly for quick spot-checks on the `l44_b22` topology.  
  Example: `GRAPHYFLOW_MAX_ITERS=2 GRAPHYFLOW_TIMEOUT_SECONDS=3600 scripts/run_l44_b22_hwemu_suite.sh`

## Docker HW Builds

- `scripts/docker_hw_build_guard.sh`  
  Runs `make all TARGET=hw` inside the Vitis docker image and monitors
  `.../_x/link/vivado/vpl/runme.log` for known crash signatures (and optional
  checkpoints like `config_hw_runs` completion). Logs to `target/parallel_logs/`.  
  Example: `scripts/docker_hw_build_guard.sh --project target/generated_hls/sssp_topo_l44_b22_hw --platform-host /path/to/platform/xilinx_u55c --device /vitis_work/platform/xilinx_u55c_gen3x16_xdma_3_202210_1.xpfm --kernel-freq 300`

- `scripts/build_hw_w4_w8_parallel.sh`  
  Queue runner that builds a fixed set of projects in parallel (default `3` at a
  time) and stops all builds on the first failure, writing a TSV summary +
  failure excerpt bundle. (Project list is currently hard-coded in the script.)  
  Example: `scripts/build_hw_w4_w8_parallel.sh --max-parallel 3`

## Emit / Compile Helpers

- `scripts/compile_hls_hw.sh`  
  Emits per-app HLS projects via `cargo run -- --emit-hls ...` and builds them
  sequentially using the emitted project `Makefile`. Useful for quick “single
  box” compilation outside the matrix runners. Supports `--kernel-freq` to pass
  `KERNEL_FREQ=<mhz>` through to `make`.  
  Example: `scripts/compile_hls_hw.sh --target hw_emu --apps sssp --kernel-freq 300`

- `scripts/compare_to_reference_topology.sh`  
  Diffs a few key emitted files against a reference output directory (handy
  during template refactors).  
  Example: `scripts/compare_to_reference_topology.sh /path/to/ref_out /path/to/new_out`

## Matrix / Retry Helpers

- `scripts/run_all_hw_emu_pending.sh`  
  Builds a large “case list” (w4/w8/w11/w16/w22/w24 × multiple graphs) and runs
  only cases that have not yet produced a PASS log/TSV entry under
  `target/parallel_logs/`. Designed for long “eventually make everything PASS”
  sessions. Tunables via env: `MAX_ROUNDS`, `RETRIES`, `STALL_SECONDS`,
  `POLL_SECONDS`. It also cleans up **only** Docker containers labeled with
  `graphyflow.repo=/path/to/Graph.hls` and `graphyflow.kind=hw_emu`.

- `scripts/run_pending_parallel.sh`  
  Splits a cases CSV by project and runs multiple projects concurrently (each
  project remains serialized to avoid `.run/` conflicts). Produces a combined
  summary TSV. It cleans up **only** Docker containers labeled with
  `graphyflow.repo=/path/to/Graph.hls` and `graphyflow.kind=hw_emu`.

- `scripts/run_large_hw_emu.sh`  
  Runs a larger `hw_emu` sweep (weighted + unweighted graphs under `target/`) across multiple project families by
  delegating to `scripts/parallel_hw_emu.sh`. Intended for long-running “bigger graph” validation sessions.  
  Example: `scripts/run_large_hw_emu.sh`

- `scripts/retry_failed_cases.sh`  
  Converts a previous TSV summary into a new retry CSV containing only failed
  rows and runs them through `scripts/hw_emu_matrix.sh`.  
  Example: `scripts/retry_failed_cases.sh target/parallel_logs/topo_hw_emu_matrix.tsv retry1 2`

## Memory / Vivado Workarounds

- `scripts/docker_peak_mem.sh`  
  Runs a docker command and samples `docker stats` to report peak container
  memory usage (MiB). Used by `scripts/parallel_hw_emu.sh --measure-only`.

- `scripts/vpl_no_webtalk.sh`  
  Wrapper for Vitis `vpl` that strips `--webtalk_flag` (workaround for some
  container crashes). Used by emitted build flows when enabled.

## Status / Process Hygiene

- `scripts/monitor_gf_container.sh`  
  Periodically prints docker state (`running` + `OOMKilled`) and tails the last
  few lines of a host-side log until the container exits. Useful to avoid
  spawning many `tail -f` sessions while a long `v++` link or `hw_emu` run is in
  progress.  
  Example: `scripts/monitor_gf_container.sh gf_hwemu_sssp_topo_l44_b22_hwemu_depthfix___graph_32_32.txt___20260224_160339 target/parallel_logs/sssp_topo_l44_b22_hwemu_depthfix__graph_32_32.txt__hw_emu_2it_20260224_160339.log`

- `scripts/refresh_topo_hwemu_status.sh`  
  Recomputes final-attempt PASS/non-pass counts from a topology matrix TSV and
  refreshes the `TOPO_MATRIX_STATUS` block in `docs/current_status.md`. Can
  optionally wait until a `tmux` session exits before updating.  
  Example: `scripts/refresh_topo_hwemu_status.sh --summary target/parallel_logs/topo_hw_emu_matrix_24_20260225_192908.tsv --wait-tmux gf_topo_matrix24`

- `scripts/refresh_topo_hwemu_status.sh`  
  Recomputes final-attempt PASS/non-pass counts from a topology `hw_emu` matrix
  TSV and refreshes the `TOPO_MATRIX_STATUS` block in `docs/current_status.md`.
  Can optionally wait for a tmux session to exit before updating.  
  Example: `scripts/refresh_topo_hwemu_status.sh --summary target/parallel_logs/topo_hw_emu_matrix_24_20260225_192908.tsv --wait-tmux gf_topo_resume_192908`

- `scripts/graphyflow_status.sh`  
  Quick “dashboard” showing tmux sessions, Docker containers (both current
  `DOCKER_HOST` context and system Docker), active `graphyflow_host` / Vitis
  processes, and recent logs under `target/parallel_logs/`.

- `scripts/cleanup_graphyflow_docker.sh`  
  Dry-run by default. Finds **system Docker** containers using
  `vivado-runner:22.04-feiyang` that mount `/vitis_work/refactor_Graphyflow`.
  By default it only matches containers that mount **this repo's root** at that
  destination (protects other workers using a different checkout). Use
  `--any-source --force` to remove any matching mount source (dangerous).

- `scripts/fix_repo_permissions_docker.sh`  
  Repairs a repo checkout after an accidental root-run Docker job created
  root-owned artifacts on the bind mount (common symptoms: `PermissionDenied`
  when re-emitting HLS projects, or `rm -rf` failures on `.run/`). It can
  remove `.run/` and `chown -R` a subtree back to your uid/gid.  
  Example: `scripts/fix_repo_permissions_docker.sh --path target/generated_hls/sssp_topo_l444_b11`
