# DDR Codegen Guide: Generating 5 Algorithm Variants

This guide keeps the current DDR codegen flow unchanged and updates the
post-generation build/run workflow so each emitted project can be run directly
in `sw_emu`, `hw_emu`, or `hw`.

## Supported Algorithms

| Algorithm | DSL File | Reduce | Edge Weight | Node Property |
|-----------|----------|--------|-------------|---------------|
| SSSP | `sssp_ddr_4b4l_codegen.dsl` | Min | `int<10>` | `fixed<32,16>` dist |
| CC | `cc_bitmask_ddr_4b4l.dsl` | Or | none | `int<32>` label |
| WCC | `wcc_ddr_4b4l.dsl` | Max | none | `int<32>` label |
| PR | `pr_ddr_4b4l.dsl` | Sum | none | `float` rank + `int<32>` out_deg |
| AR | `ar_ddr_4b4l.dsl` | Sum | none | `float` rank + `int<32>` out_deg |

## Step 1: Generate HLS Projects

```bash
# From the repo root
cargo run --bin refactor_Graphyflow -- --emit-hls apps/topology_variants/sssp_ddr_4b4l_codegen.dsl
cargo run --bin refactor_Graphyflow -- --emit-hls apps/topology_variants/cc_bitmask_ddr_4b4l.dsl
cargo run --bin refactor_Graphyflow -- --emit-hls apps/topology_variants/wcc_ddr_4b4l.dsl
cargo run --bin refactor_Graphyflow -- --emit-hls apps/topology_variants/pr_ddr_4b4l.dsl
cargo run --bin refactor_Graphyflow -- --emit-hls apps/topology_variants/ar_ddr_4b4l.dsl
```

Output lands in `target/generated_hls/<project_name>/`.

Generated project names:
- `sssp_ddr_4b4l_codegen`
- `cc_bitmask_ddr_4b4l`
- `wcc_ddr_4b4l`
- `pr_ddr_4b4l`
- `ar_ddr_4b4l`

## Step 2: Prepare Your Shell and Dataset

Source the same Vitis/XRT environment you normally use before `make`/`v++`.
If you want the generated `run.sh` to source it for you, export:

```bash
export GRAPHYFLOW_ENV_SH=/path/to/your/env.sh
```

Optional local smoke dataset from the repo root:

```bash
mkdir -p target/ddr_smoke
(
  cd target/ddr_smoke
  python3 ../../src/hls_assets/gen_random_graph.py 32 64 --have_weight
)
```

This writes `target/ddr_smoke/graph.txt`.

Use a weighted three-column edge list when you want one dataset that can be
reused across all five DDR projects. SSSP needs the third column; the other
four DDR apps tolerate it and ignore the extra weight payload.

## Step 3: Build and Run One Generated Project Directly

The emitted project interface is:

```bash
./run.sh <mode> <dataset>
```

where `<mode>` is one of `sw_emu`, `hw_emu`, or `hw`.

Example from the repo root:

```bash
cd target/generated_hls/sssp_ddr_4b4l_codegen
make all TARGET=sw_emu
./run.sh sw_emu ../../ddr_smoke/graph.txt
```

### Build Commands

- Rebuild only the host executable:

```bash
make cleanexe
make exe TARGET=<mode>
```

- Build the host executable plus the mode-specific xclbin:

```bash
make all TARGET=<mode>
```

If you need to override the emitted platform, append:

```bash
DEVICE=/path/to/platform.xpfm
```

The current emitted DDR projects default to `xilinx_u200_gen3x16_xdma_2_202110_1`
in their generated `Makefile`.

### Run Commands by Mode

| Mode | Build | Run |
|------|-------|-----|
| `sw_emu` | `make all TARGET=sw_emu` | `./run.sh sw_emu ../../ddr_smoke/graph.txt` |
| `hw_emu` | `make all TARGET=hw_emu` | `./run.sh hw_emu ../../ddr_smoke/graph.txt` |
| `hw` | `make all TARGET=hw` | `./run.sh hw ../../ddr_smoke/graph.txt` |

### Timeout Starting Points

These are wrapper-friendly starting points, not hard guarantees:

| Mode | Suggested Timeout |
|------|-------------------|
| `sw_emu` | 600s |
| `hw_emu` | 1200s for SSSP/CC/WCC, 3600s for PR/AR |
| `hw` | 3600s+ depending on board and dataset |

## Step 4: Use the Non-Docker Helper Scripts

The direct per-project `./run.sh <mode> <dataset>` interface is the primary
flow. Two repo-level helpers are available when you want build/run orchestration
from the repo root.

### Run One Generated Project

```bash
scripts/run_target_one.sh \
  --project sssp_ddr_4b4l_codegen \
  --mode hw_emu \
  --graph target/ddr_smoke/graph.txt \
  --build-kernels \
  --timeout 1800
```

Notes:
- `--rebuild-exe` forces `make cleanexe && make exe TARGET=<mode>`.
- `--build-kernels` forces `make all TARGET=<mode>`.
- `--device /path/to/platform.xpfm` overrides the `DEVICE=` value passed to `make`.
- `--device-bdf <bdf>` and `--device-index <n>` are forwarded to the hardware
  watchdog path when `--mode hw`.
- `--big-edge-per-ms <n>` and `--little-edge-per-ms <n>` are compile-time host
  settings. The runner passes them into `make` so the host is compiled with
  `-DBIG_EDGE_PER_MS=<n>` and `-DLITTLE_EDGE_PER_MS=<n>`.
- Supplying either throughput flag forces a clean host rebuild so the new macro
  values actually take effect.
- Those throughput flags are host-only. They do not trigger an implicit kernel
  rebuild. If the xclbin is missing, the helper fails and asks you to rerun
  with `--build-kernels`.
- `--log-dir <path>` overrides the default combined-log folder
  (`target/parallel_logs`).
- `--env /path/to/env.sh` lets the helper source your environment script before
  build and run.

Example with custom compile-time repartition throughput:

```bash
scripts/run_target_one.sh \
  --project sssp_ddr_4b4l_codegen \
  --mode hw_emu \
  --graph target/ddr_smoke/graph.txt \
  --big-edge-per-ms 10000 \
  --little-edge-per-ms 40000 \
  --timeout 1800
```

### Run All Five DDR Projects

The batch helper usage is:

```bash
scripts/run_ddr_codegen_all.sh <mode> [dataset_dir] [options]
```

where:
- `<mode>` is one of `sw_emu`, `hw_emu`, or `hw`
- `[dataset_dir]` is a folder containing the dataset files to run
  (default: `/path/to/datasets`)
- `[options]` are forwarded to `scripts/run_target_one.sh`

Examples:

```bash
scripts/run_ddr_codegen_all.sh \
  sw_emu \
  --timeout 600

scripts/run_ddr_codegen_all.sh \
  hw_emu \
  /path/to/datasets \
  --build-kernels \
  --timeout 3600

scripts/run_ddr_codegen_all.sh \
  hw \
  /path/to/datasets \
  --build-kernels \
  --timeout 7200
```

Common forwarded options:
- `--build-kernels`
- `--rebuild-exe`
- `--timeout <seconds>`
- `--iters <n>`
- `--device /path/to/platform.xpfm`
- `--device-bdf <bdf>` and `--device-index <n>` for `hw`
- `--big-edge-per-ms <n>` and `--little-edge-per-ms <n>`
- `--env /path/to/env.sh`
- `--output-dir <path>` for the batch log root

One concrete `hw_emu` example:

```bash
scripts/run_ddr_codegen_all.sh \
  hw_emu \
  /path/to/datasets \
  --build-kernels \
  --timeout 3600
```

This runs the fixed DDR project list sequentially across every dataset file in
the chosen folder:
- `sssp_ddr_4b4l_codegen`
- `cc_bitmask_ddr_4b4l`
- `wcc_ddr_4b4l`
- `pr_ddr_4b4l`
- `ar_ddr_4b4l`

For the default dataset folder, the batch script recognizes these 14 filenames
and applies dataset-specific compile-time host defaults for repartition
throughput:
- `graph500-scale23-ef16_adj.mtx` -> `graph500` -> `400000 / 1000000`
- `rmat-19-32.txt` -> `r19` -> `250000 / 950000`
- `rmat-21-32.txt` -> `r21` -> `290000 / 1000000`
- `rmat-24-16.txt` -> `r24` -> `270000 / 1000000`
- `amazon-2008.mtx` -> `am` -> `160000 / 460000`
- `ca-hollywood-2009.mtx` -> `hollywood` -> `300000 / 1000000`
- `dbpedia-link.mtx` -> `dbpedia` -> `190000 / 900000`
- `soc-flickr-und.mtx` -> `flickr` -> `120000 / 800000`
- `soc-LiveJournal1.txt` -> `LiveJournal1` -> `170000 / 700000`
- `soc-orkut-dir.mtx` -> `orkut` -> `280000 / 850000`
- `web-baidu-baike.mtx` -> `baidu` -> `160000 / 800000`
- `web-Google.mtx` -> `Google` -> `150000 / 580000`
- `web-hudong.mtx` -> `hudong` -> `180000 / 850000`
- `wiki-topcats.txt` -> `topcats` -> `170000 / 830000`

Passing `--big-edge-per-ms` and/or `--little-edge-per-ms` to
`scripts/run_ddr_codegen_all.sh` overrides those per-dataset defaults for the
entire batch. The script stops on the first failing project or on the first
unrecognized dataset filename. This helper is intended for the known benchmark
dataset set above, not an arbitrary scratch folder like `target/ddr_smoke/`.
Because the batch helper always passes throughput values into
`scripts/run_target_one.sh`, first-time kernel builds should use
`--build-kernels` explicitly.

## Step 5: Verify Results

Successful runs print `SUCCESS: Results match!`. The helper scripts also print
`==> PASS` on success.

`scripts/run_target_one.sh` logs are written to:

```text
target/parallel_logs/<project>__<graph>__<mode>_<n>it_<timestamp>.log
```

`scripts/run_ddr_codegen_all.sh` creates a batch log root under:

```text
target/parallel_logs/ddr_codegen_all_<timestamp>/
```

and then writes each dataset's project logs under:

```text
target/parallel_logs/ddr_codegen_all_<timestamp>/<dataset_alias>/
```

The direct `./run.sh` path writes to the terminal only.

## Dataset Notes

- No small `.txt` smoke graph is checked into the repo right now.
- Use the local smoke generation above or reuse a path from `docs/dataset_paths.md`.
- The host loader auto-detects `.txt` (0-based) versus `.mtx` (1-based).

## DSL Structure

Each `.dsl` file has three sections:

```text
{ Node: { ... } Edge: { ... } }     // Schema
HlsConfig { ... }                    // Hardware config
{ ... }                              // Algorithm (GAS dataflow)
```

### Key HlsConfig Options

| Option | Description | Default |
|--------|-------------|---------|
| `memory: ddr` | Use DDR memory backend (vs `hbm`) | required |
| `local_id_bits: 22` | Compressed vertex ID width | 22 for weighted, 32 for PR/AR |
| `zero_sentinel: true` | Use 0 as empty value in reduce | required for DDR |
| `topology` | SLR placement and pipeline count | see DSL files |

### Algorithm Dataflow Pattern

All algorithms follow the GAS (Gather-Apply-Scatter) model:

```text
edges -> scatter(src_prop [+ edge_weight]) -> reduce(key=dst, fn) -> apply(old, new) -> output
```

| Algorithm | Scatter | Reduce | Apply |
|-----------|---------|--------|-------|
| SSSP | `src.dist + e.weight` | `min(x, y)` | `min(self.dist, new)` |
| CC | `src.label` | `x \| y` | `self.label \| new` |
| WCC | `src.label` | `max(x, y)` | passthrough |
| PR | `src.rank` | `x + y` | `0.15 + 0.85 * sum / self.out_deg` |
| AR | `src.rank` | `x + y` | `0.15 + 0.85 * sum / self.out_deg` |

## Differences from SG Reference

The codegen output differs from the SG static reference in these ways:

### Parameterization (all algorithms)
- Hardcoded `ap_uint<26>` -> `ap_uint<NODE_ID_BITWIDTH - LOG_DIST_PER_WORD>`
- Hardcoded `ap_uint<20>` / `range(19,19)` -> `local_id_t` / `range(LOCAL_ID_MSB, LOCAL_ID_MSB)`
- `INFINITY_DIST = 16384` -> `INFINITY_POD = ~0u` (max unsigned value)
- `max_val = 16384.0` via `reinterpret_cast` -> compile-time `INFINITY_POD`
- `typedef` -> `using` (C++11 style)
- `switch/case` -> `if/else` chains (for `count_end_ones`)
- Templates removed, specialized for concrete stream types

### Edge packing (all algorithms)
- `AXI_BUS_WIDTH / (NODE_ID_BITWIDTH + NODE_ID_BITWIDTH)` -> `EDGES_PER_WORD` define
- `packed_edge.range(31, 22)` -> `.edge_prop` field (conditional on `EDGE_PROP_BITS > 0`)
- `edge.weight` struct field -> `edge.edge_prop` (generic name)

### Reduce logic (all algorithms)
- SG: separate `msb_out`, `lsb_out` variables for upper/lower halves
- Codegen: single `updated_val` with packed assignment
- Zero-sentinel check: `(update != 0x0) ? ...` -> `(incoming != 0u) ? ...`

### Merger logic (all algorithms)
- `tmp_prop_arrary[16]` -> `tmp_prop_arrary[DIST_PER_WORD]`
- Identity init: `max_pod` from float reinterpret -> `identity_pod = INFINITY_POD`

### Apply kernel (SSSP)
- `(old < update) ? old : update` -> `(old > update) ? update : old` (mathematically equivalent)

### PR/AR host initialization
- SG: `avg_outdegree`-based initialization
- Codegen: ReGraph-style fixed-point with `SCALE = 1<<30`, `inv_n / od` per vertex

### CC kernel
- **Identical** to SG reference (static copy, no codegen changes)

## Verified Status

The runtime flow above now supports `sw_emu`, `hw_emu`, and `hw`, but the
recorded verification status in this repo is still the last `hw_emu` check:

| Algorithm | Project | Status | Timeout Used |
|-----------|---------|--------|--------------|
| SSSP | `sssp_ddr_4b4l_codegen` | PASS | 1200s |
| CC | `cc_bitmask_ddr_4b4l` | PASS | 1200s |
| WCC | `wcc_ddr_4b4l` | PASS | 1200s |
| PR | `pr_ddr_4b4l` | PASS | 3600s |
| AR | `ar_ddr_4b4l` | PASS | 3600s |
