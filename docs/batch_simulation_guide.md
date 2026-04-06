# Batch Simulation & Plotting Guide

## Overview

Three tools form the simulation benchmarking pipeline:

1. **Rust graph converter** (`--convert-graph`) — converts raw `.txt`/`.mtx` graph files to simulator JSON
2. **Bash batch script** (`scripts/batch_simulate.sh`) — runs the simulator across datasets × DSLs, outputs CSV
3. **Python plot script** (`scripts/plot_sim_results.py`) — generates paper-quality PDF plots from CSV results

## Quick Start: Two Commands

### Simulation time plot (no baseline needed)

```bash
# 1. Run simulation on all datasets
./scripts/batch_simulate.sh --format raw --mode batch \
    --dataset /path/to/datasets/ \
    --dsl "sssp,pagerank,connected_components" \
    --output sim_results.csv

# 2. Plot simulation times
python3 scripts/plot_sim_results.py \
    --sim-csv sim_results.csv \
    --dsls "sssp,pagerank,connected_components" \
    --y-mode time \
    --short-names /path/to/overall_data.csv \
    --extra-short "wiki-topcats-categories:TCC" \
    --output sim_times.pdf
```

### Speedup plot (vs pre-collected sw_emu baseline)

```bash
# 1. Run simulation
./scripts/batch_simulate.sh --format raw --mode batch \
    --dataset /path/to/datasets/ \
    --dsl "sssp,pagerank,connected_components" \
    --output sim_results.csv

# 2. Plot speedup over sw_emu baseline
python3 scripts/plot_sim_results.py \
    --sim-csv sim_results.csv \
    --dsls "sssp,pagerank,connected_components" \
    --y-mode speedup \
    --baseline-csv data/swemu_baseline.csv \
    --baseline-cols "sssp_iter0_ms,pr_iter0_ms,cc_iter0_ms" \
    --baseline-unit ms \
    --short-names /path/to/overall_data.csv \
    --extra-short "wiki-topcats-categories:TCC" \
    --output sim_speedup.pdf
```

Note: `--baseline-cols` order must match `--dsls` order.

### Speedup plot (collecting fresh sw_emu data)

Run both simulator and sw_emu in one pass, then plot speedup from the same CSV:

```bash
# 1. Run both simulator + sw_emu (requires Docker + Vitis)
./scripts/batch_simulate.sh --format raw --mode batch \
    --dataset /path/to/datasets/ \
    --dsl sssp --run both --project sssp \
    --swemu-timeout 7200 --output combined.csv

# 2. Plot speedup using the sw_emu column from the same CSV
python3 scripts/plot_sim_results.py \
    --sim-csv combined.csv \
    --dsls sssp \
    --y-mode speedup \
    --baseline-csv combined.csv \
    --baseline-cols "sssp_swemu_time_ms" \
    --baseline-unit ms \
    --output speedup.pdf
```

Or run sw_emu only (e.g. to collect baseline data):

```bash
./scripts/batch_simulate.sh --format raw --mode batch \
    --dataset /path/to/datasets/ \
    --dsl sssp --run swemu --project sssp \
    --swemu-timeout 7200 --output swemu_only.csv
```

## Tool Reference

### Rust Graph Converter

Converts a single raw graph file to simulator-compatible JSON with algorithm-specific default properties.

```
cargo run -- --convert-graph <input.txt|.mtx> <output.json> <dsl-name>
```

- `.txt` files: edge list format, one `src dst [weight]` per line. Lines starting with `#` or `%` are skipped.
- `.mtx` files: MatrixMarket coordinate format. Handles `pattern` (no values) and `integer`/`real` variants. 1-indexed coordinates are converted to 0-indexed.
- `dsl-name`: determines initial node/edge properties:
  - `sssp`: `dist=0` for node 0, `dist=999999` for others; edge `weight` from file or default 1
  - `pagerank`: `rank=1.0`, `out_deg` computed from edges
  - `connected_components`: `label=node_id`

### Bash Batch Script

```
./scripts/batch_simulate.sh [OPTIONS]
```

| Option | Values | Description |
|--------|--------|-------------|
| `--format` | `raw`, `json` | Input format. `raw` auto-converts via `--convert-graph` to a temp dir |
| `--mode` | `single`, `batch` | `single`: one file path. `batch`: scan directory for `.txt`/`.mtx`/`.json` |
| `--dataset` | path | File path (single) or directory path (batch) |
| `--dsl` | comma-list | DSL names, e.g. `"sssp,pagerank,connected_components"` |
| `--output` | path | Output CSV path (default: `sim_results.csv`) |
| `--max-iters` | integer | Max simulation iterations (default: 32) |
| `--run` | `sim`, `swemu`, `both` | What to run (default: `sim`) |
| `--project` | name | HLS project name for sw_emu (default: DSL name) |
| `--swemu-timeout` | seconds | Timeout per sw_emu run (default: 3600) |
| `--swemu-iters` | integer | sw_emu iteration count (default: 1) |
| `--swemu-args` | string | Extra args passed to `run_hwemu_docker_one.sh` |

Output CSV columns depend on `--run`:
- `sim`: `<dsl>_status, <dsl>_simulate_time_sec, <dsl>_error`
- `swemu`: `<dsl>_swemu_status, <dsl>_swemu_time_ms, <dsl>_swemu_error`
- `both`: all six columns per DSL

The script requires a pre-built binary (`cargo build`). sw_emu mode additionally requires Docker with Vitis and a pre-built HLS project under `target/generated_hls/<project>`.

### Python Plot Script

```
python3 scripts/plot_sim_results.py [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--sim-csv` | **(required)** Simulation results CSV |
| `--dsls` | **(required)** Comma-separated DSL names to plot |
| `--y-mode` | `time` (absolute seconds) or `speedup` (ratio vs baseline) |
| `--output` | Output PDF path (default: `sim_results.pdf`) |
| `--baseline-csv` | Baseline timing CSV (speedup mode only) |
| `--baseline-cols` | Comma-separated column names in baseline CSV, one per DSL |
| `--baseline-unit` | `ms` or `s` (default: `ms`) |
| `--short-names` | CSV with `dataset` and `simple_name` columns for x-axis labels |
| `--extra-short` | Extra mappings as `key1:val1,key2:val2` |
| `--title` | Plot title |
| `--y-label` | Y-axis label override |
| `--fig-width` | Figure width in inches (default: 12) |
| `--fig-height` | Figure height in inches (default: 5) |

Requires: `pip install matplotlib pandas`

## Baseline Data

`data/swemu_baseline.csv` contains combined sw_emu iteration-0 kernel times (in milliseconds) for all 15 benchmark datasets:

- **SSSP**: all 15 datasets (merged from an earlier SSSP-only sw_emu run that succeeded on all graphs, plus the 3 rmat results from the merged batch run)
- **CC**: 14/15 datasets (wiki-topcats-categories missing — sw_emu failed)
- **PR**: 14/15 datasets (wiki-topcats-categories missing — sw_emu failed)

Sources: `plot_sim_vs_swemu.py:SW_EMU_ITER0_MS` (SSSP fallback) and `swemu_times_sssp_cc_pr_merged.csv` (CC/PR).
