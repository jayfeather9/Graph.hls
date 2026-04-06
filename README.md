# Graph.hls

A domain-specific compiler framework for FPGA-based graph accelerators.

Graph.hls organizes graph accelerator parameters into three hierarchical levels by modification cost (L1: graph constants, L2: microarchitecture, L3: dataflow strategies) and provides two engines: **SG-Architect** for automatic HLS code generation with design space exploration, and **SG-Scope** for fast IR-level simulation and validation.

## Requirements

- **Rust** edition 2024, minimum rustc 1.85+ (tested with 1.94.1)
- **Xilinx Vitis 2024.1 + XRT** (for FPGA synthesis and execution)
- **Python 3** with `matplotlib` and `pandas` (for plotting)
- **Linux** (Ubuntu recommended)

All Rust crate dependencies are managed via `cargo`:
chumsky 0.9, thiserror 1, serde/serde\_json 1, rand 0.8, rstest 0.19.

## Quick Start

```bash
# Build the compiler
cargo build --release

# Run tests
cargo test

# Parse and inspect a DSL program
cargo run -- apps/sssp.dsl

# Generate HLS project from a DSL
cargo run -- --emit-hls apps/topology_variants/sssp_topo_l11_b3.dsl target/my_sssp

# Simulate on a small graph
cargo run -- --generate sssp 1000 5000 42 > /tmp/graph.json
cargo run -- --simulate-json apps/sssp.dsl /tmp/graph.json 32
```

## Repository Structure

```
.
├── apps/                           # DSL algorithm specifications
│   ├── sssp.dsl                    # SSSP (unweighted)
│   ├── pagerank.dsl                # PageRank
│   ├── connected_components.dsl    # Connected Components
│   ├── ar.dsl                      # ArticleRank
│   ├── wcc.dsl                     # Weakly Connected Components
│   ├── bfs.dsl                     # BFS
│   ├── topology_variants/          # Algorithm DSLs with explicit topologies
│   │   ├── sssp_topo_l11_b3.dsl          # SSSP, 3B11L, 32-bit, HBM
│   │   ├── sssp_topo_l11_b3_bw8.dsl      # SSSP, 3B11L, 8-bit, HBM
│   │   ├── sssp_topo_l11_b3_no_l1.dsl    # SSSP, 3B11L, no-L1 preprocess
│   │   ├── sssp_auto_grouped_32bit.dsl   # SSSP, auto-grouped template
│   │   ├── *_ddr_4b4l.dsl               # DDR variants (U200)
│   │   └── ...                           # 50+ topology variants
│   ├── buggy_demos/                # Intentionally buggy DSLs (Table 3)
│   └── test_graphs/                # Small test fixtures
├── src/                            # Compiler source (Rust, ~10k LOC)
│   ├── main.rs                     # CLI entry point
│   ├── services/parser.rs          # DSL parser (chumsky)
│   ├── domain/                     # AST, IR, GAS types, HLS templates
│   ├── engine/                     # Compiler passes, simulator, codegen
│   └── utils/                      # Graph tools, reference calcs, predictors
├── scripts/                        # Automation scripts
│   ├── ae_emit_fig7.sh             # Emit HLS for Fig 7 (vs ReGraph, U55C)
│   ├── ae_emit_fig8.sh             # Emit HLS for Fig 8 (vs ThunderGP, U200)
│   ├── ae_emit_fig9.sh             # Emit HLS for Fig 9 (ablation study)
│   ├── ae_fig10.sh                 # Run Fig 10 (SG-Scope speedup)
│   ├── ae_tab3.sh                  # Run Table 3 (debugging speedup)
│   ├── ae_build.sh                 # Parallel HW builder
│   ├── ae_check_builds.sh          # Build status checker + retry
│   ├── ae_run.sh                   # FPGA experiment runner
│   ├── ae_plot.py                  # Consolidated figure plotter (Fig 7-9)
│   ├── batch_simulate.sh           # Batch SG-Scope simulation runner
│   ├── plot_sim_results.py         # Simulation result plotter (Fig 10)
│   ├── convert_regraph_csv.py      # ReGraph output → baseline CSV converter
│   ├── convert_thundergp_csv.py    # ThunderGP output → baseline CSV converter
│   ├── compile_hls_hw.sh           # HLS compilation orchestrator
│   ├── run_target_one.sh           # Run one HLS project natively
│   ├── run_hwemu_docker_one.sh     # Run one HLS project in Docker (hw_emu)
│   ├── run_ddr_codegen_all.sh      # Run all DDR apps on all datasets
│   ├── parallel_hw_emu.sh          # Parallel hw_emu with memory budgeting
│   ├── hw_emu_matrix.sh            # Matrix hw_emu execution
│   └── topology_sweep.py           # Topology design space exploration
├── data/
│   ├── swemu_baseline.csv          # Pre-collected C-Sim timing (Fig 10)
│   ├── hwemu_debug_baseline.tsv    # Pre-collected hw_emu timing (Table 3)
│   └── dataset_short_names.csv     # Dataset abbreviations for plots
├── docs/
│   ├── dsl_writing_guide.md        # How to write DSL programs
│   ├── batch_simulation_guide.md   # Batch simulation usage
│   ├── simulator_speed_guide.md    # Simulator internals and speed
│   ├── emit_hls.md                 # HLS emission guide
│   ├── ddr_codegen_guide.md        # DDR code generation guide
│   ├── hw_emu_docker.md            # Docker setup for hw_emu
│   ├── scripts.md                  # Script reference
│   ├── hls_templates_overview.md   # HLS template design
│   ├── hls_template_status.md      # Template coverage status
│   ├── grouping_static_model_*.json  # Predictor models (8-bit and 32-bit)
│   └── ...
├── tests/                          # Integration tests
├── ARCHITECTURE.md                 # Compiler architecture overview
├── language_definition.md          # DSL formal specification
├── algorithm_defines.md            # Supported algorithms
├── Cargo.toml
└── Cargo.lock
```

## Artifact Evaluation

This section describes how to reproduce every experimental result in the paper.

### Datasets

14 benchmark graphs are used (Table 2 in the paper). Place them in a single directory:

| Short | Full Name | Source |
|-------|-----------|--------|
| R19 | rmat-19-32.txt | R-MAT generator |
| R21 | rmat-21-32.txt | R-MAT generator |
| R24 | rmat-24-16.txt | R-MAT generator |
| G23 | graph500-scale23-ef16\_adj.mtx | graph500 |
| AM | amazon-2008.mtx | Network Data Repository |
| HW | ca-hollywood-2009.mtx | Network Data Repository |
| DB | dbpedia-link.mtx | Network Data Repository |
| FU | soc-flickr-und.mtx | Network Data Repository |
| LJ | soc-LiveJournal1.txt | SNAP |
| OR | soc-orkut-dir.mtx | Network Data Repository |
| BB | web-baidu-baike.mtx | Network Data Repository |
| GG | web-Google.mtx | Network Data Repository |
| HD | web-hudong.mtx | Network Data Repository |
| TC | wiki-topcats.txt | SNAP |

Default dataset path: `/path/to/datasets/`. Override with `--dataset-dir`.

### Overview: End-to-End Workflow

```
1. Build compiler     →  cargo build --release
2. Emit HLS projects  →  ae_emit_fig*.sh
3. Build bitstreams   →  ae_build.sh (parallel, ~4-6h per project)
4. Run on FPGA        →  ae_run.sh (sequential, ~1-10 min per graph)
5. Plot figures        →  ae_plot.py / plot_sim_results.py
```

### Figure 7: Graph.hls vs ReGraph (U55C, HBM)

Compares PR, CC, SSSP performance on Alveo U55C. Expected: 2.6x average speedup.

```bash
# 1. Emit + build Graph.hls
./scripts/ae_emit_fig7.sh
./scripts/ae_build.sh --build-list target/ae/fig7/build_list.txt --parallel 1

# 2. Run Graph.hls on all datasets
./scripts/ae_run.sh --figure 7

# 3. Build + run ReGraph (separate repo, see instructions printed by ae_emit_fig7.sh)
#    ReGraph app mapping: bfs=SSSP, pr=PageRank, cc=CC

# 4. Convert ReGraph results and plot
python3 scripts/convert_regraph_csv.py regraph_used.csv regraph_baseline.csv
python3 scripts/ae_plot.py --fig7 \
    --csv target/ae/fig7/results.csv \
    --baseline-csv regraph_baseline.csv \
    -o fig7_vs_regraph.pdf
```

### Figure 8: Graph.hls vs ThunderGP (U200, DDR)

Compares PR, Weighted SSSP, CC, AR, WCC on Alveo U200. Expected: 1.2x average speedup.

```bash
# 1. Emit + build Graph.hls (DDR frequencies auto-patched)
./scripts/ae_emit_fig8.sh
./scripts/ae_build.sh --build-list target/ae/fig8/build_list.txt --parallel 1

# 2. Run Graph.hls
./scripts/ae_run.sh --figure 8

# 3. Build + run ThunderGP (separate copies, see instructions printed by ae_emit_fig8.sh)
#    Each app: cd ThunderGP_<app> && python3 benchmark.py

# 4. Convert ThunderGP results and plot
python3 scripts/convert_thundergp_csv.py \
    ThunderGP_sssp/benchmark_sssp_*.csv \
    ThunderGP_pr/benchmark_pr_*.csv \
    ThunderGP_cc/benchmark_cc_*.csv \
    ThunderGP_ar/benchmark_ar_*.csv \
    ThunderGP_wcc/benchmark_wcc_*.csv \
    thundergp_baseline.csv
python3 scripts/ae_plot.py --fig8 \
    --csv target/ae/fig8/results.csv \
    --baseline-csv thundergp_baseline.csv \
    -o fig8_vs_thundergp.pdf
```

### Figure 9: Ablation Study (L1/L2/L3 Levels)

SSSP on U55C with 5 configurations. Expected: Naive 0.71x, L1 1.99x, L1+L2 2.95x, L1+L3 2.52x, L1+L2+L3 4.48x.

```bash
# 1. Emit all 5 configs (Naive, L1, L1+L2, L1+L3, L1+L2+L3)
./scripts/ae_emit_fig9.sh

# 2. Build all projects
./scripts/ae_build.sh --build-list target/ae/fig9/build_list.txt --parallel 1

# 3. Run all configs
./scripts/ae_run.sh --figure 9

# 4. Plot (needs ReGraph SSSP baseline from Fig 7)
python3 scripts/ae_plot.py --fig9 \
    --csv target/ae/fig9/results.csv \
    --baseline-csv regraph_baseline.csv \
    -o fig9_ablation.pdf
```

The 5 configurations:
- **Naive**: 32-bit, 3B11L, `no_l1_preprocess: true` (fixed 80/20 partition, no throughput tuning)
- **L1**: 32-bit, 3B11L, with L1 throughput-based repartitioning
- **L1+L2**: 8-bit, 3B11L, with L1
- **L1+L3**: 32-bit, per-dataset grouped topology (32-bit predictor), with L1
- **L1+L2+L3**: 8-bit, per-dataset grouped topology (8-bit predictor), with L1

### Figure 10: SG-Scope Simulation Speedup

SG-Scope IR simulation vs Vitis C-Sim for PR, CC, SSSP. Expected: 301.6x average speedup. **No FPGA required.**

```bash
./scripts/ae_fig10.sh
# Output: target/ae/fig10/fig10_sgscope_speedup.pdf
```

This runs `batch_simulate.sh` across all datasets and plots speedup using pre-collected C-Sim baseline from `data/swemu_baseline.csv`.

### Table 3: Debugging Time Comparison

SG-Scope vs HW emulation across 3 error scenarios (32K nodes, 512K edges). **No FPGA required.**

```bash
./scripts/ae_tab3.sh
# Output: target/ae/tab3/tab3_results.csv + printed table
```

Pre-collected HW emulation times in `data/hwemu_debug_baseline.tsv`. To reproduce HW emulation yourself, see instructions printed by the script (requires Docker + Vitis).

## DSL Reference

Algorithm specifications are written in the Graph.hls DSL:

```
{
    Node: { dist: int<32> }
    Edge: { weight: int<32> }
}

HlsConfig {
    memory: hbm                    # hbm (default) or ddr
    no_l1_preprocess: false        # true for naive partition mode
    topology: {
        apply_slr: 1
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: [
            { pipelines: 11 merger_slr: 1 pipeline_slr: [0,1,2,0,1,2,0,1,2,0,1] }
        ]
        big_groups: [
            { pipelines: 3 merger_slr: 1 pipeline_slr: [2,1,2] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + e.weight)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    relaxed = map([min_dists], lambda d: self.dist > d ? d : self.dist)
    return relaxed as result_node_prop.dist
}
```

Supported types: `int<N>`, `fixed<N,F>`, `float<N>`. Auto-grouping: set `little_groups: auto` and `big_groups: auto` with `--auto-emit-hls-from-dsl-32bit`.

## CLI Reference

| Command | Description |
|---------|-------------|
| `--emit-hls <dsl> [dest]` | Generate HLS project from DSL |
| `--simulate-json <dsl> <graph.json> [max_iters]` | Run SG-Scope simulation |
| `--generate <app> <nodes> <edges> [seed]` | Generate random test graph |
| `--convert-graph <input> <output> <dsl>` | Convert .txt/.mtx to simulator JSON |
| `--auto-emit-hls-from-dsl-32bit <dsl> <dataset> [dest] [model]` | Emit with 32-bit grouping prediction |
| `--auto-emit-sssp-bw8 <dataset> [dest] [model]` | Emit 8-bit SSSP with predicted grouping |
| `--predict-grouping32-from-static-model <model> <dataset>` | Predict 32-bit topology |
| `--predict-grouping-for-dataset <dataset> [model]` | Predict 8-bit topology |

## License

MIT
