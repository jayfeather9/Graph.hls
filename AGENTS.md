# Repository Guidelines

## Project Structure & Module Organization

- `src/domain/`: core types (AST/IR/GAS), errors, and HLS template primitives.
- `src/engine/`: lowering passes, simulator, and HLS codegen/extraction.
- `src/services/`: parsing and front-end utilities.
- `src/utils/`: graph generation and reference calculations.
- `apps/`: example programs in `*.dsl` plus fixtures under `apps/test_graphs/`.
- `src/hls_assets/`: HLS project scaffold + golden kernel sources under `src/hls_assets/scripts/kernel/`.
- `tests/`: integration tests (simulator/reference checks and optional host build smoke test).

## Build, Test, and Development Commands

- `cargo build`: compile the Rust crate.
- `cargo test`: run unit + integration tests (may include golden diffs for HLS templates).
- `cargo fmt`: format Rust sources with rustfmt.
- `cargo clippy --all-targets`: lint all targets (recommended before PRs).
- `cargo run -- <app|dsl-path>`: parse/lower and print AST/IR/GAS debug summaries.
- `cargo run -- --generate sssp 100 500 [seed]`: generate a random graph as JSON.
- `cargo run -- --simulate-json sssp path/to/graph.json [max_iters]`: run the reference simulator on a JSON graph.
- `cargo run -- --emit-hls sssp /tmp/hls_out`: emit an HLS project tree (see `docs/emit_hls.md`).

## Coding Style & Naming Conventions

- Rust style: follow `cargo fmt`; prefer small, pure helpers and cohesive modules.
- Naming: `snake_case` for functions/tests, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Fixtures: keep names aligned (e.g., `apps/sssp.dsl` ↔ `apps/test_graphs/sssp_small.json`).

## Testing Guidelines

- Frameworks: `#[test]` and `rstest` (for parameterized cases).
- HLS goldens: when changing `src/domain/hls_template/`, re-emit with `--emit-hls` and intentionally update `src/hls_assets/scripts/kernel/*` if needed.
- Optional host build smoke test: `RUN_HOST_BUILD_TEST=1 cargo test host_build_smoke` (requires external Xilinx/Vitis environment configured in `tests/host_build.rs`).

## Commit & Pull Request Guidelines

- Commit messages: keep short, imperative, and specific (e.g., “Fix …”, “Add …”).
- PRs: include a clear summary, link relevant docs/issues, and explicitly note when emitted artifacts or HLS goldens change. Ensure `cargo test` is green.

