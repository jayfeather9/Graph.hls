# `--emit-hls` workflow

How the CLI materializes an HLS project today and how to keep it healthy.

## What happens
1. CLI entrypoint: `refactor_Graphyflow --emit-hls <app|dsl> [output_dir]`.
2. Input: DSL is parsed and lowered to GAS, but emission only checks the target property (`dist`) to guard the SSSP flow.
3. Templates: the generator copies the static project scaffold from `src/hls_assets/` into the destination.
4. Kernel rendering: all kernel sources/headers are re-rendered from Rust templates into `scripts/kernel/`:
   - `shared_kernel_params.h`
   - `apply_kernel.cpp`
   - `big_merger.cpp`
   - `little_merger.cpp`
   - `hbm_writer.cpp`
   - `graphyflow_big.cpp`
   - `graphyflow_little.cpp`
5. Output: the destination contains a ready-to-build Vitis project tree (host sources are still copied, not templated).

## Keeping outputs in sync
- Goldens for the kernel sources live in `src/hls_assets/scripts/kernel/`.
- When you change a template, regenerate with `cargo run -- --emit-hls sssp /tmp/hls_out` and copy the updated kernel files back into `src/hls_assets/scripts/kernel/`.
- Unit tests in `src/domain/hls_template/mod.rs` normalize whitespace and diff rendered output against those goldens; update the goldens whenever the template code changes intentionally.

## Tests
- `cargo test builds_sssp_hls_project` ensures the emitted tree contains expected artifacts.
- Golden diff tests: `apply_kernel_matches_golden`, `big_merger_matches_golden`, `little_merger_matches_golden`, `hbm_writer_matches_golden`, `graphyflow_big_matches_golden`, `graphyflow_little_matches_golden`.

## Gaps / future work
- Host files (`src/hls_assets/scripts/host/*`) are not templated; they are copied verbatim. Port them to Rust templates and add goldens/tests once available.
- `graphyflow_big` still relies on raw snippets; extend `domain::hls` to remove those raw blocks.
