# HLS Templates Overview

Rust modules under `src/domain/hls_template/` describe every kernel we emit. Each module builds an `HlsCompilationUnit` and now feeds the `--emit-hls` workflow and golden tests.

## File map
- **Shared header:** `shared_kernel_params.rs` → `shared_kernel_params.h` (constants, typedefs, structs, prototypes).
- **Gather/Scatter kernels:** `graphyflow_big.rs`, `graphyflow_little.rs` build the main compute kernels.
- **Mergers:** `big_merger.rs`, `little_merger.rs` reduce parallel output streams.
- **Apply stage:** `apply_kernel.rs` merges big/little bursts and applies the relaxation rule.
- **Property server:** `hbm_writer.rs` serves property loads and flushes final outputs.

## Where to edit (G/A/S)
- **Gather:** request formation and property fetches live in `dist_req_packer`/`cacheline_req_sender` (big) and `request_manager` (little). Property-serving behavior sits in the loaders in `hbm_writer.rs`.
- **Apply:** per-lane relaxation is in `apply_func_loop` inside `apply_kernel.rs`. Change this for a different update rule; adjust `merge_big_little_writes` for merge policy.
- **Scatter:** update reduction/flush in `reduce_single_pe` + drains (big) or `reduce_unit` + drains (little). Merger behavior is in the per-lane reductions in `big_merger.rs` and `little_merger.rs`.

## Using and updating goldens
- Generated C++/headers live under `src/hls_assets/scripts/kernel/` and are treated as goldens.
- `cargo run -- --emit-hls sssp <out_dir>` regenerates the kernels from templates; copy the outputs back into `src/hls_assets/scripts/kernel/` when template logic changes.
- Golden tests in `src/domain/hls_template/mod.rs` whitespace-normalize and diff rendered output against those files. Keep the goldens in sync to keep tests green.

## Notes
- Kernels are fully expressed via the HLS DSL. Host code is still static under `src/hls_assets/scripts/host/` and not yet rendered from templates.
