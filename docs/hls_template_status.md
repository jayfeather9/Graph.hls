# HLS Template Status

Current coverage of the Rust-driven HLS templates and what’s left to do.

## What’s rendered today

| File | Status | Notes |
| --- | --- | --- |
| `shared_kernel_params.h` | ✅ Complete | Constants, typedefs, structs, prototypes. |
| `apply_kernel.cpp` | ✅ Complete | Merge big/little bursts and apply min-relaxation; no raw blocks. |
| `big_merger.cpp` | ✅ Complete | Four-way reducer; fully structured. |
| `little_merger.cpp` | ✅ Complete | Ten-way reducer; fully structured. |
| `hbm_writer.cpp` | ✅ Structured | Property loaders/packers and top function expressed in the DSL. |
| `graphyflow_big.cpp` | ✅ Structured | Helper stages and top kernel expressed in the DSL (no raw blocks). |
| `graphyflow_little.cpp` | ✅ Structured | Request manager, reducers, drains expressed in DSL. |
| Host sources | 🚧 Not templated | Host code is copied from `src/hls_assets`; no Rust templates yet. |

## Test coverage
- Golden comparisons (whitespace-normalized) now run for: `apply_kernel`, `big_merger`, `little_merger`, `hbm_writer`, `graphyflow_big`, `graphyflow_little`, and `shared_kernel_params` indirectly via their render tests.
- Goldens live under `src/hls_assets/scripts/kernel/` and are refreshed by running `cargo run -- --emit-hls <app> <out_dir>` and copying the outputs back into `src/hls_assets`.

## TODOs
1. **Eliminate raw blocks**
   - Extend `domain::hls` to cover casts/constructs still expressed via `HlsStatement::Raw`, then clean up `hbm_writer` and `graphyflow_big`.
2. **Host-side templating**
   - Port host sources (`graph_loader.cpp`, `fpga_executor.cpp/h`, `generated_host.cpp/h`, `host.cpp`, etc.) into Rust templates and wire them into `emit_hls_project`.
   - Add goldens/tests for host files once templated.
3. **Broader validation**
   - Keep golden tests in sync when templates change; consider lightweight normalization for pragmas if needed.
   - Add an end-to-end smoke test that runs `--emit-hls` into a temp dir and asserts expected file presence (already partially covered by `builds_sssp_hls_project`).

Track these items as issues or PR checklists to keep the template surface from regressing.
