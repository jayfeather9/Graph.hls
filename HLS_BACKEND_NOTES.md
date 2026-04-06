# HLS Backend Notes

The repository no longer carries standalone `.cpp`/`.h` template trees. Instead, every HLS source is described inside `src/domain/hls_template` using the builder primitives from `domain::hls`. This keeps the compiler in charge of materializing files on demand and opens the door for programmatic rewrites (scatter/gather/apply substitution, pragma injection, etc.).

## Current layout

- `src/domain/hls_template/mod.rs` – Error type plus helper exports. Each concrete file exposes a `*_unit()` function that yields an `HlsCompilationUnit`.
- `shared_kernel_params.rs` – Pure-Rust description of `shared_kernel_params.h`. All macros, type aliases, structs, and prototypes are modeled through `HlsDefine`, `HlsStruct`, `HlsFunctionPrototype`, etc., so the generator can selectively rewrite sections later on.
- `apply_kernel.rs` – Temporary bridge for `apply_kernel.cpp`. The full C++ text is embedded via `include_str!` and wrapped inside an `HlsStatement::Raw`. This removes the need for a template file immediately; the intent is to incrementally replace the raw block with proper structured statements as feature coverage in `domain::hls` grows.

The historical `templates/hls_sssp` folder (and the TOML manifest) has been deleted to enforce the “Rust-first” representation requirement.

## Consuming the templates

1. Call the appropriate `*_unit()` function to obtain an `HlsCompilationUnit`.
2. Emit the unit with `.to_code()` into the desired build directory.
3. For files that still rely on raw fallbacks, treat the rendered string as immutable until the structured rewrite lands.

## Integration roadmap

1. Extend the `domain::hls` DSL where necessary (method calls, header guards, extern prototypes, etc.). This work has already begun with support for method calls, break/continue statements, and struct attributes.
2. Port the remaining kernel sources (`graphyflow_big.cpp`, `graphyflow_little.cpp`, host drivers, …) into Rust modules that rely solely on structured statements—no raw fallbacks.
3. Reintroduce marker metadata (scatter/gather/apply hooks) once the structured descriptions cover those regions; at that point replacements can be expressed as AST rewrites rather than textual slicing.
4. Add unit tests that render each `HlsCompilationUnit` and diff against the authoritative sources under `generated_project_ori` to ensure deterministic output.
