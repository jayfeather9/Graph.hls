# Graphyflow Architecture

This crate implements a toy Graph Analytics DSL that lowers programs into a three-stage GAS (Gather–Apply–Scatter) representation and can interpret the resulting kernels directly. The codebase is structured as a mini compiler + runtime with the following high-level pipeline:

1. **Parsing (`services::parser`)** – The DSL is tokenized and parsed (via `chumsky`) into a strongly typed AST (`domain::ast`).
2. **IR construction (`engine::ir_builder`)** – AST statements are converted into an `OperationGraph`, a normalized SSA-like dataflow graph.
3. **GAS lowering (`engine::gas_lower`)** – The operation graph is validated and mapped into explicit Gather/Apply/Scatter stages plus inferred `GasType`s.
4. **Execution (`engine::gas_simulator`)** – A small interpreter executes GAS programs over concrete graphs; reference graph generators and calculators live under `utils`.
5. **CLI (`src/main.rs`)** – Binds everything: parse + lower DSL sources, optionally simulate the resulting GAS program or synthesize random graph inputs.

The sections below walk through each area in more detail.

---

## Workspace Layout

```
src/
  domain/        # Core data types (AST, IR, GAS model, HLS utilities, shared errors)
  services/      # DSL parsing service built on chumsky
  engine/        # Compiler + runtime stages (IR builder, GAS lowering, GAS simulator)
  utils/         # Support code (graph generation, indent helper, reference calculators)
  main.rs        # CLI entrypoint, exposes generation & simulation commands
tests/           # Cross-app integration tests that drive the full pipeline
apps/            # Sample DSL programs plus JSON graphs for testing & demos
```

---

## Domain Layer (`src/domain`)

| Module | Responsibility | Key Types |
| --- | --- | --- |
| `ast.rs` | Defines every syntax element produced by the parser (schema block, algorithm statements, expressions, selectors, type expressions). Implements `DebugSummary` renderings for quick inspection. | `Program`, `SchemaBlock`, `Operation`, `Expr`, `TypeExpr`, `Identifier` |
| `errors.rs` | User-facing error enums plus span helpers shared by parser/IR builder. | `GraphyflowError`, `ParseError`, `IrError`, `Span` |
| `ir.rs` | Intermediate dataflow graph used between AST and GAS to ease validation/reasoning. Includes IR-specific expressions (`IrExpr`) and lambdas. | `OperationGraph`, `OperationNode`, `OperationStage`, `IrLambda`, `ResultBinding` |
| `gas.rs` | Final GAS data model describing the three stages and their inferred types (`GasType`). Also houses the `DebugSummary` used in CLI output. | `GasProgram`, `GasScatterStage`, `GasGatherStage`, `GasApplyStage`, `GasType` |
| `hls.rs` | A self-contained C++/HLS codegen toolbox (validated identifiers, type renderers, statements, functions). Currently unused by the binary but provides the building blocks for future hardware emitters. | `HlsType`, `HlsExpr`, `HlsStatement`, `HlsFunction`, `HlsCompilationUnit` |

Every module intentionally avoids allocation-heavy APIs and favors plain structs/enums so they can be cloned between stages without unsafe code.

---

## Services Layer (`src/services/parser.rs`)

The parser wraps two phases:

1. **Lexer** – Uses `chumsky` primitives to emit `(Token, Span)` pairs. Keywords (e.g., `map`, `reduce`, `lambda`, `int`, `vector`) are normalized into a small `Keyword` enum, while identifiers fall back to `Token::Ident`. Comments (`//…`) and whitespace are skipped by `skip_ws_or_comment`.
2. **Token Parser** – Stateless builder that recognizes the Graphyflow DSL grammar:
   - Schema block of `{ Node: { ... } Edge: { ... } }` with typed properties.
   - Algorithm block containing `iteration_input`, `map`, `filter`, `reduce` statements assigned to bindings, followed by `return <binding> as result_node_prop.<prop>`.
   - Expression grammar covering identifiers, literals, calls, member access, unary/binary ops, ternaries, and tuple/array indices.

Error handling leverages `SimpleReason` to attach spans, producing `ParseError::WithSpan` for IDE-friendly diagnostics.

---

## Engine Layer

### IR Builder (`src/engine/ir_builder.rs`)

`LoweredProgram::parse_and_lower` is the convenience entry used by the CLI/tests. The IR builder:

- Walks algorithm statements in source order.
- Checks binding availability with a `HashSet`, returning `IrError::UnknownBinding` or `IrError::UnknownReduceKey` when dependencies are missing.
- Converts AST lambdas into `IrLambda`/`IrExpr` via `From` impls.
- Produces an `OperationGraph` with an ordered vec of `OperationNode`s (each keeps its output binding list) and a trailing `ResultBinding`.

### GAS Lowering (`src/engine/gas_lower.rs`)

Transforms an `OperationGraph` into explicit Scatter/Gather/Apply stages, enforcing GAS-friendly invariants:

- Exactly one reduce node (`locate_reduce`), and the scatter side must start with `iteration_input(G.EDGES)`.
- Tracks binding types through `TypeEnv` seeded from schema definitions and per-node/edge property maps.
- Collects lambda definitions for scatter map outputs to ensure key/value streams are adjacent and only depend on edge data (`enforce_scatter_rules`).
- Guards against property reads in gather lambdas and enforces apply contiguity with the reduce output.
- Performs lightweight type inference across `IrExpr` trees to annotate `GasType`s (ints/floats/fixed, tuples, sets, vectors/matrices, node/edge records, etc.).

Errors such as `ScatterReadsDstProperties` or `ApplyBindingMismatch` bubble up with context, helping DSL authors understand why lowering failed.

### GAS Simulator (`src/engine/gas_simulator.rs`)

A pure-Rust interpreter capable of running lowered GAS kernels:

- Represents runtime values with the `Value` enum (ints/floats/bools, tuples/arrays, sets, vectors/matrices, node/edge refs, etc.).
- `simulate_gas` loops until convergence or `max_iters`, executing scatter, gather, and apply phases in sequence.
- Includes a small standard library of supported lambda calls (`make_set`, `set_union`, `vector_add`, `solve_linear`, …) plus JSON loaders that coerce inputs according to inferred `GasType`s.
- Provides helpers (`GraphState`, `GraphInput`, `NodeRecord`, `EdgeRecord`) plus JSON converters consumed by the CLI and tests.

---

## Utilities (`src/utils`)

- `graph_generator.rs` – Generates random graphs tailored to the built-in demo apps. Uses deterministic seeding, enforces edge-count limits, and populates node/edge properties (e.g., `dist`, `rank`, `out_deg`, ALS vectors) suitable for feeding into the simulator.
- `reference_calcs.rs` – Deterministic CPU baselines for each app (SSSP relaxation, PageRank power iteration, Union-Find for connected components, greedy graph coloring, ALS vector passthrough). Used to validate simulator output.
- `indent_block` helper – Tiny function + unit test for indenting multiline strings, used by documentation/rendering routines.

---

## CLI (`src/main.rs`)

The binary exposes three modes:

1. `refactor_Graphyflow <dsl-file|app-name>` – Parse + lower + GAS-lower the program and print debug summaries of every stage.
2. `refactor_Graphyflow --generate <app> <nodes> <edges> [seed]` – Produce a randomly populated graph JSON for a known app kind.
3. `refactor_Graphyflow --simulate-json <app|dsl-path> <graph.json> [max_iters]` – Load a DSL program and a graph JSON, run the GAS simulator, optionally compare against reference calculations, and emit the computed property values as JSON.

Support functions resolve DSL paths (either explicit path or lookup under `apps/`), pretty-print results (`value_to_json`), and perform reference comparisons when available.

---

## Assets & Tests

- **Sample DSL apps (`apps/*.dsl`)** – SSSP, PageRank, Connected Components, Graph Coloring, ALS. They serve as fixtures for regression tests and manual experimentation.
- **Graph fixtures (`apps/test_graphs/*`)** – Deterministic JSON graphs used by integration tests.
- **Integration tests (`tests/gas_apps.rs`)** – End-to-end coverage: parse → IR → GAS → simulate, then assert simulator output matches `reference_calcs`.
- **Unit tests** – Spread throughout modules (parser lexing logic, AST helpers, IR conversions, GAS type inference, simulator arithmetic, utils). Running `cargo test` executes all of them.

---

## Known Limitations & Refinement Opportunities

1. **Single-reduce restriction** – `gas_lower` currently accepts only one reduce node and enforces hard-coded scatter/gather shapes. Extending to multi-phase GAS or supporting node iteration would require reworking `locate_reduce`, contiguity checks, and type propagation.
2. **Type inference coverage** – `infer_call_type` supports a limited intrinsic set. Adding more DSL intrinsics or richer types (maps, structs) will need systematic handling plus parser validation.
3. **Simulator convergence** – `simulate_gas` stops when no property changes, but some algorithms (e.g., Pagerank) might need tolerance-based convergence and damping factors per stage rather than fixed iterations.
4. **HLS backend integration** – The comprehensive `domain::hls` module is unused. Wiring GAS outputs into HLS code generation (or pruning dead code) would clarify intent and reduce maintenance.
5. **Error surfacing** – Many errors bubble up with `.to_string()` contexts in the CLI. A richer diagnostic layer (spans from AST through IR) would help IDEs pinpoint issues in source files.
6. **Performance** – Interpreter + graph generator clone data frequently (e.g., `Value::EdgeRef` per scatter iteration). For larger graphs, consider arena allocations or borrowed data to avoid repeated clones.
7. **Configuration/testing UX** – Tests rely on fixture files in `apps/test_graphs`, which can fail silently if a new app is added without a fixture. A procedural macro or build script could ensure coverage stays in sync with available DSL programs.

These items can guide future refactors depending on whether the goal is richer DSL expressiveness, better runtime performance, or integration with downstream code generators.
