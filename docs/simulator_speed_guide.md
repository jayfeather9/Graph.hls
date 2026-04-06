# Simulator Speed Guide

This guide documents the current simulator speed-oriented execution paths in
`src/engine/gas_simulator.rs` and the behaviors that were not covered by the
existing repo docs.

## Scope

The simulator now has three execution modes:

1. Specialized kernel fast path
2. Generic compiled plan
3. Interpreted fallback

The public entrypoint is `simulate_gas(program, graph, max_iters)`.

`simulate_gas()` tries the compiled stack first. It only falls back to the
interpreter when compilation returns `GasSimError::UnsupportedOp`. Runtime
errors such as missing properties, missing nodes, type mismatches, or
non-convergence are returned directly.

## Dispatch Order

`simulate_gas_compiled()` now uses this order:

1. Try to recognize a built-in specialized kernel from the lowered GAS program.
2. If specialization matches, run the specialized kernel directly on
   `GraphState`.
3. Otherwise, build a generic `CompiledPlan` and execute it on `DenseGraph`.

This matters because the specialized path now avoids the dense graph materialize
step completely.

## Generic Compiled Plan

The generic compiled path is the default accelerator for non-specialized
programs.

### What it does

- Converts `GraphState` into a `DenseGraph`.
- Sorts node IDs once and builds an `id_to_idx` map.
- Stores node properties in columnar vectors (`HashMap<String, Vec<Value>>`).
- Stores edge properties in columnar vectors as well.
- Runs one compiled scatter/gather/apply loop per iteration.

### Why it is faster than the interpreter

- Lambda bodies are compiled into `CompiledExpr` trees once instead of being
  re-walked as IR every iteration.
- Common reducers are recognized and replaced with direct fast reducers:
  `sum`, `min`, and `max`.
- The scatter key `e.dst` is recognized and bypasses general lambda evaluation.
- Node update comparison uses `Value::approx_eq()` directly on the compiled
  result vectors.

### Important behavior details

- Generic compiled execution already supports arbitrary node IDs because
  `DenseGraph` remaps external node IDs to dense indices.
- Reduce keys must still resolve to a node ID (`Value::Int` or `Value::NodeRef`).
- If the target node property does not exist yet, the compiled path allocates a
  `Value::Unit` column and writes the new property into it.

## Specialized Kernel Fast Path

Three applications have exact-pattern specialized kernels today:

- PageRank
- SSSP
- Connected Components

These are recognized from the lowered GAS program, not from the DSL filename.

### Why this path is faster

- It skips `DenseGraph::from_state()`.
- It skips generic compiled lambda evaluation entirely.
- It reads node properties into typed `Vec<f64>` or `Vec<i64>` once.
- It runs tight per-edge loops with direct arithmetic and direct convergence
  checks.

### What is newly supported

The specialized path now supports sparse or non-zero-based node IDs through
`NodeIndexer`.

`NodeIndexer` has two modes:

- `Dense`: used when node IDs are exactly `0..N-1`
- `Sparse`: used for every other node ID layout

Before this change, the specialized path depended on dense materialization.
Now it can operate directly on `GraphState` while still handling sparse node ID
layouts.

### Exact specialization rules

The matcher is intentionally strict.

#### PageRank

Recognized when:

- scatter key is destination node (`e.dst` or `e.dst.id`)
- gather reducer is `sum`
- scatter value is a source-node property
- apply lambda matches `base + scale * (gathered / self.<prop>)`

The source property used in scatter must be the same property written in apply.

#### SSSP

Recognized when:

- scatter key is destination node
- gather reducer is `min`
- scatter value is exactly `e.src.<target> + e.<edge_prop>`
- apply lambda is the standard min-with-self ternary form

The SSSP matcher does not accept arbitrary equivalent rewrites. It is matching
structure, not semantic equivalence.

#### Connected Components

Recognized when:

- scatter key is destination node
- gather reducer is `min`
- scatter value is `e.src.<target>`
- there is no apply lambda

## Uniform Edge Property Compression

The JSON loader now detects edge properties that have the same value on every
edge and stores them once in `GraphState.edge_uniform_props`.

### How detection works

- The first edge is used as the baseline value for each declared edge property.
- Remaining edges are compared against that baseline with `Value::approx_eq()`.
- If every edge matches, the property is stored once in
  `edge_uniform_props`.
- If any edge differs, the property stays in per-edge storage.

### Why this helps

- Uniform edge properties do not need to be duplicated into every `EdgeState`.
- The specialized SSSP kernel can read a uniform weight once and avoid per-edge
  property lookup entirely.
- The interpreted evaluator can still resolve `e.<prop>` because member access
  now checks both `edge.props` and `state.edge_uniform_props`.
- The generic compiled path seeds dense edge columns from `edge_uniform_props`
  before adding non-uniform per-edge values.

### Default-value interaction

`load_graph_from_json()` still coerces missing JSON properties to type defaults:

- `int` -> `0`
- `float` / `fixed` -> `0.0`
- `bool` -> `false`
- collection types -> empty or zero-filled values

That means a missing edge property and an explicit default-valued property are
treated the same during uniform-property detection.

### Storage caveat

Uniform-property inference only happens in `load_graph_from_json()`.

If some other caller constructs `GraphState` directly, it must populate either:

- per-edge `EdgeState.props`, or
- `GraphState.edge_uniform_props`

No later pass infers uniform properties automatically.

## Convergence and Writeback

All simulator modes still stop when:

- an iteration produces no observable change, or
- `max_iters` is reached

If convergence is not reached, the simulator returns `GasSimError::NoConvergence`.

Writeback behavior differs slightly by path:

- Specialized kernels clone the original `GraphState` and update only the target
  node property, so uniform edge-property compaction is preserved.
- The generic compiled path materializes edge columns back into per-edge props in
  `DenseGraph::into_state()`, so the returned `GraphState` no longer carries
  `edge_uniform_props`; edge props are expanded back out.

## Error Behavior

Current error handling that was previously undocumented:

- Compiled-to-interpreted fallback only happens on `UnsupportedOp`.
- Missing edge properties in interpreted member access are resolved from
  `edge_uniform_props` before reporting `MissingEdgeProp`.
- Type mismatches in specialized kernels are reported as `TypeMismatch`, not as
  matcher failure.

Current fast-path assumption:

- The specialized dense branch assumes edge endpoints are valid dense indices.
  If a caller constructs a malformed dense-ID `GraphState`, that branch may
  panic instead of returning a structured simulator error.

## What Still Does Not Get Accelerated

These cases still use the generic compiled plan or the interpreter:

- DSL programs that do not match one of the exact built-in patterns
- Reducers other than recognized `sum` / `min` / `max`
- Apply lambdas that are semantically equivalent to PageRank or SSSP but written
  in a different IR shape
- Operations that cannot be compiled into `CompiledExpr`

The generic compiled path is already much faster than the interpreter for many
cases, but it still pays the dense graph conversion cost.

## CLI Behavior

`refactor_Graphyflow --simulate-json <app|dsl> <graph.json> [max_iters]` uses the
same `simulate_gas()` entrypoint described above.

For known built-in apps, the CLI also computes reference results and prints
whether the simulator output matches the reference implementation.

## Validation in This Repo

Current tests that cover the speed-update paths:

- `compiled_matches_interpreted_on_builtin_apps`
- `detects_specialized_kernels_for_core_apps`
- `specialized_dense_fast_path_reports_missing_node_instead_of_panicking`
- integration tests in `tests/gas_apps.rs`

These validate correctness of the optimized paths, but they are not benchmark
tests and do not record timing numbers.

## Practical Reading Order

If you want to inspect the implementation quickly, read these sections in order:

1. `simulate_gas()`
2. `simulate_gas_compiled()`
3. `SpecializedKernel::from_program()`
4. `simulate_*_on_state()` specialized kernels
5. `CompiledPlan::from_program()`
6. `execute_iteration_compiled()`
7. `load_graph_from_json()`
