# GraphyFlow DSL Writing Guide (Latest Syntax)

This guide is for writing DSL programs.
It focuses on the current, preferred language form.

## 1. Program Shape

Write your program using this block order:

1. `GraphConfig` (required)
2. `HlsConfig` (optional)
3. `HierarchicalParam` (optional)
4. `Iteration` (required)

If both optional blocks are used, keep `HlsConfig` before `HierarchicalParam`.

## 2. Minimal Template

```graphyflow
GraphConfig {
 Node:{ value:float }
 Edge:{}
}

Iteration {
 edges=iteration_input(G.EDGES)
 dst=map([edges], lambda e: e.dst)
 vals=map([edges], lambda e: e.src.value)
 agg=reduce(key=dst, values=[vals], function=lambda x,y: x+y)
 return agg as result_node_prop.value
}
```

## 3. `GraphConfig` (Schema)

Use `GraphConfig` to define node and edge properties.

```graphyflow
GraphConfig {
 Node:{ rank:float out_deg:int<32> }
 Edge:{ weight:int<32> }
}
```

Rules:

- `Node` and `Edge` are entries inside `GraphConfig`.
- `Node:{}` or `Edge:{}` is valid.
- Edge property names `src` and `dst` are reserved; do not define them.
- No semicolons are needed.

## 4. Supported Types

Scalar types:

- `int<width>`
- `float`
- `fixed<width, int_width>`
- `bool`

Collection/compound types:

- `set<T>`
- `tuple<T1, T2, ...>`
- `array<T>`
- `vector<T, len>`
- `matrix<T, rows, cols>`

## 5. `Iteration` Block

`Iteration` contains assignment statements plus one final return statement.

Statement form:

`name = operation(...)`

Final line must be:

`return <binding> as result_node_prop.<node_property>`

Example:

```graphyflow
Iteration {
 edges=iteration_input(G.EDGES)
 dst=map([edges], lambda e: e.dst)
 vals=map([edges], lambda e: e.src.dist+e.weight)
 best=reduce(key=dst, values=[vals], function=lambda x,y: x>y?y:x)
 return best as result_node_prop.dist
}
```

## 6. Operations

### `iteration_input`

```graphyflow
edges=iteration_input(G.EDGES)
nodes=iteration_input(G.NODES)
```

### `map`

```graphyflow
out=map([in1,in2], lambda a,b: <expr>)
```

### `filter`

```graphyflow
out=filter([in1,in2], lambda a,b: <bool_expr>)
```

### `reduce`

```graphyflow
out=reduce(key=keys, values=[vals], function=lambda x,y: <expr>)
```

Recommended: use one values stream (`values=[vals]`) for compatibility with the standard compile/simulate flow.

## 7. Lambda Expressions

Supported expression pieces:

- identifiers
- int / float / bool literals
- member access (`a.b`)
- index access (`a.0`)
- function call (`fn(x,y)`)
- unary: `!`, `~`
- binary: `+ - * / < <= > >= == != & |`
- ternary: `cond ? t : f`

Important: unary minus is not supported directly.

- Not valid: `-1`
- Valid alternative: `0-1`

## 8. Common Property Access Patterns

Inside edge-based lambdas:

- `e.src` and `e.dst` are source/destination nodes
- `e.src.<node_prop>` reads source node property
- `e.dst.<node_prop>` reads destination node property
- `e.<edge_prop>` reads edge property

Inside apply map (after reduce):

- `self.<node_prop>` reads current node's old property

## 9. Built-in Functions

Available function names (usable in lambda calls):

- `make_set(x)`
- `set_union(a,b)`
- `mex(s)`
- `pair(a,b)`
- `outer_product(v1,v2)`
- `vector_scale(v,s)`
- `vector_add(v1,v2)`
- `matrix_add(m1,m2)`
- `solve_linear(m,b)`

## 10. `HierarchicalParam` (Frontend Parameters)

Use this block to carry hierarchical frontend parameters.

```graphyflow
HierarchicalParam {
 L1:{ URAM_SIZE:262144 }
 L3:{ pipe_partition:[{type:big, number:2},{type:little, number:3},{type:little, number:3}] }
}
```

Notes:

- Comma-separated object fields are supported (for example `{type:little, number:3}`).
- This block is accepted by the compiler front-end.

## 11. `HlsConfig` (Hardware Topology)

Use this only when you want explicit hardware topology control.

```graphyflow
HlsConfig {
 topology:{
  apply_slr:1
  hbm_writer_slr:0
  cross_slr_fifo_depth:16
  little_groups:[
   {pipelines:6, merger_slr:0, pipeline_slr:[0,0,0,0,1,1]}
  ]
  big_groups:[
   {pipelines:2, merger_slr:2, pipeline_slr:[2,2]}
  ]
 }
}
```

Topology keys:

- `apply_slr`
- `hbm_writer_slr`
- `cross_slr_fifo_depth`
- `little_groups`
- `big_groups`

Group keys:

- `pipelines`
- `merger_slr`
- `pipeline_slr`

## 12. Style Rules That Keep Programs Working

- Use `GraphConfig` and `Iteration` explicitly.
- Keep exactly one final `return` at the end of `Iteration`.
- Define and use bindings in order (no forward references).
- For graph algorithms, prefer the canonical flow:
  - edge input
  - map key/value
  - reduce
  - optional apply map
  - return
- Keep reduce key as destination node IDs (`e.dst` or equivalent).

## 13. Full PageRank Example (Latest Style)

```graphyflow
GraphConfig {
 Node:{ rank:float out_deg:int<32> }
 Edge:{}
}

HierarchicalParam {
 L1:{ URAM_SIZE:262144 }
 L3:{ pipe_partition:[{type:big, number:2},{type:little, number:3},{type:little, number:3}] }
}

Iteration {
 edges=iteration_input(G.EDGES)
 dst_ids=map([edges], lambda e: e.dst)
 contribs=map([edges], lambda e: e.src.rank)
 summed=reduce(key=dst_ids, values=[contribs], function=lambda x,y: x+y)
 new_rank=map([summed], lambda r: 0.15+0.85*(r/self.out_deg))
 return new_rank as result_node_prop.rank
}
```
