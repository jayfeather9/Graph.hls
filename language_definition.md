Here is the formal specification of the **graphyflow DSL** in English Markdown format.

-----

# graphyflow Domain Specific Language Specification

**Version:** 1.0.0
**Status:** Draft

## 1\. Introduction

graphyflow is a declarative, strongly-typed Domain Specific Language (DSL) designed for high-performance graph processing. It abstracts the complexity of parallel graph algorithms using a **Vertex-Centric** or **Edge-Centric** iterative model (similar to BSP/Pregel).

A graphyflow program consists of two primary blocks:

1.  **Schema Definition**: Defines the properties associated with nodes and edges.
2.  **Computation Logic**: Defines a single iteration (superstep) of the algorithm using functional primitives (`map`, `reduce`, `filter`).

The runtime environment automatically iterates the Computation Logic until the graph state converges (i.e., no node properties are updated in an iteration).

-----

## 2\. Type System

graphyflow supports strict static typing. Types are categorized into Scalar and Composite types.

### 2.1 Scalar Types

| Type Name | Syntax | Description |
| :--- | :--- | :--- |
| **Integer** | `int<W>` | Signed integer with width $W$ bits (e.g., `int<32>`). |
| **Floating Point** | `float` | IEEE 754 single-precision (32-bit) floating point. |
| **Fixed Point** | `fixed<W, I>` | Fixed-point number with total width $W$ and integer width $I$. <br> *Value* = $Raw \times 2^{-(W-I)}$. |
| **Boolean** | `bool` | Logical value `true` or `false`. |

### 2.2 Composite Types

| Type Name | Syntax | Description |
| :--- | :--- | :--- |
| **Tuple** | `tuple<T1, T2, ...>` | Fixed-size, heterogeneously typed ordered sequence. |
| **Array** | `array<T>` | Variable-length, homogeneously typed sequence. |
| **Set** | `set<T>` | Unordered collection of unique elements of type `T`. |

-----

## 3\. Graph Data Model

The graph $G = (V, E)$ is a directed property graph defined in the `GraphSchema` block.

### 3.1 Schema Definition Syntax

```graphyflow
GraphSchema {
    Node: { <PropertyDefinition>* } | <Empty>
    Edge: { <PropertyDefinition>* } | <Empty>
}
```

### 3.2 Default Type Resolution

  * **Nodes**: If the `Node` block is empty, a Node object resolves to `int<32>` (representing the Node ID).
  * **Edges**: If the `Edge` block is empty, an Edge object resolves to `tuple<node, node>` (representing `{src, dst}`).

### 3.3 Semantic Constraints

1.  **Reserved Keywords**: It is strictly forbidden to define edge properties named `src` or `dst`.
2.  **Topology Access**:
      * For any Edge object `e`, `e.src` is an alias for `e.0` (the source node).
      * For any Edge object `e`, `e.dst` is an alias for `e.1` (the destination node).

-----

## 4\. Computation Model

The computation logic is defined within an implicit `while (!converged)` loop. The code block represents one **superstep**.

### 4.1 Input Selectors

Data is ingested into the pipeline using `iteration_input`.

  * `iteration_input(G.NODES)`: Returns a collection of all active nodes.
  * `iteration_input(G.EDGES)`: Returns a collection of all active edges.

### 4.2 Functional Primitives

#### 4.2.1 Map

Transforms input collections element-wise.

  * **Signature**: `map(inputs: [List<T1>, ...], func: Lambda) -> List<R>`
  * **Inputs**: A list of data arrays. All input arrays must have equal length.
  * **Logic**: Applies `func` to elements at the same index across all input arrays.
  * **Output**: A new array where $Output[i] = func(Input_1[i], Input_2[i], ...)$.

#### 4.2.2 Filter

Selects elements based on a predicate.

  * **Signature**: `filter(inputs: [List<T1>, ...], func: Lambda) -> [List<T1>, ...]`
  * **Logic**: Applies `func` to elements. If `func` returns `true`, the elements at that index are preserved; otherwise, they are discarded.
  * **Output**: A list of filtered arrays, preserving the alignment of inputs.

#### 4.2.3 Reduce

Aggregates values based on a key.

  * **Signature**: `reduce(key: List<int>, values: [List<T>], function: Lambda) -> List<R>`
  * **Key**: Must be of type `int` (typically Node ID).
  * **Behavior**:
    1.  Groups `values` by `key`.
    2.  Applies the binary aggregation `function` to reduce values with the same key.
    3.  Sorts the result by `key` in ascending order.
  * **Output**: A list of aggregated values corresponding to the distinct, sorted keys.

### 4.3 State Update (Return Statement)

The iteration concludes with a specific return syntax:
`return <Vector> as result_node_prop.<Property>`

  * The system maps the values in `<Vector>` to nodes based on the implicit order (from `reduce` or `map`).
  * **Convergence Check**: The runtime compares the returned values against the current graph state. If values change, another iteration is triggered.

-----

## 5\. Expression Syntax (Lambda)

Lambda functions are side-effect-free expressions using C-style syntax.

### 5.1 Operators & Precedence

| Priority | Operator | Description | Associativity |
| :--- | :--- | :--- | :--- |
| 1 | `.` | Member access / Tuple index | Left |
| 2 | `!`, `~`, `(type)` | Logical NOT, Bitwise NOT, Cast | Right |
| 3 | `*`, `/` | Multiply, Divide | Left |
| 4 | `+`, `-` | Add, Subtract | Left |
| 5 | `<`, `<=`, `>`, `>=` | Relational operators | Left |
| 6 | `==`, `!=` | Equality check | Left |
| 7 | `&` | Bitwise/Logical AND | Left |
| 8 | `|` | Bitwise/Logical OR | Left |
| 9 | `? :` | Ternary conditional | Right |

### 5.2 Access Rules

  * **Struct Property**: `object.property`
  * **Tuple Element**: `tuple.N` (where N is an integer literal, e.g., `e.0`).
  * **Global State Access**: Within a lambda, accessing a node (e.g., `e.src`) allows implicit access to that node's current properties (e.g., `e.src.dist`).

-----

## 6\. Formal Grammar (EBNF)

```ebnf
Program         ::= SchemaBlock AlgoBlock

/* Schema Definition */
SchemaBlock     ::= "{" SchemaContent "}"
SchemaContent   ::= (NodeDef EdgeDef) | (EdgeDef NodeDef)
NodeDef         ::= "Node" ":" "{" PropertyList "}"
EdgeDef         ::= "Edge" ":" "{" PropertyList "}"
PropertyList    ::= (Identifier ":" TypeDef)* TypeDef         ::= "int" "<" Integer ">"
                  | "float"
                  | "fixed" "<" Integer "," Integer ">"
                  | "bool"
                  | "set" "<" TypeDef ">"
                  | "tuple" "<" TypeDef ("," TypeDef)* ">"
                  | "array" "<" TypeDef ">"

/* Algorithm Definition */
AlgoBlock       ::= "{" Statement* ReturnStmt "}"
Statement       ::= Identifier "=" Operation
Operation       ::= "iteration_input" "(" Selector ")"
                  | "map" "(" "[" ArgList "]" "," Lambda ")"
                  | "filter" "(" "[" ArgList "]" "," Lambda ")"
                  | "reduce" "(" "key" "=" Identifier "," "values" "=" "[" ArgList "]" "," "function" "=" Lambda ")"

Selector        ::= "G.NODES" | "G.EDGES"
ArgList         ::= Identifier ("," Identifier)*
ReturnStmt      ::= "return" Identifier "as" "result_node_prop" "." Identifier

/* Expressions */
Lambda          ::= "lambda" ParamList ":" Expr
Expr            ::= Term | Expr BinOp Term | UnaryOp Expr | Expr "?" Expr ":" Expr
Term            ::= Identifier | Literal | Term "." Identifier | Term "." Integer
BinOp           ::= "+" | "-" | "*" | "/" | "==" | "!=" | ">" | "<" | ">=" | "<=" | "&" | "|"
UnaryOp         ::= "!" | "~"
```

-----

## 7\. Example: Single Source Shortest Path (SSSP)

```graphyflow
// Definition of Graph Properties
{
    Node: {
        dist: int<32>
    }
    Edge: {
        weight: int<32>
    }
}

// Computation Logic for One Iteration
{
    // 1. Load all edges in the graph
    edges = iteration_input(G.EDGES)

    // 2. Map: Extract destination Node IDs (to be used as keys later)
    dst_ids = map([edges], lambda e: e.dst)

    // 3. Map: Calculate potential new distance via this edge
    // Accesses implicit global state: e.src.dist
    updates = map([edges], lambda e: e.src.dist + e.weight)

    // 4. Reduce: Find the minimum distance for each destination node
    // Groups 'updates' by 'dst_ids'. 
    // If multiple edges point to the same node, keep the smallest path.
    min_dists = reduce(
        key=dst_ids, 
        values=[updates], 
        function=lambda x, y: x > y ? y : x
    )

    // 5. Update: Apply the calculated minimums to the 'dist' property
    return min_dists as result_node_prop.dist
}
```
