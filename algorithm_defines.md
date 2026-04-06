# Algorithms

## Simple Algorithms

### SSSP

For GAS version of Single Source Shortest Path (SSSP) algorithm, we implement Bellman-Ford algorithm. The algorithm works by iteratively relaxing the edges of the graph. In each iteration, each vertex updates its distance based on the distances of its neighboring vertices.

### PageRank

For GAS version of PageRank algorithm, we implement the iterative method to compute the PageRank values of vertices in a graph. Each vertex distributes its PageRank value to its neighbors, and in each iteration, the PageRank values are updated based on the contributions from neighboring vertices.

The PageRank needs to get node's out-degree, so we have a preprocessing step to calculate and store the out-degree of each vertex before running the main PageRank iterations.

### Connected Components

For GAS version of Connected Components algorithm, we implement a label propagation method. Each vertex starts with a unique label (its own ID), and in each iteration, it updates its label to the minimum label among its neighbors. This process continues until no more label changes occur, resulting in each connected component having a unique label.

## Complex Algorithms

### Graph Coloring

**Concept:** Assign an integer color to every node such that no two adjacent nodes share the same color, minimizing the maximum color value.

#### GAS Implementation

- **Scatter:** Send pair `{src.color, src.priority}` to neighbors.
- **Gather:**
  - *Logic:* Collect all incoming colors into a `Set<int>`.
  - *Constraint Fix:* Since G cannot see `dst.color` to filter conflicts immediately, it must aggregate **all** neighbor colors into a variable-length list/set (allowed by Capability #2).
- **Apply:**
  - Check `dst.color` against the incoming set.
  - If conflict exists (and `src.priority > dst.priority`), select the smallest integer **not** in the set (MEX - Minimum Excluded value) and update `dst.color`.

### Alternating Least Squares (ALS)

**Concept:** Collaborative filtering algorithm. Uses a user-item graph to predict missing ratings by solving linear equations for latent vectors ($Ax=b$).

#### GAS Implementation

- **Scatter:**
  - Compute Outer Product: `mat_part = src.vec @ src.vec`.
  - Compute Vector Part: `vec_part = mat_part * edge.rating`.
  - Send `{mat_part, vec_part}`.
- **Gather:** Sum all `mat_part` matrices and `vec_part` vectors element-wise.
- **Apply:**
  - Construct equation $Ax=b$ where $A = \sum mat\_part + \lambda I$ and $b = \sum vec\_part$.
  - Solve for $x$ and update `dst.vector`.
