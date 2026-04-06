#ifndef __ALGORITHM_CONFIG_H__
#define __ALGORITHM_CONFIG_H__

#include <string>

enum class NumericKind {
    Fixed,
    Float,
    Int,
};

enum class AlgorithmKind {
    Sssp,
    ConnectedComponents,
    Pagerank,
    Bfs,
    ArticleRank,
    Wcc,
};

enum class ConvergenceMode {
    MinImprove,     // classic SSSP: stop when no value improves
    EqualityStable, // stop when value stops changing (e.g., CC labels)
    DeltaThreshold, // stop when delta < threshold
    FixedIterations, // run fixed number of iterations
    NewlyDiscoveredZero // BFS: stop when no new vertices discovered
};

enum class UpdateMode {
    Min,
    Max,
    Overwrite,
};

struct AlgorithmConfig {
    std::string target_property;   // name used for logging/output
    NumericKind numeric_kind;
    int bitwidth;
    int int_width;                 // for fixed-point; ignored otherwise
    ConvergenceMode convergence_mode;
    float delta_threshold;         // used when mode == DeltaThreshold
    int max_iterations;            // used when mode == FixedIterations
    bool needs_edge_weight;        // whether host must supply edge weights
    bool needs_out_degree;         // whether apply needs per-node out-degree
    UpdateMode update_mode;        // host merge/update policy for results
    unsigned int active_mask;      // BFS packed-prop active bit (0 if unused)
    unsigned int inf_value;        // BFS packed-prop INF value (0 if unused)
    AlgorithmKind algorithm_kind;
};

#endif // __ALGORITHM_CONFIG_H__
