#include "host_verifier.h"
#include "host_bellman_ford.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>

namespace {

std::vector<unsigned int> verify_sssp(const GraphCSR &graph, int start_node,
                                      const AlgorithmConfig &config) {
    const unsigned int ref_inf =
        (!DISTANCE_SIGNED && DISTANCE_BITWIDTH == 8) ? 126u
                                                     : static_cast<unsigned int>(INFINITY_POD);
    std::vector<unsigned int> distances(graph.num_vertices, ref_inf);
    if (start_node >= 0 && start_node < graph.num_vertices) {
        distances[start_node] = 0;
    }

    auto sssp_iteration = [&](std::vector<unsigned int> &state) {
        bool changed = false;
        std::vector<unsigned int> next_state = state;

        for (int u = 0; u < graph.num_vertices; ++u) {
            if (state[u] == ref_inf) {
                continue;
            }
            for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
                int v = graph.columns[i];
                if ((v & 0x40000000) != 0) {
                    continue;
                }
                const unsigned int weight =
                    config.needs_edge_weight
                        ? static_cast<unsigned int>(graph.weights[i])
                        : 1u;
                const uint64_t candidate =
                    static_cast<uint64_t>(state[u]) + static_cast<uint64_t>(weight);
                if (candidate < static_cast<uint64_t>(next_state[v]) &&
                    candidate < static_cast<uint64_t>(ref_inf)) {
                    next_state[v] = static_cast<unsigned int>(candidate);
                    changed = true;
                }
            }
        }

        state.swap(next_state);

        return changed;
    };

    int max_iterations = graph.num_vertices;
    if (config.convergence_mode == ConvergenceMode::FixedIterations &&
        config.max_iterations > 0) {
        max_iterations = config.max_iterations;
    }
    int iter = 0;

    std::cout << "\nStarting Host verification..." << std::endl;

    if (config.convergence_mode == ConvergenceMode::FixedIterations) {
        for (iter = 0; iter < max_iterations; ++iter) {
            (void)sssp_iteration(distances);
        }
        std::cout << "Host computation ran for " << iter
                  << " iterations (fixed)." << std::endl;
    } else {
        bool changed = true;
        while (changed && iter < max_iterations) {
            changed = sssp_iteration(distances);
            iter++;
        }
        std::cout << "Host computation converged after " << iter
                  << " iterations." << std::endl;
    }

    if (config.convergence_mode != ConvergenceMode::FixedIterations &&
        iter == max_iterations) {
        auto probe = distances;
        if (sssp_iteration(probe)) {
            std::cout
                << "Warning: Negative weight cycle detected by host verifier."
                << std::endl;
        }
    }

    return distances;
}

bool cc_min_iteration(const GraphCSR &graph,
                      const std::vector<ap_fixed_pod_t> &labels_in,
                      std::vector<ap_fixed_pod_t> &labels_out) {
    labels_out = labels_in;

    for (int u = 0; u < graph.num_vertices; ++u) {
        ap_fixed_pod_t src_label = labels_in[u];
        for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
            int v = graph.columns[i];
            if ((v & 0x40000000) != 0) {
                continue;
            }
            if (src_label < labels_out[v]) {
                labels_out[v] = src_label;
            }
        }
    }

    return labels_in != labels_out;
}

std::vector<unsigned int>
verify_connected_components(const GraphCSR &graph, const AlgorithmConfig &config) {
    std::vector<ap_fixed_pod_t> labels_k(graph.num_vertices, 0);
    std::vector<ap_fixed_pod_t> labels_kplus1(graph.num_vertices, 0);

    if (DISTANCE_BITWIDTH < 32) {
        const uint64_t max_label = DISTANCE_SIGNED
                                       ? ((uint64_t(1) << (DISTANCE_BITWIDTH - 1)) - 1)
                                       : ((uint64_t(1) << DISTANCE_BITWIDTH) - 1);
        if (static_cast<uint64_t>(graph.num_vertices) > max_label) {
            std::cout << "Warning: CC labels exceed "
                      << DISTANCE_BITWIDTH
                      << "-bit range; values will wrap." << std::endl;
        }
    }

    for (int u = 0; u < graph.num_vertices; ++u) {
        labels_k[u] = static_cast<ap_fixed_pod_t>(u);
    }

    int max_iterations = graph.num_vertices;
    if (config.convergence_mode == ConvergenceMode::FixedIterations &&
        config.max_iterations > 0) {
        max_iterations = config.max_iterations;
    }
    int iter = 0;

    std::cout << "\nStarting Host verification..." << std::endl;

    if (config.convergence_mode == ConvergenceMode::FixedIterations) {
        for (iter = 0; iter < max_iterations; ++iter) {
            (void)cc_min_iteration(graph, labels_k, labels_kplus1);
            labels_k.swap(labels_kplus1);
        }
        std::cout << "Host computation ran for " << iter
                  << " iterations (fixed)." << std::endl;
    } else {
        bool changed = true;
        while (changed && iter < max_iterations) {
            changed = cc_min_iteration(graph, labels_k, labels_kplus1);
            labels_k.swap(labels_kplus1);
            iter++;
        }
        std::cout << "Host computation converged after " << iter
                  << " iterations." << std::endl;
    }

    std::vector<unsigned int> out;
    out.reserve(labels_k.size());
    for (const auto &label : labels_k) {
        out.push_back(static_cast<unsigned int>(label));
    }
    return out;
}

std::vector<unsigned int> verify_pagerank(const GraphCSR &graph,
                                          const AlgorithmConfig &config) {
    constexpr uint32_t k_damp_fix = 108u;
    constexpr uint32_t k_max_abs_delta_eps = 1u;
    const uint64_t scale = (1ull << 30);

    const int n = graph.num_vertices;
    const uint32_t inv_n =
        (n > 0) ? static_cast<uint32_t>(scale / static_cast<uint64_t>(n)) : 0u;
    const uint32_t base_arg =
        (n > 0)
            ? static_cast<uint32_t>(
                  (scale * 15ull) / (100ull * static_cast<uint64_t>(n)))
            : 0u;

    std::vector<uint32_t> out_degree(static_cast<size_t>(n), 0u);
    for (int u = 0; u < n; ++u) {
        uint32_t deg = 0;
        for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
            int v = graph.columns[i];
            if ((v & 0x40000000) != 0) {
                continue;
            }
            (void)v;
            deg++;
        }
        out_degree[static_cast<size_t>(u)] = deg;
    }

    std::vector<int32_t> contrib(static_cast<size_t>(n), 0);
    for (int u = 0; u < n; ++u) {
        const uint32_t od = out_degree[static_cast<size_t>(u)];
        contrib[static_cast<size_t>(u)] =
            (od != 0u) ? static_cast<int32_t>(inv_n / od) : 0;
    }

    const int max_iterations =
        (config.max_iterations > 0) ? config.max_iterations : 200;

    std::cout << "\nStarting Host verification..." << std::endl;

    for (int iter = 0; iter < max_iterations; ++iter) {
        std::vector<int64_t> sum_in(static_cast<size_t>(n), 0);
        for (int u = 0; u < n; ++u) {
            const int32_t src_contrib = contrib[static_cast<size_t>(u)];
            if (src_contrib == 0) {
                continue;
            }
            for (int ei = graph.offsets[u]; ei < graph.offsets[u + 1]; ++ei) {
                int v = graph.columns[ei];
                if ((v & 0x40000000) != 0) {
                    continue;
                }
                sum_in[static_cast<size_t>(v)] += static_cast<int64_t>(src_contrib);
            }
        }

        uint32_t max_abs_delta = 0u;
        std::vector<int32_t> next_contrib(static_cast<size_t>(n), 0);
        for (int v = 0; v < n; ++v) {
            const uint32_t od = out_degree[static_cast<size_t>(v)];
            int32_t new_contrib = 0;
            if (od != 0u) {
                const int32_t sum32 = static_cast<int32_t>(sum_in[static_cast<size_t>(v)]);
                const int32_t new_score =
                    static_cast<int32_t>(base_arg) +
                    static_cast<int32_t>(
                        (static_cast<int64_t>(k_damp_fix) *
                         static_cast<int64_t>(sum32)) >>
                        7);
                const int32_t recip = static_cast<int32_t>((1u << 16) / od);
                new_contrib = static_cast<int32_t>(
                    (static_cast<int64_t>(new_score) *
                     static_cast<int64_t>(recip)) >>
                    16);
            }
            next_contrib[static_cast<size_t>(v)] = new_contrib;
            const int64_t delta =
                static_cast<int64_t>(new_contrib) - contrib[static_cast<size_t>(v)];
            const uint32_t abs_delta =
                (delta < 0) ? static_cast<uint32_t>(-delta)
                            : static_cast<uint32_t>(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
        }

        contrib.swap(next_contrib);

        if (config.convergence_mode != ConvergenceMode::FixedIterations &&
            max_abs_delta <= k_max_abs_delta_eps) {
            break;
        }
    }

    std::vector<unsigned int> out;
    out.reserve(contrib.size());
    for (int32_t value : contrib) {
        out.push_back(static_cast<uint32_t>(value));
    }
    return out;
}

std::vector<unsigned int> verify_bfs(const GraphCSR &graph, int start_node,
                                     const AlgorithmConfig &config) {
    const uint32_t active =
        config.active_mask != 0u ? config.active_mask : 0x80000000u;
    const uint32_t low_mask = ~active;
    const uint32_t inf =
        config.inf_value != 0u ? config.inf_value : 0x7ffffffeu;

    std::vector<uint32_t> prop(static_cast<size_t>(graph.num_vertices), inf);
    if (start_node >= 0 && start_node < graph.num_vertices) {
        prop[static_cast<size_t>(start_node)] = active | 1u;
    }

    const int max_iters = config.max_iterations > 0
                              ? config.max_iterations
                              : (graph.num_vertices + 8);
    std::cout << "\nStarting Host verification..." << std::endl;

    for (int iter = 0; iter < max_iters; ++iter) {
        std::vector<uint32_t> gathered(static_cast<size_t>(graph.num_vertices),
                                       0u);

        for (int u = 0; u < graph.num_vertices; ++u) {
            if ((prop[static_cast<size_t>(u)] & active) == 0u) {
                continue;
            }
            const uint32_t update = prop[static_cast<size_t>(u)] + 1u;
            for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
                int v = graph.columns[i];
                if ((v & 0x40000000) != 0) {
                    continue;
                }
                uint32_t &cur = gathered[static_cast<size_t>(v)];
                if (cur == 0u || ((cur & low_mask) > (update & low_mask))) {
                    cur = update;
                }
            }
        }

        uint32_t newly_discovered = 0u;
        for (int v = 0; v < graph.num_vertices; ++v) {
            const uint32_t incoming = gathered[static_cast<size_t>(v)];
            const uint32_t old = prop[static_cast<size_t>(v)];
            if ((incoming & active) != 0u && old == inf) {
                prop[static_cast<size_t>(v)] = incoming;
                newly_discovered++;
            } else {
                prop[static_cast<size_t>(v)] = old & low_mask;
            }
        }

        if (newly_discovered == 0u) {
            std::cout << "Host computation converged after " << (iter + 1)
                      << " iterations." << std::endl;
            break;
        }
    }

    std::vector<unsigned int> out;
    out.reserve(prop.size());
    for (uint32_t v : prop) {
        out.push_back(v);
    }
    return out;
}

std::vector<unsigned int> verify_article_rank(const GraphCSR &graph,
                                              const AlgorithmConfig &config) {
    const uint32_t k_damp_fix = 108u;
    const uint32_t const_term = 1258291u;
    const uint32_t scale_degree = 1u << 16;

    std::vector<uint32_t> indeg(static_cast<size_t>(graph.num_vertices), 0u);
    std::vector<uint32_t> out_deg(static_cast<size_t>(graph.num_vertices), 0u);
    for (int u = 0; u < graph.num_vertices; ++u) {
        uint32_t deg = 0;
        for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
            int v = graph.columns[i];
            if ((v & 0x40000000) != 0) {
                continue;
            }
            indeg[static_cast<size_t>(v)]++;
            deg++;
        }
        out_deg[static_cast<size_t>(u)] = deg;
    }

    uint64_t total_out_deg = 0;
    for (uint32_t d : out_deg) {
        total_out_deg += static_cast<uint64_t>(d);
    }
    const uint32_t avg =
        graph.num_vertices > 0
            ? static_cast<uint32_t>(
                  total_out_deg / static_cast<uint64_t>(graph.num_vertices))
            : 0u;

    std::vector<uint32_t> denom(static_cast<size_t>(graph.num_vertices), 1u);
    for (int u = 0; u < graph.num_vertices; ++u) {
        uint32_t d = out_deg[static_cast<size_t>(u)] + avg;
        if (d == 0u) {
            d = 1u;
        }
        denom[static_cast<size_t>(u)] = d;
    }

    std::vector<uint32_t> score(static_cast<size_t>(graph.num_vertices), 0u);
    std::vector<uint32_t> next(static_cast<size_t>(graph.num_vertices), 0u);
    std::vector<uint32_t> sums(static_cast<size_t>(graph.num_vertices), 0u);

    const int iters = config.max_iterations > 0 ? config.max_iterations : 10;

    std::cout << "\nStarting Host verification..." << std::endl;

    for (int iter = 0; iter < iters; ++iter) {
        std::fill(sums.begin(), sums.end(), 0u);
        for (int u = 0; u < graph.num_vertices; ++u) {
            uint32_t src = score[static_cast<size_t>(u)];
            for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
                int v = graph.columns[i];
                if ((v & 0x40000000) != 0) {
                    continue;
                }
                sums[static_cast<size_t>(v)] += src;
            }
        }

        for (int v = 0; v < graph.num_vertices; ++v) {
            if (indeg[static_cast<size_t>(v)] == 0u) {
                next[static_cast<size_t>(v)] = score[static_cast<size_t>(v)];
                continue;
            }
            uint32_t t_prop = sums[static_cast<size_t>(v)];
            uint32_t d = denom[static_cast<size_t>(v)];
            uint32_t tmp = d == 0u ? 0u : (scale_degree / d);
            uint32_t new_score = k_damp_fix * t_prop + const_term;
            next[static_cast<size_t>(v)] = new_score * tmp;
        }

        score.swap(next);
    }

    std::vector<unsigned int> out;
    out.reserve(score.size());
    for (uint32_t v : score) {
        out.push_back(v);
    }
    return out;
}

bool wcc_max_iteration(const GraphCSR &graph,
                       const std::vector<ap_fixed_pod_t> &labels_in,
                       std::vector<ap_fixed_pod_t> &labels_out) {
    labels_out = labels_in;

    for (int u = 0; u < graph.num_vertices; ++u) {
        ap_fixed_pod_t src_label = labels_in[u];
        for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
            int v = graph.columns[i];
            if ((v & 0x40000000) != 0) {
                continue;
            }
            if (src_label > labels_out[v]) {
                labels_out[v] = src_label;
            }
        }
    }

    return labels_in != labels_out;
}

std::vector<unsigned int> verify_wcc(const GraphCSR &graph,
                                     const AlgorithmConfig &config) {
    std::vector<ap_fixed_pod_t> labels_k(graph.num_vertices, 0);
    std::vector<ap_fixed_pod_t> labels_kplus1(graph.num_vertices, 0);

    if (DISTANCE_BITWIDTH < 32) {
        const uint64_t max_label = DISTANCE_SIGNED
                                       ? ((uint64_t(1) << (DISTANCE_BITWIDTH - 1)) - 1)
                                       : ((uint64_t(1) << DISTANCE_BITWIDTH) - 1);
        if (static_cast<uint64_t>(graph.num_vertices) > max_label) {
            std::cout << "Warning: WCC labels exceed "
                      << DISTANCE_BITWIDTH
                      << "-bit range; values will wrap." << std::endl;
        }
    }

    for (int u = 0; u < graph.num_vertices; ++u) {
        labels_k[u] = static_cast<ap_fixed_pod_t>(u);
    }

    int max_iterations = graph.num_vertices;
    if (config.max_iterations > 0) {
        max_iterations = config.max_iterations;
    }
    int iter = 0;
    bool changed = true;

    std::cout << "\nStarting Host verification..." << std::endl;

    while (changed && iter < max_iterations) {
        changed = wcc_max_iteration(graph, labels_k, labels_kplus1);
        labels_k.swap(labels_kplus1);
        iter++;
    }

    std::cout << "Host computation converged after " << iter << " iterations."
              << std::endl;

    std::vector<unsigned int> out;
    out.reserve(labels_k.size());
    for (const auto &label : labels_k) {
        out.push_back(static_cast<unsigned int>(label));
    }
    return out;
}

} // namespace

std::vector<unsigned int> verify_on_host(const GraphCSR &graph, int start_node,
                                         const AlgorithmConfig &config) {
    switch (config.algorithm_kind) {
    case AlgorithmKind::Bfs:
        return verify_bfs(graph, start_node, config);
    case AlgorithmKind::ArticleRank:
        return verify_article_rank(graph, config);
    case AlgorithmKind::ConnectedComponents:
        return verify_connected_components(graph, config);
    case AlgorithmKind::Pagerank:
        return verify_pagerank(graph, config);
    case AlgorithmKind::Wcc:
        return verify_wcc(graph, config);
    case AlgorithmKind::Sssp:
    default:
        return verify_sssp(graph, start_node, config);
    }
}
