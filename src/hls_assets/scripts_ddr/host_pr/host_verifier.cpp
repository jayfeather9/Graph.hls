#include "host_verifier.h"
#include <algorithm>
#include <iostream>
#include <vector>

std::vector<unsigned int> verify_on_host(const GraphCSR &graph, int start_node) {
    (void)start_node;

    constexpr int kMaxIterations = 200;
    constexpr uint32_t kMaxAbsDeltaEps = 1;

    const int n = graph.num_vertices;
    const uint64_t SCALE = (1ull << 30);
    const uint32_t inv_n = (n > 0) ? (uint32_t)(SCALE / (uint64_t)n) : 0;
    const uint32_t base_arg =
        (n > 0) ? (uint32_t)((SCALE * 15ull) / (100ull * (uint64_t)n)) : 0;

    std::vector<uint32_t> out_degree(n, 0);
    for (int u = 0; u < n; ++u) {
        out_degree[u] = (uint32_t)(graph.offsets[u + 1] - graph.offsets[u]);
    }

    std::vector<int32_t> contrib(n, 0);
    for (int u = 0; u < n; ++u) {
        uint32_t od = out_degree[u];
        contrib[u] = (od != 0) ? (int32_t)(inv_n / od) : 0;
    }

    int iter = 0;

    std::cout << "\nStarting Host PR verification..." << std::endl;

    while (iter < kMaxIterations) {
        std::vector<int64_t> sum_in(n, 0);
        for (int u = 0; u < n; ++u) {
            int32_t c = contrib[u];
            if (c == 0) continue;
            for (int ei = graph.offsets[u]; ei < graph.offsets[u + 1]; ++ei) {
                int v = graph.columns[ei];
                if (v < 0 || v >= n) continue;
                sum_in[v] += (int64_t)c;
            }
        }

        uint32_t max_abs_delta = 0;
        std::vector<int32_t> next_contrib(n, 0);
        for (int v = 0; v < n; ++v) {
            uint32_t od = out_degree[v];
            int32_t new_contrib = 0;
            if (od != 0) {
                int32_t sum32 = (int32_t)sum_in[v];
                int32_t new_score =
                    (int32_t)base_arg +
                    (int32_t)(((int64_t)108 * (int64_t)sum32) >> 7);
                int32_t tmp = (int32_t)((1u << 16) / od);
                new_contrib =
                    (int32_t)(((int64_t)new_score * (int64_t)tmp) >> 16);
            }
            next_contrib[v] = new_contrib;
            int64_t delta = (int64_t)new_contrib - (int64_t)contrib[v];
            uint32_t abs_delta =
                (delta < 0) ? (uint32_t)(-delta) : (uint32_t)(delta);
            if (abs_delta > max_abs_delta) {
                max_abs_delta = abs_delta;
            }
        }

        contrib.swap(next_contrib);
        iter++;
        if (max_abs_delta <= kMaxAbsDeltaEps) {
            break;
        }
    }

    std::cout << "Host computation converged after " << iter << " iterations."
              << std::endl;

    std::vector<unsigned int> result;
    result.reserve(contrib.size());
    for (int32_t c : contrib) {
        result.push_back((uint32_t)c);
    }
    return result;
}
