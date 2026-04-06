#include "host_verifier.h"
#include <algorithm>
#include <iostream>
#include <vector>

std::vector<unsigned int> verify_on_host(const GraphCSR &graph, int start_node) {
    (void)start_node;

    constexpr int kMaxIterations = 200;
    constexpr uint32_t kMaxAbsDeltaEps = 1;

    const int n = graph.num_vertices;
    const int kScaleDegree = 16;
    const int kScaleDamping = 7;
    const int32_t kDampFixPoint = 108; // 0.85 * 128
    const int32_t kBase =
        (int32_t)(((int64_t)1 << (kScaleDegree + kScaleDamping)) * 15 / 100);

    std::vector<uint32_t> out_degree(n, 0);
    for (int u = 0; u < n; ++u) {
        out_degree[u] = (uint32_t)(graph.offsets[u + 1] - graph.offsets[u]);
    }

    uint64_t total_outdegree = 0;
    for (int u = 0; u < n; ++u) {
        total_outdegree += out_degree[u];
    }
    const uint32_t avg_outdegree =
        (n > 0) ? (uint32_t)(total_outdegree / (uint64_t)n) : 0;

    std::vector<int32_t> contrib(n, 0);

    int iter = 0;

    std::cout << "\nStarting Host AR verification..." << std::endl;

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
            uint32_t denom = od + avg_outdegree;
            int32_t new_contrib = 0;
            if (denom != 0) {
                int32_t sum32 = (int32_t)sum_in[v];
                int32_t new_score =
                    (int32_t)((int64_t)kDampFixPoint * (int64_t)sum32) +
                    kBase;
                int32_t tmp = (int32_t)((1u << kScaleDegree) / denom);
                new_contrib = (int32_t)((int64_t)new_score * (int64_t)tmp);
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
