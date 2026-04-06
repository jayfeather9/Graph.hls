#include "host_verifier.h"
#include <iostream>

std::vector<unsigned int> verify_on_host(const GraphCSR &graph,
                                         int start_node) {
    const int n = graph.num_vertices;
    std::vector<unsigned int> labels_in(n, 0);
    if (start_node >= 0 && start_node < n) {
        labels_in[start_node] = 1;
    }

    std::vector<uint8_t> has_incoming(n, 0);
    for (int e = 0; e < graph.num_edges; ++e) {
        int v = graph.columns[e];
        if (v >= 0 && v < n) {
            has_incoming[v] = 1;
        }
    }

    int max_iterations = graph.num_vertices;
    int iter = 0;
    bool changed = true;

    std::cout << "\nStarting Host WCC verification..." << std::endl;

    while (changed && iter < max_iterations) {
        // S-G-A style: each iteration depends only on last iteration.
        // Apply stage only keeps previous labels for nodes with no incoming
        // edges (non-dst nodes), matching FPGA behavior.
        std::vector<unsigned int> labels_out(n, 0);
        for (int v = 0; v < n; ++v) {
            if (!has_incoming[v]) {
                labels_out[v] = labels_in[v];
            }
        }

        // Scatter-Gather: propagate labels along edges and take max at dest.
        for (int u = 0; u < n; ++u) {
            unsigned int label = labels_in[u];
            if (label == 0) {
                continue;
            }
            for (int ei = graph.offsets[u]; ei < graph.offsets[u + 1]; ++ei) {
                int v = graph.columns[ei];
                if (v < 0 || v >= n) {
                    continue;
                }
                if (label > labels_out[v]) {
                    labels_out[v] = label;
                }
            }
        }

        changed = (labels_out != labels_in);
        labels_in.swap(labels_out);
        iter++;
    }

    std::cout << "Host computation converged after " << iter << " iterations."
              << std::endl;

    return labels_in;
}
