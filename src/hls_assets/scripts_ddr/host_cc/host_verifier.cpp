#include "host_verifier.h"
#include "host_bellman_ford.h"
#include <algorithm>
#include <iostream>

std::vector<unsigned int> verify_on_host(const GraphCSR &graph, int start_node) {
    (void)start_node;

    std::vector<unsigned int> bitmasks_in(graph.num_vertices, 0);
    int seed_count = std::min(graph.num_vertices, 32);
    for (int i = 0; i < seed_count; ++i) {
        bitmasks_in[i] = (1u << i);
    }

    std::vector<unsigned int> bitmasks_out(graph.num_vertices, 0);

    int max_iterations = graph.num_vertices;
    int iter = 0;
    bool changed = true;

    std::cout << "\nStarting Host CC verification..." << std::endl;
    while (changed && iter < max_iterations) {
        changed = host_cc_iteration(graph, bitmasks_in, bitmasks_out);
        bitmasks_in.swap(bitmasks_out);
        iter++;
    }

    std::cout << "Host computation converged after " << iter << " iterations."
              << std::endl;

    return bitmasks_in;
}
