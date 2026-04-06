#include "host_verifier.h"
#include "host_bellman_ford.h"
#include <iostream>

std::vector<int> verify_on_host(const GraphCSR &graph, int start_node) {
    std::vector<int> distances(graph.num_vertices, INFINITY_DIST);
    distances[start_node] = 0;

    int max_iterations = graph.num_vertices;
    int iter = 0;
    bool changed = true;

    std::cout << "\nStarting Host verification..." << std::endl;

    while (changed && iter < max_iterations) {
        // Run one iteration of the algorithm
        changed = host_bellman_ford_iteration(graph, distances);
        iter++;
    }

    std::cout << "Host computation converged after " << iter << " iterations."
              << std::endl;

    // Check for negative weight cycles (optional but good practice)
    if (iter == max_iterations &&
        host_bellman_ford_iteration(graph, distances)) {
        std::cout << "Warning: Negative weight cycle detected by host verifier."
                  << std::endl;
    }

    return distances;
}