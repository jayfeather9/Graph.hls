#ifndef __HOST_VERIFIER_H__
#define __HOST_VERIFIER_H__

#include "common.h"

// Main function to run the Bellman-Ford algorithm on the host CPU for
// verification.
std::vector<int> verify_on_host(const GraphCSR &graph, int start_node);

#endif // __HOST_VERIFIER_H__