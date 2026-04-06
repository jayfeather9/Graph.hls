#ifndef __GRAPH_LOADER_H__
#define __GRAPH_LOADER_H__

#include "common.h"

// Loads a graph from a text file (edge list format: src dst weight)
// and converts it into a GraphCSR object.
GraphCSR load_graph_from_file(const std::string &file_path);

#endif // __GRAPH_LOADER_H__