#ifndef GRAPH_LOADER_HPP
#define GRAPH_LOADER_HPP

#include "common.h"

class GraphLoader {
public:
    GraphLoader() = default;
    ~GraphLoader() = default;

    bool load_graph(const std::string& file_path);

    const std::vector<int>& get_row_ptr() const { return row_ptr; }
    const std::vector<int>& get_col_idx() const { return col_idx; }
    const std::vector<int>& get_weights() const { return weights; }
    int get_num_vertices() const { return num_vertices; }
    int get_num_edges() const { return num_edges; }

private:
    int num_vertices = 0;
    int num_edges = 0;
    std::vector<int> row_ptr;
    std::vector<int> col_idx;
    std::vector<int> weights;
};

#endif // GRAPH_LOADER_HPP
