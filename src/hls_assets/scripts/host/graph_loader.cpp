#include "graph_loader.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <sys/stat.h>
#include <vector>

struct Edge {
    int src;
    int dest;
    int weight;
#if EDGE_PROP_COUNT > 0
    std::array<uint64_t, EDGE_PROP_COUNT> props{};
#endif
};

GraphCSR load_graph_from_file(const std::string &file_path) {
    bool is_one_based = false;
    char comment_char = '#';

    if (file_path.size() > 4 &&
        file_path.substr(file_path.size() - 4) == ".mtx") {
        is_one_based = true;
        comment_char = '%';
        std::cout << "Detected .mtx format (1-based indexing)." << std::endl;
    } else {
        std::cout << "Detected .txt format (0-based indexing)." << std::endl;
    }

    std::FILE *file = std::fopen(file_path.c_str(), "r");
    if (!file) {
        std::cerr << "Error: Could not open graph file: " << file_path << " ("
                  << std::strerror(errno) << ")" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    static constexpr size_t kIOBufSize = 8u * 1024u * 1024u;
    static std::vector<char> io_buf(kIOBufSize);
    std::setvbuf(file, io_buf.data(), _IOFBF, io_buf.size());

    std::deque<Edge> edges;

    int max_vertex_id = -1;
    int min_vertex_id = 1;
    long line_num = 0;

    static constexpr size_t kLineBufSize = 1u * 1024u * 1024u;
    std::vector<char> line_buf(kLineBufSize);

    while (std::fgets(line_buf.data(), static_cast<int>(line_buf.size()), file)) {
        line_num++;

        if (std::strchr(line_buf.data(), '\n') == nullptr) {
            int c = 0;
            while ((c = std::fgetc(file)) != EOF) {
                if (c == '\n') {
                    break;
                }
            }
        }

        if (line_buf[0] == '\0' || line_buf[0] == '\n' || line_buf[0] == '\r' ||
            line_buf[0] == comment_char) {
            continue;
        }

        char *p = line_buf.data();
        char *end = nullptr;

        long src = std::strtol(p, &end, 10);
        if (end == p) {
            std::cerr << "Warning: Skipping malformed line " << line_num << ": "
                      << line_buf.data() << std::endl;
            continue;
        }
        p = end;

        long dest = std::strtol(p, &end, 10);
        if (end == p) {
            std::cerr << "Warning: Skipping malformed line " << line_num << ": "
                      << line_buf.data() << std::endl;
            continue;
        }
        p = end;

        Edge edge;
        edge.src = static_cast<int>(src);
        edge.dest = static_cast<int>(dest);

        std::vector<uint64_t> parsed_props;
        parsed_props.reserve((EDGE_PROP_COUNT > 0) ? EDGE_PROP_COUNT : 1);
        while (true) {
            long long value = std::strtoll(p, &end, 10);
            if (end == p) {
                break;
            }
            parsed_props.push_back(static_cast<uint64_t>(value));
            p = end;
        }

#if EDGE_PROP_COUNT > 0
        if (parsed_props.empty()) {
            edge.props[0] = 1;
        } else {
            for (size_t i = 0; i < edge.props.size() && i < parsed_props.size();
                 ++i) {
                edge.props[i] = parsed_props[i];
            }
        }
        edge.weight = static_cast<int>(edge.props[0]);
#else
        edge.weight =
            parsed_props.empty() ? 1 : static_cast<int>(parsed_props[0]);
#endif

        if (is_one_based) {
            edge.src--;
            edge.dest--;
        }

        if (edge.src < 0 || edge.dest < 0) {
            std::cerr << "Warning: Skipping edge with negative vertex ID on line "
                      << line_num << std::endl;
            continue;
        }

        if (edge.src > max_vertex_id) {
            max_vertex_id = edge.src;
        }
        if (edge.dest > max_vertex_id) {
            max_vertex_id = edge.dest;
        }
        if (edge.src < min_vertex_id) {
            min_vertex_id = edge.src;
        }
        if (edge.dest < min_vertex_id) {
            min_vertex_id = edge.dest;
        }

        edges.push_back(std::move(edge));
    }

    std::fclose(file);

    if (min_vertex_id == 1 && !is_one_based) {
        std::cout << "Converting graph from 1-based to 0-based indexing."
                  << std::endl;
        for (auto &edge : edges) {
            edge.src--;
            edge.dest--;
        }
    }

    GraphCSR graph;
    if (max_vertex_id == -1) {
        graph.num_vertices = 0;
        graph.num_edges = 0;
        std::cout << "Graph is empty." << std::endl;
        return graph;
    }

    graph.num_vertices = max_vertex_id + 1;
    graph.num_edges = static_cast<int>(edges.size());

    std::cout << "Graph loaded: " << graph.num_vertices << " vertices, "
              << graph.num_edges << " edges." << std::endl;

    std::sort(edges.begin(), edges.end(), [](const Edge &a, const Edge &b) {
        if (a.src != b.src) {
            return a.src < b.src;
        }
        return a.dest < b.dest;
    });

    graph.offsets.resize(graph.num_vertices + 1);
    graph.columns.resize(graph.num_edges);
    graph.weights.resize(graph.num_edges);
    if (EDGE_PROP_COUNT > 0) {
        graph.edge_props.resize(static_cast<size_t>(graph.num_edges) *
                                EDGE_PROP_COUNT);
    }

    int last_src = -1;
    for (int i = 0; i < graph.num_edges; ++i) {
        const Edge &e = edges[static_cast<size_t>(i)];
        graph.columns[static_cast<size_t>(i)] = e.dest;
        graph.weights[static_cast<size_t>(i)] = e.weight;

#if EDGE_PROP_COUNT > 0
        {
            const size_t base = static_cast<size_t>(i) * EDGE_PROP_COUNT;
            for (size_t p_idx = 0; p_idx < EDGE_PROP_COUNT; ++p_idx) {
                graph.edge_props[base + p_idx] = e.props[p_idx];
            }
        }
#endif

        if (e.src != last_src) {
            const int fill_from = last_src + 1;
            const int fill_to = e.src;
            for (int v = fill_from; v <= fill_to; ++v) {
                graph.offsets[static_cast<size_t>(v)] = i;
            }
            last_src = e.src;
        }
    }

    for (int v = last_src + 1; v <= graph.num_vertices; ++v) {
        graph.offsets[static_cast<size_t>(v)] = graph.num_edges;
    }

    return graph;
}
