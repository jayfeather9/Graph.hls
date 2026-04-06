#include "graph_loader.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

// 边结构体，用于临时存储从文件中读取的边
struct Edge {
    int src, dest, weight;
};

// 主函数，从文件中加载图并转换为 CSR 格式
GraphCSR load_graph_from_file(const std::string &file_path) {
    // ---- 1. 根据文件扩展名判断图的格式 ----
    bool is_one_based = false; // 默认为 0-indexed
    char comment_char = '#';   // 默认注释符

    if (file_path.size() > 4 &&
        file_path.substr(file_path.size() - 4) == ".mtx") {
        is_one_based = true; // .mtx 文件通常是 1-indexed
        comment_char = '%';  // .mtx 文件使用 '%' 作为注释
        std::cout << "Detected .mtx format (1-based indexing)." << std::endl;
    } else {
        std::cout << "Detected .txt format (0-based indexing)." << std::endl;
    }

    // ---- 2. 打开文件并逐行解析 ----
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open graph file: " << file_path
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    std::vector<Edge> edges;
    int max_vertex_id = -1;
    int min_vertex_id = 1;
    std::string line;
    long line_num = 0;

    while (std::getline(file, line)) {
        line_num++;
        // 跳过空行和注释行
        if (line.empty() || line[0] == comment_char) {
            continue;
        }

        std::istringstream iss(line);
        Edge edge;

        // 尝试读取 src, dest, weight
        if (iss >> edge.src >> edge.dest >> edge.weight) {
            // 成功读取三个值
        } else {
            // 如果失败，重置流并尝试只读取 src, dest
            iss.clear();
            iss.seekg(0);
            if (iss >> edge.src >> edge.dest) {
                edge.weight = 1; // 赋予默认权重 1
            } else {
                std::cerr << "Warning: Skipping malformed line " << line_num
                          << ": " << line << std::endl;
                continue;
            }
        }

        // 如果是 1-based 格式，转换为 0-based
        if (is_one_based) {
            edge.src--;
            edge.dest--;
        }

        // 检查顶点ID是否有效
        if (edge.src < 0 || edge.dest < 0) {
            std::cerr
                << "Warning: Skipping edge with negative vertex ID on line "
                << line_num << std::endl;
            continue;
        }

        edges.push_back(edge);
        max_vertex_id = std::max({max_vertex_id, edge.src, edge.dest});
        min_vertex_id = std::min({min_vertex_id, edge.src, edge.dest});
    }
    file.close();

    if (min_vertex_id == 1 && !is_one_based) {
        // modify to 0-based
        std::cout << "Converting graph from 1-based to 0-based indexing."
                  << std::endl;
        for (auto &edge : edges) {
            edge.src--;
            edge.dest--;
        }
    }

    // ---- 3. 将边列表转换为 CSR 格式 ----
    GraphCSR graph;
    if (max_vertex_id == -1) { // 如果文件为空或无效
        graph.num_vertices = 0;
        graph.num_edges = 0;
        std::cout << "Graph is empty." << std::endl;
        return graph;
    }

    graph.num_vertices = max_vertex_id + 1;
    graph.num_edges = edges.size();

    std::cout << "Graph loaded: " << graph.num_vertices << " vertices, "
              << graph.num_edges << " edges." << std::endl;

    // 为了进行 CSR 转换，按源顶点 ID 对边进行排序
    std::sort(edges.begin(), edges.end(), [](const Edge &a, const Edge &b) {
        if (a.src != b.src) {
            return a.src < b.src;
        }
        return a.dest < b.dest;
    });

    // 分配 CSR 数组内存
    graph.offsets.resize(graph.num_vertices + 1);
    graph.columns.resize(graph.num_edges);
    graph.weights.resize(graph.num_edges);

    // 填充 columns 和 weights 数组，并计算每个顶点的出度
    std::vector<int> out_degree(graph.num_vertices, 0);
    for (int i = 0; i < graph.num_edges; ++i) {
        graph.columns[i] = edges[i].dest;
        graph.weights[i] = edges[i].weight;
        out_degree[edges[i].src]++;
    }

    // 通过出度的前缀和计算 offsets 数组
    graph.offsets[0] = 0;
    for (int i = 0; i < graph.num_vertices; ++i) {
        graph.offsets[i + 1] = graph.offsets[i] + out_degree[i];
    }

    return graph;
}