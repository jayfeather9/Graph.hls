#include "host_bellman_ford.h"

bool host_bellman_ford_iteration(const GraphCSR &graph,
                                 std::vector<int> &distances) {
    bool changed = false;

    // --- USER MODIFIABLE SECTION: Host Computation Logic ---
    // For each vertex, relax all outgoing edges
    for (int u = 0; u < graph.num_vertices; ++u) {
        if (distances[u] != INFINITY_DIST) {
            for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
                int v = graph.columns[i];
                if ((v & 0x40000000) != 0) {
                    continue; // Skip dummy edges
                }
                int weight = graph.weights[i];

                // Relaxation step
                if (distances[u] + weight < distances[v]) {
                    distances[v] = distances[u] + weight;
                    changed = true;
                }
            }
        }
    }
    // --- END USER MODIFIABLE SECTION ---

    return changed;
}

bool host_cc_iteration(const GraphCSR &graph,
                           const std::vector<unsigned int> &bitmasks_in,
                           std::vector<unsigned int> &bitmasks_out) {

    // 1. 将 k 状态 (in) 初始化为 k+1 状态 (out)
    // 这能确保没有入度的节点的值可以保持不变
    bitmasks_out = bitmasks_in;

    // 2. 执行标签传播
    //    只从 k 状态 (bitmasks_in) 读取
    //    只向 k+1 状态 (bitmasks_out) 写入
    for (int u = 0; u < graph.num_vertices; ++u) {
        
        // 2a. 只从 k 状态 (in) 读取 'u' 的掩码
        if (bitmasks_in[u] != 0) {
            
            for (int i = graph.offsets[u]; i < graph.offsets[u + 1]; ++i) {
                int v = graph.columns[i];

                if ((v & 0x40000000) != 0) {
                    continue; 
                }

                // 2b. 检查 'u' 的掩码 (in[u]) 是否有 'v' 的新掩码 (out[v]) 中所没有的位
                if ((bitmasks_in[u] & ~bitmasks_out[v]) != 0) {
                    
                    // 2c. 将 'u' 的掩码 (in[u]) 合并到 'v' 的 k+1 状态 (out[v]) 中
                    bitmasks_out[v] = bitmasks_out[v] | bitmasks_in[u];
                    
                    // 注意：在 Jacobi 迭代中，我们不能在这里设置 changed = true
                    // 必须在所有计算完成后再比较
                }
            }
        }
    }

    // 3. 在所有计算完成后，统一比较 k 状态 和 k+1 状态
    bool changed = false;
    for (int i = 0; i < graph.num_vertices; ++i) {
        if (bitmasks_in[i] != bitmasks_out[i]) {
            changed = true; // 发现任何差异，说明未收敛
            break;
        }
    }

    // 如果 changed = true，说明发生了变化，迭代应继续
    // 如果 changed = false，说明 k 和 k+1 状态相同，算法已收敛
    return changed;
}