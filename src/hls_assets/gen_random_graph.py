#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import random
import argparse

SEED = 42
random.seed(SEED)


def generate_random_connected_graph(num_nodes, num_edges, has_weight, undirected=1):
    """
    生成一个随机的有向连通图。

    Args:
        num_nodes (int): 图中的节点数。
        num_edges (int): 图中的边数。
        has_weight (bool): 边是否应该有权重。

    Returns:
        list: 一个表示图的元组列表。
              如果无权重，格式为 [(u, v), ...]。
              如果有权重，格式为 [(u, v, w), ...]。
    """
    if num_nodes <= 0:
        return []

    # 节点编号从 0 到 num_nodes - 1
    nodes = list(range(num_nodes))
    edges = set()

    # --- 步骤 1: 确保图是连通的 ---
    # 我们通过创建一个随机生成树来保证弱连通性。
    # 从一个随机节点开始，然后逐渐将其他节点添加到树中。

    # 跟踪已在树中的节点和尚未在树中的节点
    in_tree = {random.choice(nodes)}
    not_in_tree = list(set(nodes) - in_tree)
    random.shuffle(not_in_tree)

    # 遍历所有尚未在树中的节点
    for node_to_add in not_in_tree:
        # 从已经在树中的节点中随机选择一个连接点
        connect_to = random.choice(list(in_tree))

        # 随机决定边的方向
        u, v = (node_to_add, connect_to) if random.random() < 0.5 else (connect_to, node_to_add)

        edges.add((u, v))
        in_tree.add(node_to_add)

    # --- 步骤 2: 添加剩余的边以达到指定的边数 ---
    # 我们现在已经有了 num_nodes - 1 条边，构成了一个连通的骨架。
    while len(edges) < num_edges:
        u = random.choice(nodes)
        v = random.choice(nodes)

        # 避免自环和重复的边
        if u != v and (u, v) not in edges:
            edges.add((u, v))

    # --- 步骤 3: 如果需要，为每条边分配随机权重 ---
    final_graph = []
    if has_weight:
        # 权重范围可以根据需要调整
        min_weight, max_weight = 1, 100
        for u, v in edges:
            weight = random.randint(min_weight, max_weight)
            final_graph.append((u, v, weight))
            if undirected:
                final_graph.append((v, u, weight))
    else:
        final_graph = list(edges)
        if undirected:
            final_graph = [(u, v) for u, v in edges] + [(v, u) for u, v in edges]

    # 最后打乱顺序，使输出看起来更随机
    random.shuffle(final_graph)

    return final_graph


def main():
    """主函数，用于解析命令行参数并打印生成的图。"""
    parser = argparse.ArgumentParser(
        description="生成一个随机的有向连通图。",
        epilog="例如: python gen_random_graph.py 10 15 --have_weight",
    )
    parser.add_argument("nodes", type=int, help="图中的节点数 (一个正整数)")
    parser.add_argument("edges", type=int, help="图中的边数")
    parser.add_argument("--have_weight", action="store_true", help="如果指定此标志，则为边生成随机权重")

    args = parser.parse_args()

    # --- 参数验证 ---
    num_nodes = args.nodes
    num_edges = args.edges

    if num_nodes <= 0:
        print(f"错误: 节点数必须是正数。", file=sys.stderr)
        sys.exit(1)

    min_required_edges = num_nodes - 1
    # 对于有向图，不考虑自环时，最大边数为 N * (N - 1)
    max_possible_edges = num_nodes * (num_nodes - 1)

    if num_edges < min_required_edges:
        print(f"错误: 对于 {num_nodes} 个节点的连通图, 至少需要 {min_required_edges} 条边。", file=sys.stderr)
        sys.exit(1)

    if num_edges > max_possible_edges:
        print(
            f"错误: 对于 {num_nodes} 个节点, 最多只能有 {max_possible_edges} 条边 (无自环)。", file=sys.stderr
        )
        sys.exit(1)

    # --- 生成并打印图 ---
    graph = generate_random_connected_graph(num_nodes, num_edges, args.have_weight)

    # for edge_info in graph:
    #     # 使用 ' '.join() 可以优雅地处理元组中的所有元素
    #     print(" ".join(map(str, edge_info)))
    
    # write to graph.txt
    with open("graph.txt", "w") as f:
        for edge_info in graph:
            f.write(" ".join(map(str, edge_info)) + "\n")

    # generate a "1 -> 2;" graphviz representation to a file 524289 vertices, 15483988 edges.
    # with open("graph.dot", "w") as f:
    #     f.write("digraph G {\n")
    #     for edge_info in graph:
    #         if args.have_weight:
    #             u, v, w = edge_info
    #             f.write(f"    {u} -> {v} [label={w}];\n")
    #         else:
    #             u, v = edge_info
    #             f.write(f"    {u} -> {v};\n")
    #     f.write("}\n")


if __name__ == "__main__":
    main()
