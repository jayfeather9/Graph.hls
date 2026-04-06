{
    Node: {
        dist: int<32>
    }
    Edge: {
        weight: int<32>
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + e.weight)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    return min_dists as result_node_prop.dist
}