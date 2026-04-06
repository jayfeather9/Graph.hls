{
    Node: {
        dist: int<32>
    }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + 1)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    relaxed = map([min_dists], lambda d: self.dist < d ? d : self.dist)
    return relaxed as result_node_prop.dist
}
