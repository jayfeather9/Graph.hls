{
    Node: {
        score: int<32>
        out_deg: int<32>
    }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.score)
    summed = reduce(key=dst_ids, values=[updates], function=lambda x, y: x + y)
    next_score = map([summed], lambda t: (108 * t + 1258291) * (65536 / self.out_deg))
    return next_score as result_node_prop.score
}
