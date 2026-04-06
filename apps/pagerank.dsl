{
    Node: {
        rank: float
        out_deg: int<32>
    }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    contribs = map([edges], lambda e: e.src.rank)
    summed = reduce(key=dst_ids, values=[contribs], function=lambda x, y: x + y)
    new_rank = map([summed], lambda r: 0.15 + 0.85 * (r / self.out_deg))
    return new_rank as result_node_prop.rank
}
