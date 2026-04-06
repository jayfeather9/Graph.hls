{
    Node: {
        dist: int<32>
    }
    Edge: {
        weight: int<32>
        prop1: int<32>
        prop2: int<32>
        prop3: int<32>
        prop4: int<32>
        prop5: int<32>
        prop6: int<32>
        prop7: int<32>
        prop8: int<32>
        prop9: int<32>
        prop10: int<32>
        prop11: int<32>
        prop12: int<32>
        prop13: int<32>
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + e.weight)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    relaxed = map([min_dists], lambda d: self.dist > d ? d : self.dist)
    return relaxed as result_node_prop.dist
}
