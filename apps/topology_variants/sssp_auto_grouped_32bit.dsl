{
    Node: {
        dist: int<32>
    }
    Edge: {}
}

HlsConfig {
    topology: {
        apply_slr: 1
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: auto
        big_groups: auto
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + 1)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    relaxed = map([min_dists], lambda d: self.dist > d ? d : self.dist)
    return relaxed as result_node_prop.dist
}
