{
    Node: {
        dist: int<32>
    }
    Edge: {
        weight: int<32>
    }
}

HlsConfig {
    topology: {
        apply_slr: 1
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: [
            { pipelines: 6 merger_slr: 1 pipeline_slr: [1,1,1,2,2,2] }
        ]
        big_groups: [
            { pipelines: 8 merger_slr: 0 pipeline_slr: [0,0,0,0,0,0,0,0] }
        ]
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

