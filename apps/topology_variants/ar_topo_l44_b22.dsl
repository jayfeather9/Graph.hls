{
    Node: {
        score: int<32>
        out_deg: int<32>
    }
    Edge: {}
}

HlsConfig {
    topology: {
        apply_slr: 0
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: [
            { pipelines: 4 merger_slr: 0 pipeline_slr: [0,0,0,0] },
            { pipelines: 4 merger_slr: 1 pipeline_slr: [1,1,1,1] }
        ]
        big_groups: [
            { pipelines: 2 merger_slr: 2 pipeline_slr: [2,2] },
            { pipelines: 2 merger_slr: 2 pipeline_slr: [2,2] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.score)
    summed = reduce(key=dst_ids, values=[updates], function=lambda x, y: x + y)
    next_score = map([summed], lambda t: (108 * t + 1258291) * (65536 / self.out_deg))
    return next_score as result_node_prop.score
}
