{
    Node: {
        score: int<32>
        out_deg: int<32>
        avg_out_deg: int<32>
    }
    Edge: {}
}

HlsConfig {
    memory: ddr
    zero_sentinel: true
    topology: {
        apply_slr: 1
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: [
            { pipelines: 4 merger_slr: 1 pipeline_slr: [0,0,2,2] }
        ]
        big_groups: [
            { pipelines: 4 merger_slr: 1 pipeline_slr: [0,2,1,1] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    contribs = map([edges], lambda e: e.src.score)
    summed = reduce(key=dst_ids, values=[contribs], function=lambda x, y: x + y)
    new_score = map([summed], lambda t: (108 * t + 1258291) * (65536 / (self.out_deg + self.avg_out_deg)))
    return new_score as result_node_prop.score
}
