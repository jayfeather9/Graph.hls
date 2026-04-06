{
    Node: {
        rank: float
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
            { pipelines: 12 merger_slr: 0 pipeline_slr: [0,0,0,0,0,0,0,0,0,0,0,0] }
        ]
        big_groups: [
            { pipelines: 2 merger_slr: 0 pipeline_slr: [0,0] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    contribs = map([edges], lambda e: e.src.rank)
    summed = reduce(key=dst_ids, values=[contribs], function=lambda x, y: x + y)
    new_rank = map([summed], lambda r: 0.15 + 0.85 * (r / self.out_deg))
    return new_rank as result_node_prop.rank
}

