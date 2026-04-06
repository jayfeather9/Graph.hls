{
    Node: {
        prop: int<32>
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
    updates = map([edges], lambda e: ((e.src.prop & 2147483648) != 0) ? (e.src.prop + 1) : 0)
    gathered = reduce(key=dst_ids, values=[updates], function=lambda ori, update: (update == 0) ? ori : ((ori == 0) ? update : (((ori & 2147483647) > (update & 2147483647)) ? update : ori)))
    next = map([gathered], lambda u: (u == 0) ? (self.prop & 2147483647) : ((self.prop == 2147483646) ? u : (self.prop & 2147483647)))
    return next as result_node_prop.prop
}
