{
    Node: {
        label: int<32>
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
            { pipelines: 4 merger_slr: 0 pipeline_slr: [0,0,0,0] },
            { pipelines: 4 merger_slr: 0 pipeline_slr: [0,0,0,0] }
        ]
        big_groups: [
            { pipelines: 1 merger_slr: 0 pipeline_slr: [0] },
            { pipelines: 1 merger_slr: 0 pipeline_slr: [0] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    src_labels = map([edges], lambda e: e.src.label)
    min_labels = reduce(key=dst_ids, values=[src_labels], function=lambda x, y: x < y ? x : y)
    return min_labels as result_node_prop.label
}

