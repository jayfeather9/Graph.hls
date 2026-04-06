{
    Node: {
        label: int<32>
    }
    Edge: {}
}

HlsConfig {
    topology: {
        apply_slr: 1
        hbm_writer_slr: 0
        cross_slr_fifo_depth: 16
        little_groups: [
            { pipelines: 3 merger_slr: 0 pipeline_slr: [0,0,0] },
            { pipelines: 3 merger_slr: 1 pipeline_slr: [1,1,1] },
            { pipelines: 3 merger_slr: 2 pipeline_slr: [2,2,2] }
        ]
        big_groups: [
            { pipelines: 2 merger_slr: 0 pipeline_slr: [0,1] },
            { pipelines: 3 merger_slr: 2 pipeline_slr: [1,2,2] }
        ]
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    src_labels = map([edges], lambda e: e.src.label)
    max_labels = reduce(key=dst_ids, values=[src_labels], function=lambda x, y: x > y ? x : y)
    return max_labels as result_node_prop.label
}
