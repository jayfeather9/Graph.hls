{
    Node: {
        label: int<32>
    }
    Edge: {}
}

HlsConfig {
    memory: ddr
    local_id_bits: 22
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
    src_labels = map([edges], lambda e: e.src.label)
    max_labels = reduce(key=dst_ids, values=[src_labels], function=lambda x, y: x > y ? x : y)
    return max_labels as result_node_prop.label
}
