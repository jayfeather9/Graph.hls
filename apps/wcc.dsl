{
    Node: {
        label: int<32>
    }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    src_labels = map([edges], lambda e: e.src.label)
    max_labels = reduce(key=dst_ids, values=[src_labels], function=lambda x, y: x > y ? x : y)
    return max_labels as result_node_prop.label
}
