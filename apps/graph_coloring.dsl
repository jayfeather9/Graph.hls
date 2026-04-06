{
    Node: {
        color: int<32>
        priority: int<32>
    }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    color_sets = map([edges], lambda e: make_set(e.src.color))
    merged_colors = reduce(key=dst_ids, values=[color_sets], function=lambda a, b: set_union(a, b))
    chosen_colors = map([merged_colors], lambda s: mex(s))
    return chosen_colors as result_node_prop.color
}
