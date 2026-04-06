{
    Node: {
        vec: vector<float, 16>
    }
    Edge: {
        rating: float
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    pair_parts = map([edges], lambda e: pair(outer_product(e.src.vec, e.src.vec), vector_scale(e.src.vec, e.rating)))
    accum = reduce(key=dst_ids, values=[pair_parts], function=lambda a, b: pair(matrix_add(a.0, b.0), vector_add(a.1, b.1)))
    solved = map([accum], lambda t: solve_linear(t.0, t.1))
    return solved as result_node_prop.vec
}
