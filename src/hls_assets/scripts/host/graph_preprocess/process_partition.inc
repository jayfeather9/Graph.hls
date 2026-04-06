    auto process_partition = [&](const Edge *partition_edges,
                                 size_t partition_edge_count,
                                 const std::vector<int> &ordered_dst_vertices,
                                 bool is_dense,
                                 int num_pipelines,
                                 std::vector<int> &global_to_local)
        -> PartitionDescriptor {
        PartitionDescriptor pd;
        pd.is_dense = is_dense;
        pd.num_pipelines = static_cast<unsigned int>(num_pipelines);
        pd.pipeline_edges.resize(static_cast<size_t>(num_pipelines));

        for (int pip = 0; pip < num_pipelines; ++pip) {
            pd.pipeline_edges[static_cast<size_t>(pip)].pipeline_id =
                static_cast<unsigned int>(pip);
            pd.pipeline_edges[static_cast<size_t>(pip)].num_edges = 0;
        }

        if (partition_edge_count == 0) {
            pd.num_edges = 0;
            pd.num_vertices = 0;
            pd.num_dsts = 0;
            return pd;
        }

        constexpr size_t SRC_ASSIGN_GRANULARITY = 256;

        int local_id_counter = 0;
        std::vector<int> touched_global_ids;
        touched_global_ids.reserve(ordered_dst_vertices.size() + 1024);

        auto map_vertex = [&](int global_id) {
            if (global_to_local[static_cast<size_t>(global_id)] != -1) {
                return;
            }
            const int local_id = local_id_counter++;
            global_to_local[static_cast<size_t>(global_id)] = local_id;
            touched_global_ids.push_back(global_id);
            pd.vtx_map[global_id] = local_id;
            pd.vtx_map_rev[local_id] = global_id;
        };

        for (int global_id : ordered_dst_vertices) {
            map_vertex(global_id);
        }
        pd.num_dsts = static_cast<unsigned int>(ordered_dst_vertices.size());

        const unsigned int max_dst =
            is_dense ? static_cast<unsigned int>(MAX_DST_LITTLE)
                     : static_cast<unsigned int>(MAX_DST_BIG);
        if (pd.num_dsts > max_dst) {
            std::cerr << "[ERROR] Partition has " << pd.num_dsts
                      << " destination vertices; exceeds MAX_DST=" << max_dst
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }

        for (size_t idx = 0; idx < partition_edge_count; ++idx) {
            map_vertex(partition_edges[idx].src);
            map_vertex(partition_edges[idx].dest);
        }

        pd.num_vertices = static_cast<unsigned int>(local_id_counter);
        pd.num_edges = static_cast<unsigned int>(partition_edge_count);
        const int invalid_dst_id =
            is_dense ? static_cast<int>(INVALID_LOCAL_ID_LITTLE)
                     : static_cast<int>(INVALID_LOCAL_ID_BIG);

        const size_t num_vertices = static_cast<size_t>(pd.num_vertices);
        const size_t num_blocks =
            (num_vertices + SRC_BUFFER_SIZE - 1) / SRC_BUFFER_SIZE;

        const size_t num_units =
            (num_vertices + SRC_ASSIGN_GRANULARITY - 1) / SRC_ASSIGN_GRANULARITY;
        std::vector<size_t> unit_edge_counts(num_units, 0);
        for (size_t idx = 0; idx < partition_edge_count; ++idx) {
            const int src_local =
                global_to_local[static_cast<size_t>(partition_edges[idx].src)];
            const size_t unit = static_cast<size_t>(src_local) / SRC_ASSIGN_GRANULARITY;
            unit_edge_counts[unit]++;
        }

        const size_t units_per_block = SRC_BUFFER_SIZE / SRC_ASSIGN_GRANULARITY;
        if ((SRC_BUFFER_SIZE % SRC_ASSIGN_GRANULARITY) != 0) {
            std::cerr << "[ERROR] SRC_ASSIGN_GRANULARITY must divide "
                         "SRC_BUFFER_SIZE."
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }

        auto padded_work_for_unit_range = [&](size_t start_unit,
                                              size_t end_unit) -> size_t {
            size_t work_with_pads = 0;
            int last_edge_block = -1;
            for (size_t unit = start_unit; unit < end_unit; ++unit) {
                const size_t edges = unit_edge_counts[unit];
                if (edges == 0) {
                    continue;
                }
                const int block = static_cast<int>(unit / units_per_block);
                if (is_dense && last_edge_block != -1 && block != last_edge_block) {
                    work_with_pads += pad_to_word(work_with_pads);
                }
                last_edge_block = block;
                work_with_pads += edges;
            }
            if (last_edge_block != -1) {
                work_with_pads += pad_to_word(work_with_pads);
            }
            return work_with_pads;
        };

        size_t total_edges = 0;
        for (size_t unit = 0; unit < num_units; ++unit) {
            total_edges += unit_edge_counts[unit];
        }

        std::vector<size_t> cut_units(static_cast<size_t>(num_pipelines) + 1, 0);
        cut_units[0] = 0;

        size_t unit_idx = 0;
        size_t cum_edges = 0;
        for (int pip = 0; pip < num_pipelines - 1; ++pip) {
            const size_t target =
                (total_edges * static_cast<size_t>(pip + 1) +
                 static_cast<size_t>(num_pipelines) - 1) /
                static_cast<size_t>(num_pipelines);

            while (unit_idx < num_units &&
                   cum_edges + unit_edge_counts[unit_idx] < target) {
                cum_edges += unit_edge_counts[unit_idx];
                ++unit_idx;
            }

            if (unit_idx < num_units) {
                const size_t without = cum_edges;
                const size_t with = cum_edges + unit_edge_counts[unit_idx];
                if ((with >= target) && (with - target <= target - without)) {
                    cum_edges = with;
                    ++unit_idx;
                }
            }

            cut_units[static_cast<size_t>(pip + 1)] = unit_idx;
        }
        cut_units[static_cast<size_t>(num_pipelines)] = num_units;

        auto recompute_work = [&](std::vector<size_t> &work) {
            work.assign(static_cast<size_t>(num_pipelines), 0);
            for (int pip = 0; pip < num_pipelines; ++pip) {
                work[static_cast<size_t>(pip)] = padded_work_for_unit_range(
                    cut_units[static_cast<size_t>(pip)],
                    cut_units[static_cast<size_t>(pip + 1)]);
            }
        };

        auto find_active_minmax = [&](const std::vector<size_t> &work,
                                      int &min_pip,
                                      int &max_pip,
                                      size_t &min_work,
                                      size_t &max_work) -> bool {
            min_pip = -1;
            max_pip = -1;
            min_work = 0;
            max_work = 0;
            for (int pip = 0; pip < num_pipelines; ++pip) {
                const size_t w = work[static_cast<size_t>(pip)];
                if (w == 0) {
                    continue;
                }
                if (min_pip == -1 || w < min_work) {
                    min_pip = pip;
                    min_work = w;
                }
                if (max_pip == -1 || w > max_work) {
                    max_pip = pip;
                    max_work = w;
                }
            }
            return min_pip != -1 && max_pip != -1 && min_pip != max_pip;
        };

        auto balanced_under_1pct = [&](const std::vector<size_t> &work) {
            int min_pip = -1;
            int max_pip = -1;
            size_t min_work = 0;
            size_t max_work = 0;
            if (!find_active_minmax(work, min_pip, max_pip, min_work, max_work)) {
                return true;
            }
            if (max_work == 0) {
                return true;
            }
            return (max_work - min_work) * 100 <= max_work;
        };

        std::vector<size_t> work_with_pads;
        recompute_work(work_with_pads);
        if (total_edges != 0 && !balanced_under_1pct(work_with_pads)) {
            constexpr int MAX_ADJUST_ITERS = 4096;
            for (int iter = 0; iter < MAX_ADJUST_ITERS; ++iter) {
                int min_pip = -1;
                int max_pip = -1;
                size_t min_work = 0;
                size_t max_work = 0;
                if (!find_active_minmax(work_with_pads, min_pip, max_pip, min_work,
                                        max_work)) {
                    break;
                }
                if ((max_work - min_work) * 100 <= max_work) {
                    break;
                }

                bool improved = false;
                size_t best_improvement = 0;
                enum class MoveKind { None, ShiftStartRight, ShiftEndLeft };
                MoveKind best_move = MoveKind::None;

                if (max_pip > 0) {
                    const size_t start = cut_units[static_cast<size_t>(max_pip)];
                    const size_t end = cut_units[static_cast<size_t>(max_pip + 1)];
                    if (start < end) {
                        const size_t new_start = start + 1;
                        if (new_start <= end &&
                            new_start >= cut_units[static_cast<size_t>(max_pip - 1)]) {
                            const size_t w_left = padded_work_for_unit_range(
                                cut_units[static_cast<size_t>(max_pip - 1)],
                                new_start);
                            const size_t w_mid =
                                padded_work_for_unit_range(new_start, end);

                            std::vector<size_t> tmp = work_with_pads;
                            tmp[static_cast<size_t>(max_pip - 1)] = w_left;
                            tmp[static_cast<size_t>(max_pip)] = w_mid;

                            int tmp_min_pip = -1;
                            int tmp_max_pip = -1;
                            size_t tmp_min_work = 0;
                            size_t tmp_max_work = 0;
                            if (find_active_minmax(tmp, tmp_min_pip, tmp_max_pip,
                                                   tmp_min_work, tmp_max_work)) {
                                const size_t old_span = max_work - min_work;
                                const size_t new_span = tmp_max_work - tmp_min_work;
                                if (new_span < old_span) {
                                    best_improvement = old_span - new_span;
                                    best_move = MoveKind::ShiftStartRight;
                                    improved = true;
                                }
                            }
                        }
                    }
                }

                if (max_pip < num_pipelines - 1) {
                    const size_t start = cut_units[static_cast<size_t>(max_pip)];
                    const size_t end = cut_units[static_cast<size_t>(max_pip + 1)];
                    if (start < end) {
                        const size_t new_end = end - 1;
                        if (new_end >= start &&
                            new_end <= cut_units[static_cast<size_t>(max_pip + 2)]) {
                            const size_t w_mid =
                                padded_work_for_unit_range(start, new_end);
                            const size_t w_right = padded_work_for_unit_range(
                                new_end,
                                cut_units[static_cast<size_t>(max_pip + 2)]);

                            std::vector<size_t> tmp = work_with_pads;
                            tmp[static_cast<size_t>(max_pip)] = w_mid;
                            tmp[static_cast<size_t>(max_pip + 1)] = w_right;

                            int tmp_min_pip = -1;
                            int tmp_max_pip = -1;
                            size_t tmp_min_work = 0;
                            size_t tmp_max_work = 0;
                            if (find_active_minmax(tmp, tmp_min_pip, tmp_max_pip,
                                                   tmp_min_work, tmp_max_work)) {
                                const size_t old_span = max_work - min_work;
                                const size_t new_span = tmp_max_work - tmp_min_work;
                                if (new_span < old_span) {
                                    const size_t improvement = old_span - new_span;
                                    if (!improved || improvement > best_improvement) {
                                        best_improvement = improvement;
                                        best_move = MoveKind::ShiftEndLeft;
                                        improved = true;
                                    }
                                }
                            }
                        }
                    }
                }

                if (!improved || best_move == MoveKind::None) {
                    break;
                }

                if (best_move == MoveKind::ShiftStartRight) {
                    cut_units[static_cast<size_t>(max_pip)] += 1;
                } else if (best_move == MoveKind::ShiftEndLeft) {
                    cut_units[static_cast<size_t>(max_pip + 1)] -= 1;
                }
                recompute_work(work_with_pads);
            }
        }

        std::vector<int> unit_to_pipeline(num_units, 0);
        int cur_pip = 0;
        for (size_t unit = 0; unit < num_units; ++unit) {
            while (cur_pip < num_pipelines - 1 &&
                   unit >= cut_units[static_cast<size_t>(cur_pip + 1)]) {
                cur_pip++;
            }
            unit_to_pipeline[unit] = cur_pip;
        }

        std::vector<uint32_t> outdeg_per_src(num_vertices, 0);
        std::vector<int> last_src_in_block(
            static_cast<size_t>(num_pipelines) * num_blocks, -1);
        std::vector<uint32_t> block_edges_in_pipeline(
            static_cast<size_t>(num_pipelines) * num_blocks, 0);

        for (size_t idx = 0; idx < partition_edge_count; ++idx) {
            const auto &e = partition_edges[idx];
            const int src_local = global_to_local[static_cast<size_t>(e.src)];
            const size_t block = static_cast<size_t>(src_local) / SRC_BUFFER_SIZE;
            const size_t unit =
                static_cast<size_t>(src_local) / SRC_ASSIGN_GRANULARITY;
            const int pip = unit_to_pipeline[unit];

            outdeg_per_src[static_cast<size_t>(src_local)]++;
            block_edges_in_pipeline[static_cast<size_t>(pip) * num_blocks + block]++;
            int &last =
                last_src_in_block[static_cast<size_t>(pip) * num_blocks + block];
            if (src_local > last) {
                last = src_local;
            }
        }

        for (int pip = 0; pip < num_pipelines; ++pip) {
            auto &pe = pd.pipeline_edges[static_cast<size_t>(pip)];
            std::vector<std::pair<int, uint16_t>> pads_at_src;
            pads_at_src.reserve(num_blocks + 1);

            size_t work_with_pads_local = 0;
            int prev_last_src = -1;
            bool has_blocks = false;

            for (size_t block = 0; block < num_blocks; ++block) {
                const uint32_t edges_in_block =
                    block_edges_in_pipeline[static_cast<size_t>(pip) * num_blocks +
                                            block];
                if (edges_in_block == 0) {
                    continue;
                }

                const int last_src =
                    last_src_in_block[static_cast<size_t>(pip) * num_blocks + block];
                if (last_src < 0) {
                    std::cerr << "[ERROR] Missing last_src for pipeline " << pip
                              << ", block " << block << "." << std::endl;
                    std::exit(EXIT_FAILURE);
                }

                if (has_blocks && is_dense) {
                    const size_t pad = pad_to_word(work_with_pads_local);
                    if (pad != 0) {
                        pads_at_src.push_back(
                            {prev_last_src, static_cast<uint16_t>(pad)});
                        work_with_pads_local += pad;
                    }
                }

                work_with_pads_local += static_cast<size_t>(edges_in_block);
                prev_last_src = last_src;
                has_blocks = true;
            }

            if (has_blocks) {
                const size_t pad = pad_to_word(work_with_pads_local);
                if (pad != 0) {
                    pads_at_src.push_back(
                        {prev_last_src, static_cast<uint16_t>(pad)});
                    work_with_pads_local += pad;
                }
            }

            pe.offsets.assign(num_vertices + 1, 0);
            size_t pad_idx = 0;
            for (size_t v = 0; v < num_vertices; ++v) {
                size_t row = 0;
                const size_t unit = v / SRC_ASSIGN_GRANULARITY;
                if (unit_to_pipeline[unit] == pip) {
                    row = static_cast<size_t>(outdeg_per_src[v]);
                }
                if (pad_idx < pads_at_src.size() &&
                    static_cast<size_t>(pads_at_src[pad_idx].first) == v) {
                    row += static_cast<size_t>(pads_at_src[pad_idx].second);
                    pad_idx++;
                }
                pe.offsets[v + 1] = pe.offsets[v] + static_cast<int>(row);
            }

            pe.num_edges =
                static_cast<unsigned int>(pe.offsets[static_cast<size_t>(pd.num_vertices)]);
            pe.columns.assign(static_cast<size_t>(pe.num_edges), invalid_dst_id);
            pe.weights.assign(static_cast<size_t>(pe.num_edges), 1);

            if (EDGE_PROP_COUNT > 0) {
                pe.edge_props.assign(static_cast<size_t>(pe.num_edges) *
                                         EDGE_PROP_COUNT,
                                     0);
            }

            for (const auto &pad_entry : pads_at_src) {
                const int src = pad_entry.first;
                const uint16_t pad = pad_entry.second;
                if (pad == 0) {
                    continue;
                }
                const size_t unit =
                    static_cast<size_t>(src) / SRC_ASSIGN_GRANULARITY;
                if (unit_to_pipeline[unit] != pip) {
                    std::cerr << "[ERROR] Padding src assigned to wrong pipeline."
                              << std::endl;
                    std::exit(EXIT_FAILURE);
                }

                const size_t start =
                    static_cast<size_t>(pe.offsets[static_cast<size_t>(src)]) +
                    static_cast<size_t>(outdeg_per_src[static_cast<size_t>(src)]);
                for (size_t k = 0; k < static_cast<size_t>(pad); ++k) {
                    pe.columns[start + k] = invalid_dst_id;
                    pe.weights[start + k] = 1;
                    if (EDGE_PROP_COUNT > 0) {
                        const size_t base = (start + k) * EDGE_PROP_COUNT;
                        pe.edge_props[base] = 1;
                    }
                }
            }
        }

        std::vector<uint32_t> write_pos_per_src(num_vertices, 0);
        for (size_t v = 0; v < num_vertices; ++v) {
            const int pip = unit_to_pipeline[v / SRC_ASSIGN_GRANULARITY];
            write_pos_per_src[v] = static_cast<uint32_t>(
                pd.pipeline_edges[static_cast<size_t>(pip)].offsets[v]);
        }

        for (size_t idx = 0; idx < partition_edge_count; ++idx) {
            const auto &e = partition_edges[idx];
            const int src_local = global_to_local[static_cast<size_t>(e.src)];
            const int dst_local = global_to_local[static_cast<size_t>(e.dest)];
            const size_t unit =
                static_cast<size_t>(src_local) / SRC_ASSIGN_GRANULARITY;
            const int pip = unit_to_pipeline[unit];

            auto &pe = pd.pipeline_edges[static_cast<size_t>(pip)];
            const size_t pos =
                static_cast<size_t>(write_pos_per_src[static_cast<size_t>(src_local)]++);
            pe.columns[pos] = dst_local;
            pe.weights[pos] = e.weight;

            if (EDGE_PROP_COUNT > 0) {
                const size_t base = pos * EDGE_PROP_COUNT;
                for (size_t p_idx = 0; p_idx < EDGE_PROP_COUNT; ++p_idx) {
                    pe.edge_props[base + p_idx] = e.props[p_idx];
                }
            }
        }

        for (int global_id : touched_global_ids) {
            global_to_local[static_cast<size_t>(global_id)] = -1;
        }

        return pd;
    };
