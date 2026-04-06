#ifndef __HOST_CONFIG_H__
#define __HOST_CONFIG_H__

#include <stdint.h>

#define BIG_KERNEL_NUM 4
#define LITTLE_KERNEL_NUM 10

#define NUM_LITTLE_MERGERS 1
#define NUM_BIG_MERGERS 1

#define NUM_KERNEL (BIG_KERNEL_NUM + LITTLE_KERNEL_NUM)

static constexpr uint32_t LITTLE_MERGER_PIPELINE_LENGTHS[] = {LITTLE_KERNEL_NUM};
static constexpr uint32_t LITTLE_MERGER_KERNEL_OFFSETS[] = {0};
static constexpr uint32_t BIG_MERGER_PIPELINE_LENGTHS[] = {BIG_KERNEL_NUM};
static constexpr uint32_t BIG_MERGER_KERNEL_OFFSETS[] = {0};
static constexpr uint32_t LITTLE_KERNEL_GROUP_ID[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
static constexpr uint32_t BIG_KERNEL_GROUP_ID[] = {0, 0, 0, 0};

#define MAX_DST_BIG 524288
#define MAX_DST_LITTLE 65536
#define LOCAL_ID_BITWIDTH 32
#define LOCAL_ID_MSB (LOCAL_ID_BITWIDTH - 1)
#define INVALID_LOCAL_ID_BIG (1u << LOCAL_ID_MSB)
#define INVALID_LOCAL_ID_LITTLE (1u << LOCAL_ID_MSB)

#define DISTANCE_BITWIDTH 32
#define DISTANCE_INTEGER_PART 32
#define DISTANCE_SIGNED 1
#define DIST_PER_WORD 16
#define LOG_DIST_PER_WORD 4
#define DISTANCES_PER_REDUCE_WORD 2

#define EDGE_PROP_BITS 32
#define EDGE_PROP_COUNT 1
static const uint32_t EDGE_PROP_WIDTHS[EDGE_PROP_COUNT] = {32};

#define LITTLE_KERNEL_HBM_EDGE_ID {0, 2, 4, 6, 8, 10, 12, 14, 16, 18}
#define LITTLE_KERNEL_HBM_NODE_ID {1, 3, 5, 7, 9, 11, 13, 15, 17, 19}
#define BIG_KERNEL_HBM_EDGE_ID {20, 22, 24, 26}
#define BIG_KERNEL_HBM_NODE_ID {21, 23, 25, 27}

// HBM bank IDs used by system.cfg (see src/engine/hls_codegen.rs).
// Keep these in sync with the `sp=...:HBM[...]` mappings.
#define WRITER_OUTPUT_HBM_ID 31
#define APPLY_KERNEL_NODE_HBM_ID 30
#define APPLY_KERNEL_HAS_AUX_NODE_PROPS 1

#endif /* __HOST_CONFIG_H__ */
