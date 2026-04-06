#ifndef __ACC_SETUP_H__
#define __ACC_SETUP_H__

#include "common.h"
#include "host_config.h"
#include "xcl2.h"

typedef struct AccDescriptor {
    cl::CommandQueue q;

    std::vector<cl::CommandQueue> big_gs_queue;
    std::vector<cl::CommandQueue> little_gs_queue;
    std::vector<cl::CommandQueue> big_prop_loader_queue;
    std::vector<cl::CommandQueue> little_prop_loader_queue;
    cl::CommandQueue apply_queue;

    int num_big_krnl = BIG_KERNEL_NUM;
    int num_little_krnl = LITTLE_KERNEL_NUM;

    std::vector<cl::Kernel> big_gs_krnls;
    std::vector<cl::Kernel> little_gs_krnls;
    std::vector<cl::Kernel> big_prop_loader_krnls;
    std::vector<cl::Kernel> little_prop_loader_krnls;
    cl::Kernel apply_krnl;

    std::vector<std::vector<cl::Event>> big_kernel_events;
    std::vector<std::vector<cl::Event>> little_kernel_events;
    std::vector<cl::Event> big_prop_loader_events;
    std::vector<cl::Event> little_prop_loader_events;
    cl::Event apply_kernel_event;

    cl::Context context;

    // 新增
    std::vector<int> big_kernel_hbm_edge_id = BIG_KERNEL_DDR_EDGE_ID;
    std::vector<int> big_kernel_hbm_node_id = BIG_KERNEL_DDR_NODE_ID;

    std::vector<int> little_kernel_hbm_edge_id = LITTLE_KERNEL_DDR_EDGE_ID;
    std::vector<int> little_kernel_hbm_node_id = LITTLE_KERNEL_DDR_NODE_ID;

} AccDescriptor;

AccDescriptor initAccelerator(std::string xcl_file);

#endif
